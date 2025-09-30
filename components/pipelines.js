
// $ parsers
const { chunkForSize } = require("./parsers.js");

// $ streamers
const _ = require('highland');

// $ networking + filesystem
const { exportEvents, exportProfiles, deleteProfiles } = require('./exporters');
const { flushLookupTable, flushToMixpanel, flushToMixpanelWithUndici } = require('./importers.js');
const { replaceAnnotations, getAnnotations, deleteAnnotations } = require('./meta.js');
const fs = require('fs');


// $ env
const cliParams = require('./cli.js');
const counter = cliParams.showProgress;
const { logger } = require('../components/logs.js');

// $ transforms
const { isNotEmpty } = require('./transforms.js');

// $ utils
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);
const { callbackify } = require('util');







/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */
/** @typedef {import('../index').Options} Options */
/** @typedef {import('../index').Creds} Creds */
/** @typedef {import('../index').ImportResults} ImportResults */

/**
 * the core pipeline 
 * @param {ReadableStream | null} stream 
 * @param {JobConfig} job 
 * @returns {Promise<ImportResults>} a promise
 */
function corePipeline(stream, job, toNodeStream = false) {
	const l = logger(job);
	if (job.recordType === 'table') return flushLookupTable(stream, job);
	// @ts-ignore
	if (job.recordType === 'export' && typeof stream === 'string') return exportEvents(stream, job);
	// @ts-ignore
	if (job.recordType === 'profile-export' && typeof stream === 'string') return exportProfiles(stream, job);
	// @ts-ignore
	if (job.recordType === 'annotations') return replaceAnnotations(stream, job);


	if (job.recordType === 'get-annotations') return getAnnotations(job);
	if (job.recordType === 'delete-annotations') return deleteAnnotations(job);
	if (job.recordType === 'profile-delete') return deleteProfiles(job);


	const LOG_INTERVAL = 100; // ms
	let lastLogUpdate = Date.now();

	let flush;
	// Select transport based on job.transport setting
	if (job.transport === 'undici') {
		flush = _.wrapCallback(callbackify(flushToMixpanelWithUndici));
	} else {
		// Default to 'got' transport for backwards compatibility
		flush = _.wrapCallback(callbackify(flushToMixpanel));
	}

	let fileStream;
	if (job.writeToFile) {
		fileStream = fs.createWriteStream(job.outputFilePath, { flags: 'a', highWaterMark: job.highWater });
	}

	// Cache for JSON strings to avoid re-stringifying
	const jsonCache = new WeakMap();
	const bytesCache = new WeakMap();



	// @ts-ignore
	const mpPipeline = _.pipeline(


		// * only JSON from stream with maxRecords termination
		// @ts-ignore
		_.consume(function FIRST_EXISTENCE(err, x, push, next) {
			if (err) {
				push(err);
				next();
				return;
			}
			
			if (x === _.nil) {
				push(null, x);
				return;
			}

			// Check maxRecords limit BEFORE processing
			if (job.maxRecords !== null && job.recordsProcessed >= job.maxRecords) {
				// Terminate the stream completely
				push(null, _.nil);
				return;
			}

			job.recordsProcessed++;

			// very small chance of mem sampling
			Math.random() <= 0.00005 ? job.memSamp() : null;

			const exists = isNotEmpty(x);
			if (exists) {
				push(null, x);
				next();
			} else {
				job.empty++;
				next(); // Skip this item but continue
			}
		}),

		// * apply vendor transforms
		// @ts-ignore
		_.map(function VENDOR_TRANSFORM(data) {
			// @ts-ignore
			if (job.vendor && job.vendorTransform) data = job.vendorTransform(data, job.heavyObjects);
			return data;
		}),

		// * allow for "exploded" transforms [{},{},{}] to emit single events {}
		// @ts-ignore
		_.flatten(),

		// * apply user defined transform
		// @ts-ignore
		_.map(function USER_TRANSFORM(data) {
			if (job.transformFunc) data = job.transformFunc(data, job.heavyObjects);
			return data;
		}),

		// * allow for "exploded" transforms [{},{},{}] to emit single events {}
		// @ts-ignore
		_.flatten(),

		// * dedupe
		// @ts-ignore
		_.map(function DEDUPE(data) {
			if (job.dedupe) data = job.deduper(data);
			return data;
		}),

		// * post-transform filter to ignore nulls + empty objects
		// @ts-ignore
		_.filter(function SECOND_EXISTENCE(data) {
			const exists = isNotEmpty(data);
			if (exists) return true;
			else {
				job.empty++;
				return false;
			}

		}),

		// * helper transforms
		// @ts-ignore
		_.map(function HELPER_TRANSFORMS(data) {
			if (job.shouldApplyAliases) job.applyAliases(data);
			if (job.recordType === "scd") data = job.scdTransform(data);
			if (job.fixData) data = job.ezTransform(data);
			if (job.removeNulls) job.nullRemover(data);
			if (job.timeOffset) job.UTCoffset(data);
			if (job.shouldAddTags) job.addTags(data);
			if (job.shouldWhiteBlackList) data = job.whiteAndBlackLister(data);
			if (job.shouldEpochFilter) data = job.epochFilter(data);
			if (job.propertyScrubber) job.propertyScrubber(data);
			if (job.columnDropper) job.columnDropper(data);
			if (job.flattenData) job.flattener(data);
			if (job.fixJson) job.jsonFixer(data);
			if (job.shouldCreateInsertId) job.insertIdAdder(data);
			if (job.addToken) job.tokenAdder(data);
			if (job.fixTime) job.timeTransform(data);
			return data;
		}),

		// * post-transform filter to ignore nulls + cache JSON
		// @ts-ignore
		_.map(function THIRD_EXISTENCE_AND_STRINGIFY(data) {
			const exists = isNotEmpty(data);
			if (!exists) {
				job.empty++;
				return null; // Will be filtered out
			}

			// Cache JSON stringification and byte count using WeakMaps
			const jsonString = JSON.stringify(data);
			const byteLength = Buffer.byteLength(jsonString, 'utf-8');
			job.bytesProcessed += byteLength;

			// Store in WeakMaps (doesn't modify original object)
			jsonCache.set(data, jsonString);
			bytesCache.set(data, byteLength);

			return data;
		}),

		// * filter out nulls from existence check
		// @ts-ignore
		_.filter(data => data !== null),

		// * batch for # of items
		// @ts-ignore
		_.batch(job.recordsPerBatch),

		// * batch for req size (with cached optimization)
		// @ts-ignore
		_.consume(chunkForSize({ ...job, bytesCache })),

		// * send to mixpanel
		// @ts-ignore
		_.map(function HTTP_REQUESTS(batch) {
			job.requests++;
			job.batches++;
			job.addBatchLength(batch.length); // Use bounded collection method

			if (job.dryRun) return _(Promise.resolve([null, batch]));

			if (job.writeToFile) {
				batch.forEach(item => {
					// Use cached JSON from WeakMap
					const cachedJson = jsonCache.get(item);
					fileStream.write((cachedJson || JSON.stringify(item)) + '\n');
				});
				return _(Promise.resolve(batch));
			}

			return flush(batch, job);
		}),

		// * concurrency
		// @ts-ignore
		_.mergeWithLimit(job.workers),

		// * verbose
		// @ts-ignore
		_.doto(function VERBOSE(result) {
			const [response, batch] = result;
			if (job.responseHandler && typeof job.responseHandler === 'function') {
				job.responseHandler(response, batch);

			}

			if (job.dryRun) {
				batch.forEach(data => {
					job.dryRunResults.push(data);
					// if (job.verbose) console.log(JSON.stringify(data, null, 2));
				});
			}
			else {
				const now = Date.now();
				if ((job.verbose || job.showProgress) && (now - lastLogUpdate >= LOG_INTERVAL)) {
					counter(job.recordType, job.recordsProcessed, job.requests, job.getEps(), job.success, job.failed, job.bytesProcessed, job.progressCallback);
					lastLogUpdate = now;
				}

			}

		}),

		// * errors
		// @ts-ignore
		_.errors(function ERRORS(e) {
			throw e;
		})
	);

	// log exactly once, on the very first data record
	mpPipeline.once('data', () => {
		l(`\n\nDATA FLOWING\n`);
	});


	if (toNodeStream) {
		return mpPipeline;
	}

	// @ts-ignore
	stream.pipe(mpPipeline);
	return mpPipeline.collect().toPromise(Promise)
		.then((results) => {
			if (fileStream) {
				fileStream.end();
			}
			return results;
		});

}




module.exports = {
	corePipeline
};