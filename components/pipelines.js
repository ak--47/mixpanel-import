
// $ parsers
const { chunkForSize } = require("./parsers.js");

// $ streamers
const _ = require('highland');

// $ networking + filesystem
const { exportEvents, exportProfiles } = require('./exporters');
const { flushLookupTable, flushToMixpanel } = require('./importers.js');
const fs = require('fs');


// $ env
const cliParams = require('./cli.js');
const counter = cliParams.showProgress;

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
 * @returns {Promise<ImportResults> | Promise<null>} a promise
 */
function corePipeline(stream, job, toNodeStream = false) {

	if (job.recordType === 'table') return flushLookupTable(stream, job);
	if (job.recordType === 'export' && typeof stream === 'string') return exportEvents(stream, job);
	if (job.recordType === 'profile-export' && typeof stream === 'string') return exportProfiles(stream, job);

	const flush = _.wrapCallback(callbackify(flushToMixpanel));
	let fileStream;
	if (job.writeToFile) {
		fileStream = fs.createWriteStream(job.outputFilePath, { flags: 'a', highWaterMark: job.highWater });
	}


	// @ts-ignore
	const mpPipeline = _.pipeline(
	

		// * only JSON from stream
		// @ts-ignore
		_.filter(function FIRST_EXISTENCE(data) {
			job.recordsProcessed++;
			// very small chance of mem sampling
			Math.random() <= 0.00005 ? job.memSamp() : null;

			const exists = isNotEmpty(data);
			if (exists) return true;
			else {
				job.empty++;
				return false;
			}


		}),

		// * apply vendor transforms
		// @ts-ignore
		_.map(function VENDOR_TRANSFORM(data) {
			if (job.vendor && job.vendorTransform) data = job.vendorTransform(data);
			return data;
		}),

		// * allow for "exploded" transforms [{},{},{}] to emit single events {}
		// @ts-ignore
		_.flatten(),

		// * apply user defined transform
		// @ts-ignore
		_.map(function USER_TRANSFORM(data) {
			if (job.transformFunc) data = job.transformFunc(data);
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
			if (job.flattenData) job.flattener(data);
			if (job.fixJson) job.jsonFixer(data);
			if (job.shouldCreateInsertId) job.insertIdAdder(data);
			if (job.addToken) job.tokenAdder(data);
			return data;
		}),

		// * post-transform filter to ignore nulls + count byte size
		// @ts-ignore
		_.filter(function THIRD_EXISTENCE(data) {
			const exists = isNotEmpty(data);
			if (exists) {
				job.bytesProcessed += Buffer.byteLength(JSON.stringify(data), 'utf-8');
				return true;
			}
			else {
				job.empty++;
				return false;
			}
		}),

		// * batch for # of items
		// @ts-ignore
		_.batch(job.recordsPerBatch),

		// * batch for req size
		// @ts-ignore
		_.consume(chunkForSize(job)),

		// * send to mixpanel
		// @ts-ignore
		_.map(function HTTP_REQUESTS(batch) {
			job.requests++;
			job.batches++;
			job.batchLengths.push(batch.length);
			job.lastBatchLength = batch.length;
			if (job.dryRun) return _(Promise.resolve(batch));
			if (job.writeToFile) {
				batch.forEach(data => {
					fileStream.write(JSON.stringify(data) + '\n');
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
		_.doto(function VERBOSE(batch) {
			if (job.dryRun) {
				batch.forEach(data => {
					job.dryRunResults.push(data);
					if (job.verbose) console.log(JSON.stringify(data, null, 2));
				});
			}
			else {
				if (job.verbose || job.showProgress) counter(job.recordType, job.recordsProcessed, job.requests, job.getEps());
			}

		}),

		// * errors
		// @ts-ignore
		_.errors(function ERRORS(e) {
			throw e;
		})
	);

	if (toNodeStream) {
		return mpPipeline;
	}

	// @ts-ignore
	stream.pipe(mpPipeline);
	return mpPipeline.collect().toPromise(Promise)
		.then(() => {
			if (fileStream) {
				fileStream.end();
			}
		});

}




module.exports = {
	corePipeline
};