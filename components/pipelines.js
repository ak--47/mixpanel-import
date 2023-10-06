
// $ parsers
const { chunkForSize } = require("./parsers.js");

// $ streamers
const _ = require('highland');

// $ networking
const { exportEvents, exportProfiles } = require('./exporters');
const { flushLookupTable, flushToMixpanel } = require('./importers.js');

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
 * @param {JobConfig} jobConfig 
 * @returns {Promise<ImportResults> | Promise<null>} a promise
 */
function corePipeline(stream, jobConfig, toNodeStream = false) {

	if (jobConfig.recordType === 'table') return flushLookupTable(stream, jobConfig);
	if (jobConfig.recordType === 'export' && typeof stream === 'string') return exportEvents(stream, jobConfig);
	if (jobConfig.recordType === 'peopleExport' && typeof stream === 'string') return exportProfiles(stream, jobConfig);

	const flush = _.wrapCallback(callbackify(flushToMixpanel));

	// @ts-ignore
	const mpPipeline = _.pipeline(

		// * only JSON from stream
		// @ts-ignore
		_.filter((data) => {
			jobConfig.recordsProcessed++;
			// very small chance of mem sampling
			Math.random() <= 0.00005 ? jobConfig.memSamp() : null;

			const exists = isNotEmpty(data);
			if (exists) return true;
			else {
				jobConfig.empty++;
				return false;
			}


		}),

		// * apply vendor transforms
		// @ts-ignore
		_.map((data) => {
			if (jobConfig.vendor && jobConfig.vendorTransform) data = jobConfig.vendorTransform(data);
			return data;
		}),

		// * apply user defined transform
		// @ts-ignore
		_.map((data) => {
			if (jobConfig.transformFunc) data = jobConfig.transformFunc(data);
			return data;
		}),

		// * allow for "exploded" transforms [{},{},{}] to emit single events {}
		// @ts-ignore
		_.flatten(),

		// * dedupe
		// @ts-ignore
		_.map((data) => {
			if (jobConfig.dedupe) data = jobConfig.deduper(data);
			return data;
		}),

		// * post-transform filter to ignore nulls + empty objects
		// @ts-ignore
		_.filter((data) => {
			const exists = isNotEmpty(data);
			if (exists) return true;
			else {
				jobConfig.empty++;
				return false;
			}

		}),

		// * helper transforms
		// @ts-ignore
		_.map((data) => {
			if (jobConfig.shouldApplyAliases) data = jobConfig.applyAliases(data);
			if (jobConfig.fixData) data = jobConfig.ezTransform(data);
			if (jobConfig.removeNulls) data = jobConfig.nullRemover(data);
			if (jobConfig.timeOffset) data = jobConfig.UTCoffset(data);
			if (jobConfig.shouldAddTags) data = jobConfig.addTags(data);
			if (jobConfig.shouldWhiteBlackList) data = jobConfig.whiteAndBlackLister(data);
			if (jobConfig.shouldEpochFilter) data = jobConfig.epochFilter(data);
			return data;
		}),

		// * post-transform filter to ignore nulls + count byte size
		// @ts-ignore
		_.filter((data) => {
			const exists = isNotEmpty(data);
			if (exists) {
				jobConfig.bytesProcessed += Buffer.byteLength(JSON.stringify(data), 'utf-8');
				return true;
			}
			else {
				jobConfig.empty++;
				return false;
			}
		}),

		// * batch for # of items
		// @ts-ignore
		_.batch(jobConfig.recordsPerBatch),

		// * batch for req size
		// @ts-ignore
		_.consume(chunkForSize(jobConfig)),

		// * send to mixpanel
		// @ts-ignore
		_.map((batch) => {
			jobConfig.requests++;
			jobConfig.batches++;
			jobConfig.batchLengths.push(batch.length);
			if (jobConfig.dryRun) return _(Promise.resolve(batch));
			return flush(batch, jobConfig);
		}),

		// * concurrency
		// @ts-ignore
		_.mergeWithLimit(jobConfig.workers),

		// * verbose
		// @ts-ignore
		_.doto((batch) => {
			if (jobConfig.dryRun) {
				batch.forEach(data => {
					jobConfig.dryRunResults.push(data);
					if (jobConfig.verbose) console.log(JSON.stringify(data, null, 2));
				});
			}
			else {
				if (jobConfig.verbose) counter(jobConfig.recordType, jobConfig.recordsProcessed, jobConfig.requests);
			}

		}),

		// * errors
		// @ts-ignore
		_.errors((e) => {
			throw e;
		})
	);

	if (toNodeStream) {
		return mpPipeline;
	}

	// @ts-ignore
	stream.pipe(mpPipeline);
	return mpPipeline.collect().toPromise(Promise);

}




module.exports = {
	corePipeline
};