// $ config
const importJob = require('./job.js');

// $ parsers
const { getEnvVars, chunkForSize } = require("./parsers.js");

// $ streamers
const _ = require('highland');

// $ networking
const { exportEvents, exportProfiles } = require('./exporters');
const { flushLookupTable, flushToMixpanel } = require('./importers.js');

// $ env
const cliParams = require('./cli.js');
const counter = cliParams.showProgress;


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
	const epochStart = dayjs.unix(jobConfig.epochStart).utc();
	const epochEnd = dayjs.unix(jobConfig.epochEnd).utc();

	// @ts-ignore
	const mpPipeline = _.pipeline(
		
		// * only JSON from stream
		// @ts-ignore
		_.filter((data) => {
			jobConfig.recordsProcessed++;
			if (data && JSON.stringify(data) !== '{}') {
				return true;
			}
			else {
				jobConfig.empty++;
				return false;
			}
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
			if (data) {
				const str = JSON.stringify(data);
				if (str !== '{}' && str !== '[]' && str !== '""' && str !== 'null') {

					return true;
				}
				else {
					jobConfig.empty++;
					return false;
				}
			}
			else {
				jobConfig.empty++;
				return false;
			}
		}),

		// * helper transforms
		// @ts-ignore
		_.map((data) => {
			if (Object.keys(jobConfig.aliases).length) data = jobConfig.applyAliases(data);
			if (jobConfig.fixData) data = jobConfig.ezTransform(data);
			if (jobConfig.removeNulls) data = jobConfig.nullRemover(data);
			if (jobConfig.timeOffset) data = jobConfig.UTCoffset(data);
			if (Object.keys(jobConfig.tags).length) data = jobConfig.addTags(data);
			if (jobConfig.shouldWhiteBlackList) data = jobConfig.whiteAndBlackLister(data);

			//start/end epoch filtering
			//todo: move this to it's own function
			if (jobConfig.recordType === 'event') {
				if (data?.properties?.time) {
					let eventTime = data.properties.time;
					if (eventTime.toString().length === 10) eventTime = eventTime * 1000;
					eventTime = dayjs.utc(eventTime);
					if (eventTime.isBefore(epochStart)) {
						jobConfig.outOfBounds++;
						return null;
					}
					else if (eventTime.isAfter(epochEnd)) {
						jobConfig.outOfBounds++;
						return null;
					}
				}
			}
			return data;
		}),

		// * post-transform filter to ignore nulls + count byte size
		// @ts-ignore
		_.filter((data) => {
			if (!data) {
				jobConfig.empty++;
				return false;
			}
			const str = JSON.stringify(data);
			if (str === '{}' || str === '[]' || str === 'null') {
				jobConfig.empty++;
				return false;
			}
			jobConfig.bytesProcessed += Buffer.byteLength(str, 'utf-8');
			return true;
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
			return flush(batch, jobConfig);
		}),

		// * concurrency
		// @ts-ignore
		_.mergeWithLimit(jobConfig.workers),

		// * verbose
		// @ts-ignore
		_.doto(() => {
			if (jobConfig.verbose) counter(jobConfig.recordType, jobConfig.recordsProcessed, jobConfig.requests);
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



/**
 * @param {Creds} creds - mixpanel project credentials
 * @param {Options} opts - import options
 * @param {function(): importJob | void} finish - end of pipelines
 * @returns a transform stream
 */
function pipeInterface(creds = {}, opts = {}, finish = () => { }) {
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	config.timer.start();

	const pipeToMe = corePipeline(null, config, true);

	// * handlers
	// @ts-ignore
	pipeToMe.on('end', () => {
		config.timer.end(false);

		// @ts-ignore
		finish(null, config.summary());
	});

	// @ts-ignore
	pipeToMe.on('pipe', () => {

		// @ts-ignore
		pipeToMe.resume();
	});

	// @ts-ignore
	pipeToMe.on('error', (e) => {
		if (config.verbose) {
			console.log(e);
		}
		// @ts-ignore
		finish(e, config.summary());
	});

	return pipeToMe;
}


module.exports = {
	corePipeline,
	pipeInterface
};