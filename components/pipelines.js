// $ config
const importJob = require('./config.js');

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

/**
 * the core pipeline 
 * @param {ReadableStream} stream 
 * @param {importJob} config 
 * @returns {Promise<import('../index.d.ts').ImportResults> | Promise<void>} a promise
 */
function corePipeline(stream, config, toNodeStream = false) {

	if (config.recordType === 'table') return flushLookupTable(stream, config);
	if (config.recordType === 'export') return exportEvents(stream, config);
	if (config.recordType === 'peopleExport') return exportProfiles(stream, config);

	const flush = _.wrapCallback(callbackify(flushToMixpanel));
	const epochStart = dayjs.unix(config.epochStart).utc();
	const epochEnd = dayjs.unix(config.epochEnd).utc();

	// @ts-ignore
	const mpPipeline = _.pipeline(

		// * only JSON from stream
		// @ts-ignore
		_.filter((data) => {
			config.recordsProcessed++;
			if (data && JSON.stringify(data) !== '{}') {
				return true;
			}
			else {
				config.empty++;
				return false;
			}
		}),

		// * apply user defined transform
		// @ts-ignore
		_.map((data) => {
			if (config.transformFunc) data = config.transformFunc(data);
			return data;
		}),

		// * allow for "exploded" transforms [{},{},{}] to emit single events {}
		// @ts-ignore
		_.flatten(),

		// * dedupe
		// @ts-ignore
		_.map((data) => {
			if (config.dedupe) data = config.deduper(data);
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
					config.empty++;
					return false;
				}
			}
			else {
				config.empty++;
				return false;
			}
		}),

		// * helper transforms
		// @ts-ignore
		_.map((data) => {
			if (Object.keys(config.aliases).length) data = config.applyAliases(data);
			if (config.fixData) data = config.ezTransform(data);
			if (config.removeNulls) data = config.nullRemover(data);
			if (config.timeOffset) data = config.UTCoffset(data);
			if (Object.keys(config.tags).length) data = config.addTags(data);
			if (config.shouldWhiteBlackList) data = config.whiteAndBlackLister(data);

			//start/end epoch filtering
			//todo: move this to it's own function
			if (config.recordType === 'event') {
				if (data?.properties?.time) {
					let eventTime = data.properties.time;
					if (eventTime.toString().length === 10) eventTime = eventTime * 1000;
					eventTime = dayjs.utc(eventTime);
					if (eventTime.isBefore(epochStart)) {
						config.outOfBounds++;
						return null;
					}
					else if (eventTime.isAfter(epochEnd)) {
						config.outOfBounds++;
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
				config.empty++;
				return false;
			}
			const str = JSON.stringify(data);
			if (str === '{}' || str === '[]' || str === 'null') {
				config.empty++;
				return false;
			}
			config.bytesProcessed += Buffer.byteLength(str, 'utf-8');
			return true;
		}),

		// * batch for # of items
		// @ts-ignore
		_.batch(config.recordsPerBatch),

		// * batch for req size
		// @ts-ignore
		_.consume(chunkForSize(config)),

		// * send to mixpanel
		// @ts-ignore
		_.map((batch) => {
			config.requests++;
			config.batches++;
			config.batchLengths.push(batch.length);
			return flush(batch, config);
		}),

		// * concurrency
		// @ts-ignore
		_.mergeWithLimit(config.workers),

		// * verbose
		// @ts-ignore
		_.doto(() => {
			if (config.verbose) counter(config.recordType, config.recordsProcessed, config.requests);
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
 * @param {import('../index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('../index.d.ts').Options} opts - import options
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
}