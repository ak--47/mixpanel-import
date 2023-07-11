#! /usr/bin/env node

/*
----
MIXPANEL IMPORT
by AK
purpose: stream events, users, groups, tables into mixpanel... with best practices!
----
*/


/*
-----
DEPS
-----
*/

// $ config
const importJob = require('./config');

// $ parsers
const readline = require('readline');

const Papa = require('papaparse');

const { parser: jsonlParser } = require('stream-json/jsonl/Parser');
// const { parser: jsonlParser } = require('stream-json-ak/jsonl/Parser');

const StreamArray = require('stream-json/streamers/StreamArray'); //json parser
// const StreamArray = require('stream-json-ak/streamers/StreamArray'); //json parser


// $ streamers
const _ = require('highland');
const stream = require('stream');
const got = require('got');
const https = require('https');
const MultiStream = require('multistream');

// $ file system
const path = require('path');
const fs = require('fs');
const os = require("os");


// $ env
require('dotenv').config();
const cliParams = require('./cli');

// $ utils
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);
const { pick } = require('underscore');
const { gzip } = require('node-gzip');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);
const dateFormat = `YYYY-MM-DD`;
const { callbackify } = require('util');

// $ exporters
const { exportEvents, exportProfiles } = require('./exporters');



/*
----
CORE
----
*/

/**
 * Mixpanel Importer
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * const mp = require('mixpanel-import')
 * const imported = await mp(creds, data, options)
 * @param {import('./index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('./index.d.ts').Data} data - data to import
 * @param {import('./index.d.ts').Options} opts - import options
 * @param {boolean} isCLI - `true` when run as CLI
 * @returns {Promise<import('./index.d.ts').ImportResults>} API receipts of imported data
 */
async function main(creds = {}, data, opts = {}, isCLI = false, telemetry = true) {
	if (telemetry) track('start', { runId });
	let cliData = {};

	// gathering params
	const envVar = getEnvVars();
	let cli = {};
	if (isCLI) {
		cli = cliParams();
		cliData = cli._[0];
	}

	const config = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	if (isCLI) config.verbose = true;
	const l = logger(config);
	l(cliParams.welcome);
	global.l = l;

	// ETL
	config.timer.start();

	const stream = await determineData(data || cliData, config); // always stream[]

	try {
		// eslint-disable-next-line no-unused-vars
		const result = await corePipeline(stream, config).finally(() => {
			l(`\n\nFINISHED!\n\n`);
		});
	}

	catch (e) {
		l(`ERROR: ${e.message}`);
		if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
	}

	l('\n');

	// clean up
	config.timer.end(false);
	const summary = config.summary();
	l(`${config.type === 'export' ? 'export' : 'import'} complete in ${summary.human}`);
	if (config.logs) await writeLogs(summary);
	if (telemetry) track('end', { runId, ...config.summary(false) });
	return summary;
}


/**
 * the core pipeline 
 * @param {ReadableStream} stream 
 * @param {importJob} config 
 * @returns {Promise<import('./index.d.ts').ImportResults> | Promise<void>} a promise
 */
function corePipeline(stream, config, toNodeStream = false) {

	if (config.recordType === 'table') return flushLookupTable(stream, config);
	if (config.recordType === 'export') return exportEvents(stream, config);
	if (config.recordType === 'peopleExport') return exportProfiles(stream, config);

	const flush = _.wrapCallback(callbackify(flushToMixpanel));

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
			if (config.verbose) showProgress(config.recordType, config.recordsProcessed, config.requests);
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
 * Mixpanel Importer Stream
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * // pipe a stream to mixpanel
 * const { mpStream } = require('mixpanel-import')
 * const mpStream = createMpStream(creds, opts, callback);
 * const observer = new PassThrough({objectMode: true})
 * observer.on('data', (response)=> { })
 * // create a pipeline
 * myStream.pipe(mpStream).pipe(observer);
 * @param {import('./index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('./index.d.ts').Options} opts - import options
 * @param {function(): importJob} finish - end of pipelines
 * @returns a transform stream
 */
// @ts-ignore
function pipeInterface(creds = {}, opts = {}, finish = () => { }, telemetry = true) {
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	config.timer.start();

	const pipeToMe = corePipeline(null, config, true);

	// * handlers
	// @ts-ignore
	pipeToMe.on('end', () => {
		config.timer.end(false);
		if (telemetry) track('end', { runId, ...config.summary(false) });
		// @ts-ignore
		finish(null, config.summary());
	});

	// @ts-ignore
	pipeToMe.on('pipe', () => {
		if (telemetry) track('start', { runId });
		// @ts-ignore
		pipeToMe.resume();
	});

	// @ts-ignore
	pipeToMe.on('error', (e) => {
		if (config.verbose) {
			console.log(e);
		}
	});

	return pipeToMe;
}


/*
----
HELPERS
----
*/
/**
 * @param  {Object[]} batch
 * @param  {Object} config
 */
async function flushToMixpanel(batch, config) {
	try {
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (config.recordType === 'event' && config.compress) {
			body = await gzip(body, { level: config.compressionLevel || 6 });
			config.encoding = 'gzip';
		}

		/** @type {got.Options} */
		const options = {
			url: config.url,
			searchParams: {
				ip: 0,
				verbose: 1,
				strict: Number(config.strict)
			},
			method: config.reqMethod,
			retry: {
				limit: config.maxRetries || 10,
				statusCodes: [429, 500, 501, 503, 524, 502],
				errorCodes: [
					`ETIMEDOUT`,
					`ECONNRESET`,
					`EADDRINUSE`,
					`ECONNREFUSED`,
					`EPIPE`,
					`ENOTFOUND`,
					`ENETUNREACH`,
					`EAI_AGAIN`,
					`ESOCKETTIMEDOUT`,
					`ECONNABORTED`,
					`EHOSTUNREACH`
				],
				methods: ['POST'],
				// @ts-ignore
				noise: 100

			},
			headers: {
				"Authorization": `${config.auth}`,
				"Content-Type": config.contentType,
				"Content-Encoding": config.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			agent: {
				https: new https.Agent({ keepAlive: true, maxSockets: 100 })
			},
			hooks: {
				// @ts-ignore
				beforeRetry: [(req, error, count) => {
					try {
						// @ts-ignore
						l(`got ${error.message}...retrying request...#${count}`);
					}
					catch (e) {
						//noop
					}
					config.retries++;
					config.requests++;
					if (error?.response?.statusCode?.toString() === "429") {
						config.rateLimited++;
					}
					else if (error?.response?.statusCode?.toString()?.startsWith("5")) {
						config.serverErrors++;
					}
					else {
						config.clientErrors++;
					}
				}],

			},
			body
		};
		// @ts-ignore
		if (config.project) options.searchParams.project_id = config.project;

		let req, res, success;
		try {
			// @ts-ignore
			req = await got(options);
			res = JSON.parse(req.body);
			success = true;
		}

		catch (e) {
			if (u.isJSONStr(e?.response?.body)) {
				res = JSON.parse(e.response.body);
			}
			else {
				res = e;
			}
			success = false;

		}

		if (config.recordType === 'event') {
			config.success += res.num_records_imported || 0;
			config.failed += res?.failed_records?.length || 0;
		}
		if (config.recordType === 'user' || config.recordType === 'group') {
			if (!res.error || res.status) config.success += batch.length;
			if (res.error || !res.status) config.failed += batch.length;
		}

		config.store(res, success);
		return res;
	}

	catch (e) {
		try {
			// @ts-ignore
			l(`\nBATCH FAILED: ${e.message}\n`);
		}
		catch (e) {
			//noop
		}
	}
}



async function determineData(data, config) {
	//exports are saved locally
	if (config.recordType === 'export') {
		if (config.where) {
			return path.resolve(config.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return filename;
	}

	if (config.recordType === 'peopleExport') {
		if (config.where) {
			return path.resolve(config.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		return path.resolve(folder);
	}

	// lookup tables are not streamed
	if (config.recordType === 'table') {
		if (fs.existsSync(path.resolve(data))) return await u.load(data);
		return data;
	}

	// data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		if (data.readableObjectMode) return data;
		return _(existingStream(data));
	}

	// data is an object in memory
	if (Array.isArray(data)) {
		return stream.Readable.from(data, { objectMode: true, highWaterMark: config.highWater });
	}

	//todo: support array of files
	try {

		// data refers to file/folder on disk
		if (fs.existsSync(path.resolve(data))) {
			const fileOrDir = fs.lstatSync(path.resolve(data));

			//file case			
			if (fileOrDir.isFile()) {
				//check for jsonl first... many jsonl files will have the same extension as json
				if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75 && !config.forceStream) {
						const file = await u.load(path.resolve(data));
						const parsed = file.trim().split('\n').map(JSON.parse);
						return stream.Readable.from(parsed, { objectMode: true, highWaterMark: config.highWater });
					}

					return itemStream(path.resolve(data), "jsonl", config.highWater);
				}

				if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75 && !config.forceStream) {
						const file = await u.load(path.resolve(data), true);
						return stream.Readable.from(file, { objectMode: true, highWaterMark: config.highWater });
					}

					//otherwise, stream it
					return itemStream(path.resolve(data), "json", config.highWater);
				}

				//csv case
				// todo: refactor this inside the itemStream function
				if (config.streamFormat === 'csv') {
					const fileStream = fs.createReadStream(path.resolve(data));
					const mappings = Object.entries(config.aliases);
					const csvParser = Papa.parse(Papa.NODE_STREAM_INPUT, {
						header: true,
						skipEmptyLines: true,
						transformHeader: (header) => {
							const mapping = mappings.filter(pair => pair[0] === header).pop();
							if (mapping) header = mapping[1];
							return header;
						}
					});
					const transformer = new stream.Transform({
						objectMode: true, highWaterMark: config.highWater, transform: (chunk, encoding, callback) => {
							const { distinct_id = "", $insert_id = " ", time, event, ...props } = chunk;
							const mixpanelEvent = {
								event,
								properties: {
									distinct_id,
									$insert_id,
									time: dayjs.utc(time).valueOf(),
									...props
								}
							};
							callback(null, mixpanelEvent);
						}
					});
					const outStream = fileStream.pipe(csvParser).pipe(transformer);
					return outStream;
				}
			}

			//folder case
			if (fileOrDir.isDirectory()) {
				const enumDir = await u.ls(path.resolve(data));
				const files = enumDir.filter(filePath => config.supportedFileExt.includes(path.extname(filePath)));
				if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "jsonl", config.highWater);
				}
				if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "json", config.highWater);
				}
			}
		}
	}

	catch (e) {
		//noop
	}

	// data is a string, and we have to guess what it is
	if (typeof data === 'string') {

		//stringified JSON
		try {
			return stream.Readable.from(JSON.parse(data), { objectMode: true, highWaterMark: config.highWater });
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			// @ts-ignore
			return stream.Readable.from(data.trim().split('\n').map(JSON.parse), { objectMode: true, highWaterMark: config.highWater });
		}

		catch (e) {
			//noop
		}

		//CSV or TSV
		try {
			return stream.Readable.from(Papa.parse(data, { header: true, skipEmptyLines: true }));
		}
		catch (e) {
			//noop
		}
	}

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string... (i could not determine it's type)`);
	process.exit(1);

}

async function flushLookupTable(stream, config) {
	const res = await flushToMixpanel(stream, config);
	config.recordsProcessed = stream.split('\n').length - 1;
	config.success = config.recordsProcessed;
	return res;
}

function existingStream(stream) {
	const rl = readline.createInterface({
		input: stream,
		crlfDelay: Infinity
	});

	const generator = (push, next) => {
		rl.on('line', line => {
			push(null, JSON.parse(line));
		});
		rl.on('close', () => {
			next();
			push(null, _.nil); //end of stream

		});
	};

	return generator;
}

function itemStream(filePath, type = "jsonl", highWater) {
	let stream;
	let parsedStream;
	const parser = type === "jsonl" ? jsonlParser : StreamArray.withParser;
	const streamOpts = {
		highWaterMark: highWater,
		autoClose: true,
		emitClose: true

	};
	//parsing folders
	if (Array.isArray(filePath)) {

		if (type === "jsonl") {
			stream = new MultiStream(filePath.map((file) => { return fs.createReadStream(file, streamOpts); }), streamOpts);
			parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: undefined, ...streamOpts })).map(token => token.value);
			return parsedStream;

		}
		if (type === "json") {
			stream = filePath.map((file) => fs.createReadStream(file));
			parsedStream = MultiStream.obj(stream.map(s => s.pipe(parser(streamOpts)).map(token => token.value)));
			return parsedStream;
		}
	}

	//parsing files
	else {
		stream = fs.createReadStream(filePath, streamOpts);
		parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: undefined, ...streamOpts })).map(token => token.value);
	}

	return parsedStream;

}

function chunkForSize(config) {
	return (err, batch, push, next) => {
		const maxBatchSize = config.bytesPerBatch;

		if (err) {
			// pass errors along the stream and consume next value
			push(err);
			next();
		}

		else if (batch === _.nil) {
			// pass nil (end event) along the stream
			push(null, batch);
		}

		else {
			// if batch is below max size, continue
			if (JSON.stringify(batch).length <= maxBatchSize) {
				push(null, batch);
			}

			// if batch is above max size, chop into smaller chunks
			else {
				let tempArr = [];
				let runningSize = 0;
				const sizedChunks = batch.reduce(function (accum, curr, index, source) {
					//catch leftovers at the end
					if (index === source.length - 1) {
						accum.push(tempArr);
					}
					//fill each batch 95%
					if (runningSize >= maxBatchSize * .95) {
						accum.push(tempArr);
						runningSize = 0;
						tempArr = [];
					}

					runningSize += JSON.stringify(curr).length;
					tempArr.push(curr);
					return accum;

				}, []);

				for (const chunk of sizedChunks) {
					push(null, chunk);
				}

			}
			next();
		}
	};
}

function getEnvVars() {
	const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`, `MP_TYPE`, `MP_TABLE_ID`, `MP_GROUP_KEY`, `MP_START`, `MP_END`);
	const envKeyNames = {
		MP_PROJECT: "project",
		MP_ACCT: "acct",
		MP_PASS: "pass",
		MP_SECRET: "secret",
		MP_TOKEN: "token",
		MP_TYPE: "recordType",
		MP_TABLE_ID: "lookupTableId",
		MP_GROUP_KEY: "groupKey",
		MP_START: "start",
		MP_END: "end"
	};
	const envCreds = u.rnKeys(envVars, envKeyNames);

	return envCreds;
}


/*
----
LOGGING
----
*/

function showProgress(record, processed, requests) {
	const { rss, heapTotal, heapUsed } = process.memoryUsage();
	const percentHeap = (heapUsed / heapTotal) * 100;
	const percentRSS = (heapUsed / rss) * 100;
	const line = `${record}s: ${u.comma(processed)} | batches: ${u.comma(requests)} | memory: ${u.bytesHuman(heapUsed)} (heap: ${u.round(percentHeap)}% total:${u.round(percentRSS)}%)\t\t`;
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(line);
}


function logger(config) {
	return (message) => {
		if (config.verbose) console.log(message);
	};
}

async function writeLogs(data) {
	const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
	const fileDir = u.mkdir('./logs');
	const fileName = `${data.recordType}-import-log-${dateTime}.json`;
	const filePath = `${fileDir}/${fileName}`;
	const file = await u.touch(filePath, data, true);
	// @ts-ignore
	l(`\nCOMPLETE\nlog written to:\n${file}`);
}




/*
-------
EXPORTS
-------
*/

const mpImport = module.exports = main;
mpImport.createMpStream = pipeInterface;

// * this allows the program to run as a CLI
if (require.main === module) {
	main(undefined, undefined, undefined, true, true).then(() => {
		process.exit(0);
	}).catch((e) => {
		console.log('\n\nUH OH! something went wrong; the error is:\n\n');
		console.error(e);
		process.exit(1);
	}).finally(() => {
		process.exit(0);
	});
}
