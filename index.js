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

// * types
// eslint-disable-next-line no-unused-vars
const types = require("./types.js");

// * parsers
const readline = require('readline');
const Papa = require('papaparse');
const { parser: jsonlParser } = require('stream-json/jsonl/Parser');
const StreamArray = require('stream-json/streamers/StreamArray'); //json parser

//* streamers
const _ = require('highland');
const stream = require('stream');
const got = require('got');
const https = require('https');

// * file system
const path = require('path');
const fs = require('fs');

// * env
require('dotenv').config();
const cliParams = require('./cli');

// * utils
const transforms = require('./transforms.js');
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);
const { pick } = require('underscore');
const { gzip } = require('node-gzip');


/*
-------
CONFIG
-------
*/

/**
 * a singleton to hold state about the imported data
 * @example
 * const config = new importJob(creds, opts)
 * @class 
 * @param {types.Creds} creds - mixpanel project credentials
 * @param {types.Options} opts - options for import
 * @method summary summarize state of import
*/
class importJob {
	constructor(creds, opts) {
		// ? credentials
		this.acct = creds.acct || ``; //service acct username
		this.pass = creds.pass || ``; //service acct secret
		this.project = creds.project || ``; //project id
		this.secret = creds.secret || ``; //api secret (deprecated auth)
		this.token = creds.token || ``; //project token 
		this.lookupTableId = creds.lookupTableId || ``; //lookup table id
		this.groupKey = creds.groupKey || ``; //group key id
		this.auth = this.resolveProjInfo();

		// ? string options
		this.recordType = opts.recordType || `event`; // event, user, group or table		
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.region = opts.region || `US`; // US or EU

		// ? number options
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)		
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 2 * 1024 * 1024; // max bytes in each req

		// ? don't allow batches bigger than API limits
		if (this.type === 'event' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'user' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'group' && this.recordsPerBatch > 200) this.recordsPerBatch = 200;

		// ? transform options
		this.transformFunc = opts.transformFunc || function noop(a) { return a; }; //will be called on every record

		// ? boolean options
		this.compress = u.is(undefined, opts.compress) ? false : opts.compress; //gzip data (events only)
		this.strict = u.is(undefined, opts.strict) ? true : opts.strict; // use strict mode?
		this.logs = u.is(undefined, opts.logs) ? true : opts.logs; // print to stdout?
		this.verbose = u.is(undefined, opts.verbose) ? true : opts.verbose;
		this.fixData = u.is(undefined, opts.fixData) ? false : opts.fixData; //apply transforms on the data

		// ? counters
		this.recordsProcessed = 0;
		this.success = 0;
		this.failed = 0;
		this.retries = 0;
		this.batches = 0;
		this.requests = 0;
		this.timer = u.time('etl');

		// ? requests
		this.reqMethod = "POST";
		this.contentType = "application/json";
		this.encoding = "";
		this.responses = [];
		this.errors = [];

		// ? transforms
		if (this.fixData) transforms(this);

		// ? allow plurals
		if (this.recordType === 'events') this.recordType === 'event';
		if (this.recordType === 'users') this.recordType === 'user';
		if (this.recordType === 'groups') this.recordType === 'group';
		if (this.recordType === 'tables') this.recordType === 'table';

		// ? headers for lookup tables
		if (this.recordType === "table") {
			this.reqMethod = 'PUT';
			this.contentType = 'text/csv';
			this.fixData = false;
		}

	}

	// ? props
	version = this.getVersion();
	supportedTypes = ['event', 'user', 'group', 'table'];
	lineByLineFileExt = ['.txt', '.jsonl', '.ndjson'];
	objectModeFileExt = ['.json'];
	supportedFileExt = [...this.lineByLineFileExt, ...this.objectModeFileExt, '.csv'];
	endpoints = {
		us: {
			event: `https://api.mixpanel.com/import`,
			user: `https://api.mixpanel.com/engage`,
			group: `https://api.mixpanel.com/groups`,
			table: `https://api.mixpanel.com/lookup-tables/`
		},
		eu: {
			event: `https://api-eu.mixpanel.com/import`,
			user: `https://api-eu.mixpanel.com/engage`,
			group: `https://api-eu.mixpanel.com/groups`,
			table: `https://api-eu.mixpanel.com/lookup-tables/`
		}

	};

	// ? get/set	
	get type() {
		return this.recordType;
	}
	get url() {
		let url = this.endpoints[this.region.toLowerCase()][this.recordType.toLowerCase()];
		if (this.recordType === "table") url += this.lookupTableId;
		return url;
	}
	get opts() {
		const { recordType, compress, streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc } = this;
		return { recordType, compress, streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc };
	}
	get creds() {
		const { acct, pass, project, secret, token, lookupTableId, groupKey, auth } = this;
		return { acct, pass, project, secret, token, lookupTableId, groupKey, auth };
	}
	set batchSize(chunkSize) {
		this.recordsPerBatch = chunkSize;
	}
	set transform(fn) {
		this.transformFunc = fn;
	}

	// ? methods
	report() {
		return Object.assign(this);
	}
	store(response, success = true) {
		if (success) this.responses.push(response);
		if (!success) this.errors.push(response);
	}
	getVersion() {
		const { version } = require('./package.json');
		if (version) return version;
		if (process.env.npm_package_version) return process.env.npm_package_version;
		return 'unknown';
	}
	resolveProjInfo() {
		//preferred method: service acct
		if (this.acct && this.pass && this.project) {
			return Buffer.from(this.acct + ':' + this.pass, 'binary').toString('base64');
		}

		//fallback method: secret auth
		else if (this.secret) {
			return Buffer.from(this.secret + ':', 'binary').toString('base64');
		}

		else {
			console.error('no secret or service account provided! quitting...');
			process.exit(0);
		}

	}
	/**
	 * summary of the results of an import
	 * @param {boolean} includeResponses - should `errors` and `responses` be included in summary
	 * @returns {types.ImportResults} `{success, failed, total, requests, duration}`
	 */
	summary(includeResponses = true) {
		const summary = {
			success: this.success,
			failed: this.failed,
			total: this.recordsProcessed,
			requests: this.responses.length,
			recordType: this.recordType,
			duration: this.timer.report(false).delta,
			human: this.timer.report(false).human,
			retries: this.retries,
			version: this.version
		};

		if (includeResponses) {
			summary.responses = this.responses;
			summary.errors = this.errors;
		}

		return summary;
	}
}

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
 * @param {types.Creds} creds - mixpanel project credentials
 * @param {types.Data} data - data to import
 * @param {types.Options} opts - import options
 * @param {boolean} isCLI - `true` when run as CLI
 * @param {importJob} existingConfig - used to recycle a config for `.pipe()` streams
 * @returns {Promise<types.ImportResults>} API receipts of imported data
 */
async function main(creds = {}, data, opts = {}, isCLI = false, existingConfig) {
	track('start', { runId });
	let config = {};
	let cliData = {};
	if (existingConfig) {
		config = existingConfig;
	}
	else {
		// gathering params
		const envVar = getEnvVars();
		let cli = {};
		if (isCLI) {
			cli = cliParams();
			cliData = cli._[0];
		}
		config = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });
	}

	if (isCLI) config.verbose = true;
	const l = logger(config);
	l(cliParams.welcome);
	global.l = l;

	// ETL
	config.timer.start();
	const streams = await determineData(data || cliData, config); // always stream[]
	for (const stream of streams) {
		try {
			await corePipeline(stream, config);
		}

		catch (e) {
			l(`ERROR: ${e.message}`);
		}
	}
	l('\n');

	// clean up
	config.timer.end(false);
	const summary = config.summary();
	l(`import complete in ${summary.human}`);
	if (config.logs) await writeLogs(summary);
	track('end', { runId, ...config.summary(false) });
	return summary;
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
 * @param {types.Creds} creds - mixpanel project credentials
 * @param {types.Options} opts - import options
 * @param {function(): importJob} finish - end of pipelines
 * @returns a transform stream
 */
function pipeInterface(creds = {}, opts = {}, finish = () => { }) {
	// ! figure out how to use corePipeline() instead of main()
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	const { recordsPerBatch = 2000 } = opts;
	config.logs = false;
	config.verbose = false;

	const extStream = new stream.Transform({ objectMode: true, highWaterMark: recordsPerBatch });
	extStream.batch = [];

	extStream.on('finish', () => {
		finish(null, config.summary());
	});

	extStream.on('error', (e) => {
		throw e;
	});

	extStream._transform = (data, encoding, callback) => {
		extStream.batch.push(data);
		if (extStream.batch.length === recordsPerBatch) {
			main(null, extStream.batch, null, false, config).then((results) => {
				extStream.batch = [];
				const result = [...results.responses.slice(-1), ...results.errors.slice(-1)][0];
				callback(null, result);
			});
		}

		else {
			callback();
		}
	};

	extStream._flush = function (callback) {
		if (extStream.batch.length) {
			//data is still left in the stream; flush it!
			main(null, extStream.batch, null, false, config).then((results) => {
				extStream.batch = [];
				const result = [...results.responses.slice(-1), ...results.errors.slice(-1)][0];
				callback(null, result);
			});

			extStream.batch = [];
		}

		else {
			callback(null);
		}
	};

	return extStream;

}

/**
 * the core pipeline 
 * @param {types.ReadableStream} stream 
 * @param {importJob} config 
 * @returns {Promise<types.ImportResults>} a promise
 */
async function corePipeline(stream, config) {

	if (config.recordType === 'table') return await flushLookupTable(stream, config);

	const pipeline = _(stream)
		// * transform source data
		.map((data) => {
			config.recordsProcessed++;
			return config.transformFunc(data);
		})

		// * batch for # of items
		.batch(config.recordsPerBatch)


		// * batch for req size
		.consume(chunkForSize(config))

		// * send to mixpanel
		.map(async (batch) => {
			config.requests++;
			const res = await flushToMixpanel(batch, config);
			return res;
		})

		// * verbose
		.doto(() => {
			if (config.verbose) showProgress(config.recordType, config.recordsProcessed, config.requests);
		})

		// * errors
		.errors((e) => {
			throw e;
		});


	return Promise.all(await pipeline.collect().toPromise(Promise));

}


/*
----
HELPERS
----
*/

async function flushToMixpanel(batch, config) {
	try {
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (config.recordType === 'event' && config.compress) {
			body = await gzip(body);
			config.encoding = 'gzip';
		}
		const options = {
			url: config.url,
			searchParams: {
				ip: 0,
				project_id: config.project,
				verbose: 1,
				strict: Number(config.strict)
			},
			method: config.reqMethod,
			retry: { limit: 50 },
			headers: {
				"Authorization": `Basic ${config.auth}`,
				"Content-Type": config.contentType,
				"Content-Encoding": config.encoding

			},
			agent: {
				https: new https.Agent({ keepAlive: true })
			},
			hooks: {
				beforeRetry: [(err, count) => {
					l(`retrying request...#${count}`);
					config.retries++;
				}]
			},
			body
		};

		let req, res, success;
		try {
			req = await got(options);
			res = JSON.parse(req.body);
			success = true;
		}

		catch (e) {
			res = JSON.parse(e.response.body);
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
		l(`\nBATCH FAILED: ${e.message}\n`);
	}
}

async function determineData(data, config) {
	// lookup tables are not streamed
	if (config.recordType === 'table') {
		if (fs.existsSync(path.resolve(data))) return [await u.load(data)];
		return [data];
	}

	// data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		return [existingStream(data)];
	}

	// data is an object in memory
	if (Array.isArray(data)) {
		return [stream.Readable.from(data, { objectMode: true })];
	}

	// data refers to file/folder on disk
	if (fs.existsSync(path.resolve(data))) {
		const fileOrDir = fs.lstatSync(path.resolve(data));

		//file case
		if (fileOrDir.isFile()) {
			if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(data))) {
				return itemStream(path.resolve(data), "json");
			}
			if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(data))) {
				return itemStream(path.resolve(data), "jsonl");
			}
		}

		//folder case
		if (fileOrDir.isDirectory()) {
			const enumDir = await u.ls(path.resolve(data));
			const files = enumDir.filter(filePath => config.supportedFileExt.includes(path.extname(filePath)));
			if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(files[0]))) {
				return itemStream(files, "jsonl");
			}
			if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(files[0]))) {
				return itemStream(files, "json");
			}
		}
	}

	// data is a string, and we have to guess what it is
	if (typeof data === 'string') {

		//stringified JSON
		try {
			return [stream.Readable.from(JSON.parse(data), { objectMode: true })];
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			return [stream.Readable.from(data.split('\n').map(JSON.parse), { objectMode: true })];
		}

		catch (e) {
			//noop
		}

		//CSV or TSV
		try {
			return [stream.Readable.from(Papa.parse(data, { header: true, skipEmptyLines: true }))];
		}
		catch (e) {
			//noop
		}
	}

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string...`);
	process.exit();

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
		crlfDelay: Infinity,
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

function itemStream(filePath, type = "jsonl") {
	let stream;
	if (Array.isArray(filePath)) {
		stream = filePath.map((file) => fs.createReadStream(file));
	}
	else {
		stream = [fs.createReadStream(filePath)];
	}

	//use the right parser based on the type of file
	const parser = type === "jsonl" ? jsonlParser : StreamArray.withParser;
	return stream.map(s => s.pipe(parser()).map(token => token.value));
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
				config.batches++;
				push(null, batch);
			}

			// if batch is above max size, chop into smaller chunks
			else {
				let tempArr = [];
				let runningSize = 0;
				const sizedChunks = batch.reduce(function (accum, curr, index, source) {
					//catch leftovers at the end
					if (index === source.length - 1) {
						config.batches++;
						accum.push(tempArr);
					}
					//fill each batch 95%
					if (runningSize >= maxBatchSize * .95) {
						config.batches++;
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
	const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`, `MP_TYPE`, `MP_TABLE_ID`, `MP_GROUP_KEY`);
	const envKeyNames = {
		MP_PROJECT: "project",
		MP_ACCT: "acct",
		MP_PASS: "pass",
		MP_SECRET: "secret",
		MP_TOKEN: "token",
		MP_TYPE: "recordType",
		MP_TABLE_ID: "lookupTableId",
		MP_GROUP_KEY: "groupKey"
	};
	const envCreds = u.rnKeys(envVars, envKeyNames);

	return envCreds;
}


/*
----
LOGGING
----
*/

function showProgress(record, processed, requests,) {
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(`\t${record}s processed: ${u.comma(processed)} | batches sent: ${u.comma(requests)}`);
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
	main(undefined, undefined, undefined, true).then(() => { });
}

//for some reason, vscode throws this when --inspect is on...
process.on('uncaughtException', (err) => {
	l(`\nFAILURE!\n\n${err.stack}\n\n${err.message}`);
});


process.on('unhandledRejection', (err) => {
	l(`\nREJECTION!\n\n${err.stack}\n\n${err.message}`);
});