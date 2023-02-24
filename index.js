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

// $ types
// eslint-disable-next-line no-unused-vars
const types = require("./types.js");

// $ parsers
const readline = require('readline');
const Papa = require('papaparse');
const { parser: jsonlParser } = require('stream-json/jsonl/Parser');
const StreamArray = require('stream-json/streamers/StreamArray'); //json parser

// $ streamers
const _ = require('highland');
const stream = require('stream');
const got = require('got');
const https = require('https');

// $ file system
const path = require('path');
const fs = require('fs');
const os = require("os");


// $ env
require('dotenv').config();
const cliParams = require('./cli');

// $ utils
const transforms = require('./transforms.js');
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);
const { pick } = require('underscore');
const { gzip } = require('node-gzip');
const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;
const { promisify, callbackify } = require('util');



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
		this.bearer = creds.bearer || ``;
		this.token = creds.token || ``; //project token 
		this.lookupTableId = creds.lookupTableId || ``; //lookup table id
		this.groupKey = creds.groupKey || ``; //group key id
		this.auth = this.resolveProjInfo();


		//? dates
		if (opts.start) {
			this.start = dayjs(opts.start).format(dateFormat);
		}
		else {
			this.start = dayjs().subtract(30, 'd').format(dateFormat);

		}
		if (opts.end) {
			this.end = dayjs(opts.end).format(dateFormat);
		}

		else {
			this.end = dayjs().format(dateFormat);
		}

		// ? string options
		this.recordType = opts.recordType || `event`; // event, user, group or table		
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.region = opts.region || `US`; // US or EU

		// ? number options
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)		
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 2 * 1024 * 1024; // max bytes in each req
		this.maxRetries = opts.maxRetries || 10; // number of times to retry a batch

		// ? don't allow batches bigger than API limits
		if (this.type === 'event' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'user' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'group' && this.recordsPerBatch > 200) this.recordsPerBatch = 200;

		// ? boolean options
		this.compress = u.isNil(opts.compress) ? false : opts.compress; //gzip data (events only)
		this.strict = u.isNil(opts.strict) ? true : opts.strict; // use strict mode?
		this.logs = u.isNil(opts.logs) ? true : opts.logs; //create log file
		this.where = u.isNil(opts.logs) ? '' : opts.where; // where to put logs
		this.verbose = u.isNil(opts.verbose) ? true : opts.verbose;  // print to stdout?
		this.fixData = u.isNil(opts.fixData) ? false : opts.fixData; //apply transforms on the data
		this.abridged = u.isNil(opts.fixData) ? false : opts.abridged; //apply transforms on the data

		// ? transform options
		this.transformFunc = opts.transformFunc || function noop(a) { return a; }; //will be called on every record
		this.ezTransform = function noop(a) { return a; }; //placeholder for ez transforms
		if (this.fixData) this.ezTransform = transforms(this);

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
		this.workers = Number.isInteger(opts.workers) ? opts.workers : 10;
		this.highWater = (this.workers * this.recordsPerBatch) || 2000;

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

		// ? headers for exports
		if (this.recordType === "export") {
			this.reqMethod = 'GET';
		}
		this.file = "";
		this.folder = "";

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
			table: `https://api.mixpanel.com/lookup-tables/`,
			export: `https://data.mixpanel.com/api/2.0/export`,
			peopleexport: `https://mixpanel.com/api/2.0/engage`
		},
		eu: {
			event: `https://api-eu.mixpanel.com/import`,
			user: `https://api-eu.mixpanel.com/engage`,
			group: `https://api-eu.mixpanel.com/groups`,
			table: `https://api-eu.mixpanel.com/lookup-tables/`,
			export: `https://data-eu.mixpanel.com/api/2.0/export`,
			peopleexport: `https://eu.mixpanel.com/api/2.0/engage`
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
		const { recordType, compress, streamSize, workers, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc } = this;
		return { recordType, compress, streamSize, workers, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc };
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
		if (!this.abridged) {
			if (success) this.responses.push(response);
		}
		
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
			return `Basic ${Buffer.from(this.acct + ':' + this.pass, 'binary').toString('base64')}`;
		}

		//fallback method: secret auth
		else if (this.secret) {
			return `Basic ${Buffer.from(this.secret + ':', 'binary').toString('base64')}`;
		}

		else if (this.bearer) {
			return `Bearer ${this.bearer}`;
		}

		else if (this.type === 'user' || this.type === 'group') {
			return ``;
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
			requests: this.requests,
			batches: this.batches,
			recordType: this.recordType,
			duration: this.timer.report(false).delta,
			human: this.timer.report(false).human,
			retries: this.retries,
			version: this.version,
			workers: this.workers
		};

		summary.eps = Math.floor(this.recordsProcessed / this.timer.report(false).delta * 1000);
		summary.rps = u.round(summary.requests / this.timer.report(false).delta * 1000, 3);

		if (includeResponses) {
			summary.responses = this.responses;
			summary.errors = this.errors;
		}

		if (this.file) {
			summary.file = this.file;
		}

		if (this.folder) {
			summary.folder = this.folder;
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
async function main(creds = {}, data, opts = {}, isCLI = false) {
	track('start', { runId });
	let config = {};
	let cliData = {};

	// gathering params
	const envVar = getEnvVars();
	let cli = {};
	if (isCLI) {
		cli = cliParams();
		cliData = cli._[0];
	}
	config = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

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
			if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
		}
	}
	l('\n');

	// clean up
	config.timer.end(false);
	const summary = config.summary();
	l(`${config.type === 'export' ? 'export' : 'import'} complete in ${summary.human}`);
	if (config.logs) await writeLogs(summary, config.where);
	track('end', { runId, ...config.summary(false) });
	return summary;
}


/**
 * the core pipeline 
 * @param {types.ReadableStream} stream 
 * @param {importJob} config 
 * @returns {Promise<types.ImportResults>} a promise
 */
function corePipeline(stream, config, toNodeStream = false) {

	if (config.recordType === 'table') return flushLookupTable(stream, config);
	if (config.recordType === 'export') return exportEvents(stream, config);
	if (config.recordType === 'peopleExport') return exportProfiles(stream, config);

	const flush = _.wrapCallback(callbackify(flushToMixpanel));

	const mpPipeline = _.pipeline(
		// * transforms
		_.map((data) => {
			config.recordsProcessed++;
			if (config.fixData) {
				return config.ezTransform(config.transformFunc(data));
			}
			return config.transformFunc(data);
		}),

		// * batch for # of items
		_.batch(config.recordsPerBatch),

		// * batch for req size
		_.consume(chunkForSize(config)),

		// * send to mixpanel
		_.map((batch) => {
			config.requests++;
			return flush(batch, config);
		}),

		// * concurrency
		_.mergeWithLimit(config.workers),

		// * verbose
		_.doto(() => {
			if (config.verbose) showProgress(config.recordType, config.recordsProcessed, config.requests);
		}),

		// * errors
		_.errors((e) => {
			throw e;
		})
	);

	if (toNodeStream) {
		return mpPipeline;
	}

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
 * @param {types.Creds} creds - mixpanel project credentials
 * @param {types.Options} opts - import options
 * @param {function(): importJob} finish - end of pipelines
 * @returns a transform stream
 */
function pipeInterface(creds = {}, opts = {}, finish = () => { }) {
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	config.timer.start();

	const pipeToMe = corePipeline(null, config, true);

	// * handlers
	pipeToMe.on('end', () => {
		config.timer.end(false);
		track('end', { runId, ...config.summary(false) });
		finish(null, config.summary());
	});

	pipeToMe.on('pipe', () => {
		track('start', { runId });
		pipeToMe.resume();
	});

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

async function flushToMixpanel(batch, config) {
	try {
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (config.recordType === 'event' && config.compress) {
			body = await gzip(body);
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
				statusCodes: [429, 500, 501, 503],
				errorCodes: [],
				methods: ['POST'],
				noise: 2500

			},
			headers: {
				"Authorization": `${config.auth}`,
				"Content-Type": config.contentType,
				"Content-Encoding": config.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			agent: {
				https: new https.Agent({ keepAlive: true })
			},
			hooks: {
				beforeRetry: [(req, resp, count) => {
					try {
						l(`got ${resp.message}...retrying request...#${count}`);
					}
					catch (e) {
						//noop
					}
					config.retries++;
				}]
			},
			body
		};
		if (config.project) options.searchParams.project_id = config.project;

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
		try {
			l(`\nBATCH FAILED: ${e.message}\n`);
		}
		catch (e) {
			//noop
		}
	}
}

async function exportEvents(filename, config) {
	const pipeline = promisify(stream.pipeline);

	/** @type {got.Options} */
	const options = {
		url: config.url,
		searchParams: {
			from_date: config.start,
			to_date: config.end
		},
		method: config.reqMethod,
		retry: { limit: 50 },
		headers: {
			"Authorization": `${config.auth}`
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

	};

	if (config.project) options.searchParams.project_id = config.project;

	const request = got.stream(options);

	request.on('response', (res) => {
		config.requests++;
		config.responses.push({
			status: res.statusCode,
			ip: res.ip,
			url: res.requestUrl,
			...res.headers
		});
	});

	request.on('error', (e) => {
		config.failed++;
		config.responses.push({
			status: e.statusCode,
			ip: e.ip,
			url: e.requestUrl,
			...e.headers,
			message: e.message
		});
		throw e;

	});

	request.on('downloadProgress', (progress) => {
		downloadProgress(progress.transferred);
	});

	const exportedData = await pipeline(
		request,
		fs.createWriteStream(filename)
	);

	console.log('\n\ndownload finished\n\n');

	const lines = await countFileLines(filename);
	config.recordsProcessed += lines;
	config.success += lines;
	config.file = filename;

	return exportedData;
}

async function exportProfiles(folder, config) {
	const auth = config.auth;
	const allFiles = [];

	let iterations = 0;
	let fileName = `people-${iterations}.json`;
	let file = path.resolve(`${folder}/${fileName}`);

	/** @type {got.Options} */
	const options = {
		method: 'POST',
		url: config.url,
		headers: {
			Authorization: auth
		},
		searchParams: {},
		responseType: 'json'
	};
	if (config.project) options.searchParams.project_id = config.project;

	let request = await got(options).catch(e => {
		config.failed++;
		config.responses.push({
			status: e.statusCode,
			ip: e.ip,
			url: e.requestUrl,
			...e.headers,
			message: e.message
		});
		throw e;
	});
	let response = request.body;



	//grab values for recursion
	let { page, page_size, session_id } = response;
	let lastNumResults = response.results.length;

	// write first page of profiles
	let profiles = response.results;
	const firstFile = await u.touch(file, profiles, true);
	let nextFile;
	allFiles.push(firstFile);

	//update config
	config.recordsProcessed += profiles.length;
	config.success += profiles.length;
	config.requests++;
	config.responses.push({
		status: request.statusCode,
		ip: request.ip,
		url: request.requestUrl,
		...request.headers
	});

	showProgress("profile", config.success, iterations + 1, 'received');


	// recursively consume all profiles
	// https://developer.mixpanel.com/reference/engage-query
	while (lastNumResults >= page_size) {
		page++;
		iterations++;

		fileName = `people-${iterations}.json`;
		file = path.resolve(`${folder}/${fileName}`);
		options.searchParams.page = page;
		options.searchParams.session_id = session_id;

		request = await got(options).catch(e => {
			config.failed++;
			config.responses.push({
				status: e.statusCode,
				ip: e.ip,
				url: e.requestUrl,
				...e.headers,
				message: e.message
			});
		});
		response = request.body;

		//update config
		config.requests++;
		config.responses.push({
			status: request.statusCode,
			ip: request.ip,
			url: request.requestUrl,
			...request.headers
		});
		config.success += profiles.length;
		config.recordsProcessed += profiles.length;
		showProgress("profile", config.success, iterations + 1);

		profiles = response.results;

		nextFile = await u.touch(file, profiles, true);
		allFiles.push(nextFile);

		// update recursion
		lastNumResults = response.results.length;

	}

	console.log('\n\ndownload finished\n\n');

	config.file = allFiles;
	config.folder = folder;

	return folder;

}

async function determineData(data, config) {
	//exports are saved locally
	if (config.recordType === 'export') {
		const folder = u.mkdir(config.where || './mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return [filename];
	}

	if (config.recordType === 'peopleExport') {
		const folder = u.mkdir(config.where || './mixpanel-exports');
		return [path.resolve(folder)];
	}

	// lookup tables are not streamed
	if (config.recordType === 'table') {
		if (fs.existsSync(path.resolve(data))) return [await u.load(data)];
		return [data];
	}

	// data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		if (data.readableObjectMode) return [data];
		return [_(existingStream(data))];
	}

	// data is an object in memory
	if (Array.isArray(data)) {
		return [stream.Readable.from(data, { objectMode: true, highWaterMark: config.highWater })];
	}

	try {

		// data refers to file/folder on disk
		if (fs.existsSync(path.resolve(data))) {
			const fileOrDir = fs.lstatSync(path.resolve(data));


			//file case			
			if (fileOrDir.isFile()) {
				if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75) {
						const file = await u.load(path.resolve(data), true);
						return [stream.Readable.from(file, { objectMode: true, highWaterMark: config.highWater })];
					}

					//otherwise, stream it
					return itemStream(path.resolve(data), "json", config.highWater);
				}
				if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75) {
						const file = await u.load(path.resolve(data));
						const parsed = file.trim().split('\n').map(JSON.parse);
						return [stream.Readable.from(parsed, { objectMode: true, highWaterMark: config.highWater })];
					}

					return itemStream(path.resolve(data), "jsonl", config.highWater);
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
			return [stream.Readable.from(JSON.parse(data), { objectMode: true, highWaterMark: config.highWater })];
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			return [stream.Readable.from(data.trim().split('\n').map(JSON.parse), { objectMode: true, highWaterMark: config.highWater })];
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

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string... (i could not determine it's type)`);
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

function itemStream(filePath, type = "jsonl", workers) {
	let stream;
	if (Array.isArray(filePath)) {
		stream = filePath.map((file) => fs.createReadStream(file));
	}
	else {
		stream = [fs.createReadStream(filePath)];
	}

	//use the right parser based on the type of file
	const parser = type === "jsonl" ? jsonlParser : StreamArray.withParser;
	return stream.map(s => s.pipe(parser({ highWaterMark: workers * 2000 })).map(token => token.value));
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

function showProgress(record, processed, requests, sentOrReceived = 'sent') {
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(`\t${record}s processed: ${u.comma(processed)} | batches ${sentOrReceived}: ${u.comma(requests)}\t`);
}

function downloadProgress(amount) {
	if (amount < 1000000) {
		//noop
	}
	else {
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`\tdownloaded: ${u.bytesHuman(amount, 2, true)}    \t`);
	}
}

function logger(config) {
	return (message) => {
		if (config.verbose) console.log(message);
	};
}

async function writeLogs(data, where = '') {
	const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
	const fileDir = u.mkdir(where || './logs');
	const fileName = `${data.recordType}-import-log-${dateTime}.json`;
	const filePath = `${fileDir}/${fileName}`;
	const file = await u.touch(filePath, data, true);
	l(`\nCOMPLETE\nlog written to:\n${file}`);
}

async function countFileLines(filePath) {
	return new Promise((resolve, reject) => {
		let lineCount = 0;
		fs.createReadStream(filePath)
			.on("data", (buffer) => {
				let idx = -1;
				lineCount--; // Because the loop will run once for idx=-1
				do {
					idx = buffer.indexOf(10, idx + 1);
					lineCount++;
				} while (idx !== -1);
			}).on("end", () => {
				resolve(lineCount);
			}).on("error", reject);
	});
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
