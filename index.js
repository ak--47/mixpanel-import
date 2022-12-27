#! /usr/bin/env node
/* eslint-disable no-unused-vars */



// mixpanel-import
// by AK
// purpose: stream events, users, groups, tables into mixpanel... with best practices!

/*
-----
TYPES
-----
*/

/**
 * @typedef {Object} Creds - mixpanel project credentilas
 * @property {string} acct - service account username
 * @property {string} pass - service account password
 * @property {(string | number)} project - project id
 * @property {string} [token] - project token (for importing user profiles)
 * @property {string} [lookupTableId] - lookup table ID (for importing lookup tables)
 * @property {string} [groupKey] - group identifier (for importing group profiles)
 */

/**
 * @typedef {Object} Options
 * @property {string} [recordType=event] - type of record to import (event, user, group, or table)
 * @property {boolean} [compress=false] - use gzip compression (events only)
 * @property {number} [streamSize=27] - 2^N; highWaterMark value for stream
 * @property {string} [region=US] - US or EU (data residency)
 * @property {number} [recordsPerBatch=2000] - max # of records in each payload (max 2000; max 200 for group profiles) 
 * @property {number} [bytesPerBatch=2*1024*1024] - max # of bytes in each payload (max 2MB)
 * @property {boolean} [strict=true] - validate data on send (events only)
 * @property {boolean} [logs=true] - log data to console
 * @property {boolean} [fixData=false] - apply transformations to ensure data is properly ingested
 * @property {string} [streamFormat] - format of underlying data stream; json or jsonl
 * @property {function} [transformFunc=()=>{}] - a function to apply to every record before sending
 */

/**
 * @typedef {string | Array<{event: string, properties: {Object}}>} Data
 */


/*
---------
CORE DEPS
---------
*/

const fetch = require('axios');
const axiosRetry = require('axios-retry');
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);
const path = require('path');
const readline = require('readline');
const md5 = require('md5');
const Papa = require('papaparse');
const fs = require('fs');


const { Agent } = require('https');
const { pick } = require('underscore');
const { createReadStream, existsSync, readdirSync, lstatSync } = require('fs');
const { gzip } = require('node-gzip');

const _ = require('highland');
const got = require('got');
const yargs = require('yargs');

/*
-------------
PIPELINE DEPS
-------------
*/

const { chain } = require('stream-chain');
const StreamArray = require('stream-json/streamers/StreamArray');
const JsonlParser = require('stream-json/jsonl/Parser');
const Batch = require('stream-json/utils/Batch');
const stream = require('stream');
require('dotenv').config();

/*
----
PARAM GATHERING
----
*/

class importJob {
	constructor(creds, opts) {
		// * credentials
		this.acct = creds.acct || ``; //service acct username
		this.pass = creds.pass || ``; //service acct secret
		this.project = creds.project || ``; //project id
		this.secret = creds.secret || ``; //api secret (deprecated auth)
		this.token = creds.token || ``; //project token 
		this.lookupTableId = creds.lookupTableId || ``; //lookup table id
		this.groupKey = creds.groupKey || ``; //group key id    

		// * options
		this.recordType = opts.recordType || `event`; // event, user, group or table
		this.compress = opts.compress || false; //gzip data (events only)
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)
		this.region = opts.region || `US`; // US or EU
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 2 * 1024 * 1024; // max bytes in each req
		this.strict = opts.strict || true; // use strict mode?
		this.logs = opts.logs || true; // print to stdout?
		this.fixData = opts.fixData || false; //apply transforms on the data
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.transformFunc = opts.transformFunc || function noop(a) { return a; }; //will be called on every record

		// * counters
		this.recordsProcessed = 0;
		this.success = 0;
		this.failed = 0;
		this.retries = 0;
		this.time = 0;

	}

	static supportedTypes() {
		return ['event', 'user', 'group', 'table'];
	}

	static supportedFileExt() {
		return ['.json', '.txt', '.jsonl', '.ndjson', '.csv'];
	}

	static urls() {
		return {
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
	}

	get auth() {
		return {};
	}


	get dataType() {
		return this.recordType;
	}

	get url() {
		return this.urls[this.region.toLowerCase()[this.recordType.toLowerCase()]];
	}

	get fileStreamOpts() {
		return { writableObjectMode: false, readableObjectMode: true };
	}
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
	const envCreds = renameKeys(envVars, envKeyNames);
}

function getCLIParams() {
	const argv = yargs(process.argv.splice(2))
		.usage(`Usage: $0 --project [pid] --acct [serviceAcct] --pass [servicePass] --secret [secret] --token [token] --type [recordType] --table [lookuptableId] --group [groupKey]`);
}


/*
-----------
RETRIES
-----------
*/

const exponentialBackoff = generateDelays();
axiosRetry(fetch, {
	retries: 6, // number of retries
	retryDelay: (retryCount) => {
		if (retryCount > 5) throw Error('failed afer 5 retries');
		log(`	retrying request... attempt: ${retryCount}`);
		return exponentialBackoff[retryCount] + u.rand(1000, 5000); // interval between retries
	},
	retryCondition: (error) => {
		error.response.status === 429;
	},
	onRetry: function (retryCount, error, requestConfig) {
		retries++;
		if (error.response.status === 429) {
			return requestConfig;
		}
		else {
			track('error', { runId, ...requestConfig.data });
		}
	}
}
);

/*
---------
ENDPOINTS
---------
*/
const ENDPOINTS = {
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

/*
--------
GLOBALS
--------
*/


const supportedTypes = ['event', 'user', 'group', 'table'];
const supportedFileTypes = ['.json', '.txt', '.jsonl', '.ndjson', '.csv'];
let fileStreamOpts = { writableObjectMode: false, readableObjectMode: true };

let logging = false;
let strict = true;
let compress = false;

let url = ``;
let recordType = ``;

let totalRecordCount = 0;
let retries = 0;


/*
----
MAIN
----
*/


/**
 * import data to mixpanel
 * @async
 * @param {Creds} creds 
 * @param {Data} data - data to import
 * @param {Options} [opts]
 * @param {boolean} [isStream] - used when data is a stream; can also be inferred or supplied in options
 */
async function main(creds = {}, data = [], opts = {}, isStream = false) {

	const defaultOpts = {
		recordType: `event`, // event, user, group or table
		compress: false, //gzip data (events only)
		streamSize: 27, // power of 2 for highWaterMark in stream  (default 134 MB)
		region: `US`, // US or EU
		recordsPerBatch: 2000, // records in each req; max 2000 (200 for groups)
		bytesPerBatch: 2 * 1024 * 1024, // max bytes in each req
		strict: true, // use strict mode?
		logs: false, // print to stdout?
		fixData: false, //apply transforms on the data
		streamFormat: '', // json or jsonl ... only relevant for streams
		transformFunc: function noop(a) { return a; } //will be called on every record
	};
	let options;
	if (typeof opts === 'string' && supportedTypes.includes(opts.toLowerCase())) {
		options = { ...defaultOpts, recordType: opts };
	}
	else {
		options = { ...defaultOpts, ...opts };
	}
	if (opts.log) options.logs = true;
	if (opts.logging) options.logs = true;
	if (opts.verbose) options.logs = true;

	options.recordType = options.recordType.toLowerCase();
	options.streamFormat = options.streamFormat.toLowerCase();
	options.region = options.region.toLowerCase();
	if (recordType === 'event' && options.compress) compress = true;


	const defaultCreds = {
		acct: ``, //service acct username
		pass: ``, //service acct secret
		project: ``, //project id
		secret: ``, //api secret (deprecated auth)
		token: ``, //project token 
		lookupTableId: ``, //lookup table id
		groupKey: `` //group key id       
	};

	//sweep .env to pickup MP_ keys
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
	const envCreds = renameKeys(envVars, envKeyNames);
	const project = resolveProjInfo({ ...defaultCreds, ...envCreds, ...creds });
	if (envCreds.recordType) options.recordType = envCreds.recordType;
	if (envCreds.lookupTableId) project.lookupTableId = envCreds.lookupTableId;
	Object.freeze(project);
	if (options.fixData) ezTransforms(options, project);
	Object.freeze(options);
	const { streamSize, region, recordsPerBatch, bytesPerBatch, transformFunc } = options;
	let { streamFormat } = options;
	recordType = options.recordType;
	logging = options.logs;
	fileStreamOpts = { highWaterMark: 2 ** streamSize, ...fileStreamOpts };
	strict = options.strict;
	url = ENDPOINTS[region][recordType];

	if (recordType === `table`) {
		if (project.lookupTableId) {
			url += project.lookupTableId;
		}
		else {
			throw Error('saw type table, but no lookup table id was supplied');
		}

	}

	//if script is run standalone, use CLI params as source data
	const lastArgument = [...process.argv].pop();
	if (data?.length === 0 && supportedFileTypes.some(type => lastArgument.includes(type))) {
		data = lastArgument;
		logging = true;
	}
	else if (lastArgument?.toLowerCase()?.includes('help')) {
		console.log(banner);
		console.log(helpText);
		process.exit(0);
	}
	const startTime = Date.now();
	time('pipeline', 'start');
	track('start', { runId, ...options });

	//CORE PIPELINES
	const dataType = determineData(data, isStream);
	let pipeline;
	let files;
	let fileStream;
	switch (dataType) {
		case `file`:
			//todo lookup table
			if (recordType === 'table') {
				pipeline = await prepareLookupTable(data, project, `file`);
			}
			else {
				fileStream = createReadStream(path.resolve(data));
				if (!streamFormat) {
					streamFormat = inferStreamFormat(fileStream);
				}
				log(`streaming ${streamFormat} stream of ${recordType}s from file ${data}...`);
				pipeline = await streamingPipeline(fileStream, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, streamFormat);
			}

			log('\n');
			break;

		case `directory`:
			pipeline = [];
			files = readdirSync(data).map(fileName => {
				return {
					name: fileName,
					path: path.resolve(`${data}/${fileName}`)
				};
			});
			log(`found ${addComma(files.length)} files in ${data}`);

			walkDirectory: for (const file of files) {
				log(`streaming ${recordType}s from ${file.name}`);
				try {
					const localStream = createReadStream(path.resolve(file.path));
					const localFormat = inferStreamFormat(localStream);
					log(`streaming ${localFormat} stream of ${recordType}s from file ${file.path}...`);
					const result = await streamingPipeline(localStream, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, localFormat);
					// let result = await filePipeLine(file.path, project, recordsPerBatch, bytesPerBatch, transformFunc);
					pipeline.push({
						[file.name]: result
					});
					log('\n');
				} catch (e) {
					log(`  ${file.name} is not valid ${recordType} JSON or NDJSON; skipping`);
					continue walkDirectory;
				}
			}
			break;
		case `structString`:
			log(`parsing ${recordType}s...`);
			if (recordType === 'table') {
				pipeline = await prepareLookupTable(data, project, `memory`);
			}
			else {
				data = JSON.parse(data);
				pipeline = await dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType);
			}
			break;

		case `inMem`:
			log(`parsing ${recordType}s...`);
			//todo lookup table
			if (recordType === 'table') {
				pipeline = await prepareLookupTable(data, project, `memory`);
			}
			else {
				pipeline = await dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType);
			}
			break;

		case `stream`:
			if (!streamFormat) {
				streamFormat = inferStreamFormat(data);
			}
			log(`consuming ${streamFormat} stream of ${recordType}s from ${data?.path}...`);
			pipeline = await streamingPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, streamFormat);
			break;

		default:
			log(`could not determine data source`);
			throw Error(`mixpanel-import was not able to import: ${data}`);
	}

	time('pipeline', 'stop');
	track('end', { runId, ...options });

	// summary of pipeline results
	const endTime = Date.now();
	const duration = (endTime - startTime) / 1000;
	let total = totalRecordCount;
	let success, failed;
	if (recordType === `event`) {
		if (dataType === `directory`) {
			const flatRes = [];
			for (const [index, fileRes] of pipeline.entries()) {
				flatRes.push(pipeline[index][Object.keys(fileRes)[0]]);
			}
			success = flatRes.flat().map(res => res.num_records_imported).reduce((prev, curr) => prev + curr, 0);
			failed = total - success;
		}

		else {
			success = pipeline.map(res => res.num_records_imported).reduce((prev, curr) => prev + curr, 0);
			failed = total - success;
		}


	}

	if (recordType === `user` || recordType === `group`) {
		if (dataType === `directory`) {
			const flatRes = [];
			for (const [index, fileRes] of pipeline.entries()) {
				flatRes.push(pipeline[index][Object.keys(fileRes)[0]]);
			}
			success = flatRes.flat().filter(res => res.error === null).length * recordsPerBatch;
			failed = flatRes.flat().filter(res => res.error !== null).length * recordsPerBatch;
		}
		else {
			success = pipeline.filter(res => res.error === null).length * recordsPerBatch;
			failed = pipeline.filter(res => res.error !== null).length * recordsPerBatch;
		}
	}

	if (recordType === `table`) {
		success = 0;
		failed = 0;
		if (pipeline.code === 200) {
			success++;
		}
		else {
			failed++;
		}

		try {
			total = data.split('\n').length - 1;
		}
		catch (e) {
			//noop
		}
	}

	const summary = {
		results: {
			success,
			failed,
			total,
			batches: pipeline.length,
			recordType,
			duration,
			retries

		},
		responses: pipeline
	};
	return summary;

}

/*
----
STREAMERS
----
*/
function lineByLineStream(filePath) {
	const rl = readline.createInterface({
		input: fs.createReadStream(filePath),
		crlfDelay: Infinity,
	});

	const generator = (push, next) => {
		rl.on('line', line => {
			push(null, line);
		});
		rl.on('close', () => {
			push(null, _.nil);
		});
	};

	return generator;
}




/*
---------
PIPELINES
---------
https://github.com/uhop/stream-chain/wiki

*/



async function corePipeline(stream, infos, fileType) {
	_(generator)
		.map(JSON.parse)
		.batch(2000)
		.toArray(function (xs) {
			debugger;
		});

}


/**
 * @param  {} data
 * @param  {} project
 * @param  {} recordsPerBatch
 * @param  {} bytesPerBatch
 * @param  {} transformFunc
 * @param  {} recordType
 */
async function dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType) {
	time('chunk', 'start');
	let dataIn = data.map(transformFunc);
	const batches = chunkEv(dataIn, recordsPerBatch);
	time('chunk', 'stop');
	log(`\nloaded ${addComma(dataIn.length)} ${recordType}s`);

	//flush to mixpanel
	time('flush');
	let responses = [];
	let iter = 0;
	for (let batch of batches) {
		iter += 1;

		showProgress(recordType, recordsPerBatch * iter, dataIn.length, iter, batches.length);
		if (recordType === `event` && compress) batch = await gzip(JSON.stringify(batch));
		let res = await sendDataToMixpanel(project, batch);
		responses.push(res);
	}
	totalRecordCount = dataIn.length;
	log('\n');
	time('flush', 'stop');
	log('\n');

	return responses;

}

// https://medium.com/florence-development/working-with-node-js-stream-api-60c12437a1be
function pipeToMixpanel(creds = {}, opts = {}, finish = () => { }) {
	const { recordsPerBatch = 2000 } = opts;
	const logs = [];
	const piped = new stream.Transform({ objectMode: true, highWaterMark: recordsPerBatch });

	piped.on('finish', () => {
		const consumerLogs = aggregateLogs(logs);
		finish(consumerLogs);
	});
	piped.batch = [];

	piped._transform = (data, encoding, callback) => {
		piped.batch.push(data);
		if (piped.batch.length >= recordsPerBatch) {
			main(creds, piped.batch, opts).then((results) => {
				logs.push(results);
				piped.batch = [];
				callback(null, results);
			});
		}
		else {
			callback();
		}
	};

	piped._flush = function (callback) {
		if (piped.batch.length) {
			piped.push(piped.batch);

			//data is still left in the stream; flush it!
			main(creds, piped.batch, opts).then((results) => {
				logs.push(results);
				piped.batch = [];
				callback(null, results);
			});

			piped.batch = [];
		}

		else {
			callback(null, aggregateLogs(logs));
		}
	};

	return piped;



}
/**
 * @param  {ReadableStream} data
 * @param  {number} project
 * @param  {number} recordsPerBatch
 * @param  {number} bytesPerBatch
 * @param  {function} transformFunc
 * @param  {string} recordType
 * @param  {string} streamFormat='json'
 */
async function streamingPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, streamFormat = 'json') {
	let records = 0;
	let batches = 0;
	let responses = [];

	return new Promise((resolve, reject) => {
		const pipeline = chain([
			//parse stream as JSON
			streamParseType(data, streamFormat),
			//transform
			(data) => {
				records += 1;
				return transformFunc(data.value);
			},
			//batch
			new Batch({ batchSize: recordsPerBatch }),
			//load
			async (batch) => {
				batches += 1;
				if (recordType === `event` && compress) batch = await gzip(JSON.stringify(batch));
				const sent = await sendDataToMixpanel(project, batch);
				return sent;
			}
		], fileStreamOpts);

		//listening to pipeline
		pipeline.on('error', (error) => {
			reject(error);
		});
		pipeline.on('data', (response) => {
			showProgress(recordType, records, records, batches, batches);
			responses.push(response);
		});
		pipeline.on('end', () => {
			log('');
			totalRecordCount += records;
			resolve(responses);
		});

		data.pipe(pipeline);
	});
}

/**
 * @param  {string} data
 * @param  {number} project
 * @param  {string} type=`file`
 */
async function prepareLookupTable(data, project, type = `file`) {
	if (type === 'memory') {
		return await sendDataToMixpanel(project, data, 'text/csv');
	}

	if (type === 'file') {
		const file = await u.load(data);
		return await sendDataToMixpanel(project, file, 'text/csv');
	}

	throw Error('could not determine lookup table type');
}

/*
-----
FLUSH
-----
*/

/**
 * @param  {number} proj
 * @param  {unknown} batch
 * @param  {string} contentType='application/json'
 */
async function sendDataToMixpanel(proj, batch, contentType = 'application/json') {
	const authString = proj.auth;

	const reqConfig = {
		method: contentType === 'application/json' ? 'POST' : "PUT",
		headers: {
			'Authorization': authString,
			'Content-Type': contentType,

		},
		params: {
			ip: 0,
			verbose: 1,
			strict: Number(strict),
			project_id: proj.projId
		},
		httpsAgent: new Agent({ keepAlive: true, maxTotalSockets: 20 }),
		data: batch
	};

	//events are gzipped
	if (recordType === `event` && compress) {
		reqConfig.headers['Content-Encoding'] = 'gzip';
	}

	try {
		const req = await fetch(url, reqConfig);
		const res = req.data;
		return res;

	} catch (e) {
		log(`\nproblem with request: ${e.message}\n${e.response.data.error}\n`);
		return e.response.data;
	}
}



/*
--------
HELPERS
--------
*/
/**
 * @param  {string | Data | ReadableStream} data
 */
function inferStreamFormat(data) {
	let formatInferred = false;
	let streamFormat = '';
	if (data?.path.endsWith('.json')) {
		streamFormat = 'json';
		formatInferred = true;
	}
	if (data?.path.endsWith('.jsonl')) {
		streamFormat = 'jsonl';
		formatInferred = true;
	}
	if (data?.path.endsWith('.ndjson')) {
		streamFormat = 'jsonl';
		formatInferred = true;
	}
	if (!formatInferred || !streamFormat) {
		throw Error(`no stream format could be inferred for ${data?.path}; please specify streamFormat as json or jsonl in your options`);
	}

	return streamFormat;
}

/**
 * @param  {number} start=2000
 * @param  {number} end=60000
 */
function generateDelays(start = 2000, end = 60000) {
	// https://developer.mixpanel.com/reference/import-events#rate-limits
	const result = [start];
	let current = start;
	while (current < end) {
		let next = current * 2;
		result.push(current * 2);
		current = next;
	}

	return result;
}

/**
 * @param  {string} fileName
 * @param  {string} type
 */
function streamParseType(fileName, type) {
	if (type === 'json') {
		return StreamArray.withParser();
	}

	if (type === 'jsonl') {
		return new JsonlParser();
	}

	if (fileName?.endsWith('.json')) {
		return StreamArray.withParser();
	} else if (fileName?.endsWith('.ndjson') || fileName?.endsWith('.jsonl') || fileName?.endsWith('.txt')) {
		const jsonlParser = new JsonlParser();
		return jsonlParser;
	} else {
		throw Error(`could not identify data; it does not end with: .json, .ndjson, .jsonl, .txt\nif you are .pipe() a stream to this module, specify streamFormat`);
	}

}

/**
 * @param  {} data
 * @param  {} isStream=false
 */
function determineData(data, isStream = false) {
	//identify streams?
	//some duck typing right here
	if (data.pipe || isStream) {
		return `stream`;
	}

	switch (typeof data) {
		case `string`:
			try {
				//could be stringified data
				JSON.parse(data);
				return `structString`;
			} catch (e) {
				//data is not already json
			}

			try {
				const csv = Papa.parse(data, { header: true, skipEmptyLines: true });
				if (csv.errors.length === 0) return `structString`;
			}

			catch (e) {
				//csv parser failed
			}

			//probably a file or directory; stream it
			data = path.resolve(data);
			if (!existsSync(data)) {
				throw Error(`could not find ${data} ... does it exist?`);
			} else {
				let fileMeta = lstatSync(data);
				if (fileMeta.isDirectory()) return `directory`;
				if (fileMeta.isFile()) return `file`;
				return `file`;
			}

		case `object`:
			//probably structured data; just load it
			if (!Array.isArray(data)) console.error(`only arrays of events are supported`);
			return `inMem`;

		default:
			throw Error(`${data} is not an in memory array of objects, a stream, or a string...`);
	}
}

/**
 * @param  {Object} obj
 * @param  {Object} newKeys
 */
function renameKeys(obj, newKeys) {
	//https://stackoverflow.com/a/45287523
	const keyValues = Object.keys(obj).map(key => {
		const newKey = newKeys[key] || key;
		return {
			[newKey]: obj[key]
		};
	});
	return Object.assign({}, ...keyValues);
}

/**
 * @param  {Creds} auth
 */
function resolveProjInfo(auth) {
	let result = {
		auth: `Basic `,
		method: ``
	};

	//preferred method: service acct
	if (auth.acct && auth.pass && auth.project) {
		result.auth += Buffer.from(auth.acct + ':' + auth.pass, 'binary').toString('base64');
		result.method = `serviceAcct`;

	}

	//fallback method: secret auth
	else if (auth.secret) {
		result.auth += Buffer.from(auth.secret + ':', 'binary').toString('base64');
		result.method = `secret`;
	}


	else {
		console.error('no secret or service account provided! quitting...');
		process.exit(0);
	}

	result.token = auth.token;
	result.projId = auth.project;
	result.lookupTableId = auth.lookupTableId;
	result.groupKey = auth.groupKey;
	return result;
}

/**
 * @param  {Data} arrayOfEvents
 * @param  {number} chunkSize
 */
function chunkEv(arrayOfEvents, chunkSize) {
	return arrayOfEvents.reduce((resultArray, item, index) => {
		const chunkIndex = Math.floor(index / chunkSize);

		if (!resultArray[chunkIndex]) {
			resultArray[chunkIndex] = []; // start a new chunk
		}

		resultArray[chunkIndex].push(item);

		return resultArray;
	}, []);
}

/**
 * @param  {Options} options
 * @param  {number} project
 */
function ezTransforms(options, project) {
	//for group imports, ensure 200 max size
	if (options.fixData && options.recordType === `group` && options.recordsPerBatch > 200) {
		options.recordsPerBatch = 200;
	}

	//for user + event imports, ensure 2000 max size
	if (options.fixData && (options.recordType === `user` || options.recordType === `event`) && options.recordsPerBatch > 2000) {
		options.recordsPerBatch = 2000;
	}

	//for strict event imports, make every record has an $insert_id
	if (options.fixData && options.recordType === `event` && options.transformFunc('A') === 'A') {
		options.transformFunc = function addInsertIfAbsent(event) {
			if (!event?.properties?.$insert_id) {
				try {
					let deDupeTuple = [event.name, event.properties.distinct_id || "", event.properties.time];
					let hash = md5(deDupeTuple);
					event.properties.$insert_id = hash;
				}
				catch (e) {
					event.properties.$insert_id = event.properties.distinct_id;
				}
				return event;
			}
			else {
				return event;
			}
		};

	}

	//for user imports, make sure every record has a $token and the right shape
	if (options.fixData && options.recordType === `user` && options.transformFunc('A') === 'A') {
		options.transformFunc = function addUserTokenIfAbsent(user) {
			//wrong shape; fix it
			if (!(user.$set || user.$set_once || user.$add || user.$union || user.$append || user.$remove || user.$unset)) {
				user = { $set: { ...user } };
				user.$distinct_id = user.$set.$distinct_id;
				delete user.$set.$distinct_id;
				delete user.$set.$token;
			}

			//catch missing token
			if ((!user.$token) && project.token) user.$token = project.token;

			return user;
		};
	}


	//for group imports, make sure every record has a $token and the right shape
	if (options.fixData && options.recordType === `group` && options.transformFunc('A') === 'A') {
		options.transformFunc = function addGroupKeysIfAbsent(group) {
			//wrong shape; fix it
			if (!(group.$set || group.$set_once || group.$add || group.$union || group.$append || group.$remove || group.$unset)) {
				group = { $set: { ...group } };
				if (group.$set?.$group_key) group.$group_key = group.$set.$group_key;
				if (group.$set?.$distinct_id) group.$group_id = group.$set.$distinct_id;
				if (group.$set?.$group_id) group.$group_id = group.$set.$group_id;
				delete group.$set.$distinct_id;
				delete group.$set.$group_id;
				delete group.$set.$token;
			}

			//catch missing token
			if ((!group.$token) && project.token) group.$token = project.token;

			//catch group key
			if ((!group.$group_key) && project.groupKey) group.$group_key = project.groupKey;

			return group;
		};
	}


}



/*
-------
LOGGING
-------
*/

function log(message) {
	if (logging) {
		console.log(`${message}\n`);
	}
}

function time(label = `foo`, directive = `start`) {
	if (logging) {
		if (directive === `start`) {
			console.time(`${label} took`);
		} else if (directive === `stop`) {
			console.timeEnd(`${label} took`);
		}
	}
}

function addComma(x) {
	return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function showProgress(record, ev, evTotal, batch, batchTotal) {
	if (logging) {
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`\t${record}s processed: ${addComma(ev)}/${addComma(evTotal)} | batches sent: ${addComma(batch)}/${addComma(batchTotal)}`);
	}
}

function aggregateLogs(logs = []) {
	const finalLog = {
		results: {},
		responses: []
	};

	for (const log of logs) {
		finalLog.responses.push(log.responses[0]);

		for (let key in log.results) {
			if (!finalLog.results[key]) {
				finalLog.results[key] = log.results[key];
			}
			else if (typeof log.results[key] === 'number') {
				finalLog.results[key] += log.results[key];
			}
		}
	}

	finalLog.results.batches = logs.length;

	return finalLog;
}



/*
-----
WORDS
-----
*/

const banner = String.raw`
        .__                                   .__      .__                              __   
  _____ |__|__  ______________    ____   ____ |  |     |__| _____ ______   ____________/  |_ 
 /     \|  \  \/  /\____ \__  \  /    \_/ __ \|  |     |  |/     \\____ \ /  _ \_  __ \   __\
|  Y Y  \  |>    < |  |_> > __ \|   |  \  ___/|  |__   |  |  Y Y  \  |_> >  <_> )  | \/|  |  
|__|_|  /__/__/\_ \|   __(____  /___|  /\___  >____/   |__|__|_|  /   __/ \____/|__|   |__|  
      \/         \/|__|       \/     \/     \/                  \/|__|                       
... streamer of data... to mixpanel!
  by AK
  ak@mixpanel.com
`;


const helpText = `
QUICK USAGE:

$ echo 'MP_SECRET=your-project-secret' > .env
$ mixpanel-import ./pathToData

pathToData can be a .json, .jsonl, .ndjson, or .txt file OR a directory which contains said files.

CONFIGURE:

for more options, require() as a module:

$ npm npm i mixpanel-import --save
const mpImport  =  require('mixpanel-import') 
const importedData = await mpImport(creds, data, options);

const creds = {
	acct: 'my-servce-acct',
	pass: 'my-service-seccret', 
	project: 'my-project-id', 
	token: 'my-project-token'  
}

const options = {
	recordType: "event", //event, user, OR group
	streamSize: 27, 
	region: "US", //US or EU
	recordsPerBatch: 2000, 
	bytesPerBatch: 2 * 1024 * 1024, 
	strict: true, 
	logs: false,
	fixData: false, //apply simple transforms 
	streamFormat: 'json', //or jsonl	
	transformFunc: function noop(a) { return a } //called on every record
}

DOCS: https://github.com/ak--47/mixpanel-import    
`;

/*
-------
EXPORTS
-------
*/

const mpImport = module.exports = main;
mpImport.createMpStream = pipeToMixpanel;

//this allows the module to function as a standalone script
if (require.main === module) {
	console.log(banner);
	main(undefined, undefined, { logs: true, fixData: true }).then((result) => {
		const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
		const fileDir = u.mkdir('./logs');
		const fileName = `${recordType}-import-log-${dateTime}.json`;
		const filePath = `${fileDir}/${fileName}`;

		u.touch(filePath, result, true).then(() => {
			console.log(`results written to: ./${fileName}\n`);
		});

	});

}

/*
---
CYA
---
*/

process.on('uncaughtException', (error) => {
	if (error.errono === -54) return false; //axios keeps throwing these for no reason :(
});


process.on('unhandledRejection', (err) => {
	console.error(`\nFAILURE!\n\n${err.stack}`);
});