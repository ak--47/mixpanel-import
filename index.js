#! /usr/bin/env node

// mixpanel-import
// by AK
// purpose: stream events, users, groups, tables into mixpanel... with best practices!

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

const { Agent } = require('https');
const { pick } = require('underscore');
const { createReadStream, existsSync, readdirSync } = require('fs');
const { gzip } = require('node-gzip');


/*
-------------
PIPELINE DEPS
-------------
*/

const { Transform, PassThrough, Readable, Writable } = require('stream');
const { chain } = require('stream-chain');
const StreamArray = require('stream-json/streamers/StreamArray');
const JsonlParser = require('stream-json/jsonl/Parser');
const Batch = require('stream-json/utils/Batch');
require('dotenv').config();


/*
-----------
RETRIES
-----------
*/

const exponentialBackoff = generateDelays();
axiosRetry(fetch, {
	retries: 5, // number of retries
	retryDelay: (retryCount) => {
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

let logging = false;
let fileStreamOpts = {};
let url = ``;
let recordType = ``;
let strict = true;
const supportedTypes = ['.json', '.txt', '.jsonl', '.ndjson', '.csv'];
let totalRecordCount = 0;
let totalReqs = 0;
let retries = 0;


/*
----
MAIN
----
*/

async function main(creds = {}, data = [], opts = {}, isStream = false) {

	const defaultOpts = {
		recordType: `event`, // event, user, group or table
		streamSize: 27, // power of 2 for highWaterMark in stream  (default 134 MB)
		region: `US`, // US or EU
		recordsPerBatch: 2000, // records in each req; max 2000 (200 for groups)
		bytesPerBatch: 2 * 1024 * 1024, // max bytes in each req
		strict: true, // use strict mode?
		logs: false, // print to stdout?
		streamFormat: '', // json or jsonl ... only relevant for streams
		transformFunc: function noop(a) { return a; } //will be called on every record
	};
	const options = { ...defaultOpts, ...opts };
	options.recordType = options.recordType.toLowerCase();
	options.streamFormat = options.streamFormat.toLowerCase();
	options.region = options.region.toLowerCase();


	const defaultCreds = {
		acct: ``, //service acct username
		pass: ``, //service acct secret
		project: ``, //project id
		secret: ``, //api secret (deprecated auth)
		token: ``, //project token 
		lookupTableId: `` //lookup table id       
	};

	//sweep .env to pickup MP_ keys
	const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`, `MP_TYPE`, `MP_TABLE_ID`);
	const envKeyNames = {
		MP_PROJECT: "project",
		MP_ACCT: "acct",
		MP_PASS: "pass",
		MP_SECRET: "secret",
		MP_TOKEN: "token",
		recordType: "MP_TYPE",
		lookupTableId: "MP_TABLE_ID"
	};
	const envCreds = renameKeys(envVars, envKeyNames);
	const project = resolveProjInfo({ ...defaultCreds, ...envCreds, ...creds });
	if (envCreds.recordType) options.recordType = envCreds.recordType;
	if (envCreds.lookupTableId) project.lookupTableId = envCreds.lookupTableId;

	//for strict event imports, make every record has an $insert_id
	if (options.strict && options.recordType === `event` && options.transformFunc('A') === 'A') {
		options.transformFunc = function addInsertIfAbsent(event) {
			if (!event?.properties?.$insert_id) {
				try {
					let deDupeTuple = [event.name, event.properties.distinct_id || "", event.properties.time];
					let hash = md5(deDupeTuple);
					event.properties.$insert_id = hash;
				}
				catch (e) {

				}
				return event;
			}
			else {
				return event;
			}
		};

	}

	//for strict user imports, make sure every record has a $token and the right shape
	if (options.strict && options.recordType === `user` && options.transformFunc('A') === 'A') {
		options.transformFunc = function addUserTokenIfAbsent(user) {
			//wrong shape; fix it
			if (!user.$set || !user.$set_once || !user.$add || !user.$union || !user.$append || !user.$remove || !user.$unset) {
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

	//for group imports, ensure 200 max size
	if (options.strict && options.recordType === `group` && options.recordsPerBatch > 200) {
		options.recordsPerBatch = 200
	}

	const { streamSize, region, recordsPerBatch, bytesPerBatch, transformFunc } = options;
	let { streamFormat } = options;
	recordType = options.recordType;
	logging = options.logs;
	fileStreamOpts = { highWaterMark: 2 ** streamSize, writableObjectMode: false, readableObjectMode: true };
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
	if (data?.length === 0 && supportedTypes.some(type => lastArgument.includes(type))) {
		data = lastArgument;
		logging = true;
	}
	else if (lastArgument?.toLowerCase()?.includes('help')) {
		console.log(banner);
		console.log(helpText);
		process.exit(0);
	}
	const startTime = Date.now();
	time('ETL', 'start');
	track('start', { runId, ...options });

	//CORE PIPELINES
	const dataType = determineData(data, isStream);
	let pipeline;
	switch (dataType) {
		case `file`:
			log(`streaming ${recordType}s from ${data}...`);
			time('stream pipeline', 'start');
			//todo lookup table
			if (recordType === 'table') {
				pipeline = await prepareLookupTable(data, project, `file`);
			}
			else {
				pipeline = await filePipeLine(data, project, recordsPerBatch, bytesPerBatch, transformFunc);
			}

			log('\n');
			time('stream pipeline', 'stop');
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
				let formatInferred = false;
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
				if (!formatInferred) {
					throw Error(`no stream format specified for ${data?.path}; please use json or jsonl`);
				}
			}
			log(`consuming ${streamFormat} stream of ${recordType}s from ${data?.path}...`);
			pipeline = await streamingPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, streamFormat);
			break;

		case `directory`:
			pipeline = [];
			const files = readdirSync(data).map(fileName => {
				return {
					name: fileName,
					path: path.resolve(`${data}/${fileName}`)
				};
			});
			log(`found ${addComma(files.length)} files in ${data}`);
			time('stream pipeline', 'start');

			walkDirectory: for (const file of files) {
				log(`streaming ${recordType}s from ${file.name}`);
				try {
					let result = await filePipeLine(file.path, project, recordsPerBatch, bytesPerBatch, transformFunc);
					pipeline.push({
						[file.name]: result
					});
					log('\n');
				} catch (e) {
					log(`  ${file.name} is not valid JSON/NDJSON; skipping`);
					continue walkDirectory;
				}
			}

			time('stream pipeline', 'stop');
			break;

		default:
			log(`could not determine data source`);
			throw Error(`mixpanel-import was not able to import: ${data}`);
			break;
	}

	time('ETL', 'stop');
	track('end', { runId, ...options });

	// summary of pipeline results
	const endTime = Date.now();
	const duration = (endTime - startTime) / 1000;
	const total = totalRecordCount;
	const batches = totalReqs;
	let success, failed;
	if (recordType === `event`) {
		if (dataType === `directory`) {
			const flatRes = [];
			for (const [index, fileRes] of pipeline.entries()) {
				flatRes.push(pipeline[index][Object.keys(fileRes)[0]]);
			}
			success = flatRes.flat().map(res => res.num_records_imported).reduce((prev, curr) => prev + curr);
			failed = total - success;
		}

		else {
			success = pipeline.map(res => res.num_records_imported).reduce((prev, curr) => prev + curr);
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
		success = pipeline.filter(res => res.code === 200).length;
		failed = pipeline.filter(res => res.code !== 200).length;
	}

	const summary = {
		results: {
			success,
			failed,
			total,
			batches,
			recordType,
			duration,
			retries

		},
		responses: pipeline
	};
	return summary;

}

/*
---------
PIPELINES
---------
https://github.com/uhop/stream-chain/wiki

*/

async function dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType) {
	time('chunk', 'start');
	let dataIn = data.map(transformFunc);
	let batches;
	if (recordType === `event`) {
		batches = chunkMaxSize(chunkEv(dataIn, recordsPerBatch), bytesPerBatch);
	} else {
		batches = chunkEv(dataIn, recordsPerBatch);
	}
	time('chunk', 'stop');
	log(`\nloaded ${addComma(dataIn.length)} ${recordType}s`);

	//flush to mixpanel
	time('flush');
	let responses = [];
	let iter = 0;
	for (const batch of batches) {
		iter += 1;
		totalReqs += 1;

		showProgress(recordType, recordsPerBatch * iter, dataIn.length, iter, batches.length);
		let res = await sendDataToMixpanel(project, batch);
		responses.push(res);
	}
	totalRecordCount = dataIn.length;
	log('\n');
	time('flush', 'stop');
	log('\n');

	return responses;

}

async function filePipeLine(data, project, recordsPerBatch, bytesPerBatch, transformFunc) {
	let records = 0;
	let batches = 0;
	let responses = [];

	return new Promise((resolve, reject) => {
		//streaming files to mixpanel!       
		const pipeline = chain([
			createReadStream(path.resolve(data)),
			streamParseType(data),
			(data) => {
				records += 1;
				return transformFunc(data.value);
			},
			new Batch({ batchSize: recordsPerBatch }),
			async (batch) => {
				batches += 1;
				return await sendDataToMixpanel(project, batch);
			}
		], fileStreamOpts);

		//listening to the pipeline
		pipeline.on('error', (error) => {
			reject(error);
		});
		pipeline.on('data', (response, f, o) => {
			totalReqs += 1;
			showProgress(recordType, records, records, batches, batches);
			responses.push(response);
		});
		pipeline.on('end', () => {
			totalRecordCount += records;
			resolve(responses);
		});
	});
}

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
				const sent = await sendDataToMixpanel(project, batch);
				return sent;
			}
		], fileStreamOpts);

		//listening to pipeline
		pipeline.on('error', (error) => {
			reject(error);
		});
		pipeline.on('data', (response, f, o) => {
			totalReqs += 1;
			showProgress(recordType, records, records, batches, batches);
			responses.push(response);
		});
		pipeline.on('end', () => {
			totalRecordCount += records;
			resolve(responses);
		});


		data.pipe(pipeline);
	});
}

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

async function sendDataToMixpanel(proj, batch, contentType = 'application/json') {
	const authString = proj.auth;

	const reqConfig = {
		method: 'POST',
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
	try {
		const req = await fetch(url, reqConfig);
		const res = req.data;
		return res;

	} catch (e) {
		log(`problem with request: ${e.message}\n${e.response.data.error}\n`);
		return e.response.data;
	}
}


/*
--------------
IN DEVELOPMENT
--------------
*/

const pipeToMixpanelPipeline = new Transform({
	defaultEncoding: 'utf8',
	transform(chunk, encoding, cb) {
		this.push(chunk.toString('utf8'));
		cb();
	},
	flush(cb) {
		this.push(null);
		cb();
	}

});

pipeToMixpanelPipeline.on('data', async (stream, b, c) => {
	let pipeData = await main({}, stream, {}, true);
	return pipeData;
});


/*
--------
HELPERS
--------
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
			} catch (error) {
				//data is not stringified
			}

			//probably a file or directory; stream it
			let dataPath = path.resolve(data);
			if (!existsSync(dataPath)) {
				console.error(`could not find ${data} ... does it exist?`);
			} else {
				let fileMeta = lstatSync(dataPath);
				if (fileMeta.isDirectory()) return `directory`;
				if (fileMeta.isFile()) return `file`;
				return `file`;
			}
			break;

		case `object`:
			//probably structured data; just load it
			if (!Array.isArray(data)) console.error(`only arrays of events are supported`);
			return `inMem`;
			break;

		default:
			console.error(`${data} is not an Array or string...`);
			return `unknown`;
			break;
	}
}

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

function resolveProjInfo(auth) {
	let result = {
		auth: `Basic `,
		method: ``
	};
	//fallback method: secret auth
	if (auth.secret) {
		result.auth += Buffer.from(auth.secret + ':', 'binary').toString('base64');
		result.method = `secret`;

	}

	//preferred method: service acct
	if (auth.acct && auth.pass && auth.project) {
		result.auth += Buffer.from(auth.acct + ':' + auth.pass, 'binary').toString('base64');
		result.method = `serviceAcct`;

	}

	result.token = auth.token;
	result.projId = auth.project;
	return result;
}

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

function chunkMaxSize(data, size) {
	const sizeChunked = data.map((batch) => { return sizeChunker(batch, size); });
	return sizeChunked.flat();

}

function sizeChunker(input, bytesSize = Number.MAX_SAFE_INTEGER, failOnOversize = false) {
	const output = [];
	let outputSize = 0;
	let outputFreeIndex = 0;

	if (!input || input.length === 0 || bytesSize <= 0) {
		return output;
	}

	for (let obj of input) {
		const objSize = getObjectSize(obj);
		if (objSize > bytesSize && failOnOversize) {
			throw new Error(`Can't chunk array as item is bigger than the max chunk size`);
		}

		const fitsIntoLastChunk = (outputSize + objSize) <= bytesSize;

		if (fitsIntoLastChunk) {
			if (!Array.isArray(output[outputFreeIndex])) {
				output[outputFreeIndex] = [];
			}

			output[outputFreeIndex].push(obj);
			outputSize += objSize;
		} else {
			if (output[outputFreeIndex]) {
				outputFreeIndex++;
				outputSize = 0;
			}

			output[outputFreeIndex] = [];
			output[outputFreeIndex].push(obj);
			outputSize += objSize;
		}
	}

	return output;
};

function getObjectSize(obj) {
	try {
		const str = stringify(obj);
		return Buffer.byteLength(str, 'utf8');
	} catch (error) {
		return 0;
	}
}

async function zipChunks(arrayOfBatches) {
	const allBatches = arrayOfBatches.map(async function (batch) {
		return await gzip(JSON.stringify(batch));
	});
	return Promise.all(allBatches);
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
			console.time(label);
		} else if (directive === `stop`) {
			console.timeEnd(label);
		}
	}
}

function addComma(x) {
	return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function showProgress(record, ev, evTotal, batch, batchTotal) {
	if (logging) {
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`  ${record}s sent: ${addComma(ev)}/${addComma(evTotal)} | batches sent: ${addComma(batch)}/${addComma(batchTotal)}\n\n`);
	}
}

// THIS IS WEIRD!!!

process.on('uncaughtException', (error, origin) => {
	if (error.errono === -54) return false; //axios keeps throwing these for no reason :(
});


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
// mpImport.mpStream = pipeToMixpanelPipeline;


//this allows the module to function as a standalone script
if (require.main === module) {
	main({logs: true}).then((result) => {
		console.log(`RESULTS:\n\n`);
		console.log(JSON.stringify(result, null, 2));
	});

}
