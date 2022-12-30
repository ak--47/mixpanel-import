#! /usr/bin/env node
/* eslint-disable no-unused-vars */

// mixpanel-import
// by AK
// purpose: stream events, users, groups, tables into mixpanel... with best practices!

// todos:
/*
- lookup tables
- directories
- cool banners
- CLI interface
- streaming interface
- retries

*/

/*
-----
TYPES
-----
*/

/**
 * @typedef {Object} Creds - mixpanel project credentials
 * @property {string} acct - service account username
 * @property {string} pass - service account password
 * @property {(string | number)} project - project id
 * @property {string} [token] - project token (for importing user profiles)
 * @property {string} [lookupTableId] - lookup table ID (for importing lookup tables)
 * @property {string} [groupKey] - group identifier (for importing group profiles)
 */

/**
 * @typedef {Object} Options - import options
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



/*
---------
CORE DEPS
---------
*/

// * parsers
const readline = require('readline');
const Papa = require('papaparse');
const { parser: jsonlParser } = require('stream-json/jsonl/Parser');
const StreamArray = require('stream-json/streamers/StreamArray'); //json parser

//* streamers
const _ = require('highland');
const stream = require('stream');

const got = require('got');

const MultiStream = require('multistream');
const CombinedStream = require('combined-stream');
const StreamConcat = require('stream-concat');
const merge2 = require('merge2');

// * file system
const path = require('path');
const fs = require('fs');

// * CLI
const yargs = require('yargs');
require('dotenv').config();


// * utils
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);
const md5 = require('md5');
const { pick } = require('underscore');
const { gzip } = require('node-gzip');




/*
-------
CONFIG
-------
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
		this.auth = this.resolveProjInfo();

		// * options
		this.recordType = opts.recordType || `event`; // event, user, group or table
		this.compress = opts.compress || false; //gzip data (events only)
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)
		this.region = opts.region || `US`; // US or EU
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 2 * 1024 * 1024; // max bytes in each req
		this.strict = opts.strict || true; // use strict mode?
		this.logs = opts.logs || true; // print to stdout?
		this.verbose = opts.verbose || true;
		this.fixData = opts.fixData || false; //apply transforms on the data
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.transformFunc = opts.transformFunc || function noop(a) { return a; }; //will be called on every record

		// * counters
		this.recordsProcessed = 0;
		this.success = 0;
		this.failed = 0;
		this.retries = 0;
		this.batches = 0;
		this.requests = 0;
		this.responses = [];
		this.timer = u.time('etl');

		// * request stuff
		this.reqMethod = "POST";
		this.mimeType = "application/json";
		this.encoding = "";

		// ! apply EZ transforms: this will mutate the instance when called
		if (this.fixData) ezTransforms(this);

		// ! fix plurals
		if (this.recordType === 'events') this.recordType === 'event';
		if (this.recordType === 'users') this.recordType === 'user';
		if (this.recordType === 'groups') this.recordType === 'group';
		if (this.recordType === 'tables') this.recordType === 'table';

		//! apply correct headers
		if (this.recordType === "table") {
			this.reqMethod = 'PUT';
			this.mimeType = 'text/csv';
			this.transformFunc = (a) => { return a; };
		}

	}

	// ? private props
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

	// getters
	get type() {
		return this.recordType;
	}

	get url() {
		let url = this.endpoints[this.region.toLowerCase()][this.recordType.toLowerCase()];
		if (this.recordType === "table") url += this.lookupTableId;
		return url;
	}

	get streamOpts() {
		return { writableObjectMode: false, readableObjectMode: true };
	}

	get opts() {
		const { recordType, compress, streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc } = this;
		return { recordType, compress, streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc };
	}

	get creds() {
		const { acct, pass, project, secret, token, lookupTableId, groupKey, auth } = this;
		return { acct, pass, project, secret, token, lookupTableId, groupKey, auth };
	}

	// ? setter methods
	set batchSize(chunkSize) {
		this.recordsPerBatch = chunkSize;
	}

	set transform(fn) {
		this.transformFunc = fn;
	}

	set results(response) {
		this.responses.push(response);
		this.responses = this.responses.flat();
	}

	//methods
	report() {
		return Object.assign(this);
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

	validate() {
		// todo
	}

	summary() {
		const summary = {
			success: this.success,
			failed: this.failed,
			total: this.recordsProcessed,
			requests: this.responses.length,
			recordType: this.recordType,
			duration: this.timer.report(false).delta,
			human: this.timer.report(false).human,
			retries: 0,
			responses: this.responses
		};

		return summary;
	}
}

/*
----------
EXT PARAMS
----------
*/
function getEnvVars() {
	// * only resolves credentials
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

function getCLIParams() {
	// todo
	const args = yargs(process.argv.splice(2))
		.argv;
	return args;
}


/*
----
CORE
----
*/

/**
 * Mixpanel Importer
 * @example
 * mpImport(creds, data, opts)
 * @param {Creds} creds 
 * @param {*} data 
 * @param {Options} opts 
 * @param {boolean} isStream 
 * @returns API reciepts of imported data
 */
async function main(creds = {}, data = null, opts = {}, isStream = false) {
	// gathering params
	const envVar = getEnvVars();
	const cli = getCLIParams();
	const cliData = cli._[0];
	const config = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	// ETL
	config.timer.start();
	const streams = await determineData(data || cliData, config);
	for (const stream of streams) {
		const imported = await corePipeline(stream, config);
		config.results = imported;
	}

	config.timer.end(false);

	// clean up
	const summary = config.summary();
	console.log('\n');
	return summary;
}

async function corePipeline(stream, config, exposeStream = false) {
	if (config.recordType === 'table') {
		// todo ERROR HANDLING
		const res = await flushToMixpanel(stream, config);
		config.recordsProcessed = stream.split('\n').length - 1;
		config.success = config.recordsProcessed;
		return res;
	}

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
			showProgress(config.recordType, config.recordsProcessed, config.requests);
		})

		// .errors((e) => {
		// 	debugger;
		// })

		// * consume stream
		.collect();


	// for consumers to pipe()
	if (exposeStream) {
		return pipeline.toNodeStream({ objectMode: true });
	}

	// for consumers to await
	else {
		return await Promise.all(await pipeline.toPromise(Promise));
	}

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
			headers: {
				"Authorization": `Basic ${config.auth}`,
				"Content-Type": config.mimeType,
				"Content-Encoding": config.encoding
			},
			body
		};
		const req = await got(options);
		const res = JSON.parse(req.body);
		if (config.recordType === 'event') {
			config.success += res.num_records_imported || 0;
			config.failed += res?.failed_records?.length || 0;
		}
		if (config.recordType === 'user' || config.recordType === 'group') {
			if (!res.error || res.status) config.success += batch.length;
			if (res.error || !res.status) config.failed += batch.length;
		}
		return res;
	}

	catch (e) {
		if (config.recordType === 'user' || config.recordType === 'group') {
			config.failed += batch.length;
		}
		batch;
		config;
		console.error(`request failed! ${e.message}`);
		console.error(u.json(JSON.parse(e.response.body)));
		debugger;
	}
}


async function determineData(data, config) {
	if (config.recordType === 'table') {
		return [data];
	}

	//data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		return [existingStream(data)];
	}

	//data is an object in memory
	if (Array.isArray(data)) {
		return [stream.Readable.from(data, { objectMode: true })];
	}

	//data refers to file/folder on disk
	if (fs.existsSync(path.resolve(data))) {

		const fileOrDir = fs.lstatSync(path.resolve(data));

		if (fileOrDir.isFile()) {
			if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(data))) {
				return lineByLineStream(path.resolve(data));
			}

			if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(data))) {
				return objectModeStream(path.resolve(data));
			}
		}

		if (fileOrDir.isDirectory()) {
			const enumDir = await u.ls(path.resolve(data));
			const files = enumDir.filter(filePath => config.supportedFileExt.includes(path.extname(filePath)));
			if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(files[0]))) {
				return lineByLineStream(files);
			}
			if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(files[0]))) {
				return objectModeStream(files);
			}
		}
	}

	if (typeof data === 'string') {
		//stringified JSON
		try {
			return [stream.Readable.from(JSON.parse(data), { objectMode: true })];
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

	console.error(`ERROR: NOT FOUND\n\t${data} is not a file, a folder, an array, a stream, or a string...`);
	process.exit();

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
			push(null, _.nil);
		});
	};

	return generator;
}

function lineByLineStream(filePath) {
	let stream;
	if (Array.isArray(filePath)) {
		stream = filePath.map((file) => fs.createReadStream(file));
	}
	else {
		stream = [fs.createReadStream(filePath)];
	}

	return stream.map(s => s.pipe(jsonlParser()).map(token => token.value));
}

function objectModeStream(filePath) {
	let stream;
	if (Array.isArray(filePath)) {
		stream = filePath.map((file) => fs.createReadStream(file));
	}
	else {
		stream = [fs.createReadStream(filePath)];
	}

	return stream.map(s => {
		s.pipe(StreamArray.withParser())
			.map(token => token.value);
	});
}


function ezTransforms(config) {
	//for group imports, ensure 200 max size
	if (config.recordType === `group` && config.recordsPerBatch > 200) {
		config.batchSize = 200;
	}

	//for user + event imports, ensure 2000 max size
	if ((config.recordType === `user` || config.recordType === `event`) && config.recordsPerBatch > 2000) {
		config.batchSize = 2000;
	}

	//for strict event imports, make every record has an $insert_id
	if (config.recordType === `event` && config.transformFunc('A') === 'A') {
		config.transform = function addInsertIfAbsent(event) {
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
	if (config.recordType === `user` && config.transformFunc('A') === 'A') {
		config.transform = function addUserTokenIfAbsent(user) {
			//wrong shape; fix it
			if (!(user.$set || user.$set_once || user.$add || user.$union || user.$append || user.$remove || user.$unset)) {
				user = { $set: { ...user } };
				user.$distinct_id = user.$set.$distinct_id;
				delete user.$set.$distinct_id;
				delete user.$set.$token;
			}

			//catch missing token
			if ((!user.$token) && config.token) user.$token = config.token;

			return user;
		};
	}


	//for group imports, make sure every record has a $token and the right shape
	if (config.recordType === `group` && config.transformFunc('A') === 'A') {
		config.transform = function addGroupKeysIfAbsent(group) {
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
			if ((!group.$token) && config.token) group.$token = config.token;

			//catch group key
			if ((!group.$group_key) && config.groupKey) group.$group_key = config.groupKey;

			return group;
		};
	}

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


/*
----
LOGGING
----
*/

function showProgress(record, processed, requests,) {
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(`\t${record}s processed: ${u.comma(processed)} | batches sent: ${u.comma(requests)}`);
}

async function writeLogs(data) {
	const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
	const fileDir = u.mkdir('./logs');
	const fileName = `${data.recordType}-import-log-${dateTime}.json`;
	const filePath = `${fileDir}/${fileName}`;
	const file = await u.touch(filePath, data, true);
	console.log(`results written to: ./${file}\n`);
}


// // https://medium.com/florence-development/working-with-node-js-stream-api-60c12437a1be
// function pipeToMixpanel(creds = {}, opts = {}, finish = () => { }) {
// 	const { recordsPerBatch = 2000 } = opts;
// 	const logs = [];
// 	const piped = new stream.Transform({ objectMode: true, highWaterMark: recordsPerBatch });

// 	piped.on('finish', () => {
// 		const consumerLogs = aggregateLogs(logs);
// 		finish(consumerLogs);
// 	});
// 	piped.batch = [];

// 	piped._transform = (data, encoding, callback) => {
// 		piped.batch.push(data);
// 		if (piped.batch.length >= recordsPerBatch) {
// 			main(creds, piped.batch, opts).then((results) => {
// 				logs.push(results);
// 				piped.batch = [];
// 				callback(null, results);
// 			});
// 		}
// 		else {
// 			callback();
// 		}
// 	};

// 	piped._flush = function (callback) {
// 		if (piped.batch.length) {
// 			piped.push(piped.batch);

// 			//data is still left in the stream; flush it!
// 			main(creds, piped.batch, opts).then((results) => {
// 				logs.push(results);
// 				piped.batch = [];
// 				callback(null, results);
// 			});

// 			piped.batch = [];
// 		}

// 		else {
// 			callback(null, aggregateLogs(logs));
// 		}
// 	};

// 	return piped;



// }




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

--project [pid] 
--acct [serviceAcct] 
--pass [servicePass] 
--secret [secret] 
--token [token] 
--type [recordType] 
--table [lookuptableId] 
--group [groupKey] 
--recordType [event, user, group, table] 
--compress 
--strict 
--logs 
--fixData 
--streamFormat [jsonl] 
--streamSize [27] 
--region [US] 
--recordsPerBatch [2000] 
--bytesPerBatch [1024*1024*8]

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
// todo
// mpImport.createMpStream = pipeToMixpanel;

// todo
if (require.main === module) {
	console.log(banner);
	main().then((result) => {
		const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
		const fileDir = u.mkdir('./logs');
		const fileName = `${result.recordType}-import-log-${dateTime}.json`;
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