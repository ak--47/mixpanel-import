#! /usr/bin/env node

// mixpanel-import
// by AK
// purpose: import events, users, groups, tables into mixpanel... quickly

//stream stuff
const { Transform, PassThrough, Readable, Writable } = require('stream');
const u = require('ak-tools');
const track = u.tracker('mixpanel-import');
const runId = u.uid(32);


//https://github.com/uhop/stream-json/wiki
const { parser } = require('stream-json');
const StreamArray = require('stream-json/streamers/StreamArray');
const JsonlParser = require('stream-json/jsonl/Parser');
const Batch = require('stream-json/utils/Batch');
//https://github.com/uhop/stream-chain/wiki
const { chain } = require('stream-chain');
const Chain = require('stream-chain');

const split = require('split2');

//first party
const { createReadStream, existsSync, lstatSync, readdirSync } = require('fs');
const path = require('path');
const { pick } = require('underscore');
const readline = require('readline');

//third party
const { gzip, ungzip } = require('node-gzip');
const md5 = require('md5');
const isGzip = require('is-gzip');
const fetch = require('node-fetch');

//.env (if used)
require('dotenv').config();

//endpoints
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

//globals (local to this module)
let logging = false;
let fileStreamOpts = {};
let url = ``;
let recordType = ``;
let strict = true;
const supportedTypes = ['.json', '.txt', '.jsonl', '.ndjson'];
let totalRecordCount = 0;
let totalReqs = 0;

async function main(creds = {}, data = [], opts = {}, isStream = false) {
	const defaultOpts = {
		recordType: `event`, //event, user, group (todo lookup table)
		streamSize: 27, //power of 2 for highWaterMark in stream  (default 134 MB)
		region: `US`, //US or EU
		recordsPerBatch: 2000, //records in each req; max 2000 (200 for groups)
		bytesPerBatch: 2 * 1024 * 1024, //bytes in each req
		strict: true, //use strict mode?
		logs: false, //print to stdout?
		streamFormat: 'jsonl', //or json ... only relevant for streams
		transformFunc: function noop(a) { return a; } //will be called on every record
	};
	const options = { ...defaultOpts, ...opts };


	const defaultCreds = {
		acct: ``, //service acct username
		pass: ``, //service acct secret
		project: ``, //project id
		secret: ``, //api secret (deprecated auth)
		token: `` //project token        
	};

	//sweep .env to pickup MP_ keys; i guess the .env convention is to use all caps? so be it...
	const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`);
	const envKeyNames = { MP_PROJECT: "project", MP_ACCT: "acct", MP_PASS: "pass", MP_SECRET: "secret", MP_TOKEN: "token" };
	const envCreds = renameKeys(envVars, envKeyNames);
	const project = resolveProjInfo({ ...defaultCreds, ...creds, ...envCreds });

	//these values are used in the pipeline
	const { streamSize, region, recordsPerBatch, bytesPerBatch, transformFunc, streamFormat } = options;

	//these a 'globals' set by the caller
	recordType = options.recordType;
	logging = options.logs;
	fileStreamOpts = { highWaterMark: 2 ** streamSize };
	strict = options.strict;
	url = ENDPOINTS[region.toLowerCase()][recordType];

	//if script is run standalone, use CLI params as source data
	//if script is run standalone, use CLI params as source data
	const lastArgument = [...process.argv].pop();
	if (data?.length === 0 && supportedTypes.some(type => lastArgument.includes(type))) {
		data = lastArgument;
		logging = true;
	}
	else if (lastArgument?.toLowerCase()?.includes('help')) {
		const banner = String.raw`
        .__                                   .__      .__                              __   
  _____ |__|__  ______________    ____   ____ |  |     |__| _____ ______   ____________/  |_ 
 /     \|  \  \/  /\____ \__  \  /    \_/ __ \|  |     |  |/     \\____ \ /  _ \_  __ \   __\
|  Y Y  \  |>    < |  |_> > __ \|   |  \  ___/|  |__   |  |  Y Y  \  |_> >  <_> )  | \/|  |  
|__|_|  /__/__/\_ \|   __(____  /___|  /\___  >____/   |__|__|_|  /   __/ \____/|__|   |__|  
      \/         \/|__|       \/     \/     \/                  \/|__|                       
`;
		console.log(banner);
		console.log('... streamer of data... to mixpanel!');
		console.log('by AK');
		console.log('ak@mixpanel.com');
		console.log(`
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
    `);
		process.exit(0);
	}
	time('ETL', 'start');
	track('start', {runId, ...options});

	//implemented pipeline
	let pipeline;
	const dataType = determineData(data, isStream);
	switch (dataType) {
		case `file`:
			log(`streaming ${recordType}s from ${data}`);
			time('stream pipeline', 'start');

			pipeline = await filePipeLine(data, project, recordsPerBatch, bytesPerBatch, transformFunc);

			log('\n');
			time('stream pipeline', 'stop');
			break;

		case `inMem`:
			log(`parsing ${recordType}s`);
			pipeline = await dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType);
			break;

		case `stream`:
			log(`consuming stream of ${recordType}s from ${data?.path}`);
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
			throw new Error(`mixpanel-import was not able to import: ${data}`);
			break;
	}

	time('ETL', 'stop');
	track('end', {runId, ...options});
	const summary = {
		results: {
			// numSuccess : 0,
			// numFailed : 0,
			totalRecordCount,
			totalReqs,
			recordType
		},
		responses: pipeline
	};
	return summary;

}

//CORE PIPELINE(S)
async function filePipeLine(data, project, recordsPerBatch, bytesPerBatch, transformFunc) {
	return new Promise((resolve, reject) => {
		//streaming files to mixpanel!       
		const pipeline = chain([
			createReadStream(path.resolve(data)),
			streamParseType(data),
			//transform func
			(data) => {
				return transformFunc(data.value);
			},
			new Batch({ batchSize: recordsPerBatch }),
			async (batch) => {

				records += batch.length;
				batches += 1;

				if (recordType === `event`) {
					return await gzip(JSON.stringify(batch));
				} else {
					return Promise.resolve(JSON.stringify(batch));
				}

			},
			async (batch) => {
				return await sendDataToMixpanel(project, batch);
			}
		]);

		//listening to the pipeline
		let records = 0;
		let batches = 0;
		let responses = [];

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

async function dataInMemPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType) {
	time('chunk', 'start');
	let dataIn = data.map(transformFunc);
	let batches;
	if (recordType === `event`) {
		batches = await zipChunks(chunkSize(chunkEv(dataIn, recordsPerBatch), bytesPerBatch));
	} else {
		batches = chunkSize(chunkEv(dataIn, recordsPerBatch), bytesPerBatch);
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

async function sendDataToMixpanel(proj, batch) {
	let authString = proj.auth;

	let options = {
		method: 'POST',
		headers: {
			'Authorization': authString,
			'Content-Type': 'application/json',

		},
		body: batch
	};

	if (recordType === `event`) options.headers['Content-Encoding'] = 'gzip';
	else {
		//only stringify non-stringied records
		if (typeof options.body !== `string`) {
			options.body = JSON.stringify(options.body);
		}
	}

	try {
		let req = await fetch(`${url}?ip=0&verbose=1&strict=${Number(strict)}&project_id=${proj.projId}`, options);
		let res = await req.json();
		return res;

	} catch (e) {
		log(`problem with request:\n${e}`);
	}
}

async function streamingPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc, recordType, streamFormat = 'json') {
	//needs to .pipe() from data source!
	return new Promise((resolve, reject) => {
		//streaming files to mixpanel!       
		const pipeline = chain([
			streamParseType(data, streamFormat),
			//transform func
			(data) => {
				// debugger;
				return transformFunc(data.value);
			},
			new Batch({ batchSize: recordsPerBatch }),
			async (batch) => {

				records += batch.length;
				batches += 1;

				if (recordType === `event`) {
					return await gzip(JSON.stringify(batch));
				} else {
					return Promise.resolve(JSON.stringify(batch));
				}

			},
			async (batch) => {
				return await sendDataToMixpanel(project, batch);
			}
		]);

		data.pipe(pipeline);

		//listening to the pipeline
		let records = 0;
		let batches = 0;
		let responses = [];

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


// const streamPipe = new Chain([
// 	(stream) => { new Readable().wrap(stream) },
// 	// (stream) => { return stream.pipe(split())},
// 	async (stream, enc) => { return await main({}, stream, {}, true) },
// 		(result) => { debugger; return result }
// ]).on('end', (res) => {
// 	debugger;
// })





// const pipeToMixpanelPipeline = (data, enc) => {
//     if (data instanceof Buffer) {
// 		debugger;
// 	}

// 	else {
// 		return ()=>{} //no-op
// 	}

// }



//HELPERS
function calcResults(arrOfResponses) {
	//for events


	//for engage
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
			if (!Array.isArray(data)) console.error(`only arrays of events are support`);
			return `inMem`;
			break;

		default:
			console.error(`${data} is not an Array or string...`);
			return `unknown`;
			break;
	}
}

//https://stackoverflow.com/a/45287523
function renameKeys(obj, newKeys) {
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

function chunkSize(arrayOfBatches, maxBytes) {
	return arrayOfBatches.reduce((resultArray, item, index) => {
		const currentLengthInBytes = JSON.stringify(item).length; //assume each character is a byte

		//if the batch is too big; cut it in half
		if (currentLengthInBytes >= maxBytes) {
			//todo: make this is a little smarter
			let midPointIndex = Math.ceil(item.length / 2);
			let firstHalf = item.slice(0, midPointIndex);
			let secondHalf = item.slice(-midPointIndex);
			resultArray.push(firstHalf);
			resultArray.push(secondHalf);
		} else {
			resultArray.push(item);
		}

		return resultArray;
	}, []);
}

async function zipChunks(arrayOfBatches) {
	const allBatches = arrayOfBatches.map(async function (batch) {
		return await gzip(JSON.stringify(batch));
	});
	return Promise.all(allBatches);
}

//side effects + logging things
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

const mpImport = module.exports = main;
mpImport.mpStream = pipeToMixpanelPipeline;


//this allows the module to function as a standalone script
if (require.main === module) {
	main(null).then((result) => {
		console.log(`RESULTS:\n\n`);
		console.log(JSON.stringify(result, null, 2));
	});

}