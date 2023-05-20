//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const stream = require('stream');
const u = require('ak-tools');
const Types = require("../index.js");



export default async function main() {
	const JSON = `./benchmarks/testData/dnd250.json`;
	const NDJSON = `./benchmarks/testData/dnd250.ndjson`;
	const data = require(`.${JSON}`);
	const objStream = new stream.Readable.from(data, { objectMode: true });

	/** @type {Types.Options} */
	const opts = {
		logs: false,
		verbose: false,
	};

	const res = {
		JSON: {},
		NDJSON: {},
		objMode: {}
	};

	console.log('JSON v.s. NDJSON v.s OBJECT STREAMS');

	// JSON V.S. NDJSON
	console.log('\tJSON START');
	const jsonImport = await mpStream({}, JSON, { ...opts, streamFormat: 'json' });
	res.JSON.time = jsonImport.duration;
	res.JSON.eps = jsonImport.eps;
	res.JSON.rps = jsonImport.rps;
	res.JSON.workers = jsonImport.workers;
	res.JSON.human = jsonImport.human;
	console.log('\tJSON END');
	console.log('\n\n');
	console.log('\tNDJSON START');
	const ndJSONImport = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl' });
	res.NDJSON.time = ndJSONImport.duration;
	res.NDJSON.eps = ndJSONImport.eps;
	res.NDJSON.rps = ndJSONImport.rps;
	res.NDJSON.workers = ndJSONImport.workers;
	res.NDJSON.human = ndJSONImport.human;
	console.log('\tNDJSON END');
	console.log('\n\n');
	console.log('\tSTREAM START');
	const objMode = await mpStream({}, objStream, { ...opts });
	res.objMode.time = objMode.duration;
	res.objMode.eps = objMode.eps;
	res.objMode.rps = objMode.rps;
	res.objMode.workers = objMode.workers;
	res.objMode.human = objMode.human;
	console.log('\tSTREAM END');
	console.log('\n\n');

	console.log(`
ANALYSIS:

10 WORKERS; Same Data, Different Formats

JSON:
-----
	- RPS: ${res.JSON.rps}
	- EPS: ${u.comma(res.JSON.eps)}
	- TIME: ${res.JSON.human}


NDJSON:
------
	- RPS: ${res.NDJSON.rps}
	- EPS: ${u.comma(res.NDJSON.eps)}
	- TIME: ${res.NDJSON.human}


OBJECT STREAM:
------
	- RPS: ${res.objMode.rps}
	- EPS: ${u.comma(res.objMode.eps)}
	- TIME: ${res.objMode.human}


NDJSON is ${u.round(100 * ((res.NDJSON.eps - res.JSON.eps) / res.JSON.eps))}% faster than JSON
OBJECT STREAMS are  ${u.round(100 * ((res.objMode.eps - res.NDJSON.eps) / res.NDJSON.eps))}% faster than NDJSON
`);
	return res;
}
