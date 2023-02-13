/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const u = require('ak-tools');
const Types = require("../types.js");

export default async function main() {
	const JSON = `./benchmarks/testData/dnd250.json`;
	const NDJSON = `./benchmarks/testData/dnd250.ndjson`;
	const CSV = `./benchmarks/testData/dnd.csv`;

	/** @type {Types.Options} */
	const opts = {
		logs: false,
		verbose: false,
	};

	const res = {
		JSON: {},
		NDJSON: {}
	};

	console.log('JSON v.s. NDJSON');

	// JSON V.S. NDJSON
	console.log('\tJSON START');
	const jsonImport = await mpStream({}, JSON, { ...opts, streamFormat: 'json' });
	res.JSON.time = jsonImport.duration;
	res.JSON.eps = jsonImport.eps;
	res.JSON.rps = jsonImport.rps;
	res.JSON.workers = jsonImport.workers;
	res.JSON.human = jsonImport.human;
	console.log('\tJSON END');
	console.log('\n\n')
	console.log('\tNDJSON START');
	const ndJSONImport = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl' });
	res.NDJSON.time = ndJSONImport.duration;
	res.NDJSON.eps = ndJSONImport.eps;
	res.NDJSON.rps = ndJSONImport.rps;
	res.NDJSON.workers = ndJSONImport.workers;
	res.NDJSON.human = ndJSONImport.human;
	console.log('\tNDJSON END');

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

NDJSON is ${u.round(100 * ((res.NDJSON.eps - res.JSON.eps) / res.JSON.eps))}% faster than JSON
`);
	return res;
}
