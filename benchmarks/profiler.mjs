import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const DATA = `./benchmarks/testData/dnd250.ndjson`;
const Types = require("../index.js");

// ! CONCLUSION: ????

export default async function main() {
	/** @type {Types.Options} */
	const opts = {
		logs: false,
		verbose: true,
		streamFormat: 'jsonl'
	};

	const res = {
		
	};

	console.log('\tSTART');
	const streamImport = await mpStream({}, DATA, { ...opts, forceStream: false});
	res.time = streamImport.duration;
	res.eps = streamImport.eps;
	res.rps = streamImport.rps;
	res.workers = streamImport.workers;
	res.human = streamImport.human;
	console.log('\tEND');

	return res;

}