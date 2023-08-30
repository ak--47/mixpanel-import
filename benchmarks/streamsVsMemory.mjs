import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const DATA = `./benchmarks/testData/dnd250.ndjson`;
const Types = require("../index.js");

// ! CONCLUSION: MEMORY IS FASTER THAN STREAMS (250k @ 12 seconds vs 14 seconds)

export default async function main() {
	/** @type {Types.Options} */
	const opts = {
		logs: false,
		verbose: false,
		streamFormat: 'jsonl'
	};

	const res = {
		stream: {},
		memory: {},
	};

	console.log('STREAM v.s. MEMORY ');

	// JSON V.S. NDJSON
	console.log('\tSTREAM START');
	const streamImport = await mpStream({}, DATA, { ...opts, forceStream: true});
	res.stream.time = streamImport.duration;
	res.stream.eps = streamImport.eps;
	res.stream.rps = streamImport.rps;
	res.stream.workers = streamImport.workers;
	res.stream.human = streamImport.human;
	console.log('\tSTREAM END');
	console.log('\n\n');
	console.log('\tMEMORY START');
	const memoryImport = await mpStream({}, DATA, { ...opts, forceStream: false});
	res.memory.time = memoryImport.duration;
	res.memory.eps = memoryImport.eps;
	res.memory.rps = memoryImport.rps;
	res.memory.workers = memoryImport.workers;
	res.memory.human = memoryImport.human;
	console.log('\tMEMORY END');
	console.log('\n\n');
	return res;
}