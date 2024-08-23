import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const NORMAL = require('../index.js');
const DATA = `./benchmarks/testData/dnd250.ndjson`;
const FAST = require('../components/fastMode.js');
const TOKEN = process.env.MP_TOKEN;


/** @type {import('../index.d.ts').Options} */
const opts = {
	logs: false,
	verbose: true,
	streamFormat: 'jsonl',

};

console.log('NORMAL v.s FAST MODE');

console.log('\nNORMAL START');
const slowMode = await NORMAL({ token: TOKEN }, DATA, { ...opts, forceStream: false, verbose: true, workers: 50 });
console.log('\nNORMAL END');
console.log('\n\n');

console.log('\nFAST START');
const fastMode = await FAST({ token: TOKEN }, DATA, {verbose: true, concurrency: 50})
console.log('\nFAST END');
console.log('\n\n');


debugger;