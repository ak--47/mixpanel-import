//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const u = require('ak-tools');
const Types = require("../index.js");



export default async function main() {
	const NDJSON = `./benchmarks/testData/dnd250.ndjson`;


	/** @type {Types.Options} */
	const opts = {
		logs: false,
		verbose: true,
	};

	const res = {
		one: {},
		five: {},
		ten: {},
		twentyFive: {},
		fifty: {},
		oneHundred: {},
		twoHundred: {},


	};

	console.log('HTTP1 + 2');

	console.log('\tHTTP1');
	const one = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 50, http2: false });
	res.one.time = one.duration;
	res.one.eps = one.eps;
	res.one.rps = one.rps;
	res.one.workers = one.workers;
	res.one.human = one.human;
	console.log('\ttHTTP1 done!');

	console.log('\tHTTP2');
	const five = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 50, http2: true });
	res.five.time = five.duration;
	res.five.eps = five.eps;
	res.five.rps = five.rps;
	res.five.workers = five.workers;
	res.five.human = five.human;
	console.log('\ttHTTP2 done!');

	console.log(`
ANALYSIS:

HTTP + 1

HTTP1:
----
	- RPS: ${res.one.rps}
	- EPS: ${u.comma(res.one.eps)}
	- TIME: ${res.one.human}

HTTP2:
----
	- RPS: ${res.five.rps} (${u.round(100 * ((res.five.rps - res.one.rps) / res.one.rps))}% faster)
	- EPS: ${u.comma(res.five.eps)}  (${u.round(100 * ((res.five.eps - res.one.eps) / res.one.eps))}% faster)
	- TIME: ${res.five.human} (${u.round(-100 * ((res.five.time - res.one.time) / res.one.time))}% faster)
`);


	return res;
}


main()