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
		verbose: false,
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

	console.log('1-200 WORKERS');

	console.log('\t1 worker start!');
	const one = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 1 });
	res.one.time = one.duration;
	res.one.eps = one.eps;
	res.one.rps = one.rps;
	res.one.workers = one.workers;
	res.one.human = one.human;
	console.log('\t1 worker done!');

	console.log('\t5 workers start!');
	const five = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 5 });
	res.five.time = five.duration;
	res.five.eps = five.eps;
	res.five.rps = five.rps;
	res.five.workers = five.workers;
	res.five.human = five.human;
	console.log('\t5 workers done!');

	console.log('\t10 workers start!');
	const ten = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 10 });
	res.ten.time = ten.duration;
	res.ten.eps = ten.eps;
	res.ten.rps = ten.rps;
	res.ten.workers = ten.workers;
	res.ten.human = ten.human;
	console.log('\t10 workers done!');

	console.log('\t25 workers start!');
	const twentyFive = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 25 });
	res.twentyFive.time = twentyFive.duration;
	res.twentyFive.eps = twentyFive.eps;
	res.twentyFive.rps = twentyFive.rps;
	res.twentyFive.workers = twentyFive.workers;
	res.twentyFive.human = twentyFive.human;
	console.log('\t25 workers done!');

	console.log('\t50 workers start!');
	const fifty = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 50 });
	res.fifty.time = fifty.duration;
	res.fifty.eps = fifty.eps;
	res.fifty.rps = fifty.rps;
	res.fifty.workers = fifty.workers;
	res.fifty.human = fifty.human;
	console.log('\t50 workers done!');

	console.log('\t100 workers start!');
	const oneHundred = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 100 });
	res.oneHundred.time = oneHundred.duration;
	res.oneHundred.eps = oneHundred.eps;
	res.oneHundred.rps = oneHundred.rps;
	res.oneHundred.workers = oneHundred.workers;
	res.oneHundred.human = oneHundred.human;
	console.log('\t100 workers done!');


	console.log('\t200 workers start!');
	const twoHundred = await mpStream({}, NDJSON, { ...opts, streamFormat: 'jsonl', workers: 200 });
	res.twoHundred.time = twoHundred.duration;
	res.twoHundred.eps = twoHundred.eps;
	res.twoHundred.rps = twoHundred.rps;
	res.twoHundred.workers = twoHundred.workers;
	res.twoHundred.human = twoHundred.human;
	console.log('\t200 workers done!');
	

	console.log(`
ANALYSIS:

1 - 200 workers

${res.one.workers}:
----
	- RPS: ${res.one.rps}
	- EPS: ${u.comma(res.one.eps)}
	- TIME: ${res.one.human}

${res.five.workers}:
----
	- RPS: ${res.five.rps} (${u.round(100 * ((res.five.rps - res.one.rps) / res.one.rps))}% faster)
	- EPS: ${u.comma(res.five.eps)}  (${u.round(100 * ((res.five.eps - res.one.eps) / res.one.eps))}% faster)
	- TIME: ${res.five.human} (${u.round(-100 * ((res.five.time - res.one.time) / res.one.time))}% faster)

${res.ten.workers}:
----
	- RPS: ${res.ten.rps} (${u.round(100 * ((res.ten.rps - res.five.rps) / res.five.rps))}% faster)
	- EPS: ${u.comma(res.ten.eps)}  (${u.round(100 * ((res.ten.eps - res.five.eps) / res.five.eps))}% faster)
	- TIME: ${res.ten.human} (${u.round(-100 * ((res.ten.time - res.five.time) / res.five.time))}% faster)

${res.twentyFive.workers}:
----
	- RPS: ${res.twentyFive.rps} (${u.round(100 * ((res.twentyFive.rps - res.ten.rps) / res.ten.rps))}% faster)
	- EPS: ${u.comma(res.twentyFive.eps)} (${u.round(100 * ((res.twentyFive.eps - res.ten.eps) / res.ten.eps))}% faster)
	- TIME: ${res.twentyFive.human} (${u.round(-100 * ((res.twentyFive.time - res.ten.time) / res.ten.time))}% faster)

${res.fifty.workers}:
----
	- RPS: ${res.fifty.rps} (${u.round(100 * ((res.fifty.rps - res.twentyFive.rps) / res.twentyFive.rps))}% faster)
	- EPS: ${u.comma(res.fifty.eps)} (${u.round(100 * ((res.fifty.eps - res.twentyFive.eps) / res.twentyFive.eps))}% faster)
	- TIME: ${res.fifty.human} (${u.round(-100 * ((res.fifty.time - res.twentyFive.time) / res.twentyFive.time))}% faster)

${res.oneHundred.workers}:
----
	- RPS: ${res.oneHundred.rps} (${u.round(100 * ((res.oneHundred.rps - res.fifty.rps) / res.fifty.rps))}% faster)
	- EPS: ${u.comma(res.oneHundred.eps)} (${u.round(100 * ((res.oneHundred.eps - res.fifty.eps) / res.fifty.eps))}% faster)
	- TIME: ${res.oneHundred.human} (${u.round(-100 * ((res.oneHundred.time - res.fifty.time) / res.fifty.time))}% faster)

${res.twoHundred.workers}:
----
	- RPS: ${res.twoHundred.rps} (${u.round(100 * ((res.twoHundred.rps - res.oneHundred.rps) / res.oneHundred.rps))}% faster)
	- EPS: ${u.comma(res.twoHundred.eps)} (${u.round(100 * ((res.twoHundred.eps - res.oneHundred.eps) / res.oneHundred.eps))}% faster)
	- TIME: ${res.twoHundred.human} (${u.round(-100 * ((res.twoHundred.time - res.oneHundred.time) / res.oneHundred.time))}% faster)

200 workers is ${u.round(100 * ((res.twoHundred.eps - res.one.eps) / res.one.eps))}% faster than 1 workers
`);


	console.log('1-200 WORKERS END');
	return res;
}
