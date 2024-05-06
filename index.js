#! /usr/bin/env node

/*
----
MIXPANEL IMPORT
by AK
purpose: stream events, users, groups, tables into mixpanel... with best practices!
----
*/


/*
-----
DEPS
-----
*/

// $ job configuration
const importJob = require('./components/job.js');

// $ parsers
const { determineDataType, getEnvVars } = require("./components/parsers.js");

// $ pipelines
const { corePipeline } = require('./components/pipelines.js');

// $ env
require('dotenv').config();
const cliParams = require('./components/cli.js');
const { logger, writeLogs } = require('./components/logs.js');

// $ utils
const u = require('ak-tools');

// $ validators
const { validateToken } = require('./components/validators.js');

const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);

/** @typedef {import('./index.d.ts').Creds} Creds */
/** @typedef {import('./index.d.ts').Data} Data */
/** @typedef {import('./index.d.ts').Options} Options */
/** @typedef {import('./index.d.ts').ImportResults} ImportResults */


/*
----
CORE
----
*/


/**
 * Mixpanel Importer
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * const mp = require('mixpanel-import')
 * const imported = await mp(creds, data, options)
 * @param {Creds} creds - mixpanel project credentials
 * @param {Data} data - data to import
 * @param {Options} opts - import options
 * @param {boolean} isCLI - `true` when run as CLI
 * @returns {Promise<ImportResults>} API receipts of imported data
 */
async function main(creds = {}, data, opts = {}, isCLI = false) {
	let cliData = {};

	// gathering params
	const envVar = getEnvVars();
	let cli = {};
	if (isCLI) {
		cli = cliParams();
		cliData = cli._[0];
	}

	const jobConfig = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	if (isCLI) jobConfig.verbose = true;
	const l = logger(jobConfig);
	l(cliParams.welcome);
	if (isCLI) global.l = l; // hacky way to make logger available globally

	// ETL
	jobConfig.timer.start();

	const stream = await determineDataType(data || cliData, jobConfig); // always stream[]

	try {

		await corePipeline(stream, jobConfig).finally(() => {
			l(`\n\nFINISHED!\n\n`);
		});
	}

	catch (e) {
		l(`ERROR: ${e.message}`);
		if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
	}

	l('\n');

	// clean up
	jobConfig.timer.end(false);
	const summary = jobConfig.summary();
	l(`${jobConfig.type === 'export' ? 'export' : 'import'} complete in ${summary.durationHuman}\n\n`);
	const stats = {
		total: u.comma(summary.total),
		success: u.comma(summary.success),
		failed: u.comma(summary.failed),
		bytes: summary.bytesHuman,
		requests: u.comma(summary.requests),
		"rate (per sec)": u.comma(summary.eps)
	};

	l("STATS");
	l(stats, true);
	l('\n');

	if (jobConfig.logs) await writeLogs(summary);
	return summary;
}



/**
 * Mixpanel Streamer
 * this function returns a transform stream that takes in data and streams it to mixpanel
 * @param {Creds} creds - mixpanel project credentials
 * @param {Options} opts - import options
 * @param {function(): importJob | void} finish - end of pipelines
 * @returns a transform stream
 */
function pipeInterface(creds = {}, opts = {}, finish = () => { }) {
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	config.timer.start();

	const pipeToMe = corePipeline(null, config, true);

	// * handlers
	// @ts-ignore
	pipeToMe.on('end', () => {
		config.timer.end(false);

		// @ts-ignore
		finish(null, config.summary());
	});

	// @ts-ignore
	pipeToMe.on('pipe', () => {

		// @ts-ignore
		pipeToMe.resume();
	});

	// @ts-ignore
	pipeToMe.on('error', (e) => {
		if (config.verbose) {
			console.log(e);
		}
		// @ts-ignore
		finish(e, config.summary());
	});

	return pipeToMe;
}


/*
-------
EXPORTS
-------
*/

main.validateToken = validateToken;
const mpImport = module.exports = main;
mpImport.createMpStream = pipeInterface;

// this is for CLI
if (require.main === module) {
	// @ts-ignore
	main(undefined, undefined, undefined, true).then(() => {
		//noop
	}).catch((e) => {
		console.log('\n\nUH OH! something went wrong; the error is:\n\n');
		console.error(e);
		process.exit(1);
	}).finally(() => {
		process.exit(0);
	});
}
