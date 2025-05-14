#! /usr/bin/env node
/* eslint-disable no-unused-vars */

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

	const job = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	if (isCLI) job.verbose = true;
	const l = logger(job);
	if (isCLI) l(cliParams.welcome);
	if (isCLI) global.l = l; // hacky way to make logger available globally

	// ETL
	job.timer.start();

	const stream = await determineDataType(data || cliData, job); // always stream[]

	try {

		await corePipeline(stream, job)
			.finally(() => {
				l(`\n\nFINISHED!\n\n`);
			});
	}

	catch (e) {
		l(`ERROR: ${e.message}`);
		if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
	}

	l('\n');
	if (job.createProfiles) {
		job.transform = function (record) {
			const { groupKey, token } = job;
			const profile = {
				$token: token,
				$ip: 0,
				$set: {}
			};

			if (groupKey) {
				profile.$group_key = groupKey;
				profile.$group_id = record[groupKey] || record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
				profile.$set.id = record[groupKey] || record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
			}
			if (!groupKey) {
				profile.$distinct_id = record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
				profile.$set.id = record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
			}

			return profile;
		};
		job.recordType = 'user';
		const copyStream = await determineDataType(data || cliData, job);
		const copyPipeline = await corePipeline(copyStream, job);


	}

	// clean up
	job.timer.end(false);
	const summary = job.summary();
	if (isCLI) l(`${job.type === 'export' ? 'export' : 'import'} complete in ${summary.durationHuman}\n\n`);
	const stats = {
		total: u.comma(summary.total),
		success: u.comma(summary.success),
		failed: u.comma(summary.failed),
		bytes: summary.bytesHuman,
		requests: u.comma(summary.requests),
		"rate (per sec)": u.comma(summary.eps)
	};

	if (isCLI) {
		l("STATS");
		l(stats, true);
		l('\n');
	}

	if (job.logs) await writeLogs(summary);
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

	// @ts-ignore
	return pipeToMe.toNodeStream();
}


// async function createProfilesFromSCD() {

// }

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
