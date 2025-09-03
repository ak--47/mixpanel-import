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

	let hasPassedInCreds = Boolean(creds && Object.keys(creds).length);
	let finalCreds;
	if (hasPassedInCreds) finalCreds = creds;
	else if (isCLI) finalCreds = { ...envVar, ...cli };
	else finalCreds = envVar;

	let hasPassedInOpts = Boolean(opts && Object.keys(opts).length);
	let finalOpts;
	if (hasPassedInOpts) finalOpts = opts;
	else if (isCLI) finalOpts = { ...envVar, ...cli };
	else finalOpts = envVar;

	const job = new importJob(finalCreds, finalOpts);
	// const job = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	if (isCLI) job.verbose = true;
	const l = logger(job);
	if (isCLI) l(cliParams.welcome);
	if (isCLI) global.l = l; // hacky way to make logger available globally
	l(`\n🚀 MIXPANEL IMPORTER\n`);
	
	// Enhanced job creation logging with configuration details
	l(`\n✅ JOB CREATED!\n`);
	if (job.verbose) {
		l(`📋 Configuration Summary:`);
		l(`   • Record Type: ${job.recordType}`);
		l(`   • Region: ${job.region.toUpperCase()}`);
		l(`   • Workers: ${job.workers}`);
		l(`   • Records per Batch: ${u.comma(job.recordsPerBatch)}`);
		l(`   • Stream Format: ${job.streamFormat}`);
		if (job.recordType.includes('export')) {
			l(`   • Export Mode: ${job.where ? (job.where.startsWith('gs://') ? '☁️  GCS' : job.where.startsWith('s3://') ? '☁️  S3' : '💾 Local') : '💾 Local'}`);
			if (job.params && Object.keys(job.params).length > 0) {
				// Format params more cleanly
				const paramCount = Object.keys(job.params).length;
				l(`   • Export Params: ${paramCount} custom parameter${paramCount > 1 ? 's' : ''} configured`);
			}
		}
		if (job.transformFunc) l(`   • Transform: Custom function`);
		if (job.vendor) l(`   • Vendor Transform: ${job.vendor}`);
		l(``);
	}
	await job.init();
	l(`\n📦 DEPS LOADED!\n`);

	// ETL
	l(`\n⚡ ETL STARTED!\n`);
	job.timer.start();

	let stream;
	try {

		stream = await determineDataType(data || cliData, job); // always stream[]
		l(`\n🌊 STREAM CREATED!\n`);
	}
	catch (e) {
		l(`ERROR: Failed to create stream - ${e.message}`);
		if (isCLI) {
			process.exit(1);
		} else {
			throw e;
		}
	}

	try {

		await corePipeline(stream, job)
			.finally(() => {
				l(`\nSTREAM CONSUMED!\n`);
			});
	}

	catch (e) {
		l(`ERROR: ${e.message}`);
		if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
	}

	l('\n');
	if (job.createProfiles)  //job.transform = await createProfiles(job);

		// clean up
		job.timer.end(false);
	const summary = job.summary();
	// Always show completion message for exports (even when not CLI)
	if (job.type === 'export' || isCLI) {
		l(`\n🎉 ${job.type === 'export' ? 'EXPORT' : 'IMPORT'} COMPLETE in ${summary.durationHuman}!\n`);
	}
	
	const stats = {
		total: u.comma(summary.total),
		success: u.comma(summary.success),
		failed: u.comma(summary.failed),
		bytes: summary.bytesHuman,
		requests: u.comma(summary.requests),
		"rate (per sec)": u.comma(summary.eps)
	};

	if (isCLI) {
		l("📊 STATS");
		l(stats, true);
		l('\n');
	}
	
	// For verbose mode, show key info even when not CLI
	if (job.verbose && !isCLI) {
		if (job.type === 'export') {
			l(`📊 ${summary.success} records exported in ${summary.durationHuman}`);
			if (job.where && (job.where.startsWith('gs://') || job.where.startsWith('s3://'))) {
				l(`☁️  Saved to: ${job.where}`);
			}
		} else {
			// Import summary
			l(`📊 ${summary.success} records imported in ${summary.durationHuman}`);
			if (summary.failed > 0) {
				l(`❌ ${summary.failed} records failed`);
			}
			if (summary.duplicates > 0) {
				l(`🔄 ${summary.duplicates} duplicates skipped`);
			}
			l(`⚡ ${summary.eps} events/sec • ${summary.rps} requests/sec`);
		}
		l('');
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


// async function createProfiles(job, record) {
// 	const { groupKey, token } = job;
// 	const profile = {
// 		$token: token,
// 		$ip: 0,
// 		$set: {}
// 	};

// 	if (groupKey) {
// 		profile.$group_key = groupKey;
// 		profile.$group_id = record[groupKey] || record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
// 		profile.$set.id = record[groupKey] || record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
// 	}
// 	if (!groupKey) {
// 		profile.$distinct_id = record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
// 		profile.$set.id = record.distinct_id || record.$distinct_id || record.user_id || record.$user_id;
// 	}

// 	return profile;

// 	job.recordType = 'user';
// 	const copyStream = await determineDataType(data || cliData, job);
// 	const copyPipeline = await corePipeline(copyStream, job);
// };

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
	// Check if --ui flag is present
	const args = cliParams();

	if (args.ui) {
		// Start the web UI
		const { startUI } = require('./ui/server.js');
		startUI().catch((error) => {
			console.error('Failed to start UI:', error.message);
			process.exit(1);
		});
	} else {
		// Regular CLI import
		// @ts-ignore
		main(undefined, undefined, undefined, true).then(() => {
			//noop
		}).catch((e) => {
			console.log('\nUH OH! something went wrong; the error is:\n');
			console.error(e);
			process.exit(1);
		}).finally(() => {
			process.exit(0);
		});
	}
}
