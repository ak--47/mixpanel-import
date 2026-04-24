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
 * @function main
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
	
	// Job created
	l(`\n✅ JOB CREATED!\n`);
	if (job.verbose) {
		l(`╔════════════════════════════════════════════════════════════════╗`);
		l(`║                     CONFIGURATION SUMMARY                       ║`);
		l(`╠════════════════════════════════════════════════════════════════╣`);
		l(`║ Pipeline Configuration:                                         ║`);
		l(`║   • Record Type: ${job.recordType.padEnd(46)}║`);
		l(`║   • Region: ${job.region.toUpperCase().padEnd(51)}║`);
		l(`║   • Stream Format: ${job.streamFormat.padEnd(44)}║`);
		l(`║                                                                  ║`);
		l(`║ Performance Settings:                                           ║`);
		l(`║   • Workers: ${String(job.workers).padEnd(51)}║`);
		l(`║   • High Water Mark: ${String(job.highWater).padEnd(43)}║`);
		l(`║   • Records per Batch: ${u.comma(job.recordsPerBatch).padEnd(41)}║`);
		l(`║   • Bytes per Batch: ${u.bytesHuman(job.bytesPerBatch).padEnd(43)}║`);
		l(`║   • Compression: ${(job.compress ? 'Enabled' : 'Disabled').padEnd(47)}║`);

		// Data processing options
		if (job.vendor || job.transformFunc || job.fixData || job.fixTime || job.removeNulls || job.matchMixpanelDefaults) {
			l(`║                                                                  ║`);
			l(`║ Data Processing:                                                ║`);
			if (job.vendor) {
				const vendorText = `${job.vendor.toUpperCase()} vendor transform`;
				l(`║   • Vendor: ${vendorText.padEnd(51)}║`);
			}
			if (job.transformFunc) l(`║   • Custom Transform: Enabled                                   ║`);
			if (job.fixData) l(`║   • Fix Data: Enabled                                           ║`);
			if (job.fixTime) l(`║   • Fix Time: Enabled                                           ║`);
			if (job.removeNulls) l(`║   • Remove Nulls: Enabled                                       ║`);
			if (job.matchMixpanelDefaults) l(`║   • Match MP Defaults: Enabled                                  ║`);
			if (job.dedupe) l(`║   • Deduplication: Enabled                                      ║`);
		}

		// Export specific settings
		if (job.recordType.includes('export')) {
			l(`║                                                                  ║`);
			l(`║ Export Settings:                                                ║`);
			l(`║   • Export Mode: ${(job.where ? (job.where.startsWith('gs://') ? '☁️  GCS' : job.where.startsWith('s3://') ? '☁️  S3' : '💾 Local') : '💾 Local').padEnd(47)}║`);
			if (job.params && Object.keys(job.params).length > 0) {
				const paramCount = Object.keys(job.params).length;
				l(`║   • Export Params: ${(String(paramCount) + ' parameter' + (paramCount > 1 ? 's' : '')).padEnd(45)}║`);
			}
		}

		// Memory management
		if (job.throttleGCS || job.throttleMemory || job.aggressiveGC) {
			l(`║                                                                  ║`);
			l(`║ Memory Management:                                              ║`);
			if (job.throttleGCS || job.throttleMemory) {
				l(`║   • Memory Throttling: Enabled                                  ║`);
				l(`║     - Pause at: ${u.bytesHuman((job.throttlePauseMB || 1500) * 1024 * 1024).padEnd(48)}║`);
				l(`║     - Resume at: ${u.bytesHuman((job.throttleResumeMB || 1000) * 1024 * 1024).padEnd(47)}║`);
			}
			if (job.aggressiveGC) l(`║   • Aggressive GC: Enabled (periodic + emergency)               ║`);
		}

		l(`╚════════════════════════════════════════════════════════════════╝`);
		l(``);
	}
	await job.init();
	l(`\n📦 DEPS LOADED!\n`);

	// ETL
	l(`\n⚡ ETL STARTED!\n`);
	// Timer is auto-started in Job constructor

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

		// Re-throw the error so tests and non-CLI usage can catch it
		if (!isCLI) {
			throw e;
		}
		// For CLI, we continue to show the summary even on error
	}

	l('\n');
	if (job.createProfiles)  //job.transform = await createProfiles(job);

	// clean up - stop timer before getting summary
	job.timer.stop(false);
	const summary = job.summary();
	// Always show completion message for exports (even when not CLI)
	const isExport = job.recordType && job.recordType.includes('export');
	if (isExport || isCLI) {
		l(`\n🎉 ${isExport ? 'EXPORT' : 'IMPORT'} COMPLETE in ${summary.durationHuman}!\n`);
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
		if (job.recordType && job.recordType.includes('export')) {
			l(`📊 ${u.comma(summary.success)} records exported in ${summary.durationHuman}`);
			if (job.where && (job.where.startsWith('gs://') || job.where.startsWith('s3://'))) {
				l(`☁️  Saved to: ${job.where}`);
			}
		} else {
			// Import summary
			l(`📊 ${u.comma(summary.success)} records imported in ${summary.durationHuman}`);
			if (summary.failed > 0) {
				l(`❌ ${u.comma(summary.failed)} records failed`);
			}
			if (summary.duplicates > 0) {
				l(`🔄 ${u.comma(summary.duplicates)} duplicates skipped`);
			}
			const rpsValue = summary.rps ? summary.rps.toFixed(2) : '0.00';
			l(`⚡ ${u.comma(summary.eps || 0)} events/sec • ${rpsValue} requests/sec`);
		}
		l('');
	}

	if (job.logs) {
		const logPath = await writeLogs(summary);
		if (logPath && isCLI) {
			l(`📝 Log saved: ${logPath}`);
			// Output just the path on the last line for tests
			console.log(logPath);
		}
	}
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
async function pipeInterface(creds = {}, opts = {}, finish = () => { }) {
	const envVar = getEnvVars();
	const config = new importJob({ ...envVar, ...creds }, { ...envVar, ...opts });
	// Timer is auto-started in Job constructor

	const pipeToMe = await corePipeline(null, config, true);

	// * handlers
	// @ts-ignore
	pipeToMe.on('finish', () => {
		config.timer.stop(false);

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

	// Return the native stream directly
	return pipeToMe;
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

	// @ts-ignore
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
