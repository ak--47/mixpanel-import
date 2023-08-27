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

// $ config
const importJob = require('./components/config.js');

// $ parsers
const { determineDataType, getEnvVars } = require("./components/parsers.js");

// $ pipelines
const { corePipeline, pipeInterface } = require('./components/pipelines.js');

// $ env
require('dotenv').config();
const cliParams = require('./components/cli.js');
const { logger, writeLogs } = require('./components/logs.js');

// $ utils
const u = require('ak-tools');

const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);



/*
----
CORE
----
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

	const config = new importJob({ ...envVar, ...cli, ...creds }, { ...envVar, ...cli, ...opts });

	if (isCLI) config.verbose = true;
	const l = logger(config);
	l(cliParams.welcome);
	if (isCLI) global.l = l; // hacky way to make logger available globally

	// ETL
	config.timer.start();

	const stream = await determineDataType(data || cliData, config); // always stream[]

	try {

		await corePipeline(stream, config).finally(() => {
			l(`\n\nFINISHED!\n\n`);
		});
	}

	catch (e) {
		l(`ERROR: ${e.message}`);
		if (e?.response?.body) l(`RESPONSE: ${u.json(e.response.body)}\n`);
	}

	l('\n');

	// clean up
	config.timer.end(false);
	const summary = config.summary();
	l(`${config.type === 'export' ? 'export' : 'import'} complete in ${summary.human}`);
	if (config.logs) await writeLogs(summary);
	return summary;
}


/*
-------
EXPORTS
-------
*/

const mpImport = module.exports = main;
mpImport.createMpStream = pipeInterface;

// this is for CLI
if (require.main === module) {
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
