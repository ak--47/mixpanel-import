const yargs = require('yargs');
const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;
const { version } = require('../package.json');
const readline = require('readline');
const u = require('ak-tools');


function cliParams() {
	const args = yargs(process.argv.splice(2))
		.scriptName("mixpanel-import")
		.usage(`${welcome}\n\nusage:\nnpx $0 --yes [file or folder] [options]

ex:
npx  --yes  $0 ./events.ndjson --secret 1234 --format jsonl
npx  --yes  $0 ./pathToData/ --secret 1234 --type user --format json

DOCS: https://github.com/ak--47/mixpanel-import`)
		.command('$0', 'import data to mixpanel', () => { })
		.option("project", {
			demandOption: false,
			describe: 'mixpanel project id',
			type: 'number'
		})
		.option("acct", {
			demandOption: false,
			describe: 'service account username',
			type: 'string'
		})
		.option("pass", {
			demandOption: false,
			describe: 'service account password',
			type: 'string'
		})
		.option("secret", {
			demandOption: false,
			describe: 'project API secret (deprecated auth)',
			type: 'string'
		})
		.option("bearer", {
			demandOption: false,
			describe: 'bearer token (staff auth)',
			type: 'string'
		})
		.option("token", {
			demandOption: false,
			describe: 'project token',
			type: 'string'
		})
		.option("table", {
			demandOption: false,
			describe: "existing lookup table's key",
			type: 'string'
		})
		.option("group", {
			demandOption: false,
			describe: 'the group analytics group key',
			type: 'string'
		})
		.option("type", {
			demandOption: false,
			alias: "recordType",
			default: 'event',
			describe: 'event, user, group, table, export, or peopleExport',
			type: 'string'
		})
		.option("compress", {
			demandOption: false,
			alias: "gzip",
			default: false,
			describe: 'gzip on egress',
			type: 'boolean'
		})
		.option("strict", {
			demandOption: false,
			default: true,
			describe: 'validate data on ingestion',
			type: 'boolean'
		})
		.option("logs", {
			demandOption: false,
			default: true,
			describe: 'log import results to file',
			type: 'boolean'
		})
		.option("where", {
			demandOption: false,
			describe: 'where to put logs + files',
			type: 'string'
		})
		.option("verbose", {
			demandOption: false,
			default: true,
			describe: 'show progress bar',
			type: 'boolean'
		})
		.option("format", {
			demandOption: false,
			alias: 'streamFormat',
			default: 'jsonl',
			describe: 'either json, jsonl, or csv',
			type: 'string'
		})
		.option("stream", {
			alias: "forceStream",
			demandOption: false,
			default: true,
			describe: 'always use streams to load files',
			type: 'boolean'
		})
		.option("streamSize", {
			demandOption: false,
			default: 27,
			describe: '2^n value of highWaterMark',
			type: 'number'
		})
		.option("workers", {
			demandOption: false,
			default: 10,
			describe: 'concurrent connections',
			type: 'number'
		})
		.option("maxRetries", {
			demandOption: false,
			default: 10,
			describe: 'max attempts on 429',
			type: 'number'
		})
		.option("region", {
			demandOption: false,
			default: 'US',
			describe: 'either US or EU',
			type: 'string'
		})
		.option("fixData", {
			demandOption: false,
			default: false,
			describe: 'fix common mistakes',
			type: 'boolean'
		})
		.option("removeNulls", {
			demandOption: false,
			default: false,
			describe: 'remove null values',
			type: 'boolean'
		})
		.option("recordsPerBatch", {
			demandOption: false,
			default: 2000,
			describe: '# records in each request',
			type: 'number'
		})
		.option("bytesPerBatch", {
			demandOption: false,
			default: '2MB',
			describe: 'max size of each request',
			type: 'number'
		})
		.option("start", {
			demandOption: false,
			default: dayjs().subtract(30, 'd').format(dateFormat),
			describe: 'start date (exports)',
			type: 'string'
		})
		.option('timeOffset', {
			demandOption: false,
			default: 0,
			describe: 'add or remove hours from data',
			type: 'number'
		})
		.option("end", {
			demandOption: false,
			default: dayjs().format(dateFormat),
			describe: 'end date (exports)',
			type: 'string'
		})
		.options("tags", {
			demandOption: false,
			default: "{}",
			describe: 'tags to add to each record; {"key": "value"}',
			type: 'string'
		})
		.options("aliases", {
			demandOption: false,
			default: "{}",
			describe: 'rename property keys on each record; {"oldPropKey": "newPropKey"}',
			type: 'string'
		})
		.options("epoch-start", {
			demandOption: false,
			alias: 'epochStart',
			default: 0,
			describe: 'don\'t import data before this timestamp (UNIX EPOCH)',
			type: 'number'
		})
		.options("epoch-end", {
			demandOption: false,
			default: 9991427224,
			alias: 'epochEnd',
			describe: 'don\'t import data after this timestamp (UNIX EPOCH)',
			type: 'number'
		})
		.options("dedupe", {
			demandOption: false,
			default: false,
			describe: 'dedupe records by murmur hash',
			type: 'boolean'
		})
		.options('event-whitelist', {
			demandOption: false,
			default: '[]',
			alias: 'eventWhitelist',
			describe: 'only send events on whitelist',
			type: 'string'
		})
		.options('event-blacklist', {
			demandOption: false,
			default: '[]',
			alias: 'eventBlacklist',
			describe: 'don\'t send events on blacklist',
			type: 'string'
		})
		.options('prop-key-whitelist', {
			demandOption: false,
			default: '[]',
			alias: 'propKeyWhitelist',
			describe: 'only send events with prop keys on whitelist',
			type: 'string'
		})
		.options('prop-key-blacklist', {
			demandOption: false,
			default: '[]',
			alias: 'propKeyBlacklist',
			describe: 'don\'t send events with prop keys on blacklist',
			type: 'string'
		})
		.options('prop-val-whitelist', {
			demandOption: false,
			default: '[]',
			alias: 'propValWhitelist',
			describe: 'only send events with prop values on whitelist',
			type: 'string'
		})
		.options('prop-val-blacklist', {
			demandOption: false,
			default: '[]',
			alias: 'propValBlacklist',
			describe: 'don\'t send events with prop values on blacklist',
			type: 'string'
		})
		.options('dry-run', {
			demandOption: false,
			alias: 'dryRun',
			default: false,
			describe: 'just transform data; don\'t send it',
			type: 'boolean'
		})
		.help()
		.wrap(null)
		.argv;
	// @ts-ignore
	if (args._.length === 0 && !args.type?.toLowerCase()?.includes('export')) {
		yargs.showHelp();
		process.exit();
	}
	return args;
}

const hero = String.raw`
_  _ _ _  _ ___  ____ _  _ ____ _       _ _  _ ___  ____ ____ ___ 
|\/| |  \/  |__] |__| |\ | |___ |       | |\/| |__] |  | |__/  |  
|  | | _/\_ |    |  | | \| |___ |___    | |  | |    |__| |  \  |                                                                    
`;

const banner = `... streamer of data... to mixpanel! (v${version || 2})
\tby AK (ak@mixpanel.com)\n\n`;

const welcome = hero.concat('\n').concat(banner);
cliParams.welcome = welcome;

/** progress bar
 * @param  {string} record
 * @param  {number} processed
 * @param  {number} requests
 */
function showProgress(record, processed, requests) {
	const { rss, heapTotal, heapUsed } = process.memoryUsage();
	const percentHeap = (heapUsed / heapTotal) * 100;
	const percentRSS = (heapUsed / rss) * 100;
	const line = `${record}s: ${u.comma(processed)} | batches: ${u.comma(requests)} | memory: ${u.bytesHuman(heapUsed)} (heap: ${u.round(percentHeap)}% total:${u.round(percentRSS)}%)\t\t`;
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(line);
}

cliParams.showProgress = showProgress;

module.exports = cliParams;