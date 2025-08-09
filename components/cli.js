const yargs = require('yargs');
const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;
const { version } = require('../package.json');
const readline = require('readline');
const u = require('ak-tools');


function cliParams() {
	// @ts-ignore
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
		.option("secondToken", {
			demandOption: false,
			describe: 'second project token (for export-import-events)',
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
			describe: 'event, user, group, table, export, scd, or profile-export, or export-import-events or export-import-profiles',
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
		.option("whereClause", {
			demandOption: false,
			describe: 'where clause for /export',
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
		.option("water", {
			demandOption: false,
			default: 27,
			describe: '2^n value of highWaterMark',
			type: 'number',
			alias: 'streamSize'
		})
		.option("workers", {
			demandOption: false,
			default: 10,
			describe: 'concurrent connections',
			type: 'number'
		})
		.option("retries", {
			demandOption: false,
			default: 10,
			describe: 'max attempts on 429',
			type: 'number',
			alias: "maxRetries"
		})
		.option("region", {
			demandOption: false,
			default: 'US',
			describe: 'either US, EU, or IN',
			type: 'string'
		})
		.option("fix", {
			demandOption: false,
			default: false,
			describe: 'fix common mistakes',
			type: 'boolean',
			alias: 'fixData'
		})
		.option("createProfiles", {
			demandOption: false,
			default: false,
			describe: 'create profiles for SCD',
			type: 'boolean'
		})
		.option("scdType", {
			demandOption: false,
			default: 'string',
			describe: 'string/number/boolean',
			type: 'string'
		})
		.option("scdKey", {
			demandOption: false,
			default: '',
			describe: 'prop KEY value for SCD',
			type: 'string'
		})
		.option("scdLabel", {
			demandOption: false,
			default: '',
			describe: 'label for SCD',
			type: 'string'
		})
		.option("clean", {
			demandOption: false,
			default: false,
			describe: 'remove null values',
			type: 'boolean',
			alias: 'removeNulls'
		})
		.option("batch", {
			demandOption: false,
			default: 2000,
			describe: '# records in each request',
			type: 'number',
			alias: 'recordsPerBatch'
		})
		.option("bytes", {
			demandOption: false,
			default: '2MB',
			describe: 'max size of each request',
			type: 'number',
			alias: 'bytesPerBatch'
		})
		.option("start", {
			demandOption: false,
			default: dayjs().subtract(30, 'd').format(dateFormat),
			describe: 'start date (exports)',
			type: 'string'
		})
		.option('offset', {
			demandOption: false,
			default: 0,
			describe: 'add or remove hours from data',
			type: 'number',
			alias: 'timeOffset'
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
		.options('scrub-props', {
			demandOption: false,
			default: '[]',
			alias: 'scrubProps',
			describe: 'remove properties from events',
			type: 'string'
		})
		.options('dry-run', {
			demandOption: false,
			alias: 'dryRun',
			default: false,
			describe: 'just transform data; don\'t send it',
			type: 'boolean'
		})
		.options('write-to-file', {
			demandOption: false,
			alias: 'writeToFile',
			default: false,
			describe: 'transform data + write locally',
			type: 'boolean'
		})
		.options('vendor', {
			demandOption: false,
			default: '',
			describe: 'transform amplitude, heap, ga4 data',
			type: 'string'
		})
		.options('vendor-opts', {
			demandOption: false,
			default: "{}",
			alias: 'vendorOpts',
			describe: 'vendor transform options {user_id = ""} ',
			type: 'string'
		})
		.options('flatten', {
			demandOption: false,
			default: false,
			type: 'boolean',
			describe: 'flatten nested objects (properties)',
			alias: 'flattenData'
		})
		.options('fix-json', {
			demandOption: false,
			default: false,
			type: 'boolean',
			describe: 'attempt to fix malformed json',
			alias: 'fixJson'
		})
		.options('add-token', {
			demandOption: false,
			default: false,
			type: 'boolean',
			describe: 'add token to each record',
			alias: 'addToken'
		})
		.options('cohort-id', {
			demandOption: false,
			type: 'number',
			describe: 'cohort id for people exports',
			alias: 'cohortId'
		})
		.options('data-group-id', {
			demandOption: false,
			alias: 'dataGroupId',
			type: 'number',
			describe: 'data group id for group profile exports'
		})
		.option('ui', {
			demandOption: false,
			default: false,
			describe: 'start the web UI for interactive imports',
			type: 'boolean'
		})
		.help()
		.wrap(null)
		.argv;
	// @ts-ignore
	if (args._.length === 0 && !args.type?.toLowerCase()?.includes('export') && !args.ui) {
		// @ts-ignore
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
function showProgress(record = "", processed = 0, requests = 0, eps = "", amountSent = 0) {
	const { heapUsed } = process.memoryUsage();
	// const { rss, heapTotal, heapUsed } = process.memoryUsage();
	// const percentHeap = (heapUsed / heapTotal) * 100;
	// const percentRSS = (heapUsed / rss) * 100;
	let line = `${record}s: ${u.comma(processed)}`;
	if (requests) {
		line += ` | req: ${u.comma(requests)} reqs`;
	}
	if (eps) {
		line += ` | eps: ${u.comma(eps)} `;
	}
	if (heapUsed) {
		line += ` | mem: ${u.bytesHuman(heapUsed)}`;
	}
	if (amountSent) {
		line += ` | proc: ${u.bytesHuman(amountSent)}`;
	}
	// Get the terminal width
	const terminalWidth = process.stdout.columns || 80; // Default to 80 if columns is undefined

	// Pad the line with spaces to fill the terminal width
	if (line.length < terminalWidth) {
		line = line.padEnd(terminalWidth, ' ');
	}
	// @ts-ignore
	readline.cursorTo(process.stdout, 0);
	// @ts-ignore
	readline.clearLine(process.stdout, 0);
	process.stdout.write(line);
}

cliParams.showProgress = showProgress;

module.exports = cliParams;