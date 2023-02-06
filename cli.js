const yargs = require('yargs');
const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;


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
			describe: '/import strict mode',
			type: 'boolean'
		})
		.option("logs", {
			demandOption: false,
			default: true,
			describe: 'log import results to file',
			type: 'boolean'
		})
		.option("verbose", {
			demandOption: false,
			default: true,
			describe: 'show progress bar',
			type: 'boolean'
		})
		.option("streamFormat", {
			demandOption: false,
			alias: 'format',
			default: 'jsonl',
			describe: 'either json or jsonl',
			type: 'string'
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
		.option("streamSize", {
			demandOption: false,
			default: 27,
			describe: '2^n value of highWaterMark',
			type: 'number'
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
			describe: 'start date for exports',
			type: 'string'
		})
		.option("end", {
			demandOption: false,
			default: dayjs().format(dateFormat),
			describe: 'end date for exports',
			type: 'string'
		})	
		.help()
		.argv;
	if (args._.length === 0 && !args.type?.toLowerCase()?.includes('export') ) {
		yargs.showHelp();
		process.exit()
	}
	return args;
}

const hero = String.raw`
_  _ _ _  _ ___  ____ _  _ ____ _       _ _  _ ___  ____ ____ ___ 
|\/| |  \/  |__] |__| |\ | |___ |       | |\/| |__] |  | |__/  |  
|  | | _/\_ |    |  | | \| |___ |___    | |  | |    |__| |  \  |                                                                    
`

const banner = `... streamer of data... to mixpanel! (v${process.env.npm_package_version || 2})
\tby AK (ak@mixpanel.com)`

const welcome = hero.concat('\n').concat(banner)

cliParams.welcome = welcome

module.exports = cliParams