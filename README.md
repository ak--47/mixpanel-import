# mixpanel-import

## 🤨 wat.

stream data to mixpanel... _quickly_

![stream events, users, and groups into mixpanel](https://aktunes.neocities.org/mp-import.gif)

`mixpanel-import` implements Mixpanel's [`/import`](https://developer.mixpanel.com/reference/events#import-events), [`/engage`](https://developer.mixpanel.com/reference/profile-set), [`/groups`](https://developer.mixpanel.com/reference/group-set-property), and [`/lookup`](https://developer.mixpanel.com/reference/replace-lookup-table) APIs with [best practices](https://developer.mixpanel.com/reference/import-events#rate-limits), providing a clean, configurable interface to stream JSON, NDJSON, or CSV files compliant with Mixpanel's [data model](https://developer.mixpanel.com/docs/data-structure-deep-dive) through Mixpanel's ingestion pipeline.

by implementing interfaces as [streams in node.js](https://nodejs.org/api/stream.html), high-throughput backfills are possible with no intermediate storage and a low memory footprint.

**note:** if you're trying to add real-time mixpanel tracking to a node.js web application - this module is **NOT** what you want; you want **[mixpanel-node](https://github.com/mixpanel/mixpanel-node)** the official node.js SDK.

## 👔 tldr;

this module can be used in _two ways_:

-   **as a [CLI](#cli)**, standalone script via:

```bash
npx mixpanel-import file --options
```

-   **as a [module](#mod)** in code via

```javascript
//for esm:
import mpStream from 'mixpanel-import'
//for cjs:
const mpStream = require('mixpanel-import')

const myImportedData = await mpSteam(creds, data, options)
```

 <div id="cli"></div>

### 💻 CLI usage

```
npx --yes mixpanel-import@latest ./pathToData
```

when running as a CLI, `pathToData` can be:
- a `.json`, `.jsonl`, `.ndjson`, `.csv`, `.parquet`, or `.txt` file
- a **directory** which contains said files
- a **Google Cloud Storage path** like `gs://my-bucket/file.json` 
- an **Amazon S3 path** like `s3://my-bucket/file.json` (requires S3 credentials)

when using the CLI, you will supply params to specify options of the form `--option value`, for example your project credentials:

```
npx --yes mixpanel-import ./data.ndjson --secret abc123
```

many other options are available; to see a full list of CLI params, use the `--help` option:

```
npx --yes mixpanel-import --help
```

alternatively, you may use an [`.env` configuration file](#env) to provide your project credentials (and some other values).

the CLI will write response logs to a `./logs` directory by default. you can specify a `--where dir` option as well if you prefer to put logs elsewhere.

<div id="mod"></div>

### 🔌 module usage

install `mixpanel-import` as a dependency in your project

```
npm i mixpanel-import --save
```

then use it in code:

```javascript
const mpStream = require("mixpanel-import");
const importedData = await mpStream(credentials, data, options);

console.log(importedData);
/*

{
	success: 5003,
	failed: 0,
	total: 5003,
	batches: 3,
	rps: 3,
	eps: 5000,
	recordType: "event",
	duration: 1.299,
	retries: 0,
	responses: [ ... ],
	errors: [ ... ]
}

*/
```

read more about [`credentials`](#creds), [`data`](#data), and [`options`](#opts) below

 <div id="arg"></div>

## 🗣️ arguments

when using `mixpanel-import` in code, you will pass in 3 arguments: [`credentials`](#credentials), [`data`](#data), and [`options`](#opts)

 <div id="creds"></div>

### 🔐 credentials

Mixpanel's ingestion APIs authenticate with [service accounts](https://developer.mixpanel.com/reference/service-accounts) OR [API secrets](https://developer.mixpanel.com/reference/authentication#service-account); service accounts are the preferred authentication method.

 <div id="sa"></div>

#### 🤖 service account:

```javascript
const creds = {
  acct: `my-service-acct`, //service acct username
  pass: `my-service-secret`, //service acct secret
  project: `my-project-id`, //project id
};
const importedData = await mpStream(creds, data, options);
```

 <div id="secret"></div>

#### 🙊 API secret:

```javascript
const creds = {
  secret: `my-api-secret`, //api secret (deprecated auth)
};
const importedData = await mpStream(creds, data, options);
```

 <div id="prof"></div>

#### 🏓 profiles + tables:

if you are importing `user` profiles, `group` profiles, or `lookup tables`, you should _also_ provide also provide the you project `token` and some other values in your `creds` configuration:

```javascript
const creds = {
		token: `my-project-token`, //for user/group profiles
		groupKey: `my-group-key`, //for group profiles
		lookupTableId: `my-lookup-table-id`, //for lookup tables
		
		// for Amazon S3 cloud storage access
		s3Key: `my-s3-access-key`,
		s3Secret: `my-s3-secret-key`, 
		s3Region: `us-east-1`
	}
```

 <div id="env"></div>

#### 🤖 environment variables:

it is possible to delegate the authentication details to environment variables, using a `.env` file under the `MP_` prefix of the form:

```
# if using service account auth; these 3 values are required:
MP_PROJECT={{your-mp-project}}
MP_ACCT={{your-service-acct}}
MP_PASS={{your-service-pass}}

# if using secret based auth; only this value is required
MP_SECRET={{your-api-secret}}

# type of records to import; valid options are event, user, group or table
MP_TYPE=event

# required for user profiles + group profiles
MP_TOKEN={{your-mp-token}}

# required for group profiles
MP_GROUP_KEY={{your-group-key}}

# required for lookup tables
MP_TABLE_ID={{your-lookup-id}}

# required for Amazon S3 cloud storage access
S3_KEY={{your-s3-access-key}}
S3_SECRET={{your-s3-secret-key}}
S3_REGION={{your-s3-region}}
```

note: pass `null` (or `{}`) as the `creds` to the module to use `.env` variables for authentication:

```javascript
const importedData = await mpStream(null, data, options);
```

 <div id="data"></div>

### 📈 data

the `data` param represents the data you wish to import; this might be [events](https://developer.mixpanel.com/reference/import-events), [user profiles](https://developer.mixpanel.com/reference/profile-set), [group profiles](https://developer.mixpanel.com/reference/group-set-property), or [lookup tables](https://developer.mixpanel.com/reference/lookup-tables)

the value of data can be:

-   **a path to a _file_**, which contains records as `.json`, `.jsonl`, `.ndjson`, `.csv`, `.parquet`, or `.txt`

```javascript
const data = `./myEventsToImport.json`;
const importedData = await mpStream(creds, data, options);
```

-   **a path to a _directory_**, which contains files that have records as `.json`, `.jsonl`, `.ndjson`, `.csv`, `.parquet`, or `.txt`

```javascript
const data = `./myEventsToImport/`; //has json files
const importedData = await mpStream(creds, data, options);
```

-   **a list of paths**, which contains files that have records as `.json`, `.jsonl`, `.ndjson`, `.csv`, `.parquet`, or `.txt`

```javascript
const data = [`./file1.jsonl`, `./file2.jsonl`] ; //has json files
const importedData = await mpStream(creds, data, options);
```

-   **a Google Cloud Storage file path** (streaming support for all formats + gzip compression)

```javascript
const data = `gs://my-bucket/events.json`;
// Also supports: .json.gz, .jsonl, .jsonl.gz, .csv, .csv.gz, .parquet, .parquet.gz
const importedData = await mpStream(creds, data, options);
```

-   **an Amazon S3 file path** (streaming support for all formats + gzip compression)

```javascript
const data = `s3://my-bucket/events.json`;
// Also supports: .json.gz, .jsonl, .jsonl.gz, .csv, .csv.gz, .parquet, .parquet.gz
const importedData = await mpStream(creds, data, {
  ...options,
  s3Key: 'YOUR_ACCESS_KEY',
  s3Secret: 'YOUR_SECRET_KEY',
  s3Region: 'us-east-1'
});
```

-   **multiple cloud storage files** (mix and match is not supported - all files must be from same provider)

```javascript
const data = [
  `gs://my-bucket/events1.json`, 
  `gs://my-bucket/events2.json`
];
// or for S3:
const data = [
  `s3://my-bucket/events1.json`, 
  `s3://my-bucket/events2.json`
];
const importedData = await mpStream(creds, data, options);
```

-   **an array of objects** (records), in memory

```javascript
const data = [{event: "foo"}, {event: "bar"}, {event: "baz"}]
const importedData = await mpStream(creds, data, options);
```

-   **a stringified array of objects**, in memory

```javascript
const records = [{event: "foo"}, {event: "bar"}, {event: "baz"}]
const data = JSON.stringify(data);
const importedData = await mpStream(creds, data, options);
```

-   **a JSON (or JSONL) readable file stream**

```javascript
const myStream = fs.createReadStream("./myData/lines.json");
const imported = await mpStream(creds, myStream, { streamFormat: `json` });
```

note: please specify `streamFormat` as `json` or `jsonl` in the [options](#options)

-   **an "object mode" readable stream**:

```javascript
const { createMpStream } = require('mixpanel-import');
const mixpanelStream = createMpStream(creds, options, (results) => { ... })

const myStream = new Readable.from(data, { objectMode: true });
const myOtherStream = new PassThrough()


myOtherStream.on('data', (response) => { ... });

myStream.pipe(mixpanelStream).pipe(myOtherStream)
```

note: object mode streams use a different **named** import: **`createMpStream()`** ... the `callback` receives a summary of the import and downstream consumers of the stream will receives API responses from Mixpanel.

you will use the **[`options`](#opts)** (below) to specify what type of records you are importing; `event` is the default type

 <div id="opts"></div>

### 🎛 options

`options` is an object that allows you to configure the behavior of this module. there are LOTS of options for different types of import use cases. you can specify options as the third argument in [module mode](#mod) or as flags in [CLI mode](#cliOpt).



 <div id="modOpt"></div>

##### module options

all options are... optional... for a full list of what these do, see [the type definition](https://github.com/ak--47/mixpanel-import/blob/main/index.d.ts#L78-L81)

```typescript
export type Options = {
	recordType?: RecordType;
	vendor?: "amplitude" | "heap" | "mixpanel" | "ga4" | "adobe" | "pendo" | "mparticle" | "posthog"
	region?: Regions;
	streamFormat?: SupportedFormats;
	compress?: boolean;
	compressionLevel?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;
	strict?: boolean;
	logs?: boolean;
	verbose?: boolean;
	fixData?: boolean;
	removeNulls?: boolean;
	abridged?: boolean;
	forceStream?: boolean;
	streamSize?: number;
	timeOffset?: number;
	recordsPerBatch?: number;
	bytesPerBatch?: number;
	maxRetries?: number;
	workers?: number;
	where?: string;
	transformFunc?: transFunc;
	parseErrorHandler?: transFunc;
	tags?: genericObj;
	aliases?: genericObj;
	epochStart?: number;
	epochEnd?: number;
	dedupe?: boolean;
	eventWhitelist?: string[];
	eventBlacklist?: string[];
	propKeyWhitelist?: string[];
	propKeyBlacklist?: string[];
	propValWhitelist?: string[];
	propValBlacklist?: string[];
	start?: string;
	end?: string;
	maxRecords?: number;
};
```

 <div id="cliOpt"></div>

##### cli options

use `npx mixpanel-import --help` to see the full list.

```
option, alias			description		default
----------------------------------------------------------------
  --type, --recordType      event/user/group/table	"event"
  --compress, --gzip        gzip on egress             	false
  --strict                  /import strict mode         true
  --logs                    log import results to file  true
  --verbose                 show progress bar           true
  --streamFormat, --format  either json or jsonl     	"jsonl"
  --region                  either US or EU             "US"
  --fixData                 fix common mistakes        	false
  --streamSize              2^n value of highWaterMark  27
  --recordsPerBatch         # records in each request   2000
  --bytesPerBatch           max size of each request    2MB
  --maxRecords              max records to process      null
  --where                   directory to put logs
```

**note**: the `recordType` param is very important; by default this module assumes you wish to import `event` records.

**added in 2.5.20**: you can now specify certain `vendor`'s in the options like `amplitude` or `ga4` and `mixpanel-import` will provide the correct transform on the source data to bring it into mixpanel.

**`maxRecords` parameter**: when set to a number, the import will stop processing after reaching that many records. This is particularly useful for testing transforms and configurations on large datasets without processing the entire file. For example, `maxRecords: 1000` will stop after processing 1000 records. When null (default), all records will be processed.

change this value to `user`, `group`, or `table` if you are importing other entities.

<div id="rec"></div>

## 👨‍🍳️ recipes

the `transformFunc` is useful because it can pre-process records in the pipeline using arbitrary javascript.

here are some examples:

-   putting a `token` on every `user` record:

```javascript
function addToken(user) {
  user.token = `{{my token}}`;
  return user;
}

const imported = await mpStream(creds, data, {
  transformFunc: addToken,
  recordType: "user",
});
```

-   constructing an `$insert_id` for each event:

```javascript
const md5 = require('md5')

function addInsert(event) {
	const hash = md5(event);
	event.properties.$insert_id = hash;
	return event
}

const imported = await mpStream(creds, data, { transformFunc: addInsert })
```

-   reshape/rename profile data with a proper `$set` key and `$distinct_id` value

```javascript
function fixProfiles(user) {
  const mpUser = { $set: { ...user } };
  mpUser.$set.$distinct_id = user.uuid;
  return mpUser
}

const imported = await mpStream(creds, data, { transformFunc: fixProfiles, recordType: "user"});
```

-   only bringing in certain events; by returning `{}` from the `transformFunc`, results will be omitted

```javascript
function onlyProps(event) {
	if (!event.properties) return {}; //don't send events without props
	return event;
}
const data = [{ event: "foo" }, {event: "bar"}, {event: "baz", properties: {}}]
const imported = await mpStream(creds, data, { transformFunc: onlyProps }); //imports only one event
```

-   "exploding" single events into many; by returning an `[]` from the `transformFunc`, each item will be treated as a new record

```javascript
const data = [{ event: false }, {event: "foo"}]

// turns "false" event into 100 events
function exploder(o) => {
	if (!o.event) {
		const results = [];
		const template = { event: "explode!" };
		for (var i = 0; i < 100; i++) {
			results.push(template);
		}
		return results;
	}
	return o;
};

const imported = await mpStream(creds, data, { transformFunc: exploder }) //imports 101 events
```

-   importing a CSV file of events using `aliases` to identify the correct mixpanel fields:

```javascript
const eventsCSV = './myEvents.csv'
/*
myEvents.csv looks like this:
row_id,uuid,timestamp,action,colorTheme,luckyNumber
a50b0a01b9df43e74707afb679132452aee00a1f,7e1dd089-8773-5fc9-a3bc-37ba5f186ffe,2023-05-15 09:57:44,button_click,yellow,43
09735b6f19fe5ee7be5cd5df59836e7165021374,7e1dd089-8773-5fc9-a3bc-37ba5f186ffe,2023-06-13 12:11:12,button_click,orange,7
*/
const imported = await mpStream(creds, eventsCSV, {
	streamFormat: "csv",
	aliases: {
			row_id: "$insert_id",
			uuid: "distinct_id",
			action: "event",
			timestamp: "time"
		}
	}
);
```

<div id="testData"></div>

## ⚗️ test data

sometimes it's helpful to generate test data, so this module includes [a separate utility](https://github.com/ak--47/mixpanel-import/blob/main/generateFakeData.js) to do that:

```bash
$ npm run generate
```

`someTestData.json` will be written to `./testData` ... so you can then `node index.js ./testData/someTestData.json`

## 🤷 why?

because... i needed this and it didn't exist... so i made it.

then i made it public it because i thought it would be useful to others. then it was, so i made some improvements.

found a bug? have an idea?

[let me know](https://github.com/ak--47/mixpanel-import/issues)
