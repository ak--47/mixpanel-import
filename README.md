# mixpanel-import

**note:** if you're trying to add real-time mixpanel tracking to a node.js web application - this module is **NOT** what you want; you want **[mixpanel-node](https://github.com/mixpanel/mixpanel-node)** the official node.js SDK.

## wat.

this module is designed for streaming large amounts of event or object data to Mixpanel from a node.js environment; it can backfill millions of events in seconds:

![stream events, users, and groups into mixpanel](https://aktunes.neocities.org/bigImport.gif)

`mixpanel-import` implements Mixpanel's [`/import`](https://developer.mixpanel.com/reference/events#import-events), [`/engage`](https://developer.mixpanel.com/reference/profile-set), [`/groups`](https://developer.mixpanel.com/reference/group-set-property), and [`/lookup`](https://developer.mixpanel.com/reference/replace-lookup-table) APIs, providing an interface to stream JSON (or NDJSON) files compliant with Mixpanel's [data model](https://developer.mixpanel.com/docs/data-structure-deep-dive).

this utility uses streams to provide high-throughput backfills; it's particularly useful for cloud/lambda data pipelines where RETL is not available.

## tldr;

this module can be used in _two ways_:

- **as a CLI**, standalone script via `npx mixpanel-import`
- **as a module** in code via `require('mixpanel-import')`

### CLI usage

```
npx --yes mixpanel-import ./pathToData
```

when running stand-alone, `pathToData` can be a `.json`, `.jsonl`, `.ndjson`, `.csv` or `.txt` file OR a directory which contains said files.

when using the CLI, you can supply params to specify your project credentials:

```
npx --yes mixpanel-import ./data.ndjson --secret abc123
```

many other options are available; to see a full list of CLI params, use the `--help` option:

```
npx --yes mixpanel-import --help
```

alternatively you may use an [`.env` configuration file](#environment-variables) to provide your project credentials.

the CLI will write response logs to a `./logs` directory by default.

### module usage

install `mixpanel-import` as a dependency in your project

```
npm i mixpanel-import --save
```

then use it in code:

```javascript
const mpImport = require("mixpanel-import");
const importedData = await mpImport(credentials, data, options);

console.log(importedData);
/* 

{
	success: 5003,
	failed: 0,
	total: 5003,
	batches: 3,
	recordType: "event",
	duration: 1.299,
	retries: 0,	
	responses: [ ... ],
	errors: [ ... ]    
}

*/
```

read more about [`credentials`](#credentials), [`data`](#data), and [`options`](#options) below

## arguments

when using `mixpanel-import` in code, you will pass in 3 arguments: [`credentials`](#credentials), [`data`](#data), and [`options`](#options)

### credentials

Mixpanel's ingestion APIs authenticate with [service accounts](https://developer.mixpanel.com/reference/service-accounts) OR [API secrets](https://developer.mixpanel.com/reference/authentication#service-account); service accounts are the preferred authentication method.

#### service account:

```javascript
const creds = {
  acct: `my-service-acct`, //service acct username
  pass: `my-service-secret`, //service acct secret
  project: `my-project-id`, //project id
};
const importedData = await mpImport(creds, data, options);
```

#### API secret:

```javascript
const creds = {
  secret: `my-api-secret`, //api secret (deprecated auth)
};
const importedData = await mpImport(creds, data, options);
```

#### profiles + tables:

if you are importing `user` profiles, `group` profiles, or `lookup tables`, you should _also_ provide also provide the corresponding values in your `creds` configuration:

```javascript
const creds = {
		token: `my-project-token`, //for user/group profiles
		groupKey: `my-group-key` //for group profiles
		lookupTableId: `my-lookup-table-id`, //for lookup tables
	}
```

#### environment variables:

it is possible to delegate the authentication details to environment variables, using a `.env` file of the form:

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
```

note: pass `null` as the `creds` to the module to use `.env` variables for authentication:

```javascript
const importedData = await mpImport(null, data, options);
```

### data

the `data` param represents the data you wish to import; this might be [events](https://developer.mixpanel.com/reference/import-events), [user profiles](https://developer.mixpanel.com/reference/profile-set), [group profiles](https://developer.mixpanel.com/reference/group-set-property), or [lookup tables]()

the value of data can be:

- **a path to a _file_**, which contains records as `.json`, `.jsonl`, `.ndjson`, or `.txt`

```javascript
const data = `./myEventsToImport.json`;
const importedData = await mpImport(creds, data, options);
```

- **a path to a _directory_**, which contains files that have records as `.json`, `.jsonl`, `.ndjson`, or `.txt`

```javascript
const data = `./myEventsToImport/`;
const importedData = await mpImport(creds, data, options);
```

- **an array of objects** (records), in memory

```javascript
const data = require("./myEventsToImport.json");
const importedData = await mpImport(creds, data, options);
```

- **a stringified array of objects**, in memory

```javascript
const records = require("./myEventsToImport.json");
const data = JSON.stringify(data);
const importedData = await mpImport(creds, data, options);
```

- **a JSON (or JSONL) readable file stream**

```javascript
const myStream = fs.createReadStream("./testData/lines.json");
const imported = await mpImport(creds, myStream, { streamFormat: `json` });
```

note: please specify `streamFormat` as `json` or `jsonl` in the [options](#options)

- **an "object mode" readable stream**:

```javascript
const { createMpStream } = require('mixpanel-import');
const mixpanelStream = createMpStream(creds, options, (results) => { ... })

const myStream = new Readable.from(data, { objectMode: true });
const myOtherStream = new PassThrough()


myOtherStream.on('data', (response) => { ... });

myStream.pipe(mixpanelStream).pipe(myOtherStream)
```

note: object mode streams use a different import: **`createMpStream()`** ... the `callback` receives a summary of the import and downstream consumers of the stream will receives API responses from Mixpanel.

you will use the **[`options`](#options)** (below) to specify what type of records you are importing; `event` is the default type

### options

`options` is an object that allows you to configure the behavior of this module. you can specify options as the third argument in [module mode](#module-usage) or as flags in [CLI mode](#cli-options).

Below, the default values are given, but you can override them with your own values:

##### module options

```javascript
const options = {
  recordType: `event`, // event, user, group or table
  compress: false, //gzip payload on egress (events only)
  region: `US`, // US or EU
  recordsPerBatch: 2000, // records in each req; max 2000
  bytesPerBatch: 2 * 1024 * 1024, // max bytes in each req
  strict: true, // use strict mode
  logs: false, // write results to a log file
  verbose: true, // show progress bar
  fixData: false, //apply transforms on the data to fix common mistakes
  streamFormat: "jsonl", // json or jsonl ... only relevant for streams

  //will be called on every record
  transformFunc: function noop(a) {
    return a;
  },
};
```

##### cli options

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
```

**note**: the `recordType` param is very important; by default this module assumes you wish to import `event` records.

change this value to `user`, `group`, or `table` if you are importing other entities.

## recipes

the `transformFunc` is useful because it can pre-process records in the pipeline using arbitrary javascript.

here are some examples:

- putting a `token` on every `user` record:

```javascript
function addToken(user) {
  user.token = `{{my token}}`;
  return user;
}

let imported = await mpImport(creds, data, {
  transformFunc: addToken,
  recordType: "user",
});
```

- constructing an `$insert_id` for each event:

```javascript
const md5 = require('md5')

function addInsert(event) {
	let hash = md5(event);
	event.properties.$insert_id = hash;
	return event
}

let imported = await mpImport(creds, data, { transformFunc: addInsert })
```

- reshape/rename profile data with a proper `$set` key and `$distinct_id` value

```javascript
function fixProfiles(user) {
  const mpUser = { $set: { ...user } };
  mpUser.$set.$distinct_id = user.uuid;
  return mpUser  
}

let imported = await mpImport(creds, data, { transformFunc: fixProfiles, recordType: "user"});
```

## test data

sometimes it's helpful to generate test data, so this module includes [a separate utility](https://github.com/ak--47/mixpanel-import/blob/main/generateFakeData.js) to do that:

```bash
$ npm run generate
```

`someTestData.json` will be written to `./testData` ... so you can then `node index.js ./testData/someTestData.json`

## why?

because... i needed this and it didn't exist... so i made it.

then i made it public it because i thought it would be useful to others

found a bug? have an idea? 

[let me know](https://github.com/ak--47/mixpanel-import/issues)
