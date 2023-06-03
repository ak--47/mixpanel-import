# mixpanel-import

## 🤨 wat.

create data streams to mixpanel... _quickly_


![stream events, users, and groups into mixpanel](https://aktunes.neocities.org/mp-import.gif)

`mixpanel-import` implements Mixpanel's [`/import`](https://developer.mixpanel.com/reference/events#import-events), [`/engage`](https://developer.mixpanel.com/reference/profile-set), [`/groups`](https://developer.mixpanel.com/reference/group-set-property), and [`/lookup`](https://developer.mixpanel.com/reference/replace-lookup-table) APIs with [best practices](https://developer.mixpanel.com/reference/import-events#rate-limits), providing a clean, configurable interface to stream JSON (or NDJSON) files compliant with Mixpanel's [data model](https://developer.mixpanel.com/docs/data-structure-deep-dive).

by implementing all interfaces as [streams in node.js](https://nodejs.org/api/stream.html), high-throughput backfills are possible with no intermediate storage and a low memory footprint.

**note:** if you're trying to add real-time mixpanel tracking to a node.js web application - this module is **NOT** what you want; you want **[mixpanel-node](https://github.com/mixpanel/mixpanel-node)** the official node.js SDK.

## 👔 tldr;

this module can be used in _two ways_:

- **as a [CLI](#cli)**, standalone script via: 
```bash
npx mixpanel-import file --options
```
- **as a [module](#mod)** in code via 
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

when running as a CLI, `pathToData` can be a `.json`, `.jsonl`, `.ndjson`, `.csv` or `.txt` file OR a **directory** which contains said files.

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
```

note: pass `null` (or `{}`) as the `creds` to the module to use `.env` variables for authentication:

```javascript
const importedData = await mpStream(null, data, options);
```

 <div id="data"></div>

### 📈 data

the `data` param represents the data you wish to import; this might be [events](https://developer.mixpanel.com/reference/import-events), [user profiles](https://developer.mixpanel.com/reference/profile-set), [group profiles](https://developer.mixpanel.com/reference/group-set-property), or [lookup tables](https://developer.mixpanel.com/reference/lookup-tables)

the value of data can be:

- **a path to a _file_**, which contains records as `.json`, `.jsonl`, `.ndjson`, or `.txt`

```javascript
const data = `./myEventsToImport.json`;
const importedData = await mpStream(creds, data, options);
```

- **a path to a _directory_**, which contains files that have records as `.json`, `.jsonl`, `.ndjson`, or `.txt`

```javascript
const data = `./myEventsToImport/`; //has json files
const importedData = await mpStream(creds, data, options);
```

- **an array of objects** (records), in memory

```javascript
const data = [{event: "foo"}, {event: "bar"}, {event: "baz"}]
const importedData = await mpStream(creds, data, options);
```

- **a stringified array of objects**, in memory

```javascript
const records = [{event: "foo"}, {event: "bar"}, {event: "baz"}]
const data = JSON.stringify(data);
const importedData = await mpStream(creds, data, options);
```

- **a JSON (or JSONL) readable file stream**

```javascript
const myStream = fs.createReadStream("./myData/lines.json");
const imported = await mpStream(creds, myStream, { streamFormat: `json` });
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

note: object mode streams use a different **named** import: **`createMpStream()`** ... the `callback` receives a summary of the import and downstream consumers of the stream will receives API responses from Mixpanel.

you will use the **[`options`](#opts)** (below) to specify what type of records you are importing; `event` is the default type

 <div id="opts"></div>

### 🎛 options

`options` is an object that allows you to configure the behavior of this module. you can specify options as the third argument in [module mode](#mod) or as flags in [CLI mode](#cliOpt).

Below, the default values are given, but you can override them with your own values:

 <div id="modOpt"></div>

##### module options

all options are... optional... for a full list of what these do, see [the type definition](https://github.com/ak--47/mixpanel-import/blob/main/index.d.ts#L78-L81)

```typescript
export type Options = {
    recordType?: RecordType;
    region?: "US" | "EU";
    streamFormat?: "json" | "jsonl";
    compress?: boolean;
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
    transformFunc?: transFunc; // called on every record
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
  --where                   directory to put logs
```

**note**: the `recordType` param is very important; by default this module assumes you wish to import `event` records.

change this value to `user`, `group`, or `table` if you are importing other entities.

<div id="rec"></div>

## 👨‍🍳️ recipes

the `transformFunc` is useful because it can pre-process records in the pipeline using arbitrary javascript.

here are some examples:

- putting a `token` on every `user` record:

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

- constructing an `$insert_id` for each event:

```javascript
const md5 = require('md5')

function addInsert(event) {
	const hash = md5(event);
	event.properties.$insert_id = hash;
	return event
}

const imported = await mpStream(creds, data, { transformFunc: addInsert })
```

- reshape/rename profile data with a proper `$set` key and `$distinct_id` value

```javascript
function fixProfiles(user) {
  const mpUser = { $set: { ...user } };
  mpUser.$set.$distinct_id = user.uuid;
  return mpUser  
}

const imported = await mpStream(creds, data, { transformFunc: fixProfiles, recordType: "user"});
```

- only bringing in certain events; by returning `{}` from the `transformFunc`, results will be omitted

```javascript
function onlyProps(event) {
	if (!event.properties) return {}; //don't send events without props
	return event;
}
const data = [{ event: "foo" }, {event: "bar"}, {event: "baz", properties: {}}]
const imported = await mpStream(creds, data, { transformFunc: onlyProps }); //imports only one event
```

- "exploding" single events into many; by returning an `[]` from the `transformFunc`, each item will be treated as a new record

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
