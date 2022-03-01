
  

# mixpanel-import
**note:** if you're trying to add real-time tracking mixpanel to a node.js web application - this module is **NOT** what you want; you want **[mixpanel-node](https://github.com/mixpanel/mixpanel-node)** the official node.js SDK.
    
## wat.

![stream events, users, and groups into mixpanel](https://aktunes.neocities.org/mixpanel-import.gif)

This module is designed for streaming large amounts of event or object data to Mixpanel from a node.js environment. It implements the  [`/import`](https://developer.mixpanel.com/reference/events#import-events),  [`/engage`](https://developer.mixpanel.com/reference/profile-set), and [`/groups`](https://developer.mixpanel.com/reference/group-set-property) APIs by streaming JSON files that are compliant with Mixpanel's [data model](https://developer.mixpanel.com/docs/data-structure-deep-dive).

This utility is particularly useful for running one-time backfills or streaming data into Mixpanel from cloud-based data pipelines where RETL is not available.
  

## tldr;

 this module can be used in *two ways*: 
- as a module via `require('mixpanel-import')`
- as a standalone script via `npx mixpanel-import`

### module usage
install `mixpanel-import` as a dependency
```
npm i mixpanel-import --save
```

use it in code:
```
const mpImport  =  require('mixpanel-import') 
	...
const importedData = await mp(credentials, data, options);
console.log(importedData) // array of responses from Mixpanel
```

read more about [`credentials`](#credentials), [`data`](#data), and [`options`](#options) 

### stand-alone usage
clone the module:
```
git clone https://github.com/ak--47/mixpanel-import.git
```
run it and providing a path to the data you wish to import:
```
$ node index.js ./pathToData
```
when running stand-alone, `pathToData` can be a `.json`, `.jsonl`, `.ndjson`, or `.txt` file OR a directory which contains said files.

you will also need a [`.env` configuration file](#environment-variables)
 
 ## arguments

when using `mixpanel-import` in code, you will pass in 3 arguments:  [`credentials`](#credentials), [`data`](#data), and [`options`](#options) 

### credentials
mixpanel's ingestion APIs authenticate with [service accounts](https://developer.mixpanel.com/reference/service-accounts) OR [API secrets](https://developer.mixpanel.com/reference/authentication#service-account); service accounts are the preferred authentication method.

#### service account:
```javascript
const creds = {
	acct: `{{my-servce-acct}}`, //service acct username
	pass: `{{my-service-seccret}}`, //service acct secret
	project: `{{my-project-id}}`, //project id
	token: `{{my-project-token}}`  //project token
}
const importedData = await mpImport(creds, data, options);
```
#### API secret:
```javascript
const creds = {
	secret: `{{my-api-secret}}`, //api secret (deprecated auth)
	token: `{{my-project-token}}`  //project token
}
const importedData = await mpImport(creds, data, options);
```

#### environment variables:
note: it is possible to delegate the authentication details to environment variables, using a `.env` file of the form:

```
# if using service account auth; these 3 values are required:
MP_PROJECT={{your-mp-project}}
MP_ACCT={{your-service-acct}}
MP_PASS={{your-service-pass}}

# if using secret based auth; only this value is required
MP_SECRET={{your-api-secret}}

# this is optional (but strongly encouraged)
MP_TOKEN={{your-mp-token}}
```

if using environment variables for authentication, pass `null` as the `creds` (first argument) to the module:

```javascript
const importedData = await mpImport(null, data, options);
```

### data
the `data` param represents the data you wish to import; this might be [events](https://developer.mixpanel.com/reference/import-events), [user profiles](https://developer.mixpanel.com/reference/profile-set), or [group profiles](https://developer.mixpanel.com/reference/group-set-property)

the value of data can be:

- a path to a _file_, which contains records as `.json`, `.jsonl`, `.ndjson`, or `.txt`
```javascript
const data = `./myEventsToImport.json`
const importedData = await mpImport(creds, data, options);
```
- a path to a _directory_, which contains files that have records as `.json`, `.jsonl`, `.ndjson`, or `.txt`	
```javascript
const data = `./myEventsToImport/`
const importedData = await mpImport(creds, data, options);
```
 - an array of objects (records), in memory
```javascript
const data = require('./myEventsToImport.json')
const importedData = await mpImport(creds, data, options);
```
 - a  stringified array of objects
```javascript
const records = require('./myEventsToImport.json')
const data = JSON.stringify(data)
const importedData = await mpImport(creds, data, options);
```
**important note**: you will use the  [`options`](#options) (below) to specify what type of records you are importing

### options
`options` is an object that allows you to configure the behavior of this module. 

Below, the default values are given, but you can override them with you own value:

```javascript
const options = {
	recordType: `event`, //event, user, OR group
	streamSize: 27, // highWaterMark for streaming chunks (2^27 ~= 134MB)
	region: `US`, //US or EU
	recordsPerBatch: 2000, //max # of records in each batch
	bytesPerBatch: 2 * 1024 * 1024, //max # of bytes in each batch
	strict: true, //use strict mode?
	logs: false, //print to stdout?

	//a function reference to be called on every record
	//useful if you need to transform the data
	transformFunc: function noop(a) { return a }
}
```
**note**: the `recordType` param is very important; by default this module assumes you wish to import `event` records but change this value to `user` or `group` if you are importing other entities.

## recipies
the `transformFunc` is useful because it can preprocess the records using arbitrary javascript. here are some examples:

- putting a `token` on every `user` record:
```javascript
function addToken(user) {
	user.token = `{{my token}}`
	return user
}

let res = await mpImport(creds, data, { transformFunc: addToken, recordType: 'user' })
```
- constructing an `$insert_id` for each event:

```javascript
const md5 = require('md5')

function addInsert(event) {
	let hash = md5(event);
	event.properties.$insert_id = hash;
	return event
}
let res = await mpImport(creds, data, { transformFunc: addInsert }
```

## test data

sometimes it's helpful to generate test data, so this module includes [a separate utility](https://github.com/ak--47/mixpanel-import/blob/main/generateFakeData.js) to do that:

```bash
$ npm run generate
```
`someTestData.json` will be written to `./testData`

## why?
because... i needed this and it didn't exists... so i made it.

then i open-sourced it because i thought it would be useful to others