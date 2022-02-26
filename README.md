
# mp import node.js

  

## wat.

  

This is a one-off script that implement's [Mixpanel's `/import` API](https://developer.mixpanel.com/reference/events#import-events) in node.js. It uses [service accounts](https://developer.mixpanel.com/reference/authentication#service-accounts) for authentication, and can batch import millions of events, quickly.

  

This script is meant to be run **locally**; for a **cloud-based** data import, [see our in-depth guide](https://developer.mixpanel.com/docs/cloud-ingestion).

  

## tldr;

install as a module:

```
npm install mp-batch-import --save
```
then use in your code:
```
const mpImport = require('mp-batch-import');
const credentials = {
	project_id: `{{mp project id}}`,
	username: `{{service account user}}`
	password: `{{service account pass}}`
}
const res = mpImport(credentials, `./pathToData.json`).then(res => console.log(res));
```

run locally:
```

git clone https://github.com/ak--47/mpBatchImport-node.git

  

cd mpBatchImport-node/

  

npm install

  

echo 'PROJECTID=<your-project-id>

USERNAME=<your-service-account-user>

PASSWORD=<your-service-secret-secret>

' > .env

  

npm run import ./path-To-JSON-Data

```

  

or if you want to generate some test data first:

  

```

npm run generate

  

npm run import

```

  

## Detailed Instructions

  

### Install Dependencies

  

This script uses `npm` to manage dependencies, similar to a web application.

  

After cloning the repo, `cd` into the `/mpBatchImport-node` and run:

  

```

npm install

```

  

this only needs to be done once.

  

### Authentication

  

Authentication for `/import` is handled by [service accounts](https://developer.mixpanel.com/reference/authentication#service-accounts). You'll need to create a service account in your Mixpanel project and provide this script with your credentials (`project_id` , `username`, `secret`)

  

There are two ways to do that; you can choose whichever best suits your needs:

  

#### Add Credentials to the Script

You can supply your credentials directly in the script by editing [lines 19-26](https://github.com/ak--47/mpBatchImport-node/blob/main/index.js#L19-L26):

  

```

const creds = {

project_id: 'myProjectId'

username: 'myServiceAccount',

password: 'myServiceSecret'

}

```

  
  

#### Use an .env file

  

Alternatively, you can provide credentials to this script via a `.env` file of the form:

  

```

PROJECTID=myProjectId

USERNAME=myServiceAccount

PASSWORD=myServiceSecret

```

the `.env` file should be in the root directory of the script.

  

### Passing In Data

  

You can pass in data files of any size, as long as they are valid [JSON](https://jsonlint.com/) or [NDJSON](http://ndjson.org/). If the data is compressed (`gzip`), the script will automatically decompress it.

  

The data should have the general form of Mixpanel's [event specification](https://developer.mixpanel.com/reference/events#track-event):

  

```

{

"event": "eventName" //required: event name

"properties": {

"distinct_id": 1337, //required: user id

"time": 1629120141 //required: unix epoc (sec or ms)

"foo": "bar" //optional: any other props

}

}

```

  

For data imports, `token` is not required in `properties`; this script generates `$insert_id` using `md5` hashing. You can add additional data transformations on [line 120](https://github.com/ak--47/mpBatchImport-node/blob/main/index.js#L120-L122).

  

There are two ways to pass data into the script; choose which best suits your needs:

  

#### Reference the absolute path

  

[Line 36](https://github.com/ak--47/mpBatchImport-node/blob/main/index.js#L36) specifies the path to the source data:

  

```

let pathToDataFile = `./someTestData.json`

```

  

change this to a valid path that points to the data you wish to import

  

#### Use command line arguments

  

The script also accepts a single command line argument to reference the data you wish to import:

  

```

npm run import ./path-to-data.json

```

  

### Generating Test Data

  

If you do not have data to import, but want to test/evaluate Mixpanel's `/import` API, this script will generate data for you.

  

Simply run:

```

npm run generate

```

  

and you will see a file `./someTestData.ndjson` is created in the top level of the project directory.

  

Optionally, you may add a command line argument to specify the number of test events to create (defaults to `10,000`):

  

```

npm run generate 20000

```

  

### Sending Data to Mixpanel

  

Once you have specified:

  

- Your credentials (in the script OR using `.env`)

- Path to your data filed (in the script OR using command line arguments)

  

You are ready to run an import:

  

```

npm run import

```

  

The script will output messages to keep you informed of it's progress:

  

```

starting up...

  

using .env supplied credentials:

  

project id: 2507188

user: nodeIsTheThing.bd59fa.mp-service-account

parsed 10,000 events from ./someTestData.ndjson

  

sending 10,000 events in 5 batches

  

{ code: 200, num_records_imported: 2000, status: 'OK' }

{ code: 200, num_records_imported: 2000, status: 'OK' }

{ code: 200, num_records_imported: 2000, status: 'OK' }

{ code: 200, num_records_imported: 2000, status: 'OK' }

{ code: 200, num_records_imported: 2000, status: 'OK' }

  

successfully imported 10,000 events

finshed.

```

  

For more on the `/import` API's various responses, [see the relevant documentation](https://developer.mixpanel.com/reference/events#import-events).npm