// mixpanel-import
// by AK
// purpose: import events, users, groups, tables into mixpanel... quickly


//stream stuff
//https://github.com/uhop/stream-json/wiki
const { parser } = require('stream-json');
const StreamArray = require('stream-json/streamers/StreamArray');
const JsonlParser = require('stream-json/jsonl/Parser');
const Batch = require('stream-json/utils/Batch');
//https://github.com/uhop/stream-chain/wiki
const { chain } = require('stream-chain');

//first party
const { pipeline } = require('stream/promises')
const { createReadStream, existsSync } = require('fs');
const path = require('path')
const { pick } = require('underscore')
const readline = require('readline');
const zlib = require('zlib');

//third party
const { gzip, ungzip } = require('node-gzip')
const md5 = require('md5')
const isGzip = require('is-gzip')
const fetch = require('node-fetch')
const split = require('split');

//.env (if used)
require('dotenv').config()

//endpoints
const ENDPOINTS = {
    us: {
        event: `https://api.mixpanel.com/import`,
        user: `https://api.mixpanel.com/engage`,
        group: `https://api.mixpanel.com/groups`,
        table: `https://api.mixpanel.com/lookup-tables/`
    },
    eu: {
        event: `https://api-eu.mixpanel.com/import`,
        user: `https://api-eu.mixpanel.com/engage`,
        group: `https://api-eu.mixpanel.com/groups`,
        table: `https://api-eu.mixpanel.com/lookup-tables/`
    }
}

//globals (local to this module)
let logging = false;
let fileStreamOpts = {};
let url = ``;
let recordType = ``

async function main(creds = {}, data = [], opts = {}) {
    const defaultOpts = {
        recordType: `event`, //event, user, group (todo lookup table)
        streamSize: 27, //power of 2 for bytes  
        region: `US`, //US or EU
        recordsPerBatch: 2000, //event in each req
        bytesPerBatch: 2 * 1024 * 1024, //bytes in each req
        strict: true, //use strict mode?
        logs: false, //print to stdout?
        transformFunc: function noop(a) { return a } //will be called on every record
    }
    const options = { ...defaultOpts, ...opts }


    const defaultCreds = {
        acct: ``, //service acct username
        pass: ``, //service acct secret
        project: ``, //project id
        secret: ``, //api secret (deprecated auth)
        token: `` //project token        
    }

    //sweep .env to pickup MP_ keys; i guess the .env convention is to use all caps? so be it...
    const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`)
    const envKeyNames = { MP_PROJECT: "project", MP_ACCT: "acct", MP_PASS: "pass", MP_SECRET: "secret", MP_TOKEN: "token" }
    const envCreds = renameKeys(envVars, envKeyNames)
    const project = resolveProjInfo({ ...defaultCreds, ...creds, ...envCreds })

    //these values are used for configuation   
    const { streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs, transformFunc } = options
    recordType = options.recordType
    logging = options.logs
    fileStreamOpts = { highWaterMark: 2 ** streamSize }
    url = ENDPOINTS[region.toLowerCase()][recordType]

    //in case this is run as CLI
    const lastArgument = [...process.argv].pop()

    if (data?.length === 0 && lastArgument.includes('json')) {
        data = lastArgument;
        logging = true;
    }

    //the pipeline
    let pipeline;
    const dataType = determineData(data)
    switch (dataType) {
        case `file`:
            if (logging) log(`streaming ${recordType}s from ${data}`)
            pipeline = await streamPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc)
            break;
        case `inMem`:
            if (logging) log(`parsing ${recordType}s`)
            pipeline = await sendDataInMem(data, project, recordsPerBatch, bytesPerBatch, transformFunc)
            break;
        default:
            if (logging) log(`could not determine data source`)
            pipeline = `error`
            break;
    }

    return pipeline;

}

//CORE PIPELINE(S)
async function streamPipeline(data, project, recordsPerBatch, bytesPerBatch, transformFunc) {
    return new Promise((resolve, reject) => {
        //streaming files to mixpanel!
        if (logging) console.time('stream pipeline')
        const pipeline = chain([
            createReadStream(path.resolve(data)),
            streamParseType(data),
            //transform func
            (data) => {
                return transformFunc(data.value)
            },
            new Batch({ batchSize: recordsPerBatch }),
            async (batch) => await gzip(JSON.stringify(batch)),
                async (batch) => {
                    return await sendDataToMixpanel(project, batch)
                }
        ]);

        //listening to the pipeline
        let records = 0;
        let batches = 0;
        let responses = [];

        pipeline.on('error', (error) => {
            if (logging) log(error)
            reject(error)
        });

        pipeline.on('data', (response) => {
            batches += 1;
            records += Number(response.num_records_imported)
            if (logging) showProgress(recordType, records, records, batches, batches)
            responses.push(response)


        });
        pipeline.on('end', () => {
            if (logging) log(``);
            if (logging) console.timeEnd('stream pipeline');
            resolve(responses)
        });
    })
}

async function sendDataInMem(data, project, recordsPerBatch, bytesPerBatch, transformFunc) {
    if (logging) console.time('chunk')
    let dataIn = data.map(transformFunc)
    const batches = await zipChunks(chunkSize(chunkEv(dataIn, recordsPerBatch), bytesPerBatch));
    if (logging) console.timeEnd('chunk')
    log(`\nloaded ${addComma(dataIn.length)} ${recordType}s`)

    //flush to mixpanel
    if (logging) console.time('flush')
    let responses = []
    let iter = 0;
    for (const batch of batches) {
        iter += 1
        if (logging) showProgress(recordType, recordsPerBatch * iter, dataIn.length, iter, batches.length)
        let res = await sendDataToMixpanel(project, batch)
        responses.push(res)
    }
    if (logging) log('\n');
    if (logging) console.timeEnd('flush')

    return responses

}

async function sendDataToMixpanel(proj, batch) {
    let authString = proj.auth

    let options = {
        method: 'POST',
        headers: {
            'Authorization': authString,
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip'

        },
        body: batch
    }

    try {
        let req = await fetch(`${url}?ip=0&verbose=1`, options)
        let res = await req.json()
        return res

    } catch (e) {
        if (logging) log(`problem with request:\n${e}`)
    }
}


//HELPERS
function streamParseType(fileName) {
    if (fileName.endsWith('.json')) {
        return StreamArray.withParser()
    }

    if (fileName.endsWith('.ndjson') || fileName.endsWith('.jsonl')) {
        const jsonlParser = new JsonlParser();
        return jsonlParser
    }

}

function determineData(data) {
    switch (typeof data) {
        case `string`:
            try {
                //could be stringified data
                JSON.parse(data)
                return `structString`
            } catch (error) {

            }

            //probably a file; stream it
            let dataPath = path.resolve(data)
            if (!existsSync(dataPath)) console.error(`could not find ${data} ... does it exist?`)
            return `file`
            break;

        case `object`:
            //probably structured data; just load it
            if (!Array.isArray(data)) console.error(`only arrays of events are support`)
            return `inMem`
            break;

        default:
            console.error(`could not determine the type of ${data} ... `)
            process.exit(0)
            break;
    }
}

//https://stackoverflow.com/a/45287523
function renameKeys(obj, newKeys) {
    const keyValues = Object.keys(obj).map(key => {
        const newKey = newKeys[key] || key
        return {
            [newKey]: obj[key]
        }
    })
    return Object.assign({}, ...keyValues)
}

function resolveProjInfo(auth) {
    let result = {
        auth: `Basic `
    }
    //fallback method: secret auth
    if (auth.secret) {
        result.auth += Buffer.from(auth.secret + ':', 'binary').toString('base64')
        result.method = `secret`

    }

    //preferred method: service acct
    if (auth.acct && auth.pass && auth.project) {
        result.auth += Buffer.from(auth.acct + ':' + auth.pass, 'binary').toString('base64')
        result.method = `serviceAcct`

    }

    result.token = auth.token
    result.projId = auth.project
    return result
}

function chunkEv(arrayOfEvents, chunkSize) {
    return arrayOfEvents.reduce((resultArray, item, index) => {
        const chunkIndex = Math.floor(index / chunkSize)

        if (!resultArray[chunkIndex]) {
            resultArray[chunkIndex] = [] // start a new chunk
        }

        resultArray[chunkIndex].push(item)

        return resultArray
    }, [])
}

function chunkSize(arrayOfBatches, maxBytes) {
    return arrayOfBatches.reduce((resultArray, item, index) => {
        //assume each character is a byte
        const currentLengthInBytes = JSON.stringify(item).length

        if (currentLengthInBytes >= maxBytes) {
            //if the batch is too big; cut it in half
            //todo: make this is a little smarter
            let midPointIndex = Math.ceil(item.length / 2)
            let firstHalf = item.slice(0, midPointIndex)
            let secondHalf = item.slice(-midPointIndex)
            resultArray.push(firstHalf)
            resultArray.push(secondHalf)
        } else {
            resultArray.push(item)
        }

        return resultArray
    }, [])
}

async function zipChunks(arrayOfBatches) {
    const allBatches = arrayOfBatches.map(async function(batch) {
        return await gzip(JSON.stringify(batch))
    })
    return Promise.all(allBatches)
}


function log(message) {
    if (logging) {
        console.log(`${message}\n`)
    }
}

function showProgress(record, ev, evTotal, batch, batchTotal) {
    if (logging) {
        readline.cursorTo(process.stdout, 0);
        process.stdout.write(`  ${record}s sent: ${addComma(ev)}/${addComma(evTotal)} | batches sent: ${addComma(batch)}/${addComma(batchTotal)}`);
    }
}

// 1000 => 1,000
function addComma(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


module.exports = main

if (require.main === module) {
    main();
}

//test
//main(null, `./testData/someTestData.ndjson`, { logs: true })