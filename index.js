// import things into mixpanel... quickly


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

//globals for this module
let logging = false;
let streamOpts = {};
let url = ``;

async function main(creds = {}, data = [], opts = {}) {
    const defaultOpts = {
        recordType: `event`, //event, user, group (todo lookup table)
        streamSize: 27, //power of 2 for bytes  
        region: `US`, //US or EU
        recordsPerBatch: 2000, //event in each req
        bytesPerBatch: 2 * 1024 * 1024, //bytes in each req
        strict: true, //use strict mode?
        logs: false //print to stdout?
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
    const { recordType, streamSize, region, recordsPerBatch, bytesPerBatch, strict, logs } = options
    logging = options.logs
    streamOpts = { highWaterMark: 2 ** streamSize }
    url = ENDPOINTS[region.toLowerCase()][recordType]
    if (logging) console.time('parse')

    //streaming files to mixpanel!
    if (logging) console.time('pipeline')
    const pipeline = chain([
        createReadStream(path.resolve(data)),
        parseType(data),
        //transform func
        (data) => data.value,
        new Batch({ batchSize: recordsPerBatch }),        
        async (batch) => await gzip(JSON.stringify(batch)),
        async (batch) => {
            return await sendDataToMixpanel(project, batch)
        }
    ]);
    
    //listening to the pipeline
    let records = 0;
    let batches = 0;
    pipeline.on('error', error => console.log(error));
    pipeline.on('data', (response)=>{        
        batches += 1;
        records += Number(response.num_records_imported)
        if (logging) showProgress(recordType, records, records, batches, batches)
        
        
    });
    pipeline.on('end', ()=>{
        if (logging) log(``); console.timeEnd('pipeline');
    });



    /*
    essentially:

    pipeline(
        loadData,
        transform
        chunk,
        zip,
        flush
    )

    */

    // //parse, partition, and compress the data
    // const dataIn = await loadData(data);
    // const batches = await zipChunks(chunkSize(chunkEv(dataIn, recordsPerBatch), bytesPerBatch));
    // if (logging) console.timeEnd('parse')
    // log(`\nloaded ${addComma(dataIn.length)} ${recordType}s`)

    // //flush to mixpanel
    // let responses = []
    // let iter = 0;
    // if (logging) console.time('flush')
    // for (const batch of batches) {
    //     iter += 1
    //     showProgress(recordType, recordsPerBatch * iter, dataIn.length, iter, batches.length)
    //     let res = await sendDataToMixpanel(project, batch);
    //     responses.push(res)
    // }
    // if (logging) log('\n');
    // console.timeEnd('flush')
    // let foo;







}


//HELPERS
function parseType(fileName) {
    if (fileName.endsWith('.json')) {
        return StreamArray.withParser()
    }

    if (fileName.endsWith('.ndjson') || fileName.endsWith('.jsonl')) {
        const jsonlParser = new JsonlParser();
        return jsonlParser
    }

}

async function decompress(fileName) {
    let foo;
}

async function loadData(data) {
    switch (typeof data) {
        case `string`:
            //probably a file; stream it
            let dataPath = path.resolve(data)
            if (!existsSync(dataPath)) console.error(`could not find ${data} ... does it exist?`)
            return handleFile(createReadStream(dataPath, streamOpts), dataPath)
            break;
        case `object`:
            //probably structured data; return it
            if (!Array.isArray(data)) console.error(`only arrays of events are support`)
            return data
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
        //console.log(`           ${JSON.stringify(res)}`)
        return res

    } catch (e) {
        console.log(`   problem with request:\n${e}`)
    }
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

function addComma(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


module.exports = main

//test
main(null, `./testData/someTestData.json`, { logs: true })



// //AUTH
// //prefer .env credentials, if they exist
// if (process.env.PROJECTID && process.env.USERNAME && process.env.PASSWORD) {
//     console.log(`using .env supplied credentials:\n
//         project id: ${process.env.PROJECTID}
//         user: ${process.env.USERNAME}
//     `);

//     credentials.project_id = process.env.PROJECTID
//     credentials.username = process.env.USERNAME
//     credentials.password = process.env.PASSWORD
// } else {
//     console.log(`using hardcoded credentials:\n
//     project id: ${credentials.project_id}
//     user: ${credentials.username}
//     `)
// }

// //LOAD
// let file = await readFilePromisified(dataFile).catch((e) => {
//     console.error(`failed to load ${dataFile}... does it exist?\n`);
//     console.log(`if you require some test data, try 'npm run generate' first...`);
//     process.exit(1);
// });


// //DECOMPRESS
// let decompressed;
// if (isGzip(file)) {
//     console.log('unzipping file\n')
//     decompressed = await (await ungzip(file)).toString();
// } else {
//     decompressed = file.toString();
// }

// //UNIFY
// //if it's already JSON, just use that
// let allData;
// try {
//     allData = JSON.parse(decompressed)
// } catch (e) {
//     //it's probably NDJSON, so iterate over each line
//     try {
//         allData = decompressed.split('\n').map(line => JSON.parse(line));
//     } catch (e) {
//         //if we don't have JSON or NDJSON... fail...
//         console.log('failed to parse data... only valid JSON or NDJSON is supported by this script')
//         console.log(e)
//     }
// }

// console.log(`parsed ${addComma(allData.length)} events from ${pathToDataFile}\n`);

// //TRANSFORM
// for (singleEvent of allData) {

//     //ensure each event has an $insert_id prop
//     if (!singleEvent.properties.$insert_id) {
//         let hash = md5(singleEvent);
//         singleEvent.properties.$insert_id = hash;
//     }

//     //ensure each event doesn't have a token prop
//     if (singleEvent.properties.token) {
//         delete singleEvent.properties.token
//     }

//     //etc...

//     //other checks and transforms go here
//     //consider checking for the existince of event name, distinct_id, and time, and max 255 props
//     //as per: https://developer.mixpanel.com/reference/events#validation
// }


// //CHUNK

// //max 2000 events per batch
// const batches = chunkEv(allData, EVENTS_PER_BATCH);

// //max 2MB size per batch
// const batchesSized = chunkSize(batches, BYTES_PER_BATCH);


// //COMPRESS
// const compressed = await compressChunks(batchesSized)


// //FLUSH
// console.log(`sending ${addComma(allData.length)} events in ${addComma(batches.length)} batches\n`);
// let numRecordsImported = 0;
// for (eventBatch of compressed) {
//     let result = await sendDataToMixpanel(credentials, eventBatch);
//     console.log(result);
//     numRecordsImported += result.num_records_imported || 0;
// }

// //FINISH
// console.log(`\nsuccessfully imported ${addComma(numRecordsImported)} events`);
// console.log('finshed.');
// process.exit(0);