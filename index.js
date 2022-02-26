// import things into mixpanel... quickly

//DEPENDENCIES
const { createReadStream, existsSync } = require('fs/promises')
const { readFile } = require('fs')
const { gzip, ungzip } = require('node-gzip')
const md5 = require('md5')
const isGzip = require('is-gzip')
const fetch = require('node-fetch')
const path = require('path')
const { pick } = require('underscore')

//.env (if used)
require('dotenv').config()

//endpoints
const ENDPOINTS = {
    us: {
        event : `https://api.mixpanel.com/import`,
        user: `https://api.mixpanel.com/engage`,
        group: `https://api.mixpanel.com/groups`,
        table: `https://api.mixpanel.com/lookup-tables/`
    },
    eu: {
        event : `https://api-eu.mixpanel.com/import`,
        user: `https://api-eu.mixpanel.com/engage`,
        group: `https://api-eu.mixpanel.com/groups`,
        table: `https://api-eu.mixpanel.com/lookup-tables/`
    }
}


async function main(creds = {}, data = [], opts = {}) {
    const defaultOpts = {
        recordType: `event`, //event, user, group (todo lookup table)
        streamSize: 27, //power of 2 for bytes  
        region: `US`, //US or EU
        eventsPerBatch: 2000, //event in each req
        bytesPerBatch: 2 * 1024 * 1024, //bytes in each req
        strict: true, //use strict mode?
        compress: true, //gzip
        logs: false //keep logs
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
    const envKeyNames = {MP_PROJECT: "project", MP_ACCT: "acct", MP_PASS: "pass", MP_SECRET: "secret", MP_TOKEN: "token" }
    const envCreds = renameKeys(envVars, envKeyNames)    
    const projInfo = getBasicAuthStr({ ...defaultCreds, ...creds, ...envCreds})
        
    //these values are used    
    const { recordType, streamSize, region, eventBatch, bytesPerBatch, strict, compress, logs } = options
    const url = ENDPOINTS[region.toLowerCase()][recordType]
    const { auth, method, projId, token } = projInfo
    const streamOpts = { highWaterMark: 2 ** streamSize }

    //figure out what data is passed in
    let foo;




   
}


//HELPERS

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

function getBasicAuthStr(auth) {
    let result = {
        auth : `Basic `
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

function chunkForNumOfEvents(arrayOfEvents, chunkSize) {
    return arrayOfEvents.reduce((resultArray, item, index) => {
        const chunkIndex = Math.floor(index / chunkSize)

        if (!resultArray[chunkIndex]) {
            resultArray[chunkIndex] = [] // start a new chunk
        }

        resultArray[chunkIndex].push(item)

        return resultArray
    }, [])
}

function chunkForSize(arrayOfBatches, maxBytes) {
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

async function compressChunks(arrayOfBatches) {
    const allBatches = arrayOfBatches.map(async function(batch) {
        return await gun.gzip(JSON.stringify(batch))
    })
    return Promise.all(allBatches)
}

async function sendDataToMixpanel(auth, batch) {
    let authString = 'Basic ' + Buffer.from(auth.username + ':' + auth.password, 'binary').toString('base64')
    let url = `${ENDPOINT_URL}?project_id=${auth.project_id}&strict=1`
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
        let req = await fetch(url, options)
        let res = await req.json()
        //console.log(`           ${JSON.stringify(res)}`)
        return res

    } catch (e) {
        console.log(`   problem with request:\n${e}`)
    }
}

function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
}


module.exports = main

//test
main()



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

    // console.log(`parsed ${numberWithCommas(allData.length)} events from ${pathToDataFile}\n`);

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
    // const batches = chunkForNumOfEvents(allData, EVENTS_PER_BATCH);

    // //max 2MB size per batch
    // const batchesSized = chunkForSize(batches, BYTES_PER_BATCH);


    // //COMPRESS
    // const compressed = await compressChunks(batchesSized)


    // //FLUSH
    // console.log(`sending ${numberWithCommas(allData.length)} events in ${numberWithCommas(batches.length)} batches\n`);
    // let numRecordsImported = 0;
    // for (eventBatch of compressed) {
    //     let result = await sendDataToMixpanel(credentials, eventBatch);
    //     console.log(result);
    //     numRecordsImported += result.num_records_imported || 0;
    // }

    // //FINISH
    // console.log(`\nsuccessfully imported ${numberWithCommas(numRecordsImported)} events`);
    // console.log('finshed.');
    // process.exit(0);