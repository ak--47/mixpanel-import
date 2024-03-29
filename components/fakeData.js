// @ts-nocheck
const fs = require('fs')
const Chance = require('chance'); //https://github.com/chancejs/chancejs
const chance = new Chance();
const readline = require('readline');


//possible names of events for test data
const eventNames = ['app open', 'log in', 'send message', 'receive message', 'roll dice', 'attack', 'defend', 'level up', 'start game']

//time stuffs
const now = Date.now();
const dayInMs = 8.64e+7;

function main(numEvents = 1000, numDays) {
    require('dotenv').config({ override: true })
    const arrOfEvents = [];
    let numOfEvents = Number(process.env.events) || Number(process.env.NUMEVENTS) || numEvents || 10000
    
    const lastArgument = [...process.argv].pop()
    if (!isNaN(lastArgument)) {
        numOfEvents = Number(lastArgument);
    }

    console.log('starting data generator...\n');
    const sinceDays = Number(process.env.days) || Number(process.env.SINCEDAYS) || numDays || 90

    //mixin for generating random events
    chance.mixin({
        'event': function () {
            return {
                event: chance.pickone(eventNames),
                properties: {
                    distinct_id: chance.guid(),                    
                    time: chance.integer({
                        min: now - dayInMs * sinceDays, //90 days in the past
                        max: now
                    }),
                    $source: "mixpanel import by AK (test Data)",
                    luckyNumber: chance.prime({min: 1, max: 10000}),
                    ip: chance.ip(),
                    email: chance.email(),
                    tag: "bar"
                }


            };
        }
    });

    console.log(`generating ${numberWithCommas(numOfEvents)} events... over the last ${sinceDays} days \n`);

    for (let index = 1; index < numOfEvents+1; index++) {
        arrOfEvents.push(chance.event());
        showProgress('events', index)       
    }

    console.log(`\n\nsaving ${numberWithCommas(numOfEvents)} events to ./someTestData.json\n`);

    fs.writeFile("./testData/someTestData.json", JSON.stringify(arrOfEvents), function(err) {
        if(err) {
			console.log(err);
            process.exit(1)
        }
        console.log("\ntry:\n\nnpm run import ./testData/someTestData.json\n\nto send the data to mixpanel!");
        process.exit(0)
    }); 

}

//helpers
function numberWithCommas(x) {
    return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function showProgress(thing, p) {
    //readline.clearLine(process.stdout);
    readline.cursorTo(process.stdout, 0);
    process.stdout.write(`${thing} created: ${numberWithCommas(p)}`);
}

module.exports = main;

// ;)
main();