/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
require('dotenv').config();




const mp = require('../index.js');
const { createMpStream } = require('../index.js');
const { createReadStream } = require('fs');
const { Readable, Transform, Writable, PassThrough } = require('stream');
const u = require('ak-tools');

const people = `./testData/people.ndjson`;
const events = `./testData/events.ndjson`;
const groups = `./testData/groups.ndjson`;
const table = `./testData/table.csv`;
const folderjsonl = `./testData/multi`;
const folderjson = `./testData/multijson`;
const moarEvents = require('../testData/moarEvents.json');
const moarPpl = require('../testData/tenkppl.json');
const eventNinetyNine = require('../testData/events-nine.json');


const opts = {
	recordType: `event`,
	compress: false,
	streamSize: 27,
	region: `US`,
	recordsPerBatch: 2000,
	bytesPerBatch: 2 * 1024 * 1024,
	strict: true,
	logs: false,
	fixData: true,
	verbose: false,
	streamFormat: 'jsonl',
	transformFunc: function noop(a) { return a; }
};

describe('do tests work?', () => {
	test('a = a', () => {
		expect(true).toBe(true);
	});
});

describe('filenames', () => { 
	test('event', async () => { 
		const data = await mp({}, events, { ...opts });		
		expect(data.success).toBe(5003);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test('user', async () => { 
		const data = await mp({}, people, { ...opts, recordType: `user` });		
		expect(data.success).toBe(5000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
		
	});

	test('group', async () => { 
		const data = await mp({}, groups, { ...opts, recordType: `group` });		
		expect(data.success).toBe(1860);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});
	
	test('table', async () => { 
		const lookup = await u.load(table);
		const data = await mp({}, lookup, { ...opts, recordType: `table` });		
		expect(data.success).toBe(1000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

})

describe('folders', () => { 
	test('jsonl', async () => { 
	
		const data = await mp({}, folderjsonl, { ...opts});		
		expect(data.success).toBe(3009);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test('json', async () => { 

		const data = await mp({}, folderjson, { ...opts, streamFormat: "json"});	
		expect(data.success).toBe(2664);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});
})


describe('in memory', () => { 
	test('events', async () => { 
		const data = await mp({}, moarEvents, { ...opts });	
		expect(data.success).toBe(666);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test('users', async () => { 
		const data = await mp({}, moarPpl, { ...opts, recordType: "user" });	
		expect(data.success).toBe(10000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

})

describe('file streams', () => { 
	test('event', async () => { 
		const data = await mp({}, createReadStream(events), { ...opts });		
		expect(data.success).toBe(5003);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test('user', async () => { 
		const data = await mp({}, createReadStream(people), { ...opts, recordType: `user` });		
		expect(data.success).toBe(5000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
		
	});

	test('group', async () => { 
		const data = await mp({}, createReadStream(groups), { ...opts, recordType: `group` });		
		expect(data.success).toBe(1860);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});
})

// ! not working!
// describe('object streams', () => { 
// 	test('events', () => { 
// 	const streamInMem = new Readable.from(eventNinetyNine, { objectMode: true });
// 	const mpStream = createMpStream({}, { ...opts }, (err, results)=> {
// 		expect(results.success).toBe(1860);
// 		expect(results.failed).toBe(0);
// 		expect(results.duration).toBeGreaterThan(0);	
// 	});
// 	streamInMem.pipe(mpStream)		
// 	})
// })
