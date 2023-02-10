/* NOTE: to make tests work, you need a .env file of the form

MP_PROJECT=project
MP_ACCT=acct
MP_PASS=password
MP_SECRET=secret
MP_TOKEN=token

and then download the test data here:

unzip it in ./testData

*/

/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
require('dotenv').config();
const { execSync } = require("child_process");
const longTimeout = 60000


const mp = require('../index.js');
const { createMpStream } = require('../index.js');
const { createReadStream } = require('fs');
const { Readable, Transform, Writable, PassThrough } = require('stream');
const u = require('ak-tools');
const events = `./testData/events.ndjson`;
const people = `./testData/people.ndjson`;
const groups = `./testData/groups.ndjson`;
const table = `./testData/table.csv`;
const folderjsonl = `./testData/multi`;
const folderjson = `./testData/multijson`;
const moarEvents = require('../testData/moarEvents.json');
const moarPpl = require('../testData/tenkppl.json');
const eventNinetyNine = require('../testData/events-nine.json');
const twoFiftyK = `./testData/big.ndjson`;
const needTransform = `./testData/needDateTransform.ndjson`;
const dayjs = require('dayjs');

const opts = {
	recordType: `event`,
	compress: false,
	workers: 20,
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
	}, longTimeout);

	test('user', async () => {
		const data = await mp({}, people, { ...opts, recordType: `user` });
		expect(data.success).toBe(5000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);

	}, longTimeout);

	test('group', async () => {
		const data = await mp({}, groups, { ...opts, recordType: `group` });
		expect(data.success).toBe(1860);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

	test('table', async () => {
		const lookup = await u.load(table);
		const data = await mp({}, lookup, { ...opts, recordType: `table` });
		expect(data.success).toBe(1000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

});

describe('folders', () => {
	test('jsonl', async () => {

		const data = await mp({}, folderjsonl, { ...opts, streamFormat: "jsonl" });
		expect(data.success).toBe(3009);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

	test('json', async () => {

		const data = await mp({}, folderjson, { ...opts, streamFormat: "json" });
		expect(data.success).toBe(2664);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);
});


describe('in memory', () => {
	test('events', async () => {
		const data = await mp({}, moarEvents, { ...opts });
		expect(data.success).toBe(666);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

	test('users', async () => {
		const data = await mp({}, moarPpl, { ...opts, recordType: "user" });
		expect(data.success).toBe(10000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

});

describe('file streams', () => {
	test('event', async () => {
		const data = await mp({}, createReadStream(events), { ...opts });
		expect(data.success).toBe(5003);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

	test('user', async () => {
		const data = await mp({}, createReadStream(people), { ...opts, recordType: `user` });
		expect(data.success).toBe(5000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);

	}, longTimeout);

	test('group', async () => {
		const data = await mp({}, createReadStream(groups), { ...opts, recordType: `group` });
		expect(data.success).toBe(1860);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);
});

describe('transform', () => {
	test('can use custom transform', async () => {
		const data = await mp({}, createReadStream(needTransform), {
			...opts, transformFunc: (ev) => {
				const eventModel = {
					event: ev.event,
					properties: { ...ev }
				};

				eventModel.properties.time = dayjs(eventModel.properties.time).unix();

				return eventModel;
			}
		});
		expect(data.success).toBeGreaterThan(1004);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);
});

describe('object streams', () => {
	test('events', (done) => {
		const streamInMem = new Readable.from(eventNinetyNine, { objectMode: true });
		const mpStream = createMpStream({}, { ...opts }, (err, results) => {
			expect(results.success).toBe(9999);
			expect(results.failed).toBe(0);
			expect(results.duration).toBeGreaterThan(0);
			done();
		});
		streamInMem.pipe(mpStream);
	});

	test('users', (done) => {
		const streamInMem = new Readable.from(moarPpl, { objectMode: true });
		const mpStream = createMpStream({}, { ...opts, recordType: 'user' }, (err, results) => {
			expect(results.success).toBe(10000);
			expect(results.failed).toBe(0);
			expect(results.duration).toBeGreaterThan(0);
			done();
		});
		streamInMem.pipe(mpStream);
	});

});

describe('exports', () => {	
	test('can export event data', async () => {
		const data = await mp({}, null, { ...opts, recordType: 'export', start: '2023-01-01', end: '2023-01-03' });
		expect(data.duration).toBeGreaterThan(0);
		expect(data.requests).toBe(1);
		expect(data.failed).toBe(0);
		expect(data.total).toBeGreaterThan(92);
		expect(data.success).toBeGreaterThan(92);
	}, longTimeout);

	test('can export profile data', async () => {
		
		const data = await mp({}, null, { ...opts, "recordType": "peopleExport" });
		expect(data.duration).toBeGreaterThan(0);
		expect(data.requests).toBeGreaterThan(5);
		expect(data.responses.length).toBeGreaterThan(5);
		expect(data.failed).toBe(0);
		expect(data.total).toBeGreaterThan(5999);
		expect(data.success).toBeGreaterThan(5999);

	}, longTimeout);
});


describe('big files', () => {
	jest.setTimeout(10000);
	test('250k events', async () => {
		const data = await mp({}, twoFiftyK, { ...opts, streamFormat: `jsonl` });
		expect(data.success).toBe(250000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);
});

describe('cli', () => {
	test('events', async () => {		const output = execSync(`node ./index.js ${events} --fixData`).toString().trim().split('\n').pop();

		const result = await u.load(output, true);
		expect(result.success).toBe(5003);
	}, longTimeout);


	test('users', async () => {
		const output = execSync(`node ./index.js ${people} --type user --fixData`).toString().trim().split('\n').pop();
		const result = await u.load(output, true);
		expect(result.success).toBe(5000);
	}, longTimeout);


	test('groups', async () => {
		const output = execSync(`node ./index.js ${groups} --type group --fixData`).toString().trim().split('\n').pop();
		const result = await u.load(output, true);
		expect(result.success).toBe(1860);
	}, longTimeout);

	test('tables', async () => {
		const output = execSync(`node ./index.js ${table} --type table --fixData`).toString().trim().split('\n').pop();
		const result = await u.load(output, true);
		expect(result.success).toBe(1000);
	}, longTimeout);
});

afterAll(async () => {
	execSync(`npm run prune`);
});