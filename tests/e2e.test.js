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
const longTimeout = 60000;


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
	test('events', async () => {
		const output = execSync(`node ./index.js ${events} --fixData`).toString().trim().split('\n').pop();

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

describe('options', () => {
	test('abridged mode', async () => {
		const data = await mp({}, events, { ...opts, abridged: true });
		expect(data.success).toBe(5003);
		expect(data.failed).toBe(0);
		expect(data.responses.length).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);

	test('properly removes nulls', async () => {
		const data = await mp({}, [{
			"event": "nullTester",
			"properties": {
				"time": 1678931922817,
				"distinct_id": "foo",
				"$insert_id": "bar",
				"actual_null": null,
				"undef": undefined,
				"empty str": "",
				"zero": 0,
				"bool false": false,
				"empty array": [],
				"empty obj": {},
				"arr of null": [null, null, null]
			}
		}], { ...opts, abridged: true, removeNulls: true });
		expect(data.success).toBe(1);
		expect(data.failed).toBe(0);
		expect(data.responses.length).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	}, longTimeout);


	test('time offsets', async () => {
		const dataPoint = [{
			"event": "add to cart 4",
			"properties":
			{
				"time": 1678865417,
				"distinct_id": "186e5979172b50-05055db7ae8024-1e525634-1fa400-186e59791738d4"
			}
		}];

		const data = await mp({}, dataPoint, { ...opts, timeOffset: 7 });
		expect(data.success).toBe(1);
		expect(data.failed).toBe(0);
		expect(data.responses.length).toBe(1);
		expect(data.duration).toBeGreaterThan(0);

	}, longTimeout);

	test('where clause', async () => {
		const data = await mp({}, null, { ...opts, recordType: 'export', start: '2023-01-01', end: '2023-01-01', where: './tmp/events.ndjson' });
		const folder = await u.ls('./tmp');
		expect(folder[1]).toBe(`/Users/ak/code/mixpanel-import/tmp/events.ndjson`);
		expect(data.duration).toBeGreaterThan(0);
		expect(data.requests).toBe(1);
		expect(data.failed).toBe(0);
		expect(data.total).toBeGreaterThan(33);
		expect(data.success).toBeGreaterThan(33);
	}, longTimeout);

});

describe('data fixes', () => {
	test('deal with /engage payloads', async () => {
		const data = [{ "$distinct_id": "28e929d8-46aa-5dda-8941-0cb6a6cff1c6", "$properties": { "avatar": "https://randomuser.me/api/portraits/women/18.jpg", "colorTheme": "violet", "created": "2023-03-04T03:12:23", "email": "hemiravaw@udte.vg", "lat": -34.93, "long": -67.11, "luckyNumber": 5, "name": "Louisa de Graaf", "phone": "+9457951595", "uuid": "28e929d8-46aa-5dda-8941-0cb6a6cff1c6" } }, { "$distinct_id": "1a632c4e-bccc-55f6-8915-b768479dbe55", "$properties": { "avatar": "https://randomuser.me/api/portraits/women/70.jpg", "colorTheme": "blue", "created": "2023-03-03T06:56:39", "email": "weraruz@dapa.pm", "lat": 0.63, "long": -40.78, "luckyNumber": 2, "name": "Verna Sorelli", "phone": "+8714237993", "uuid": "1a632c4e-bccc-55f6-8915-b768479dbe55" } }, { "$distinct_id": "c26f798f-62d4-5f26-8e87-2010d58e5016", "$properties": { "avatar": "https://randomuser.me/api/portraits/men/60.jpg", "colorTheme": "blue", "created": "2023-03-02T09:12:55", "email": "jog@mulzitil.tj", "lat": -82.8, "long": 51.9, "luckyNumber": 19, "name": "Rodney Bonnet", "phone": "+6827435739", "uuid": "c26f798f-62d4-5f26-8e87-2010d58e5016" } }];
		const job = await mp({}, data, { ...opts, recordType: 'user', fixData: true });
		expect(job.success).toBe(3);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(3);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
	}, longTimeout);




	test('flat user props', async () => {
		const data = [
			{
				$distinct_id: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6",
				avatar: "https://randomuser.me/api/portraits/women/18.jpg",
				colorTheme: "violet",
				created: "2023-03-04T03:12:23",
				email: "hemiravaw@udte.vg",
				lat: -34.93,
				long: -67.11,
				luckyNumber: 5,
				name: "Louisa de Graaf",
				phone: "+9457951595",
				uuid: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6",
			}
		];
		const job = await mp({}, data, { ...opts, recordType: 'user', fixData: true });
		expect(job.success).toBe(1);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(1);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
	}, longTimeout);
});

afterAll(async () => {
	execSync(`npm run prune`);
});
