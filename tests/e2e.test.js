// @ts-nocheck
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
require("dotenv").config();
const { execSync } = require("child_process");
const longTimeout = 75000;

const {
	MP_PROJECT = "",
	MP_ACCT = "",
	MP_PASS = "",
	MP_SECRET = "",
	MP_TOKEN = "",
	MP_TABLE_ID = "" } = process.env;

if (!MP_PROJECT || !MP_ACCT || !MP_PASS || !MP_SECRET || !MP_TOKEN || !MP_TABLE_ID) {
	console.error("Please set the following environment variables: MP_PROJECT, MP_ACCT, MP_PASS, MP_SECRET, MP_TOKEN, MP_TABLE_ID");
	process.exit(1);
}

const mp = require("../index.js");
const { createMpStream } = require("../index.js");
const { createReadStream } = require("fs");
const { Readable, Transform, Writable, PassThrough } = require("stream");
const u = require("ak-tools");
const events = `./testData/events.ndjson`;
const eventsNDJSONdisguise = `./testData/events.json`;
const people = `./testData/people.ndjson`;
const groups = `./testData/groups.ndjson`;
const table = `./testData/table.csv`;
const folderjsonl = `./testData/multi`;
const folderjson = `./testData/multijson`;
const moarEvents = require("../testData/moarEvents.json");
const moarPpl = require("../testData/tenkppl.json");
const eventNinetyNine = require("../testData/events-nine.json");
const twoFiftyK = `./testData/big.ndjson`;
const needTransform = `./testData/needDateTransform.ndjson`;
const dayjs = require("dayjs");
const badData = `./testData/bad_data.jsonl`;
const eventsCSV = `./testData/eventAsTable.csv`;
const dupePeople = `./testData/pplWithDupes.ndjson`;
const heapParseError = `./testData/heap-parse-error.jsonl`;

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
	streamFormat: "jsonl",
	transformFunc: function noop(a) {
		return a;
	}
};

describe("filenames", () => {
	test(
		"event",
		async () => {
			const data = await mp({}, events, { ...opts });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
			expect(data).toHaveProperty("startTime");
			expect(data).toHaveProperty("endTime");
		},
		longTimeout
	);

	test(
		"event (.json ext, but jsonl)",
		async () => {
			const data = await mp({}, eventsNDJSONdisguise, { ...opts, streamFormat: "jsonl" });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
			expect(data).toHaveProperty("startTime");
			expect(data).toHaveProperty("endTime");
		},
		longTimeout
	);

	test(
		"user",
		async () => {
			const data = await mp({}, people, { ...opts, recordType: `user` });
			expect(data.success).toBe(5000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"group",
		async () => {
			const data = await mp({}, groups, { ...opts, recordType: `group` });
			expect(data.success).toBe(1860);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"table",
		async () => {
			const lookup = await u.load(table);
			const data = await mp({}, lookup, { ...opts, recordType: `table` });
			expect(data.success).toBe(1000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("folders", () => {
	test(
		"jsonl",
		async () => {
			const data = await mp({}, folderjsonl, { ...opts, streamFormat: "jsonl" });
			expect(data.success).toBe(3009);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"json",
		async () => {
			const data = await mp({}, folderjson, { ...opts, streamFormat: "json" });
			expect(data.success).toBe(2664);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"array of filenames (JSONL)",
		async () => {
			const data = await mp({}, [events, events, events], {
				...opts,
				streamFormat: "jsonl",
				parseErrorHandler: (err, d, f) => {
					console.log(err);
					debugger;
				}
			});
			expect(data.success).toBe(5003 * 3);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
			expect(data).toHaveProperty("startTime");
			expect(data).toHaveProperty("endTime");
		},
		longTimeout
	);

	const aliases = { row_id: "$insert_id", uuid: "distinct_id", action: "event", timestamp: "time" };
	test(
		"array of filenames (CSV)",
		async () => {
			const data = await mp({}, [eventsCSV, eventsCSV], { ...opts, streamFormat: "csv", aliases, forceStream: true });
			expect(data.success).toBe(10003 * 2);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("in memory", () => {
	test(
		"events",
		async () => {
			const data = await mp({}, moarEvents, { ...opts });
			expect(data.success).toBe(666);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"users",
		async () => {
			const data = await mp({ token: MP_TOKEN }, moarPpl, { ...opts, recordType: "user", fixData: true});
			expect(data.success).toBe(10000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("inference", () => {


	test("infers json", async () => {
		const data = await mp({}, './testData/moarEvents.json', { ...opts, streamFormat: "" });
		expect(data.success).toBe(666);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});
	test("infers ndjson", async () => {
		const data = await mp({}, events, { ...opts, streamFormat: "" });
		expect(data.success).toBe(5003);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
		expect(data).toHaveProperty("startTime");
		expect(data).toHaveProperty("endTime");
	},);
	test("infers csv", async () => {
		const data = await mp({}, eventsCSV, { ...opts, streamFormat: "", aliases: { row_id: "$insert_id", uuid: "distinct_id", action: "event", timestamp: "time" }, forceStream: false });
		expect(data.success).toBe(10003);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	},
		longTimeout);



});

describe("file streams", () => {
	test(
		"event",
		async () => {
			const data = await mp({}, createReadStream(events), { ...opts });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"user",
		async () => {
			const data = await mp({}, createReadStream(people), { ...opts, recordType: `user` });
			expect(data.success).toBe(5000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"group",
		async () => {
			const data = await mp({}, createReadStream(groups), { ...opts, recordType: `group` });
			expect(data.success).toBe(1860);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("transform", () => {
	test(
		"can use custom transform",
		async () => {
			const data = await mp({}, createReadStream(needTransform), {
				...opts,
				transformFunc: ev => {
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
		},
		longTimeout
	);

	test(
		"can explode records",
		async () => {
			const data = [
				{ event: false },
				{ event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } },
				{ event: "foo", properties: { distinct_id: "naz", time: 1681750925148, $insert_id: "4321" } }
			];
			const func = o => {
				if (!o.event) {
					const results = [];
					const template = { event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } };
					for (let i = 0; i < 100; i++) {
						results.push(template);
					}
					return results;
				}
				return o;
			};
			const job = await mp({}, data, { ...opts, recordType: "event", fixData: false, transformFunc: func });
			expect(job.success).toBe(102);
			expect(job.failed).toBe(0);
			expect(job.empty).toBe(0);
			expect(job.total).toBe(3);
		},
		longTimeout
	);

	test(
		"tags: event",
		async () => {
			const data = await mp({}, events, { ...opts, tags: { foo: "bar" } });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"aliases: event",
		async () => {
			const data = await mp({}, events, { ...opts, aliases: { colorTheme: "color", luckyNumber: "lucky!!!" } });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"tags: user",
		async () => {
			const data = await mp({}, people, { ...opts, recordType: `user`, tags: { baz: "qux" } });
			expect(data.success).toBe(5000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"aliases: user",
		async () => {
			const data = await mp({}, people, { ...opts, recordType: `user`, aliases: { colorTheme: "color", luckyNumber: "lucky!!!" } });
			expect(data.success).toBe(5000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"tags: group",
		async () => {
			const data = await mp({}, groups, { ...opts, recordType: `group`, tags: { foo: "bar", mux: "dux", hey: "you", guys: "yo" } });
			expect(data.success).toBe(1860);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"aliases: group",
		async () => {
			const data = await mp({}, groups, { ...opts, recordType: `group`, aliases: { colorTheme: "color", luckyNumber: "lucky!!!" } });
			expect(data.success).toBe(1860);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	const aliases = { row_id: "$insert_id", uuid: "distinct_id", action: "event", timestamp: "time" };
	test(
		"event CSV! (stream)",
		async () => {
			const data = await mp({}, eventsCSV, { ...opts, streamFormat: "csv", aliases, forceStream: true });
			expect(data.success).toBe(10003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"event CSV! (memory)",
		async () => {
			const data = await mp({}, eventsCSV, { ...opts, streamFormat: "csv", aliases, forceStream: false });
			expect(data.success).toBe(10003);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);


	test(
		"scrubs event props",
		async () => {
			const records = [{
				event: 'test',
				properties: {
					$user_id: '123',
					$device_id: '456',
					email: 'ak@foo.com',
					nested: {
						foo: "bar",
						baz: "qux"
					},
					cart: [{
						item: "apple",
						price: 1.02
					}, {
						item: "banana",
						price: 2.03
					}]
				},
			},
			{
				event: 'jest',
				properties: {
					$user_id: '123',
					$device_id: '456',
					email: 'ak@foo.com',
					nested: {
						foo: "bar",
						baz: "qux"
					},
					cart: [{
						item: "apple",
						price: 1.02
					}, {
						item: "banana",
						price: 2.03
					}]
				},
			}];
			const data = await mp({}, records, { ...opts, scrubProperties: ["email", "foo", "item"], dryRun: true });
			const { dryRun: results } = data;
			expect(results.every((e) => !e.properties.email)).toBe(true);
			expect(results.every((e) => !e.properties.nested.foo)).toBe(true);
			expect(results.map(e => e.properties.cart).every(c => !c.item)).toBe(true);
		}
	);


	test(
		"scrubs user props",
		async () => {
			const records = [{
				$distinct_id: 'test',
				$set: {
					$user_id: '123',
					$device_id: '456',
					email: 'ak@foo.com',
					nested: {
						foo: "bar",
						baz: "qux"
					},
					cart: [{
						item: "apple",
						price: 1.02
					}, {
						item: "banana",
						price: 2.03
					}]
				},
			},
			{
				$distinct_id: 'jest',
				$set: {
					$user_id: '123',
					$device_id: '456',
					email: 'ak@foo.com',
					nested: {
						foo: "bar",
						baz: "qux"
					},
					cart: [{
						item: "apple",
						price: 1.02
					}, {
						item: "banana",
						price: 2.03
					}]
				},
			}];
			const data = await mp({}, records, { ...opts, recordType: 'user', scrubProperties: ["email", "foo", "item"], dryRun: true });
			const { dryRun: results } = data;
			expect(results.every((e) => !e.$set.email)).toBe(true);
			expect(results.every((e) => !e.$set.nested.foo)).toBe(true);
			expect(results.map(e => e.$set.cart).every(c => !c.item)).toBe(true);
		}
	);
});

describe("object streams", () => {
	test("events", done => {
		const streamInMem = new Readable.from(eventNinetyNine, { objectMode: true });
		const mpStream = createMpStream({}, { ...opts }, (err, results) => {
			expect(results.success).toBe(9999);
			expect(results.failed).toBe(0);
			expect(results.duration).toBeGreaterThan(0);
			done();
		});
		streamInMem.pipe(mpStream);
	});

	test("users", done => {
		const streamInMem = new Readable.from(moarPpl, { objectMode: true });
		const mpStream = createMpStream({}, { ...opts, recordType: "user" }, (err, results) => {
			expect(results.success).toBe(10000);
			expect(results.failed).toBe(0);
			expect(results.duration).toBeGreaterThan(0);
			done();
		});
		streamInMem.pipe(mpStream);
	});
});

describe("exports", () => {
	test(
		"can export event data",
		async () => {
			const data = await mp({}, null, { ...opts, recordType: "export", start: "2023-01-01", end: "2023-01-03" });
			expect(data.duration).toBeGreaterThan(0);
			expect(data.requests).toBe(1);
			expect(data.failed).toBe(0);
			expect(data.total).toBeGreaterThan(80);
			expect(data.success).toBeGreaterThan(92);
		},
		longTimeout
	);

	test(
		"can export profile data",
		async () => {
			const data = await mp({}, null, { ...opts, recordType: "profile-export" });
			expect(data.duration).toBeGreaterThan(0);
			expect(data.requests).toBeGreaterThan(5);
			expect(data.responses.length).toBeGreaterThan(5);
			expect(data.failed).toBe(0);
			expect(data.total).toBeGreaterThan(5999);
			expect(data.success).toBeGreaterThan(5999);
		},
		longTimeout
	);
});

describe("big files", () => {
	jest.setTimeout(10000);
	test(
		"250k events",
		async () => {
			const data = await mp({}, twoFiftyK, { ...opts, streamFormat: `jsonl` });
			expect(data.success).toBe(250000);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test("large events", async () => {
		const data = await mp({}, "./testData/nykaa/largeEvents.ndjson", { ...opts, streamFormat: `jsonl` });
		expect(data.success).toBe(4077);
		expect(data.total).toBe(2000);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
		expect(data.batches).toBe(22);
		expect(data.errors.length).toBe(0);
	});
});

describe("cli", () => {
	test(
		"events",
		async () => {
			const output = execSync(`node ./index.js ${events} --fixData`).toString().trim().split("\n").pop();

			const result = await u.load(output, true);
			expect(result.success).toBe(5003);
		},
		longTimeout
	);

	test(
		"users",
		async () => {
			const output = execSync(`node ./index.js ${people} --type user --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(5000);
		},
		longTimeout
	);

	test(
		"groups",
		async () => {
			const output = execSync(`node ./index.js ${groups} --type group --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(1860);
		},
		longTimeout
	);

	test(
		"tables",
		async () => {
			const output = execSync(`node ./index.js ${table} --type table --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(1000);
		},
		longTimeout
	);

	test(
		"folder",
		async () => {
			const output = execSync(`node ./index.js  ${folderjsonl} --format jsonl`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(3009);
		},
		longTimeout
	);
});

describe("options", () => {
	test(
		"abridged mode",
		async () => {
			const data = await mp({}, events, { ...opts, abridged: true });
			expect(data.success).toBe(5003);
			expect(data.failed).toBe(0);
			expect(data.responses.length).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"properly removes nulls",
		async () => {
			const data = await mp(
				{},
				[
					{
						event: "nullTester",
						properties: {
							time: 1678931922817,
							distinct_id: "foo",
							$insert_id: "bar",
							actual_null: null,
							undef: undefined,
							"empty str": "",
							zero: 0,
							"bool false": false,
							"empty array": [],
							"empty obj": {},
							"arr of null": [null, null, null]
						}
					}
				],
				{ ...opts, abridged: true, removeNulls: true }
			);
			expect(data.success).toBe(1);
			expect(data.failed).toBe(0);
			expect(data.responses.length).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"time offsets",
		async () => {
			const dataPoint = [
				{
					event: "add to cart 4",
					properties: {
						time: 1678865417,
						distinct_id: "186e5979172b50-05055db7ae8024-1e525634-1fa400-186e59791738d4"
					}
				}
			];

			const data = await mp({}, dataPoint, { ...opts, timeOffset: 7 });
			expect(data.success).toBe(1);
			expect(data.failed).toBe(0);
			expect(data.responses.length).toBe(1);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"where clause",
		async () => {
			const data = await mp({}, null, { ...opts, recordType: "export", start: "2023-01-01", end: "2023-01-01", where: "./tmp/events.ndjson" });
			const folder = await u.ls("./tmp");
			expect(folder[1]).toBe(`/Users/ak/code/mixpanel-import/tmp/events.ndjson`);
			expect(data.duration).toBeGreaterThan(0);
			expect(data.requests).toBe(1);
			expect(data.failed).toBe(0);
			expect(data.total).toBeGreaterThan(33);
			expect(data.success).toBeGreaterThan(33);
		},
		longTimeout
	);
});

describe("data fixes", () => {
	test(
		"deal with /engage payloads",
		async () => {
			const data = [
				{
					$distinct_id: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6",
					$properties: {
						avatar: "https://randomuser.me/api/portraits/women/18.jpg",
						colorTheme: "violet",
						created: "2023-03-04T03:12:23",
						email: "hemiravaw@udte.vg",
						lat: -34.93,
						long: -67.11,
						luckyNumber: 5,
						name: "Louisa de Graaf",
						phone: "+9457951595",
						uuid: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6"
					}
				},
				{
					$distinct_id: "1a632c4e-bccc-55f6-8915-b768479dbe55",
					$properties: {
						avatar: "https://randomuser.me/api/portraits/women/70.jpg",
						colorTheme: "blue",
						created: "2023-03-03T06:56:39",
						email: "weraruz@dapa.pm",
						lat: 0.63,
						long: -40.78,
						luckyNumber: 2,
						name: "Verna Sorelli",
						phone: "+8714237993",
						uuid: "1a632c4e-bccc-55f6-8915-b768479dbe55"
					}
				},
				{
					$distinct_id: "c26f798f-62d4-5f26-8e87-2010d58e5016",
					$properties: {
						avatar: "https://randomuser.me/api/portraits/men/60.jpg",
						colorTheme: "blue",
						created: "2023-03-02T09:12:55",
						email: "jog@mulzitil.tj",
						lat: -82.8,
						long: 51.9,
						luckyNumber: 19,
						name: "Rodney Bonnet",
						phone: "+6827435739",
						uuid: "c26f798f-62d4-5f26-8e87-2010d58e5016"
					}
				}
			];
			const job = await mp({}, data, { ...opts, recordType: "user", fixData: true });
			expect(job.success).toBe(3);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(3);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(1);
		},
		longTimeout
	);

	test(
		"filter out {} /import",
		async () => {
			const data = [{ event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } }, {}, {}, {}];
			const job = await mp({}, data, { ...opts, recordType: "event", fixData: false });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(4);
			expect(job.empty).toBe(3);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(1);
		},
		longTimeout
	);

	test(
		"filter out {} /engage",
		async () => {
			const data = [{ $distinct_id: "foo", token: process.env.MP_TOKEN, $set: { bar: "baz" } }, {}, {}, {}];
			const job = await mp({}, data, { ...opts, recordType: "user", fixData: false });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(4);
			expect(job.empty).toBe(3);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(1);
		},
		longTimeout
	);

	test(
		"flat user props",
		async () => {
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
					uuid: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6"
				}
			];
			const job = await mp({}, data, { ...opts, recordType: "user", fixData: true });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(1);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(1);
		},
		longTimeout
	);

	test(
		"skips bad lines",
		async () => {
			const job = await mp({}, badData, { ...opts, recordType: "user", fixData: true, transformFunc: badDataTrans });
			expect(job.success).toBe(5077);
			expect(job.unparsable).toBe(3);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5080);
			expect(job.empty).toBe(3);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(3);
		},
		longTimeout
	);

	test(
		`can transform out {}'s`,
		async () => {
			const data = [
				{ event: "me" },
				{ event: "you" },
				{ event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } },
				{ event: "foo", properties: { distinct_id: "naz", time: 1681750925148, $insert_id: "4321" } }
			];
			const func = o => {
				if (!o.properties) return {};
				return o;
			};
			const job = await mp({}, data, { ...opts, recordType: "event", fixData: false, transformFunc: func });
			expect(job.success).toBe(2);
			expect(job.failed).toBe(0);
			expect(job.empty).toBe(2);
			expect(job.total).toBe(4);
		},
		longTimeout
	);

	test("fixes time", async () => {
		const data = [
			{
				event: "watch_video",
				properties: {
					distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
					time: "2023-06-09 11:25:31",
					$insert_id: null
				}
			},
			{
				event: "page_view",
				properties: {
					distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
					time: "2023-06-09 11:25:31",
					$insert_id: null
				}
			}
		];

		const job = await mp({}, data, { ...opts, recordType: "event", fixData: true });
		expect(job.success).toBe(2);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(2);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
	});

	test("fixes bad shape", async () => {
		const data = [
			{
				event: "watch_video",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: "2023-06-09 11:25:31",
				$insert_id: null
			},
			{
				event: "page_view",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: "2023-06-09 11:25:31",
				$insert_id: null
			}
		];

		const job = await mp({}, data, { ...opts, recordType: "event", fixData: true });
		expect(job.success).toBe(2);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(2);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
	});

	test("epoch start + end", async () => {
		const data = [
			{
				event: "foo",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: 1691429413,
				$insert_id: "321"
			},
			{
				event: "foo",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: 1691429414,
				$insert_id: "123"
			},
			{
				event: "foo",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: 1691429415,
				$insert_id: "456"
			},
			{
				event: "foo",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: 1691429416,
				$insert_id: "789"
			},
			{
				event: "foo",
				distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
				time: 1691429417,
				$insert_id: "012"
			}
		];

		const job = await mp({}, data, { ...opts, recordType: "event", epochStart: 1691429414, epochEnd: 1691429416 });
		expect(job.success).toBe(3);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(5);
		expect(job.outOfBounds).toBe(2);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
	});

	test(
		"dedupe",
		async () => {
			const job = await mp({}, dupePeople, { ...opts, recordType: "user", dedupe: true });
			expect(job.success).toBe(10);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(2250);
			expect(job.outOfBounds).toBe(0);
			expect(job.duration).toBeGreaterThan(0);
			expect(job.requests).toBe(1);
			expect(job.empty).toBe(2240);
			expect(job.duplicates).toBe(2240);
		},
		longTimeout
	);

	test("fix unparseable", async () => {
		function parseErrorHandler(err, record) {
			let attemptedParse;
			try {
				attemptedParse = JSON.parse(record.replace(/\\\\/g, "\\"));
			} catch (e) {
				attemptedParse = {};
			}
			return attemptedParse;
		}
		const job = await mp({}, heapParseError, { ...opts, recordType: "user", parseErrorHandler });
		expect(job.success).toBe(3);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(8);
		expect(job.outOfBounds).toBe(0);
		expect(job.duration).toBeGreaterThan(0);
		expect(job.requests).toBe(1);
		expect(job.empty).toBe(5);
		expect(job.duplicates).toBe(0);
	});
});

describe("white + blacklist", () => {
	const data = [
		{
			event: "foo",
			distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
			time: 1691429413,
			$insert_id: "321",
			happy: "kinda"
		},
		{
			event: "bar",
			distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
			time: 1691429414,
			$insert_id: "123",
			sad: "sorta"
		},
		{
			event: "baz",
			distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
			time: 1691429415,
			$insert_id: "456",
			maybe: "cool"
		},
		{
			event: "qux",
			distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
			time: 1691429416,
			$insert_id: "789",
			deal: "with it"
		},
		{
			event: "mux",
			distinct_id: "24377a8a-8096-55d4-be61-54010bc27adf",
			time: 1691429417,
			$insert_id: "012",
			because: "why",
			happy: "nope"
		}
	];

	test(
		"event: whitelist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", eventWhitelist: ["foo", "baz"] });
			expect(job.success).toBe(2);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(3);
			expect(job.whiteListSkipped).toBe(3);
		},
		longTimeout
	);

	test(
		"event: blacklist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", eventBlacklist: ["mux", "qux"] });
			expect(job.success).toBe(3);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(2);
			expect(job.blackListSkipped).toBe(2);
		},
		longTimeout
	);

	test(
		"prop key: whitelist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", propKeyWhitelist: ["because"] });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(4);
			expect(job.whiteListSkipped).toBe(4);
		},
		longTimeout
	);

	test(
		"prop key: blacklist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", propKeyBlacklist: ["happy"] });
			expect(job.success).toBe(3);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(2);
			expect(job.blackListSkipped).toBe(2);
		},
		longTimeout
	);

	test(
		"prop val: whitelist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", propValWhitelist: ["cool"] });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(4);
			expect(job.whiteListSkipped).toBe(4);
		},
		longTimeout
	);

	test(
		"prop val: blacklist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", propValBlacklist: ["with it"] });
			expect(job.success).toBe(4);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(5);
			expect(job.empty).toBe(1);
			expect(job.blackListSkipped).toBe(1);
		},
		longTimeout
	);

	test("dry runs", async () => {
		const csvData = `event_type,event_action,screen_type,screen_category,screen_name,phone_number,url,survey_key,questions_answered_count,survey_score,app_user_id,event_datetime
allowNotification,set,App Navigation,,app:#/acceptInvitationCodeView,,,,,,00000000-6476-d725-cd82-7d45c82c3dd6,6/13/23 11:08
allowNotification,set,App Navigation,,app:#/acceptInvitationCodeView,,,,,,00000000-6476-d781-cd82-7d217c2c4b69,6/9/23 17:01
allowNotification,cancel,App Navigation,,app:#/eligibilityCheckView,,,,,,00000000-6476-d767-cd82-7d65a52c47d7,6/13/23 6:04
allowNotification,cancel,App Navigation,,app:#/eligibilityCheckView,,,,,,00000000-6476-d7b8-cd82-7d62362c5336,6/18/23 16:50
allowNotification,set,App Navigation,,app:#/eligibilityCheckView,,,,,,00000000-6476-d7d2-cd82-7d1a882c5716,6/2/23 3:30
allowNotification,set,App Navigation,,app:#/eligibilityCheckView,,,,,,00000000-6476-d7df-cd82-7dfc4b2c58d6,6/7/23 1:06
allowNotification,set,App Navigation,,app:#/eligibilityCheckView,,,,,,00000000-648a-2546-79f7-94f8072b002e,6/14/23 20:43
allowNotification,set,App Navigation,,app:#/eligibilityHelpView,,,,,,00000000-6476-d703-cd82-7d83e12c38bf,6/6/23 20:59
allowNotification,set,App Navigation,,app:#/intakeForm,,,,,,00000000-6476-d76f-cd82-7d95de2c48e6,6/10/23 23:22
allowNotification,set,App Navigation,,app:#/OnBoardingSurveyView/welcome/introduction,,,,,,00000000-6476-d6b6-cd82-7d60ee2c2d81,6/14/23 2:53`;

		const expected = `[{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/acceptInvitationCodeView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d725-cd82-7d45c82c3dd6","time":1686668880000,"$insert_id":"2371213552"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/acceptInvitationCodeView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d781-cd82-7d217c2c4b69","time":1686344460000,"$insert_id":"4063295875"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"cancel","screen_category":"","screen_name":"app:#/eligibilityCheckView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d767-cd82-7d65a52c47d7","time":1686650640000,"$insert_id":"3850300538"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"cancel","screen_category":"","screen_name":"app:#/eligibilityCheckView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d7b8-cd82-7d62362c5336","time":1687121400000,"$insert_id":"185936942"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/eligibilityCheckView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d7d2-cd82-7d1a882c5716","time":1685691000000,"$insert_id":"4127120184"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/eligibilityCheckView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d7df-cd82-7dfc4b2c58d6","time":1686114360000,"$insert_id":"1740874358"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/eligibilityCheckView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-648a-2546-79f7-94f8072b002e","time":1686789780000,"$insert_id":"29978696"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/eligibilityHelpView","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d703-cd82-7d83e12c38bf","time":1686099540000,"$insert_id":"2516290719"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/intakeForm","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d76f-cd82-7d95de2c48e6","time":1686453720000,"$insert_id":"911076893"}},{"event":"App Navigation","properties":{"event_type":"allowNotification","event_action":"set","screen_category":"","screen_name":"app:#/OnBoardingSurveyView/welcome/introduction","phone_number":"","url":"","survey_key":"","questions_answered_count":"","survey_score":"","distinct_id":"00000000-6476-d6b6-cd82-7d60ee2c2d81","time":1686725580000,"$insert_id":"2840891370"}}]`;
		const job = await mp({}, csvData, {
			...opts,
			recordType: "event",
			dryRun: true,
			streamFormat: "csv",
			aliases: { screen_type: "event", app_user_id: "distinct_id", event_datetime: "time" }
		});
		expect(job.total).toBe(10);
		expect(job.dryRun.length).toBe(10);


	});
});




function badDataTrans(badData) {
	const mixpanelProfile = {
		$distinct_id: badData.identity || badData.id || "none",
		$ip: badData.initial_ip,
		$set: badData
	};

	return mixpanelProfile;
}
