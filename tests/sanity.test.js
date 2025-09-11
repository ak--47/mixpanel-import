// @ts-nocheck
/* NOTE: to make tests work, you need a .env file of the form

MP_PROJECT=project
MP_ACCT=acct
MP_PASS=password
MP_SECRET=secret
MP_TOKEN=token

*/

/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
require("dotenv").config();
const { execSync } = require("child_process");
const longTimeout = 60000; // Reduced from 750900
const shortTimeout = 15000;
jest.setTimeout(shortTimeout);

const {
	MP_PROJECT = "",
	MP_ACCT = "",
	MP_PASS = "",
	MP_SECRET = "",
	MP_TOKEN = "",
	MP_TABLE_ID = "",
	MP_PROFILE_EXPORT_TOKEN = "",
	MP_PROFILE_EXPORT_SECRET = "",
	MP_PROFILE_EXPORT_GROUP_KEY = "",
	MP_PROFILE_EXPORT_DATAGROUP_ID = ""
} = process.env;

if (!MP_PROJECT || !MP_ACCT || !MP_PASS || !MP_SECRET || !MP_TOKEN || !MP_TABLE_ID) {
	console.error("Please set the following environment variables: MP_PROJECT, MP_ACCT, MP_PASS, MP_SECRET, MP_TOKEN, MP_TABLE_ID");
	process.exit(1);
}

function isDebugMode() {
	// Check for Node.js debug flags
	if (process.execArgv.some(arg => arg.includes('--inspect') || arg.includes('--debug'))) {
		return true;
	}

	// Check NODE_OPTIONS
	if (process.env.NODE_OPTIONS?.match(/--inspect|--debug/)) {
		return true;
	}

	// Check if debugger port is set
	if (process.debugPort) {
		return true;
	}

	// VS Code specific
	if (process.env.VSCODE_DEBUG === 'true') {
		return true;
	}

	return false;
}

const IS_DEBUG_MODE = isDebugMode();

const mp = require("../index.js");
const { createMpStream } = require("../index.js");
const { createReadStream } = require("fs");
const { Readable, Transform, Writable, PassThrough } = require("stream");
const u = require("ak-tools");

// Small test data files
const events = `./testData/events-small.ndjson`;
const eventsJSON = `./testData/events-small.json`;
const people = `./testData/people-small.ndjson`;
const groups = `./testData/groups-small.ndjson`;
const table = `./testData/table-small.csv`;

// Small in-memory test data
const smallEvents = [
	{"event":"test event","properties":{"distinct_id":"user-1","time":1666488875497,"$source":"sanity test"}},
	{"event":"another event","properties":{"distinct_id":"user-2","time":1666488876497,"$source":"sanity test"}}
];

const smallPeople = [
	{"$distinct_id":"user-1","$set":{"name":"Alice","email":"alice@test.com"}},
	{"$distinct_id":"user-2","$set":{"name":"Bob","email":"bob@test.com"}}
];

const smallScdData = [
	{"distinct_id":"user-1","NPS":8,"time":"2023-01-01"},
	{"distinct_id":"user-1","NPS":9,"time":"2023-02-01"},
	{"distinct_id":"user-2","NPS":7,"time":"2023-01-01"}
];

/** @type {import('../index.d.ts').Options} */
const opts = {
	recordType: `event`,
	compress: false,
	workers: 5, // Reduced from 20
	region: `US`,
	recordsPerBatch: 100, // Reduced from 2000
	bytesPerBatch: 1024 * 64, // Reduced from 2MB
	strict: true,
	logs: false,
	fixData: true,
	showProgress: true,
	verbose: false,
	streamFormat: "jsonl",
	transformFunc: function noop(a) {
		return a;
	}
};

describe("sanity: filenames", () => {
	test(
		"event",
		async () => {
			const data = await mp({}, events, { ...opts });
			expect(data.success).toBe(5);
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
			expect(data.success).toBe(3);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"group",
		async () => {
			const data = await mp({}, groups, { ...opts, recordType: `group` });
			expect(data.success).toBe(3);
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
			expect(data.success).toBe(3);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"scd (user)",
		async () => {
			const result = await mp({}, smallScdData, { ...opts, recordType: `scd`, scdKey: "NPS", scdType: "number", scdLabel: 'net-promo-score', fixData: true });
			const { success, failed, duration, total } = result;
			expect(success).toBe(3);
			expect(failed).toBe(0);
			expect(duration).toBeGreaterThan(0);
			expect(total).toBe(3);
		}
	);

	test(
		"scd (+ profiles)",
		async () => {
			const result = await mp({}, smallScdData, { ...opts, createProfiles: true, recordType: `scd`, scdKey: "NPS", scdType: "number", scdLabel: 'net-promo-score', fixData: true });
			const { success, failed, duration, total } = result;
			expect(success).toBeGreaterThan(2);
			expect(failed).toBe(0);
			expect(duration).toBeGreaterThan(0);
			expect(total).toBeGreaterThan(2);
		}
	);
});

describe("sanity: folders", () => {
	test(
		"array of filenames (JSONL)",
		async () => {
			const data = await mp({}, [events, events], {
				...opts,
				streamFormat: "jsonl"
			});
			expect(data.success).toBe(10);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
			expect(data).toHaveProperty("startTime");
			expect(data).toHaveProperty("endTime");
		},
		longTimeout
	);
});

describe("sanity: in memory", () => {
	test(
		"events",
		async () => {
			const data = await mp({}, smallEvents, { ...opts });
			expect(data.success).toBe(2);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"users",
		async () => {
			const data = await mp({ token: MP_TOKEN }, smallPeople, { ...opts, recordType: "user", fixData: true });
			expect(data.success).toBe(2);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("sanity: inference", () => {
	test("parses strict_json when explicitly specified", async () => {
		const data = await mp({}, eventsJSON, { ...opts, streamFormat: "strict_json" });
		expect(data.success).toBe(3);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test("infers ndjson", async () => {
		const data = await mp({}, events, { ...opts, streamFormat: "" });
		expect(data.success).toBe(5);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
		expect(data).toHaveProperty("startTime");
		expect(data).toHaveProperty("endTime");
	});

	test("handles strict_json format with explicit streamFormat", async () => {
		// Test with a file that contains a JSON array - requires explicit strict_json format
		const data = await mp({}, './testData/multijson/1.json', { ...opts, streamFormat: "strict_json" });
		expect(data.success).toBeGreaterThan(0);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test("auto-detects .json files as JSONL format", async () => {
		// Test that .json files are now auto-detected as JSONL (newline-delimited) format
		// Using events.json which is actually JSONL format despite .json extension
		const data = await mp({}, './testData/events.json', { ...opts, streamFormat: "" });
		expect(data.success).toBeGreaterThan(0);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});

	test("infers csv", async () => {
		const data = await mp({}, table, { ...opts, strict: false, streamFormat: "none", aliases: { id: "$insert_id", name: "distinct_id", category: "event" }, forceStream: false });
		expect(data.success).toBe(3);
		expect(data.failed).toBe(0);
		expect(data.duration).toBeGreaterThan(0);
	});
});

describe("sanity: file streams", () => {
	test(
		"event",
		async () => {
			const data = await mp({}, createReadStream(events), { ...opts });
			expect(data.success).toBe(5);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"user",
		async () => {
			const data = await mp({}, createReadStream(people), { ...opts, recordType: `user` });
			expect(data.success).toBe(3);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"group",
		async () => {
			const data = await mp({}, createReadStream(groups), { ...opts, recordType: `group` });
			expect(data.success).toBe(3);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);
});

describe("sanity: transform", () => {
	test(
		"can use custom transform",
		async () => {
			const testData = [
				{"event":"test","time":"2023-01-01","distinct_id":"user-1"},
				{"event":"test2","time":"2023-01-02","distinct_id":"user-2"}
			];
			const data = await mp({}, testData, {
				...opts,
				transformFunc: ev => {
					const eventModel = {
						event: ev.event,
						properties: { ...ev }
					};
					eventModel.properties.time = new Date(eventModel.properties.time).getTime() / 1000;
					return eventModel;
				}
			});
			expect(data.success).toBe(2);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"can explode records",
		async () => {
			const data = [
				{ event: false },
				{ event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } }
			];
			const func = o => {
				if (!o.event) {
					const results = [];
					const template = { event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } };
					for (let i = 0; i < 3; i++) { // Reduced from 100
						results.push(template);
					}
					return results;
				}
				return o;
			};
			const job = await mp({}, data, { ...opts, recordType: "event", fixData: false, transformFunc: func });
			expect(job.success).toBe(4);
			expect(job.failed).toBe(0);
			expect(job.empty).toBe(0);
			expect(job.total).toBe(2);
		},
		longTimeout
	);

	test(
		"tags: event",
		async () => {
			const data = await mp({}, smallEvents, { ...opts, tags: { foo: "bar" } });
			expect(data.success).toBe(2);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"aliases: event",
		async () => {
			const data = await mp({}, smallEvents, { ...opts, aliases: { event: "eventName" } });
			expect(data.success).toBe(2);
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
					email: 'ak@foo.com',
					nested: {
						foo: "bar"
					}
				},
			}];
			const data = await mp({}, records, { ...opts, scrubProperties: ["email", "foo"], dryRun: true });
			const { dryRun: results } = data;
			expect(results.every((e) => !e.properties.email)).toBe(true);
			expect(results.every((e) => !e.properties.nested.foo)).toBe(true);
		}
	);
});

// describe("sanity: exports", () => {
// 	test(
// 		"export event data to file",
// 		async () => {
// 			const data = await mp({}, null, { ...opts, recordType: "export", start: "2023-01-01", end: "2023-01-01" });
// 			expect(data.duration).toBeGreaterThan(0);
// 			expect(data.requests).toBe(1);
// 			expect(data.failed).toBe(0);
// 			expect(data.total).toBeGreaterThan(10);
// 			expect(data.success).toBeGreaterThan(10);
// 		},
// 		longTimeout
// 	);

// 	test(
// 		"export event data in memory",
// 		async () => {
// 			const data = await mp({}, null, { ...opts, skipWriteToDisk: true, recordType: "export", start: "2023-01-01", end: "2023-01-01" });
// 			const { dryRun, success, total } = data;
// 			const minRecords = 10;
// 			expect(dryRun.length).toBeGreaterThan(minRecords);
// 			expect(success).toBeGreaterThan(minRecords);
// 			expect(total).toBeGreaterThan(minRecords);
// 			expect(dryRun.every(e => e.event)).toBe(true);
// 			expect(dryRun.every(e => e.properties)).toBe(true);
// 		},
// 		longTimeout
// 	);
// });

describe("sanity: fixing stuff", () => {
	test(
		"deal with /engage payloads",
		async () => {
			const data = [{
				$distinct_id: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6",
				$properties: {
					name: "Test User",
					email: "test@example.com"
				}
			}];
			const job = await mp({}, data, { ...opts, recordType: "user", fixData: true });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(1);
		},
		longTimeout
	);

	test(
		"filter out {} /import",
		async () => {
			const data = [{ event: "foo", properties: { distinct_id: "bar", time: 1681750925188, $insert_id: "1234" } }, {}, {}];
			const job = await mp({}, data, { ...opts, recordType: "event", fixData: false });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(3);
			expect(job.empty).toBe(2);
		},
		longTimeout
	);

	test(
		"flat user props",
		async () => {
			const data = [{
				$distinct_id: "28e929d8-46aa-5dda-8941-0cb6a6cff1c6",
				name: "Test User",
				email: "test@example.com"
			}];
			const job = await mp({}, data, { ...opts, recordType: "user", fixData: true });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(1);
		},
		longTimeout
	);

	test("fixes time", async () => {
		const data = [{
			event: "test_event",
			properties: {
				distinct_id: "user-1",
				time: "2023-06-09 11:25:31",
				$insert_id: "test-1"
			}
		}];
		const job = await mp({}, data, { ...opts, recordType: "event", fixData: true });
		expect(job.success).toBe(1);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(1);
	});

	test("fixes bad shape", async () => {
		const data = [{
			event: "test_event",
			distinct_id: "user-1",
			time: "2023-06-09 11:25:31",
			$insert_id: "test-1"
		}];
		const job = await mp({}, data, { ...opts, recordType: "event", fixData: true });
		expect(job.success).toBe(1);
		expect(job.failed).toBe(0);
		expect(job.total).toBe(1);
	});
});

describe("sanity: white + blacklist", () => {
	const data = [
		{
			event: "foo",
			distinct_id: "user-1",
			time: 1691429413,
			$insert_id: "321",
			happy: "kinda"
		},
		{
			event: "bar",
			distinct_id: "user-2",
			time: 1691429414,
			$insert_id: "123",
			sad: "sorta"
		}
	];

	test(
		"event: whitelist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", eventWhitelist: ["foo"] });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(2);
			expect(job.empty).toBe(1);
			expect(job.whiteListSkipped).toBe(1);
		},
		longTimeout
	);

	test(
		"event: blacklist",
		async () => {
			const job = await mp({}, data, { ...opts, recordType: "event", eventBlacklist: ["bar"] });
			expect(job.success).toBe(1);
			expect(job.failed).toBe(0);
			expect(job.total).toBe(2);
			expect(job.empty).toBe(1);
			expect(job.blackListSkipped).toBe(1);
		},
		longTimeout
	);

	test("dry runs", async () => {
		const csvData = `event_type,app_user_id,event_datetime
test,user-1,6/13/23 11:08
test2,user-2,6/9/23 17:01
test3,user-3,6/8/23 15:00
test4,user-4,6/7/23 14:00
test5,user-5,6/6/23 13:00
test6,user-6,6/5/23 12:00
test7,user-7,6/4/23 11:00
test8,user-8,6/3/23 10:00
test9,user-9,6/2/23 9:00
test10,user-10,6/1/23 08:00
test11,user-11,5/31/23 07:00
test12,user-12,5/30/23 06:00
test13,user-13,5/29/23 05:00
test14,user-14,5/28/23 04:00
test15,user-15,5/27/23 03:00
test16,user-16,5/26/23 02:00
test17,user-17,5/25/23 01:00
test18,user-18,5/24/23 00:00
test19,user-19,5/23/23 23:00
test20,user-20,5/22/23 22:00
test21,user-21,5/21/23 21:00
test22,user-22,5/20/23 20:00
test23,user-23,5/19/23 19:00
test24,user-24,5/18/23 18:00
test25,user-25,5/17/23 17:00
test26,user-26,5/16/23 16:00
test27,user-27,5/15/23 15:00
test28,user-28,5/14/23 14:00
test29,user-29,5/13/23 13:00
test30,user-30,5/12/23 12:00`;


		const job = await mp({}, csvData, {
			...opts,
			recordType: "event",
			dryRun: true,
			streamFormat: "csv",
			aliases: { event_type: "event", app_user_id: "distinct_id", event_datetime: "time" }
		});
		expect(job.total).toBe(30);
		expect(job.dryRun.length).toBe(30);
	});
});

describe("sanity: cli", () => {
	test(
		"events",
		async () => {
			const output = execSync(`node ./index.js ${events} --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(5);
		},
		longTimeout
	);

	test(
		"users",
		async () => {
			const output = execSync(`node ./index.js ${people} --type user --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(3);
		},
		longTimeout
	);

	test(
		"groups",
		async () => {
			const output = execSync(`node ./index.js ${groups} --type group --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(3);
		},
		longTimeout
	);

	test(
		"tables",
		async () => {
			const output = execSync(`node ./index.js ${table} --type table --fixData`).toString().trim().split("\n").pop();
			const result = await u.load(output, true);
			expect(result.success).toBe(3);
		},
		longTimeout
	);
});

describe("sanity: options", () => {
	test(
		"abridged mode",
		async () => {
			const data = await mp({}, smallEvents, { ...opts, abridged: true });
			expect(data.success).toBe(2);
			expect(data.failed).toBe(0);
			expect(data.responses).toBe(undefined);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"properly removes nulls",
		async () => {
			const data = await mp(
				{},
				[{
					event: "nullTester",
					properties: {
						time: 1678931922817,
						distinct_id: "foo",
						$insert_id: "bar",
						actual_null: null,
						undef: undefined,
						"empty str": "",
						zero: 0,
						"bool false": false
					}
				}],
				{ ...opts, abridged: true, removeNulls: true }
			);
			expect(data.success).toBe(1);
			expect(data.failed).toBe(0);
		},
		longTimeout
	);

	test(
		"time offsets",
		async () => {
			const dataPoint = [{
				event: "test event",
				properties: {
					time: 1678865417,
					distinct_id: "user-1"
				}
			}];
			const data = await mp({}, dataPoint, { ...opts, timeOffset: 7 });
			expect(data.success).toBe(1);
			expect(data.failed).toBe(0);
		},
		longTimeout
	);
});

function badDataTrans(badData) {
	const mixpanelProfile = {
		$distinct_id: badData.identity || badData.id || "none",
		$ip: badData.initial_ip,
		$set: badData
	};
	return mixpanelProfile;
}