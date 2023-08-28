// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
const _ = require("highland");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const { Readable } = require("stream");

// ! MODULES
const Job = require("../components/job.js");
const { UTCoffset, addTags, applyAliases, dedupeRecords, ezTransforms, removeNulls, whiteAndBlackLister } = require("../components/transforms.js");
const { getEnvVars, JsonlParser, chunkForSize, determineDataType, existingStreamInterface, itemStream } = require("../components/parsers.js");
const fakeCreds = { acct: "test", pass: "test", project: "test" };

describe("job config", () => {
	test("creds required", () => {
		expect(() => {
			new Job();
		}).toThrow("no credentials provided!");
	});

	test("default params", () => {
		const job = new Job(fakeCreds);
		expect(job.start).toBeDefined();
		expect(job.end).toBeDefined();
	});

	test("can store stuff", () => {
		const job = new Job(fakeCreds);
		job.store("response", true);
		expect(job.responses.length).toBe(1);
		expect(job.responses[0]).toBe("response");
	});

	test("stores errors too", () => {
		const job = new Job(fakeCreds);
		job.store("error_response", false);
		expect(job.errors.length).toBe(1);
		expect(job.errors[0]).toBe("error_response");
	});

	test("auth string", () => {
		const creds = fakeCreds;
		const job = new Job(creds);
		expect(job.resolveProjInfo()).toBe(`Basic ${Buffer.from(creds.acct + ":" + creds.pass, "binary").toString("base64")}`);
	});

	test("summary", () => {
		const job = new Job(fakeCreds);
		const summary = job.summary();
		expect(summary).toHaveProperty("recordType");
		expect(summary).toHaveProperty("total");
		expect(summary).toHaveProperty("success");
		// ... (more properties to check)
	});
});

describe("transforms", () => {
	const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];

	// Sample JobConfig for testing:
	const sampleJobConfig = {
		recordType: "event",
		tags: { tagKey: "tagValue" },
		aliases: { oldKey: "newKey" },
		token: "sampleToken",
		groupKey: "sampleGroupKey",
		hashTable: new Set(),
		duplicates: 0,
		whiteListSkipped: 0,
		blackListSkipped: 0
	};

	test("fix time", () => {
		const config = { recordType: "event" };
		const record = {
			event: "TestEvent",
			properties: {
				time: dayjs().toString(),
				distinct_id: "123"
			}
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.properties.time).toBeNumber();
		expect(transformed.properties.$insert_id).toBeTruthy();
	});

	test("fix shape", () => {
		const config = { recordType: "event" };
		const record = {
			event: "TestEvent",
			time: dayjs().toString(),
			distinct_id: "123"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.properties.time).toBeNumber();
		expect(transformed.properties.$insert_id).toBeTruthy();
		expect(transformed.time).toBeUndefined();
	});

	test("adds token", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			$set: { name: "John" }
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$set.name).toBe("John");
	});

	test("fix profile shape", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			name: "John"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$set.name).toBe("John");
		expect(transformed.name).toBeUndefined();
	});

	test("add group token", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "testGroupKey" };
		const record = {
			$group_id: "123",
			$group_key: "customGroupKey",
			$set: { name: "GroupA" }
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$group_key).toBe("customGroupKey");
		expect(transformed.$set.name).toBe("GroupA");
	});

	test("fix group shape", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "testGroupKey" };
		const record = {
			$group_id: "123",
			name: "GroupA"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$group_key).toBe("testGroupKey");
		expect(transformed.$set.name).toBe("GroupA");
		expect(transformed.name).toBeUndefined();
	});

	test("noop if good", () => {
		const config = { recordType: "unknown" };
		const record = { data: "test" };
		const transformed = ezTransforms(config)(record);
		expect(transformed).toEqual(record);
	});

	test("null remover", () => {
		const record = { properties: { key1: null, key2: "", key3: "value", key4: undefined } };
		expect(removeNulls()(record)).toEqual({ properties: { key3: "value" } });
	});
	test("tag adder", () => {
		const record = { event: "click", properties: { key1: "value1" } };
		expect(addTags(sampleJobConfig)(record)).toEqual({ event: "click", properties: { key1: "value1", tagKey: "tagValue" } });
	});
	test("rename keys", () => {
		const record = { event: "click", properties: { oldKey: "value1" } };
		expect(applyAliases(sampleJobConfig)(record)).toEqual({ event: "click", properties: { newKey: "value1" } });
	});
	test("time offset", () => {
		const now = dayjs.utc();
		const twoHoursAgo = now.subtract(2, "h").valueOf();
		const record = { properties: { time: now.unix() } };
		expect(UTCoffset(-2)(record).properties.time.toString().slice(0, 10)).toEqual(twoHoursAgo.toString().slice(0, 10));
	});
	test("dedupe", () => {
		const record1 = { event: "click", properties: { key: "value" } };
		const record2 = { event: "click", properties: { key: "value" } };

		expect(dedupeRecords(sampleJobConfig)(record1)).toEqual(record1);
		expect(dedupeRecords(sampleJobConfig)(record2)).toEqual({}); // this should be filtered out
	});

	test("event: whitelist", () => {
		const params = {
			eventWhitelist: ["allowedEvent"]
		};
		const recordAllowed = { event: "allowedEvent", properties: { key: "value" } };
		const recordDisallowed = { event: "disallowedEvent", properties: { key: "value" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("event: blacklist", () => {
		const params = {
			eventBlacklist: ["disallowedEvent"]
		};
		const recordAllowed = { event: "allowedEvent", properties: { key: "value" } };
		const recordDisallowed = { event: "disallowedEvent", properties: { key: "value" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("keys: whitelist", () => {
		const params = {
			propKeyWhitelist: ["allowedKey"]
		};
		const recordAllowed = { properties: { allowedKey: "value" } };
		const recordDisallowed = { properties: { disallowedKey: "value" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("keys: blacklist", () => {
		const params = {
			propKeyBlacklist: ["disallowedKey"]
		};
		const recordAllowed = { properties: { allowedKey: "value" } };
		const recordDisallowed = { properties: { disallowedKey: "value" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("values: whitelist", () => {
		const params = {
			propValWhitelist: ["allowedValue"]
		};
		const recordAllowed = { properties: { key: "allowedValue" } };
		const recordDisallowed = { properties: { key: "disallowedValue" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("values: blacklist", () => {
		const params = {
			propValBlacklist: ["disallowedValue"]
		};
		const recordAllowed = { properties: { key: "allowedValue" } };
		const recordDisallowed = { properties: { key: "disallowedValue" } };

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});
});

describe("parsers", () => {
	const mockJobConfig = {
		bytesPerBatch: 1000, // For example
		recordsPerBatch: 5 // For example
	};

	test("chunk: drop items", done => {
		const oversizedItem = { data: "a".repeat(1100) };
		const generator = (push, next) => {
			push(null, [oversizedItem]);
			push(null, _.nil);
		};
		const stream = _(generator).consume(chunkForSize(mockJobConfig));

		stream.toArray(data => {
			expect(data.length).toBe(0);
			done();
		});
	});

	test("chunk: maxBatchSize", done => {
		const normalItem = { data: "a".repeat(300) };
		const generator = (push, next) => {
			push(null, [normalItem, normalItem, normalItem, normalItem]);
			push(null, _.nil);
		};
		const stream = _(generator).consume(chunkForSize(mockJobConfig));

		stream.toArray(data => {
			expect(data.length).toBe(2);
			expect(data[0].length).toBe(3);
			expect(data[1].length).toBe(1);
			done();
		});
	});

	test("chun: maxBatchCount", done => {
		const smallItem = { data: "a".repeat(10) };
		const generator = (push, next) => {
			push(null, Array(10).fill(smallItem));
			push(null, _.nil);
		};
		const stream = _(generator).consume(chunkForSize(mockJobConfig));

		stream.toArray(data => {
			expect(data.length).toBe(2);
			expect(data[0].length).toBe(5);
			expect(data[1].length).toBe(5);
			done();
		});
	});

	// todo: handle errors...
	// test("handle errors", done => {
	// 	const errorMsg = "Test Error";
	// 	const generator = (push, next) => {
	// 		push(new Error(errorMsg));
	// 		push(null, _.nil);
	// 	};
	// 	const stream = _(generator).consume(chunkForSize(mockJobConfig));

	// 	stream.errors(err => {
	// 		expect(err.message).toBe(errorMsg);
	// 		done();
	// 	});
	// });

	test("export: where", async () => {
		const jobConfig = {
			recordType: "export",
			where: "/path/to/file"
		};
		const result = await determineDataType({}, jobConfig);
		expect(result).toBe("/path/to/file");
	});

	// ... other 'export' type scenarios

	test("people export: where", async () => {
		const jobConfig = {
			recordType: "peopleExport",
			where: "/path/to/folder"
		};
		const result = await determineDataType({}, jobConfig);
		expect(result).toBe("/path/to/folder");
	});

	test("object streams", async () => {
		const mockStream = new Readable({ objectMode: true });
		mockStream.push({ some: "data" });
		mockStream.push(null);
		const jobConfig = {};

		const result = await determineDataType(mockStream, jobConfig);
		expect(result).toBeInstanceOf(require("stream").Stream);
	});
});
