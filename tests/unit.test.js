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
const murmurhash = require("murmurhash");
const stringify = require("json-stable-stringify");

// ! MODULES
const Job = require("../components/job.js");
const { UTCoffset,
	addTags,
	applyAliases,
	dedupeRecords,
	ezTransforms,
	removeNulls,
	whiteAndBlackLister,
	flattenProperties,
	addInsert,
	fixJson,
	resolveFallback,
	scrubProperties,
	addToken
} = require("../components/transforms.js");

const { getEnvVars,
	JsonlParser,
	chunkForSize,
	determineDataType,
	existingStreamInterface,
	itemStream
} = require("../components/parsers.js");
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

	const flattenProps = flattenProperties();

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

	test("gets user_id", () => {
		const config = { recordType: "event" };
		const record = {
			event: "TestEvent",
			time: dayjs().toString(),
			user_id: "123"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.properties.time).toBeNumber();
		expect(transformed.properties.$insert_id).toBeTruthy();
		expect(transformed.properties.$user_id).toBe("123");
		expect(transformed.properties.user_id).toBeUndefined();
		
	});

	test("gets device_id", () => {
		const config = { recordType: "event" };
		const record = {
			event: "TestEvent",
			time: dayjs().toString(),
			device_id: "123"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.properties.time).toBeNumber();
		expect(transformed.properties.$insert_id).toBeTruthy();
		expect(transformed.properties.$device_id).toBe("123");
		expect(transformed.properties.device_id).toBeUndefined();
		
	});

	test("gets source", () => {
		const config = { recordType: "event" };
		const record = {
			event: "TestEvent",
			time: dayjs().toString(),
			device_id: "123",
			source: "web"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.properties.time).toBeNumber();
		expect(transformed.properties.$insert_id).toBeTruthy();
		expect(transformed.properties.$source).toBe("web");
		expect(transformed.properties.source).toBeUndefined();
		
	});

	test("adds token (implicit)", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			$set: { name: "John" }
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$set.name).toBe("John");
	});

	test("adds events token (explicit)", () => {
		const config = { recordType: "event", token: "testToken" };
		const record = {
			event: "foo",
			properties: { name: "bar" }
		};
		const transformed = addToken(config)(record);
		expect(transformed.properties.token).toBe("testToken");
	});


	test("adds users token (explicit)", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			$set: { name: "John" }
		};
		const transformed = addToken(config)(record);
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


	test("flatten: nested objects", () => {
		const record = {
			event: "foo",
			properties: {
				nested: { key1: "value1", key2: "value2" },
				key3: "value3"
			}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			properties: {
				"nested.key1": "value1",
				"nested.key2": "value2",
				key3: "value3"
			}
		});
	});

	test("flatten: ignore arrays", () => {
		const record = {
			event: "foo",
			properties: {
				array: [1, 2, 3],
				key: "value"
			}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			properties: {
				array: [1, 2, 3],
				key: "value"
			}
		});
	});

	test("flatten: handle empty", () => {
		const record = {
			event: "foo",
			properties: {}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			properties: {}
		});
	});

	test("flatten: non-objects", () => {
		const record = {
			event: "foo",
			properties: {
				key1: "value1",
				key2: 123,
				key3: true
			}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			properties: {
				key1: "value1",
				key2: 123,
				key3: true
			}
		});
	});

	test("flatten: deep nested", () => {
		const record = {
			event: "foo",
			properties: {
				nested: { level2: { key: "value" } },
				key: "value"
			}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			properties: {
				"nested.level2.key": "value",
				key: "value"
			}
		});
	});

	test("flaten: $set as well", () => {
		const record = {
			event: "foo",
			$set: {
				nested: { key1: "value1", key2: "value2" },
				key3: "value3"
			}
		};
		expect(flattenProps(record)).toEqual({
			event: "foo",
			$set: {
				"nested.key1": "value1",
				"nested.key2": "value2",
				key3: "value3"
			}
		});
	});

	test("flatten: don't break", () => {
		const record = { event: "foo" };
		expect(flattenProps(record)).toEqual({});
	});


	test("insert_id: basic", () => {
		const record = {
			event: "userLogin",
			distinct_id: "user123",
			time: "2021-01-01T00:00:00Z",
			properties: {}
		};
		const enhanceRecord = addInsert(["event", "distinct_id", "time"]);
		const enhancedRecord = enhanceRecord(record);
		const expectedInsertId = murmurhash.v3([record.event, record.distinct_id, record.time].join("-")).toString();

		expect(enhancedRecord.properties.$insert_id).toEqual(expectedInsertId);
	});

	test("insert_id: always the same", () => {
		const record = {
			"event": "addNewAddressAction",
			"properties": {
				"$soure": "mp-historical-import-apr-2024",
				"appType": {
					"member0": "SNAPLITE"
				},
				"time": 1704392999755,
				"distinct_id": "94f91ab1-76bf-49ef-9c7b-c406a2cc5953",
				"assetVersion": "125",
				"browserDetails": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
				"clientTimestamp": "1704392998944",
				"cookie": "170439279814661197",
				"deviceType": "WAP",
				"dpDay": "4",
				"dpHour": "23",
				"dpMonth": "1",
				"dpYM": "202401",
				"dpYMD": "20240104",
				"dpYear": "2024",
				"eventId": "1704392998931_5082_170439279814661197",
				"eventKey": "addNewAddressAction",
				"eventName": "addNewAddressAction",
				"eventType": "clickStream",
				"imsId": "VjUwIzFjOWM4OTQwLTQyY2EtNDY3Zi05N2NjLTlkMmE0ZmMzNmVmNw",
				"inTime": "1704392999757",
				"isBot": false,
				"isRestrictedIp": false,
				"isSystemUpdatedEvent": false,
				"locale": "undefined",
				"newSession": false,
				"orgId": "1001_1",
				"outTime": "1704392999758",
				"platformType": "Linux armv81",
				"refPage": "paymentShippingNew",
				"refPageId": "1704392951585_7181_170439279814661197",
				"sessionFirstChannel": "FacebookPaid",
				"sessionId": "170439280188907439",
				"sessionInfoId": "1",
				"sessionLastChannel": "FacebookPaid",
				"sourceChannelName": "Direct",
				"sourceChannelType": "Organic",
				"storeTime": "202401050005",
				"timestamp": "1704392999755",
				"transactional": false
			}
		};

		const enhanceRecord = addInsert(["eventId"]);
		const unExpectedHash = murmurhash.v3(stringify(record)).toString();
		const expectedHash = murmurhash.v3([record.properties.eventId].join("-")).toString();
		const enhancedRecord = enhanceRecord(record);
		expect(enhancedRecord.properties.$insert_id).toEqual(expectedHash);
		expect(enhancedRecord.properties.$insert_id).not.toEqual(unExpectedHash);
	});

	test("insert_id: missing fields", () => {
		const record = {
			event: "userLogin",
			distinct_id: "user123",
			properties: {}
		};
		const enhanceRecord = addInsert(["event", "distinct_id", "time"]);
		const expectedHash = murmurhash.v3(stringify(record)).toString();
		const enhancedRecord = enhanceRecord(record);

		expect(enhancedRecord.properties.$insert_id).toEqual(expectedHash);
	});

	test("insert_id: fields in props", () => {
		const record = {
			event: "userLogin",
			properties: {
				distinct_id: "user123",
				time: "2021-01-01T00:00:00Z"
			}
		};
		const expectedInsertId = murmurhash.v3([record.event, record.properties.distinct_id, record.properties.time].join("-")).toString();
		const enhanceRecord = addInsert(["event", "distinct_id", "time"]);
		const enhancedRecord = enhanceRecord(record);
		expect(enhancedRecord.properties.$insert_id).toEqual(expectedInsertId);
	});

	test("insert_id: empty record", () => {
		const record = {};
		const enhanceRecord = addInsert();

		expect(enhanceRecord(record)).toEqual(record);
	});

	test("insert_id: null", () => {
		const record = {
			event: null,
			distinct_id: null,
			time: null,
			properties: {}
		};
		const enhanceRecord = addInsert("event", "distinct_id", "time");
		const expectedHash = murmurhash.v3(stringify(record)).toString();
		const enhancedRecord = enhanceRecord(record);


		expect(enhancedRecord.properties.$insert_id).toEqual(expectedHash);
	});

	test("insert_id: fallback", () => {
		const record = {
			event: "userLogin",
			properties: {
				distinct_id: "user123"
			}
		};
		const enhanceRecord = addInsert("event", "distinct_id", "time");
		const expectedHash = murmurhash.v3(stringify(record)).toString();
		const enhancedRecord = enhanceRecord(record);
		expect(enhancedRecord.properties.$insert_id).toEqual(expectedHash);
	});

	const jsonProcessor = fixJson();

	test('fix json: obj', () => {
		const record = { properties: { key1: '{"name":"John"}' } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: { name: 'John' } } });
	});

	test('fix json: array', () => {
		const record = { properties: { key1: '["apple", "banana"]' } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: ["apple", "banana"] } });
	});

	test('fix json: str', () => {
		const record = { properties: { key1: JSON.stringify('{"name":"John"}') } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: { name: 'John' } } });
	});

	test('fix json: esc', () => {
		const record = { properties: { key1: '{"name":"John \\\\ Doe"}' } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: { name: 'John \\ Doe' } } });
	});

	test('fix json: double', () => {
		const record = { properties: { key1: JSON.stringify(JSON.stringify({ name: 'John' })) } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: { name: 'John' } } });
	});

	test('fix json: all good', () => {
		const record = { properties: { key1: 'Just a regular string' } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: 'Just a regular string' } });
	});

	test('fix json: dont fail', () => {
		const record = { properties: { key1: 'This is not a JSON string: {name:"John"}' } };
		expect(jsonProcessor(record)).toEqual({ properties: { key1: 'This is not a JSON string: {name:"John"}' } });
	});

	const fallBackData = {
		key1: '',
		key2: null,
		key3: undefined,
		key4: 'value4',
		key5: 0,
		key6: false,
		key7: [],
		key8: {}
	};

	test('resolve fallback: no keys', () => {
		expect(resolveFallback(fallBackData, [])).toBeNull();
	});

	test('resolve fallback: no object', () => {
		expect(resolveFallback(null, ['key1', 'key2'])).toBeNull();
		expect(resolveFallback(undefined, ['key1', 'key2'])).toBeNull();
	});

	test('resolve fallback: should return null if none of the keys exist in the data object', () => {
		expect(resolveFallback(fallBackData, ['key9', 'key10'])).toBeNull();
	});

	test('resolve fallback: first key', () => {
		expect(resolveFallback(fallBackData, ['key1', 'key2', 'key3', 'key4'])).toBe('value4');
		expect(resolveFallback(fallBackData, ['key4', 'key5', 'key6'])).toBe('value4');
		expect(resolveFallback(fallBackData, ['key7', 'key8'])).toBeNull();
	});

	test('resolve fallback: non strings should be null', () => {
		expect(resolveFallback(fallBackData, ['key5'])).toBe('0');
		expect(resolveFallback(fallBackData, ['key6'])).toBe('false');
	});

	test('resolve fallback: nested keys', () => {
		const nestedData = {
			level1: {
				level2: {
					level3: 'nestedValue'
				}
			}
		};
		expect(resolveFallback(nestedData, ['level3'])).toBe('nestedValue');
	});


	test('scrub data', () => {
		const data = {
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
		};

		const scrubKeys = ['email', 'item', 'foo'];

		const expected = {
			event: 'test',
			properties: {
				$user_id: '123',
				$device_id: '456',
				nested: {
					baz: "qux"
				},
				cart: [{
					price: 1.02
				}, {
					price: 2.03
				}]
			},
		};

		const scrubber = scrubProperties(scrubKeys);
		const scrubbed = scrubber(data);

		expect(scrubbed).toEqual(expected);

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

	test("chunk: maxBatchCount", done => {
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
			recordType: "profile-export",
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
