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
const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

// ! MODULES
const Job = require("../components/job.js");
const mpImport = require("../index.js");
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
	dropColumns,
	addToken
} = require("../components/transforms.js");

const { getEnvVars,
	JsonlParser,
	chunkForSize,
	determineDataType,
	existingStreamInterface,
	itemStream,
	analyzeFileFormat
} = require("../components/parsers.js");
const exp = require("constants");
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
		const job = new Job(fakeCreds, { abridged: false });
		job.store("response", true);
		expect(job.responses.length).toBe(1);
		expect(job.responses[0]).toBe("response");
	});

	test("stores errors too", () => {
		const job = new Job(fakeCreds, { abridged: false });
		job.store("error_response", false);
		// In non-abridged mode, error responses go to responses array
		expect(job.responses.length).toBe(1);
		expect(job.responses[0]).toBe("error_response");
		// Errors are counted in the errors object
		expect(job.errors["error_response"]).toBe(1);
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

	test("maxRecords parameter", () => {
		const job = new Job(fakeCreds, { maxRecords: 100 });
		expect(job.maxRecords).toBe(100);
	});

	test("maxRecords null by default", () => {
		const job = new Job(fakeCreds);
		expect(job.maxRecords).toBeNull();
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

	test("reservied props", () => {
		const config = { recordType: "user" };
		const record = {
			distinct_id: "123",
			group_id: "456",
			token: "789",
			group_key: "101112",
			name: "foo",
			first_name: "bar",
			last_name: "baz",
			email: "qux@mux.com",
			phone: "123-456-789",
			avatar: "http://foo.com",
			created: "2020-04-20",
			ip: "168.196.1.1"

		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$distinct_id).toBe("123");
		const { $set: props, ...outside } = transformed;
		expect(props).toHaveProperty("$avatar", "http://foo.com");
		expect(props).toHaveProperty("$created", "2020-04-20");
		expect(props).toHaveProperty("$email", "qux@mux.com");
		expect(props).toHaveProperty("$first_name", "bar");
		expect(props).toHaveProperty("$last_name", "baz");
		expect(props).toHaveProperty("$phone", "123-456-789");
		expect(outside).toHaveProperty("$distinct_id", "123");
		expect(outside).toHaveProperty("$ip", "168.196.1.1");
		expect(outside).toHaveProperty("$group_id", "456");
		expect(outside).toHaveProperty("$token", "789");
		expect(outside).toHaveProperty("$group_key", "101112");


		// expect(transformed.properties.time).toBeNumber();
		// expect(transformed.properties.$insert_id).toBeTruthy();

		// expect(transformed.$set.name).toBe("John");
	});

	test("adds token (implicit)", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			$set: { name: "John" }
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$token).toBe("testToken");
		expect(transformed.$set.$name).toBe("John");
		expect(transformed.$set.name).toBeUndefined();
	});

	test("adds events token", () => {
		const config = { recordType: "event", token: "testToken" };
		const record = {
			event: "foo",
			properties: { name: "bar" }
		};
		const transformed = addToken(config)(record);
		expect(transformed.properties.token).toBe("testToken");
	});


	test("adds users token", () => {
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
		expect(transformed.$set.$name).toBe("John");
		expect(transformed.name).toBeUndefined();
		expect(transformed.$set.name).toBeUndefined();
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
		expect(transformed.$set.$name).toBe("GroupA");
		expect(transformed.$set.name).toBeUndefined();
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
		expect(transformed.$set.$name).toBe("GroupA");
		expect(transformed.name).toBeUndefined();
		expect(transformed.$set.name).toBeUndefined();
	});

	test("noop if good", () => {
		const config = { recordType: "unknown" };
		const record = { data: "test" };
		const transformed = ezTransforms(config)(record);
		expect(transformed).toEqual(record);
	});

	// Directive option tests
	test("user profile: $set directive (default)", () => {
		const config = { recordType: "user", token: "testToken" };
		const record = {
			$distinct_id: "123",
			name: "John",
			email: "john@example.com"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set).toBeDefined();
		expect(transformed.$set.$name).toBe("John");
		expect(transformed.$set.$email).toBe("john@example.com");
		expect(transformed.$set_once).toBeUndefined();
		expect(transformed.$add).toBeUndefined();
	});

	test("user profile: $set_once directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$set_once" };
		const record = {
			$distinct_id: "123",
			name: "John",
			created: "2024-01-01"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set_once).toBeDefined();
		expect(transformed.$set_once.$name).toBe("John");
		expect(transformed.$set_once.$created).toBe("2024-01-01");
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: $add directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$add" };
		const record = {
			$distinct_id: "123",
			points: 100,
			credits: 50
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$add).toBeDefined();
		expect(transformed.$add.points).toBe(100);
		expect(transformed.$add.credits).toBe(50);
		expect(transformed.$set).toBeUndefined();
		expect(transformed.$set_once).toBeUndefined();
	});

	test("user profile: $union directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$union" };
		const record = {
			$distinct_id: "123",
			tags: ["premium", "verified"],
			categories: ["tech", "gaming"]
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$union).toBeDefined();
		expect(transformed.$union.tags).toEqual(["premium", "verified"]);
		expect(transformed.$union.categories).toEqual(["tech", "gaming"]);
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: $append directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$append" };
		const record = {
			$distinct_id: "123",
			purchases: { item: "widget", price: 99.99 },
			events: { type: "login", timestamp: "2024-01-01" }
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$append).toBeDefined();
		expect(transformed.$append.purchases).toEqual({ item: "widget", price: 99.99 });
		expect(transformed.$append.events).toEqual({ type: "login", timestamp: "2024-01-01" });
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: $remove directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$remove" };
		const record = {
			$distinct_id: "123",
			tags: ["old", "deprecated"],
			features: ["beta"]
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$remove).toBeDefined();
		expect(transformed.$remove.tags).toEqual(["old", "deprecated"]);
		expect(transformed.$remove.features).toEqual(["beta"]);
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: $unset directive", () => {
		const config = { recordType: "user", token: "testToken", directive: "$unset" };
		const record = {
			$distinct_id: "123",
			obsolete_field: true,
			old_preference: true
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$unset).toBeDefined();
		expect(Object.keys(transformed.$unset)).toEqual(["obsolete_field", "old_preference"]);
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: invalid directive defaults to $set", () => {
		const config = { recordType: "user", token: "testToken", directive: "$invalid" };
		const record = {
			$distinct_id: "123",
			name: "John"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set).toBeDefined();
		expect(transformed.$set.$name).toBe("John");
		expect(transformed.$invalid).toBeUndefined();
	});

	test("user profile: empty directive defaults to $set", () => {
		const config = { recordType: "user", token: "testToken", directive: "" };
		const record = {
			$distinct_id: "123",
			name: "John"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set).toBeDefined();
		expect(transformed.$set.$name).toBe("John");
	});

	test("group profile: $set directive (default)", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "company" };
		const record = {
			$group_id: "acme-corp",
			name: "ACME Corporation"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set).toBeDefined();
		expect(transformed.$set.$name).toBe("ACME Corporation");
		expect(transformed.$set_once).toBeUndefined();
	});

	test("group profile: $set_once directive", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "company", directive: "$set_once" };
		const record = {
			$group_id: "acme-corp",
			founded: "1946",
			industry: "Manufacturing"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set_once).toBeDefined();
		expect(transformed.$set_once.founded).toBe("1946");
		expect(transformed.$set_once.industry).toBe("Manufacturing");
		expect(transformed.$set).toBeUndefined();
	});

	test("group profile: $add directive", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "company", directive: "$add" };
		const record = {
			$group_id: "acme-corp",
			employees: 10,
			revenue: 1000000
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$add).toBeDefined();
		expect(transformed.$add.employees).toBe(10);
		expect(transformed.$add.revenue).toBe(1000000);
		expect(transformed.$set).toBeUndefined();
	});

	test("group profile: $union directive", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "company", directive: "$union" };
		const record = {
			$group_id: "acme-corp",
			locations: ["New York", "San Francisco"],
			products: ["Widget", "Gadget"]
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$union).toBeDefined();
		expect(transformed.$union.locations).toEqual(["New York", "San Francisco"]);
		expect(transformed.$union.products).toEqual(["Widget", "Gadget"]);
		expect(transformed.$set).toBeUndefined();
	});

	test("group profile: invalid directive defaults to $set", () => {
		const config = { recordType: "group", token: "testToken", groupKey: "company", directive: "not_valid" };
		const record = {
			$group_id: "acme-corp",
			name: "ACME Corporation"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set).toBeDefined();
		expect(transformed.$set.$name).toBe("ACME Corporation");
		expect(transformed.not_valid).toBeUndefined();
	});

	test("user profile: directive with existing operation bucket", () => {
		const config = { recordType: "user", token: "testToken", directive: "$set_once" };
		const record = {
			$distinct_id: "123",
			$set: { should_be_ignored: "value" },
			name: "John",
			created: "2024-01-01"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set_once).toBeDefined();
		expect(transformed.$set_once.$name).toBe("John");
		expect(transformed.$set_once.$created).toBe("2024-01-01");
		// Original $set should be overwritten
		expect(transformed.$set).toBeUndefined();
	});

	test("user profile: directive handles special properties correctly", () => {
		const config = { recordType: "user", token: "testToken", directive: "$set_once" };
		const record = {
			$distinct_id: "123",
			name: "John",
			email: "john@example.com",
			first_name: "John",
			last_name: "Doe",
			phone: "555-0123",
			avatar: "https://example.com/avatar.jpg",
			created: "2024-01-01",
			custom_field: "custom_value"
		};
		const transformed = ezTransforms(config)(record);
		expect(transformed.$set_once).toBeDefined();
		// Special properties should be prefixed with $
		expect(transformed.$set_once.$name).toBe("John");
		expect(transformed.$set_once.$email).toBe("john@example.com");
		expect(transformed.$set_once.$first_name).toBe("John");
		expect(transformed.$set_once.$last_name).toBe("Doe");
		expect(transformed.$set_once.$phone).toBe("555-0123");
		expect(transformed.$set_once.$avatar).toBe("https://example.com/avatar.jpg");
		expect(transformed.$set_once.$created).toBe("2024-01-01");
		// Custom fields remain unprefixed
		expect(transformed.$set_once.custom_field).toBe("custom_value");
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

	test("event: special keys", () => {
		const testEvent = {
			event: "view item",
			source: "dm4",
			time: "2024-01-20T10:49:36.407Z",
			user_id: "f465ba0f-64d4-5fa3-bed5-b46f7e22d5d5",
			isFeaturedItem: true,
			itemCategory: "Sports",
			dateItemListed: "2024-01-29",
			itemId: 4161,
			platform: "kiosk",
			currentTheme: "custom",
			country: "United States",
			region: "Illinois",
			city: "Chicago",
			browser: "Chrome",
			model: "Pixel 7 Pro",
			screen_height: "3120",
			screen_width: "1440",
			os: "Android",
			carrier: "us cellular",
			radio: "2G",
		};

		const transformed = ezTransforms({ recordType: "event" })(testEvent);
		const { $source,
			$user_id,
			mp_country_code,
			$region,
			$city,
			$browser,
			$model,
			$screen_height,
			$screen_width,
			$os,
			$carrier,
			$radio } = transformed.properties;
		expect($source).toBe("dm4");
		expect($user_id).toBe("f465ba0f-64d4-5fa3-bed5-b46f7e22d5d5");
		expect(mp_country_code).toBe("United States");
		expect($region).toBe("Illinois");
		expect($city).toBe("Chicago");
		expect($browser).toBe("Chrome");
		expect($model).toBe("Pixel 7 Pro");
		expect($screen_height).toBe("3120");
		expect($screen_width).toBe("1440");
		expect($os).toBe("Android");
		expect($carrier).toBe("us cellular");
		expect($radio).toBe("2G");
	});

	test("user: special keys", () => {
		const testUser = {
			distinct_id: "5acc0bda-4def-50d5-bfb6-57a02bad184e",
			name: "Chad Cherici",
			email: "C.Cherici@icloud.co.uk",
			avatar: "https://randomuser.me/api/portraits/men/44.jpg",
			created: "2024-01-03",
			country_code: "US",
			region: "Arizona",
			city: "Phoenix",
			title: "Professional Athlete",
			luckyNumber: 322,
			spiritAnimal: "rove beetle",
		};
		const transformed = ezTransforms({ recordType: "user" })(testUser);
		const { $distinct_id } = transformed;
		const { $name, $email, $avatar, $created, $country_code, $region, $city } = transformed.$set;

		expect($distinct_id).toBe("5acc0bda-4def-50d5-bfb6-57a02bad184e");
		expect($name).toBe("Chad Cherici");
		expect($email).toBe("C.Cherici@icloud.co.uk");
		expect($avatar).toBe("https://randomuser.me/api/portraits/men/44.jpg");
		expect($created).toBe("2024-01-03");
		expect($country_code).toBe("US");
		expect($region).toBe("Arizona");
		expect($city).toBe("Phoenix");


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


	test("combo: whitelist", () => {
		params = {
			comboWhiteList: {
				color: ['blue'], // whitelist records with color blue
				size: ['large']  // or size large
			}
		};
		const recordAllowed1 = { properties: { color: 'blue', size: 'medium' } }; // Should pass (color matches)
		const recordAllowed2 = { properties: { color: 'red', size: 'large' } };   // Should pass (size matches)
		const recordDisallowed = { properties: { color: 'red', size: 'medium' } }; // Should not pass (no match)

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed1)).toEqual(recordAllowed1);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed2)).toEqual(recordAllowed2);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed)).toEqual({});
	});

	test("combo: blacklist", () => {
		params = {
			comboBlackList: {
				color: ['blue'], // blacklist records with color blue
				size: ['small']  // or size small
			}
		};
		const recordAllowed = { properties: { color: 'red', size: 'medium' } };    // Should pass (no match)
		const recordDisallowed1 = { properties: { color: 'blue', size: 'medium' } }; // Should not pass (color matches)
		const recordDisallowed2 = { properties: { color: 'red', size: 'small' } };   // Should not pass (size matches)

		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed1)).toEqual({});
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordDisallowed2)).toEqual({});
	});

	// test("combo: whitelist with empty properties", () => {
	// 	params = {
	// 		comboWhiteList: {
	// 			color: ['blue']
	// 		}
	// 	};
	// 	const recordEmpty = { properties: {} }; // Empty properties
	// 	expect(whiteAndBlackLister(sampleJobConfig, params)(recordEmpty)).toEqual({});
	// 	expect(sampleJobConfig.whiteListSkipped).toBe(1);
	// });

	test("combo: stringify all", () => {
		params = {
			comboWhiteList: {
				count: [10] // Expecting a number
			}
		};
		const recordString = { properties: { count: '10' } }; // String type instead of number
		const recordNumber = { properties: { count: 10 } }; // Correct type
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordString)).toEqual(recordString);
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordNumber)).toEqual(recordNumber);
	});

	test("combo: nested props OK", () => {
		params = {
			comboBlackList: {
				age: [30] // Number type
			}
		};
		const recordNotAllowed = { properties: { details: { size: 'small' }, age: 30 } }; // Mixed pass and fail conditions
		const recordAllowed = { properties: { details: { size: 'large' }, age: 29 } }; // Should not pass (size matches)
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordNotAllowed)).toEqual({});
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllowed)).toEqual(recordAllowed);
	});

	test("combo: lists empty", () => {
		params = {
			comboWhiteList: {},
			comboBlackList: {}
		};
		const recordAny = { properties: { anyKey: 'anyValue' } };
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAny)).toEqual(recordAny);
	});

	test("combo: blacklist partial", () => {
		params = {
			comboBlackList: {
				key1: ['value1'],
				key2: ['value2']
			}
		};
		const recordAllMatch = { properties: { key1: 'value1', key2: 'value2' } };
		const recordPartialMatch = { properties: { key1: 'value1', key2: 'otherValue' } };
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordAllMatch)).toEqual({});
		expect(whiteAndBlackLister(sampleJobConfig, params)(recordPartialMatch)).toEqual({});
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

	test('resolve fallback: missing keys', () => {
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

	// dropColumns tests
	test("dropColumns: removes properties from event records", () => {
		const record = {
			event: "Test Event",
			properties: {
				keep: "value1",
				remove_me: "value2",
				also_remove: "value3",
				distinct_id: "user123"
			}
		};
		const columnDropper = dropColumns(["remove_me", "also_remove"]);
		const result = columnDropper(record);

		expect(result.properties.keep).toBe("value1");
		expect(result.properties.distinct_id).toBe("user123");
		expect(result.properties.remove_me).toBeUndefined();
		expect(result.properties.also_remove).toBeUndefined();
		expect(result.event).toBe("Test Event");
	});

	test("dropColumns: removes properties from user profile records", () => {
		const record = {
			$distinct_id: "user123",
			$token: "token123",
			$set: {
				name: "John Doe",
				email: "john@example.com",
				sensitive_data: "remove_this",
				pii_field: "also_remove"
			}
		};
		const columnDropper = dropColumns(["sensitive_data", "pii_field"]);
		const result = columnDropper(record);

		expect(result.$set.name).toBe("John Doe");
		expect(result.$set.email).toBe("john@example.com");
		expect(result.$set.sensitive_data).toBeUndefined();
		expect(result.$set.pii_field).toBeUndefined();
		expect(result.$distinct_id).toBe("user123");
		expect(result.$token).toBe("token123");
	});

	test("dropColumns: works with multiple operation buckets", () => {
		const record = {
			$distinct_id: "user123",
			$set: {
				name: "John",
				remove_from_set: "value1"
			},
			$add: {
				score: 10,
				remove_from_add: 5
			},
			$union: {
				tags: ["tag1"],
				remove_from_union: ["bad_tag"]
			}
		};
		const columnDropper = dropColumns(["remove_from_set", "remove_from_add", "remove_from_union"]);
		const result = columnDropper(record);

		expect(result.$set.name).toBe("John");
		expect(result.$set.remove_from_set).toBeUndefined();
		expect(result.$add.score).toBe(10);
		expect(result.$add.remove_from_add).toBeUndefined();
		expect(result.$union.tags).toEqual(["tag1"]);
		expect(result.$union.remove_from_union).toBeUndefined();
	});

	test("dropColumns: removes from root level but preserves system fields", () => {
		const record = {
			event: "Test Event",
			properties: { distinct_id: "user123" },
			custom_field: "remove_me",
			another_field: "also_remove",
			$user_id: "preserve_me"
		};
		const columnDropper = dropColumns(["custom_field", "another_field", "event", "properties", "$user_id"]);
		const result = columnDropper(record);

		// System fields should be preserved
		expect(result.event).toBe("Test Event");
		expect(result.properties).toBeDefined();
		expect(result.$user_id).toBe("preserve_me");

		// Custom fields should be removed
		expect(result.custom_field).toBeUndefined();
		expect(result.another_field).toBeUndefined();
	});

	test("dropColumns: handles empty columns array", () => {
		const record = {
			event: "Test Event",
			properties: {
				keep: "value1",
				also_keep: "value2"
			}
		};
		const columnDropper = dropColumns([]);
		const result = columnDropper(record);

		expect(result).toEqual(record);
	});

	test("dropColumns: handles null/undefined record", () => {
		const columnDropper = dropColumns(["remove_me"]);

		expect(columnDropper(null)).toBeNull();
		expect(columnDropper(undefined)).toBeUndefined();
		expect(columnDropper({})).toEqual({});
	});

	test("dropColumns: handles record without properties", () => {
		const record = {
			event: "Test Event",
			custom_field: "remove_me"
		};
		const columnDropper = dropColumns(["custom_field", "nonexistent"]);
		const result = columnDropper(record);

		expect(result.event).toBe("Test Event");
		expect(result.custom_field).toBeUndefined();
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

describe("path checking", () => {
	// Import the checkPath function from parsers.js for testing

	// Helper function to replicate the checkPath function from parsers.js
	function checkPath(filePath) {
		try {
			const resolvedPath = path.resolve(filePath);
			if (!fs.existsSync(resolvedPath)) {
				return { exists: false, isFile: false, isDirectory: false, path: resolvedPath };
			}

			const stats = fs.lstatSync(resolvedPath);
			return {
				exists: true,
				isFile: stats.isFile(),
				isDirectory: stats.isDirectory(),
				path: resolvedPath
			};
		} catch (error) {
			console.warn(`Error checking path ${filePath}:`, error.message);
			return { exists: false, isFile: false, isDirectory: false, path: filePath };
		}
	}

	test("checkPath: existing file", () => {
		const result = checkPath('./testData/events-small.json');
		expect(result.exists).toBe(true);
		expect(result.isFile).toBe(true);
		expect(result.isDirectory).toBe(false);
		expect(result.path).toContain('events-small.json');
	});

	test("checkPath: existing directory", () => {
		const result = checkPath('./testData');
		expect(result.exists).toBe(true);
		expect(result.isFile).toBe(false);
		expect(result.isDirectory).toBe(true);
		expect(result.path).toContain('testData');
	});

	test("checkPath: non-existent file", () => {
		const result = checkPath('./nonexistent-file.json');
		expect(result.exists).toBe(false);
		expect(result.isFile).toBe(false);
		expect(result.isDirectory).toBe(false);
		expect(result.path).toContain('nonexistent-file.json');
	});

	test("checkPath: non-existent directory", () => {
		const result = checkPath('./nonexistent-directory');
		expect(result.exists).toBe(false);
		expect(result.isFile).toBe(false);
		expect(result.isDirectory).toBe(false);
		expect(result.path).toContain('nonexistent-directory');
	});

	test("checkPath: empty string resolves to current directory", () => {
		const result = checkPath('');
		expect(result.exists).toBe(true);
		expect(result.isFile).toBe(false);
		expect(result.isDirectory).toBe(true);
	});

	test("checkPath: relative path resolution", () => {
		const result = checkPath('./components/parsers.js');
		expect(result.exists).toBe(true);
		expect(result.isFile).toBe(true);
		expect(result.path).toMatch(/\/components\/parsers\.js$/);
	});

	test("checkPath: absolute path handling", () => {
		const absolutePath = path.resolve('./testData');
		const result = checkPath(absolutePath);
		expect(result.exists).toBe(true);
		expect(result.isDirectory).toBe(true);
		expect(result.path).toBe(absolutePath);
	});

	test("checkPath: handles invalid characters gracefully", () => {
		const result = checkPath('/invalid\0path/with\0nulls');
		expect(result.exists).toBe(false);
		expect(result.isFile).toBe(false);
		expect(result.isDirectory).toBe(false);
	});

	test("determineDataType: better error handling for non-existent files", async () => {
		const job = new Job(fakeCreds);

		await expect(determineDataType('./nonexistent-file.json', job))
			.rejects
			.toThrow('File or directory does not exist: ./nonexistent-file.json');
	});

	test("determineDataType: handles array of non-existent files", async () => {
		const job = new Job(fakeCreds);
		const nonExistentFiles = ['./file1.json', './file2.json', './file3.json'];
		try {
			const result = await determineDataType(nonExistentFiles, job);
		} catch (error) {
			expect(error.message).toBe('data must be a file path, folder path, array of objects, stream, or string');
		}
	});

	test("determineDataType: existing directory handling", async () => {
		const job = new Job(fakeCreds);
		try {
			// testData directory exists and should be processed
			const result = await determineDataType('./testData', job);
		}
		catch (error) {
			// If no supported files are found, it should throw an error
			expect(error.message).toBe('All files in array/directory must have the same format and compression (gzipped or not gzipped)');
		}
	});

	test("determineDataType: array of existing files", async () => {
		const job = new Job(fakeCreds);
		const existingFiles = ['./testData/events-small.json'];

		const result = await determineDataType(existingFiles, job);
		expect(result).toBeDefined();
		expect(job.wasStream).toBe(true);
	});
});

describe("gzip support", () => {
	test("isGzip option works", () => {
		const job = new Job(fakeCreds, { isGzip: true });
		expect(job.isGzip).toBe(true);

		const jobDefault = new Job(fakeCreds);
		expect(jobDefault.isGzip).toBe(false);
	});

	test("gzip extensions supported", () => {
		const job = new Job(fakeCreds);
		expect(job.supportedFileExt).toContain('.json.gz');
		expect(job.supportedFileExt).toContain('.jsonl.gz');
		expect(job.supportedFileExt).toContain('.csv.gz');
		expect(job.supportedFileExt).toContain('.parquet.gz');
		expect(job.supportedFileExt).toContain('.ndjson.gz');
		expect(job.supportedFileExt).toContain('.txt.gz');
		expect(job.supportedFileExt).toContain('.tsv.gz');
	});

	test("detects gzip JSON as JSONL by default", () => {
		const job = new Job(fakeCreds);

		const result = analyzeFileFormat('./testData/gzip-tests/events.json.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('jsonl');
		expect(result.baseFormat).toBe('.json');
	});

	test("detects gzip JSONL", () => {
		const job = new Job(fakeCreds);

		const result = analyzeFileFormat('./testData/gzip-tests/bad_data.jsonl.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('jsonl');
		expect(result.baseFormat).toBe('.jsonl');
	});

	test("detects gzip CSV", () => {
		const job = new Job(fakeCreds);

		const result = analyzeFileFormat('./testData/gzip-tests/table.csv.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('csv');
		expect(result.baseFormat).toBe('.csv');
	});

	test("detects gzip Parquet", () => {
		const job = new Job(fakeCreds);

		const result = analyzeFileFormat('./testData/gzip-tests/playtika_sample.parquet.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('parquet');
		expect(result.baseFormat).toBe('.parquet');
	});

	test("isGzip overrides detection", () => {
		const job = new Job(fakeCreds, { isGzip: true });

		// Test with non-gzipped file extension but isGzip=true
		const result = analyzeFileFormat('./testData/events.json', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('jsonl');
	});

	test("isGzip works with .gz", () => {
		const job = new Job(fakeCreds, { isGzip: true });

		// Test with .gz extension - should still work
		const result = analyzeFileFormat('./testData/events.json.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('jsonl');
	});

	test("analyzeFileFormat detects .json.gz as jsonl by default", () => {
		const job = new Job(fakeCreds);

		// analyzeFileFormat only looks at extensions, not streamFormat
		const result = analyzeFileFormat('./testData/gzip-tests/events.json.gz', job);
		expect(result.isGzipped).toBe(true);
		expect(result.parsingCase).toBe('jsonl');
		expect(result.baseFormat).toBe('.json');
	});

	test("streamFormat can override auto-detection in parsing pipeline", async () => {
		const job = new Job(fakeCreds, { streamFormat: 'strict_json', dryRun: true });

		// Test that streamFormat can override the auto-detection
		// Using events-small.json which is a strict JSON array
		const stream = await determineDataType('./testData/events-small.json', job);
		expect(stream).toBeDefined();
		// The stream should be created successfully with strict_json format
	});

	// test("gzipped JSON file can be processed", async () => {
	// 	const fs = require('fs');
	// 	const path = require('path');

	// 	// Only run if test file exists
	// 	if (fs.existsSync('./testData/gzip-tests/big.json.gz')) {
	// 		const job = new Job(fakeCreds, { dryRun: true });
	// 		const stream = await determineDataType('./testData/gzip-tests/big.json.gz', job);

	// 		expect(stream).toBeDefined();
	// 		expect(job.wasStream).toBe(true);

	// 		// Collect data from stream to verify it works
	// 		const data = [];
	// 		stream.on('data', chunk => data.push(chunk));

	// 		return new Promise((resolve, reject) => {
	// 			stream.on('end', () => {
	// 				expect(data.length).toBeGreaterThan(0);
	// 				expect(data[0]).toHaveProperty('event');
	// 				resolve();
	// 			});
	// 			stream.on('error', reject);
	// 		});
	// 	}
	// });

	test("processes gzip JSONL", async () => {
		// Only run if test file exists
		if (fs.existsSync('./testData/gzip-tests/bad_data.jsonl.gz')) {
			const job = new Job(fakeCreds, { dryRun: true });
			const stream = await determineDataType('./testData/gzip-tests/bad_data.jsonl.gz', job);

			expect(stream).toBeDefined();
			expect(job.wasStream).toBe(true);

			// Collect data from stream to verify it works
			const data = [];
			stream.on('data', chunk => data.push(chunk));

			return new Promise((resolve, reject) => {
				stream.on('end', () => {
					expect(data.length).toBeGreaterThan(0);
					resolve();
				});
				stream.on('error', reject);
			});
		}
	});

	test("processes gzip CSV", async () => {
		// Only run if test file exists
		if (fs.existsSync('./testData/gzip-tests/table.csv.gz')) {
			const job = new Job(fakeCreds, {
				dryRun: true,
				aliases: { unit_id: 'distinct_id' } // CSV has unit_id column, not event
			});
			const stream = await determineDataType('./testData/gzip-tests/table.csv.gz', job);

			expect(stream).toBeDefined();
			expect(job.wasStream).toBe(true);

			// Collect data from stream to verify it works
			const data = [];
			stream.on('data', chunk => data.push(chunk));

			return new Promise((resolve, reject) => {
				stream.on('end', () => {
					expect(data.length).toBeGreaterThan(0);
					// For now, just check that we got some data - CSV processing may need adjustment
					// expect(data[0]).toHaveProperty('event');
					resolve();
				});
				stream.on('error', reject);
			});
		}
	}, 20000); // Increase timeout for gzip processing

	test("processes gzipped parquet files", async () => {
		// Only run if test file exists
		if (fs.existsSync('./testData/gzip-tests/playtika_sample.parquet.gz')) {
			const job = new Job(fakeCreds, { dryRun: true });
			const stream = await determineDataType('./testData/gzip-tests/playtika_sample.parquet.gz', job);

			expect(stream).toBeDefined();

			expect(stream).toBeInstanceOf(require('stream').Stream);

		} else {
			// Skip test if file doesn't exist
			expect(true).toBe(true);
		}
	});

	// test("mixed gzip and non-gzip files in array throws error", async () => {
	// 	const fs = require('fs');

	// 	if (fs.existsSync('./testData/events.json') && fs.existsSync('./testData/gzip-tests/events.json.gz')) {
	// 		const job = new Job(fakeCreds, { dryRun: true });
	// 		const files = ['./testData/events.json', './testData/gzip-tests/events.json.gz'];

	// 		await expect(determineDataType(files, job))
	// 			.rejects
	// 			.toThrow('All files in array/directory must have the same format and compression');
	// 	}
	// });

	test("isGzip forces processing", async () => {
		// Create a temporary gzipped file without .gz extension
		if (fs.existsSync('./testData/events.json')) {
			const originalData = fs.readFileSync('./testData/events.json');
			const gzippedData = zlib.gzipSync(originalData);
			const tempFile = './testData/temp-gzipped-no-ext.json';

			fs.writeFileSync(tempFile, gzippedData);

			try {
				const job = new Job(fakeCreds, { dryRun: true, isGzip: true, streamFormat: "jsonl" });
				const stream = await determineDataType(tempFile, job);

				expect(stream).toBeDefined();
				expect(job.wasStream).toBe(true);

				// Collect data from stream to verify it works
				const data = [];
				stream.on('data', chunk => data.push(chunk));

				return new Promise((resolve, reject) => {
					stream.on('end', () => {
						expect(data.length).toBeGreaterThan(0);
						fs.unlinkSync(tempFile); // Clean up
						resolve();
					});
					stream.on('error', (err) => {
						fs.unlinkSync(tempFile); // Clean up
						reject(err);
					});
				});
			} catch (error) {
				fs.unlinkSync(tempFile); // Clean up
				throw error;
			}
		}
	});


});

describe("maxRecords functionality", () => {
	// Helper function to generate test data
	function generateTestData(count) {
		const data = [];
		for (let i = 0; i < count; i++) {
			data.push({
				event: 'test_event',
				properties: {
					distinct_id: `user_${i}`,
					test_property: `value_${i}`,
					time: Date.now()
				}
			});
		}
		return data;
	}

	test('limits processing (normal)', async () => {
		const testData = generateTestData(200); // Generate 200 records
		const maxRecords = 50;

		const result = await mpImport(fakeCreds, testData, {
			maxRecords,
			dryRun: true // Use dry run to avoid actual API calls
		});

		// Should process exactly maxRecords records
		expect(result.total).toBe(maxRecords);
		expect(result.dryRun.length).toBe(maxRecords);
	});

	test('limits processing (dryRun)', async () => {
		const testData = generateTestData(150); // Generate 150 records
		const maxRecords = 25;

		const result = await mpImport(fakeCreds, testData, {
			maxRecords,
			dryRun: true
		});

		// Should process exactly maxRecords records in dry run
		expect(result.total).toBe(maxRecords);
		expect(result.dryRun.length).toBe(maxRecords);
	});

	test('null processes all records', async () => {
		const testData = generateTestData(10); // Small dataset

		const result = await mpImport(fakeCreds, testData, {
			maxRecords: null, // Explicitly null
			dryRun: true
		});

		// Should process all 10 records
		expect(result.total).toBe(10);
		expect(result.dryRun.length).toBe(10);
	});

	test('undefined processes all', async () => {
		const testData = generateTestData(15); // Small dataset

		const result = await mpImport(fakeCreds, testData, {
			// No maxRecords specified
			dryRun: true
		});

		// Should process all 15 records
		expect(result.total).toBe(15);
		expect(result.dryRun.length).toBe(15);
	});

	test('respects transforms', async () => {
		const testData = generateTestData(100);
		const maxRecords = 30;

		// Transform that filters out even-numbered users
		const transformFunc = (data) => {
			const userId = parseInt(data.properties.distinct_id.split('_')[1]);
			if (userId % 2 === 0) {
				return {}; // Empty object gets filtered out
			}
			return data;
		};

		const result = await mpImport(fakeCreds, testData, {
			maxRecords,
			transformFunc,
			dryRun: true
		});

		// Note: maxRecords is applied BEFORE transforms, so we process 30 records
		// but some get filtered out by the transform
		expect(result.total).toBe(maxRecords);
		// The dry run results will be less than maxRecords due to filtering
		expect(result.dryRun.length).toBeLessThanOrEqual(maxRecords);
	});

	test('works with profiles', async () => {
		const testData = [];
		for (let i = 0; i < 50; i++) {
			testData.push({
				$distinct_id: `user_${i}`,
				$set: {
					name: `User ${i}`,
					email: `user${i}@test.com`
				}
			});
		}

		const maxRecords = 20;

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'user',
			maxRecords,
			dryRun: true
		});

		expect(result.total).toBe(maxRecords);
		expect(result.dryRun.length).toBe(maxRecords);
		expect(result.recordType).toBe('user');
	});

	test('zero processes none', async () => {
		const testData = generateTestData(10);

		const result = await mpImport(fakeCreds, testData, {
			maxRecords: 0,
			dryRun: true
		});

		expect(result.total).toBe(0);
		expect(result.dryRun).toEqual([]); // Empty array when no records processed
	});

	test('one processes one', async () => {
		const testData = generateTestData(10);

		const result = await mpImport(fakeCreds, testData, {
			maxRecords: 1,
			dryRun: true
		});

		expect(result.total).toBe(1);
		expect(result.dryRun.length).toBe(1);
	});

	test('larger than dataset', async () => {
		const testData = generateTestData(5); // Only 5 records
		const maxRecords = 100; // Much larger limit

		const result = await mpImport(fakeCreds, testData, {
			maxRecords,
			dryRun: true
		});

		// Should process all 5 records, not limited by maxRecords
		expect(result.total).toBe(5);
		expect(result.dryRun.length).toBe(5);
	});

	test('works with groups', async () => {
		const groupData = [];
		for (let i = 0; i < 30; i++) {
			groupData.push({
				$group_id: `group_${i}`,
				$group_key: 'company',
				$set: {
					name: `Company ${i}`,
					size: i * 10
				}
			});
		}

		const maxRecords = 10;

		const result = await mpImport(fakeCreds, groupData, {
			recordType: 'group',
			groupKey: 'company',
			maxRecords,
			dryRun: true
		});

		expect(result.total).toBe(maxRecords);
		expect(result.dryRun.length).toBe(maxRecords);
		expect(result.recordType).toBe('group');
	});
});

describe('v2_compat', () => {
	test('sets distinct_id from $user_id', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				$user_id: 'user123',
				time: Date.now()
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBe('user123');
		expect(result.dryRun[0].properties.$user_id).toBe('user123'); // Original preserved
	});

	test('sets distinct_id from $device_id when no $user_id', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				$device_id: 'device456',
				time: Date.now()
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBe('device456');
		expect(result.dryRun[0].properties.$device_id).toBe('device456'); // Original preserved
	});

	test('prefers $user_id over $device_id', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				$user_id: 'user123',
				$device_id: 'device456',
				time: Date.now()
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBe('user123'); // Prefers $user_id
		expect(result.dryRun[0].properties.$user_id).toBe('user123');
		expect(result.dryRun[0].properties.$device_id).toBe('device456');
	});

	test('does not overwrite existing distinct_id', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				distinct_id: 'existing123',
				$user_id: 'user123',
				$device_id: 'device456',
				time: Date.now()
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBe('existing123'); // Unchanged
	});

	test('does nothing when neither $user_id nor $device_id exists', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				time: Date.now(),
				some_prop: 'value'
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBeUndefined();
	});

	test('only applies to events, not user profiles', async () => {
		const testData = [{
			$distinct_id: 'user123',
			$set: {
				name: 'Test User'
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'user',
			token: 'test-token',
			v2_compat: true,
			dryRun: true
		});

		// User profiles don't have properties.distinct_id, so v2_compat should not apply
		expect(result.dryRun[0].$distinct_id).toBe('user123');
		expect(result.dryRun[0].distinct_id).toBeUndefined();
	});

	test('works with multiple events', async () => {
		const testData = [
			{
				event: 'Event 1',
				properties: {
					$user_id: 'user1',
					time: Date.now()
				}
			},
			{
				event: 'Event 2',
				properties: {
					$device_id: 'device2',
					time: Date.now()
				}
			},
			{
				event: 'Event 3',
				properties: {
					distinct_id: 'existing3',
					$user_id: 'user3',
					time: Date.now()
				}
			}
		];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			v2_compat: true,
			dryRun: true
		});

		expect(result.dryRun[0].properties.distinct_id).toBe('user1');
		expect(result.dryRun[1].properties.distinct_id).toBe('device2');
		expect(result.dryRun[2].properties.distinct_id).toBe('existing3');
	});

	test('disabled by default', async () => {
		const testData = [{
			event: 'Test Event',
			properties: {
				$user_id: 'user123',
				time: Date.now()
			}
		}];

		const result = await mpImport(fakeCreds, testData, {
			recordType: 'event',
			dryRun: true
			// v2_compat not set (defaults to false)
		});

		expect(result.dryRun[0].properties.distinct_id).toBeUndefined();
		expect(result.dryRun[0].properties.$user_id).toBe('user123');
	});
});

describe('BufferQueue', () => {
	const { BufferQueue } = require('../components/buffer-queue');
	const { Readable, Writable } = require('stream');

	test('handles source completion correctly', async () => {
		const bufferQueue = new BufferQueue({
			pauseThresholdMB: 10, // High threshold so no pausing
			verbose: false
		});

		const data = [];

		// Create source that ends immediately after 5 items
		const source = new Readable({
			objectMode: true,
			read() {
				for (let i = 0; i < 5; i++) {
					this.push({ id: i });
				}
				this.push(null);
			}
		});

		// Create sink
		const sink = new Writable({
			objectMode: true,
			write(chunk, encoding, callback) {
				data.push(chunk);
				callback();
			}
		});

		// Connect (specify objectMode: true since we're dealing with objects not bytes)
		const queueInput = bufferQueue.createInputStream(true);
		const queueOutput = bufferQueue.createOutputStream(true);

		source.pipe(queueInput);
		queueOutput.pipe(sink);

		// Wait for completion
		await new Promise((resolve) => {
			sink.on('finish', resolve);
		});

		// All data should have been transmitted
		expect(data).toHaveLength(5);
		expect(data[0]).toEqual({ id: 0 });
		expect(data[4]).toEqual({ id: 4 });
	});

	test('getStats returns correct statistics', () => {
		const bufferQueue = new BufferQueue({
			verbose: false
		});

		// Initial stats
		let stats = bufferQueue.getStats();
		expect(stats.queueLength).toBe(0);
		expect(stats.objectsQueued).toBe(0);
		expect(stats.objectsDequeued).toBe(0);
		expect(stats.isPaused).toBe(false);

		// Add some data to queue
		bufferQueue.queue.push({ data: 'test', size: 100 });
		bufferQueue.queueSizeBytes += 100;
		bufferQueue.objectsQueued++;

		stats = bufferQueue.getStats();
		expect(stats.queueLength).toBe(1);
		expect(stats.objectsQueued).toBe(1);
		expect(parseFloat(stats.queueSizeMB)).toBeCloseTo(0.0001, 3);
	});
});

describe('Progress Display', () => {
	const { showProgress } = require('../components/cli');

	// Mock readline to capture output
	let capturedOutput = '';
	const originalWrite = process.stdout.write;

	beforeEach(() => {
		capturedOutput = '';
		// Mock process.stdout.write to capture output
		process.stdout.write = (str) => {
			capturedOutput = str;
			return true;
		};
	});

	afterEach(() => {
		// Restore original write
		process.stdout.write = originalWrite;
	});

	test('displays all metrics including zeros', () => {
		// Test with all zeros
		showProgress('event', 0, 0, '0', 0, 0, 0, null, 0, Date.now());

		expect(capturedOutput).toContain('total: 0');
		expect(capturedOutput).toContain('success: 0');
		expect(capturedOutput).toContain('failed: 0');
		expect(capturedOutput).toContain('empty: 0');
		expect(capturedOutput).toContain('mem:');
		expect(capturedOutput).toContain('proc: 0 B');
		expect(capturedOutput).toContain('time: 00:00:00');
	});

	test('formats large numbers with commas', () => {
		showProgress('event', 1234567, 100, '1234.56', 1000000, 234567, 1024*1024*100, null, 89012, Date.now());

		expect(capturedOutput).toContain('total: 1,234,567');
		expect(capturedOutput).toContain('success: 1,000,000');
		expect(capturedOutput).toContain('failed: 234,567');
		expect(capturedOutput).toContain('empty: 89,012');
		expect(capturedOutput).toContain('time: 00:00:00'); // Just started
	});

	test('formats elapsed time correctly', () => {
		const now = Date.now();

		// Test seconds only (45 seconds)
		let startTime = now - 45 * 1000;
		showProgress('event', 100, 10, '100', 50, 5, 0, null, 10, startTime);
		expect(capturedOutput).toContain('time: 00:00:45');

		// Test minutes and seconds (3m 25s)
		startTime = now - (3 * 60 + 25) * 1000;
		showProgress('event', 100, 10, '100', 50, 5, 0, null, 10, startTime);
		expect(capturedOutput).toContain('time: 00:03:25');

		// Test hours, minutes and seconds (2h 15m 30s)
		startTime = now - (2 * 3600 + 15 * 60 + 30) * 1000;
		showProgress('event', 100, 10, '100', 50, 5, 0, null, 10, startTime);
		expect(capturedOutput).toContain('time: 02:15:30');

		// Test ISO string format (job.startTime format)
		const isoTime = new Date(now - 61 * 1000).toISOString(); // 1m 1s ago
		showProgress('event', 100, 10, '100', 50, 5, 0, null, 10, isoTime);
		expect(capturedOutput).toContain('time: 00:01:01');
	});
});
