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

/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
require("dotenv").config();
const { execSync } = require("child_process");
const longTimeout = 750900;
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

const mp = require("../index.js");
const u = require('ak-tools');

const GCS_BUCKET_PREFIX = `gs://ak-bucky/mixpanel-import`;
const FILES = ['someTestData-1', 'someTestData-2'];
const NUM_RECORDS_PER_FILE = 3000;
const FORMATS = ['.json', '.json.gz', '.csv', '.csv.gz', '.parquet', '.parquet.gz'];

// Generate all test file paths
const TEST_PATHS = {};
for (const format of FORMATS) {
	const cleanFormat = format.replace(/\./g, ''); // Remove all dots for object key
	TEST_PATHS[cleanFormat] = [];

	for (const file of FILES) {
		const fullPath = `${GCS_BUCKET_PREFIX}/${cleanFormat}/${file}${format}`;
		TEST_PATHS[cleanFormat].push(fullPath);
	}
}

// Now TEST_PATHS contains:
// {
//   json: [],   jsongz: [],   csv: [],   csvgz: [],   parquet: [],   ]
// }


/** @type {import('../index.d.ts').Options} */
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
	showProgress: IS_DEBUG_MODE,
	verbose: IS_DEBUG_MODE,
	transformFunc: function fillInDistinctId(e) {
		if (e?.properties) {
			if (!e.properties.distinct_id) {
				if (e.properties.user_id) {
					e.properties.distinct_id = e.properties.user_id;
				} else {
					e.properties.distinct_id = u.uid();
				}
			}
		}
		return e;
	},
	responseHandler: (data) => {
		if (IS_DEBUG_MODE) {
			console.log(`\nRESPONSE!\n`);
		}
	}
};

function parquetTransform(e) {
	if (e.user_id) e.distinct_id = e.user_id;
	if (e.insert_id) e.insert_id = e.insert_id.toString();
	return e;
}

function csvTransform(record) {
	// Transform CSV record as needed
	const eventName = record.event;
	const properties = JSON.parse(record.properties.properties.replaceAll(/'/g, '"'));
	return record;
}

describe("google cloud storage", () => {
	test(
		"json: single file",
		async () => {
			const file = TEST_PATHS.json[0];
			const data = await mp({}, file, { ...opts });
			expect(data.success).toBe(NUM_RECORDS_PER_FILE);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"json: multiple files",
		async () => {
			const files = TEST_PATHS.json;
			const data = await mp({}, files, { ...opts });
			expect(data.success).toBe(NUM_RECORDS_PER_FILE * files.length);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"json.gz: single file",
		async () => {
			const file = TEST_PATHS.jsongz[0];
			const data = await mp({}, file, { ...opts });
			expect(data.success).toBe(NUM_RECORDS_PER_FILE);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"json.gz: multiple files",
		async () => {
			const files = TEST_PATHS.jsongz;
			const data = await mp({}, files, { ...opts });
			expect(data.success).toBe(NUM_RECORDS_PER_FILE * files.length);
			expect(data.failed).toBe(0);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"csv: single file",
		async () => {
			const file = TEST_PATHS.csv[0];
			const data = await mp({}, file, { ...opts, fixData: true });
			expect(data.total).toBe(NUM_RECORDS_PER_FILE - 1);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE / 2);
			expect(data.failed).toBeLessThan(1000);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"csv: multiple files",
		async () => {
			const files = TEST_PATHS.csv;
			const data = await mp({}, files, { ...opts, fixData: true });
			expect(data.total).toBe((NUM_RECORDS_PER_FILE * files.length) - 2);
			expect(data.success).toBeGreaterThan((NUM_RECORDS_PER_FILE * files.length) / 2);
			expect(data.failed).toBeLessThan(1000);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);


	test(
		"csv.gz: single file",
		async () => {
			const file = TEST_PATHS.csvgz[0];
			const data = await mp({}, file, { ...opts, fixData: true });
			expect(data.total).toBe(NUM_RECORDS_PER_FILE - 1);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE / 2);
			expect(data.failed).toBeLessThan(1000);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"csv.gz: multiple files",
		async () => {
			const files = TEST_PATHS.csvgz;
			const data = await mp({}, files, { ...opts, fixData: true });
			expect(data.total).toBe((NUM_RECORDS_PER_FILE * files.length) - 2);
			expect(data.success).toBeGreaterThan((NUM_RECORDS_PER_FILE * files.length) / 2);
			expect(data.failed).toBeLessThan(1000);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"parquet: single file",
		async () => {
			const file = TEST_PATHS.parquet[0];
			const data = await mp({}, file, { ...opts, fixData: true, transformFunc: parquetTransform });
			expect(data.total).toBe(NUM_RECORDS_PER_FILE-1);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE/2);
			expect(data.failed).toBeLessThan(500);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"parquet: multiple files",
		async () => {
			const files = TEST_PATHS.parquet;
			const data = await mp({}, files, { ...opts, fixData: true, transformFunc: parquetTransform });
			expect(data.total).toBe((NUM_RECORDS_PER_FILE * files.length) - 2);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE * files.length / 2);
			expect(data.failed).toBeLessThan(700);
			expect(data.duration).toBeGreaterThan(0);
		},
		longTimeout
	);

	test(
		"parquet.gz: single file",
		async () => {
			const file = TEST_PATHS.parquetgz[0];
			const data = await mp({}, file, { ...opts, fixData: true, transformFunc: parquetTransform });
			expect(data.total).toBe(NUM_RECORDS_PER_FILE-1);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE/2);
			expect(data.failed).toBeLessThan(500);
			expect(data.duration).toBeGreaterThan(0);
		}
	);

	test(
		"parquet.gz: multiple files",
		async () => {
			const files = TEST_PATHS.parquetgz;
			const data = await mp({}, files, { ...opts, fixData: true, transformFunc: parquetTransform });
			expect(data.total).toBe((NUM_RECORDS_PER_FILE * files.length) - 2);
			expect(data.success).toBeGreaterThan(NUM_RECORDS_PER_FILE * files.length / 2);
			expect(data.failed).toBeLessThan(700);
			expect(data.duration).toBeGreaterThan(0);
		}
	);

});
