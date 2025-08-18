// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
require("dotenv").config();
const { execSync } = require("child_process");

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
const longTimeout = IS_DEBUG_MODE ? 60000 : 10000;

const mp = require("../index.js");
const { showProgress } = require("../components/cli.js");

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
	verbose: IS_DEBUG_MODE,
	showProgress: IS_DEBUG_MODE,
	streamFormat: "jsonl",
	transformFunc: function noop(a) {
		return a;
	}
};


describe("vendor tests", () => {
	test(
		"mixpanel: events",
		async () => {
			const job = await mp({}, "testData/mixpanel/mixpanel-export-format.json", { ...opts, recordType: "event", vendor: "mixpanel", dryRun: true });
			const numRecords = job.dryRun.length;
			const numDistinctIds = job.dryRun.filter(a => a.properties.distinct_id).length;
			expect(numRecords).toBe(3000);
			expect(numDistinctIds).toBe(3000);
		}, longTimeout
	);
	test(
		"amplitude: events",
		async () => {
			const job = await mp({}, "./testData/amplitude/2023-04-10_1#0.json", { ...opts, recordType: "event", vendor: "amplitude", dryRun: true, vendorOpts: { v2_compat: false } });
			const numRecords = job.dryRun.length;
			const numDistinctIds = job.dryRun.filter(a => a.properties.distinct_id).length;
			expect(numRecords).toBe(4011);
			expect(numDistinctIds).toBe(0);

		},
		longTimeout
	);

	test(
		"amplitude: users",
		async () => {
			const job = await mp({}, "./testData/amplitude/2023-04-10_1#0.json", { ...opts, recordType: "user", vendor: "amplitude", dryRun: true });
			expect(job.dryRun.length).toBe(216);
		},
		longTimeout
	);


	test(
		"amplitude: events v2 compat",
		async () => {
			const job = await mp({}, "./testData/amplitude/2023-04-10_1#0.json", { ...opts, recordType: "event", vendor: "amplitude", dryRun: true, vendorOpts: { v2_compat: true } });
			const numRecords = job.dryRun.length;
			const numDistinctIds = job.dryRun.filter(a => a.properties.distinct_id).length;
			expect(numRecords).toBe(4011);
			expect(numDistinctIds).toBe(numRecords);


		},
		longTimeout
	);

	const heapIdMap = "./testData/heap/merged-users-mappings-test.json";

	test(
		"heap: events",
		async () => {
			const job = await mp({}, "./testData/heap/heap-events-ex.json", { ...opts, recordType: "event", vendor: "heap", dryRun: true });
			expect(job.dryRun.length).toBe(10000);

			const jobWithMerge = await mp({}, "./testData/heap/events-can-merge.json", { ...opts, recordType: "event", vendor: "heap", dryRun: true, vendorOpts: { device_id_file: heapIdMap } });
			expect(jobWithMerge.dryRun.length).toBe(12685);
			expect(jobWithMerge.dryRun.filter(a => a.properties.$user_id).length).toBe(12685); //.toBe(11510);
		},
		longTimeout
	);

	test(
		"heap: users",
		async () => {
			const job = await mp({}, "./testData/heap/heap-users-ex.json", { ...opts, recordType: "user", vendor: "heap", dryRun: true });
			expect(job.dryRun.length).toBe(1500);
		},
		longTimeout
	);


	test(
		"ga4: events",
		async () => {
			const job = await mp({}, "./testData/ga4/ga4_sample.json", { ...opts, recordType: "event", vendor: "ga4", dryRun: true });
			expect(job.dryRun.length).toBe(10000);
		},
		longTimeout
	);

	test(
		"ga4: users",
		async () => {
			const job = await mp({}, "./testData/ga4/ga4_sample.json", { ...opts, recordType: "user", vendor: "ga4", dryRun: true });
			expect(job.dryRun.length).toBeGreaterThan(3276);
		},
		longTimeout
	);

	test(
		"mparticle: events",
		async () => {
			const job = await mp({}, "./testData/mparticle/sample_data.txt", { ...opts, recordType: "event", vendor: "mparticle", dryRun: true, "streamFormat": "jsonl" });
			expect(job.dryRun.length).toBe(177);
			const { dryRun: data } = job;
			expect(data.every(e => e.event)).toBe(true);
			expect(data.every(e => e.properties)).toBe(true);
			expect(data.every(e => e.properties?.$device_id || e.properties?.$user_id)).toBe(true);
		},
		longTimeout
	);

	test(
		"mparticle: users",
		async () => {
			const job = await mp({}, "./testData/mparticle/sample_data.txt", { ...opts, recordType: "user", vendor: "mparticle", dryRun: true });
			expect(job.dryRun.length).toBe(64);
			const { dryRun: data } = job;
			expect(data.every(u => u.$distinct_id)).toBe(true);
			expect(data.every(u => u.$ip)).toBe(true);
			expect(data.every(u => u.$set)).toBe(true);
		},
		longTimeout
	);

	test(
		"posthog: events",
		async () => {
			const job = await mp({}, "./testData/posthog/events001.parquet",
				{
					...opts,
					streamFormat: 'parquet',
					recordType: "event",
					vendor: "posthog",
					vendorOpts: {
						v2_compat: true,

					},
					dryRun: true,
					dimensionMaps: [
						{
							filePath: "./testData/posthog/persons-smol.ndjson",
							keyOne: "distinct_id",
							keyTwo: "person_id",
							label: "people",

						}
					]
				}
			);
			expect(job.dryRun.length).toBe(17);
			const { dryRun: data } = job;
			expect(data.every(e => e.event)).toBe(true);
			expect(data.every(e => e.properties)).toBe(true);
		},
		longTimeout);



	test(
		"posthog: users",
		async () => {
			const job = await mp({}, "./testData/posthog/people-abbrev.parquet",
				{
					...opts,
					streamFormat: 'parquet',
					recordType: "user",
					vendor: "posthog",
					"epochStart": 1746392400,
					"epochEnd": 1748120400,
					dedupe: true,
					dryRun: true
				});
			
			expect(job.total).toBe(500);
			expect(job.dryRun.length).toBe(500);
			expect(job.duplicates).toBe(0);			
			const { dryRun: data } = job;
			expect(data.every(e => e.$distinct_id)).toBe(true);
		},
		longTimeout
	);

	test(
		"june: events",
		async () => {
			const job = await mp({}, "./testData/june/events-small.csv", { 
				...opts, 
				recordType: "event", 
				vendor: "june", 
				dryRun: true,
				streamFormat: "csv"
			});
			expect(job.dryRun.length).toBeGreaterThan(0);
			const { dryRun: data } = job;
			expect(data.every(e => e.event)).toBe(true);
			expect(data.every(e => e.properties)).toBe(true);
			expect(data.every(e => e.properties?.distinct_id)).toBe(true);
			expect(data.every(e => e.properties?.$source === 'june-to-mixpanel')).toBe(true);
			expect(data.every(e => e.properties?.$insert_id)).toBe(true);
		},
		longTimeout
	);

	test(
		"june: users",
		async () => {
			const job = await mp({}, "./testData/june/id-small.csv", { 
				...opts, 
				recordType: "user", 
				vendor: "june", 
				dryRun: true,
				streamFormat: "csv"
			});
			expect(job.dryRun.length).toBeGreaterThan(0);
			const { dryRun: data } = job;
			expect(data.every(u => u.$distinct_id)).toBe(true);
			expect(data.every(u => u.$set)).toBe(true);
			expect(data.every(u => u.$set?.$source === 'june-to-mixpanel')).toBe(true);
		},
		longTimeout
	);

	test(
		"june: groups",
		async () => {
			const job = await mp({}, "./testData/june/group-small.csv", { 
				...opts, 
				recordType: "group", 
				vendor: "june", 
				dryRun: true,
				streamFormat: "csv"
			});
			expect(job.dryRun.length).toBeGreaterThan(0);
			const { dryRun: data } = job;
			expect(data.every(g => g.$group_id)).toBe(true);
			expect(data.every(g => g.$group_key)).toBe(true);
			expect(data.every(g => g.$set)).toBe(true);
			expect(data.every(g => g.$set?.$source === 'june-to-mixpanel')).toBe(true);
		},
		longTimeout
	);


});

afterAll(async () => {
	execSync(`npm run prune`);
});