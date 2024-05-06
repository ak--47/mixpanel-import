// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
require("dotenv").config();
const { execSync } = require("child_process");
const longTimeout = 75000;

const mp = require("../index.js");

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


describe("vendor tests", () => {

	test(
		"amplitude: events",
		async () => {
			const job = await mp({}, "./testData/amplitude/2023-04-10_1#0.json", { ...opts, recordType: "event", vendor: "amplitude", dryRun: true, vendorOpts: { v2_compat: false }});
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
			expect(jobWithMerge.dryRun.filter(a => a.properties.$user_id).length).toBe(11510);
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
			expect(job.dryRun.length).toBe(3276);
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


});

afterAll(async () => {
	execSync(`npm run prune`);
});