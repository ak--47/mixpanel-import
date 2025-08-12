// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-debugger */
/* eslint-disable no-unused-vars */
/* cSpell:disable */

const mpImport = require('../index.js');

describe('maxRecords functionality', () => {
	const fakeCreds = { acct: "test", pass: "test", project: "test" };

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

	test('maxRecords limits processing in normal mode', async () => {
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

	test('maxRecords limits processing in dryRun mode', async () => {
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

	test('maxRecords null processes all records', async () => {
		const testData = generateTestData(10); // Small dataset
		
		const result = await mpImport(fakeCreds, testData, {
			maxRecords: null, // Explicitly null
			dryRun: true
		});

		// Should process all 10 records
		expect(result.total).toBe(10);
		expect(result.dryRun.length).toBe(10);
	});

	test('no maxRecords processes all records', async () => {
		const testData = generateTestData(15); // Small dataset
		
		const result = await mpImport(fakeCreds, testData, {
			// No maxRecords specified
			dryRun: true
		});

		// Should process all 15 records
		expect(result.total).toBe(15);
		expect(result.dryRun.length).toBe(15);
	});

	test('maxRecords respects transforms and filters', async () => {
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

	test('maxRecords works with user profiles', async () => {
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

	test('maxRecords=0 processes no records', async () => {
		const testData = generateTestData(10);
		
		const result = await mpImport(fakeCreds, testData, {
			maxRecords: 0,
			dryRun: true
		});

		expect(result.total).toBe(0);
		expect(result.dryRun.length).toBe(0);
	});

	test('maxRecords larger than dataset processes all records', async () => {
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

	test('maxRecords works with different record types', async () => {
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