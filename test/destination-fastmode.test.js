/**
 * Sanity tests for fastMode and destination features
 */

const main = require('../index.js');
const fs = require('fs');
const path = require('path');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);

describe('Destination and FastMode Features', () => {
	const testDir = path.join(__dirname, 'test-output');

	// Sample test data
	const testEvents = [
		{
			event: 'Page View',
			properties: {
				distinct_id: 'user_001',
				time: 1234567890000,
				$browser: 'Chrome',
				$os: 'Windows',
				page: '/home'
			}
		},
		{
			event: 'Button Click',
			properties: {
				distinct_id: 'user_002',
				time: 1234567891000,
				$browser: 'Firefox',
				button: 'signup',
				page: '/pricing'
			}
		},
		{
			event: 'Form Submit',
			properties: {
				distinct_id: 'user_003',
				time: 1234567892000,
				form: 'contact',
				success: true
			}
		}
	];

	beforeAll(() => {
		// Create test directory
		if (!fs.existsSync(testDir)) {
			fs.mkdirSync(testDir, { recursive: true });
		}
	});

	afterEach(() => {
		// Clean up test files after each test
		const files = fs.readdirSync(testDir);
		files.forEach(file => {
			fs.unlinkSync(path.join(testDir, file));
		});
	});

	afterAll(() => {
		// Remove test directory
		if (fs.existsSync(testDir)) {
			fs.rmSync(testDir, { recursive: true });
		}
	});

	describe('Destination Feature', () => {
		test('should write to local file with explicit destination', async () => {
			const destPath = path.join(testDir, 'explicit-dest.ndjson');

			const result = await main(
				{ token: 'test-token' },
				testEvents,
				{
					recordType: 'event',
					dryRun: true,
					destination: destPath,
					verbose: false
				}
			);

			// Check file exists
			expect(fs.existsSync(destPath)).toBe(true);

			// Read and verify content
			const content = fs.readFileSync(destPath, 'utf8');
			const lines = content.trim().split('\n');
			expect(lines).toHaveLength(3);

			// Parse and verify first line
			const firstRecord = JSON.parse(lines[0]);
			expect(firstRecord.event).toBe('Page View');
			expect(firstRecord.properties.distinct_id).toBe('user_001');
		});

		test('should auto-generate filename when destination is a directory', async () => {
			const result = await main(
				{ token: 'test-token' },
				testEvents,
				{
					recordType: 'event',
					dryRun: true,
					destination: testDir, // Just provide directory
					verbose: false
				}
			);

			// Check that a file was created with pattern event-YYYY-MM-DD-*.ndjson
			const files = fs.readdirSync(testDir);
			const eventFiles = files.filter(f => f.startsWith('event-') && f.endsWith('.ndjson'));
			expect(eventFiles).toHaveLength(1);

			// Verify content
			const filePath = path.join(testDir, eventFiles[0]);
			const content = fs.readFileSync(filePath, 'utf8');
			const lines = content.trim().split('\n');
			expect(lines).toHaveLength(3);
		});

		test('should write compressed file when destination ends with .gz', async () => {
			const destPath = path.join(testDir, 'compressed.ndjson.gz');

			const result = await main(
				{ token: 'test-token' },
				testEvents,
				{
					recordType: 'event',
					dryRun: true,
					destination: destPath,
					verbose: false
				}
			);

			// Check file exists and is smaller than uncompressed
			expect(fs.existsSync(destPath)).toBe(true);
			const stats = fs.statSync(destPath);
			expect(stats.size).toBeGreaterThan(0);
			expect(stats.size).toBeLessThan(500); // Compressed should be small
		});

		test('should work with destinationOnly mode', async () => {
			const destPath = path.join(testDir, 'destination-only.ndjson');

			const result = await main(
				{ token: 'not-needed' },
				testEvents,
				{
					recordType: 'event',
					destination: destPath,
					destinationOnly: true,
					verbose: false
				}
			);

			// Check file exists
			expect(fs.existsSync(destPath)).toBe(true);

			// Verify no API calls were made
			expect(result.requests).toBe(0);
			expect(result.success).toBe(0); // No records sent to Mixpanel

			// But file should have all records
			const content = fs.readFileSync(destPath, 'utf8');
			const lines = content.trim().split('\n');
			expect(lines).toHaveLength(3);
		});
	});

	describe('FastMode Feature', () => {
		test('should skip transformations in fast mode', async () => {
			const destPath = path.join(testDir, 'fast-mode.ndjson');

			// Events with intentionally bad time format that would normally be fixed
			const eventsWithBadTime = [
				{
					event: 'Test Event',
					properties: {
						distinct_id: 'user_001',
						time: '2024-01-01T00:00:00Z', // String time (would normally be converted to ms)
						value: 'not-transformed'
					}
				}
			];

			const result = await main(
				{ token: 'test-token' },
				eventsWithBadTime,
				{
					recordType: 'event',
					dryRun: true,
					fastMode: true,
					fixTime: true, // This should be ignored in fast mode
					destination: destPath,
					verbose: false
				}
			);

			// Read and verify time was NOT transformed
			const content = fs.readFileSync(destPath, 'utf8');
			const record = JSON.parse(content.trim());

			// In fast mode, time should remain as string
			expect(record.properties.time).toBe('2024-01-01T00:00:00Z');
			expect(typeof record.properties.time).toBe('string');
		});

		test('should apply transformations in normal mode', async () => {
			const destPath = path.join(testDir, 'normal-mode.ndjson');

			// Same event as above
			const eventsWithBadTime = [
				{
					event: 'Test Event',
					properties: {
						distinct_id: 'user_001',
						time: '2024-01-01T00:00:00Z',
						value: 'will-be-transformed'
					}
				}
			];

			const result = await main(
				{ token: 'test-token' },
				eventsWithBadTime,
				{
					recordType: 'event',
					dryRun: true,
					fastMode: false, // Normal mode
					fixTime: true,
					destination: destPath,
					verbose: false
				}
			);

			// Read and verify time WAS transformed
			const content = fs.readFileSync(destPath, 'utf8');
			const record = JSON.parse(content.trim());

			// In normal mode, time should be converted to milliseconds
			expect(typeof record.properties.time).toBe('number');
			expect(record.properties.time).toBe(1704067200000); // 2024-01-01T00:00:00Z in ms
		});

		test('should be faster in fast mode', async () => {
			// Generate larger dataset for measurable difference
			const largeDataset = [];
			for (let i = 0; i < 1000; i++) {
				largeDataset.push({
					event: `Event ${i}`,
					properties: {
						distinct_id: `user_${i}`,
						time: Date.now(),
						index: i,
						nested: { deep: { value: i } }
					}
				});
			}

			// Measure fast mode
			const fastStart = Date.now();
			await main(
				{ token: 'test-token' },
				largeDataset,
				{
					recordType: 'event',
					dryRun: true,
					fastMode: true,
					destination: path.join(testDir, 'fast-large.ndjson'),
					verbose: false
				}
			);
			const fastTime = Date.now() - fastStart;

			// Measure normal mode
			const normalStart = Date.now();
			await main(
				{ token: 'test-token' },
				largeDataset,
				{
					recordType: 'event',
					dryRun: true,
					fastMode: false,
					destination: path.join(testDir, 'normal-large.ndjson'),
					verbose: false
				}
			);
			const normalTime = Date.now() - normalStart;

			// Fast mode should be at least slightly faster (transforms are skipped)
			console.log(`Fast mode: ${fastTime}ms, Normal mode: ${normalTime}ms`);
			expect(fastTime).toBeLessThanOrEqual(normalTime);
		});
	});

	describe('Auto-generated filenames', () => {
		test('should generate event filename with timestamp', async () => {
			const result = await main(
				{ token: 'test-token' },
				testEvents,
				{
					recordType: 'event',
					dryRun: true,
					destination: testDir,
					verbose: false
				}
			);

			const files = fs.readdirSync(testDir);
			const eventFile = files.find(f => f.startsWith('event-') && f.endsWith('.ndjson'));
			expect(eventFile).toBeDefined();

			// Check filename format: event-YYYY-MM-DDTHH-mm-ss-sssZ.ndjson
			const pattern = /^event-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.ndjson$/;
			expect(eventFile).toMatch(pattern);
		});

		test('should generate user profile filename', async () => {
			const userProfiles = [
				{ $distinct_id: 'user_001', $set: { name: 'Alice' } },
				{ $distinct_id: 'user_002', $set: { name: 'Bob' } }
			];

			const result = await main(
				{ token: 'test-token' },
				userProfiles,
				{
					recordType: 'user',
					dryRun: true,
					destination: testDir,
					verbose: false
				}
			);

			const files = fs.readdirSync(testDir);
			const userFile = files.find(f => f.startsWith('user-') && f.endsWith('.ndjson'));
			expect(userFile).toBeDefined();
			expect(userFile).toMatch(/^user-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z\.ndjson$/);
		});

		test('should generate group profile filename', async () => {
			const groupProfiles = [
				{ $group_key: 'company', $group_id: 'acme', $set: { name: 'Acme Corp' } }
			];

			const result = await main(
				{ token: 'test-token', groupKey: 'company' },
				groupProfiles,
				{
					recordType: 'group',
					dryRun: true,
					destination: testDir,
					verbose: false
				}
			);

			const files = fs.readdirSync(testDir);
			const groupFile = files.find(f => f.startsWith('group-') && f.endsWith('.ndjson'));
			expect(groupFile).toBeDefined();
		});
	});

	describe('Edge cases', () => {
		test('should throw error if destinationOnly without destination', async () => {
			await expect(main(
				{ token: 'test' },
				testEvents,
				{
					recordType: 'event',
					destinationOnly: true,
					// No destination specified!
					verbose: false
				}
			)).rejects.toThrow('destination is required when destinationOnly is true');
		});

		test('should handle empty dataset', async () => {
			const destPath = path.join(testDir, 'empty.ndjson');

			const result = await main(
				{ token: 'test-token' },
				[], // Empty array
				{
					recordType: 'event',
					dryRun: true,
					destination: destPath,
					verbose: false
				}
			);

			// File should be created but empty or very small
			expect(fs.existsSync(destPath)).toBe(true);
			const content = fs.readFileSync(destPath, 'utf8');
			expect(content.trim()).toBe('');
		});

		test('should work with both destination and fastMode', async () => {
			const destPath = path.join(testDir, 'fast-dest.ndjson');

			const result = await main(
				{ token: 'test-token' },
				testEvents,
				{
					recordType: 'event',
					dryRun: true,
					fastMode: true,
					destination: destPath,
					verbose: false
				}
			);

			// Check file exists
			expect(fs.existsSync(destPath)).toBe(true);

			// Verify data is untransformed (fast mode)
			const content = fs.readFileSync(destPath, 'utf8');
			const lines = content.trim().split('\n');
			expect(lines).toHaveLength(3);

			const firstRecord = JSON.parse(lines[0]);
			// Time should be exactly as provided (not transformed)
			expect(firstRecord.properties.time).toBe(1234567890000);
		});
	});
});