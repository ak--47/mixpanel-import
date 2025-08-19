//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
import { Readable } from "stream";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * STREAM TYPES BENCHMARK
 * 
 * Compares performance of different input data formats:
 * - JSON files (traditional array format)
 * - NDJSON files (newline-delimited JSON)
 * - Object streams (direct JavaScript objects)
 * 
 * Previous findings: Object Streams > NDJSON > JSON (11, 15, 29 seconds respectively)
 */

// Check for required environment variables
function checkCredentials() {
	const required = ['MP_PROJECT', 'MP_SECRET', 'MP_TOKEN'];
	const missing = required.filter(env => !process.env[env]);
	
	if (missing.length > 0) {
		console.error('❌ Missing required environment variables:');
		missing.forEach(env => console.error(`   - ${env}`));
		console.error('\nPlease set these environment variables before running benchmarks:');
		console.error('export MP_PROJECT=your_project_id');
		console.error('export MP_SECRET=your_api_secret');
		console.error('export MP_TOKEN=your_project_token');
		process.exit(1);
	}
	
	return {
		project: process.env.MP_PROJECT,
		secret: process.env.MP_SECRET,
		token: process.env.MP_TOKEN
	};
}

export default async function streamTypes(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('📄 STREAM TYPES BENCHMARK');
	console.log('Comparing JSON vs NDJSON vs Object Stream performance...');
	console.log('');

	const baseOptions = {
		logs: false,
		verbose: false,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 10,
		fixData: true,
		compress: true,
		recordsPerBatch: 2000
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	const results = {
		description: 'Data format comparison: JSON vs NDJSON vs Object Streams',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Derive file paths from the base data file
	const ndjsonFile = dataFile;
	const jsonFile = dataFile.replace(/\.ndjson$/, '.json').replace(/\.jsonl$/, '.json');
	
	// Create test configurations
	const streamTests = [
		{
			name: 'JSON File',
			dataSource: jsonFile,
			options: { ...baseOptions, streamFormat: 'json' },
			description: 'Traditional JSON array format'
		},
		{
			name: 'NDJSON File', 
			dataSource: ndjsonFile,
			options: { ...baseOptions, streamFormat: 'jsonl' },
			description: 'Newline-delimited JSON format'
		}
	];

	// Add object stream test if we can read the data
	try {
		let jsonData;
		if (await fileExists(jsonFile)) {
			jsonData = require(`../..${jsonFile.replace('./benchmarks', '/benchmarks')}`);
		} else if (await fileExists(ndjsonFile)) {
			// Read NDJSON and convert to array
			const { readFileSync } = await import('fs');
			const ndjsonContent = readFileSync(ndjsonFile, 'utf-8');
			jsonData = ndjsonContent.trim().split('\n').map(line => JSON.parse(line));
		}

		if (jsonData && Array.isArray(jsonData)) {
			const objectStream = new Readable({
				objectMode: true,
				read() {
					for (const item of jsonData) {
						this.push(item);
					}
					this.push(null);
				}
			});

			streamTests.push({
				name: 'Object Stream',
				dataSource: objectStream,
				options: { ...baseOptions },
				description: 'Direct JavaScript object stream'
			});
		}
	} catch (error) {
		console.log(`    ⚠️  Object stream test skipped: ${error.message}`);
	}

	// Run all tests
	for (let i = 0; i < streamTests.length; i++) {
		const test = streamTests[i];
		console.log(`  [${i + 1}/${streamTests.length}] Testing: ${test.name}...`);
		
		// Skip test if data file doesn't exist (for file-based tests)
		if (typeof test.dataSource === 'string' && !(await fileExists(test.dataSource))) {
			console.log(`    ⏭️  Skipped: Data file not found (${test.dataSource})`);
			continue;
		}

		try {
			const startTime = Date.now();
			const result = await mpStream({}, test.dataSource, test.options);
			const endTime = Date.now();
			
			const testResult = {
				name: test.name,
				description: test.description,
				config: test.options,
				dataSource: typeof test.dataSource === 'string' ? test.dataSource : '[Object Stream]',
				result: {
					eps: result.eps,
					rps: result.rps,
					mbps: result.mbps,
					duration: result.duration,
					durationHuman: result.durationHuman,
					success: result.success,
					failed: result.failed,
					total: result.total,
					bytes: result.bytes,
					bytesHuman: result.bytesHuman,
					memory: result.memory,
					wasStream: result.wasStream,
					avgBatchLength: result.avgBatchLength,
					workers: result.workers,
					requests: result.requests,
					retries: result.retries
				},
				actualDuration: endTime - startTime
			};

			results.tests.push(testResult);

			// Track best result (highest EPS)
			if (!results.bestResult || result.eps > results.bestResult.eps) {
				results.bestResult = {
					...testResult.result,
					name: testResult.name,
					config: testResult.config
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const successRate = ((result.success / result.total) * 100).toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB`);
			console.log(`    Duration: ${result.durationHuman}, Success: ${successRate}%`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				description: test.description,
				config: test.options,
				dataSource: typeof test.dataSource === 'string' ? test.dataSource : '[Object Stream]',
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeStreamTypeResults(results.tests);
	
	console.log('');
	console.log('📊 Stream Types Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   🏆 Fastest Format: ${results.analysis.fastestFormat}`);
		console.log(`   📈 Performance Ranking: ${results.analysis.ranking.join(' > ')}`);
		console.log(`   📊 Speed Improvements: ${results.analysis.improvements}`);
		console.log(`   💾 Memory Comparison: ${results.analysis.memoryComparison}`);
		console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

async function fileExists(filePath) {
	try {
		const { existsSync } = await import('fs');
		return existsSync(filePath);
	} catch {
		return false;
	}
}

function analyzeStreamTypeResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Sort by EPS performance
	const sortedTests = successfulTests.sort((a, b) => b.result.eps - a.result.eps);
	
	// Find fastest format
	const fastestTest = sortedTests[0];
	
	// Create performance ranking
	const ranking = sortedTests.map(test => test.name);
	
	// Calculate relative improvements
	const improvements = [];
	for (let i = 1; i < sortedTests.length; i++) {
		const current = sortedTests[i];
		const fastest = sortedTests[0];
		const improvement = ((fastest.result.eps - current.result.eps) / current.result.eps * 100).toFixed(1);
		improvements.push(`${fastest.name} is ${improvement}% faster than ${current.name}`);
	}

	// Memory comparison
	let memoryComparison = 'Memory usage varies by format';
	if (successfulTests.length >= 2) {
		const memoryUsages = successfulTests.map(t => ({
			name: t.name,
			memory: Math.round((t.result.memory?.heapUsed || 0) / 1024 / 1024)
		}));
		
		const lowest = memoryUsages.reduce((min, curr) => curr.memory < min.memory ? curr : min);
		const highest = memoryUsages.reduce((max, curr) => curr.memory > max.memory ? curr : max);
		
		if (lowest.memory !== highest.memory) {
			memoryComparison = `${lowest.name} uses least memory (${lowest.memory}MB), ${highest.name} uses most (${highest.memory}MB)`;
		}
	}

	// Generate recommendation
	let recommendation = `Use ${fastestTest.name} for optimal performance`;
	
	// Consider memory usage for large datasets
	const fastestMemory = Math.round((fastestTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	if (fastestMemory > 500) {
		// Find streaming alternative if available
		const streamingTest = successfulTests.find(t => t.result.wasStream);
		if (streamingTest && streamingTest.name !== fastestTest.name) {
			recommendation += `. For large datasets, consider ${streamingTest.name} to reduce memory usage`;
		}
	}

	return {
		fastestFormat: `${fastestTest.name} (${Math.round(fastestTest.result.eps).toLocaleString()} EPS)`,
		ranking,
		improvements: improvements.join('; '),
		memoryComparison,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length,
		detailedResults: successfulTests.map(t => ({
			name: t.name,
			eps: Math.round(t.result.eps).toLocaleString(),
			duration: t.result.durationHuman,
			memory: Math.round((t.result.memory?.heapUsed || 0) / 1024 / 1024) + 'MB',
			wasStream: t.result.wasStream
		}))
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	streamTypes(config).catch(console.error);
}