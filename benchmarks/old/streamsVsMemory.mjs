//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * STREAMS VS MEMORY BENCHMARK
 * 
 * Compares streaming processing vs memory-based processing to determine
 * the optimal approach for different dataset sizes and memory constraints.
 * 
 * Previous findings: Memory is faster than streams (250k @ 12 seconds vs 14 seconds)
 * This benchmark will verify and expand on those findings with detailed analysis.
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

export default async function streamsVsMemory(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🌊 STREAMS VS MEMORY BENCHMARK');
	console.log('Comparing streaming vs memory processing performance...');
	console.log('');

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 10,
		fixData: true,
		compress: true,
		recordsPerBatch: 2000,
		streamFormat: 'jsonl'
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	const results = {
		description: 'Streaming vs memory processing comparison',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Test configurations
	const processingTests = [
		{
			name: 'Memory Processing',
			config: { ...baseOptions, forceStream: false },
			description: 'Load entire dataset into memory for processing'
		},
		{
			name: 'Stream Processing',
			config: { ...baseOptions, forceStream: true },
			description: 'Process data as a stream to minimize memory usage'
		}
	];

	// Run all tests
	for (let i = 0; i < processingTests.length; i++) {
		const test = processingTests[i];
		console.log(`  [${i + 1}/${processingTests.length}] Testing: ${test.name}...`);
		
		try {
			const startTime = Date.now();
			const startMemory = process.memoryUsage();
			
			const result = await mpStream({}, dataFile, test.config);
			
			const endTime = Date.now();
			const endMemory = process.memoryUsage();
			
			// Calculate memory delta
			const memoryDelta = {
				heapUsed: endMemory.heapUsed - startMemory.heapUsed,
				heapTotal: endMemory.heapTotal - startMemory.heapTotal,
				external: endMemory.external - startMemory.external,
				rss: endMemory.rss - startMemory.rss
			};
			
			const testResult = {
				name: test.name,
				description: test.description,
				config: test.config,
				dataFile: dataFile,
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
				memoryProfile: {
					startMemory,
					endMemory,
					memoryDelta,
					peakHeapMB: Math.round((result.memory?.heapUsed || 0) / 1024 / 1024),
					memoryGrowthMB: Math.round(memoryDelta.heapUsed / 1024 / 1024),
					memoryEfficiency: Math.round(result.total / Math.max(1, memoryDelta.heapUsed / 1024))
				},
				actualDuration: endTime - startTime
			};

			results.tests.push(testResult);

			// Track best result (highest EPS)
			if (!results.bestResult || result.eps > results.bestResult.eps) {
				results.bestResult = {
					...testResult.result,
					name: testResult.name,
					config: testResult.config,
					memoryProfile: testResult.memoryProfile
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const memoryGrowth = Math.round(memoryDelta.heapUsed / 1024 / 1024);
			const successRate = ((result.success / result.total) * 100).toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Duration: ${result.durationHuman}`);
			console.log(`    Memory: ${memoryMB}MB peak, +${memoryGrowth}MB growth`);
			console.log(`    Mode: ${result.wasStream ? 'Streaming' : 'Memory'}, Success: ${successRate}%`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				description: test.description,
				config: test.config,
				dataFile: dataFile,
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeStreamsVsMemoryResults(results.tests);
	
	console.log('');
	console.log('📊 Streams vs Memory Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   🏆 Faster Method: ${results.analysis.fasterMethod}`);
		console.log(`   ⚡ Speed Difference: ${results.analysis.speedDifference}`);
		console.log(`   💾 Memory Difference: ${results.analysis.memoryDifference}`);
		console.log(`   📊 Efficiency Trade-offs: ${results.analysis.tradeoffs}`);
		console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

function analyzeStreamsVsMemoryResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length < 2) {
		return { error: 'Need at least 2 successful tests to compare streams vs memory' };
	}

	const memoryTest = successfulTests.find(t => !t.result.wasStream);
	const streamTest = successfulTests.find(t => t.result.wasStream);

	if (!memoryTest || !streamTest) {
		return { error: 'Missing either memory or stream test results' };
	}

	// Performance comparison
	const memoryEPS = memoryTest.result.eps;
	const streamEPS = streamTest.result.eps;
	const speedDifferencePercent = ((Math.abs(memoryEPS - streamEPS) / Math.min(memoryEPS, streamEPS)) * 100).toFixed(1);
	const fasterMethod = memoryEPS > streamEPS ? 'Memory Processing' : 'Stream Processing';
	const speedDifference = `${fasterMethod} is ${speedDifferencePercent}% faster (${Math.round(memoryEPS).toLocaleString()} vs ${Math.round(streamEPS).toLocaleString()} EPS)`;

	// Memory comparison
	const memoryUsageMB = memoryTest.memoryProfile?.peakHeapMB || 0;
	const streamUsageMB = streamTest.memoryProfile?.peakHeapMB || 0;
	const memoryGrowthDiff = Math.abs(memoryUsageMB - streamUsageMB);
	const memoryDifference = memoryUsageMB > streamUsageMB ? 
		`Memory processing uses ${memoryGrowthDiff}MB more (${memoryUsageMB}MB vs ${streamUsageMB}MB)` :
		`Stream processing uses ${memoryGrowthDiff}MB more (${streamUsageMB}MB vs ${memoryUsageMB}MB)`;

	// Trade-offs analysis
	let tradeoffs = '';
	if (memoryEPS > streamEPS && memoryUsageMB > streamUsageMB) {
		tradeoffs = 'Memory processing trades higher memory usage for better performance';
	} else if (streamEPS > memoryEPS && streamUsageMB < memoryUsageMB) {
		tradeoffs = 'Stream processing offers better memory efficiency with comparable performance';
	} else if (memoryEPS > streamEPS && streamUsageMB > memoryUsageMB) {
		tradeoffs = 'Memory processing is faster but streaming uses more memory (unexpected)';
	} else {
		tradeoffs = 'Stream processing is faster and more memory efficient (ideal scenario)';
	}

	// Generate recommendation
	let recommendation = '';
	const speedDiffNum = parseFloat(speedDifferencePercent);
	
	if (speedDiffNum < 5) {
		// Small performance difference
		recommendation = 'Use stream processing for memory efficiency with minimal performance impact';
	} else if (memoryEPS > streamEPS) {
		// Memory is significantly faster
		if (memoryUsageMB < 500) {
			recommendation = 'Use memory processing for better performance with acceptable memory usage';
		} else {
			recommendation = 'Use memory processing for small datasets, stream processing for large datasets';
		}
	} else {
		// Stream is faster (unexpected but possible)
		recommendation = 'Use stream processing for both performance and memory advantages';
	}

	return {
		fasterMethod,
		speedDifference,
		memoryDifference,
		tradeoffs,
		recommendation,
		detailedComparison: {
			memory: {
				eps: Math.round(memoryEPS).toLocaleString(),
				duration: memoryTest.result.durationHuman,
				peakMemory: `${memoryUsageMB}MB`,
				memoryGrowth: `+${memoryTest.memoryProfile?.memoryGrowthMB || 0}MB`,
				wasStream: memoryTest.result.wasStream
			},
			stream: {
				eps: Math.round(streamEPS).toLocaleString(),
				duration: streamTest.result.durationHuman,
				peakMemory: `${streamUsageMB}MB`,
				memoryGrowth: `+${streamTest.memoryProfile?.memoryGrowthMB || 0}MB`,
				wasStream: streamTest.result.wasStream
			}
		}
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	streamsVsMemory(config).catch(console.error);
}