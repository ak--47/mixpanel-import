//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * TRANSPORT BENCHMARK
 * 
 * Compares performance between different HTTP transport libraries:
 * - GOT: Traditional HTTP client (current default)
 * - UNDICI: Modern high-performance HTTP client
 * 
 * Tests networking efficiency, request throughput, and connection handling
 * to determine the optimal transport layer for Mixpanel API requests.
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

export default async function transport(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🚀 TRANSPORT BENCHMARK');
	console.log('Comparing GOT vs UNDICI HTTP transport performance...');
	console.log('');

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 25, // Optimal from previous testing
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
		description: 'HTTP transport comparison: GOT vs UNDICI',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Test configurations  
	const transportTests = [
		{
			name: 'GOT Transport',
			config: { ...baseOptions, transport: 'got' },
			description: 'Traditional HTTP client with proven stability'
		},
		{
			name: 'UNDICI Transport',
			config: { ...baseOptions, transport: 'undici' },
			description: 'Modern high-performance HTTP client'
		}
	];

	// Run all tests
	for (let i = 0; i < transportTests.length; i++) {
		const test = transportTests[i];
		console.log(`  [${i + 1}/${transportTests.length}] Testing: ${test.name}...`);
		
		try {
			const startTime = Date.now();
			const result = await mpStream({}, dataFile, test.config);
			const endTime = Date.now();
			
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
			const retryRate = result.requests > 0 ? ((result.retries / result.requests) * 100).toFixed(1) : '0.0';
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB`);
			console.log(`    Duration: ${result.durationHuman}, Success: ${successRate}%`);
			console.log(`    Requests: ${u.comma(result.requests)}, Retries: ${u.comma(result.retries)} (${retryRate}%)`);
			console.log(`    Throughput: ${result.mbps.toFixed(1)} MB/s, Data: ${result.bytesHuman}`);
			
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

		// Small delay between tests to avoid any interference
		if (i < transportTests.length - 1) {
			console.log('    ⏳ Waiting 3 seconds before next test...');
			await new Promise(resolve => setTimeout(resolve, 3000));
		}
	}

	// Generate analysis
	results.analysis = analyzeTransportResults(results.tests);
	
	console.log('');
	console.log('📊 Transport Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   🏆 Faster Transport: ${results.analysis.fasterTransport}`);
		console.log(`   ⚡ Performance Improvement: ${results.analysis.performanceImprovement}`);
		console.log(`   🌐 Throughput Improvement: ${results.analysis.throughputImprovement}`);
		console.log(`   🔄 Reliability Comparison: ${results.analysis.reliabilityComparison}`);
		console.log(`   💾 Memory Comparison: ${results.analysis.memoryComparison}`);
		console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

function analyzeTransportResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length < 2) {
		return { error: 'Need at least 2 successful tests to compare transport methods' };
	}

	const gotTest = successfulTests.find(t => t.name.includes('GOT'));
	const undiciTest = successfulTests.find(t => t.name.includes('UNDICI'));

	if (!gotTest || !undiciTest) {
		return { error: 'Missing either GOT or UNDICI test results' };
	}

	// Performance comparison
	const gotEPS = gotTest.result.eps;
	const undiciEPS = undiciTest.result.eps;
	const fasterTransport = undiciEPS > gotEPS ? 'UNDICI' : 'GOT';
	const performanceImprovement = undiciEPS > gotEPS ? 
		`UNDICI is ${((undiciEPS - gotEPS) / gotEPS * 100).toFixed(1)}% faster (${Math.round(undiciEPS).toLocaleString()} vs ${Math.round(gotEPS).toLocaleString()} EPS)` :
		`GOT is ${((gotEPS - undiciEPS) / undiciEPS * 100).toFixed(1)}% faster (${Math.round(gotEPS).toLocaleString()} vs ${Math.round(undiciEPS).toLocaleString()} EPS)`;

	// Throughput comparison
	const gotMBPS = gotTest.result.mbps;
	const undiciMBPS = undiciTest.result.mbps;
	const throughputImprovement = undiciMBPS > gotMBPS ?
		`UNDICI achieves ${((undiciMBPS - gotMBPS) / gotMBPS * 100).toFixed(1)}% higher throughput (${undiciMBPS.toFixed(1)} vs ${gotMBPS.toFixed(1)} MB/s)` :
		`GOT achieves ${((gotMBPS - undiciMBPS) / undiciMBPS * 100).toFixed(1)}% higher throughput (${gotMBPS.toFixed(1)} vs ${undiciMBPS.toFixed(1)} MB/s)`;

	// Reliability comparison (based on retry rates)
	const gotRetryRate = gotTest.result.requests > 0 ? (gotTest.result.retries / gotTest.result.requests * 100) : 0;
	const undiciRetryRate = undiciTest.result.requests > 0 ? (undiciTest.result.retries / undiciTest.result.requests * 100) : 0;
	const reliabilityComparison = gotRetryRate < undiciRetryRate ?
		`GOT has lower retry rate (${gotRetryRate.toFixed(1)}% vs ${undiciRetryRate.toFixed(1)}%)` :
		undiciRetryRate < gotRetryRate ?
		`UNDICI has lower retry rate (${undiciRetryRate.toFixed(1)}% vs ${gotRetryRate.toFixed(1)}%)` :
		`Both transports have similar retry rates (~${gotRetryRate.toFixed(1)}%)`;

	// Memory comparison
	const gotMemoryMB = Math.round((gotTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	const undiciMemoryMB = Math.round((undiciTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	const memoryDiff = Math.abs(gotMemoryMB - undiciMemoryMB);
	const memoryComparison = memoryDiff < 5 ?
		`Similar memory usage (~${gotMemoryMB}MB)` :
		gotMemoryMB < undiciMemoryMB ?
		`GOT uses ${memoryDiff}MB less memory (${gotMemoryMB}MB vs ${undiciMemoryMB}MB)` :
		`UNDICI uses ${memoryDiff}MB less memory (${undiciMemoryMB}MB vs ${gotMemoryMB}MB)`;

	// Generate recommendation
	let recommendation = '';
	const significantPerformanceDiff = Math.abs(undiciEPS - gotEPS) / Math.min(undiciEPS, gotEPS) > 0.1; // >10% difference
	
	if (undiciEPS > gotEPS && significantPerformanceDiff) {
		recommendation = 'Use UNDICI transport for better performance';
		if (undiciRetryRate > gotRetryRate * 1.5) {
			recommendation += ', but monitor retry rates in production';
		}
	} else if (gotEPS > undiciEPS && significantPerformanceDiff) {
		recommendation = 'Use GOT transport for better performance and proven stability';
	} else {
		// Performance is similar
		if (undiciRetryRate < gotRetryRate) {
			recommendation = 'Use UNDICI transport for better reliability with similar performance';
		} else {
			recommendation = 'Both transports perform similarly - use GOT for stability or UNDICI for modern features';
		}
	}

	return {
		fasterTransport: `${fasterTransport} (${Math.round(Math.max(undiciEPS, gotEPS)).toLocaleString()} EPS)`,
		performanceImprovement,
		throughputImprovement,
		reliabilityComparison,
		memoryComparison,
		recommendation,
		detailedComparison: {
			got: {
				eps: Math.round(gotEPS).toLocaleString(),
				rps: Math.round(gotTest.result.rps).toLocaleString(),
				mbps: gotMBPS.toFixed(1),
				duration: gotTest.result.durationHuman,
				retryRate: gotRetryRate.toFixed(1) + '%',
				memory: gotMemoryMB + 'MB',
				successRate: ((gotTest.result.success / gotTest.result.total) * 100).toFixed(1) + '%'
			},
			undici: {
				eps: Math.round(undiciEPS).toLocaleString(),
				rps: Math.round(undiciTest.result.rps).toLocaleString(),
				mbps: undiciMBPS.toFixed(1),
				duration: undiciTest.result.durationHuman,
				retryRate: undiciRetryRate.toFixed(1) + '%',
				memory: undiciMemoryMB + 'MB',
				successRate: ((undiciTest.result.success / undiciTest.result.total) * 100).toFixed(1) + '%'
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
	transport(config).catch(console.error);
}