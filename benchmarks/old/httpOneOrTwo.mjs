//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * HTTP/1.1 vs HTTP/2 BENCHMARK
 * 
 * Tests performance differences between HTTP/1.1 and HTTP/2 protocols.
 * This benchmark helps determine if HTTP/2 provides performance benefits
 * for the mixpanel-import module's API requests.
 * 
 * Key findings from previous tests:
 * - HTTP/2 can provide performance improvements in certain scenarios
 * - Benefits vary based on connection patterns and server support
 * - Worker count and batch size affect the impact
 */

export default async function httpOneOrTwo(config = {}) {
	const { dataFile, dryRun = true } = config;
	
	console.log('🌐 HTTP/1.1 vs HTTP/2 BENCHMARK');
	console.log('Testing HTTP protocol performance differences...');
	console.log('');

	// Use good defaults from other benchmarks
	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		streamFormat: 'jsonl',
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 50, // Higher worker count to stress HTTP connections
		recordsPerBatch: 2000,
		compress: true,
		fixData: true
	};

	// Test HTTP protocol configurations
	const httpTests = [
		{
			name: 'HTTP/1.1 (Default)',
			config: {
				http2: false
			}
		},
		{
			name: 'HTTP/2 (Multiplexed)',
			config: {
				http2: true
			}
		}
	];

	// Also test with different worker counts to see HTTP/2 scaling
	const workerTests = [
		{
			name: 'HTTP/1.1 (10 Workers)',
			config: {
				http2: false,
				workers: 10
			}
		},
		{
			name: 'HTTP/2 (10 Workers)',
			config: {
				http2: true,
				workers: 10
			}
		},
		{
			name: 'HTTP/1.1 (25 Workers)',
			config: {
				http2: false,
				workers: 25
			}
		},
		{
			name: 'HTTP/2 (25 Workers)',
			config: {
				http2: true,
				workers: 25
			}
		},
		{
			name: 'HTTP/1.1 (50 Workers)',
			config: {
				http2: false,
				workers: 50
			}
		},
		{
			name: 'HTTP/2 (50 Workers)',
			config: {
				http2: true,
				workers: 50
			}
		}
	];

	const results = {
		description: 'HTTP/1.1 vs HTTP/2 protocol performance comparison',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	const allTests = [...httpTests, ...workerTests];

	for (let i = 0; i < allTests.length; i++) {
		const test = allTests[i];
		console.log(`  [${i + 1}/${allTests.length}] Testing: ${test.name}...`);
		
		const options = {
			...baseOptions,
			...test.config
		};

		try {
			const startTime = Date.now();
			const result = await mpStream({}, dataFile, options);
			const endTime = Date.now();
			
			const testResult = {
				name: test.name,
				config: test.config,
				result: {
					eps: result.eps,
					rps: result.rps,
					mbps: result.mbps,
					duration: result.duration,
					success: result.success,
					failed: result.failed,
					bytes: result.bytes,
					bytesHuman: result.bytesHuman,
					memory: result.memory,
					avgBatchLength: result.avgBatchLength,
					retries: result.retries,
					workers: result.workers,
					http2: test.config.http2
				},
				actualDuration: endTime - startTime
			};

			results.tests.push(testResult);

			// Track best result (highest RPS for HTTP comparison)
			if (!results.bestResult || result.rps > results.bestResult.rps) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config,
					name: testResult.name
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const protocol = test.config.http2 ? 'HTTP/2' : 'HTTP/1.1';
			const workers = test.config.workers || baseOptions.workers;
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Protocol: ${protocol}, Workers: ${workers}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				config: test.config,
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeHttpProtocols(results.tests);
	
	console.log('');
	console.log('📊 HTTP Protocol Analysis:');
	console.log(`   🏆 Best Protocol: ${results.analysis.bestProtocol} (${Math.round(results.analysis.bestRps).toLocaleString()} RPS)`);
	console.log(`   📈 Performance Difference: ${results.analysis.protocolComparison}`);
	console.log(`   👥 Worker Scaling: ${results.analysis.workerScaling}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

function analyzeHttpProtocols(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find best protocol
	const bestTest = successfulTests.reduce((best, current) => 
		current.result.rps > best.result.rps ? current : best
	);

	// Compare base HTTP/1.1 vs HTTP/2 tests
	const http1Test = successfulTests.find(t => t.name === 'HTTP/1.1 (Default)');
	const http2Test = successfulTests.find(t => t.name === 'HTTP/2 (Multiplexed)');
	
	const protocolComparison = analyzeProtocolComparison(http1Test, http2Test);
	
	// Analyze worker scaling differences
	const workerScaling = analyzeWorkerScaling(successfulTests);
	
	// Generate recommendation
	const recommendation = generateHttpRecommendation(successfulTests);

	return {
		bestProtocol: bestTest.result.http2 ? 'HTTP/2' : 'HTTP/1.1',
		bestRps: bestTest.result.rps,
		protocolComparison,
		workerScaling,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeProtocolComparison(http1Test, http2Test) {
	if (!http1Test || !http2Test) {
		return 'Insufficient protocol comparison data';
	}

	const rpsImprovement = ((http2Test.result.rps - http1Test.result.rps) / http1Test.result.rps * 100);
	const epsImprovement = ((http2Test.result.eps - http1Test.result.eps) / http1Test.result.eps * 100);
	const timeImprovement = ((http1Test.result.duration - http2Test.result.duration) / http1Test.result.duration * 100);

	const winner = http2Test.result.rps > http1Test.result.rps ? 'HTTP/2' : 'HTTP/1.1';
	const rpsChange = Math.abs(rpsImprovement).toFixed(1);
	
	return `${winner} is ${rpsChange}% ${rpsImprovement > 0 ? 'faster' : 'slower'} (RPS: ${Math.round(http1Test.result.rps)} vs ${Math.round(http2Test.result.rps)})`;
}

function analyzeWorkerScaling(tests) {
	const workerTests = tests.filter(t => t.name.includes('Workers'));
	
	if (workerTests.length < 4) {
		return 'Insufficient worker scaling data';
	}

	// Group by protocol and worker count
	const http1Tests = workerTests.filter(t => !t.result.http2);
	const http2Tests = workerTests.filter(t => t.result.http2);

	if (http1Tests.length === 0 || http2Tests.length === 0) {
		return 'Missing protocol data for worker scaling analysis';
	}

	// Find best performing worker count for each protocol
	const bestHttp1 = http1Tests.reduce((best, current) => 
		current.result.rps > best.result.rps ? current : best
	);
	
	const bestHttp2 = http2Tests.reduce((best, current) => 
		current.result.rps > best.result.rps ? current : best
	);

	const http1Workers = bestHttp1.result.workers;
	const http2Workers = bestHttp2.result.workers;
	
	return `Best: HTTP/1.1 with ${http1Workers} workers vs HTTP/2 with ${http2Workers} workers`;
}

function generateHttpRecommendation(tests) {
	// Find overall best performer
	const bestTest = tests.reduce((best, current) => 
		current.result.rps > best.result.rps ? current : best
	);
	
	const protocol = bestTest.result.http2 ? 'HTTP/2' : 'HTTP/1.1';
	const workers = bestTest.result.workers;
	const rpsFormatted = Math.round(bestTest.result.rps).toLocaleString();
	
	// Check if there's a significant difference between protocols
	const http1Tests = tests.filter(t => !t.result.http2);
	const http2Tests = tests.filter(t => t.result.http2);
	
	if (http1Tests.length > 0 && http2Tests.length > 0) {
		const avgHttp1Rps = http1Tests.reduce((sum, test) => sum + test.result.rps, 0) / http1Tests.length;
		const avgHttp2Rps = http2Tests.reduce((sum, test) => sum + test.result.rps, 0) / http2Tests.length;
		
		const difference = Math.abs((avgHttp2Rps - avgHttp1Rps) / avgHttp1Rps * 100);
		
		if (difference < 5) {
			return `Minimal difference between protocols (<5%). Use HTTP/1.1 for better compatibility`;
		}
	}
	
	return `Use ${protocol} with ${workers} workers for best performance (${rpsFormatted} RPS)`;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true
	};
	httpOneOrTwo(config).catch(console.error);
}