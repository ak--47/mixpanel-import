//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * TRANSPORT COMPARISON BENCHMARK
 * 
 * Tests different HTTP transport implementations to find the fastest client.
 * This is critical for overall performance since HTTP requests are often the bottleneck.
 * 
 * Transports tested:
 * - got: Current default HTTP client (robust, feature-rich)
 * - undici: Modern HTTP/1.1 client (potentially faster)
 * - node-fetch: Popular fetch implementation
 * - native fetch: Node.js built-in fetch (Node 18+)
 * 
 * Key metrics:
 * - Requests per second (RPS)
 * - Connection efficiency
 * - Error rates and retry behavior
 * - Memory usage patterns
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

export default async function transportComparison(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🚀 TRANSPORT COMPARISON BENCHMARK');
	console.log('Testing different HTTP transport implementations...');
	console.log('');

	// Use optimal settings from previous benchmarks
	const optimalWorkers = 20;

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		streamFormat: 'jsonl',
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: optimalWorkers,
		recordsPerBatch: 2000,
		compress: true,
		compressionLevel: 6,
		fixData: true,
		strict: false
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	// Test different transport configurations
	const transportTests = [
		{
			name: 'GOT (Default)',
			config: {
				transport: 'got'
			}
		},
		{
			name: 'UNDICI (Fast)',
			config: {
				transport: 'undici'
			}
		},
		{
			name: 'Node Fetch',
			config: {
				transport: 'node-fetch'
			}
		},
		{
			name: 'Native Fetch',
			config: {
				transport: 'fetch'
			}
		}
	];

	// Add retry behavior tests for the best performing transport
	const retryTests = [
		{
			name: 'GOT (No Retries)',
			config: {
				transport: 'got',
				retries: 0
			}
		},
		{
			name: 'GOT (Standard Retries)',
			config: {
				transport: 'got',
				retries: 3
			}
		},
		{
			name: 'GOT (Aggressive Retries)',
			config: {
				transport: 'got',
				retries: 5
			}
		}
	];

	const results = {
		description: 'HTTP transport performance comparison',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Test main transports
	for (let i = 0; i < transportTests.length; i++) {
		const test = transportTests[i];
		console.log(`  [${i + 1}/${transportTests.length}] Testing: ${test.name}...`);
		
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
					workers: result.workers
				},
				actualDuration: endTime - startTime,
				efficiency: calculateTransportEfficiency(result)
			};

			results.tests.push(testResult);

			// Track best result (highest RPS - most important for transport)
			if (!results.bestResult || result.rps > results.bestResult.rps) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config,
					name: testResult.name,
					efficiency: testResult.efficiency
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const efficiency = testResult.efficiency.toFixed(1);
			const errorRate = result.success > 0 ? ((result.failed / (result.success + result.failed)) * 100).toFixed(1) : '0';
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Efficiency: ${efficiency}, Error Rate: ${errorRate}%`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				config: test.config,
				error: error.message
			});
		}
	}

	// Test retry configurations for best transport (if dryRun is false)
	if (!dryRun && results.bestResult) {
		console.log('');
		console.log('🔄 Testing retry configurations for best transport...');
		
		const bestTransport = results.bestResult.config.transport;
		const retryTestsForBest = retryTests.filter(t => t.config.transport === bestTransport);
		
		for (let i = 0; i < retryTestsForBest.length; i++) {
			const test = retryTestsForBest[i];
			console.log(`  [${i + 1}/${retryTestsForBest.length}] Testing: ${test.name}...`);
			
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
						retries: result.retries,
						memory: result.memory
					},
					actualDuration: endTime - startTime,
					efficiency: calculateTransportEfficiency(result)
				};

				results.tests.push(testResult);
				
				const rpsFormatted = Math.round(result.rps).toLocaleString();
				const retryCount = result.retries || 0;
				console.log(`    RPS: ${rpsFormatted}, Retries: ${retryCount}, Success: ${result.success}`);
				
			} catch (error) {
				console.log(`    ❌ Failed: ${error.message}`);
				results.tests.push({
					name: test.name,
					config: test.config,
					error: error.message
				});
			}
		}
	}

	// Generate analysis
	results.analysis = analyzeTransportResults(results.tests);
	
	console.log('');
	console.log('📊 Transport Comparison Analysis:');
	console.log(`   🏆 Fastest Transport: ${results.analysis.fastestTransport} (${Math.round(results.analysis.fastestRps).toLocaleString()} RPS)`);
	console.log(`   📈 Performance Ranking: ${results.analysis.ranking.join(' > ')}`);
	console.log(`   🔄 Retry Impact: ${results.analysis.retryAnalysis}`);
	console.log(`   ⚡ Speed Improvement: ${results.analysis.speedImprovement}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

function calculateTransportEfficiency(result) {
	// Efficiency considers RPS, error rate, and retry count
	// Higher RPS is better, lower errors/retries are better
	
	const baseRps = result.rps || 0;
	const errorPenalty = (result.failed || 0) * 5; // 5 point penalty per error
	const retryPenalty = (result.retries || 0) * 2; // 2 point penalty per retry
	
	return Math.max(0, baseRps - errorPenalty - retryPenalty);
}

function analyzeTransportResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find fastest transport (highest RPS)
	const fastestTest = successfulTests.reduce((fastest, current) => 
		current.result.rps > fastest.result.rps ? current : fastest
	);

	// Create performance ranking
	const transportTests = successfulTests.filter(t => !t.name.includes('Retries'));
	const ranking = transportTests
		.sort((a, b) => b.result.rps - a.result.rps)
		.map(test => {
			const transport = test.config.transport || test.name.split(' ')[0];
			return transport.toUpperCase();
		});

	// Analyze retry impact
	const retryAnalysis = analyzeRetryImpact(successfulTests);
	
	// Calculate speed improvement over slowest
	const speedImprovement = calculateSpeedImprovement(transportTests);

	// Generate recommendation
	const recommendation = generateTransportRecommendation(successfulTests);

	return {
		fastestTransport: fastestTest.config.transport || fastestTest.name.split(' ')[0],
		fastestRps: fastestTest.result.rps,
		ranking,
		retryAnalysis,
		speedImprovement,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeRetryImpact(tests) {
	const retryTests = tests.filter(t => t.name.includes('Retries'));
	
	if (retryTests.length < 2) {
		return 'Insufficient retry test data';
	}

	const noRetries = retryTests.find(t => t.name.includes('No Retries'));
	const standardRetries = retryTests.find(t => t.name.includes('Standard Retries'));
	
	if (!noRetries || !standardRetries) {
		return 'Missing baseline retry tests';
	}

	const rpsImpact = ((standardRetries.result.rps - noRetries.result.rps) / noRetries.result.rps * 100).toFixed(1);
	const retryCount = standardRetries.result.retries || 0;
	
	return `${Math.abs(rpsImpact)}% ${rpsImpact > 0 ? 'improvement' : 'decrease'} with retries (${retryCount} retries made)`;
}

function calculateSpeedImprovement(tests) {
	if (tests.length < 2) return 'Insufficient data for comparison';
	
	const sorted = tests.sort((a, b) => b.result.rps - a.result.rps);
	const fastest = sorted[0];
	const slowest = sorted[sorted.length - 1];
	
	const improvement = ((fastest.result.rps - slowest.result.rps) / slowest.result.rps * 100).toFixed(1);
	
	return `${improvement}% faster than slowest transport`;
}

function generateTransportRecommendation(tests) {
	const transportTests = tests.filter(t => !t.name.includes('Retries'));
	
	if (transportTests.length === 0) return 'No transport data available';
	
	// Find best overall (considering efficiency, not just speed)
	const bestEfficiency = transportTests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);
	
	const transport = bestEfficiency.config.transport || bestEfficiency.name.split(' ')[0];
	const rps = Math.round(bestEfficiency.result.rps).toLocaleString();
	
	// Check if there are significant differences
	const rpsValues = transportTests.map(t => t.result.rps);
	const maxRps = Math.max(...rpsValues);
	const minRps = Math.min(...rpsValues);
	const difference = ((maxRps - minRps) / minRps * 100);
	
	let recommendation = `Use ${transport} transport for best performance (${rps} RPS)`;
	
	if (difference < 10) {
		recommendation += '. Note: Performance differences are minimal (<10%), choose based on reliability needs';
	} else {
		recommendation += `. Significant ${difference.toFixed(1)}% performance difference detected`;
	}
	
	return recommendation;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	transportComparison(config).catch(console.error);
}