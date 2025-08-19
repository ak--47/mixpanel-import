//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * WORKER OPTIMIZATION BENCHMARK
 * 
 * Tests different worker/concurrency levels to find the optimal configuration.
 * Worker count is one of the most impactful performance parameters.
 * 
 * Tests: 1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100 workers
 * 
 * Key findings from previous tests:
 * - Optimal is typically between 10-25 workers
 * - Diminishing returns after 50 workers  
 * - Too many workers can actually hurt performance due to overhead
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

export default async function workerOptimization(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🔧 WORKER OPTIMIZATION BENCHMARK');
	console.log('Testing optimal worker/concurrency levels...');
	console.log('');

	// Worker counts to test - from conservative to aggressive
	const workerCounts = [1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100];
	
	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		streamFormat: 'jsonl',
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		fixData: true,
		recordsPerBatch: 2000, // Standard batch size
		compress: true
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	const results = {
		description: 'Worker/concurrency optimization benchmark',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	let baselineEps = null;

	for (const workers of workerCounts) {
		console.log(`  Testing ${workers} workers...`);
		
		const options = {
			...baseOptions,
			workers: workers
		};

		try {
			const startTime = Date.now();
			const result = await mpStream({}, dataFile, options);
			const endTime = Date.now();
			
			const testResult = {
				config: { workers },
				result: {
					eps: result.eps,
					rps: result.rps,
					mbps: result.mbps,
					duration: result.duration,
					success: result.success,
					failed: result.failed,
					memory: result.memory,
					avgBatchLength: result.avgBatchLength,
					retries: result.retries
				},
				actualDuration: endTime - startTime
			};

			// Calculate improvement vs baseline (1 worker)
			if (baselineEps === null && workers === 1) {
				baselineEps = result.eps;
				testResult.improvement = '0% (baseline)';
			} else if (baselineEps !== null) {
				const improvement = ((result.eps - baselineEps) / baselineEps * 100);
				testResult.improvement = `${improvement > 0 ? '+' : ''}${improvement.toFixed(1)}%`;
			}

			results.tests.push(testResult);

			// Track best result (highest EPS)
			if (!results.bestResult || result.eps > results.bestResult.eps) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB${testResult.improvement ? `, Improvement: ${testResult.improvement}` : ''}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				config: { workers },
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeWorkerResults(results.tests);
	
	console.log('');
	console.log('📊 Worker Optimization Analysis:');
	console.log(`   🏆 Optimal Workers: ${results.analysis.optimalWorkers} (${Math.round(results.analysis.maxEps).toLocaleString()} EPS)`);
	console.log(`   📈 Peak Improvement: ${results.analysis.peakImprovement}`);
	console.log(`   ⚠️  Diminishing Returns At: ${results.analysis.diminishingReturnsAt} workers`);
	console.log(`   💾 Memory Impact: ${results.analysis.memoryImpact}`);
	console.log('');

	return results;
}

function analyzeWorkerResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find optimal worker count (highest EPS)
	const bestTest = successfulTests.reduce((best, current) => 
		current.result.eps > best.result.eps ? current : best
	);

	// Find where diminishing returns start (where adding workers yields <10% improvement)
	let diminishingReturnsAt = null;
	for (let i = 1; i < successfulTests.length; i++) {
		const current = successfulTests[i];
		const previous = successfulTests[i - 1];
		
		if (current.result.eps && previous.result.eps) {
			const improvement = (current.result.eps - previous.result.eps) / previous.result.eps;
			if (improvement < 0.1) { // Less than 10% improvement
				diminishingReturnsAt = current.config.workers;
				break;
			}
		}
	}

	// Calculate peak improvement over baseline
	const baseline = successfulTests.find(t => t.config.workers === 1);
	const peakImprovement = baseline && bestTest.result.eps && baseline.result.eps
		? `${(((bestTest.result.eps - baseline.result.eps) / baseline.result.eps) * 100).toFixed(1)}%`
		: 'N/A';

	// Analyze memory impact
	const memoryImpact = analyzeMemoryImpact(successfulTests);

	return {
		optimalWorkers: bestTest.config.workers,
		maxEps: bestTest.result.eps,
		diminishingReturnsAt: diminishingReturnsAt || 'Not detected',
		peakImprovement,
		memoryImpact,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeMemoryImpact(tests) {
	const memoryUsage = tests
		.filter(t => t.result?.memory?.heapUsed)
		.map(t => ({
			workers: t.config.workers,
			memoryMB: Math.round(t.result.memory.heapUsed / 1024 / 1024)
		}));

	if (memoryUsage.length < 2) return 'Insufficient data';

	const lowest = memoryUsage.reduce((min, current) => 
		current.memoryMB < min.memoryMB ? current : min
	);
	
	const highest = memoryUsage.reduce((max, current) => 
		current.memoryMB > max.memoryMB ? current : max
	);

	const increase = highest.memoryMB - lowest.memoryMB;
	const percentIncrease = ((increase / lowest.memoryMB) * 100).toFixed(1);

	return `${increase}MB increase (${percentIncrease}%) from ${lowest.workers} to ${highest.workers} workers`;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	workerOptimization(config).catch(console.error);
}