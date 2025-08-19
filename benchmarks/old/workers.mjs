//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * WORKERS BENCHMARK
 * 
 * Tests different worker counts to determine the optimal concurrency level
 * for maximum throughput without diminishing returns.
 * 
 * Previous findings: Optimal number of workers is between 10-25 with diminishing returns at 50+
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

export default async function workers(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('👥 WORKERS BENCHMARK');
	console.log('Testing different worker counts to find optimal concurrency...');
	console.log('');

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
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
		description: 'Worker count optimization for maximum throughput',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Test different worker counts
	const workerCounts = [1, 5, 10, 15, 20, 25, 30, 50, 75, 100];
	
	// Run all tests
	for (let i = 0; i < workerCounts.length; i++) {
		const workerCount = workerCounts[i];
		console.log(`  [${i + 1}/${workerCounts.length}] Testing: ${workerCount} workers...`);
		
		try {
			const startTime = Date.now();
			const result = await mpStream({}, dataFile, {
				...baseOptions,
				workers: workerCount
			});
			const endTime = Date.now();
			
			const testResult = {
				name: `${workerCount} Workers`,
				workerCount: workerCount,
				config: { ...baseOptions, workers: workerCount },
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
					workerCount: testResult.workerCount,
					config: testResult.config
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const successRate = ((result.success / result.total) * 100).toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Duration: ${result.durationHuman}`);
			console.log(`    Memory: ${memoryMB}MB, Success: ${successRate}%, Retries: ${u.comma(result.retries)}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: `${workerCount} Workers`,
				workerCount: workerCount,
				config: { ...baseOptions, workers: workerCount },
				dataFile: dataFile,
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeWorkersResults(results.tests);
	
	console.log('');
	console.log('📊 Workers Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   🏆 Optimal Workers: ${results.analysis.optimalWorkers}`);
		console.log(`   📈 Performance Curve: ${results.analysis.performanceCurve}`);
		console.log(`   📊 Diminishing Returns: ${results.analysis.diminishingReturns}`);
		console.log(`   💾 Memory Scaling: ${results.analysis.memoryScaling}`);
		console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

function analyzeWorkersResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length < 3) {
		return { error: 'Need at least 3 successful tests to analyze worker scaling' };
	}

	// Sort by worker count for analysis
	const sortedTests = successfulTests.sort((a, b) => a.workerCount - b.workerCount);
	
	// Find optimal worker count (highest EPS)
	const optimalTest = successfulTests.reduce((best, current) =>
		current.result.eps > best.result.eps ? current : best
	);
	
	// Analyze performance curve
	const performancePoints = sortedTests.map(t => ({
		workers: t.workerCount,
		eps: t.result.eps,
		memory: Math.round((t.result.memory?.heapUsed || 0) / 1024 / 1024)
	}));

	// Calculate efficiency (EPS per worker)
	const efficiencyPoints = performancePoints.map(p => ({
		workers: p.workers,
		efficiency: p.eps / p.workers,
		eps: p.eps
	}));

	// Find most efficient worker count
	const mostEfficientTest = efficiencyPoints.reduce((best, current) =>
		current.efficiency > best.efficiency ? current : best
	);

	// Detect diminishing returns point
	let diminishingReturnsPoint = null;
	for (let i = 1; i < performancePoints.length; i++) {
		const current = performancePoints[i];
		const previous = performancePoints[i - 1];
		const improvement = (current.eps - previous.eps) / previous.eps;
		
		// If improvement is less than 5% for a significant worker increase
		if (improvement < 0.05 && current.workers >= previous.workers * 1.5) {
			diminishingReturnsPoint = previous.workers;
			break;
		}
	}

	// Analyze memory scaling
	const memoryGrowth = performancePoints.map((p, i) => {
		if (i === 0) return { workers: p.workers, growth: 0 };
		const previous = performancePoints[i - 1];
		const growth = ((p.memory - previous.memory) / previous.memory) * 100;
		return { workers: p.workers, growth };
	});

	const avgMemoryGrowthPerWorker = memoryGrowth
		.filter(m => m.growth > 0)
		.reduce((sum, m) => sum + m.growth, 0) / Math.max(1, memoryGrowth.filter(m => m.growth > 0).length);

	// Generate performance curve description
	const lowTest = sortedTests[0];
	const highTest = sortedTests[sortedTests.length - 1];
	const performanceCurve = `${lowTest.workerCount} workers: ${Math.round(lowTest.result.eps).toLocaleString()} EPS → ${highTest.workerCount} workers: ${Math.round(highTest.result.eps).toLocaleString()} EPS (${((highTest.result.eps - lowTest.result.eps) / lowTest.result.eps * 100).toFixed(1)}% improvement)`;

	// Generate recommendation
	let recommendation = `Use ${optimalTest.workerCount} workers for maximum throughput`;
	
	if (diminishingReturnsPoint && diminishingReturnsPoint !== optimalTest.workerCount) {
		if (diminishingReturnsPoint < optimalTest.workerCount) {
			const diminishingTest = sortedTests.find(t => t.workerCount === diminishingReturnsPoint);
			if (diminishingTest) {
				const performanceDiff = ((optimalTest.result.eps - diminishingTest.result.eps) / diminishingTest.result.eps) * 100;
				if (performanceDiff < 15) {
					recommendation = `Use ${diminishingReturnsPoint} workers for best efficiency (only ${performanceDiff.toFixed(1)}% slower than optimal)`;
				}
			}
		}
	}

	// Add memory consideration
	const optimalMemory = Math.round((optimalTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	if (optimalMemory > 500) {
		recommendation += `. Consider reducing workers for large datasets due to high memory usage (${optimalMemory}MB)`;
	}

	return {
		optimalWorkers: `${optimalTest.workerCount} (${Math.round(optimalTest.result.eps).toLocaleString()} EPS)`,
		mostEfficientWorkers: `${mostEfficientTest.workers} (${Math.round(mostEfficientTest.efficiency).toLocaleString()} EPS/worker)`,
		performanceCurve,
		diminishingReturns: diminishingReturnsPoint ? 
			`Performance gains level off after ${diminishingReturnsPoint} workers` :
			'No clear diminishing returns point detected',
		memoryScaling: `Memory grows ~${avgMemoryGrowthPerWorker.toFixed(1)}% per worker increase`,
		recommendation,
		detailedResults: sortedTests.map(t => ({
			workers: t.workerCount,
			eps: Math.round(t.result.eps).toLocaleString(),
			efficiency: Math.round(t.result.eps / t.workerCount).toLocaleString(),
			duration: t.result.durationHuman,
			memory: Math.round((t.result.memory?.heapUsed || 0) / 1024 / 1024) + 'MB',
			retries: t.result.retries
		})),
		scalingAnalysis: {
			optimalWorkerCount: optimalTest.workerCount,
			optimalEPS: Math.round(optimalTest.result.eps),
			mostEfficientWorkerCount: mostEfficientTest.workers,
			mostEfficientEPS: Math.round(mostEfficientTest.eps),
			diminishingReturnsPoint,
			memoryAt1Worker: Math.round((sortedTests[0]?.result.memory?.heapUsed || 0) / 1024 / 1024),
			memoryAtOptimal: optimalMemory
		}
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	workers(config).catch(console.error);
}