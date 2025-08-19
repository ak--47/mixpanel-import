//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * PROFILER BENCHMARK
 * 
 * Comprehensive performance profiling that captures detailed metrics
 * including memory usage, processing time, and system resource utilization.
 * Provides insights into bottlenecks and optimization opportunities.
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

export default async function profiler(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🔍 PROFILER BENCHMARK');
	console.log('Capturing detailed performance metrics and system profiling...');
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
		streamFormat: 'jsonl',
		forceStream: false
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	const results = {
		description: 'Comprehensive performance profiling with detailed metrics',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Capture initial memory state
	const initialMemory = process.memoryUsage();
	
	console.log(`  [1/1] Profiling import: ${dataFile}...`);
	console.log(`    📊 Initial Memory: ${Math.round(initialMemory.heapUsed / 1024 / 1024)}MB`);
	
	try {
		// Start profiling
		const startTime = Date.now();
		const startCPU = process.cpuUsage();
		
		const result = await mpStream({}, dataFile, baseOptions);
		
		// End profiling
		const endTime = Date.now();
		const endCPU = process.cpuUsage(startCPU);
		const endMemory = process.memoryUsage();
		
		// Calculate CPU usage
		const cpuUsage = {
			user: endCPU.user / 1000, // Convert to milliseconds
			system: endCPU.system / 1000
		};
		
		// Calculate memory delta
		const memoryDelta = {
			heapUsed: endMemory.heapUsed - initialMemory.heapUsed,
			heapTotal: endMemory.heapTotal - initialMemory.heapTotal,
			external: endMemory.external - initialMemory.external,
			rss: endMemory.rss - initialMemory.rss
		};
		
		const testResult = {
			name: 'Profiler Benchmark',
			config: baseOptions,
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
			profiling: {
				actualDuration: endTime - startTime,
				cpuUsage,
				initialMemory,
				endMemory,
				memoryDelta,
				memoryEfficiency: Math.round((result.total * 100) / (memoryDelta.heapUsed / 1024)),
				cpuEfficiency: Math.round(result.total / ((cpuUsage.user + cpuUsage.system) / 1000))
			}
		};

		results.tests.push(testResult);
		results.bestResult = testResult.result;

		// Console output
		const epsFormatted = Math.round(result.eps).toLocaleString();
		const rpsFormatted = Math.round(result.rps).toLocaleString();
		const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
		const successRate = ((result.success / result.total) * 100).toFixed(1);
		
		console.log(`    📈 Performance: EPS: ${epsFormatted}, RPS: ${rpsFormatted}`);
		console.log(`    💾 Memory: Current: ${memoryMB}MB, Delta: +${Math.round(memoryDelta.heapUsed / 1024 / 1024)}MB`);
		console.log(`    ⚡ CPU: User: ${cpuUsage.user.toFixed(0)}ms, System: ${cpuUsage.system.toFixed(0)}ms`);
		console.log(`    ✅ Success: ${u.comma(result.success)}/${u.comma(result.total)} (${successRate}%)`);
		console.log(`    ⏱️  Duration: ${result.durationHuman} (actual: ${testResult.profiling.actualDuration}ms)`);
		
	} catch (error) {
		console.log(`    ❌ Failed: ${error.message}`);
		results.tests.push({
			name: 'Profiler Benchmark',
			config: baseOptions,
			dataFile: dataFile,
			error: error.message
		});
	}

	// Generate analysis
	results.analysis = analyzeProfilerResults(results.tests);
	
	console.log('');
	console.log('📊 Profiler Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   🏆 Overall Score: ${results.analysis.overallScore}/100`);
		console.log(`   ⚡ CPU Efficiency: ${results.analysis.cpuEfficiency}`);
		console.log(`   💾 Memory Efficiency: ${results.analysis.memoryEfficiency}`);
		console.log(`   📊 Throughput Score: ${results.analysis.throughputScore}`);
		console.log(`   🎯 Bottleneck: ${results.analysis.primaryBottleneck}`);
		console.log(`   💡 Optimization: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

function analyzeProfilerResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'Profiling failed - no results to analyze' };
	}

	const test = successfulTests[0];
	const result = test.result;
	const profiling = test.profiling;

	// CPU Efficiency Analysis (events per CPU millisecond)
	const totalCPUTime = profiling.cpuUsage.user + profiling.cpuUsage.system;
	const cpuEfficiencyScore = Math.min(100, Math.round((result.total / totalCPUTime) * 10));
	const cpuEfficiency = cpuEfficiencyScore > 80 ? 'Excellent' : 
						  cpuEfficiencyScore > 60 ? 'Good' : 
						  cpuEfficiencyScore > 40 ? 'Fair' : 'Poor';

	// Memory Efficiency Analysis (events per MB of heap growth)
	const heapGrowthMB = Math.max(1, profiling.memoryDelta.heapUsed / 1024 / 1024);
	const memoryEfficiencyScore = Math.min(100, Math.round((result.total / heapGrowthMB) / 1000));
	const memoryEfficiency = memoryEfficiencyScore > 80 ? 'Excellent' : 
							 memoryEfficiencyScore > 60 ? 'Good' : 
							 memoryEfficiencyScore > 40 ? 'Fair' : 'Poor';

	// Throughput Score (based on EPS relative to expectations)
	const throughputScore = Math.min(100, Math.round(result.eps / 500)); // 50k EPS = 100 points
	const throughputLevel = throughputScore > 80 ? 'Excellent' : 
							throughputScore > 60 ? 'Good' : 
							throughputScore > 40 ? 'Fair' : 'Poor';

	// Overall Score (weighted average)
	const overallScore = Math.round((cpuEfficiencyScore * 0.3) + (memoryEfficiencyScore * 0.3) + (throughputScore * 0.4));

	// Identify Primary Bottleneck
	let primaryBottleneck = 'CPU processing';
	if (memoryEfficiencyScore < cpuEfficiencyScore && memoryEfficiencyScore < throughputScore) {
		primaryBottleneck = 'Memory allocation';
	} else if (throughputScore < cpuEfficiencyScore && throughputScore < memoryEfficiencyScore) {
		primaryBottleneck = 'Network throughput';
	}

	// Generate Optimization Recommendation
	let recommendation = '';
	if (cpuEfficiencyScore < 50) {
		recommendation = 'Optimize data transformation and processing logic';
	} else if (memoryEfficiencyScore < 50) {
		recommendation = 'Consider streaming processing to reduce memory footprint';
	} else if (throughputScore < 50) {
		recommendation = 'Increase worker count or optimize batching strategy';
	} else {
		recommendation = 'Performance is well-optimized. Consider testing with larger datasets';
	}

	return {
		overallScore,
		cpuEfficiency: `${cpuEfficiency} (${cpuEfficiencyScore}/100)`,
		memoryEfficiency: `${memoryEfficiency} (${memoryEfficiencyScore}/100)`,
		throughputScore: `${throughputLevel} (${throughputScore}/100)`,
		primaryBottleneck,
		recommendation,
		detailed: {
			cpuTime: `${totalCPUTime.toFixed(0)}ms total (${profiling.cpuUsage.user.toFixed(0)}ms user + ${profiling.cpuUsage.system.toFixed(0)}ms system)`,
			memoryGrowth: `+${Math.round(heapGrowthMB)}MB heap growth`,
			eventsPerCPUMs: Math.round(result.total / totalCPUTime),
			eventsPerMB: Math.round(result.total / heapGrowthMB),
			actualVsReported: `${profiling.actualDuration}ms actual vs ${result.duration}ms reported`
		}
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	profiler(config).catch(console.error);
}