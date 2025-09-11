//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * BASIC BENCHMARK
 * 
 * Simple baseline performance test to verify the import system is working
 * and establish basic performance metrics. This serves as a foundation
 * for more complex benchmarking scenarios.
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

export default async function main(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🏁 BASIC BENCHMARK');
	console.log('Testing fundamental import performance...');
	console.log('');

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 10, // Conservative baseline
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
		description: 'Basic import performance baseline',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	console.log(`  [1/1] Testing basic import: ${dataFile}...`);
	
	try {
		const startTime = Date.now();
		const result = await mpStream({}, dataFile, baseOptions);
		const endTime = Date.now();
		
		const testResult = {
			name: 'Basic Import',
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
			actualDuration: endTime - startTime
		};

		results.tests.push(testResult);
		results.bestResult = testResult.result;

		// Console output
		const epsFormatted = Math.round(result.eps).toLocaleString();
		const rpsFormatted = Math.round(result.rps).toLocaleString();
		const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
		const successRate = ((result.success / result.total) * 100).toFixed(1);
		
		console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB`);
		console.log(`    Success: ${u.comma(result.success)}/${u.comma(result.total)} (${successRate}%)`);
		console.log(`    Duration: ${result.durationHuman}, Workers: ${result.workers}`);
		console.log(`    Mode: ${result.wasStream ? 'Streaming' : 'Memory'}, Batch Size: ${Math.round(result.avgBatchLength)}`);
		
	} catch (error) {
		console.log(`    ❌ Failed: ${error.message}`);
		results.tests.push({
			name: 'Basic Import',
			config: baseOptions,
			dataFile: dataFile,
			error: error.message
		});
	}

	// Generate analysis
	results.analysis = analyzeBasicResults(results.tests);
	
	console.log('');
	console.log('📊 Basic Benchmark Analysis:');
	if (results.analysis.error) {
		console.log(`   ❌ ${results.analysis.error}`);
	} else {
		console.log(`   📈 Performance: ${results.analysis.performanceLevel}`);
		console.log(`   🎯 Efficiency: ${results.analysis.efficiency}`);
		console.log(`   💾 Memory Usage: ${results.analysis.memoryUsage}`);
		console.log(`   🔄 Processing Mode: ${results.analysis.processingMode}`);
		console.log(`   ✅ Recommendation: ${results.analysis.recommendation}`);
	}
	console.log('');

	return results;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	main(config).catch(console.error);
}

function analyzeBasicResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'Test failed - no results to analyze' };
	}

	const test = successfulTests[0];
	const result = test.result;

	// Performance level analysis
	let performanceLevel = 'Poor';
	if (result.eps > 50000) performanceLevel = 'Excellent';
	else if (result.eps > 25000) performanceLevel = 'Good';
	else if (result.eps > 10000) performanceLevel = 'Fair';

	// Efficiency analysis
	const eventsPerRequest = result.total / result.requests;
	const efficiency = eventsPerRequest > 1500 ? 'High' : eventsPerRequest > 1000 ? 'Medium' : 'Low';

	// Memory usage analysis
	const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
	const memoryUsage = memoryMB < 100 ? 'Low' : memoryMB < 300 ? 'Medium' : 'High';

	// Processing mode analysis
	const processingMode = result.wasStream ? 'Streaming (memory efficient)' : 'Memory (performance optimized)';

	// Generate recommendation
	let recommendation = `Current setup provides ${performanceLevel.toLowerCase()} performance`;
	if (result.eps < 10000) {
		recommendation += '. Consider increasing workers or optimizing data format';
	}
	if (memoryMB > 300) {
		recommendation += '. High memory usage - consider streaming for large datasets';
	}

	return {
		performanceLevel: `${performanceLevel} (${Math.round(result.eps).toLocaleString()} EPS)`,
		efficiency: `${efficiency} (${Math.round(eventsPerRequest)} events/request)`,
		memoryUsage: `${memoryUsage} (${memoryMB}MB)`,
		processingMode,
		recommendation,
		totalRecords: result.total,
		successRate: ((result.success / result.total) * 100).toFixed(1) + '%',
		throughput: result.mbps.toFixed(1) + ' MB/s'
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	main(config).catch(console.error);
}