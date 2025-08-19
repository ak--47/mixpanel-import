//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * PARAMETER MATRIX BENCHMARK
 * 
 * Tests combinations of key performance parameters to find optimal configurations.
 * This is where we discover the ideal defaults for different scenarios.
 * 
 * Key parameters tested:
 * - recordsPerBatch: batch size optimization
 * - compress: compression impact
 * - compressionLevel: compression level tuning
 * - fixData: data fixing overhead
 * - strict: validation overhead
 * - removeNulls: null removal processing
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

export default async function parameterMatrix(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🧪 PARAMETER MATRIX BENCHMARK');
	console.log('Testing combinations of key performance parameters...');
	console.log('');

	// Based on worker optimization, use a good worker count
	const optimalWorkers = 20;

	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
		streamFormat: 'jsonl',
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: optimalWorkers
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	// Define parameter test matrix
	const testMatrix = [
		// Baseline - current defaults
		{
			name: 'Current Defaults',
			config: {
				recordsPerBatch: 2000,
				compress: true,
				compressionLevel: 6,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		
		// Batch size optimization tests
		{
			name: 'Smaller Batches',
			config: {
				recordsPerBatch: 1000,
				compress: true,
				compressionLevel: 6,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		{
			name: 'Larger Batches',
			config: {
				recordsPerBatch: 3000,
				compress: true,
				compressionLevel: 6,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		
		// Compression optimization tests
		{
			name: 'No Compression',
			config: {
				recordsPerBatch: 2000,
				compress: false,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		{
			name: 'Fast Compression',
			config: {
				recordsPerBatch: 2000,
				compress: true,
				compressionLevel: 1,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		{
			name: 'Max Compression',
			config: {
				recordsPerBatch: 2000,
				compress: true,
				compressionLevel: 9,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		},
		
		// Data processing optimization tests
		{
			name: 'Minimal Processing',
			config: {
				recordsPerBatch: 2000,
				compress: true,
				compressionLevel: 6,
				fixData: false,
				strict: false,
				removeNulls: false
			}
		},
		{
			name: 'Maximum Processing',
			config: {
				recordsPerBatch: 2000,
				compress: true,
				compressionLevel: 6,
				fixData: true,
				strict: true,
				removeNulls: true
			}
		},
		
		// Hybrid optimizations
		{
			name: 'Speed Optimized',
			config: {
				recordsPerBatch: 3000,
				compress: true,
				compressionLevel: 1,
				fixData: false,
				strict: false,
				removeNulls: false
			}
		},
		{
			name: 'Quality Optimized',
			config: {
				recordsPerBatch: 1000,
				compress: true,
				compressionLevel: 9,
				fixData: true,
				strict: true,
				removeNulls: true
			}
		},
		
		// Balanced optimizations
		{
			name: 'Balanced Performance',
			config: {
				recordsPerBatch: 2500,
				compress: true,
				compressionLevel: 3,
				fixData: true,
				strict: true,
				removeNulls: false
			}
		}
	];

	const results = {
		description: 'Parameter matrix benchmark testing key performance configurations',
		baseOptions,
		testMatrix,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	for (let i = 0; i < testMatrix.length; i++) {
		const test = testMatrix[i];
		console.log(`  [${i + 1}/${testMatrix.length}] Testing: ${test.name}...`);
		
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
					percentQuota: result.percentQuota
				},
				actualDuration: endTime - startTime,
				efficiency: calculateEfficiency(result)
			};

			results.tests.push(testResult);

			// Track best result (highest efficiency score)
			if (!results.bestResult || testResult.efficiency > results.bestResult.efficiency) {
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
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Efficiency: ${efficiency}`);
			
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
	results.analysis = analyzeParameterMatrix(results.tests);
	
	console.log('');
	console.log('📊 Parameter Matrix Analysis:');
	console.log(`   🏆 Best Configuration: ${results.analysis.bestConfig} (Efficiency: ${results.analysis.bestEfficiency})`);
	console.log(`   ⚡ Fastest EPS: ${results.analysis.fastestEPS.name} (${Math.round(results.analysis.fastestEPS.eps).toLocaleString()} EPS)`);
	console.log(`   📦 Best Compression: ${results.analysis.compressionAnalysis}`);
	console.log(`   🔧 Processing Impact: ${results.analysis.processingImpact}`);
	console.log(`   📏 Batch Size Impact: ${results.analysis.batchSizeAnalysis}`);
	console.log('');

	return results;
}

function calculateEfficiency(result) {
	// Efficiency score considers EPS, memory usage, and error rate
	// Higher EPS is better, lower memory is better, fewer errors is better
	
	const epsScore = result.eps || 0;
	const memoryPenalty = (result.memory?.heapUsed || 0) / (1024 * 1024 * 100); // Penalty for each 100MB
	const errorPenalty = (result.failed || 0) * 10; // Heavy penalty for errors
	
	return Math.max(0, epsScore - memoryPenalty - errorPenalty);
}

function analyzeParameterMatrix(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Best overall configuration (highest efficiency)
	const bestTest = successfulTests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);

	// Fastest EPS
	const fastestEPS = successfulTests.reduce((fastest, current) => 
		current.result.eps > fastest.result.eps ? current : fastest
	);

	// Analyze compression impact
	const compressionAnalysis = analyzeCompressionImpact(successfulTests);
	
	// Analyze processing overhead
	const processingImpact = analyzeProcessingImpact(successfulTests);
	
	// Analyze batch size impact
	const batchSizeAnalysis = analyzeBatchSizeImpact(successfulTests);

	return {
		bestConfig: bestTest.name,
		bestEfficiency: bestTest.efficiency.toFixed(1),
		fastestEPS: {
			name: fastestEPS.name,
			eps: fastestEPS.result.eps
		},
		compressionAnalysis,
		processingImpact,
		batchSizeAnalysis,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeCompressionImpact(tests) {
	const noCompression = tests.find(t => t.name === 'No Compression');
	const defaultCompression = tests.find(t => t.name === 'Current Defaults');
	const fastCompression = tests.find(t => t.name === 'Fast Compression');
	const maxCompression = tests.find(t => t.name === 'Max Compression');

	if (!noCompression || !defaultCompression) {
		return 'Insufficient compression test data';
	}

	const speedDiff = ((defaultCompression.result.eps - noCompression.result.eps) / noCompression.result.eps * 100).toFixed(1);
	const sizeDiff = noCompression.result.bytes && defaultCompression.result.bytes 
		? ((noCompression.result.bytes - defaultCompression.result.bytes) / noCompression.result.bytes * 100).toFixed(1)
		: 'N/A';

	return `Compression ${speedDiff}% speed impact, ~${sizeDiff}% size reduction`;
}

function analyzeProcessingImpact(tests) {
	const minimal = tests.find(t => t.name === 'Minimal Processing');
	const maximum = tests.find(t => t.name === 'Maximum Processing');
	const defaults = tests.find(t => t.name === 'Current Defaults');

	if (!minimal || !maximum || !defaults) {
		return 'Insufficient processing test data';
	}

	const minToMaxDiff = ((minimal.result.eps - maximum.result.eps) / maximum.result.eps * 100).toFixed(1);
	const defaultsVsMinimal = ((defaults.result.eps - minimal.result.eps) / minimal.result.eps * 100).toFixed(1);

	return `${minToMaxDiff}% EPS difference between minimal and maximum processing`;
}

function analyzeBatchSizeImpact(tests) {
	const small = tests.find(t => t.name === 'Smaller Batches');
	const large = tests.find(t => t.name === 'Larger Batches');
	const defaults = tests.find(t => t.name === 'Current Defaults');

	if (!small || !large || !defaults) {
		return 'Insufficient batch size test data';
	}

	// Find the best batch size
	const batchTests = [small, defaults, large].sort((a, b) => b.result.eps - a.result.eps);
	const best = batchTests[0];
	const worst = batchTests[2];
	
	const improvement = ((best.result.eps - worst.result.eps) / worst.result.eps * 100).toFixed(1);
	const optimalSize = best.config.recordsPerBatch;

	return `Optimal batch size: ${optimalSize} records (${improvement}% better than worst)`;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	parameterMatrix(config).catch(console.error);
}