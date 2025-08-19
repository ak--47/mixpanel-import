//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * TRANSFORM IMPACT BENCHMARK
 * 
 * Tests the performance impact of various data transformation and validation options.
 * This helps identify which transforms are necessary vs which add unnecessary overhead.
 * 
 * Key transforms tested:
 * - fixData: automatic data type fixing and property formatting
 * - strict: strict validation and error handling
 * - removeNulls: null property removal
 * - transformFunc: custom transform functions
 * - compress: payload compression impact
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

export default async function transformImpact(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('🔧 TRANSFORM IMPACT BENCHMARK');
	console.log('Testing performance impact of data transforms and validation...');
	console.log('');

	// Use optimal worker count from previous testing
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
		compressionLevel: 6
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	// Test different transform configurations
	const transformTests = [
		{
			name: 'Baseline (All Transforms Off)',
			config: {
				fixData: false,
				strict: false,
				removeNulls: false,
				transformFunc: null
			}
		},
		{
			name: 'Fix Data Only',
			config: {
				fixData: true,
				strict: false,
				removeNulls: false,
				transformFunc: null
			}
		},
		{
			name: 'Strict Validation Only',
			config: {
				fixData: false,
				strict: true,
				removeNulls: false,
				transformFunc: null
			}
		},
		{
			name: 'Remove Nulls Only',
			config: {
				fixData: false,
				strict: false,
				removeNulls: true,
				transformFunc: null
			}
		},
		{
			name: 'All Standard Transforms',
			config: {
				fixData: true,
				strict: true,
				removeNulls: true,
				transformFunc: null
			}
		},
		{
			name: 'Custom Transform (Simple)',
			config: {
				fixData: true,
				strict: false,
				removeNulls: false,
				transformFunc: (record) => {
					// Simple transform: add a timestamp
					record.properties = record.properties || {};
					record.properties._transform_time = Date.now();
					return record;
				}
			}
		},
		{
			name: 'Custom Transform (Heavy)',
			config: {
				fixData: true,
				strict: false,
				removeNulls: false,
				transformFunc: (record) => {
					// Heavy transform: multiple operations
					record.properties = record.properties || {};
					
					// String operations
					Object.keys(record.properties).forEach(key => {
						if (typeof record.properties[key] === 'string') {
							record.properties[key] = record.properties[key].toLowerCase().trim();
						}
					});
					
					// Add computed fields
					record.properties._record_size = JSON.stringify(record).length;
					record.properties._processed_at = new Date().toISOString();
					record.properties._property_count = Object.keys(record.properties).length;
					
					return record;
				}
			}
		},
		{
			name: 'Performance Optimized',
			config: {
				fixData: true,
				strict: false,
				removeNulls: false,
				transformFunc: null
			}
		},
		{
			name: 'Quality Optimized',
			config: {
				fixData: true,
				strict: true,
				removeNulls: true,
				transformFunc: (record) => {
					// Light validation and cleanup
					if (record.properties) {
						// Remove empty string properties
						Object.keys(record.properties).forEach(key => {
							if (record.properties[key] === '') {
								delete record.properties[key];
							}
						});
					}
					return record;
				}
			}
		}
	];

	const results = {
		description: 'Transform and validation performance impact benchmark',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	for (let i = 0; i < transformTests.length; i++) {
		const test = transformTests[i];
		console.log(`  [${i + 1}/${transformTests.length}] Testing: ${test.name}...`);
		
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
				overhead: calculateTransformOverhead(result)
			};

			results.tests.push(testResult);

			// Track best result (highest EPS)
			if (!results.bestResult || result.eps > results.bestResult.eps) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config,
					name: testResult.name,
					overhead: testResult.overhead
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const overhead = testResult.overhead.toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB, Overhead: ${overhead}%`);
			
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
	results.analysis = analyzeTransformImpact(results.tests);
	
	console.log('');
	console.log('📊 Transform Impact Analysis:');
	console.log(`   🏆 Fastest Configuration: ${results.analysis.fastestConfig} (${Math.round(results.analysis.fastestEps).toLocaleString()} EPS)`);
	console.log(`   📉 Transform Overhead: ${results.analysis.overheadAnalysis}`);
	console.log(`   ⚙️  FixData Impact: ${results.analysis.fixDataImpact}`);
	console.log(`   🔍 Validation Impact: ${results.analysis.validationImpact}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

function calculateTransformOverhead(result) {
	// Calculate overhead based on processing time vs throughput
	// Higher values indicate more processing overhead
	if (!result.eps || !result.duration) return 0;
	
	// Baseline expectation: ~50k EPS for minimal transforms
	const baselineEps = 50000;
	const actualEps = result.eps;
	
	if (actualEps >= baselineEps) return 0; // No overhead detected
	
	return ((baselineEps - actualEps) / baselineEps) * 100;
}

function analyzeTransformImpact(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find fastest configuration
	const fastestTest = successfulTests.reduce((fastest, current) => 
		current.result.eps > fastest.result.eps ? current : fastest
	);

	// Analyze different transform impacts
	const baselineTest = successfulTests.find(t => t.name === 'Baseline (All Transforms Off)');
	const fixDataTest = successfulTests.find(t => t.name === 'Fix Data Only');
	const strictTest = successfulTests.find(t => t.name === 'Strict Validation Only');
	const allTransformsTest = successfulTests.find(t => t.name === 'All Standard Transforms');

	// Calculate transform overhead
	const overheadAnalysis = analyzeOverhead(successfulTests);
	
	// FixData impact
	const fixDataImpact = baselineTest && fixDataTest 
		? calculateImpact(baselineTest.result.eps, fixDataTest.result.eps)
		: 'Insufficient data';

	// Validation impact  
	const validationImpact = baselineTest && strictTest
		? calculateImpact(baselineTest.result.eps, strictTest.result.eps)
		: 'Insufficient data';

	// Generate recommendation
	const recommendation = generateTransformRecommendation(successfulTests);

	return {
		fastestConfig: fastestTest.name,
		fastestEps: fastestTest.result.eps,
		overheadAnalysis,
		fixDataImpact,
		validationImpact,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeOverhead(tests) {
	const overheads = tests
		.filter(t => typeof t.overhead === 'number')
		.map(t => ({ name: t.name, overhead: t.overhead }))
		.sort((a, b) => a.overhead - b.overhead);

	if (overheads.length === 0) return 'No overhead data available';

	const lowest = overheads[0];
	const highest = overheads[overheads.length - 1];

	return `Overhead range: ${lowest.overhead.toFixed(1)}% (${lowest.name}) to ${highest.overhead.toFixed(1)}% (${highest.name})`;
}

function calculateImpact(baselineEps, testEps) {
	if (!baselineEps || !testEps) return 'N/A';
	
	const impact = ((testEps - baselineEps) / baselineEps * 100);
	const symbol = impact >= 0 ? '+' : '';
	
	return `${symbol}${impact.toFixed(1)}% EPS change`;
}

function generateTransformRecommendation(tests) {
	// Find the best balance of performance and data quality
	const performanceOptimized = tests.find(t => t.name === 'Performance Optimized');
	const qualityOptimized = tests.find(t => t.name === 'Quality Optimized');
	const baseline = tests.find(t => t.name === 'Baseline (All Transforms Off)');
	const fixDataOnly = tests.find(t => t.name === 'Fix Data Only');

	// If fixData has minimal impact, recommend it
	if (baseline && fixDataOnly) {
		const fixDataImpact = (baseline.result.eps - fixDataOnly.result.eps) / baseline.result.eps;
		if (fixDataImpact < 0.1) { // Less than 10% impact
			return 'Use fixData: true with strict: false for best balance of performance and data quality';
		}
	}

	// Find the fastest configuration that still has some data processing
	const nonBaseline = tests.filter(t => t.name !== 'Baseline (All Transforms Off)');
	if (nonBaseline.length > 0) {
		const fastestWithTransforms = nonBaseline.reduce((fastest, current) => 
			current.result.eps > fastest.result.eps ? current : fastest
		);
		
		return `Use "${fastestWithTransforms.name}" configuration for optimal performance with data processing`;
	}

	return 'Baseline configuration (no transforms) for maximum performance';
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	transformImpact(config).catch(console.error);
}