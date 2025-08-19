//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * FORMAT COMPARISON BENCHMARK
 * 
 * Tests different data formats to determine optimal ingestion methods.
 * Compares JSON, JSONL, CSV, and streaming vs memory processing.
 * 
 * Previous findings indicated:
 * "Object Streams > NDJSON > JSON (11, 15, 29 seconds respectively)"
 * 
 * This benchmark will verify and expand on those findings.
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

export default async function formatComparison(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('📄 FORMAT COMPARISON BENCHMARK');
	console.log('Testing different data formats and processing methods...');
	console.log('');

	// Use optimal worker count from previous testing
	const optimalWorkers = 20;

	const baseOptions = {
		logs: false,
		verbose: false,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: optimalWorkers,
		fixData: true,
		compress: true,
		recordsPerBatch: 2000
	};

	// Check credentials unless running dry run
	if (!dryRun) {
		const creds = credentials || checkCredentials();
		baseOptions.project = creds.project;
		baseOptions.secret = creds.secret;
		baseOptions.token = creds.token;
	}

	// Test different format configurations
	const formatTests = [
		{
			name: 'JSONL (Default)',
			config: {
				streamFormat: 'jsonl',
				forceStream: false
			},
			dataFile: dataFile
		},
		{
			name: 'JSONL (Force Stream)',
			config: {
				streamFormat: 'jsonl',
				forceStream: true
			},
			dataFile: dataFile
		},
		{
			name: 'JSON (Memory)',
			config: {
				streamFormat: 'json',
				forceStream: false
			},
			dataFile: dataFile.replace('.ndjson', '.json') // Assume we have a JSON version
		},
		{
			name: 'JSON (Force Stream)',
			config: {
				streamFormat: 'json',
				forceStream: true
			},
			dataFile: dataFile.replace('.ndjson', '.json')
		}
	];

	// If CSV test data exists, add CSV tests
	const csvFile = dataFile.replace('.ndjson', '.csv');
	if (await fileExists(csvFile)) {
		formatTests.push({
			name: 'CSV (Memory)',
			config: {
				streamFormat: 'csv',
				forceStream: false,
				aliases: { id: '$insert_id' } // Basic CSV mapping
			},
			dataFile: csvFile
		});
		
		formatTests.push({
			name: 'CSV (Force Stream)',
			config: {
				streamFormat: 'csv',
				forceStream: true,
				aliases: { id: '$insert_id' }
			},
			dataFile: csvFile
		});
	}

	const results = {
		description: 'Data format and processing method comparison',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	for (let i = 0; i < formatTests.length; i++) {
		const test = formatTests[i];
		console.log(`  [${i + 1}/${formatTests.length}] Testing: ${test.name}...`);
		
		const options = {
			...baseOptions,
			...test.config
		};

		// Skip test if data file doesn't exist
		if (!(await fileExists(test.dataFile))) {
			console.log(`    ⏭️  Skipped: Data file not found (${test.dataFile})`);
			continue;
		}

		try {
			const startTime = Date.now();
			const result = await mpStream({}, test.dataFile, options);
			const endTime = Date.now();
			
			const testResult = {
				name: test.name,
				config: test.config,
				dataFile: test.dataFile,
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
					wasStream: result.wasStream,
					avgBatchLength: result.avgBatchLength
				},
				actualDuration: endTime - startTime
			};

			results.tests.push(testResult);

			// Track best result (highest EPS)
			if (!results.bestResult || result.eps > results.bestResult.eps) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config,
					name: testResult.name
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const rpsFormatted = Math.round(result.rps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const streamMode = result.wasStream ? 'Streaming' : 'Memory';
			
			console.log(`    EPS: ${epsFormatted}, RPS: ${rpsFormatted}, Memory: ${memoryMB}MB, Mode: ${streamMode}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				config: test.config,
				dataFile: test.dataFile,
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeFormatResults(results.tests);
	
	console.log('');
	console.log('📊 Format Comparison Analysis:');
	console.log(`   🏆 Fastest Format: ${results.analysis.fastestFormat} (${Math.round(results.analysis.fastestEps).toLocaleString()} EPS)`);
	console.log(`   💾 Memory vs Stream: ${results.analysis.memoryVsStream}`);
	console.log(`   📈 Performance Ranking: ${results.analysis.ranking.join(' > ')}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

async function fileExists(filePath) {
	try {
		const { existsSync } = await import('fs');
		return existsSync(filePath);
	} catch {
		return false;
	}
}

function analyzeFormatResults(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find fastest format
	const fastestTest = successfulTests.reduce((fastest, current) => 
		current.result.eps > fastest.result.eps ? current : fastest
	);

	// Analyze memory vs streaming performance
	const memoryVsStream = analyzeMemoryVsStream(successfulTests);
	
	// Create performance ranking
	const ranking = successfulTests
		.sort((a, b) => b.result.eps - a.result.eps)
		.map(test => test.name.split(' ')[0]) // Get format name only
		.filter((format, index, array) => array.indexOf(format) === index); // Remove duplicates

	// Generate recommendation
	const recommendation = generateFormatRecommendation(successfulTests);

	return {
		fastestFormat: fastestTest.name,
		fastestEps: fastestTest.result.eps,
		memoryVsStream,
		ranking,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeMemoryVsStream(tests) {
	const memoryTests = tests.filter(t => t.result.wasStream === false);
	const streamTests = tests.filter(t => t.result.wasStream === true);

	if (memoryTests.length === 0 || streamTests.length === 0) {
		return 'Insufficient data for memory vs stream comparison';
	}

	// Average EPS for each mode
	const avgMemoryEps = memoryTests.reduce((sum, test) => sum + test.result.eps, 0) / memoryTests.length;
	const avgStreamEps = streamTests.reduce((sum, test) => sum + test.result.eps, 0) / streamTests.length;

	// Memory usage comparison
	const avgMemoryUsage = memoryTests.reduce((sum, test) => sum + (test.result.memory?.heapUsed || 0), 0) / memoryTests.length;
	const avgStreamUsage = streamTests.reduce((sum, test) => sum + (test.result.memory?.heapUsed || 0), 0) / streamTests.length;

	const speedDiff = ((avgMemoryEps - avgStreamEps) / avgStreamEps * 100).toFixed(1);
	const memoryDiff = ((avgMemoryUsage - avgStreamUsage) / avgStreamUsage * 100).toFixed(1);

	const winner = avgMemoryEps > avgStreamEps ? 'Memory' : 'Streaming';
	
	return `${winner} mode is ${Math.abs(speedDiff)}% faster, uses ${memoryDiff > 0 ? '+' : ''}${memoryDiff}% memory`;
}

function generateFormatRecommendation(tests) {
	const fastestTest = tests.reduce((fastest, current) => 
		current.result.eps > fastest.result.eps ? current : fastest
	);

	const format = fastestTest.name.split(' ')[0];
	const mode = fastestTest.result.wasStream ? 'streaming' : 'memory';
	
	// Consider memory usage for large datasets
	const memoryMB = Math.round((fastestTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	
	let recommendation = `Use ${format} format with ${mode} processing`;
	
	if (memoryMB > 500) {
		recommendation += ` (Note: High memory usage - consider streaming for large datasets)`;
	}
	
	return recommendation;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	formatComparison(config).catch(console.error);
}