//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * FORMAT PERFORMANCE BENCHMARK
 * 
 * Tests performance across all supported data formats to identify
 * the fastest parsing and processing combinations.
 * 
 * Formats tested:
 * - JSON (JSONL) - Line-delimited JSON
 * - JSON-GZ - Compressed JSONL
 * - CSV - Comma-separated values
 * - CSV-GZ - Compressed CSV
 * - Parquet - Columnar binary format
 * - Parquet-GZ - Compressed Parquet
 * 
 * Key metrics:
 * - Parsing speed (EPS)
 * - Memory usage during processing
 * - File size efficiency
 * - Processing overhead
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

export default async function formatPerformance(config = {}) {
	const { formatData, dataSize, dryRun = true, credentials } = config;
	
	console.log('📊 FORMAT PERFORMANCE BENCHMARK');
	console.log('Testing parsing performance across all supported formats...');
	console.log('');

	// Use optimal settings from other benchmarks
	const baseOptions = {
		logs: false,
		verbose: false,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 20, // Optimal from worker benchmarks
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

	// Test all available formats for the specified data size
	const formatTests = [];
	
	for (const [formatName, sizeData] of Object.entries(formatData)) {
		if (sizeData[dataSize]) {
			const dataFile = sizeData[dataSize];
			
			// Determine stream format and any special options
			let streamFormat, specialOptions = {};
			
			if (formatName.startsWith('json')) {
				streamFormat = 'jsonl';
			} else if (formatName.startsWith('csv')) {
				streamFormat = 'csv';
				// Add basic CSV aliases for event data
				specialOptions.aliases = {
					'event': 'event',
					'distinct_id': 'distinct_id',
					'time': 'time'
				};
			} else if (formatName.startsWith('parquet')) {
				streamFormat = 'parquet';
			}
			
			formatTests.push({
				format: formatName,
				dataFile: dataFile,
				streamFormat: streamFormat,
				specialOptions: specialOptions,
				compressed: formatName.includes('-gz')
			});
		}
	}

	const results = {
		description: 'Performance comparison across all supported data formats',
		baseOptions,
		dataSize,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	for (let i = 0; i < formatTests.length; i++) {
		const test = formatTests[i];
		console.log(`  [${i + 1}/${formatTests.length}] Testing: ${test.format} (${test.streamFormat})...`);
		
		// Check if file exists
		if (!(await fileExists(test.dataFile))) {
			console.log(`    ⏭️  Skipped: Data file not found (${test.dataFile})`);
			continue;
		}

		const options = {
			...baseOptions,
			streamFormat: test.streamFormat,
			...test.specialOptions
		};

		try {
			const startTime = Date.now();
			const result = await mpStream({}, test.dataFile, options);
			const endTime = Date.now();
			
			const testResult = {
				format: test.format,
				streamFormat: test.streamFormat,
				dataFile: test.dataFile,
				compressed: test.compressed,
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
					wasStream: result.wasStream
				},
				actualDuration: endTime - startTime,
				fileStats: await getFileStats(test.dataFile),
				efficiency: calculateFormatEfficiency(result, test.compressed)
			};

			results.tests.push(testResult);

			// Track best result (highest efficiency)
			if (!results.bestResult || testResult.efficiency > results.bestResult.efficiency) {
				results.bestResult = {
					...testResult.result,
					format: testResult.format,
					streamFormat: testResult.streamFormat,
					efficiency: testResult.efficiency,
					fileStats: testResult.fileStats
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			const fileSize = testResult.fileStats ? `${Math.round(testResult.fileStats.size / 1024 / 1024)}MB` : 'N/A';
			const efficiency = testResult.efficiency.toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, Memory: ${memoryMB}MB, File: ${fileSize}, Efficiency: ${efficiency}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				format: test.format,
				streamFormat: test.streamFormat,
				dataFile: test.dataFile,
				error: error.message
			});
		}
	}

	// Generate analysis
	results.analysis = analyzeFormatPerformance(results.tests);
	
	console.log('');
	console.log('📊 Format Performance Analysis:');
	console.log(`   🏆 Fastest Format: ${results.analysis.fastestFormat} (${Math.round(results.analysis.fastestEps).toLocaleString()} EPS)`);
	console.log(`   💾 Most Efficient: ${results.analysis.mostEfficient} (Efficiency: ${results.analysis.bestEfficiency})`);
	console.log(`   📦 Compression Impact: ${results.analysis.compressionImpact}`);
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

async function getFileStats(filePath) {
	try {
		const { statSync } = await import('fs');
		const stats = statSync(filePath);
		return {
			size: stats.size,
			sizeHuman: formatBytes(stats.size)
		};
	} catch {
		return null;
	}
}

function formatBytes(bytes) {
	if (bytes === 0) return '0 B';
	const k = 1024;
	const sizes = ['B', 'KB', 'MB', 'GB'];
	const i = Math.floor(Math.log(bytes) / Math.log(k));
	return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function calculateFormatEfficiency(result, isCompressed) {
	// Efficiency considers EPS, memory usage, and compression benefit
	let efficiency = result.eps || 0;
	
	// Penalty for high memory usage
	const memoryPenalty = (result.memory?.heapUsed || 0) / (1024 * 1024 * 100); // Per 100MB
	efficiency -= memoryPenalty;
	
	// Bonus for compression (smaller network overhead)
	if (isCompressed) {
		efficiency += (result.eps || 0) * 0.1; // 10% bonus for compressed formats
	}
	
	// Penalty for errors
	const errorPenalty = (result.failed || 0) * 10;
	efficiency -= errorPenalty;
	
	return Math.max(0, efficiency);
}

function analyzeFormatPerformance(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find fastest format (highest EPS)
	const fastestTest = successfulTests.reduce((fastest, current) => 
		current.result.eps > fastest.result.eps ? current : fastest
	);

	// Find most efficient format
	const mostEfficientTest = successfulTests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);

	// Analyze compression impact
	const compressionImpact = analyzeCompressionImpact(successfulTests);
	
	// Create performance ranking
	const ranking = successfulTests
		.sort((a, b) => b.result.eps - a.result.eps)
		.map(test => test.format)
		.slice(0, 5); // Top 5

	// Generate recommendation
	const recommendation = generateFormatRecommendation(successfulTests);

	return {
		fastestFormat: fastestTest.format,
		fastestEps: fastestTest.result.eps,
		mostEfficient: mostEfficientTest.format,
		bestEfficiency: mostEfficientTest.efficiency.toFixed(1),
		compressionImpact,
		ranking,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzeCompressionImpact(tests) {
	// Compare compressed vs uncompressed versions
	const formatPairs = {};
	
	tests.forEach(test => {
		const baseFormat = test.format.replace('-gz', '');
		if (!formatPairs[baseFormat]) {
			formatPairs[baseFormat] = {};
		}
		
		if (test.compressed) {
			formatPairs[baseFormat].compressed = test;
		} else {
			formatPairs[baseFormat].uncompressed = test;
		}
	});

	const comparisons = [];
	
	for (const [format, pair] of Object.entries(formatPairs)) {
		if (pair.compressed && pair.uncompressed) {
			const speedImpact = ((pair.compressed.result.eps - pair.uncompressed.result.eps) / pair.uncompressed.result.eps * 100);
			const sizeReduction = pair.compressed.fileStats && pair.uncompressed.fileStats 
				? ((pair.uncompressed.fileStats.size - pair.compressed.fileStats.size) / pair.uncompressed.fileStats.size * 100)
				: 0;
			
			comparisons.push({
				format,
				speedImpact: speedImpact.toFixed(1),
				sizeReduction: sizeReduction.toFixed(1)
			});
		}
	}

	if (comparisons.length === 0) {
		return 'No compression comparisons available';
	}

	const avgSpeedImpact = comparisons.reduce((sum, c) => sum + parseFloat(c.speedImpact), 0) / comparisons.length;
	const avgSizeReduction = comparisons.reduce((sum, c) => sum + parseFloat(c.sizeReduction), 0) / comparisons.length;

	return `Compression: ${avgSizeReduction.toFixed(1)}% size reduction, ${Math.abs(avgSpeedImpact).toFixed(1)}% ${avgSpeedImpact >= 0 ? 'faster' : 'slower'}`;
}

function generateFormatRecommendation(tests) {
	// Find overall best performer considering efficiency
	const bestTest = tests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);
	
	let recommendation = `Use ${bestTest.format} format for best overall performance`;
	
	// Add context based on results
	const fileSize = bestTest.fileStats ? Math.round(bestTest.fileStats.size / 1024 / 1024) : 0;
	const eps = Math.round(bestTest.result.eps);
	
	if (bestTest.compressed) {
		recommendation += ' (compression provides good efficiency gains)';
	}
	
	if (eps > 30000) {
		recommendation += '. Excellent parsing performance detected';
	} else if (eps < 10000) {
		recommendation += '. Consider optimizing for this format if used frequently';
	}
	
	return recommendation;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		formatData: {
			'json': { '250k': '../testData/formats/json/json-250k-EVENTS.json' },
			'csv': { '250k': '../testData/formats/csv/csv-250k-EVENTS.csv' },
			'parquet': { '250k': '../testData/formats/parquet/parquet-250k-EVENTS.parquet' }
		},
		dataSize: '250k',
		dryRun: true
	};
	formatPerformance(config).catch(console.error);
}