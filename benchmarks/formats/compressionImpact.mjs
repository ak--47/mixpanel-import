//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * COMPRESSION IMPACT BENCHMARK
 * 
 * Specifically tests the performance impact of compressed vs uncompressed formats.
 * This benchmark helps understand the trade-offs between file size and processing speed.
 * 
 * Comparisons:
 * - JSON vs JSON-GZ
 * - CSV vs CSV-GZ  
 * - Parquet vs Parquet-GZ
 * 
 * Key metrics:
 * - Processing speed difference
 * - File size reduction
 * - Memory usage impact
 * - Network transfer efficiency
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

export default async function compressionImpact(config = {}) {
	const { formatData, dataSize, dryRun = true, credentials } = config;
	
	console.log('🗜️ COMPRESSION IMPACT BENCHMARK');
	console.log('Testing performance impact of compressed vs uncompressed formats...');
	console.log('');

	// Use optimal settings from other benchmarks
	const baseOptions = {
		logs: false,
		verbose: false,
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: 20,
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

	// Create compression comparison pairs
	const compressionPairs = [];
	const baseFormats = ['json', 'csv', 'parquet'];
	
	for (const baseFormat of baseFormats) {
		const uncompressed = formatData[baseFormat]?.[dataSize];
		const compressed = formatData[`${baseFormat}-gz`]?.[dataSize];
		
		if (uncompressed && compressed) {
			compressionPairs.push({
				format: baseFormat,
				uncompressed: {
					file: uncompressed,
					name: `${baseFormat} (uncompressed)`
				},
				compressed: {
					file: compressed,
					name: `${baseFormat}-gz (compressed)`
				}
			});
		}
	}

	const results = {
		description: 'Compression impact analysis across data formats',
		baseOptions,
		dataSize,
		tests: [],
		comparisons: [],
		bestResult: null,
		analysis: {}
	};

	// Test each compression pair
	for (let i = 0; i < compressionPairs.length; i++) {
		const pair = compressionPairs[i];
		console.log(`  [${i + 1}/${compressionPairs.length}] Testing compression impact: ${pair.format}...`);
		
		const comparison = {
			format: pair.format,
			uncompressed: null,
			compressed: null,
			analysis: {}
		};

		// Test uncompressed version
		console.log(`    Testing ${pair.uncompressed.name}...`);
		if (await fileExists(pair.uncompressed.file)) {
			try {
				const options = {
					...baseOptions,
					streamFormat: getStreamFormat(pair.format)
				};
				
				if (pair.format === 'csv') {
					options.aliases = {
						'event': 'event',
						'distinct_id': 'distinct_id', 
						'time': 'time'
					};
				}

				const startTime = Date.now();
				const result = await mpStream({}, pair.uncompressed.file, options);
				const endTime = Date.now();
				
				comparison.uncompressed = {
					result: {
						eps: result.eps,
						rps: result.rps,
						mbps: result.mbps,
						duration: result.duration,
						memory: result.memory,
						success: result.success,
						failed: result.failed
					},
					actualDuration: endTime - startTime,
					fileStats: await getFileStats(pair.uncompressed.file)
				};
				
				const epsFormatted = Math.round(result.eps).toLocaleString();
				const fileSize = comparison.uncompressed.fileStats?.sizeHuman || 'N/A';
				console.log(`      EPS: ${epsFormatted}, Size: ${fileSize}`);
				
			} catch (error) {
				console.log(`      ❌ Failed: ${error.message}`);
				comparison.uncompressed = { error: error.message };
			}
		} else {
			console.log(`      ⏭️ Skipped: File not found`);
		}

		// Test compressed version
		console.log(`    Testing ${pair.compressed.name}...`);
		if (await fileExists(pair.compressed.file)) {
			try {
				const options = {
					...baseOptions,
					streamFormat: getStreamFormat(pair.format)
				};
				
				if (pair.format === 'csv') {
					options.aliases = {
						'event': 'event',
						'distinct_id': 'distinct_id',
						'time': 'time'
					};
				}

				const startTime = Date.now();
				const result = await mpStream({}, pair.compressed.file, options);
				const endTime = Date.now();
				
				comparison.compressed = {
					result: {
						eps: result.eps,
						rps: result.rps,
						mbps: result.mbps,
						duration: result.duration,
						memory: result.memory,
						success: result.success,
						failed: result.failed
					},
					actualDuration: endTime - startTime,
					fileStats: await getFileStats(pair.compressed.file)
				};
				
				const epsFormatted = Math.round(result.eps).toLocaleString();
				const fileSize = comparison.compressed.fileStats?.sizeHuman || 'N/A';
				console.log(`      EPS: ${epsFormatted}, Size: ${fileSize}`);
				
			} catch (error) {
				console.log(`      ❌ Failed: ${error.message}`);
				comparison.compressed = { error: error.message };
			}
		} else {
			console.log(`      ⏭️ Skipped: File not found`);
		}

		// Analyze the comparison
		if (comparison.uncompressed?.result && comparison.compressed?.result) {
			comparison.analysis = analyzeCompressionPair(comparison);
			
			console.log(`    📊 Speed Impact: ${comparison.analysis.speedImpact}`);
			console.log(`    📦 Size Reduction: ${comparison.analysis.sizeReduction}`);
			console.log(`    💾 Memory Impact: ${comparison.analysis.memoryImpact}`);
		}

		results.comparisons.push(comparison);
		console.log('');
	}

	// Generate overall analysis
	results.analysis = analyzeOverallCompressionImpact(results.comparisons);
	
	console.log('📊 Compression Impact Analysis:');
	console.log(`   ⚡ Average Speed Impact: ${results.analysis.avgSpeedImpact}`);
	console.log(`   📦 Average Size Reduction: ${results.analysis.avgSizeReduction}`);
	console.log(`   🏆 Best Compression Format: ${results.analysis.bestFormat}`);
	console.log(`   ⚠️ Worst Speed Impact: ${results.analysis.worstSpeedImpact}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

function getStreamFormat(format) {
	switch (format) {
		case 'json': return 'jsonl';
		case 'csv': return 'csv';
		case 'parquet': return 'parquet';
		default: return 'jsonl';
	}
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

function analyzeCompressionPair(comparison) {
	const uncompressed = comparison.uncompressed.result;
	const compressed = comparison.compressed.result;
	const uncompressedStats = comparison.uncompressed.fileStats;
	const compressedStats = comparison.compressed.fileStats;

	// Speed impact
	const speedImpact = ((compressed.eps - uncompressed.eps) / uncompressed.eps * 100);
	const speedImpactStr = `${speedImpact >= 0 ? '+' : ''}${speedImpact.toFixed(1)}% EPS`;

	// Size reduction
	let sizeReduction = 'N/A';
	if (uncompressedStats && compressedStats) {
		const reduction = ((uncompressedStats.size - compressedStats.size) / uncompressedStats.size * 100);
		sizeReduction = `${reduction.toFixed(1)}% smaller`;
	}

	// Memory impact
	const uncompressedMem = uncompressed.memory?.heapUsed || 0;
	const compressedMem = compressed.memory?.heapUsed || 0;
	const memoryImpact = uncompressedMem > 0 
		? `${((compressedMem - uncompressedMem) / uncompressedMem * 100).toFixed(1)}% memory change`
		: 'N/A';

	return {
		speedImpact: speedImpactStr,
		sizeReduction,
		memoryImpact,
		speedValue: speedImpact,
		compressionRatio: uncompressedStats && compressedStats 
			? compressedStats.size / uncompressedStats.size 
			: 1
	};
}

function analyzeOverallCompressionImpact(comparisons) {
	const validComparisons = comparisons.filter(c => 
		c.analysis && typeof c.analysis.speedValue === 'number'
	);

	if (validComparisons.length === 0) {
		return { error: 'No valid compression comparisons available' };
	}

	// Calculate averages
	const avgSpeedImpact = validComparisons.reduce((sum, c) => sum + c.analysis.speedValue, 0) / validComparisons.length;
	
	// Find best and worst
	const bestSpeedFormat = validComparisons.reduce((best, current) => 
		current.analysis.speedValue > best.analysis.speedValue ? current : best
	);
	
	const worstSpeedFormat = validComparisons.reduce((worst, current) => 
		current.analysis.speedValue < worst.analysis.speedValue ? current : worst
	);

	// Calculate average size reduction
	const sizeMeasurements = validComparisons.filter(c => c.analysis.compressionRatio < 1);
	const avgSizeReduction = sizeMeasurements.length > 0 
		? sizeMeasurements.reduce((sum, c) => sum + ((1 - c.analysis.compressionRatio) * 100), 0) / sizeMeasurements.length
		: 0;

	// Generate recommendation
	let recommendation;
	if (avgSpeedImpact >= -5) { // Less than 5% speed penalty
		recommendation = 'Compression recommended - minimal speed impact with significant size benefits';
	} else if (avgSpeedImpact >= -15) { // 5-15% penalty
		recommendation = 'Consider compression for network/storage constrained environments';
	} else {
		recommendation = 'Avoid compression for performance-critical applications';
	}

	return {
		avgSpeedImpact: `${avgSpeedImpact >= 0 ? '+' : ''}${avgSpeedImpact.toFixed(1)}% EPS`,
		avgSizeReduction: `${avgSizeReduction.toFixed(1)}% average reduction`,
		bestFormat: `${bestSpeedFormat.format} (${bestSpeedFormat.analysis.speedImpact})`,
		worstSpeedImpact: `${worstSpeedFormat.format} (${worstSpeedFormat.analysis.speedImpact})`,
		recommendation,
		totalComparisons: validComparisons.length
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		formatData: {
			'json': { '250k': '../testData/formats/json/json-250k-EVENTS.json' },
			'json-gz': { '250k': '../testData/formats/json-gz/json-gz-250k-EVENTS.json.gz' },
			'csv': { '250k': '../testData/formats/csv/csv-250k-EVENTS.csv' },
			'csv-gz': { '250k': '../testData/formats/csv-gz/csv-gz-250k-EVENTS.csv.gz' }
		},
		dataSize: '250k',
		dryRun: true
	};
	compressionImpact(config).catch(console.error);
}