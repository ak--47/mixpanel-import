//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * SCALING ANALYSIS BENCHMARK
 * 
 * Tests how different formats scale from small (250k) to large (1M) datasets.
 * This benchmark helps understand which formats maintain performance 
 * characteristics as data volume increases.
 * 
 * Analysis:
 * - Linear vs non-linear scaling patterns
 * - Memory usage growth patterns
 * - Processing efficiency at different scales
 * - Format-specific bottlenecks
 * 
 * Key insights:
 * - Which formats scale best with data size
 * - Memory pressure differences
 * - Processing efficiency curves
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

export default async function scalingAnalysis(config = {}) {
	const { formatData, dryRun = true, credentials } = config;
	
	console.log('📈 SCALING ANALYSIS BENCHMARK');
	console.log('Testing format performance scaling from 250k to 1M records...');
	console.log('');

	// Use optimal settings from other benchmarks
	const baseOptions = {
		logs: false,
		verbose: false,
		showProgress: true,
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

	// Find formats that have both sizes available
	const scalingFormats = [];
	
	for (const [formatName, sizeData] of Object.entries(formatData)) {
		if (sizeData['250k'] && sizeData['1m']) {
			scalingFormats.push({
				format: formatName,
				streamFormat: getStreamFormat(formatName),
				small: sizeData['250k'],
				large: sizeData['1m'],
				compressed: formatName.includes('-gz')
			});
		}
	}

	const results = {
		description: 'Performance scaling analysis across data sizes',
		baseOptions,
		tests: [],
		scalingResults: [],
		bestResult: null,
		analysis: {}
	};

	// Test each format at both sizes
	for (let i = 0; i < scalingFormats.length; i++) {
		const format = scalingFormats[i];
		console.log(`  [${i + 1}/${scalingFormats.length}] Testing scaling: ${format.format}...`);
		
		const scalingResult = {
			format: format.format,
			streamFormat: format.streamFormat,
			compressed: format.compressed,
			small: null,
			large: null,
			scaling: {}
		};

		// Test small dataset (250k)
		console.log(`    Testing 250k records...`);
		if (await fileExists(format.small)) {
			try {
				const options = {
					...baseOptions,
					streamFormat: format.streamFormat
				};
				
				if (format.streamFormat === 'csv') {
					options.aliases = {
						'event': 'event',
						'distinct_id': 'distinct_id',
						'time': 'time'
					};
				}

				const startTime = Date.now();
				const result = await mpStream({}, format.small, options);
				const endTime = Date.now();
				
				scalingResult.small = {
					result: {
						eps: result.eps,
						rps: result.rps,
						mbps: result.mbps,
						duration: result.duration,
						memory: result.memory,
						success: result.success,
						failed: result.failed,
						avgBatchLength: result.avgBatchLength
					},
					actualDuration: endTime - startTime,
					fileStats: await getFileStats(format.small)
				};
				
				const epsFormatted = Math.round(result.eps).toLocaleString();
				const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
				console.log(`      EPS: ${epsFormatted}, Memory: ${memoryMB}MB`);
				
			} catch (error) {
				console.log(`      ❌ Failed: ${error.message}`);
				scalingResult.small = { error: error.message };
			}
		} else {
			console.log(`      ⏭️ Skipped: File not found`);
		}

		// Test large dataset (1M)
		console.log(`    Testing 1M records...`);
		if (await fileExists(format.large)) {
			try {
				const options = {
					...baseOptions,
					streamFormat: format.streamFormat
				};
				
				if (format.streamFormat === 'csv') {
					options.aliases = {
						'event': 'event',
						'distinct_id': 'distinct_id',
						'time': 'time'
					};
				}

				const startTime = Date.now();
				const result = await mpStream({}, format.large, options);
				const endTime = Date.now();
				
				scalingResult.large = {
					result: {
						eps: result.eps,
						rps: result.rps,
						mbps: result.mbps,
						duration: result.duration,
						memory: result.memory,
						success: result.success,
						failed: result.failed,
						avgBatchLength: result.avgBatchLength
					},
					actualDuration: endTime - startTime,
					fileStats: await getFileStats(format.large)
				};
				
				const epsFormatted = Math.round(result.eps).toLocaleString();
				const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
				console.log(`      EPS: ${epsFormatted}, Memory: ${memoryMB}MB`);
				
			} catch (error) {
				console.log(`      ❌ Failed: ${error.message}`);
				scalingResult.large = { error: error.message };
			}
		} else {
			console.log(`      ⏭️ Skipped: File not found`);
		}

		// Analyze scaling characteristics
		if (scalingResult.small?.result && scalingResult.large?.result) {
			scalingResult.scaling = analyzeScalingCharacteristics(scalingResult);
			
			console.log(`    📊 EPS Scaling: ${scalingResult.scaling.epsScaling}`);
			console.log(`    💾 Memory Scaling: ${scalingResult.scaling.memoryScaling}`);
			console.log(`    ⚡ Efficiency: ${scalingResult.scaling.efficiency}`);
		}

		results.scalingResults.push(scalingResult);
		console.log('');
	}

	// Generate overall analysis
	results.analysis = analyzeOverallScaling(results.scalingResults);
	
	console.log('📊 Scaling Analysis:');
	console.log(`   🏆 Best Scaling Format: ${results.analysis.bestScalingFormat}`);
	console.log(`   📈 Linear Scaling Formats: ${results.analysis.linearScaling.join(', ')}`);
	console.log(`   ⚠️ Poor Scaling Formats: ${results.analysis.poorScaling.join(', ')}`);
	console.log(`   💾 Memory Efficiency: ${results.analysis.memoryEfficiency}`);
	console.log(`   🎯 Recommendation: ${results.analysis.recommendation}`);
	console.log('');

	return results;
}

function getStreamFormat(formatName) {
	if (formatName.startsWith('json')) return 'jsonl';
	if (formatName.startsWith('csv')) return 'csv';
	if (formatName.startsWith('parquet')) return 'parquet';
	return 'jsonl';
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

function analyzeScalingCharacteristics(scalingResult) {
	const small = scalingResult.small.result;
	const large = scalingResult.large.result;
	
	// Theoretical 4x scaling (1M vs 250k records)
	const expectedScalingFactor = 4;
	
	// EPS scaling analysis
	const epsScalingFactor = large.eps / small.eps;
	const epsScalingQuality = epsScalingFactor / expectedScalingFactor; // 1.0 = perfect linear scaling
	
	let epsScaling;
	if (epsScalingQuality > 0.9) {
		epsScaling = `Excellent (${epsScalingFactor.toFixed(2)}x scaling)`;
	} else if (epsScalingQuality > 0.7) {
		epsScaling = `Good (${epsScalingFactor.toFixed(2)}x scaling)`;
	} else if (epsScalingQuality > 0.5) {
		epsScaling = `Fair (${epsScalingFactor.toFixed(2)}x scaling)`;
	} else {
		epsScaling = `Poor (${epsScalingFactor.toFixed(2)}x scaling)`;
	}

	// Memory scaling analysis
	const smallMemory = small.memory?.heapUsed || 0;
	const largeMemory = large.memory?.heapUsed || 0;
	const memoryGrowth = largeMemory > 0 && smallMemory > 0 
		? (largeMemory / smallMemory).toFixed(2)
		: 'N/A';
	
	let memoryScaling;
	if (memoryGrowth === 'N/A') {
		memoryScaling = 'No data';
	} else {
		const memGrowthNum = parseFloat(memoryGrowth);
		if (memGrowthNum < 2) {
			memoryScaling = `Excellent (${memoryGrowth}x growth)`;
		} else if (memGrowthNum < 4) {
			memoryScaling = `Good (${memoryGrowth}x growth)`;
		} else if (memGrowthNum < 6) {
			memoryScaling = `Fair (${memoryGrowth}x growth)`;
		} else {
			memoryScaling = `Poor (${memoryGrowth}x growth)`;
		}
	}

	// Overall efficiency score
	let efficiency;
	if (epsScalingQuality > 0.8) {
		efficiency = 'High';
	} else if (epsScalingQuality > 0.6) {
		efficiency = 'Medium';
	} else {
		efficiency = 'Low';
	}

	return {
		epsScaling,
		memoryScaling,
		efficiency,
		epsScalingFactor,
		epsScalingQuality,
		memoryGrowthFactor: memoryGrowth === 'N/A' ? null : parseFloat(memoryGrowth)
	};
}

function analyzeOverallScaling(scalingResults) {
	const validResults = scalingResults.filter(r => 
		r.scaling && typeof r.scaling.epsScalingQuality === 'number'
	);

	if (validResults.length === 0) {
		return { error: 'No valid scaling results available' };
	}

	// Find best scaling format
	const bestScalingFormat = validResults.reduce((best, current) => 
		current.scaling.epsScalingQuality > best.scaling.epsScalingQuality ? current : best
	);

	// Categorize formats by scaling quality
	const linearScaling = validResults
		.filter(r => r.scaling.epsScalingQuality > 0.8)
		.map(r => r.format);
	
	const poorScaling = validResults
		.filter(r => r.scaling.epsScalingQuality < 0.6)
		.map(r => r.format);

	// Memory efficiency analysis
	const memoryResults = validResults.filter(r => r.scaling.memoryGrowthFactor !== null);
	const avgMemoryGrowth = memoryResults.length > 0 
		? memoryResults.reduce((sum, r) => sum + r.scaling.memoryGrowthFactor, 0) / memoryResults.length
		: 0;
	
	let memoryEfficiency;
	if (avgMemoryGrowth < 2) {
		memoryEfficiency = 'Excellent - minimal memory growth';
	} else if (avgMemoryGrowth < 4) {
		memoryEfficiency = 'Good - reasonable memory scaling';
	} else {
		memoryEfficiency = 'Poor - high memory growth';
	}

	// Generate recommendation
	let recommendation;
	if (linearScaling.length > 0) {
		recommendation = `For large datasets, prefer: ${linearScaling.slice(0, 2).join(', ')} (best scaling characteristics)`;
	} else if (validResults.length > 0) {
		recommendation = `All formats show scaling challenges - consider optimizing batch sizes and worker counts for large datasets`;
	} else {
		recommendation = 'Insufficient scaling data for recommendations';
	}

	return {
		bestScalingFormat: `${bestScalingFormat.format} (${bestScalingFormat.scaling.epsScaling})`,
		linearScaling: linearScaling.length > 0 ? linearScaling : ['None'],
		poorScaling: poorScaling.length > 0 ? poorScaling : ['None'],
		memoryEfficiency,
		recommendation,
		totalFormats: validResults.length,
		avgScalingQuality: (validResults.reduce((sum, r) => sum + r.scaling.epsScalingQuality, 0) / validResults.length).toFixed(2)
	};
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		formatData: {
			'json': { 
				'250k': '../testData/formats/json/json-250k-EVENTS.json',
				'1m': '../testData/formats/json/json-1m-EVENTS.json'
			},
			'csv': { 
				'250k': '../testData/formats/csv/csv-250k-EVENTS.csv',
				'1m': '../testData/formats/csv/csv-1m-EVENTS.csv'
			}
		},
		dryRun: true
	};
	scalingAnalysis(config).catch(console.error);
}