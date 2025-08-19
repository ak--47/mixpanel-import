//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../../index.js');
const u = require('../../node_modules/ak-tools');

/**
 * MEMORY VS STREAMING BENCHMARK
 * 
 * Tests memory-based processing vs streaming for different data sizes and scenarios.
 * This helps determine when to use streaming vs when memory processing is acceptable.
 * 
 * Key scenarios tested:
 * - Small datasets (memory should be fine)
 * - Medium datasets (streaming may help)
 * - Large datasets (streaming required)
 * - Different file formats and their streaming capabilities
 * - Memory usage patterns and garbage collection impact
 * 
 * Memory processing: Loads entire dataset into memory (faster but limited by RAM)
 * Streaming: Processes data in chunks (slower but handles any size)
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

export default async function memoryVsStream(config = {}) {
	const { dataFile, dryRun = true, credentials } = config;
	
	console.log('💾 MEMORY VS STREAMING BENCHMARK');
	console.log('Testing memory-based vs streaming data processing...');
	console.log('');

	// Use optimal settings from previous benchmarks
	const optimalWorkers = 20;

	const baseOptions = {
		logs: false,
		verbose: false,
		streamFormat: 'jsonl',
		recordType: 'event',
		abridged: true,
		dryRun: dryRun,
		workers: optimalWorkers,
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

	// Test different processing configurations
	const processingTests = [
		{
			name: 'Auto Mode (Default)',
			config: {
				forceStream: undefined, // Let system decide
				streamFormat: 'jsonl'
			}
		},
		{
			name: 'Force Memory (JSONL)',
			config: {
				forceStream: false,
				streamFormat: 'jsonl'
			}
		},
		{
			name: 'Force Streaming (JSONL)',
			config: {
				forceStream: true,
				streamFormat: 'jsonl'
			}
		},
		{
			name: 'Force Memory (JSON)',
			config: {
				forceStream: false,
				streamFormat: 'json'
			},
			dataFile: dataFile.replace('.ndjson', '.json')
		},
		{
			name: 'Force Streaming (JSON)',
			config: {
				forceStream: true,
				streamFormat: 'json'
			},
			dataFile: dataFile.replace('.ndjson', '.json')
		}
	];

	// Add high water mark tests for streaming
	const streamingTests = [
		{
			name: 'Streaming (Default Buffer)',
			config: {
				forceStream: true,
				streamFormat: 'jsonl',
				highWaterMark: undefined // Use default
			}
		},
		{
			name: 'Streaming (Small Buffer)',
			config: {
				forceStream: true,
				streamFormat: 'jsonl',
				highWaterMark: 16 * 1024 // 16KB
			}
		},
		{
			name: 'Streaming (Large Buffer)',
			config: {
				forceStream: true,
				streamFormat: 'jsonl',
				highWaterMark: 256 * 1024 // 256KB
			}
		},
		{
			name: 'Streaming (Huge Buffer)',
			config: {
				forceStream: true,
				streamFormat: 'jsonl',
				highWaterMark: 1024 * 1024 // 1MB
			}
		}
	];

	const results = {
		description: 'Memory vs streaming processing performance comparison',
		baseOptions,
		tests: [],
		bestResult: null,
		analysis: {}
	};

	// Test main processing modes
	for (let i = 0; i < processingTests.length; i++) {
		const test = processingTests[i];
		console.log(`  [${i + 1}/${processingTests.length}] Testing: ${test.name}...`);
		
		const testDataFile = test.dataFile || dataFile;
		const options = {
			...baseOptions,
			...test.config
		};

		// Skip test if data file doesn't exist
		if (!(await fileExists(testDataFile))) {
			console.log(`    ⏭️  Skipped: Data file not found (${testDataFile})`);
			continue;
		}

		try {
			// Monitor memory before test
			const memoryBefore = process.memoryUsage();
			
			const startTime = Date.now();
			const result = await mpStream({}, testDataFile, options);
			const endTime = Date.now();
			
			// Monitor memory after test
			const memoryAfter = process.memoryUsage();
			
			const testResult = {
				name: test.name,
				config: test.config,
				dataFile: testDataFile,
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
					wasStream: result.wasStream
				},
				actualDuration: endTime - startTime,
				memoryImpact: {
					before: memoryBefore,
					after: memoryAfter,
					peak: result.memory,
					increase: memoryAfter.heapUsed - memoryBefore.heapUsed
				},
				efficiency: calculateProcessingEfficiency(result, memoryAfter.heapUsed - memoryBefore.heapUsed)
			};

			results.tests.push(testResult);

			// Track best result (highest efficiency)
			if (!results.bestResult || testResult.efficiency > results.bestResult.efficiency) {
				results.bestResult = {
					...testResult.result,
					config: testResult.config,
					name: testResult.name,
					efficiency: testResult.efficiency,
					memoryImpact: testResult.memoryImpact
				};
			}

			// Console output
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const memoryMB = Math.round((memoryAfter.heapUsed) / 1024 / 1024);
			const mode = result.wasStream ? 'Streaming' : 'Memory';
			const efficiency = testResult.efficiency.toFixed(1);
			
			console.log(`    EPS: ${epsFormatted}, Memory: ${memoryMB}MB, Mode: ${mode}, Efficiency: ${efficiency}`);
			
		} catch (error) {
			console.log(`    ❌ Failed: ${error.message}`);
			results.tests.push({
				name: test.name,
				config: test.config,
				dataFile: testDataFile,
				error: error.message
			});
		}
	}

	// Test streaming buffer sizes
	console.log('');
	console.log('🔄 Testing streaming buffer configurations...');
	
	for (let i = 0; i < streamingTests.length; i++) {
		const test = streamingTests[i];
		console.log(`  [${i + 1}/${streamingTests.length}] Testing: ${test.name}...`);
		
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
					memory: result.memory,
					wasStream: result.wasStream
				},
				actualDuration: endTime - startTime,
				efficiency: calculateProcessingEfficiency(result, result.memory?.heapUsed || 0)
			};

			results.tests.push(testResult);
			
			const epsFormatted = Math.round(result.eps).toLocaleString();
			const bufferSize = test.config.highWaterMark ? `${Math.round(test.config.highWaterMark / 1024)}KB` : 'Default';
			const memoryMB = Math.round((result.memory?.heapUsed || 0) / 1024 / 1024);
			
			console.log(`    EPS: ${epsFormatted}, Buffer: ${bufferSize}, Memory: ${memoryMB}MB`);
			
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
	results.analysis = analyzeMemoryVsStream(results.tests);
	
	console.log('');
	console.log('📊 Memory vs Streaming Analysis:');
	console.log(`   🏆 Most Efficient: ${results.analysis.mostEfficient} (Efficiency: ${results.analysis.bestEfficiency})`);
	console.log(`   ⚡ Performance Impact: ${results.analysis.performanceImpact}`);
	console.log(`   💾 Memory Impact: ${results.analysis.memoryImpact}`);
	console.log(`   📊 Buffer Size Impact: ${results.analysis.bufferAnalysis}`);
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

function calculateProcessingEfficiency(result, memoryIncrease) {
	// Efficiency considers EPS and memory usage
	// Higher EPS is better, lower memory usage is better
	
	const epsScore = result.eps || 0;
	const memoryPenalty = (memoryIncrease || 0) / (1024 * 1024 * 50); // Penalty for each 50MB
	
	return Math.max(0, epsScore - memoryPenalty);
}

function analyzeMemoryVsStream(tests) {
	const successfulTests = tests.filter(t => !t.error && t.result);
	
	if (successfulTests.length === 0) {
		return { error: 'No successful tests to analyze' };
	}

	// Find most efficient configuration
	const mostEfficientTest = successfulTests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);

	// Analyze memory vs streaming performance
	const memoryTests = successfulTests.filter(t => t.result.wasStream === false);
	const streamTests = successfulTests.filter(t => t.result.wasStream === true);
	
	const performanceImpact = analyzePerformanceImpact(memoryTests, streamTests);
	const memoryImpact = analyzeMemoryImpact(successfulTests);
	
	// Analyze buffer size impact
	const bufferAnalysis = analyzeBufferImpact(successfulTests);
	
	// Generate recommendation
	const recommendation = generateProcessingRecommendation(successfulTests);

	return {
		mostEfficient: mostEfficientTest.name,
		bestEfficiency: mostEfficientTest.efficiency.toFixed(1),
		performanceImpact,
		memoryImpact,
		bufferAnalysis,
		recommendation,
		totalTestsRun: tests.length,
		successfulTests: successfulTests.length
	};
}

function analyzePerformanceImpact(memoryTests, streamTests) {
	if (memoryTests.length === 0 || streamTests.length === 0) {
		return 'Insufficient data for memory vs streaming comparison';
	}

	// Average EPS for each mode
	const avgMemoryEps = memoryTests.reduce((sum, test) => sum + test.result.eps, 0) / memoryTests.length;
	const avgStreamEps = streamTests.reduce((sum, test) => sum + test.result.eps, 0) / streamTests.length;

	const speedDiff = ((avgMemoryEps - avgStreamEps) / avgStreamEps * 100).toFixed(1);
	const winner = avgMemoryEps > avgStreamEps ? 'Memory' : 'Streaming';
	
	return `${winner} processing is ${Math.abs(speedDiff)}% ${avgMemoryEps > avgStreamEps ? 'faster' : 'slower'} on average`;
}

function analyzeMemoryImpact(tests) {
	const testsWithMemoryData = tests.filter(t => t.memoryImpact || t.result.memory);
	
	if (testsWithMemoryData.length === 0) {
		return 'No memory usage data available';
	}

	// Find highest and lowest memory usage
	const memoryUsages = testsWithMemoryData.map(t => {
		const usage = t.memoryImpact?.after?.heapUsed || t.result.memory?.heapUsed || 0;
		return {
			name: t.name,
			memory: Math.round(usage / 1024 / 1024), // MB
			wasStream: t.result.wasStream
		};
	});

	const highest = memoryUsages.reduce((max, current) => 
		current.memory > max.memory ? current : max
	);
	
	const lowest = memoryUsages.reduce((min, current) => 
		current.memory < min.memory ? current : min
	);

	const difference = highest.memory - lowest.memory;
	const percentDiff = ((difference / lowest.memory) * 100).toFixed(1);

	return `${difference}MB range (${lowest.memory}MB to ${highest.memory}MB, ${percentDiff}% difference)`;
}

function analyzeBufferImpact(tests) {
	const bufferTests = tests.filter(t => 
		t.name.includes('Buffer') && t.config.highWaterMark !== undefined
	);
	
	if (bufferTests.length < 2) {
		return 'Insufficient buffer size test data';
	}

	// Sort by buffer size
	const sorted = bufferTests
		.sort((a, b) => (a.config.highWaterMark || 0) - (b.config.highWaterMark || 0));
	
	const smallest = sorted[0];
	const largest = sorted[sorted.length - 1];
	
	const epsImprovement = ((largest.result.eps - smallest.result.eps) / smallest.result.eps * 100).toFixed(1);
	const smallestSize = Math.round((smallest.config.highWaterMark || 0) / 1024);
	const largestSize = Math.round((largest.config.highWaterMark || 0) / 1024);
	
	return `${Math.abs(epsImprovement)}% EPS ${epsImprovement > 0 ? 'improvement' : 'decrease'} from ${smallestSize}KB to ${largestSize}KB buffer`;
}

function generateProcessingRecommendation(tests) {
	// Find the best overall performer
	const bestTest = tests.reduce((best, current) => 
		current.efficiency > best.efficiency ? current : best
	);
	
	const mode = bestTest.result.wasStream ? 'streaming' : 'memory';
	const epsFormatted = Math.round(bestTest.result.eps).toLocaleString();
	
	// Consider memory constraints
	const memoryMB = bestTest.memoryImpact?.after?.heapUsed 
		? Math.round(bestTest.memoryImpact.after.heapUsed / 1024 / 1024)
		: Math.round((bestTest.result.memory?.heapUsed || 0) / 1024 / 1024);
	
	let recommendation = `Use ${mode} processing (${epsFormatted} EPS, ${memoryMB}MB memory)`;
	
	// Add specific guidance based on performance patterns
	const memoryTests = tests.filter(t => t.result.wasStream === false);
	const streamTests = tests.filter(t => t.result.wasStream === true);
	
	if (memoryTests.length > 0 && streamTests.length > 0) {
		const avgMemoryEps = memoryTests.reduce((sum, test) => sum + test.result.eps, 0) / memoryTests.length;
		const avgStreamEps = streamTests.reduce((sum, test) => sum + test.result.eps, 0) / streamTests.length;
		
		if (Math.abs(avgMemoryEps - avgStreamEps) / Math.max(avgMemoryEps, avgStreamEps) < 0.1) {
			recommendation += '. Performance difference is minimal - choose based on dataset size and memory constraints';
		}
	}
	
	if (memoryMB > 500) {
		recommendation += '. Consider streaming for large datasets to reduce memory usage';
	}
	
	return recommendation;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	const config = {
		dataFile: '../testData/dnd250.ndjson',
		dryRun: true // Set to false to use live API with credentials
	};
	memoryVsStream(config).catch(console.error);
}