#!/usr/bin/env node
/**
 * Comprehensive Benchmark Suite for Mixpanel Import v3.1.1
 * Tests performance parameters from PERFORMANCE_TUNING.md
 *
 * Run: npm run benchmark
 * or:  node benchmarks/benchmark.js
 */

const mp = require('../index.js');
const fs = require('fs');
const path = require('path');
const { performance } = require('perf_hooks');

// Console colors for output
const colors = {
	reset: '\x1b[0m',
	bright: '\x1b[1m',
	dim: '\x1b[2m',
	green: '\x1b[32m',
	yellow: '\x1b[33m',
	blue: '\x1b[34m',
	red: '\x1b[31m',
	cyan: '\x1b[36m',
	magenta: '\x1b[35m'
};

// Use realistic test data from actual production workloads
const TEST_DATA = {
	tiny_json: {
		name: 'Tiny Events JSON (700MB)',
		file: './benchmarks/testData/5m-events-TINY-700MB-RAW.json.gz',
		avgSize: 140,  // ~700MB / 5M events
		count: 100000,  // Use subset for faster benchmarks
		maxRecords: 100000
	},
	dense_json: {
		name: 'Dense Events JSON (1GB)',
		file: './benchmarks/testData/300k-events-DENSE-1GB-RAW.json.gz',
		avgSize: 3500,  // ~1GB / 300k events
		count: 30000,  // Use subset for faster benchmarks
		maxRecords: 30000
	},
	dense_parquet: {
		name: 'Dense Events Parquet (1GB)',
		file: './benchmarks/testData/1m-events-DENSE-4GB-RAW.parquet',
		avgSize: 4000,  // ~4GB / 1M events (uncompressed)
		count: 30000,  // Use subset for faster benchmarks
		maxRecords: 30000
	}
};

// Benchmark results storage
const results = {
	timestamp: new Date().toISOString(),
	system: {
		node: process.version,
		platform: process.platform,
		arch: process.arch,
		memory: Math.round(require('os').totalmem() / 1024 / 1024 / 1024) + 'GB',
		cores: require('os').cpus().length
	},
	tests: []
};

/**
 * Verify test data files exist
 */
async function generateTestData() {
	console.log(`${colors.cyan}Verifying realistic test data...${colors.reset}\n`);

	let allExist = true;
	for (const [key, config] of Object.entries(TEST_DATA)) {
		const filePath = config.file;
		if (fs.existsSync(filePath)) {
			const stats = fs.statSync(filePath);
			const sizeMB = Math.round(stats.size / 1024 / 1024);

			// Extract uncompressed size from filename (e.g., "4GB" from "1m-events-DENSE-4GB-RAW.json.gz")
			const uncompressedMatch = filePath.match(/(\d+(?:MB|GB))-RAW/);
			const uncompressedSize = uncompressedMatch ? uncompressedMatch[1] : 'unknown';

			console.log(`  ✓ ${config.name} exists (${sizeMB}MB compressed → ${uncompressedSize} uncompressed)`);
		} else {
			console.log(`  ✗ ${config.name} missing: ${filePath}`);
			allExist = false;
		}
	}

	if (!allExist) {
		console.log(`\n${colors.yellow}⚠ Some test data files are missing.${colors.reset}`);
		console.log(`Please ensure all test data files are present in ./benchmarks/testData/`);
		console.log(`You can generate them using the data generation scripts.\n`);
		process.exit(1);
	}
	console.log();
}

/**
 * Run a single benchmark test
 */
async function runBenchmark(testName, testConfig, options) {
	const startTime = performance.now();
	const startMemory = process.memoryUsage();

	try {
		const job = await mp(
			{ token: 'test_token' },
			testConfig.file,
			{
				...options,
				maxRecords: testConfig.maxRecords || testConfig.count,  // Limit records processed
				dryRun: true,
				verbose: false,
				logs: false,
				abridged: true,
				streamFormat: 'jsonl',  // All test data files are NDJSON
				isGzip: testConfig.file.endsWith('.gz')  // Auto-detect gzip
			}
		);

		const endTime = performance.now();
		const endMemory = process.memoryUsage();
		const duration = (endTime - startTime) / 1000;

		// Use actual processed count from job
		const actualCount = job.total || testConfig.count;
		const totalBytes = actualCount * testConfig.avgSize;
		const mbps = (totalBytes / 1024 / 1024) / duration;
		const eps = actualCount / duration;
		const rps = job.requests / duration;

		const result = {
			test: testName,
			config: testConfig.name,
			options: options,
			metrics: {
				duration: parseFloat(duration.toFixed(2)),
				eps: Math.round(eps),
				mbps: parseFloat(mbps.toFixed(2)),
				rps: parseFloat(rps.toFixed(2)),
				memoryUsedMB: Math.round((endMemory.heapUsed - startMemory.heapUsed) / 1024 / 1024),
				peakMemoryMB: Math.round(endMemory.heapUsed / 1024 / 1024)
			},
			stats: {
				success: job.success || actualCount,
				failed: job.failed || 0,
				batches: job.batches || Math.ceil(actualCount / (options.recordsPerBatch || 2000)),
				requests: job.requests || 0,
				total: actualCount
			}
		};

		return result;
	} catch (error) {
		return {
			test: testName,
			config: testConfig.name,
			options: options,
			error: error.message
		};
	}
}

/**
 * Test 1: Workers Performance (from PERFORMANCE_TUNING.md)
 */
async function testWorkerPerformance() {
	console.log(`${colors.bright}${colors.blue}TEST 1: Workers Performance (Parallel HTTP Requests)${colors.reset}`);
	console.log('Testing how worker count affects throughput with realistic data...\n');

	const workerCounts = [10, 25, 50, 75, 100];
	const testData = TEST_DATA.tiny_json;  // Use tiny events for worker testing

	const testResults = [];
	for (const workers of workerCounts) {
		process.stdout.write(`  Workers: ${workers.toString().padEnd(3)} `);

		const result = await runBenchmark(
			`workers_${workers}`,
			testData,
			{ workers, highWater: workers * 10 } // Auto-calculate highWater as per docs
		);

		if (result.metrics) {
			const { eps, mbps, rps, memoryUsedMB, duration } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.yellow}${rps.toFixed(1).padStart(5)} rps${colors.reset} | ` +
				`${colors.magenta}${memoryUsedMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(1)}s${colors.reset}`
			);
			testResults.push({ workers, ...result.metrics });
		} else if (result.error) {
			console.log(`${colors.red}Failed: ${result.error}${colors.reset}`);
		}

		results.tests.push(result);
	}

	// Find optimal workers
	const validResults = testResults.filter(r => r && r.eps);
	if (validResults.length > 0) {
		const optimal = validResults.reduce((best, current) =>
			current.eps > best.eps ? current : best, validResults[0]);
		console.log(`  ${colors.bright}→ Optimal: ${optimal.workers} workers${colors.reset}\n`);
	} else {
		console.log(`  ${colors.red}✗ No successful test runs${colors.reset}\n`);
	}
}

/**
 * Test 2: HighWater Buffer Size (from PERFORMANCE_TUNING.md)
 */
async function testHighWaterPerformance() {
	console.log(`${colors.bright}${colors.blue}TEST 2: HighWater Buffer Size (Stream Buffering)${colors.reset}`);
	console.log('Testing how buffer size affects throughput and memory with realistic data...\n');

	const highWaterValues = [100, 250, 500, 750, 1000];
	const testData = TEST_DATA.tiny_json;  // Use tiny events for buffer testing
	const fixedWorkers = 50;

	const testResults = [];
	for (const highWater of highWaterValues) {
		process.stdout.write(`  HighWater: ${highWater.toString().padEnd(4)} `);

		const result = await runBenchmark(
			`highwater_${highWater}`,
			testData,
			{ workers: fixedWorkers, highWater }
		);

		if (result.metrics) {
			const { eps, mbps, memoryUsedMB, duration } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.magenta}${memoryUsedMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(2)}s${colors.reset}`
			);
			testResults.push({ highWater, ...result.metrics });
		} else if (result.error) {
			console.log(`${colors.red}Failed: ${result.error}${colors.reset}`);
		}

		results.tests.push(result);
	}

	// Find optimal highWater
	const validResults = testResults.filter(r => r && r.eps);
	if (validResults.length > 0) {
		const optimal = validResults.reduce((best, current) =>
			current.eps > best.eps ? current : best, validResults[0]);
		console.log(`  ${colors.bright}→ Optimal: ${optimal.highWater} buffer size${colors.reset}\n`);
	} else {
		console.log(`  ${colors.red}✗ No successful test runs${colors.reset}\n`);
	}
}

/**
 * Test 3: Event Size Impact (from PERFORMANCE_TUNING.md scenarios)
 */
async function testEventSizeImpact() {
	console.log(`${colors.bright}${colors.blue}TEST 3: Event Size Impact (As per PERFORMANCE_TUNING.md)${colors.reset}`);
	console.log('Testing optimal configurations for different event sizes with real data...');
	console.log(`${colors.dim}(Note: Each test uses specific workers/highwater optimized for that event size)${colors.reset}\n`);

	// Configurations optimized for real data sizes
	const scenarios = [
		{
			name: 'Tiny Events JSON (~100B)',
			data: TEST_DATA.tiny_json,
			config: { workers: 50, highWater: 500, recordsPerBatch: 2000 }
		},
		{
			name: 'Dense Events JSON (~3KB)',
			data: TEST_DATA.dense_json,
			config: { workers: 25, highWater: 250, recordsPerBatch: 500 }
		},
		{
			name: 'Dense Events Parquet (~4KB)',
			data: TEST_DATA.dense_parquet,
			config: { workers: 25, highWater: 250, recordsPerBatch: 500 }
		}
	];

	for (const scenario of scenarios) {
		const configStr = `(w:${scenario.config.workers} h:${scenario.config.highWater})`;
		process.stdout.write(`  ${scenario.name.padEnd(25)} ${colors.dim}${configStr.padEnd(12)}${colors.reset} `);

		const result = await runBenchmark(
			`size_${scenario.name}`,
			scenario.data,
			scenario.config
		);

		if (result.metrics) {
			const { eps, mbps, rps, memoryUsedMB, peakMemoryMB, duration } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.yellow}${rps.toFixed(1).padStart(5)} rps${colors.reset} | ` +
				`${colors.magenta}Peak: ${peakMemoryMB} MB${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(1)}s${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}


/**
 * Test 4: Memory Management Options
 */
async function testMemoryManagement() {
	console.log(`${colors.bright}${colors.blue}TEST 4: Memory Management Options${colors.reset}`);
	console.log('Testing memory management features...');
	console.log(`${colors.dim}(Fixed config: workers=25, highWater=250)${colors.reset}\n`);

	const memoryConfigs = [
		{ name: 'Baseline', config: {} },
		{ name: 'Aggressive GC', config: { aggressiveGC: true } },
		{ name: 'Memory Monitor', config: { memoryMonitor: true } },
		{ name: 'Abridged Mode', config: { abridged: true } }
	];

	const testData = TEST_DATA.dense_json;  // Use dense events to stress memory

	for (const memConfig of memoryConfigs) {
		process.stdout.write(`  ${memConfig.name.padEnd(25)} `);

		const result = await runBenchmark(
			`memory_${memConfig.name}`,
			testData,
			{ workers: 25, highWater: 250, ...memConfig.config }
		);

		if (result.metrics) {
			const { eps, memoryUsedMB, peakMemoryMB, duration } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.magenta}Used: ${memoryUsedMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.yellow}Peak: ${peakMemoryMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(1)}s${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 5: Transform Impact
 */
async function testTransformImpact() {
	console.log(`${colors.bright}${colors.blue}TEST 5: Transform Impact${colors.reset}`);
	console.log('Testing performance impact of data transformations with real workloads...');
	console.log(`${colors.dim}(Fixed config: workers=50, highWater=500)${colors.reset}\n`);

	const testData = TEST_DATA.tiny_json;  // Use tiny events for transform testing

	const transformConfigs = [
		{
			name: 'No Transforms (baseline)',
			config: {
				fastMode: true  // This skips ALL transforms
			}
		},
		{
			name: 'Basic Transforms',
			config: {
				fixData: true,
				fixTime: true,
				removeNulls: false,
				flattenData: false
			}
		},
		{
			name: 'Heavy Transforms',
			config: {
				fixData: true,
				fixTime: true,
				removeNulls: true,
				flattenData: true,
				addToken: true,
				dedupe: true,
				tags: { source: 'benchmark', version: '3.1.1', env: 'prod' },
				aliases: { user: 'distinct_id', timestamp: 'time', userId: '$user_id' }
			}
		},
		{
			name: 'Custom Transform Function',
			config: {
				fixData: true,
				transformFunc: (record) => {
					// Simulate real transform workload
					if (record.properties) {
						// Add computed properties
						record.properties.processed_at = Date.now();
						record.properties.event_hash = record.event ? record.event.length * 31 : 0;
						// Remove sensitive data
						delete record.properties.ssn;
						delete record.properties.credit_card;
					}
					return record;
				}
			}
		}
	];

	for (const transform of transformConfigs) {
		process.stdout.write(`  ${transform.name.padEnd(30)} `);

		const result = await runBenchmark(
			`transform_${transform.name}`,
			testData,
			{ workers: 50, highWater: 500, ...transform.config }
		);

		if (result.metrics) {
			const { eps, mbps, duration, memoryUsedMB } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.magenta}${memoryUsedMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(2)}s${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}

/**
 * Generate summary report
 */
function generateSummary() {
	console.log(`${colors.bright}${colors.magenta}═══════════════════════════════════════════════════════════════${colors.reset}`);
	console.log(`${colors.bright}${colors.magenta}                    BENCHMARK SUMMARY v3.1.1                   ${colors.reset}`);
	console.log(`${colors.bright}${colors.magenta}═══════════════════════════════════════════════════════════════${colors.reset}\n`);

	// Key metrics summary
	const workerTests = results.tests.filter(t => t.test && t.test.startsWith('workers_'));
	const highWaterTests = results.tests.filter(t => t.test && t.test.startsWith('highwater_'));

	const bestWorkers = workerTests
		.filter(t => t.metrics)
		.reduce((best, current) =>
			(current.metrics.eps > (best?.metrics?.eps || 0)) ? current : best, null);

	const bestHighWater = highWaterTests
		.filter(t => t.metrics)
		.reduce((best, current) =>
			(current.metrics.eps > (best?.metrics?.eps || 0)) ? current : best, null);

	console.log(`${colors.bright}Key Performance Findings:${colors.reset}`);
	console.log(`  • Optimal Workers: ${colors.green}${bestWorkers?.options?.workers || 'N/A'}${colors.reset} (${bestWorkers?.metrics?.eps?.toLocaleString() || 'N/A'} eps)`);
	console.log(`  • Optimal HighWater: ${colors.green}${bestHighWater?.options?.highWater || 'N/A'}${colors.reset} (${bestHighWater?.metrics?.eps?.toLocaleString() || 'N/A'} eps)`);

	// Performance by event size
	console.log(`\n${colors.bright}Performance by Event Size:${colors.reset}`);
	const sizeTests = results.tests.filter(t => t.test && t.test.startsWith('size_'));
	for (const test of sizeTests) {
		if (test.metrics) {
			const name = test.test.replace('size_', '').padEnd(25);
			console.log(
				`  • ${name} ` +
				`${colors.green}${test.metrics.eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${test.metrics.mbps.toFixed(1)} MB/s${colors.reset} | ` +
				`${colors.yellow}${test.metrics.memoryUsedMB} MB${colors.reset}`
			);
		}
	}

	// Memory efficiency
	console.log(`\n${colors.bright}Memory Management Impact:${colors.reset}`);
	const memTests = results.tests.filter(t => t.test && t.test.startsWith('memory_'));
	const baseline = memTests.find(t => t.test.includes('Baseline'));
	const aggressive = memTests.find(t => t.test.includes('Aggressive'));

	if (baseline && aggressive && baseline.metrics && aggressive.metrics) {
		const savings = baseline.metrics.peakMemoryMB - aggressive.metrics.peakMemoryMB;
		const percent = (savings / baseline.metrics.peakMemoryMB * 100).toFixed(1);
		console.log(`  • Aggressive GC saves: ${colors.green}${savings} MB (${percent}%)${colors.reset}`);
	}


	console.log(`\n${colors.bright}System Info:${colors.reset}`);
	console.log(`  • Node.js: ${results.system.node}`);
	console.log(`  • Platform: ${results.system.platform} (${results.system.arch})`);
	console.log(`  • Memory: ${results.system.memory}`);
	console.log(`  • CPU Cores: ${results.system.cores}`);
}

/**
 * Save results to file
 */
function saveResults() {
	const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
	const resultsDir = './benchmarks/results';

	if (!fs.existsSync(resultsDir)) {
		fs.mkdirSync(resultsDir, { recursive: true });
	}

	const filename = `${resultsDir}/benchmark-${timestamp}.json`;
	fs.writeFileSync(filename, JSON.stringify(results, null, 2));

	console.log(`\n${colors.dim}Results saved to: ${filename}${colors.reset}`);
}

/**
 * Main benchmark runner
 */
async function main() {
	console.clear();
	console.log(`${colors.bright}${colors.cyan}╔═══════════════════════════════════════════════════════════════╗${colors.reset}`);
	console.log(`${colors.bright}${colors.cyan}║     MIXPANEL IMPORT v3.1.1 PERFORMANCE BENCHMARK SUITE        ║${colors.reset}`);
	console.log(`${colors.bright}${colors.cyan}║       Using Realistic Production Data & Workloads            ║${colors.reset}`);
	console.log(`${colors.bright}${colors.cyan}╚═══════════════════════════════════════════════════════════════╝${colors.reset}\n`);

	try {
		// Generate test data if needed
		await generateTestData();

		// Run all benchmarks
		await testWorkerPerformance();
		await testHighWaterPerformance();
		await testEventSizeImpact();
		await testMemoryManagement();
		await testTransformImpact();

		// Generate and display summary
		generateSummary();

		// Save results
		saveResults();

		console.log(`\n${colors.green}${colors.bright}✓ Benchmark complete!${colors.reset}`);
		console.log(`${colors.dim}Results based on realistic production workloads with actual transform processing${colors.reset}\n`);

	} catch (error) {
		console.error(`\n${colors.red}Benchmark failed: ${error.message}${colors.reset}`);
		console.error(error.stack);
		process.exit(1);
	}
}

// Run if executed directly
if (require.main === module) {
	main();
}

module.exports = { runBenchmark, generateTestData };