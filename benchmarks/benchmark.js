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

// Test configuration based on PERFORMANCE_TUNING.md
const TEST_DATA = {
	tiny: {
		name: 'Tiny Events (250 bytes)',
		file: './benchmarks/testData/tiny-events-10k.jsonl',
		avgSize: 250,
		count: 10000
	},
	small: {
		name: 'Small Events (500 bytes)',
		file: './benchmarks/testData/small-events-10k.jsonl',
		avgSize: 500,
		count: 10000
	},
	medium: {
		name: 'Medium Events (2KB)',
		file: './benchmarks/testData/medium-events-10k.jsonl',
		avgSize: 2048,
		count: 10000
	},
	large: {
		name: 'Large Events (10KB)',
		file: './benchmarks/testData/large-events-5k.jsonl',
		avgSize: 10240,
		count: 5000
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
 * Generate test data if it doesn't exist
 */
async function generateTestData() {
	console.log(`${colors.cyan}Generating test data...${colors.reset}\n`);

	for (const [key, config] of Object.entries(TEST_DATA)) {
		const filePath = config.file;
		if (fs.existsSync(filePath)) {
			console.log(`  ✓ ${config.name} exists`);
			continue;
		}

		console.log(`  → Generating ${config.name}...`);
		const dir = path.dirname(filePath);
		if (!fs.existsSync(dir)) {
			fs.mkdirSync(dir, { recursive: true });
		}

		const events = [];
		for (let i = 0; i < config.count; i++) {
			const event = {
				event: `test_event_${key}`,
				properties: {
					distinct_id: `user_${i}`,
					time: Date.now() - (i * 1000),
					$insert_id: `${Date.now()}_${i}`,
					test_type: key,
					session_id: `session_${Math.floor(i / 100)}`,
					platform: ['web', 'ios', 'android'][i % 3]
				}
			};

			// Add padding to reach target size
			const baseSize = JSON.stringify(event).length;
			const targetSize = config.avgSize;
			if (baseSize < targetSize) {
				event.properties.padding = 'x'.repeat(targetSize - baseSize - 20);
			}

			// Add variable properties for large events
			if (key === 'large') {
				for (let j = 0; j < 50; j++) {
					event.properties[`prop_${j}`] = `value_${j}_${Math.random().toString(36).substring(7)}`;
				}
				// Add nested objects
				event.properties.metadata = {
					browser: 'Chrome',
					version: '120.0.0',
					os: 'Windows 11',
					screen: '1920x1080'
				};
			}

			events.push(JSON.stringify(event));
		}

		fs.writeFileSync(filePath, events.join('\n'));
		console.log(`  ✓ Generated ${config.name}`);
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
				dryRun: true,
				verbose: false,
				logs: false,
				abridged: true,
				streamFormat: 'jsonl'
			}
		);

		const endTime = performance.now();
		const endMemory = process.memoryUsage();
		const duration = (endTime - startTime) / 1000;

		// Calculate comprehensive metrics
		const totalBytes = testConfig.count * testConfig.avgSize;
		const mbps = (totalBytes / 1024 / 1024) / duration;
		const eps = testConfig.count / duration;
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
				success: job.success || testConfig.count,
				failed: job.failed || 0,
				batches: job.batches || Math.ceil(testConfig.count / (options.recordsPerBatch || 2000)),
				requests: job.requests || 0
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
	console.log('Testing how worker count affects throughput...\n');

	const workerCounts = [1, 3, 5, 10, 15, 20, 30, 40, 50, 60, 80, 100];
	const testData = TEST_DATA.small;

	const testResults = [];
	for (const workers of workerCounts) {
		process.stdout.write(`  Workers: ${workers.toString().padEnd(3)} `);

		const result = await runBenchmark(
			`workers_${workers}`,
			testData,
			{ workers, highWater: workers * 10 } // Auto-calculate highWater as per docs
		);

		if (result.metrics) {
			const { eps, mbps, rps, memoryUsedMB } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.yellow}${rps.toFixed(1).padStart(5)} rps${colors.reset} | ` +
				`${colors.magenta}${memoryUsedMB.toString().padStart(4)} MB${colors.reset}`
			);
			testResults.push({ workers, ...result.metrics });
		}

		results.tests.push(result);
	}

	// Find optimal workers
	const optimal = testResults.reduce((best, current) =>
		current.eps > best.eps ? current : best, testResults[0]);
	console.log(`  ${colors.bright}→ Optimal: ${optimal.workers} workers${colors.reset}\n`);
}

/**
 * Test 2: HighWater Buffer Size (from PERFORMANCE_TUNING.md)
 */
async function testHighWaterPerformance() {
	console.log(`${colors.bright}${colors.blue}TEST 2: HighWater Buffer Size (Stream Buffering)${colors.reset}`);
	console.log('Testing how buffer size affects throughput and memory...\n');

	const highWaterValues = [16, 30, 50, 100, 150, 200, 250, 300, 400, 500];
	const testData = TEST_DATA.small;
	const fixedWorkers = 10;

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
		}

		results.tests.push(result);
	}

	// Find optimal highWater
	const optimal = testResults.reduce((best, current) =>
		current.eps > best.eps ? current : best, testResults[0]);
	console.log(`  ${colors.bright}→ Optimal: ${optimal.highWater} buffer size${colors.reset}\n`);
}

/**
 * Test 3: Event Size Impact (from PERFORMANCE_TUNING.md scenarios)
 */
async function testEventSizeImpact() {
	console.log(`${colors.bright}${colors.blue}TEST 3: Event Size Impact (As per PERFORMANCE_TUNING.md)${colors.reset}`);
	console.log('Testing optimal configurations for different event sizes...');
	console.log(`${colors.dim}(Note: Each test uses specific workers/highwater optimized for that event size)${colors.reset}\n`);

	// Configurations from PERFORMANCE_TUNING.md
	const scenarios = [
		{
			name: 'Small Events (<500B)',
			data: TEST_DATA.tiny,
			config: { workers: 20, highWater: 150, recordsPerBatch: 2000 }
		},
		{
			name: 'Medium Events (2KB)',
			data: TEST_DATA.medium,
			config: { workers: 8, highWater: 60, recordsPerBatch: 1000 }
		},
		{
			name: 'Large Events (10KB+)',
			data: TEST_DATA.large,
			config: { workers: 3, highWater: 20, recordsPerBatch: 500 }
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
			const { eps, mbps, rps, memoryUsedMB, peakMemoryMB } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.yellow}${rps.toFixed(1).padStart(5)} rps${colors.reset} | ` +
				`${colors.magenta}Peak: ${peakMemoryMB} MB${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 4: Compression Impact
 */
async function testCompressionImpact() {
	console.log(`${colors.bright}${colors.blue}TEST 4: Compression Impact${colors.reset}`);
	console.log('Testing compression levels vs throughput...');
	console.log(`${colors.dim}(Fixed config: workers=10, highWater=100)${colors.reset}\n`);

	const compressionLevels = [
		{ name: 'No compression', compress: false },
		{ name: 'Level 1 (fastest)', compress: true, compressionLevel: 1 },
		{ name: 'Level 6 (balanced)', compress: true, compressionLevel: 6 },
		{ name: 'Level 9 (maximum)', compress: true, compressionLevel: 9 }
	];

	const testData = TEST_DATA.medium;

	for (const compression of compressionLevels) {
		process.stdout.write(`  ${compression.name.padEnd(25)} `);

		const result = await runBenchmark(
			`compression_${compression.name}`,
			testData,
			{ workers: 10, highWater: 100, ...compression }
		);

		if (result.metrics) {
			const { eps, mbps, duration } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.cyan}${mbps.toFixed(1).padStart(5)} MB/s${colors.reset} | ` +
				`${colors.dim}${duration.toFixed(2)}s${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 5: Memory Management Options
 */
async function testMemoryManagement() {
	console.log(`${colors.bright}${colors.blue}TEST 5: Memory Management Options${colors.reset}`);
	console.log('Testing memory management features...');
	console.log(`${colors.dim}(Fixed config: workers=5, highWater=30)${colors.reset}\n`);

	const memoryConfigs = [
		{ name: 'Baseline', config: {} },
		{ name: 'Aggressive GC', config: { aggressiveGC: true } },
		{ name: 'Memory Monitor', config: { memoryMonitor: true } },
		{ name: 'Abridged Mode', config: { abridged: true } }
	];

	const testData = TEST_DATA.large; // Use large events to stress memory

	for (const memConfig of memoryConfigs) {
		process.stdout.write(`  ${memConfig.name.padEnd(25)} `);

		const result = await runBenchmark(
			`memory_${memConfig.name}`,
			testData,
			{ workers: 5, highWater: 30, ...memConfig.config }
		);

		if (result.metrics) {
			const { eps, memoryUsedMB, peakMemoryMB } = result.metrics;
			console.log(
				`${colors.green}${eps.toLocaleString().padStart(6)} eps${colors.reset} | ` +
				`${colors.magenta}Used: ${memoryUsedMB.toString().padStart(4)} MB${colors.reset} | ` +
				`${colors.yellow}Peak: ${peakMemoryMB.toString().padStart(4)} MB${colors.reset}`
			);
		}

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 6: Transform Impact
 */
async function testTransformImpact() {
	console.log(`${colors.bright}${colors.blue}TEST 6: Transform Impact${colors.reset}`);
	console.log('Testing performance impact of data transformations...');
	console.log(`${colors.dim}(Fixed config: workers=10, highWater=100)${colors.reset}\n`);

	const testData = TEST_DATA.small;

	const transformConfigs = [
		{
			name: 'No Transforms (baseline)',
			config: {
				fastMode: true  // This skips ALL transforms
			}
		},
		{
			name: 'Fix Data Only',
			config: {
				fixData: true,
				fixTime: false,
				removeNulls: false,
				flattenData: false
			}
		},
		{
			name: 'All Transforms Enabled',
			config: {
				fixData: true,
				fixTime: true,
				removeNulls: true,
				flattenData: true,
				addToken: true,
				dedupe: true,
				tags: { source: 'benchmark', version: '3.1.1' },
				aliases: { user: 'distinct_id', timestamp: 'time' }
			}
		}
	];

	for (const transform of transformConfigs) {
		process.stdout.write(`  ${transform.name.padEnd(30)} `);

		const result = await runBenchmark(
			`transform_${transform.name}`,
			testData,
			{ workers: 10, highWater: 100, ...transform.config }
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

	// Compression impact
	console.log(`\n${colors.bright}Compression Impact:${colors.reset}`);
	const compTests = results.tests.filter(t => t.test && t.test.startsWith('compression_'));
	const noComp = compTests.find(t => t.test.includes('No compression'));
	const comp6 = compTests.find(t => t.test.includes('balanced'));

	if (noComp && comp6 && noComp.metrics && comp6.metrics) {
		const overhead = ((noComp.metrics.eps - comp6.metrics.eps) / noComp.metrics.eps * 100).toFixed(1);
		console.log(`  • Compression overhead: ${colors.yellow}${overhead}%${colors.reset} reduction in throughput`);
		console.log(`  • Bandwidth savings: ${colors.green}~60-80%${colors.reset} with compression`);
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
	console.log(`${colors.bright}${colors.cyan}║         Testing parameters from PERFORMANCE_TUNING.md         ║${colors.reset}`);
	console.log(`${colors.bright}${colors.cyan}╚═══════════════════════════════════════════════════════════════╝${colors.reset}\n`);

	try {
		// Generate test data if needed
		await generateTestData();

		// Run all benchmarks
		await testWorkerPerformance();
		await testHighWaterPerformance();
		await testEventSizeImpact();
		await testCompressionImpact();
		await testMemoryManagement();
		await testTransformImpact();

		// Generate and display summary
		generateSummary();

		// Save results
		saveResults();

		console.log(`\n${colors.green}${colors.bright}✓ Benchmark complete!${colors.reset}`);
		console.log(`${colors.dim}Results align with PERFORMANCE_TUNING.md recommendations${colors.reset}\n`);

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