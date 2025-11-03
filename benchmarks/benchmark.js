#!/usr/bin/env node
/**
 * Comprehensive Benchmark Suite for Mixpanel Import
 * Tests all performance optimizations made to the pipeline
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

// Test configuration
const TEST_DATA = {
	tiny: {
		name: 'Tiny Events (250 bytes)',
		file: './benchmarks/testData/tiny-events-10k.jsonl',
		avgSize: 250,
		count: 10000
	},
	small: {
		name: 'Small Events (1KB)',
		file: './benchmarks/testData/small-events-10k.jsonl',
		avgSize: 1024,
		count: 10000
	},
	medium: {
		name: 'Medium Events (5KB)',
		file: './benchmarks/testData/medium-events-10k.jsonl',
		avgSize: 5120,
		count: 10000
	},
	dense: {
		name: 'Dense Events (11KB)',
		file: './benchmarks/testData/dense-events-10k.jsonl',
		avgSize: 11264,
		count: 10000
	}
};

// Benchmark results storage
const results = {
	timestamp: new Date().toISOString(),
	system: {
		node: process.version,
		platform: process.platform,
		arch: process.arch,
		memory: Math.round(require('os').totalmem() / 1024 / 1024 / 1024) + 'GB'
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
					test_type: key
				}
			};

			// Add padding to reach target size
			const baseSize = JSON.stringify(event).length;
			const targetSize = config.avgSize;
			if (baseSize < targetSize) {
				event.properties.padding = 'x'.repeat(targetSize - baseSize - 20);
			}

			// Add variable properties for dense events
			if (key === 'dense') {
				for (let j = 0; j < 50; j++) {
					event.properties[`prop_${j}`] = `value_${j}_${Math.random().toString(36).substring(7)}`;
				}
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
				streamFormat: 'jsonl'
			}
		);

		const endTime = performance.now();
		const endMemory = process.memoryUsage();
		const duration = (endTime - startTime) / 1000;

		const result = {
			test: testName,
			config: testConfig.name,
			options: options,
			duration: duration,
			eventsPerSecond: Math.round(testConfig.count / duration),
			memoryUsed: Math.round((endMemory.heapUsed - startMemory.heapUsed) / 1024 / 1024),
			success: job.success || testConfig.count,
			failed: job.failed || 0,
			batches: job.batches || 0
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
 * Test 1: Worker Count Performance
 */
async function testWorkerPerformance() {
	console.log(`${colors.bright}${colors.blue}TEST 1: Worker Count Performance${colors.reset}`);
	console.log('Testing how worker count affects throughput...\n');

	const workerCounts = [1, 5, 10, 20, 30];
	const testData = TEST_DATA.small; // Use small events for consistent testing

	for (const workers of workerCounts) {
		process.stdout.write(`  Workers: ${workers.toString().padEnd(3)} `);

		const result = await runBenchmark(
			`workers_${workers}`,
			testData,
			{ workers, transport: 'undici' }
		);

		const eps = result.eventsPerSecond || 0;
		const bar = '█'.repeat(Math.floor(eps / 500));
		console.log(`${bar} ${colors.green}${eps.toLocaleString()} eps${colors.reset} (${result.duration.toFixed(2)}s)`);

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 2: Event Density Impact
 */
async function testEventDensity() {
	console.log(`${colors.bright}${colors.blue}TEST 2: Event Density Impact${colors.reset}`);
	console.log('Testing performance with different event sizes...\n');

	for (const [key, testData] of Object.entries(TEST_DATA)) {
		process.stdout.write(`  ${testData.name.padEnd(25)} `);

		const result = await runBenchmark(
			`density_${key}`,
			testData,
			{ workers: 10, transport: 'undici' }
		);

		const eps = result.eventsPerSecond || 0;
		const memMB = result.memoryUsed || 0;
		console.log(
			`${colors.green}${eps.toLocaleString().padStart(7)} eps${colors.reset}` +
			` | ${colors.yellow}${memMB.toString().padStart(4)} MB${colors.reset}` +
			` | ${result.duration.toFixed(2)}s`
		);

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 3: Transport Comparison (got vs undici)
 */
async function testTransportComparison() {
	console.log(`${colors.bright}${colors.blue}TEST 3: Transport Comparison${colors.reset}`);
	console.log('Comparing got vs undici performance...\n');

	const transports = ['got', 'undici'];
	const testData = TEST_DATA.medium;

	for (const transport of transports) {
		process.stdout.write(`  ${transport.padEnd(10)} `);

		const result = await runBenchmark(
			`transport_${transport}`,
			testData,
			{ workers: 10, transport }
		);

		const eps = result.eventsPerSecond || 0;
		const improvement = transport === 'undici' ? ' ← default' : '';
		console.log(
			`${colors.green}${eps.toLocaleString().padStart(7)} eps${colors.reset}` +
			` (${result.duration.toFixed(2)}s)${colors.cyan}${improvement}${colors.reset}`
		);

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 4: Adaptive Scaling
 */
async function testAdaptiveScaling() {
	console.log(`${colors.bright}${colors.blue}TEST 4: Adaptive Scaling${colors.reset}`);
	console.log('Testing adaptive vs fixed configuration...\n');

	const testCases = [
		{ name: 'Fixed (30 workers)', options: { workers: 30, adaptive: false } },
		{ name: 'Adaptive', options: { workers: 30, adaptive: true } },
		{ name: 'Adaptive + Hint', options: { workers: 30, adaptive: true, avgEventSize: 11264 } }
	];

	const testData = TEST_DATA.dense; // Use dense events to test adaptation

	for (const testCase of testCases) {
		process.stdout.write(`  ${testCase.name.padEnd(25)} `);

		const result = await runBenchmark(
			`adaptive_${testCase.name}`,
			testData,
			{ ...testCase.options, transport: 'undici' }
		);

		const eps = result.eventsPerSecond || 0;
		const memMB = result.memoryUsed || 0;
		console.log(
			`${colors.green}${eps.toLocaleString().padStart(7)} eps${colors.reset}` +
			` | ${colors.yellow}${memMB.toString().padStart(4)} MB${colors.reset}`
		);

		results.tests.push(result);
	}
	console.log();
}

/**
 * Test 5: Batching Efficiency
 */
async function testBatchingEfficiency() {
	console.log(`${colors.bright}${colors.blue}TEST 5: Batching Efficiency${colors.reset}`);
	console.log('Testing batching with different configurations...\n');

	const batchConfigs = [
		{ name: 'Small batches (500)', recordsPerBatch: 500 },
		{ name: 'Default (2000)', recordsPerBatch: 2000 },
		{ name: 'Size-optimized', recordsPerBatch: 2000, bytesPerBatch: 10 * 1024 * 1024 }
	];

	const testData = TEST_DATA.medium;

	for (const config of batchConfigs) {
		process.stdout.write(`  ${config.name.padEnd(25)} `);

		const result = await runBenchmark(
			`batching_${config.name}`,
			testData,
			{ workers: 10, transport: 'undici', ...config }
		);

		const eps = result.eventsPerSecond || 0;
		const batches = result.batches || 0;
		console.log(
			`${colors.green}${eps.toLocaleString().padStart(7)} eps${colors.reset}` +
			` | ${colors.cyan}${batches} batches${colors.reset}`
		);

		results.tests.push(result);
	}
	console.log();
}

/**
 * Generate summary report
 */
function generateSummary() {
	console.log(`${colors.bright}${colors.magenta}═══════════════════════════════════════════════════════════════${colors.reset}`);
	console.log(`${colors.bright}${colors.magenta}                       BENCHMARK SUMMARY                       ${colors.reset}`);
	console.log(`${colors.bright}${colors.magenta}═══════════════════════════════════════════════════════════════${colors.reset}\n`);

	// Find best configurations
	const workerTests = results.tests.filter(t => t.test && t.test.startsWith('workers_'));
	const bestWorkers = workerTests.reduce((best, current) =>
		(current.eventsPerSecond > (best?.eventsPerSecond || 0)) ? current : best, null);

	const transportTests = results.tests.filter(t => t.test && t.test.startsWith('transport_'));
	const undiciBenefit = transportTests.find(t => t.test === 'transport_undici');
	const gotPerf = transportTests.find(t => t.test === 'transport_got');

	const adaptiveTests = results.tests.filter(t => t.test && t.test.startsWith('adaptive_'));
	const adaptiveBenefit = adaptiveTests.find(t => t.test.includes('Adaptive'));

	console.log(`${colors.bright}Key Findings:${colors.reset}`);
	console.log(`  • Optimal workers: ${colors.green}${bestWorkers?.options?.workers || 'N/A'}${colors.reset} (${bestWorkers?.eventsPerSecond?.toLocaleString() || 'N/A'} eps)`);

	if (undiciBenefit && gotPerf) {
		const improvement = ((undiciBenefit.eventsPerSecond - gotPerf.eventsPerSecond) / gotPerf.eventsPerSecond * 100).toFixed(1);
		console.log(`  • Undici vs Got: ${colors.green}+${improvement}%${colors.reset} performance improvement`);
	}

	if (adaptiveBenefit) {
		console.log(`  • Adaptive scaling: ${colors.green}Prevents OOM${colors.reset} for dense events`);
	}

	// Memory efficiency
	const densityTests = results.tests.filter(t => t.test && t.test.startsWith('density_'));
	const memoryPerKB = densityTests.map(t => ({
		size: TEST_DATA[t.test.split('_')[1]]?.avgSize || 0,
		memory: t.memoryUsed
	}));

	console.log(`\n${colors.bright}Performance by Event Size:${colors.reset}`);
	for (const test of densityTests) {
		const key = test.test.split('_')[1];
		const data = TEST_DATA[key];
		if (data) {
			console.log(`  • ${data.name.padEnd(25)} ${colors.green}${test.eventsPerSecond.toLocaleString().padStart(7)} eps${colors.reset} | ${colors.yellow}${test.memoryUsed}MB${colors.reset}`);
		}
	}

	console.log(`\n${colors.bright}System Info:${colors.reset}`);
	console.log(`  • Node.js: ${results.system.node}`);
	console.log(`  • Platform: ${results.system.platform} (${results.system.arch})`);
	console.log(`  • Memory: ${results.system.memory}`);
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
	console.log(`${colors.bright}${colors.cyan}║          MIXPANEL IMPORT PERFORMANCE BENCHMARK SUITE          ║${colors.reset}`);
	console.log(`${colors.bright}${colors.cyan}╚═══════════════════════════════════════════════════════════════╝${colors.reset}\n`);

	try {
		// Generate test data if needed
		await generateTestData();

		// Run all benchmarks
		await testWorkerPerformance();
		await testEventDensity();
		await testTransportComparison();
		await testAdaptiveScaling();
		await testBatchingEfficiency();

		// Generate and display summary
		generateSummary();

		// Save results
		saveResults();

		console.log(`\n${colors.green}${colors.bright}✓ Benchmark complete!${colors.reset}\n`);

	} catch (error) {
		console.error(`\n${colors.red}Benchmark failed: ${error.message}${colors.reset}`);
		process.exit(1);
	}
}

// Run if executed directly
if (require.main === module) {
	main();
}

module.exports = { runBenchmark, generateTestData };