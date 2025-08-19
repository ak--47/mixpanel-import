//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
import { writeFileSync, existsSync, mkdirSync } from "fs";
import path from "path";

const require = createRequire(import.meta.url);
const u = require('../../node_modules/ak-tools');

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

// Import all benchmark modules
import workerOptimization from './workerOptimization.mjs';
import parameterMatrix from './parameterMatrix.mjs';
import formatComparison from './formatComparison.mjs';
import transformImpact from './transformImpact.mjs';
import transportComparison from './transportComparison.mjs';
import memoryVsStream from './memoryVsStream.mjs';

/**
 * MIXPANEL IMPORT PERFORMANCE BENCHMARK SUITE
 * 
 * This comprehensive benchmarking suite tests all major performance parameters
 * to find optimal configurations for different use cases and data sizes.
 */

const BENCHMARK_SUITES = {
	quick: [
		'workerOptimization',
		'formatComparison'
	],
	standard: [
		'workerOptimization',
		'parameterMatrix', 
		'formatComparison',
		'transportComparison'
	],
	comprehensive: [
		'workerOptimization',
		'parameterMatrix',
		'formatComparison', 
		'transformImpact',
		'transportComparison',
		'memoryVsStream'
	]
};

const DATA_SIZES = {
	small: '../testData/dnd250.ndjson',     // 250k records, ~83MB
	large: '../testData/one-two-million.ndjson'  // 1-2M records, ~618MB
};

class BenchmarkRunner {
	constructor(options = {}) {
		this.suite = options.suite || 'standard';
		this.dataSize = options.dataSize || 'small';
		this.dryRun = options.dryRun !== false; // default to true for safety
		this.outputDir = options.outputDir || '../results';
		this.timestamp = new Date().toISOString().replace(/[:.]/g, '-');
		this.results = [];
		
		// Check credentials unless running dry run
		if (!this.dryRun) {
			this.credentials = checkCredentials();
		}
		
		// Ensure output directory exists
		if (!existsSync(this.outputDir)) {
			mkdirSync(this.outputDir, { recursive: true });
		}
	}

	async run() {
		console.log('🚀 MIXPANEL IMPORT PERFORMANCE BENCHMARK SUITE');
		console.log('='.repeat(60));
		console.log(`📊 Suite: ${this.suite.toUpperCase()}`);
		console.log(`📁 Data Size: ${this.dataSize} (${DATA_SIZES[this.dataSize]})`);
		console.log(`🔬 Dry Run: ${this.dryRun ? 'YES (no actual API calls)' : 'NO (live API calls)'}`);
		console.log(`📂 Output: ${this.outputDir}`);
		console.log('='.repeat(60));
		console.log('');

		const benchmarksToRun = BENCHMARK_SUITES[this.suite];
		const totalBenchmarks = benchmarksToRun.length;
		
		for (let i = 0; i < benchmarksToRun.length; i++) {
			const benchmarkName = benchmarksToRun[i];
			
			console.log(`[${i + 1}/${totalBenchmarks}] Running ${benchmarkName}...`);
			console.log('-'.repeat(40));
			
			try {
				const startTime = Date.now();
				const result = await this.runBenchmark(benchmarkName);
				const duration = Date.now() - startTime;
				
				result.metadata = {
					benchmarkName,
					dataSize: this.dataSize,
					dataFile: DATA_SIZES[this.dataSize],
					dryRun: this.dryRun,
					duration: duration,
					timestamp: new Date().toISOString()
				};
				
				this.results.push(result);
				
				console.log(`✅ ${benchmarkName} completed in ${u.time(duration)}`);
				console.log('');
				
			} catch (error) {
				console.error(`❌ ${benchmarkName} failed:`, error.message);
				this.results.push({
					metadata: { benchmarkName, error: error.message },
					error: true
				});
			}
		}

		await this.generateReport();
		console.log('🎉 Benchmark suite completed!');
		console.log(`📊 View results: ${path.join(this.outputDir, `benchmark-${this.timestamp}.json`)}`);
	}

	async runBenchmark(benchmarkName) {
		const config = {
			dataFile: DATA_SIZES[this.dataSize],
			dryRun: this.dryRun,
			credentials: this.credentials
		};

		switch (benchmarkName) {
			case 'workerOptimization':
				return await workerOptimization(config);
			case 'parameterMatrix':
				return await parameterMatrix(config);
			case 'formatComparison':
				return await formatComparison(config);
			case 'transformImpact':
				return await transformImpact(config);
			case 'transportComparison':
				return await transportComparison(config);
			case 'memoryVsStream':
				return await memoryVsStream(config);
			default:
				throw new Error(`Unknown benchmark: ${benchmarkName}`);
		}
	}

	async generateReport() {
		const report = {
			summary: {
				suite: this.suite,
				dataSize: this.dataSize,
				dryRun: this.dryRun,
				timestamp: this.timestamp,
				totalBenchmarks: this.results.length,
				successfulBenchmarks: this.results.filter(r => !r.error).length,
				failedBenchmarks: this.results.filter(r => r.error).length
			},
			results: this.results,
			recommendations: this.generateRecommendations()
		};

		// Save detailed JSON report
		const jsonPath = path.join(this.outputDir, `benchmark-${this.timestamp}.json`);
		writeFileSync(jsonPath, JSON.stringify(report, null, 2));

		// Generate summary report
		this.generateSummaryReport(report);
	}

	generateRecommendations() {
		const recommendations = {
			optimal: {},
			notes: []
		};

		// Analyze worker optimization results
		const workerResult = this.results.find(r => r.metadata?.benchmarkName === 'workerOptimization');
		if (workerResult && !workerResult.error) {
			const optimalWorkers = this.findOptimalWorkers(workerResult);
			if (optimalWorkers) {
				recommendations.optimal.workers = optimalWorkers;
				recommendations.notes.push(`Optimal worker count: ${optimalWorkers} (best EPS performance)`);
			}
		}

		// Analyze format performance
		const formatResult = this.results.find(r => r.metadata?.benchmarkName === 'formatComparison');
		if (formatResult && !formatResult.error) {
			const fastestFormat = this.findFastestFormat(formatResult);
			if (fastestFormat) {
				recommendations.optimal.streamFormat = fastestFormat;
				recommendations.notes.push(`Fastest format: ${fastestFormat}`);
			}
		}

		// Analyze transport performance
		const transportResult = this.results.find(r => r.metadata?.benchmarkName === 'transportComparison');
		if (transportResult && !transportResult.error) {
			const bestTransport = this.findBestTransport(transportResult);
			if (bestTransport) {
				recommendations.optimal.transport = bestTransport;
				recommendations.notes.push(`Best transport: ${bestTransport}`);
			}
		}

		return recommendations;
	}

	findOptimalWorkers(result) {
		if (!result.tests || !Array.isArray(result.tests)) return null;
		
		// Find the configuration with highest EPS
		const sorted = result.tests
			.filter(t => t.result && typeof t.result.eps === 'number')
			.sort((a, b) => b.result.eps - a.result.eps);
		
		return sorted.length > 0 ? sorted[0].config.workers : null;
	}

	findFastestFormat(result) {
		if (!result.tests || !Array.isArray(result.tests)) return null;
		
		// Find the format with highest EPS
		const sorted = result.tests
			.filter(t => t.result && typeof t.result.eps === 'number')
			.sort((a, b) => b.result.eps - a.result.eps);
		
		return sorted.length > 0 ? sorted[0].config.streamFormat : null;
	}

	findBestTransport(result) {
		if (!result.tests || !Array.isArray(result.tests)) return null;
		
		// Find the transport with highest EPS
		const sorted = result.tests
			.filter(t => t.result && typeof t.result.eps === 'number')
			.sort((a, b) => b.result.eps - a.result.eps);
		
		return sorted.length > 0 ? sorted[0].config.transport : null;
	}

	generateSummaryReport(report) {
		const summaryPath = path.join(this.outputDir, `benchmark-summary-${this.timestamp}.txt`);
		
		let summary = `MIXPANEL IMPORT BENCHMARK SUMMARY\n`;
		summary += `${'='.repeat(50)}\n\n`;
		summary += `Suite: ${report.summary.suite}\n`;
		summary += `Data Size: ${report.summary.dataSize}\n`;
		summary += `Dry Run: ${report.summary.dryRun}\n`;
		summary += `Timestamp: ${report.summary.timestamp}\n`;
		summary += `Success Rate: ${report.summary.successfulBenchmarks}/${report.summary.totalBenchmarks}\n\n`;

		// Add recommendations
		if (report.recommendations.notes.length > 0) {
			summary += `RECOMMENDATIONS:\n`;
			summary += `${'-'.repeat(20)}\n`;
			report.recommendations.notes.forEach(note => {
				summary += `• ${note}\n`;
			});
			summary += `\n`;
		}

		// Add performance highlights from each benchmark
		summary += `PERFORMANCE HIGHLIGHTS:\n`;
		summary += `${'-'.repeat(25)}\n`;
		
		report.results.forEach(result => {
			if (!result.error && result.metadata) {
				summary += `\n${result.metadata.benchmarkName.toUpperCase()}:\n`;
				
				if (result.bestResult) {
					const best = result.bestResult;
					summary += `  Best EPS: ${Math.round(best.eps || 0).toLocaleString()}\n`;
					summary += `  Best RPS: ${Math.round(best.rps || 0).toLocaleString()}\n`;
					summary += `  Duration: ${u.time(best.duration || 0)}\n`;
					if (best.config) {
						const configStr = Object.entries(best.config)
							.map(([k, v]) => `${k}=${v}`)
							.join(', ');
						summary += `  Config: ${configStr}\n`;
					}
				}
			}
		});

		writeFileSync(summaryPath, summary);
		console.log(`📋 Summary report: ${summaryPath}`);
	}
}

// CLI interface
async function main() {
	const args = process.argv.slice(2);
	const options = {};

	// Parse command line arguments
	for (let i = 0; i < args.length; i++) {
		const arg = args[i];
		if (arg === '--suite' && args[i + 1]) {
			options.suite = args[i + 1];
			i++;
		} else if (arg === '--size' && args[i + 1]) {
			options.dataSize = args[i + 1];
			i++;
		} else if (arg === '--live') {
			options.dryRun = false;
		} else if (arg === '--output' && args[i + 1]) {
			options.outputDir = args[i + 1];
			i++;
		} else if (arg === '--help') {
			console.log(`
Usage: node benchmarks/index.mjs [options]

Options:
  --suite <type>     Benchmark suite to run (quick|standard|comprehensive) [default: standard]
  --size <size>      Data size to test (small|large) [default: small]
  --live             Use live API calls instead of dry run [default: false]
  --output <dir>     Output directory for results [default: ./benchmarks/results]
  --help             Show this help message

Examples:
  node benchmarks/index.mjs                           # Run standard suite with small data (dry run)
  node benchmarks/index.mjs --suite comprehensive    # Run all benchmarks
  node benchmarks/index.mjs --size large --live      # Test with large data and live API
			`);
			return;
		}
	}

	const runner = new BenchmarkRunner(options);
	await runner.run();
}

// Export for programmatic use
export { BenchmarkRunner, DATA_SIZES, BENCHMARK_SUITES };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	main().catch(console.error);
}