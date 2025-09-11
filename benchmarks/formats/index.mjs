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

// Import all format benchmark modules
import formatPerformance from './formatPerformance.mjs';
import compressionImpact from './compressionImpact.mjs';
import scalingAnalysis from './scalingAnalysis.mjs';

/**
 * MIXPANEL IMPORT FORMAT BENCHMARK SUITE
 * 
 * This comprehensive benchmarking suite tests all supported data formats
 * to find optimal parsing performance for different scenarios.
 */

const BENCHMARK_SUITES = {
	quick: [
		'formatPerformance'
	],
	standard: [
		'formatPerformance',
		'compressionImpact'
	],
	comprehensive: [
		'formatPerformance',
		'compressionImpact',
		'scalingAnalysis'
	]
};

const DATA_SIZES = {
	small: '250k',
	large: '1m'
};

// All supported format combinations
const FORMAT_DATA = {
	'json': {
		'250k': '../testData/formats/json/json-250k-EVENTS.json',
		'1m': '../testData/formats/json/json-1m-EVENTS.json'
	},
	'json-gz': {
		'250k': '../testData/formats/json-gz/json-gz-250k-EVENTS.json.gz',
		'1m': '../testData/formats/json-gz/json-gz-1m-EVENTS.json.gz'
	},
	'csv': {
		'250k': '../testData/formats/csv/csv-250k-EVENTS.csv',
		'1m': '../testData/formats/csv/csv-1m-EVENTS.csv'
	},
	'csv-gz': {
		'250k': '../testData/formats/csv-gz/csv-gz-250k-EVENTS.csv.gz',
		'1m': '../testData/formats/csv-gz/csv-gz-1m-EVENTS.csv.gz'
	},
	'parquet': {
		'250k': '../testData/formats/parquet/parquet-250k-EVENTS.parquet',
		'1m': '../testData/formats/parquet/parquet-1m-EVENTS.parquet'
	},
	'parquet-gz': {
		'250k': '../testData/formats/parquet-gz/parquet-gz-250k-EVENTS.parquet.gz',
		'1m': '../testData/formats/parquet-gz/parquet-gz-1m-EVENTS.parquet.gz'
	}
};

class FormatBenchmarkRunner {
	constructor(options = {}) {
		this.suite = options.suite || 'standard';
		this.dataSize = options.dataSize || 'small';
		this.outputDir = options.outputDir || '../results/formats';
		this.timestamp = new Date().toISOString().replace(/[:.]/g, '-');
		this.results = [];
		
		// Always check credentials since we never do dry run
		this.credentials = checkCredentials();
		
		// Ensure output directory exists
		if (!existsSync(this.outputDir)) {
			mkdirSync(this.outputDir, { recursive: true });
		}
	}

	async run() {
		console.log('📊 MIXPANEL IMPORT FORMAT BENCHMARK SUITE');
		console.log('='.repeat(60));
		console.log(`📋 Suite: ${this.suite.toUpperCase()}`);
		console.log(`📁 Data Size: ${this.dataSize} (${DATA_SIZES[this.dataSize]} records)`);
		console.log(`🎯 Live API: YES (real performance testing)`);
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
					formatData: FORMAT_DATA,
					liveAPI: true,
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
		console.log('🎉 Format benchmark suite completed!');
		console.log(`📊 View results: ${path.join(this.outputDir, `format-benchmark-${this.timestamp}.json`)}`);
	}

	async runBenchmark(benchmarkName) {
		const config = {
			formatData: FORMAT_DATA,
			dataSize: DATA_SIZES[this.dataSize],
			dryRun: false,
			credentials: this.credentials
		};

		switch (benchmarkName) {
			case 'formatPerformance':
				return await formatPerformance(config);
			case 'compressionImpact':
				return await compressionImpact(config);
			case 'scalingAnalysis':
				return await scalingAnalysis(config);
			default:
				throw new Error(`Unknown benchmark: ${benchmarkName}`);
		}
	}

	async generateReport() {
		const report = {
			summary: {
				suite: this.suite,
				dataSize: this.dataSize,
				liveAPI: true,
				timestamp: this.timestamp,
				totalBenchmarks: this.results.length,
				successfulBenchmarks: this.results.filter(r => !r.error).length,
				failedBenchmarks: this.results.filter(r => r.error).length
			},
			results: this.results,
			recommendations: this.generateRecommendations()
		};

		// Save detailed JSON report
		const jsonPath = path.join(this.outputDir, `format-benchmark-${this.timestamp}.json`);
		writeFileSync(jsonPath, JSON.stringify(report, null, 2));

		// Generate summary report
		this.generateSummaryReport(report);
	}

	generateRecommendations() {
		const recommendations = {
			optimal: {},
			notes: []
		};

		// Analyze format performance results
		const formatResult = this.results.find(r => r.metadata?.benchmarkName === 'formatPerformance');
		if (formatResult && !formatResult.error) {
			const fastestFormat = this.findFastestFormat(formatResult);
			if (fastestFormat) {
				recommendations.optimal.format = fastestFormat;
				recommendations.notes.push(`Fastest format: ${fastestFormat} for ${this.dataSize} datasets`);
			}
		}

		// Analyze compression impact
		const compressionResult = this.results.find(r => r.metadata?.benchmarkName === 'compressionImpact');
		if (compressionResult && !compressionResult.error) {
			const compressionRecommendation = this.analyzeCompressionImpact(compressionResult);
			if (compressionRecommendation) {
				recommendations.notes.push(compressionRecommendation);
			}
		}

		return recommendations;
	}

	findFastestFormat(result) {
		if (!result.tests || !Array.isArray(result.tests)) return null;
		
		// Find the format with highest EPS
		const sorted = result.tests
			.filter(t => t.result && typeof t.result.eps === 'number')
			.sort((a, b) => b.result.eps - a.result.eps);
		
		return sorted.length > 0 ? sorted[0].format : null;
	}

	analyzeCompressionImpact(result) {
		if (!result.analysis) return null;
		
		return result.analysis.recommendation || 'Compression analysis completed';
	}

	generateSummaryReport(report) {
		const summaryPath = path.join(this.outputDir, `format-benchmark-summary-${this.timestamp}.txt`);
		
		let summary = `MIXPANEL IMPORT FORMAT BENCHMARK SUMMARY\n`;
		summary += `${'='.repeat(50)}\n\n`;
		summary += `Suite: ${report.summary.suite}\n`;
		summary += `Data Size: ${report.summary.dataSize}\n`;
		summary += `Live API: ${report.summary.liveAPI}\n`;
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
					summary += `  Best Format: ${best.format || 'N/A'}\n`;
					summary += `  Duration: ${u.time(best.duration || 0)}\n`;
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
			// Legacy flag - ignore since we always use live API now
		} else if (arg === '--output' && args[i + 1]) {
			options.outputDir = args[i + 1];
			i++;
		} else if (arg === '--help') {
			console.log(`
Usage: node benchmarks/formats/index.mjs [options]

Options:
  --suite <type>     Benchmark suite to run (quick|standard|comprehensive) [default: standard]
  --size <size>      Data size to test (small|large) [default: small]
  --live             [Deprecated] All benchmarks now use live API calls
  --output <dir>     Output directory for results [default: ../results/formats]
  --help             Show this help message

Examples:
  node benchmarks/formats/index.mjs                           # Run standard suite with small data
  node benchmarks/formats/index.mjs --suite comprehensive    # Run all format benchmarks
  node benchmarks/formats/index.mjs --size large             # Test with large data
			`);
			return;
		}
	}

	const runner = new FormatBenchmarkRunner(options);
	await runner.run();
}

// Export for programmatic use
export { FormatBenchmarkRunner, FORMAT_DATA, BENCHMARK_SUITES };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	main().catch(console.error);
}