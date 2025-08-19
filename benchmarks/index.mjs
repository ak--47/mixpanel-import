//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
import { writeFileSync, existsSync, mkdirSync } from "fs";
import path from "path";

const require = createRequire(import.meta.url);
const u = require('../node_modules/ak-tools');

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

// Import all benchmark suite runners
import { BenchmarkRunner as NewBenchmarkRunner } from './new/index.mjs';
import { FormatBenchmarkRunner } from './formats/index.mjs';

// Import individual old benchmarks
import httpOneOrTwo from './old/httpOneOrTwo.mjs';
import main from './old/main.mjs';
import profiler from './old/profiler.mjs';
import streamTypes from './old/streamTypes.mjs';
import streamsVsMemory from './old/streamsVsMemory.mjs';
import transport from './old/transport.mjs';
import workers from './old/workers.mjs';

/**
 * MIXPANEL IMPORT MASTER BENCHMARK SUITE
 * 
 * This master orchestrator runs all available benchmark suites:
 * - New: Modern comprehensive performance benchmarks
 * - Formats: Data format parsing and compression analysis  
 * - Old: Legacy benchmarks updated for compatibility
 * 
 * Provides unified reporting across all benchmark types.
 */

const BENCHMARK_SUITES = {
	// Quick suites for fast testing
	quick: {
		new: ['workerOptimization', 'formatComparison'],
		formats: ['formatPerformance'], 
		old: ['main', 'workers']
	},
	
	// Standard suites for regular optimization
	standard: {
		new: ['workerOptimization', 'parameterMatrix', 'formatComparison', 'transportComparison'],
		formats: ['formatPerformance', 'compressionImpact'],
		old: ['main', 'workers', 'transport', 'streamTypes']
	},
	
	// Comprehensive suites for complete analysis
	comprehensive: {
		new: ['workerOptimization', 'parameterMatrix', 'formatComparison', 'transformImpact', 'transportComparison', 'memoryVsStream'],
		formats: ['formatPerformance', 'compressionImpact', 'scalingAnalysis'],
		old: ['main', 'workers', 'transport', 'streamTypes', 'streamsVsMemory', 'profiler', 'httpOneOrTwo']
	},
	
	// Individual suite options
	'new-only': {
		new: ['workerOptimization', 'parameterMatrix', 'formatComparison', 'transformImpact', 'transportComparison', 'memoryVsStream'],
		formats: [],
		old: []
	},
	
	'formats-only': {
		new: [],
		formats: ['formatPerformance', 'compressionImpact', 'scalingAnalysis'],
		old: []
	},
	
	'old-only': {
		new: [],
		formats: [],
		old: ['main', 'workers', 'transport', 'streamTypes', 'streamsVsMemory', 'profiler', 'httpOneOrTwo']
	}
};

const DATA_SIZES = {
	small: '250k',
	large: '1m'
};

class MasterBenchmarkRunner {
	constructor(options = {}) {
		this.suite = options.suite || 'standard';
		this.dataSize = options.dataSize || 'small';
		this.dryRun = options.dryRun !== false; // default to true for safety
		this.outputDir = options.outputDir || './results';
		this.timestamp = new Date().toISOString().replace(/[:.]/g, '-');
		this.results = {
			new: [],
			formats: [],
			old: []
		};
		
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
		console.log('🚀 MIXPANEL IMPORT MASTER BENCHMARK SUITE');
		console.log('='.repeat(70));
		console.log(`📊 Suite: ${this.suite.toUpperCase()}`);
		console.log(`📁 Data Size: ${this.dataSize} (${DATA_SIZES[this.dataSize]} records)`);
		console.log(`🔬 Dry Run: ${this.dryRun ? 'YES (no actual API calls)' : 'NO (live API calls)'}`);
		console.log(`📂 Output: ${this.outputDir}`);
		console.log('='.repeat(70));
		console.log('');

		const suiteConfig = BENCHMARK_SUITES[this.suite];
		if (!suiteConfig) {
			throw new Error(`Unknown suite: ${this.suite}. Available: ${Object.keys(BENCHMARK_SUITES).join(', ')}`);
		}

		let totalSuites = 0;
		if (suiteConfig.new.length > 0) totalSuites++;
		if (suiteConfig.formats.length > 0) totalSuites++;
		if (suiteConfig.old.length > 0) totalSuites++;

		let currentSuite = 0;

		// Run New Benchmarks
		if (suiteConfig.new.length > 0) {
			currentSuite++;
			console.log(`[${currentSuite}/${totalSuites}] 🆕 RUNNING NEW BENCHMARK SUITE`);
			console.log('─'.repeat(50));
			
			try {
				const newRunner = new NewBenchmarkRunner({
					suite: this.getNewSuiteName(suiteConfig.new),
					dataSize: this.dataSize,
					dryRun: this.dryRun,
					outputDir: path.join(this.outputDir, 'new')
				});
				
				await newRunner.run();
				this.results.new = newRunner.results;
				
				console.log('✅ New benchmark suite completed!');
				console.log('');
				
			} catch (error) {
				console.error(`❌ New benchmark suite failed: ${error.message}`);
				this.results.new = { error: error.message };
			}
		}

		// Run Format Benchmarks
		if (suiteConfig.formats.length > 0) {
			currentSuite++;
			console.log(`[${currentSuite}/${totalSuites}] 📊 RUNNING FORMAT BENCHMARK SUITE`);
			console.log('─'.repeat(50));
			
			try {
				const formatRunner = new FormatBenchmarkRunner({
					suite: this.getFormatSuiteName(suiteConfig.formats),
					dataSize: this.dataSize,
					dryRun: this.dryRun,
					outputDir: path.join(this.outputDir, 'formats')
				});
				
				await formatRunner.run();
				this.results.formats = formatRunner.results;
				
				console.log('✅ Format benchmark suite completed!');
				console.log('');
				
			} catch (error) {
				console.error(`❌ Format benchmark suite failed: ${error.message}`);
				this.results.formats = { error: error.message };
			}
		}

		// Run Old Benchmarks
		if (suiteConfig.old.length > 0) {
			currentSuite++;
			console.log(`[${currentSuite}/${totalSuites}] 🔧 RUNNING LEGACY BENCHMARK SUITE`);
			console.log('─'.repeat(50));
			
			try {
				await this.runOldBenchmarks(suiteConfig.old);
				console.log('✅ Legacy benchmark suite completed!');
				console.log('');
				
			} catch (error) {
				console.error(`❌ Legacy benchmark suite failed: ${error.message}`);
				this.results.old = { error: error.message };
			}
		}

		// Generate master report
		await this.generateMasterReport();
		
		console.log('🎉 ALL BENCHMARK SUITES COMPLETED!');
		console.log('='.repeat(70));
		console.log(`📊 View master report: ${path.join(this.outputDir, `master-benchmark-${this.timestamp}.json`)}`);
		console.log(`📋 View summary: ${path.join(this.outputDir, `master-summary-${this.timestamp}.txt`)}`);
		console.log('='.repeat(70));
	}

	getNewSuiteName(benchmarks) {
		if (benchmarks.length <= 2) return 'quick';
		if (benchmarks.length <= 4) return 'standard';
		return 'comprehensive';
	}

	getFormatSuiteName(benchmarks) {
		if (benchmarks.length === 1) return 'quick';
		if (benchmarks.length === 2) return 'standard';
		return 'comprehensive';
	}

	async runOldBenchmarks(benchmarks) {
		const baseConfig = {
			dataFile: this.dataSize === 'small' 
				? './testData/dnd250.ndjson' 
				: './testData/one-two-million.ndjson',
			dryRun: this.dryRun,
			credentials: this.credentials
		};

		const oldBenchmarkMap = {
			httpOneOrTwo,
			main,
			profiler,
			streamTypes,
			streamsVsMemory,
			transport,
			workers
		};

		for (let i = 0; i < benchmarks.length; i++) {
			const benchmarkName = benchmarks[i];
			const benchmarkFunc = oldBenchmarkMap[benchmarkName];
			
			if (!benchmarkFunc) {
				console.log(`❌ Unknown old benchmark: ${benchmarkName}`);
				continue;
			}

			console.log(`  [${i + 1}/${benchmarks.length}] Running ${benchmarkName}...`);
			
			try {
				const startTime = Date.now();
				const result = await benchmarkFunc(baseConfig);
				const duration = Date.now() - startTime;
				
				this.results.old.push({
					benchmarkName,
					result,
					duration,
					timestamp: new Date().toISOString()
				});
				
				console.log(`    ✅ ${benchmarkName} completed in ${u.time(duration)}`);
				
			} catch (error) {
				console.log(`    ❌ ${benchmarkName} failed: ${error.message}`);
				this.results.old.push({
					benchmarkName,
					error: error.message,
					timestamp: new Date().toISOString()
				});
			}
		}
	}

	async generateMasterReport() {
		const report = {
			metadata: {
				suite: this.suite,
				dataSize: this.dataSize,
				dryRun: this.dryRun,
				timestamp: this.timestamp,
				totalDuration: Date.now() // Will be updated
			},
			summary: this.generateExecutiveSummary(),
			results: {
				new: this.results.new,
				formats: this.results.formats,
				old: this.results.old
			},
			recommendations: this.generateMasterRecommendations()
		};

		// Save detailed JSON report
		const jsonPath = path.join(this.outputDir, `master-benchmark-${this.timestamp}.json`);
		writeFileSync(jsonPath, JSON.stringify(report, null, 2));

		// Generate executive summary report
		this.generateExecutiveSummary(report);
	}

	generateExecutiveSummary() {
		const summary = {
			suitesRun: [],
			totalBenchmarks: 0,
			successfulBenchmarks: 0,
			failedBenchmarks: 0,
			keyFindings: []
		};

		// Analyze new benchmarks
		if (Array.isArray(this.results.new) && this.results.new.length > 0) {
			summary.suitesRun.push('New Performance Suite');
			summary.totalBenchmarks += this.results.new.length;
			summary.successfulBenchmarks += this.results.new.filter(r => !r.error).length;
			summary.failedBenchmarks += this.results.new.filter(r => r.error).length;
		}

		// Analyze format benchmarks
		if (Array.isArray(this.results.formats) && this.results.formats.length > 0) {
			summary.suitesRun.push('Format Analysis Suite');
			summary.totalBenchmarks += this.results.formats.length;
			summary.successfulBenchmarks += this.results.formats.filter(r => !r.error).length;
			summary.failedBenchmarks += this.results.formats.filter(r => r.error).length;
		}

		// Analyze old benchmarks
		if (Array.isArray(this.results.old) && this.results.old.length > 0) {
			summary.suitesRun.push('Legacy Benchmark Suite');
			summary.totalBenchmarks += this.results.old.length;
			summary.successfulBenchmarks += this.results.old.filter(r => !r.error).length;
			summary.failedBenchmarks += this.results.old.filter(r => r.error).length;
		}

		return summary;
	}

	generateMasterRecommendations() {
		const recommendations = {
			performance: [],
			formats: [],
			configuration: [],
			general: []
		};

		// Extract recommendations from each suite
		if (this.results.new?.length > 0) {
			recommendations.performance.push('Review New Benchmark Suite results for optimal worker counts and parameter configurations');
		}

		if (this.results.formats?.length > 0) {
			recommendations.formats.push('Review Format Benchmark Suite results for optimal data format selection');
		}

		if (this.results.old?.length > 0) {
			recommendations.configuration.push('Review Legacy Benchmark Suite results for transport and processing optimizations');
		}

		recommendations.general.push(`Benchmark suite "${this.suite}" completed with ${this.dataSize} dataset`);
		
		if (this.dryRun) {
			recommendations.general.push('Consider running with --live flag for real API performance testing');
		}

		return recommendations;
	}

	generateExecutiveSummary(report) {
		const summaryPath = path.join(this.outputDir, `master-summary-${this.timestamp}.txt`);
		
		let summary = `MIXPANEL IMPORT MASTER BENCHMARK SUMMARY\n`;
		summary += `${'='.repeat(60)}\n\n`;
		summary += `Suite: ${report.metadata.suite}\n`;
		summary += `Data Size: ${report.metadata.dataSize}\n`;
		summary += `Dry Run: ${report.metadata.dryRun}\n`;
		summary += `Timestamp: ${report.metadata.timestamp}\n`;
		summary += `Suites Run: ${report.summary.suitesRun.join(', ')}\n`;
		summary += `Total Benchmarks: ${report.summary.totalBenchmarks}\n`;
		summary += `Success Rate: ${report.summary.successfulBenchmarks}/${report.summary.totalBenchmarks}\n\n`;

		// Add recommendations by category
		Object.entries(report.recommendations).forEach(([category, recs]) => {
			if (recs.length > 0) {
				summary += `${category.toUpperCase()} RECOMMENDATIONS:\n`;
				summary += `${'-'.repeat(30)}\n`;
				recs.forEach(rec => {
					summary += `• ${rec}\n`;
				});
				summary += `\n`;
			}
		});

		// Add quick performance highlights
		summary += `PERFORMANCE HIGHLIGHTS:\n`;
		summary += `${'-'.repeat(25)}\n`;
		summary += `• New Suite: Modern performance benchmarks ${Array.isArray(this.results.new) ? 'completed' : 'not run'}\n`;
		summary += `• Format Suite: Data format analysis ${Array.isArray(this.results.formats) ? 'completed' : 'not run'}\n`;
		summary += `• Legacy Suite: Compatibility benchmarks ${Array.isArray(this.results.old) ? 'completed' : 'not run'}\n\n`;

		summary += `For detailed results, see:\n`;
		summary += `• JSON Report: master-benchmark-${this.timestamp}.json\n`;
		summary += `• Individual suite reports in subdirectories\n`;

		writeFileSync(summaryPath, summary);
		console.log(`📋 Executive summary: ${summaryPath}`);
	}
}

// CLI interface
async function runCLI() {
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
  --suite <type>     Benchmark suite to run [default: standard]
                     Available: quick, standard, comprehensive, new-only, formats-only, old-only
  --size <size>      Data size to test (small|large) [default: small]
  --live             Use live API calls instead of dry run [default: false]
  --output <dir>     Output directory for results [default: ./results]
  --help             Show this help message

Suite Types:
  quick              Fast benchmarks from all suites (~10 min)
  standard           Standard benchmarks from all suites (~30 min)  
  comprehensive      All benchmarks from all suites (~60 min)
  new-only           Only modern performance benchmarks (~30 min)
  formats-only       Only data format benchmarks (~15 min)
  old-only           Only legacy benchmarks (~20 min)

Examples:
  node benchmarks/index.mjs                              # Run standard suite with small data (dry run)
  node benchmarks/index.mjs --suite comprehensive       # Run all benchmarks from all suites
  node benchmarks/index.mjs --suite new-only --live     # Run only new benchmarks with live API
  node benchmarks/index.mjs --suite formats-only        # Run only format benchmarks
  node benchmarks/index.mjs --size large --live         # Test with large data and live API
			`);
			return;
		}
	}

	const runner = new MasterBenchmarkRunner(options);
	await runner.run();
}

// Export for programmatic use
export { MasterBenchmarkRunner, BENCHMARK_SUITES, DATA_SIZES };

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	runCLI().catch(console.error);
}