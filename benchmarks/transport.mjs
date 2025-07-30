//@ts-nocheck
/* eslint-disable no-unused-vars */
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const mpStream = require('../index.js');
const u = require('ak-tools');
const Types = require("../index.js");

//! BENCHMARK: GOT vs UNDICI TRANSPORT PERFORMANCE

export default async function main() {
	const NDJSON = `./benchmarks/testData/one-two-million.ndjson`;
	// const NDJSON = `./benchmarks/testData/dnd250.ndjson`;

	/** @type {Types.Options} */
	const baseOpts = {
		logs: false,
		verbose: true,
		streamFormat: 'jsonl',
		workers: 25, // Optimal from workers benchmark
		recordType: 'event',
		dryRun: false, // Use dry run for testing logic without actual HTTP requests
		fixData: true // Enable data fixing
	};

	const results = {
		got: {},
		undici: {}
	};

	console.log('🚀 TRANSPORT BENCHMARK: GOT vs UNDICI');
	console.log('='.repeat(50));
	console.log(`📊 Dataset: ${NDJSON}`);
	console.log(`👥 Workers: ${baseOpts.workers}`);
	console.log(`📦 Record Type: ${baseOpts.recordType}`);
	console.log('\n');

	// Test GOT transport
	console.log('🔄 Testing GOT transport...');
	const gotStart = Date.now();
	try {
		const gotResult = await mpStream({}, NDJSON, { 
			...baseOpts, 
			transport: 'got'
		});
		const gotEnd = Date.now();
		const gotDuration = gotEnd - gotStart;
		
		results.got = {
			duration: gotResult.duration || gotDuration,
			durationHuman: gotResult.durationHuman || `${gotDuration}ms`,
			eps: gotResult.eps || Math.floor((gotResult.total || 0) / ((gotResult.duration || gotDuration) / 1000)),
			rps: gotResult.rps || Math.floor((gotResult.requests || 0) / ((gotResult.duration || gotDuration) / 1000)),
			mbps: gotResult.mbps || Math.floor(((gotResult.bytes || 0) / 1e6) / ((gotResult.duration || gotDuration) / 1000)),
			total: gotResult.total || 0,
			success: gotResult.success || 0,
			failed: gotResult.failed || 0,
			requests: gotResult.requests || 0,
			retries: gotResult.retries || 0,
			bytes: gotResult.bytes || 0,
			bytesHuman: gotResult.bytesHuman || '0 B',
			workers: gotResult.workers || baseOpts.workers,
			avgBatchLength: gotResult.avgBatchLength || 0,
			percentQuota: gotResult.percentQuota || 0,
			actualDuration: gotDuration
		};
		console.log(`✅ GOT completed in ${results.got.durationHuman} (actual: ${gotDuration}ms)`);
		console.log(`📊 GOT processed ${u.comma(results.got.total)} records in ${results.got.requests} requests`);
	} catch (error) {
		console.log(`❌ GOT failed: ${error.message}`);
		results.got.error = error.message;
	}

	// Small delay between tests
	console.log('⏳ Waiting 5 seconds before next test...');
	await new Promise(resolve => setTimeout(resolve, 5000));
	console.log("\n\n")
	// Test UNDICI transport  
	console.log('🔄 Testing UNDICI transport...');
	const undiciStart = Date.now();
	try {
		const undiciResult = await mpStream({}, NDJSON, { 
			...baseOpts, 
			transport: 'undici'
		});
		const undiciEnd = Date.now();
		const undiciDuration = undiciEnd - undiciStart;
		
		results.undici = {
			duration: undiciResult.duration || undiciDuration,
			durationHuman: undiciResult.durationHuman || `${undiciDuration}ms`,
			eps: undiciResult.eps || Math.floor((undiciResult.total || 0) / ((undiciResult.duration || undiciDuration) / 1000)),
			rps: undiciResult.rps || Math.floor((undiciResult.requests || 0) / ((undiciResult.duration || undiciDuration) / 1000)),
			mbps: undiciResult.mbps || Math.floor(((undiciResult.bytes || 0) / 1e6) / ((undiciResult.duration || undiciDuration) / 1000)),
			total: undiciResult.total || 0,
			success: undiciResult.success || 0,
			failed: undiciResult.failed || 0,
			requests: undiciResult.requests || 0,
			retries: undiciResult.retries || 0,
			bytes: undiciResult.bytes || 0,
			bytesHuman: undiciResult.bytesHuman || '0 B',
			workers: undiciResult.workers || baseOpts.workers,
			avgBatchLength: undiciResult.avgBatchLength || 0,
			percentQuota: undiciResult.percentQuota || 0,
			actualDuration: undiciDuration
		};
		console.log(`✅ UNDICI completed in ${results.undici.durationHuman} (actual: ${undiciDuration}ms)`);
		console.log(`📊 UNDICI processed ${u.comma(results.undici.total)} records in ${results.undici.requests} requests`);
	} catch (error) {
		console.log(`❌ UNDICI failed: ${error.message}`);
		results.undici.error = error.message;
	}

	// Calculate performance improvements
	const calculateImprovement = (undici, got, inverse = false) => {
		if (!undici || !got || got === 0) return 'N/A';
		const improvement = inverse ? 
			((got - undici) / got) * 100 : 
			((undici - got) / got) * 100;
		const symbol = improvement > 0 ? '↑' : '↓';
		return `${symbol} ${Math.abs(improvement).toFixed(1)}%`;
	};

	// Use actual timing if available
	const gotTime = results.got.actualDuration || results.got.duration || 0;
	const undiciTime = results.undici.actualDuration || results.undici.duration || 0;

	console.log('');
	console.log('📈 PERFORMANCE ANALYSIS');
	console.log('='.repeat(50));

	if (!results.got.error && !results.undici.error) {
		console.log(`
📊 TRANSPORT PERFORMANCE COMPARISON

GOT Results:
-----------
• Duration: ${results.got.durationHuman}
• Events/sec: ${u.comma(results.got.eps)}
• Requests/sec: ${results.got.rps}
• MB/sec: ${results.got.mbps}
• Total Records: ${u.comma(results.got.total)}
• Success Rate: ${u.comma(results.got.success)}/${u.comma(results.got.total)} (${((results.got.success/results.got.total)*100).toFixed(1)}%)
• Total Requests: ${u.comma(results.got.requests)}
• Retries: ${u.comma(results.got.retries)}
• Data Transferred: ${results.got.bytesHuman}
• Avg Batch Size: ${Math.round(results.got.avgBatchLength)} records

UNDICI Results:
--------------
• Duration: ${results.undici.durationHuman}
• Events/sec: ${u.comma(results.undici.eps)}
• Requests/sec: ${results.undici.rps}
• MB/sec: ${results.undici.mbps}
• Total Records: ${u.comma(results.undici.total)}
• Success Rate: ${u.comma(results.undici.success)}/${u.comma(results.undici.total)} (${((results.undici.success/results.undici.total)*100).toFixed(1)}%)
• Total Requests: ${u.comma(results.undici.requests)}
• Retries: ${u.comma(results.undici.retries)}
• Data Transferred: ${results.undici.bytesHuman}
• Avg Batch Size: ${Math.round(results.undici.avgBatchLength)} records

🏆 PERFORMANCE IMPROVEMENTS (UNDICI vs GOT):
--------------------------------------------
• Duration: ${calculateImprovement(gotTime, undiciTime, true)} faster (${gotTime}ms vs ${undiciTime}ms)
• Events/sec: ${calculateImprovement(results.undici.eps, results.got.eps)} improvement
• Requests/sec: ${calculateImprovement(results.undici.rps, results.got.rps)} improvement  
• Throughput (MB/s): ${calculateImprovement(results.undici.mbps, results.got.mbps)} improvement
• Retry Reduction: ${calculateImprovement(results.got.retries, results.undici.retries, true)} fewer retries

${undiciTime < gotTime && undiciTime > 0 ? '🎉' : '⚠️'} UNDICI is ${undiciTime < gotTime && undiciTime > 0 ? 'FASTER' : 'SLOWER'} than GOT!

Overall Performance Gain: ${undiciTime > 0 && gotTime > 0 ? calculateImprovement(gotTime, undiciTime, true) : 'N/A'}
		`);

		// Memory and efficiency comparison
		if (results.undici.percentQuota && results.got.percentQuota) {
			console.log(`
🔋 EFFICIENCY METRICS:
---------------------
• GOT Quota Usage: ${results.got.percentQuota.toFixed(2)}% of Mixpanel quota
• UNDICI Quota Usage: ${results.undici.percentQuota.toFixed(2)}% of Mixpanel quota
• Efficiency Gain: ${calculateImprovement(results.got.percentQuota, results.undici.percentQuota, true)} quota optimization
			`);
		}

	} else {
		console.log('❌ Benchmark incomplete due to errors:');
		if (results.got.error) console.log(`• GOT Error: ${results.got.error}`);
		if (results.undici.error) console.log(`• UNDICI Error: ${results.undici.error}`);
	}

	console.log('');
	console.log('🏁 Transport benchmark complete!');
	console.log('='.repeat(50));

	return results;
}

// Run if called directly
if (import.meta.url === `file://${process.argv[1]}`) {
	main().catch(console.error);
}