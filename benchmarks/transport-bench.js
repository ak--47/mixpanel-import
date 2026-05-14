#!/usr/bin/env node
/**
 * Transport Benchmark: got vs undici
 *
 * Spec: docs/superpowers/specs/2026-05-13-got-vs-undici-benchmark-design.md
 *
 * Runs all 4 gzipped JSONL files in benchmarks/testdata/ through both
 * transports across a worker matrix. Sequential (one run at a time).
 * Hits real Mixpanel using MP_TOKEN from .env.
 *
 *   node benchmarks/transport-bench.js
 */

require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mp = require('../index.js');

const c = {
	reset: '\x1b[0m', bright: '\x1b[1m', dim: '\x1b[2m',
	red: '\x1b[31m', green: '\x1b[32m', yellow: '\x1b[33m',
	blue: '\x1b[34m', magenta: '\x1b[35m', cyan: '\x1b[36m'
};

const TESTDATA_DIR = path.resolve(__dirname, 'testdata');
const RESULTS_DIR = path.resolve(__dirname, 'results');

const FILES = [
	{ name: 'tiny-150MB',  file: '1m-events-TINY-150MB-RAW.json.gz' },
	{ name: 'tiny-700MB',  file: '5m-events-TINY-700MB-RAW.json.gz' },
	{ name: 'dense-1GB',   file: '300k-events-DENSE-1GB-RAW.json.gz' },
	{ name: 'dense-4GB',   file: '1m-events-DENSE-4GB-RAW.json.gz' }
];

const TRANSPORTS = ['got', 'undici'];
const WORKERS = [25, 50, 100];
const COOLDOWN_MS = 30_000;

const results = {
	timestamp: new Date().toISOString(),
	system: {
		node: process.version,
		platform: process.platform,
		arch: process.arch,
		memory: Math.round(require('os').totalmem() / 1024 / 1024 / 1024) + 'GB',
		cores: require('os').cpus().length
	},
	matrix: { transports: TRANSPORTS, workers: WORKERS, files: FILES.map(f => f.name) },
	runs: []
};

function preflight() {
	if (!process.env.MP_TOKEN) {
		console.error(`${c.red}MP_TOKEN missing in .env${c.reset}`);
		process.exit(1);
	}
	const missing = [];
	for (const f of FILES) {
		const p = path.join(TESTDATA_DIR, f.file);
		if (!fs.existsSync(p)) missing.push(p);
	}
	if (missing.length) {
		console.error(`${c.red}Missing test files:${c.reset}\n  ${missing.join('\n  ')}`);
		process.exit(1);
	}
	if (!fs.existsSync(RESULTS_DIR)) fs.mkdirSync(RESULTS_DIR, { recursive: true });
}

function buildPlan() {
	const plan = [];
	for (const file of FILES) {
		for (const transport of TRANSPORTS) {
			for (const workers of WORKERS) {
				plan.push({ file, transport, workers });
			}
		}
	}
	return plan;
}

async function runOne({ file, transport, workers }) {
	const filePath = path.join(TESTDATA_DIR, file.file);
	/** @type {import('../index.js').Options} */
	const opts = {
		recordType: 'event',
		streamFormat: 'jsonl',
		isGzip: true,
		fixData: true,
		addToken: true,
		recordsPerBatch: 2000,
		compress: true,
		verbose: false,
		abridged: true,
		logs: false,
		showProgress: false,
		dryRun: false,
		v2_compat: true,
		transport,
		workers,
		highWater: workers * 10,
		transformFunc: record => record,
		responseHandler: res => {
			return res;
		}
	};
	const wallStart = Date.now();
	let summary, errorMessage = null;
	try {
		summary = await mp({ token: process.env.MP_TOKEN }, filePath, opts);
	} catch (err) {
		errorMessage = err?.message || String(err);
	}
	const wallMs = Date.now() - wallStart;

	if (errorMessage) {
		return {
			file: file.name, filePath, transport, workers,
			error: errorMessage, wallMs
		};
	}
	// abridged: true strips mbps/rps/retries/serverErrors/clientErrors/batches.
	// Compute mbps/rps locally; retries/5xx/clientErrors unavailable in abridged mode.
	const durSec = (summary.duration || 0) / 1000;
	const mbps = durSec > 0 ? (summary.bytes || 0) / 1e6 / durSec : 0;
	const rps = durSec > 0 ? (summary.requests || 0) / durSec : 0;
	const eps = summary.eps ?? (durSec > 0 ? (summary.total || 0) / durSec : 0);
	return {
		file: file.name,
		filePath,
		transport,
		workers,
		wallMs,
		eps,
		mbps,
		rps,
		duration: summary.duration || 0,
		durationHuman: summary.durationHuman || '',
		bytes: summary.bytes || 0,
		bytesHuman: summary.bytesHuman || '',
		total: summary.total || 0,
		success: summary.success || 0,
		failed: summary.failed || 0,
		requests: summary.requests || 0,
		rateLimit: summary.rateLimit || 0,
		peakHeapMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024)
	};
}

function fmtRun(idx, total, run) {
	const head = `[${String(idx).padStart(2)}/${total}] ${run.file.padEnd(11)} transport=${run.transport.padEnd(6)} workers=${String(run.workers).padStart(3)}`;
	if (run.error) {
		return `${head}  ${c.red}ERROR: ${run.error}${c.reset}`;
	}
	return `${head}  ` +
		`${c.green}${run.eps.toLocaleString().padStart(7)} eps${c.reset} | ` +
		`${c.cyan}${run.mbps.toFixed(2).padStart(6)} MB/s${c.reset} | ` +
		`${c.yellow}${run.rps.toFixed(2).padStart(6)} rps${c.reset} | ` +
		`${c.dim}${run.durationHuman} | success=${run.success.toLocaleString()} failed=${run.failed.toLocaleString()} 429=${run.rateLimit}${c.reset}`;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function printSummaryTables() {
	console.log(`\n${c.bright}${c.magenta}═══════════════════════════════════════════════════════════════${c.reset}`);
	console.log(`${c.bright}${c.magenta}                    BENCHMARK SUMMARY                          ${c.reset}`);
	console.log(`${c.bright}${c.magenta}═══════════════════════════════════════════════════════════════${c.reset}\n`);

	for (const f of FILES) {
		console.log(`${c.bright}${f.name}${c.reset}  (${f.file})`);
		console.log(`  ${''.padEnd(8)} ${WORKERS.map(w => `w=${w}`.padStart(20)).join(' ')}`);
		for (const t of TRANSPORTS) {
			const row = WORKERS.map(w => {
				const r = results.runs.find(x => x.file === f.name && x.transport === t && x.workers === w);
				if (!r || r.error) return `${c.red}ERR${c.reset}`.padStart(20);
				return `${r.eps.toLocaleString()}eps/${r.mbps.toFixed(1)}MB/${r.rps.toFixed(1)}rps`.padStart(20);
			}).join(' ');
			console.log(`  ${t.padEnd(8)} ${row}`);
		}
		const cells = results.runs.filter(r => r.file === f.name && !r.error);
		if (cells.length) {
			const winner = cells.reduce((a, b) => b.eps > a.eps ? b : a);
			console.log(`  ${c.bright}→ winner: ${winner.transport} @ workers=${winner.workers} (${winner.eps.toLocaleString()} eps)${c.reset}`);
		}
		console.log();
	}

	const ok = results.runs.filter(r => !r.error);
	if (ok.length) {
		const overall = ok.reduce((a, b) => b.eps > a.eps ? b : a);
		console.log(`${c.bright}Overall peak:${c.reset} ${overall.transport} @ workers=${overall.workers} on ${overall.file} → ${c.green}${overall.eps.toLocaleString()} eps${c.reset}, ${c.cyan}${overall.mbps.toFixed(2)} MB/s${c.reset}, ${c.yellow}${overall.rps.toFixed(2)} rps${c.reset}\n`);
	}
}

function saveResults() {
	const ts = new Date().toISOString().replace(/[:.]/g, '-');
	const file = path.join(RESULTS_DIR, `transport-bench-${ts}.json`);
	fs.writeFileSync(file, JSON.stringify(results, null, 2));
	console.log(`${c.dim}Results: ${file}${c.reset}`);
	return file;
}

let interrupted = false;
process.on('SIGINT', () => {
	if (interrupted) process.exit(130);
	interrupted = true;
	console.log(`\n${c.yellow}SIGINT received — saving partial results then exiting...${c.reset}`);
});

async function main() {
	preflight();

	const plan = buildPlan();
	console.log(`${c.bright}${c.cyan}Transport Benchmark: got vs undici${c.reset}`);
	console.log(`${c.dim}files=${FILES.length} transports=${TRANSPORTS.length} workers=${WORKERS.join(',')} → ${plan.length} runs, sequential${c.reset}`);
	console.log(`${c.dim}cooldown=${COOLDOWN_MS / 1000}s between runs${c.reset}\n`);

	for (let i = 0; i < plan.length; i++) {
		if (interrupted) break;
		const cell = plan[i];
		const run = await runOne(cell);
		results.runs.push(run);
		console.log(fmtRun(i + 1, plan.length, run));
		if (i < plan.length - 1 && !interrupted) {
			await sleep(COOLDOWN_MS);
		}
	}

	printSummaryTables();
	saveResults();

	if (interrupted) process.exit(130);
}

main().catch(err => {
	console.error(`${c.red}Fatal: ${err.message}${c.reset}`);
	console.error(err.stack);
	saveResults();
	process.exit(1);
});
