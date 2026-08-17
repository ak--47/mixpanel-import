#!/usr/bin/env node
/**
 * Probe 04: THE ACCEPTANCE TEST — replay the ORIGINAL project's raw export through the new
 * identityReplay stage into the SIMPLIFIED project (or a local dry-run file).
 *
 * usage: node run-04-replay.js [--version 1] [--mode dry|live]
 *   dry  → destinationOnly: writes transformed records to data/v{N}-replay-dry.jsonl, sends nothing
 *   live → imports into SIMPLIFIED project 4054681
 * out:   ./results/v{N}-04-replay-{mode}.json + data/v{N}-graph.jsonl (artifact)
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");
const mpImport = require("../../../index.js");

const VERSION = (() => { const i = process.argv.indexOf("--version"); return i > -1 ? parseInt(process.argv[i + 1], 10) : 1; })();
const MODE = (() => { const i = process.argv.indexOf("--mode"); return i > -1 ? process.argv[i + 1] : "dry"; })();

const { SIMPLIFIED_SECRET, SIMPLIFIED_PROJECT, SIMPLIFIED_TOKEN } = process.env;
const INPUT = path.join(__dirname, "data", `replay-input-v${VERSION}`);
const inputFiles = fs.readdirSync(INPUT).map(f => path.join(INPUT, f));

async function main() {
	const opts = {
		recordType: "event",
		streamFormat: "jsonl",
		workers: 1,
		recordsPerBatch: 500,
		strict: true,
		fixData: true,             // re-nest + scrub export junk like the export-import path does
		dedupe: false,
		verbose: false,
		showProgress: true,
		abridged: false,
		identityReplay: {
			isUserId: /^\d+$/,       // canonical user ids are numeric (dataset ground truth)
			onAmbiguous: "resolve",  // demonstrate election on s06/s12 multi-user clusters
			associationProps: { dataVersion: VERSION, replaySource: "run-04" },
			graphPath: path.join(__dirname, "data", `v${VERSION}-graph.jsonl`),
		},
	};
	if (MODE === "dry") {
		opts.destination = path.join(__dirname, "data", `v${VERSION}-replay-dry.jsonl`);
		opts.destinationOnly = true;
	}
	const creds = MODE === "live"
		? { secret: SIMPLIFIED_SECRET, project: SIMPLIFIED_PROJECT, token: SIMPLIFIED_TOKEN }
		: { token: "dry-run-no-send" };

	const results = await mpImport(creds, inputFiles, opts);

	const scrubbed = JSON.parse(JSON.stringify(results, (k, v) =>
		["secret", "token", "pass", "acct", "bearer", "auth"].includes(k.toLowerCase()) ? undefined : v
	));
	const outDir = path.join(__dirname, "results");
	fs.mkdirSync(outDir, { recursive: true });
	const outPath = path.join(outDir, `v${VERSION}-04-replay-${MODE}.json`);
	fs.writeFileSync(outPath, JSON.stringify(scrubbed, null, 2));

	console.log(`\nmode=${MODE} success=${results.success} failed=${results.failed} total=${results.total}`);
	console.log("identityReplay telemetry:", JSON.stringify(results.identityReplay || null, null, 2));
	console.log(`→ ${outPath}`);
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
