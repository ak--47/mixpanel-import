#!/usr/bin/env node
/**
 * Probe 01: load the versioned torture dataset into the ORIGINAL id-merge project (4054680).
 * Captures strict-mode responses — failed_records ARE probe data (500-cap, non-uuid anon ids).
 *
 * usage: node run-01-load-original.js [--version 1]
 * out:   ./results/v{N}-01-load-original.json (creds scrubbed)
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");
const mpImport = require("../../../index.js");

const VERSION = (() => {
	const i = process.argv.indexOf("--version");
	return i > -1 ? parseInt(process.argv[i + 1], 10) : 1;
})();

const { ORIGINAL_SECRET, ORIGINAL_PROJECT, ORIGINAL_TOKEN } = process.env;
if (!ORIGINAL_SECRET || !ORIGINAL_PROJECT) throw new Error("missing ORIGINAL_* env vars");

async function main() {
	const data = path.join(__dirname, "data", `v${VERSION}-events.jsonl`);
	const results = await mpImport(
		{ secret: ORIGINAL_SECRET, project: ORIGINAL_PROJECT, token: ORIGINAL_TOKEN },
		data,
		{
			recordType: "event",
			streamFormat: "jsonl",
			workers: 1,            // preserve stream order — identity verbs are order-sensitive
			recordsPerBatch: 500,
			strict: true,
			fixData: false,
			dedupe: false,
			compress: false,
			abridged: false,
			verbose: false,
			showProgress: true,
		}
	);

	// scrub anything credential-shaped before writing to version-controlled dir
	const scrubbed = JSON.parse(JSON.stringify(results, (k, v) =>
		["secret", "token", "pass", "acct", "bearer", "auth"].includes(k.toLowerCase()) ? undefined : v
	));
	const outDir = path.join(__dirname, "results");
	fs.mkdirSync(outDir, { recursive: true });
	const outPath = path.join(outDir, `v${VERSION}-01-load-original.json`);
	fs.writeFileSync(outPath, JSON.stringify(scrubbed, null, 2));

	console.log(`\nsuccess: ${results.success} / failed: ${results.failed} / total: ${results.total}`);
	console.log(`duration: ${results.durationHuman || results.duration}`);
	const errors = Array.isArray(results.errors) ? results.errors.slice(0, 20) : results.errors;
	if (errors && (!Array.isArray(errors) || errors.length)) console.log("errors:", JSON.stringify(errors, null, 2));
	console.log(`full results → ${outPath}`);
}

main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
