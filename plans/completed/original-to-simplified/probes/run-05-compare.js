#!/usr/bin/env node
/**
 * Probe 05: THE VERDICT — per-scenario unique-user counts, ORIGINAL vs SIMPLIFIED (replayed),
 * vs manifest ground truth. Uses Power Tools getSegmentation (unique) broken out by scenario.
 *
 * usage: node run-05-compare.js [--version 1]
 * out:   ./results/v{N}-05-compare.json + console table
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");

const VERSION = (() => { const i = process.argv.indexOf("--version"); return i > -1 ? parseInt(process.argv[i + 1], 10) : 1; })();
const BASE = "https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app";
const { AK_BEARER_TOKEN, ORIGINAL_PROJECT, SIMPLIFIED_PROJECT } = process.env;

// count unique users on $all_events per scenario via one segmentation call per event name,
// then take the max? No — use a single high-volume event present in every scenario ('page view'
// misses some scenarios). Instead: query each of the 5 normal event names and union isn't
// possible via segmentation... so run one runJQL per project: uniques per scenario across ALL events.
async function jqlUniques(projectId) {
	const script = `
function main() {
  return Events({from_date: '2026-07-01', to_date: '2026-08-16'})
    .filter(e => e.properties.dataVersion == ${VERSION})
    .groupByUser(['properties.scenario'], mixpanel.reducer.count())
    .groupBy([e => e.key[1]], mixpanel.reducer.count());
}`;
	const res = await fetch(`${BASE}/query/runJQL`, {
		method: "POST",
		headers: { "Authorization": `Bearer ${AK_BEARER_TOKEN}`, "Content-Type": "application/json" },
		body: JSON.stringify({ project_id: Number(projectId), client_id: "orig-simpl-compare", script }),
	});
	const body = await res.json();
	if (!res.ok || body.status === "error") throw new Error(`JQL ${projectId}: ${JSON.stringify(body).slice(0, 400)}`);
	const rows = body.results || body;
	const out = {};
	for (const row of rows) out[row.key[0]] = row.value;
	return out;
}

async function main() {
	const manifest = require(path.join(__dirname, "data", `v${VERSION}-manifest.json`));
	const [orig, simp] = [await jqlUniques(ORIGINAL_PROJECT), await jqlUniques(SIMPLIFIED_PROJECT)];

	// ground truth: expected distinct PEOPLE per scenario if identity resolution were perfect
	const expected = {
		s01: 1, s02: 1, s03: 1, s04: 1, s05: 1,
		s06: 2,   // 2 users merged in original (1 cluster); simplified can't merge users: 2 people (elected winner holds the anons)
		s07: 1, s08: 1, s09: 1, s10: 1, s11: 1,
		s12: 2,   // user + numeric alias that IS a user id: multi-user cluster; original: 1, simplified: 2
		s13: 1, s14: 1, s15: 1,
	};

	const scenarios = Object.keys(manifest.scenarios).sort();
	const table = [];
	console.log("\nscenario | ORIGINAL uniques | SIMPLIFIED uniques | ideal | note");
	console.log("---------|------------------|--------------------|-------|-----");
	for (const s of scenarios) {
		const o = orig[s] ?? 0, si = simp[s] ?? 0, e = expected[s] ?? "?";
		const note = manifest.scenarios[s].description;
		table.push({ scenario: s, original: o, simplified: si, ideal: e, note });
		console.log(`${s} | ${o} | ${si} | ${e} | ${note.slice(0, 60)}`);
	}
	const outPath = path.join(__dirname, "results", `v${VERSION}-05-compare.json`);
	fs.mkdirSync(path.dirname(outPath), { recursive: true });
	fs.writeFileSync(outPath, JSON.stringify({ version: VERSION, ranAt: new Date().toISOString(), table }, null, 2));
	console.log(`\n→ ${outPath}`);
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
