#!/usr/bin/env node
/**
 * Probe 05b: per-scenario UNIQUE USERS via Insights runQuery ($all_events, math: unique,
 * grouped by scenario) — the identity-resolved instrument (JQL sees unresolved distinct_ids;
 * probe 05 documented that).
 *
 * usage: node run-05-compare-insights.js [--version 1]
 * out:   ./results/v{N}-05b-compare-insights.json + console table
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");

const VERSION = (() => { const i = process.argv.indexOf("--version"); return i > -1 ? parseInt(process.argv[i + 1], 10) : 1; })();
const BASE = "https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app";
const { AK_BEARER_TOKEN, ORIGINAL_PROJECT, SIMPLIFIED_PROJECT } = process.env;

async function insightsUniquesByScenario(projectId) {
	const bookmark = {
		sections: {
			show: [{
				name: "# USERS",
				behavior: {
					type: "simple",
					resourceType: "events",
					behaviors: [{ type: "event", name: "$all_events", filters: [] }],
					dataset: null,
				},
				measurement: { math: "unique" },
				type: "metric",
			}],
			group: [{
				dataset: null,
				value: "scenario",
				resourceType: "events",
				propertyType: "string",
				typeCast: null,
				unit: null,
			}],
			time: [{ dateRangeType: "between", value: ["2026-07-01", "2026-08-16"], unit: "day" }],
		},
	};
	const res = await fetch(`${BASE}/query/runQuery`, {
		method: "POST",
		headers: { "Authorization": `Bearer ${AK_BEARER_TOKEN}`, "Content-Type": "application/json" },
		body: JSON.stringify({ project_id: Number(projectId), client_id: `orig-simpl-insights-v${VERSION}`, type: "insights", bookmark }),
	});
	const body = await res.json();
	if (!res.ok || body.status === "error") throw new Error(`runQuery ${projectId}: ${JSON.stringify(body).slice(0, 500)}`);
	return body;
}

/** walk the insights series shape and produce {scenario: uniques} totals */
function extractByScenario(body) {
	// insights grouped series: {series: {"# USERS": {"<scenario>": {"<date>": n, ...} | {all: n}, ...}}}
	const out = {};
	const series = body?.results?.series || body?.result?.series || body?.series || {};
	const metric = series["# USERS"] ?? Object.values(series)[0] ?? {};
	for (const [key, val] of Object.entries(metric)) {
		if (key === "$overall") continue;
		if (typeof val === "number") { out[key] = val; continue; }
		if (val && typeof val === "object") {
			// sum across dates is WRONG for uniques; prefer 'all' | '$overall' totals if present
			if (typeof val.all === "number") out[key] = val.all;
			else if (typeof val.$overall === "number") out[key] = val.$overall;
			else if (val.$overall && typeof val.$overall === "object") out[key] = Object.values(val.$overall).reduce((a, b) => a + Number(b), 0);
			else out[key] = Math.max(...Object.values(val).map(Number).filter(Number.isFinite));
		}
	}
	return out;
}

async function main() {
	const origBody = await insightsUniquesByScenario(ORIGINAL_PROJECT);
	const simpBody = await insightsUniquesByScenario(SIMPLIFIED_PROJECT);
	const orig = extractByScenario(origBody);
	const simp = extractByScenario(simpBody);

	const ideal = { s01: 1, s02: 1, s03: 1, s04: 1, s05: 1, s06: 2, s07: 1, s08: 1, s09: 1, s10: 1, s11: 1, s12: 2, s13: 1, s14: 1, s15: 1 };
	const scenarios = [...new Set([...Object.keys(orig), ...Object.keys(simp), ...Object.keys(ideal)])].filter((s) => /^s\d+$/.test(s)).sort();

	console.log("\nscenario | ORIGINAL | SIMPLIFIED(replayed) | ideal");
	console.log("---------|----------|----------------------|------");
	const table = [];
	for (const s of scenarios) {
		console.log(`${s} | ${orig[s] ?? "-"} | ${simp[s] ?? "-"} | ${ideal[s] ?? "?"}`);
		table.push({ scenario: s, original: orig[s] ?? null, simplified: simp[s] ?? null, ideal: ideal[s] ?? null });
	}
	const outPath = path.join(__dirname, "results", `v${VERSION}-05b-compare-insights.json`);
	fs.writeFileSync(outPath, JSON.stringify({
		version: VERSION, ranAt: new Date().toISOString(), table,
		extracted: { orig, simp },
		rawBodies: { orig: origBody, simp: simpBody },
	}, null, 2));
	console.log(`\n→ ${outPath}`);
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
