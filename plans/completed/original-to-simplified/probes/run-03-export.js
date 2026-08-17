#!/usr/bin/env node
/**
 * Probe 03: raw-export both test projects and analyze identity resolution.
 *  - ORIGINAL (4054680): do verb rows appear? which props? is distinct_id cluster-resolved?
 *    $distinct_id_before_identity / $user_id / $device_id present?
 *  - SIMPLIFIED (4054681): outcomes of run-02 probes (prefixing, conflicts, double-prefix).
 *
 * usage: node run-03-export.js [--version 1] [--project original|simplified|both]
 * out:   ./data/exports/{project}-v{N}/*.json(l) + ./results/v{N}-03-export-{project}-analysis.json
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");
const mpImport = require("../../../index.js");

const VERSION = (() => { const i = process.argv.indexOf("--version"); return i > -1 ? parseInt(process.argv[i + 1], 10) : 1; })();
const WHICH = (() => { const i = process.argv.indexOf("--project"); return i > -1 ? process.argv[i + 1] : "both"; })();

const PROJECTS = {
	original: { secret: process.env.ORIGINAL_SECRET, project: process.env.ORIGINAL_PROJECT },
	simplified: { secret: process.env.SIMPLIFIED_SECRET, project: process.env.SIMPLIFIED_PROJECT },
};

async function exportProject(name) {
	const creds = PROJECTS[name];
	const dir = path.join(__dirname, "data", "exports", `${name}-v${VERSION}`);
	fs.rmSync(dir, { recursive: true, force: true });
	fs.mkdirSync(dir, { recursive: true });
	await mpImport(creds, null, {
		recordType: "export",
		start: "2026-07-01",
		end: "2026-08-15",
		where: dir,
		verbose: false,
		showProgress: false,
	});
	// collect every exported line
	const lines = [];
	for (const f of fs.readdirSync(dir)) {
		const txt = fs.readFileSync(path.join(dir, f), "utf8");
		for (const l of txt.split("\n")) { if (l.trim()) { try { lines.push(JSON.parse(l)); } catch { /* not json */ } } }
	}
	return { dir, lines };
}

function analyze(name, lines) {
	const a = {
		project: name, total: lines.length,
		byEvent: {}, verbSamples: {}, propKeys: {}, idResolution: {},
	};
	const propKeyCounts = {};
	for (const e of lines) {
		a.byEvent[e.event] = (a.byEvent[e.event] || 0) + 1;
		for (const k of Object.keys(e.properties || {})) propKeyCounts[k] = (propKeyCounts[k] || 0) + 1;
		if (e.event?.startsWith("$") && !a.verbSamples[e.event]) a.verbSamples[e.event] = e;
	}
	a.propKeys = Object.fromEntries(Object.entries(propKeyCounts).sort((x, y) => y[1] - x[1]));

	// per-scenario id resolution: distinct distinct_ids seen per scenario + sample rows
	const byScenario = {};
	for (const e of lines) {
		const s = e.properties?.scenario || "(none)";
		byScenario[s] ??= { events: 0, distinctIds: new Set(), sample: null, withDeviceId: 0, withUserId: 0, withBeforeIdentity: 0 };
		const b = byScenario[s];
		b.events++;
		b.distinctIds.add(e.properties?.distinct_id);
		if (e.properties?.$device_id !== undefined) b.withDeviceId++;
		if (e.properties?.$user_id !== undefined) b.withUserId++;
		if (e.properties?.$distinct_id_before_identity !== undefined) b.withBeforeIdentity++;
		if (!b.sample && !e.event?.startsWith("$")) b.sample = e;
	}
	a.idResolution = Object.fromEntries(Object.entries(byScenario).sort().map(([s, b]) => [s, {
		events: b.events,
		uniqueDistinctIds: b.distinctIds.size,
		distinctIds: [...b.distinctIds].slice(0, 8),
		withDeviceId: b.withDeviceId, withUserId: b.withUserId, withBeforeIdentity: b.withBeforeIdentity,
		sample: b.sample,
	}]));
	return a;
}

async function main() {
	const targets = WHICH === "both" ? ["original", "simplified"] : [WHICH];
	for (const name of targets) {
		if (!PROJECTS[name].secret) throw new Error(`missing env for ${name}`);
		console.log(`exporting ${name} (${PROJECTS[name].project})...`);
		const { dir, lines } = await exportProject(name);
		const analysis = analyze(name, lines);
		const outPath = path.join(__dirname, "results", `v${VERSION}-03-export-${name}-analysis.json`);
		fs.mkdirSync(path.dirname(outPath), { recursive: true });
		fs.writeFileSync(outPath, JSON.stringify(analysis, null, 2));
		console.log(`${name}: ${lines.length} events exported → ${dir}`);
		console.log(`  events by name: ${JSON.stringify(analysis.byEvent)}`);
		console.log(`  analysis → ${outPath}`);
	}
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
