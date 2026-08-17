#!/usr/bin/env node
/**
 * Probe 06: cluster-membership spot checks in the SIMPLIFIED project via activity stream.
 * The headline claims:
 *   (a) s07 anons ORPHANED by original's 500-id cluster cap resolve to the user after replay
 *   (b) s08 non-uuidv4 anon id (rejected by original's $identify) resolves after replay
 *   (c) s02 alias-chain anons (transitive) resolve
 *   (d) s04 anon-merge stays anonymous (inexpressible — control)
 * Querying by the ANON id: a merged cluster returns the canonical user's stream.
 *
 * usage: node run-06-spotchecks.js [--version 1]
 * out:   ./results/v{N}-06-spotchecks.json
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");
const md5 = require("md5");

const VERSION = (() => { const i = process.argv.indexOf("--version"); return i > -1 ? parseInt(process.argv[i + 1], 10) : 1; })();
const BASE = "https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app";
const { AK_BEARER_TOKEN, SIMPLIFIED_PROJECT } = process.env;

function uuid(seed) {
	const h = md5(`v${VERSION}|${seed}`);
	return [h.slice(0, 8), h.slice(8, 12), "4" + h.slice(13, 16),
		((parseInt(h[16], 16) & 0x3) | 0x8).toString(16) + h.slice(17, 20), h.slice(20, 32)].join("-");
}
const userId = (scen, idx) => `${VERSION}${String(scen).padStart(2, "0")}${String(idx).padStart(4, "0")}`;

async function activity(distinctId) {
	const res = await fetch(`${BASE}/query/getActivityStream`, {
		method: "POST",
		headers: { "Authorization": `Bearer ${AK_BEARER_TOKEN}`, "Content-Type": "application/json" },
		body: JSON.stringify({
			project_id: Number(SIMPLIFIED_PROJECT), client_id: "orig-simpl-spotcheck",
			distinct_ids: [distinctId], from_date: "2026-07-01", to_date: "2026-08-16",
		}),
	});
	const body = await res.json();
	const events = body?.results?.events || [];
	return {
		count: events.length,
		resolvedTo: [...new Set(events.map((e) => e.properties?.distinct_id))],
		scenarios: [...new Set(events.map((e) => e.properties?.scenario))],
	};
}

async function main() {
	const checks = [
		// (a) s07: anon #505 was identified AFTER the 500-cap → orphaned in original
		{ name: "s07-over-cap-anon(505)", id: `$device:${uuid("s07|a505")}`, expect: `resolves to ${userId(7, 1)}` },
		{ name: "s07-over-cap-anon(510)", id: `$device:${uuid("s07|a510")}`, expect: `resolves to ${userId(7, 1)}` },
		{ name: "s07-early-anon(1)", id: `$device:${uuid("s07|a1")}`, expect: `resolves to ${userId(7, 1)}` },
		// (b) s08: non-uuidv4 anon — original refused the $identify
		{ name: "s08-non-uuid-anon", id: `$device:session_abc123_v${VERSION}`, expect: `resolves to ${userId(8, 1)}` },
		// (c) s02: alias chain — transitive closure
		{ name: "s02-chain-a1", id: `$device:${uuid("s02|a1")}`, expect: `resolves to ${userId(2, 1)}` },
		{ name: "s02-chain-a3", id: `$device:${uuid("s02|a3")}`, expect: `resolves to ${userId(2, 1)}` },
		// (d) s04: anon↔anon merge — stays anonymous (control)
		{ name: "s04-anon-merge-a1", id: `$device:${uuid("s04|a1")}`, expect: "stays $device: (anon-only cluster)" },
		// direct user check
		{ name: "s07-user", id: userId(7, 1), expect: "full merged stream (1000+ events)" },
	];
	const out = { version: VERSION, ranAt: new Date().toISOString(), checks: {} };
	for (const c of checks) {
		try {
			out.checks[c.name] = { queried: c.id, expect: c.expect, ...(await activity(c.id)) };
		} catch (e) {
			out.checks[c.name] = { queried: c.id, error: String(e).slice(0, 200) };
		}
		const r = out.checks[c.name];
		console.log(`${c.name}: events=${r.count ?? "ERR"} resolvedTo=${JSON.stringify(r.resolvedTo ?? r.error)} (expect: ${c.expect})`);
	}
	const outPath = path.join(__dirname, "results", `v${VERSION}-06-spotchecks.json`);
	fs.mkdirSync(path.dirname(outPath), { recursive: true });
	fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
	console.log(`→ ${outPath}`);
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
