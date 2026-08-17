#!/usr/bin/env node
/**
 * Probe 02: behavioral probes against the SIMPLIFIED project (4054681).
 * Each probe is a tiny targeted import; strict responses recorded per probe.
 *
 * p01 verb-identify   → is $identify ignored or rejected? (docs say ignored; Vipps report saw rejection)
 * p02 verb-alias      → same for $create_alias
 * p03 verb-merge      → same for $merge
 * p04 dual-id-merge   → custom event with $device_id+$user_id triggers merge (rule 1 live check)
 * p05 device-conflict → same $device_id later with a DIFFERENT $user_id (first-write-wins?)
 * p06 bare-prefixed   → bare distinct_id='$device:X' classified as device
 * p07 bare-anon       → bare uuid distinct_id (unprefixed) becomes a phantom USER (the bug we prevent)
 * p08 double-prefix   → $device_id sent WITH '$device:' prefix — does ingest strip or double-prefix?
 * p09 backdated-assoc → dual-id event timestamped BEFORE p10's events: retroactive stitch check
 * p10 assoc-after     → anon events first, dual-id assoc AFTER (normal retroactive direction)
 *
 * usage: node run-02-probe-simplified.js [--version 1]
 * out:   ./results/v{N}-02-probe-simplified.json
 */
require("dotenv").config({ path: require("path").join(__dirname, "../../../.env") });
const fs = require("fs");
const path = require("path");
const md5 = require("md5");
const mpImport = require("../../../index.js");

const VERSION = (() => {
	const i = process.argv.indexOf("--version");
	return i > -1 ? parseInt(process.argv[i + 1], 10) : 1;
})();
const { SIMPLIFIED_SECRET, SIMPLIFIED_PROJECT, SIMPLIFIED_TOKEN } = process.env;
if (!SIMPLIFIED_SECRET || !SIMPLIFIED_PROJECT) throw new Error("missing SIMPLIFIED_* env vars");

const ANCHOR = Date.UTC(2026, 7, 10, 0, 0, 0) / 1000 + (VERSION - 1) * 86400;
function uuid(seed) {
	const h = md5(`v${VERSION}|probe|${seed}`);
	return [h.slice(0, 8), h.slice(8, 12), "4" + h.slice(13, 16),
		((parseInt(h[16], 16) & 0x3) | 0x8).toString(16) + h.slice(17, 20), h.slice(20, 32)].join("-");
}
let seq = 0;
function mk(event, props) {
	seq++;
	return { event, properties: { time: ANCHOR - 1000 + seq * 10, $insert_id: `v${VERSION}-probe-${seq}`, dataVersion: VERSION, ...props } };
}

const U = (n) => `${VERSION}90${String(n).padStart(4, "0")}`; // probe users: {v}90NNNN

const probes = [
	{
		name: "p01-verb-identify",
		events: [mk("$identify", { distinct_id: U(1), $identified_id: U(1), $anon_id: uuid("p01a"), scenario: "p01" })],
	},
	{
		name: "p02-verb-alias",
		events: [mk("$create_alias", { distinct_id: U(2), alias: uuid("p02a"), scenario: "p02" })],
	},
	{
		name: "p03-verb-merge",
		events: [mk("$merge", { distinct_id: uuid("p03a"), $distinct_ids: [uuid("p03a"), uuid("p03b")], scenario: "p03" })],
	},
	{
		name: "p04-dual-id-merge",
		events: [
			mk("page view", { distinct_id: `$device:${uuid("p04d")}`, $device_id: uuid("p04d"), scenario: "p04" }),
			mk("identity association", { distinct_id: U(4), $device_id: uuid("p04d"), $user_id: U(4), scenario: "p04" }),
			mk("purchase", { distinct_id: U(4), $user_id: U(4), scenario: "p04" }),
		],
	},
	{
		name: "p05-device-conflict",
		events: [
			mk("identity association", { $device_id: uuid("p05d"), $user_id: U(51), scenario: "p05" }),
			mk("identity association", { $device_id: uuid("p05d"), $user_id: U(52), scenario: "p05" }),
			mk("page view", { $device_id: uuid("p05d"), scenario: "p05" }),
		],
	},
	{
		name: "p06-bare-prefixed",
		events: [mk("page view", { distinct_id: `$device:${uuid("p06d")}`, scenario: "p06" })],
	},
	{
		name: "p07-bare-anon-unprefixed",
		events: [mk("page view", { distinct_id: uuid("p07a"), scenario: "p07" })],
	},
	{
		name: "p08-double-prefix",
		events: [mk("identity association", { $device_id: `$device:${uuid("p08d")}`, $user_id: U(8), scenario: "p08" })],
	},
	{
		name: "p09-backdated-assoc",
		events: [
			// anon events "now"-ish (band ANCHOR), assoc event backdated 30 days before ANCHOR
			mk("page view", { distinct_id: `$device:${uuid("p09d")}`, $device_id: uuid("p09d"), scenario: "p09" }),
			{ event: "identity association", properties: { time: ANCHOR - 30 * 86400, $insert_id: `v${VERSION}-probe-backdate`, dataVersion: VERSION, $device_id: uuid("p09d"), $user_id: U(9), scenario: "p09" } },
		],
	},
	{
		name: "p10-assoc-after",
		events: [
			mk("page view", { $device_id: uuid("p10d"), scenario: "p10" }),
			mk("button click", { $device_id: uuid("p10d"), scenario: "p10" }),
			mk("identity association", { $device_id: uuid("p10d"), $user_id: U(10), scenario: "p10" }),
		],
	},
];

async function main() {
	const out = { version: VERSION, ranAt: new Date().toISOString(), probes: {} };
	for (const p of probes) {
		try {
			const res = await mpImport(
				{ secret: SIMPLIFIED_SECRET, project: SIMPLIFIED_PROJECT, token: SIMPLIFIED_TOKEN },
				p.events,
				{ recordType: "event", workers: 1, strict: true, fixData: false, dedupe: false, verbose: false, showProgress: false, abridged: false }
			);
			out.probes[p.name] = {
				sent: p.events.length,
				success: res.success, failed: res.failed, empty: res.empty,
				responses: res.responses, errors: res.errors,
				sentEvents: p.events,
			};
			console.log(`${p.name}: sent=${p.events.length} success=${res.success} failed=${res.failed}`);
		} catch (e) {
			out.probes[p.name] = { error: String(e), sentEvents: p.events };
			console.log(`${p.name}: THREW ${e}`);
		}
	}
	const scrubbed = JSON.parse(JSON.stringify(out, (k, v) =>
		["secret", "token", "pass", "acct", "bearer", "auth"].includes(k.toLowerCase()) ? undefined : v
	));
	const outDir = path.join(__dirname, "results");
	fs.mkdirSync(outDir, { recursive: true });
	const outPath = path.join(outDir, `v${VERSION}-02-probe-simplified.json`);
	fs.writeFileSync(outPath, JSON.stringify(scrubbed, null, 2));
	console.log(`→ ${outPath}`);
}
main().then(() => process.exit(0)).catch(e => { console.error(e); process.exit(1); });
