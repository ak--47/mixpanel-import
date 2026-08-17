#!/usr/bin/env node
/**
 * Versioned identity torture-test dataset generator.
 * Original→Simplified ID merge research (plans/original-to-simplified).
 *
 * Deterministic: same --version → byte-identical output (ids, times, insert_ids).
 * Events + identity verbs for an ORIGINAL id-merge project; every event carries
 * dataVersion + scenario props so queries can filter per round (events are undeletable).
 *
 * usage: node generate-dataset.js [--version 1]
 * out:   ./data/v{N}-events.jsonl  +  ./data/v{N}-manifest.json
 */
const fs = require("fs");
const path = require("path");
const md5 = require("md5");

const VERSION = (() => {
	const i = process.argv.indexOf("--version");
	return i > -1 ? parseInt(process.argv[i + 1], 10) : 1;
})();

// deterministic anchor per version; all times in the 10 days before it (in retention, never future)
const ANCHOR = Date.UTC(2026, 7, 10, 0, 0, 0) / 1000 + (VERSION - 1) * 86400; // 2026-08-10 + (v-1) days
const DAY = 86400;

/** deterministic uuidv4-format id from a seed string */
function uuid(seed) {
	const h = md5(`v${VERSION}|${seed}`);
	return [
		h.slice(0, 8),
		h.slice(8, 12),
		"4" + h.slice(13, 16),
		((parseInt(h[16], 16) & 0x3) | 0x8).toString(16) + h.slice(17, 20),
		h.slice(20, 32),
	].join("-");
}
/** numeric user id: {version}{scenario 2d}{index 4d} — canonical ids are always numeric */
const userId = (scen, idx) => `${VERSION}${String(scen).padStart(2, "0")}${String(idx).padStart(4, "0")}`;

let seq = 0;
const events = [];
const NORMAL_EVENTS = ["page view", "button click", "sign up", "purchase", "level up"];

function push(event, distinctId, scen, tOffsetSec, extraProps = {}) {
	seq++;
	events.push({
		event,
		properties: {
			distinct_id: distinctId,
			time: Math.floor(ANCHOR - tOffsetSec),
			$insert_id: `v${VERSION}-s${String(scen).padStart(2, "0")}-${seq}`,
			dataVersion: VERSION,
			scenario: `s${String(scen).padStart(2, "0")}`,
			seq,
			...extraProps,
		},
	});
}
// normal event; name picked deterministically
const ev = (dId, scen, tOff, extra = {}) => push(NORMAL_EVENTS[seq % NORMAL_EVENTS.length], dId, scen, tOff, extra);
// identity verbs (original id merge)
const identify = (idfd, anon, scen, tOff) => push("$identify", idfd, scen, tOff, { $identified_id: idfd, $anon_id: anon });
const alias = (existing, aliasId, scen, tOff) => push("$create_alias", existing, scen, tOff, { alias: aliasId, distinct_id: existing });
const merge = (a, b, scen, tOff) => push("$merge", a, scen, tOff, { $distinct_ids: [a, b] });

const manifest = { version: VERSION, anchor: ANCHOR, anchorISO: new Date(ANCHOR * 1000).toISOString(), scenarios: {} };
function scenario(n, description, fn, expect) {
	const before = events.length;
	const ids = fn();
	manifest.scenarios[`s${String(n).padStart(2, "0")}`] = {
		description, ids, events: events.length - before, ...expect,
	};
}

/* ------------------------------------------------------------------ */
/* scenarios — each gets its own time band (day offset) so streams interleave when sorted */

// s01 simple identify: anon → identify → user
scenario(1, "simple: anon events, $identify, user events", () => {
	const U = userId(1, 1), A = uuid("s01|anon1");
	ev(A, 1, 9 * DAY); ev(A, 1, 9 * DAY - 3600); ev(A, 1, 9 * DAY - 7200);
	identify(U, A, 1, 9 * DAY - 10000);
	ev(U, 1, 9 * DAY - 14000); ev(U, 1, 8 * DAY); ev(U, 1, 7 * DAY);
	return { users: [U], anons: [A] };
}, {
	expectOriginal: "one cluster {U,A}, canonical likely U",
	expectReplay: "A→U association; all events resolve to U",
});

// s02 anon alias chain: A1←A2←A3, then identify(U, A1)
scenario(2, "alias chain anon→anon→anon then identify head", () => {
	const U = userId(2, 1), A1 = uuid("s02|a1"), A2 = uuid("s02|a2"), A3 = uuid("s02|a3");
	ev(A1, 2, 9 * DAY); ev(A1, 2, 9 * DAY - 1800); ev(A1, 2, 9 * DAY - 3600);
	alias(A1, A2, 2, 9 * DAY - 5400);
	ev(A2, 2, 9 * DAY - 7200); ev(A2, 2, 8.5 * DAY); ev(A2, 2, 8.4 * DAY);
	alias(A2, A3, 2, 8.3 * DAY);
	ev(A3, 2, 8.2 * DAY); ev(A3, 2, 8.1 * DAY); ev(A3, 2, 8 * DAY);
	identify(U, A1, 2, 7.9 * DAY);
	ev(U, 2, 7.8 * DAY); ev(U, 2, 7.5 * DAY); ev(U, 2, 7 * DAY);
	return { users: [U], anons: [A1, A2, A3] };
}, {
	expectOriginal: "PROBE: does identify on an aliased id merge whole chain? cluster {U,A1,A2,A3}?",
	expectReplay: "closure: A1→U, A2→U, A3→U (AK's exact bonus case)",
});

// s03 alias fan-in: two anons alias to same user
scenario(3, "alias fan-in: two anons → same user", () => {
	const U = userId(3, 1), A1 = uuid("s03|a1"), A2 = uuid("s03|a2");
	ev(A1, 3, 8 * DAY); ev(A1, 3, 8 * DAY - 3600); ev(A2, 3, 8 * DAY - 5400);
	ev(U, 3, 7.9 * DAY);
	alias(U, A1, 3, 7.8 * DAY);
	alias(U, A2, 3, 7.7 * DAY);
	ev(A1, 3, 7.6 * DAY); ev(A2, 3, 7.5 * DAY); ev(U, 3, 7.4 * DAY); ev(U, 3, 7.3 * DAY); ev(A2, 3, 7.2 * DAY);
	return { users: [U], anons: [A1, A2] };
}, {
	expectOriginal: "one cluster {U,A1,A2}",
	expectReplay: "A1→U, A2→U",
});

// s04 anon↔anon merge, never identified
scenario(4, "anon↔anon $merge, never identified", () => {
	const A1 = uuid("s04|a1"), A2 = uuid("s04|a2");
	ev(A1, 4, 7 * DAY); ev(A1, 4, 7 * DAY - 3600); ev(A2, 4, 7 * DAY - 5400); ev(A2, 4, 6.9 * DAY); ev(A2, 4, 6.8 * DAY);
	merge(A1, A2, 4, 6.7 * DAY);
	ev(A1, 4, 6.6 * DAY);
	return { users: [], anons: [A1, A2] };
}, {
	expectOriginal: "one anon-only cluster {A1,A2}",
	expectReplay: "INEXPRESSIBLE in simplified (no anon↔anon verb): both stay $device:, counted unresolvedAnonCluster",
});

// s05 anon merge chain then late identify — transitive closure showcase
scenario(5, "merge(A1,A2), merge(A2,A3), then identify(U,A1)", () => {
	const U = userId(5, 1), A1 = uuid("s05|a1"), A2 = uuid("s05|a2"), A3 = uuid("s05|a3");
	ev(A1, 5, 6.5 * DAY); ev(A1, 5, 6.4 * DAY); ev(A2, 5, 6.3 * DAY); ev(A2, 5, 6.2 * DAY); ev(A3, 5, 6.1 * DAY); ev(A3, 5, 6 * DAY);
	merge(A1, A2, 5, 5.9 * DAY);
	merge(A2, A3, 5, 5.8 * DAY);
	identify(U, A1, 5, 5.7 * DAY);
	ev(U, 5, 5.6 * DAY); ev(U, 5, 5.5 * DAY); ev(A3, 5, 5.4 * DAY);
	return { users: [U], anons: [A1, A2, A3] };
}, {
	expectOriginal: "one cluster {U,A1,A2,A3} via transitive merges",
	expectReplay: "closure: A1→U, A2→U, A3→U — stateless 1:1 only gets A1→U",
});

// s06 user↔user merge — multi-user cluster, unrepresentable in simplified
scenario(6, "two identified users then $merge(U1,U2)", () => {
	const U1 = userId(6, 1), U2 = userId(6, 2), A1 = uuid("s06|a1"), A2 = uuid("s06|a2");
	ev(A1, 6, 5.3 * DAY); ev(U1, 6, 5.2 * DAY); identify(U1, A1, 6, 5.1 * DAY);
	ev(A2, 6, 5 * DAY); ev(U2, 6, 4.9 * DAY); identify(U2, A2, 6, 4.8 * DAY);
	ev(U1, 6, 4.7 * DAY); ev(U2, 6, 4.6 * DAY);
	merge(U1, U2, 6, 4.5 * DAY);
	ev(U1, 6, 4.4 * DAY); ev(U2, 6, 4.3 * DAY);
	return { users: [U1, U2], anons: [A1, A2] };
}, {
	expectOriginal: "ONE cluster {U1,U2,A1,A2} — original allows multi-user clusters",
	expectReplay: "onAmbiguous: simplified allows exactly one $user_id/cluster; must pick winner + report",
});

// s07 big cluster: >500 ids — original caps at 500, replay should resolve all
scenario(7, "510 anons identified into one user (breaks 500-id cluster cap)", () => {
	const U = userId(7, 1);
	const anons = [];
	for (let i = 1; i <= 510; i++) {
		const A = uuid(`s07|a${i}`);
		anons.push(A);
		// spread across band; 1 anon event + identify each
		const t = 4.2 * DAY - i * 120;
		ev(A, 7, t);
		identify(U, A, 7, t - 60);
	}
	ev(U, 7, 3.4 * DAY); ev(U, 7, 3.39 * DAY); ev(U, 7, 3.38 * DAY); ev(U, 7, 3.37 * DAY); ev(U, 7, 3.36 * DAY);
	return { users: [U], anons: ["(510 generated: uuid seed s07|a1..a510)"] };
}, {
	expectOriginal: "cluster caps at 500 ids; ~last 11 anons orphaned (PROBE: strict errors? silent?)",
	expectReplay: "all 510 anons → U (no cap on device_ids in simplified) — the headline improvement",
});

// s08 non-uuidv4 anon id — original $identify should reject/no-op
scenario(8, "identify with non-uuidv4 $anon_id", () => {
	const U = userId(8, 1), A = `session_abc123_v${VERSION}`;
	ev(A, 8, 3.3 * DAY); ev(A, 8, 3.2 * DAY);
	identify(U, A, 8, 3.1 * DAY);
	ev(U, 8, 3 * DAY); ev(U, 8, 2.95 * DAY); ev(A, 8, 2.9 * DAY);
	return { users: [U], anons: [A] };
}, {
	expectOriginal: "PROBE: $identify requires uuidv4 $anon_id — expect no merge (error? silent?); A stays separate user",
	expectReplay: "A→U links fine (we don't care about uuid shape) — second headline improvement",
});

// s09 server-side identify (the Vipps checkout shape): fresh device per hit
scenario(9, "server-side shape: fresh device per event, identity only via verbs", () => {
	const U = userId(9, 1);
	const devices = [];
	for (let i = 1; i <= 5; i++) {
		const D = uuid(`s09|d${i}`);
		devices.push(D);
		ev(D, 9, 2.8 * DAY - i * 600);
		identify(U, D, 9, 2.8 * DAY - i * 600 - 60);
	}
	ev(U, 9, 2.5 * DAY); ev(U, 9, 2.4 * DAY); ev(U, 9, 2.3 * DAY);
	return { users: [U], anons: devices };
}, {
	expectOriginal: "one cluster {U + 5 devices}",
	expectReplay: "5 associations to U; without replay each device is a phantom",
});

// s10 sdk-shape dual-id rows — do reserved props do anything in ORIGINAL projects?
scenario(10, "events already carrying $device_id + $user_id (no verbs)", () => {
	const U = userId(10, 1), A = uuid("s10|a1");
	for (let i = 0; i < 5; i++) ev(U, 10, 2.2 * DAY - i * 900, { $device_id: A, $user_id: U });
	return { users: [U], anons: [A] };
}, {
	expectOriginal: "PROBE: do $device_id/$user_id props trigger any linking in original? (hypothesis: no)",
	expectReplay: "hard-truth evidence bucket: A→U from dual-id rows",
});

// s11 test-account pathology: one user hoovering devices
scenario(11, "pathological test account: 1 user, 40 devices", () => {
	const U = userId(11, 1);
	const devices = [];
	for (let i = 1; i <= 40; i++) {
		const D = uuid(`s11|d${i}`);
		devices.push(D);
		ev(D, 11, 2 * DAY - i * 300);
		identify(U, D, 11, 2 * DAY - i * 300 - 60);
	}
	ev(U, 11, 1.5 * DAY); ev(U, 11, 1.45 * DAY); ev(U, 11, 1.4 * DAY);
	return { users: [U], anons: ["(40 generated: uuid seed s11|d1..d40)"] };
}, {
	expectOriginal: "one 41-id cluster",
	expectReplay: "links fine but device-count outlier telemetry should flag U (denylist candidate)",
});

// s12 id collision: alias id that LOOKS like a user id (numeric) — isUserId ambiguity
scenario(12, "alias whose id passes isUserId (numeric) — ambiguous pair", () => {
	const U = userId(12, 1), FAKE = userId(12, 9999); // numeric string used as an alias/anon id
	ev(U, 12, 1.3 * DAY); ev(U, 12, 1.25 * DAY);
	alias(U, FAKE, 12, 1.2 * DAY);
	ev(FAKE, 12, 1.15 * DAY); ev(FAKE, 12, 1.1 * DAY); ev(U, 12, 1.05 * DAY);
	return { users: [U], anons: [FAKE] };
}, {
	expectOriginal: "one cluster {U, FAKE}",
	expectReplay: "both sides pass isUserId(/^\\d+$/) → onAmbiguous path exercised",
});

// s13 unlinked anon — control group
scenario(13, "anon events only, never linked", () => {
	const A = uuid("s13|a1");
	ev(A, 13, 1 * DAY); ev(A, 13, 0.95 * DAY); ev(A, 13, 0.9 * DAY); ev(A, 13, 0.85 * DAY);
	return { users: [], anons: [A] };
}, {
	expectOriginal: "single anon user",
	expectReplay: "distinct_id → $device:A; stays anonymous (correct)",
});

// s14 plain user — control group
scenario(14, "bare numeric distinct_id events only, no verbs", () => {
	const U = userId(14, 1);
	ev(U, 14, 0.8 * DAY); ev(U, 14, 0.75 * DAY); ev(U, 14, 0.7 * DAY); ev(U, 14, 0.65 * DAY);
	return { users: [U], anons: [] };
}, {
	expectOriginal: "single identified user",
	expectReplay: "isUserId ✓ → $user_id=U",
});

// s15 late verb: anon events long before the identify (retroactivity)
scenario(15, "events days before their $identify (retroactive merge check)", () => {
	const U = userId(15, 1), A = uuid("s15|a1");
	ev(A, 15, 9.5 * DAY); ev(A, 15, 9.45 * DAY); ev(A, 15, 9.4 * DAY);
	identify(U, A, 15, 0.5 * DAY);
	ev(U, 15, 0.45 * DAY); ev(U, 15, 0.4 * DAY); ev(U, 15, 0.35 * DAY);
	return { users: [U], anons: [A] };
}, {
	expectOriginal: "retroactive: A's day-9 events resolve to cluster after day-0 identify",
	expectReplay: "order-independent by design (graph flush at end)",
});

/* ------------------------------------------------------------------ */

events.sort((a, b) => a.properties.time - b.properties.time);
manifest.totalEvents = events.length;
manifest.timeRange = {
	from: new Date(events[0].properties.time * 1000).toISOString(),
	to: new Date(events[events.length - 1].properties.time * 1000).toISOString(),
};
manifest.verbCounts = events.reduce((acc, e) => {
	if (e.event.startsWith("$")) acc[e.event] = (acc[e.event] || 0) + 1;
	return acc;
}, {});

const outDir = path.join(__dirname, "data");
fs.mkdirSync(outDir, { recursive: true });
const evPath = path.join(outDir, `v${VERSION}-events.jsonl`);
const mfPath = path.join(outDir, `v${VERSION}-manifest.json`);
fs.writeFileSync(evPath, events.map(e => JSON.stringify(e)).join("\n") + "\n");
fs.writeFileSync(mfPath, JSON.stringify(manifest, null, 2));
console.log(`v${VERSION}: ${events.length} events (${JSON.stringify(manifest.verbCounts)})`);
console.log(`  ${evPath}\n  ${mfPath}`);
console.log(`  time range ${manifest.timeRange.from} → ${manifest.timeRange.to}`);
