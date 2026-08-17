// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */
/* cSpell:disable */
const { Readable } = require("stream");
const md5 = require("md5");
const fs = require("fs");
const path = require("path");
const os = require("os");

const {
	createIdentityReplay,
	normalizeOptions,
	classifyRecord,
	rewriteEvent,
	buildAssociationEvent
} = require("../components/identity-replay.js");

// users are numeric strings; everything else is anonymous
const isUserId = (id) => /^\d+$/.test(id);

/** run records through the stage; resolves { out, stats } (job.identityReplayStats) */
function runStage(irOpts, records) {
	return new Promise((resolve, reject) => {
		const job = { identityReplay: irOpts, highWater: 16 };
		const stage = createIdentityReplay(job);
		const out = [];
		stage.on("data", (d) => out.push(d));
		stage.on("error", (err) => {
			err.stats = job.identityReplayStats;
			reject(err);
		});
		stage.on("end", () => resolve({ out, stats: job.identityReplayStats }));
		// the stage mutates records in place (repo transform idiom) — clone so shared fixtures stay pristine
		Readable.from(records.map((r) => structuredClone(r))).pipe(stage);
	});
}

const assocOnly = (out) => out.filter((r) => r.event === "identity association");
const ordinaryOnly = (out) => out.filter((r) => r.event !== "identity association");

describe("normalizeOptions", () => {
	test("throws on missing isUserId", () => {
		expect(() => normalizeOptions({})).toThrow(/isUserId is required/);
		expect(() => normalizeOptions({ isUserId: null })).toThrow(/isUserId is required/);
	});

	test("throws on non-object options", () => {
		expect(() => normalizeOptions(null)).toThrow(/options object/);
		expect(() => normalizeOptions([])).toThrow(/options object/);
	});

	test("compiles a regex string", () => {
		const opts = normalizeOptions({ isUserId: "^\\d+$" });
		expect(opts.isUserId("42")).toBe(true);
		expect(opts.isUserId("anon-1")).toBe(false);
	});

	test("compiles a RegExp (drops sticky/global statefulness)", () => {
		const opts = normalizeOptions({ isUserId: /^\d+$/g });
		expect(opts.isUserId("42")).toBe(true);
		expect(opts.isUserId("42")).toBe(true); // no lastIndex carryover
	});

	test("accepts a function as-is", () => {
		const fn = (id) => id === "yes";
		const opts = normalizeOptions({ isUserId: fn });
		expect(opts.isUserId).toBe(fn);
	});

	test("throws on an invalid regex string", () => {
		expect(() => normalizeOptions({ isUserId: "([" })).toThrow(/not a valid regex/);
	});

	test("throws on a non-callable isUserId", () => {
		expect(() => normalizeOptions({ isUserId: 42 })).toThrow(/must be a function/);
	});

	test("applies defaults", () => {
		const opts = normalizeOptions({ isUserId });
		expect(opts.graph).toBe(true);
		expect(opts.maxGraphSize).toBe(5_000_000);
		expect(opts.onGraphOverflow).toBe("warn");
		expect(opts.identityEvents).toBe("rewrite");
		expect(opts.associationEventName).toBe("identity association");
		expect(opts.associationTimestamp).toBe("original");
		expect(opts.associationProps).toEqual({});
		expect(opts.bareDistinctId).toBe("validate");
		expect(opts.userIdFallbackProps).toEqual([]);
		expect(opts.denylist).toEqual(new Set());
		expect(opts.onAmbiguous).toBe("drop");
		expect(opts.minAssociationRate).toBe(0);
		expect(opts.scrubExportProps).toBe(true);
		expect(opts.graphPath).toBe("");
	});

	test("rejects bad enums", () => {
		expect(() => normalizeOptions({ isUserId, identityEvents: "emit" })).toThrow(/identityEvents/);
		expect(() => normalizeOptions({ isUserId, associationTimestamp: "never" })).toThrow(/associationTimestamp/);
		expect(() => normalizeOptions({ isUserId, bareDistinctId: "yolo" })).toThrow(/bareDistinctId/);
		expect(() => normalizeOptions({ isUserId, onAmbiguous: "first" })).toThrow(/onAmbiguous/);
		expect(() => normalizeOptions({ isUserId, onGraphOverflow: "explode" })).toThrow(/onGraphOverflow/);
	});

	test("rejects bad numbers", () => {
		expect(() => normalizeOptions({ isUserId, maxGraphSize: 0 })).toThrow(/maxGraphSize/);
		expect(() => normalizeOptions({ isUserId, minAssociationRate: 2 })).toThrow(/minAssociationRate/);
		expect(() => normalizeOptions({ isUserId, minAssociationRate: -1 })).toThrow(/minAssociationRate/);
	});

	test("denylist entries are stored $device:-stripped", () => {
		const opts = normalizeOptions({ isUserId, denylist: ["$device:bad-guy", "worse-guy"] });
		expect(opts.denylist.has("bad-guy")).toBe(true);
		expect(opts.denylist.has("worse-guy")).toBe(true);
	});
});

describe("rewriteEvent (design table)", () => {
	const opts = normalizeOptions({
		isUserId,
		userIdFallbackProps: ["uid"],
		denylist: ["bad-guy"]
	});

	test("dual-ID row: strips $device: from $device_id, distinct_id follows $user_id", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "whatever", $user_id: "42", $device_id: "$device:devX", time: 1 }, opts);
		expect(rec.$device_id).toBe("devX");
		expect(rec.$user_id).toBe("42");
		expect(rec.distinct_id).toBe("42");
		expect(rec.$id_replay_source).toBe("dual-row");
	});

	test("device-only row: distinct_id becomes $device:-prefixed stripped id", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "whatever", $device_id: "$device:devY" }, opts);
		expect(rec.$device_id).toBe("devY");
		expect(rec.distinct_id).toBe("$device:devY");
		expect(rec.$id_replay_source).toBe("dual-row");
	});

	test("bare distinct_id, isUserId passes → $user_id", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "42", time: 1 }, opts);
		expect(rec.$user_id).toBe("42");
		expect(rec.distinct_id).toBe("42");
		expect(rec.$id_replay_source).toBe("bare-user");
	});

	test("bare distinct_id, isUserId fails → $device_id + prefixed distinct_id", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "anonA", time: 1 }, opts);
		expect(rec.$device_id).toBe("anonA");
		expect(rec.distinct_id).toBe("$device:anonA");
		expect(rec.$id_replay_source).toBe("bare-device");
	});

	test("already-prefixed distinct_id: stripped once, treated as device, never double-prefixed", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "$device:devZ" }, opts);
		expect(rec.$device_id).toBe("devZ");
		expect(rec.distinct_id).toBe("$device:devZ");
		expect(rec.$id_replay_source).toBe("bare-device");
	});

	test("double-prefixed distinct_id ($device:$device:) strips ALL repeats", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "$device:$device:xyz" }, opts);
		expect(rec.$device_id).toBe("xyz");
		expect(rec.distinct_id).toBe("$device:xyz");
	});

	test("prefixed distinct_id is a device even when the stripped value passes isUserId", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "$device:42" }, opts);
		expect(rec.$device_id).toBe("42");
		expect(rec.$user_id).toBeUndefined();
		expect(rec.distinct_id).toBe("$device:42");
	});

	test("as-ingested id prefers $distinct_id_before_identity over exported distinct_id", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "cluster-canonical", $distinct_id_before_identity: "anonB" }, opts);
		expect(rec.$device_id).toBe("anonB");
		expect(rec.distinct_id).toBe("$device:anonB");
	});

	test("before_identity that passes isUserId → bare-user (never trust exported distinct_id)", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "some-anon-canonical", $distinct_id_before_identity: "42" }, opts);
		expect(rec.$user_id).toBe("42");
		expect(rec.distinct_id).toBe("42");
		expect(rec.$id_replay_source).toBe("bare-user");
	});

	test("userIdFallbackProps: passing + differing value makes a fallback-prop dual row", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "anonC", uid: "77" }, opts);
		expect(rec.$user_id).toBe("77");
		expect(rec.$device_id).toBe("anonC");
		expect(rec.distinct_id).toBe("77");
		expect(rec.$id_replay_source).toBe("fallback-prop");
	});

	test("userIdFallbackProps: value that fails isUserId is ignored", () => {
		const rec = rewriteEvent({ event: "e", distinct_id: "anonC", uid: "not-a-user" }, opts);
		expect(rec.$user_id).toBeUndefined();
		expect(rec.$id_replay_source).toBe("bare-device");
	});

	test("userIdFallbackProps: value equal to the as-ingested id is ignored", () => {
		const numericOpts = normalizeOptions({ isUserId, userIdFallbackProps: ["uid"] });
		// as-ingested passes isUserId, so it's bare-user; uid identical adds nothing
		const rec = rewriteEvent({ event: "e", distinct_id: "42", uid: "42" }, numericOpts);
		expect(rec.$id_replay_source).toBe("bare-user");
	});

	test("denylisted as-ingested id → null (dropped)", () => {
		expect(rewriteEvent({ event: "e", distinct_id: "bad-guy" }, opts)).toBeNull();
		expect(rewriteEvent({ event: "e", distinct_id: "$device:bad-guy" }, opts)).toBeNull();
	});

	test("denylisted $user_id / $device_id → null (dropped)", () => {
		expect(rewriteEvent({ event: "e", distinct_id: "x", $user_id: "bad-guy", $device_id: "d" }, opts)).toBeNull();
		expect(rewriteEvent({ event: "e", distinct_id: "x", $user_id: "42", $device_id: "$device:bad-guy" }, opts)).toBeNull();
	});

	test("bareDistinctId: 'passthru' leaves bare events untouched", () => {
		const passthru = normalizeOptions({ isUserId, bareDistinctId: "passthru" });
		const rec = rewriteEvent({ event: "e", distinct_id: "anonA" }, passthru);
		expect(rec.$device_id).toBeUndefined();
		expect(rec.distinct_id).toBe("anonA");
		expect(rec.$id_replay_source).toBeUndefined();
	});

	test("works on the nested {event, properties} shape too", () => {
		const rec = rewriteEvent({ event: "e", properties: { distinct_id: "anonN", time: 9 } }, opts);
		expect(rec.properties.$device_id).toBe("anonN");
		expect(rec.properties.distinct_id).toBe("$device:anonN");
	});
});

describe("classifyRecord (verb + evidence shapes)", () => {
	const opts = normalizeOptions({ isUserId, userIdFallbackProps: ["uid"], denylist: ["bad-guy"] });

	test("$identify → [$identified_id, $anon_id] rank 1, device side stripped", () => {
		const c = classifyRecord({ event: "$identify", $identified_id: "42", $anon_id: "$device:anonA", time: 5 }, opts);
		expect(c.kind).toBe("identify");
		expect(c.rewrite).toBe(false);
		expect(c.edges).toEqual([["42", "anonA", 1]]);
	});

	test("$create_alias prefers $distinct_id_before_identity for the distinct_id side", () => {
		const c = classifyRecord({ event: "$create_alias", distinct_id: "cluster-canonical", $distinct_id_before_identity: "orig", alias: "Y" }, opts);
		expect(c.kind).toBe("alias");
		expect(c.edges).toEqual([["orig", "Y", 1]]);
	});

	test("$create_alias falls back to distinct_id when before_identity is absent", () => {
		const c = classifyRecord({ event: "$create_alias", distinct_id: "X", alias: "Y" }, opts);
		expect(c.edges).toEqual([["X", "Y", 1]]);
	});

	test("$merge with exactly 2 $distinct_ids → rank 1 edge", () => {
		const c = classifyRecord({ event: "$merge", $distinct_ids: ["a", "b"] }, opts);
		expect(c.kind).toBe("merge");
		expect(c.edges).toEqual([["a", "b", 1]]);
	});

	test("$merge with anything other than exactly 2 ids → no edge", () => {
		expect(classifyRecord({ event: "$merge", $distinct_ids: ["a", "b", "c"] }, opts).edges).toEqual([]);
		expect(classifyRecord({ event: "$merge", $distinct_ids: ["a"] }, opts).edges).toEqual([]);
		expect(classifyRecord({ event: "$merge" }, opts).edges).toEqual([]);
	});

	test("ordinary event with $user_id + $device_id → rank 0 (hard) edge", () => {
		const c = classifyRecord({ event: "e", $user_id: "42", $device_id: "$device:d1" }, opts);
		expect(c.kind).toBe("event");
		expect(c.rewrite).toBe(true);
		expect(c.edges).toEqual([["42", "d1", 0]]);
	});

	test("ordinary bare event → no edges", () => {
		const c = classifyRecord({ event: "e", distinct_id: "anonZ" }, opts);
		expect(c.edges).toEqual([]);
	});

	test("ordinary bare event + fallback prop → rank 2 edge", () => {
		const c = classifyRecord({ event: "e", distinct_id: "anonZ", uid: "88" }, opts);
		expect(c.edges).toEqual([["88", "anonZ", 2]]);
	});

	test("fallback prop is ignored when it equals the as-ingested id or fails isUserId", () => {
		expect(classifyRecord({ event: "e", distinct_id: "anonZ", uid: "anonZ" }, opts).edges).toEqual([]);
		expect(classifyRecord({ event: "e", distinct_id: "anonZ", uid: "not-a-user" }, opts).edges).toEqual([]);
	});

	test("denylisted id kills the edge and is counted", () => {
		const c = classifyRecord({ event: "$identify", $identified_id: "42", $anon_id: "bad-guy" }, opts);
		expect(c.edges).toEqual([]);
		expect(c.denylisted).toBe(1);
	});

	test("verb props read from nested {event, properties} shape", () => {
		const c = classifyRecord({ event: "$identify", properties: { $identified_id: "42", $anon_id: "anonA" } }, opts);
		expect(c.edges).toEqual([["42", "anonA", 1]]);
	});

	test("self-edges are discarded", () => {
		const c = classifyRecord({ event: "$merge", $distinct_ids: ["same", "$device:same"] }, opts);
		expect(c.edges).toEqual([]);
	});
});

describe("buildAssociationEvent", () => {
	const opts = normalizeOptions({ isUserId });

	test("deterministic $insert_id = md5(user + '|' + strippedDevice), 32 hex chars", () => {
		const a = buildAssociationEvent("42", "$device:devQ", opts, { ts: 111, floorTs: 1, source: "verb" });
		expect(a.properties.$insert_id).toBe(md5("42|devQ"));
		expect(a.properties.$insert_id).toMatch(/^[a-f0-9]{32}$/);
		// same pair, different prefix representation → same id
		const b = buildAssociationEvent("42", "devQ", opts, { ts: 999, floorTs: 2, source: "closure" });
		expect(b.properties.$insert_id).toBe(a.properties.$insert_id);
	});

	test("shape: name, dual IDs, distinct_id = user, source metadata", () => {
		const a = buildAssociationEvent("42", "$device:$device:devQ", opts, { ts: 111, floorTs: 1, source: "closure" });
		expect(a.event).toBe("identity association");
		expect(a.properties.$user_id).toBe("42");
		expect(a.properties.$device_id).toBe("devQ"); // all prefix repeats stripped
		expect(a.properties.distinct_id).toBe("42");
		expect(a.properties.$id_replay_source).toBe("closure");
	});

	test("associationTimestamp 'original' uses meta.ts; 'floor' uses meta.floorTs", () => {
		const orig = buildAssociationEvent("42", "d", opts, { ts: 500, floorTs: 100 });
		expect(orig.properties.time).toBe(500);
		const floorOpts = normalizeOptions({ isUserId, associationTimestamp: "floor" });
		const floored = buildAssociationEvent("42", "d", floorOpts, { ts: 500, floorTs: 100 });
		expect(floored.properties.time).toBe(100);
	});

	test("junk ids are neutralized, never graphed, and never union users (mega-cluster guard)", async () => {
		const ZERO = "00000000-0000-0000-0000-000000000000";
		const { out, stats } = await runStage({ isUserId }, [
			// two REAL users sharing a junk $device_id — must NOT union
			{ event: "login", properties: { distinct_id: "111", $user_id: "111", $device_id: ZERO, time: 1000 } },
			{ event: "login", properties: { distinct_id: "222", $user_id: "222", $device_id: ZERO, time: 2000 } },
			// junk distinct_id → '' sink, not '$device:null'
			{ event: "page view", properties: { distinct_id: "null", time: 3000 } },
			// verb whose anon side is junk → edge suppressed silently, record swallowed as usual
			{ event: "$identify", properties: { distinct_id: "333", $identified_id: "333", $anon_id: "anonymous", time: 4000 } },
		]);
		const props = (l) => l.properties || l;
		const logins = out.filter((r) => r.event === "login");
		expect(logins.length).toBe(2);
		for (const l of logins) expect(props(l).$device_id).toBeUndefined(); // junk prop removed
		const pv = out.find((r) => r.event === "page view");
		expect(props(pv).distinct_id).toBe(""); // sink, not a phantom '$device:null' person
		expect(assocOnly(out).length).toBe(0); // no association ever touches a junk id
		expect(stats.junkNeutralized).toBe(3);
		expect(stats.clusters.multiUser).toBe(0); // 111 and 222 NOT unioned
	});

	test("scrubJunkIds: false restores raw behavior; matching is case-insensitive like ingestion", async () => {
		const opts = normalizeOptions({ isUserId, scrubJunkIds: false });
		expect(opts.junkIds.size).toBe(0);
		// ingestion's IsBadID lowercases before comparing — so do we
		const { out } = await runStage({ isUserId }, [
			{ event: "page view", properties: { distinct_id: "ANONYMOUS", time: 1000 } },
			{ event: "page view", properties: { distinct_id: "NULL", time: 2000 } },
		]);
		for (const r of out) expect((r.properties || r).distinct_id).toBe("");
	});

	test("electionScope 'device': anons resolve to THEIR user in a multi-user cluster", async () => {
		// U1 owns A/B, U2 owns C, shared device D bridges the two users into one cluster
		const recs = [
			{ event: "$identify", properties: { distinct_id: "111", $identified_id: "111", $anon_id: "devA", time: 1000 } },
			{ event: "$identify", properties: { distinct_id: "111", $identified_id: "111", $anon_id: "devB", time: 2000 } },
			{ event: "$identify", properties: { distinct_id: "222", $identified_id: "222", $anon_id: "devC", time: 3000 } },
			{ event: "$identify", properties: { distinct_id: "111", $identified_id: "111", $anon_id: "devD", time: 4000 } },
			{ event: "$identify", properties: { distinct_id: "222", $identified_id: "222", $anon_id: "devD", time: 5000 } }, // shared device
		];
		// cluster scope (default) + resolve: ALL devices go to one elected winner
		const clusterScope = await runStage({ isUserId, onAmbiguous: "resolve" }, recs);
		const cWinners = new Set(assocOnly(clusterScope.out).map((a) => a.properties.$user_id));
		expect(cWinners.size).toBe(1);
		// device scope: each device follows its own direct evidence
		const deviceScope = await runStage({ isUserId, onAmbiguous: "resolve", electionScope: "device" }, recs);
		const byDev = Object.fromEntries(assocOnly(deviceScope.out).map((a) => [a.properties.$device_id, a.properties.$user_id]));
		expect(byDev.devA).toBe("111");
		expect(byDev.devB).toBe("111");
		expect(byDev.devC).toBe("222");
		expect(byDev.devD).toBe("222"); // shared device: latest direct evidence wins (ts 5000)
	});

	test("electionScope 'device' + onAmbiguous 'drop': direct-evidence anons still resolve, fallback anons don't", async () => {
		const recs = [
			{ event: "$identify", properties: { distinct_id: "111", $identified_id: "111", $anon_id: "devA", time: 1000 } },
			{ event: "$identify", properties: { distinct_id: "222", $identified_id: "222", $anon_id: "devA", time: 2000 } }, // multi-user via shared device
			{ event: "$merge", properties: { distinct_id: "devA", $distinct_ids: ["devA", "devX"], time: 3000 } }, // devX: no direct user evidence
		];
		const { out, stats } = await runStage({ isUserId, onAmbiguous: "drop", electionScope: "device" }, recs);
		const byDev = Object.fromEntries(assocOnly(out).map((a) => [a.properties.$device_id, a.properties.$user_id]));
		expect(byDev.devA).toBe("222"); // direct evidence, latest wins
		expect(byDev.devX).toBeUndefined(); // fallback-only anon dropped under 'drop'
		expect(stats.unresolvedAnonIds).toBe(1);
	});

	test("pinned numeric associationTimestamp stamps every assoc event with that epoch", async () => {
		const PIN = 1600000000;
		const { out } = await runStage({ isUserId, associationTimestamp: PIN }, [
			{ event: "$merge", properties: { distinct_id: "anon1", $distinct_ids: ["anon1", "anon2"], time: 2000 } },
			{ event: "$identify", properties: { distinct_id: "42", $identified_id: "42", $anon_id: "anon2", time: 4000 } },
		]);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(2);
		for (const a of assoc) expect(a.properties.time).toBe(PIN);
		expect(() => normalizeOptions({ isUserId, associationTimestamp: -5 })).toThrow(/positive epoch/);
	});

	test("scrubExportProps strips Mixpanel raw-export junk (and can be disabled)", () => {
		const opts = normalizeOptions({ isUserId });
		const rec = { event: "page view", properties: { distinct_id: "42", time: 1, $insert_id: "x", $import: true, $mp_api_endpoint: "api.mixpanel.com", $mp_api_timestamp_ms: 2, $mp_event_size: 3, mp_processing_time_ms: 4, keep: "me" } };
		const out = rewriteEvent(structuredClone(rec), opts);
		expect(out.properties.$import).toBeUndefined();
		expect(out.properties.$mp_api_endpoint).toBeUndefined();
		expect(out.properties.$mp_api_timestamp_ms).toBeUndefined();
		expect(out.properties.$mp_event_size).toBeUndefined();
		expect(out.properties.mp_processing_time_ms).toBeUndefined();
		expect(out.properties.keep).toBe("me");
		const keepOpts = normalizeOptions({ isUserId, scrubExportProps: false });
		const kept = rewriteEvent(structuredClone(rec), keepOpts);
		expect(kept.properties.$import).toBe(true);
	});

	test("custom associationEventName + associationProps merged", () => {
		const custom = normalizeOptions({ isUserId, associationEventName: "stitch", associationProps: { dataVersion: 2 } });
		const a = buildAssociationEvent("42", "d", custom, { ts: 1, floorTs: 0, source: "verb" });
		expect(a.event).toBe("stitch");
		expect(a.properties.dataVersion).toBe(2);
	});
});

describe("stage: graph mode flush closure", () => {
	// AK's exact case: anon1—anon2—anon3—user chain must yield assoc events for ALL THREE anons
	const chain = [
		{ event: "page view", distinct_id: "anon1", time: 1000 },
		{ event: "$merge", $distinct_ids: ["anon1", "anon2"], time: 2000 },
		{ event: "page view", distinct_id: "anon2", time: 2100 },
		{ event: "$merge", $distinct_ids: ["anon2", "anon3"], time: 3000 },
		{ event: "$identify", $identified_id: "42", $anon_id: "anon3", time: 4000 },
		{ event: "purchase", distinct_id: "42", time: 5000 }
	];

	test("chain anon1-anon2-anon3-user emits assoc events for all three anons", async () => {
		const { out, stats } = await runStage({ isUserId }, chain);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(3);
		expect(assoc.map((a) => a.properties.$device_id).sort()).toEqual(["anon1", "anon2", "anon3"]);
		for (const a of assoc) expect(a.properties.$user_id).toBe("42");
		expect(stats.verbsSeen).toEqual({ identify: 1, alias: 0, merge: 2 });
		expect(stats.clusters).toEqual({ total: 1, resolved: 1, anonOnly: 0, multiUser: 0 });
	});

	test("verbs are swallowed; ordinary events pass rewritten 1:1", async () => {
		const { out } = await runStage({ isUserId }, chain);
		const ordinary = ordinaryOnly(out);
		expect(ordinary.length).toBe(3);
		expect(out.some((r) => r.event === "$merge" || r.event === "$identify")).toBe(false);
		const pv1 = ordinary.find((r) => (r.distinct_id === "$device:anon1"));
		expect(pv1.$device_id).toBe("anon1");
		expect(pv1.$id_replay_source).toBe("bare-device");
	});

	test("source metadata: direct verb pair = 'verb', transitive = 'closure'", async () => {
		const { out } = await runStage({ isUserId }, chain);
		const bySrc = Object.fromEntries(assocOnly(out).map((a) => [a.properties.$device_id, a.properties.$id_replay_source]));
		expect(bySrc.anon3).toBe("verb"); // directly $identify'd to 42
		expect(bySrc.anon1).toBe("closure");
		expect(bySrc.anon2).toBe("closure");
	});

	test("'original' timestamps = first EVIDENCE sighting of each device node", async () => {
		// bare sightings deliberately do NOT track ts (memory: the graph is the only store,
		// and retroactive stitching is order/time-independent — probe p09) — firstTs comes
		// from the node's first verb/dual-row appearance
		const { out } = await runStage({ isUserId }, chain);
		const byDev = Object.fromEntries(assocOnly(out).map((a) => [a.properties.$device_id, a.properties.time]));
		expect(byDev.anon1).toBe(2000); // first evidence: the $merge at t=2000
		expect(byDev.anon2).toBe(2000);
		expect(byDev.anon3).toBe(3000);
	});

	test("'floor' timestamps = (min event time seen) - 86400 on every assoc event", async () => {
		const { out } = await runStage({ isUserId, associationTimestamp: "floor" }, chain);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(3);
		for (const a of assoc) expect(a.properties.time).toBe(1000 - 86400);
	});

	test("deterministic $insert_id survives re-runs", async () => {
		const one = await runStage({ isUserId }, chain);
		const two = await runStage({ isUserId }, chain);
		const ids = (res) => assocOnly(res.out).map((a) => a.properties.$insert_id).sort();
		expect(ids(one)).toEqual(ids(two));
		expect(ids(one)).toContain(md5("42|anon1"));
	});

	test("repeated verbs dedupe into one assoc event per pair", async () => {
		const { out } = await runStage({ isUserId }, [
			{ event: "$identify", $identified_id: "42", $anon_id: "anonR", time: 100 },
			{ event: "$identify", $identified_id: "42", $anon_id: "anonR", time: 200 }
		]);
		expect(assocOnly(out).length).toBe(1);
	});

	test("anon-only clusters emit nothing and are counted unresolved", async () => {
		const { out, stats } = await runStage({ isUserId }, [
			{ event: "$merge", $distinct_ids: ["anonP", "anonQ"], time: 100 }
		]);
		expect(assocOnly(out).length).toBe(0);
		expect(stats.clusters.anonOnly).toBe(1);
		expect(stats.unresolvedAnonIds).toBe(2);
	});

	test("identityEvents 'drop' builds the graph but emits no assoc events", async () => {
		const { out, stats } = await runStage({ isUserId, identityEvents: "drop" }, chain);
		expect(assocOnly(out).length).toBe(0);
		expect(ordinaryOnly(out).length).toBe(3); // ordinary events still rewritten + pushed
		expect(stats.clusters.total).toBe(1);
		expect(stats.assocEmitted).toEqual({ live: 0, closure: 0 });
	});

	test("telemetry lands on job.identityReplayStats", async () => {
		const { stats } = await runStage({ isUserId }, chain);
		expect(stats).toBeDefined();
		expect(stats.associationRate).toBe(1); // 3 assoc / 3 verbs
		expect(stats.isUserIdPassRate).toBeGreaterThan(0);
		expect(stats.bare.device).toBe(2);
		expect(stats.bare.user).toBe(1);
	});
});

describe("stage: multi-user clusters (onAmbiguous)", () => {
	const multi = [
		{ event: "$identify", $identified_id: "42", $anon_id: "anonX", time: 100 },
		{ event: "$identify", $identified_id: "43", $anon_id: "anonX", time: 200 }
	];

	test("'drop' (default): no assoc events, cluster counted ambiguous", async () => {
		const { out, stats } = await runStage({ isUserId }, multi);
		expect(assocOnly(out).length).toBe(0);
		expect(stats.ambiguous.clusters).toBe(1);
		expect(stats.clusters.multiUser).toBe(1);
		expect(stats.clusters.resolved).toBe(0);
		expect(stats.unresolvedAnonIds).toBe(1);
	});

	test("'resolve': elects a winner (rank asc → ts desc → id asc) and links anons to it", async () => {
		const { out, stats } = await runStage({ isUserId, onAmbiguous: "resolve" }, multi);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(1); // only the anon links; the losing user stays its own identified user
		expect(assoc[0].properties.$device_id).toBe("anonX");
		expect(assoc[0].properties.$user_id).toBe("43"); // equal rank, later ts wins
		expect(stats.clusters.resolved).toBe(1);
		expect(stats.ambiguous.clusters).toBe(1);
	});

	test("'error': stream is destroyed with a descriptive Error", async () => {
		await expect(runStage({ isUserId, onAmbiguous: "error" }, multi)).rejects.toThrow(/ambiguous cluster with 2 users/);
	});
});

describe("stage: lite mode (graph: false)", () => {
	test("verbs rewrite 1:1 inline when exactly one side is a user", async () => {
		const { out, stats } = await runStage({ isUserId, graph: false }, [
			{ event: "$identify", $identified_id: "42", $anon_id: "anonL", time: 100 },
			{ event: "page view", distinct_id: "anonL", time: 150 }
		]);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(1);
		expect(assoc[0].properties.$user_id).toBe("42");
		expect(assoc[0].properties.$device_id).toBe("anonL");
		expect(assoc[0].properties.time).toBe(100); // the verb's own time
		expect(assoc[0].properties.$id_replay_source).toBe("verb");
		expect(stats.assocEmitted.live).toBe(1);
		expect(stats.assocEmitted.closure).toBe(0);
	});

	test("two-anon verbs are counted deferred-impossible; two-user verbs ambiguous", async () => {
		const { out, stats } = await runStage({ isUserId, graph: false }, [
			{ event: "$merge", $distinct_ids: ["anonP", "anonQ"], time: 100 },
			{ event: "$merge", $distinct_ids: ["42", "43"], time: 100 }
		]);
		expect(assocOnly(out).length).toBe(0);
		expect(stats.deferredImpossible).toBe(1);
		expect(stats.ambiguous.merges).toBe(1);
	});

	test("ordinary events are still rewritten", async () => {
		const { out } = await runStage({ isUserId, graph: false }, [
			{ event: "page view", distinct_id: "anonL", time: 100 },
			{ event: "purchase", distinct_id: "42", time: 200 }
		]);
		const ordinary = ordinaryOnly(out);
		expect(ordinary.length).toBe(2);
		expect(ordinary[0].distinct_id).toBe("$device:anonL");
		expect(ordinary[1].$user_id).toBe("42");
	});

	test("no flush closure in lite mode (chains stay unlinked)", async () => {
		const { out } = await runStage({ isUserId, graph: false }, [
			{ event: "$merge", $distinct_ids: ["anon1", "anon2"], time: 100 },
			{ event: "$identify", $identified_id: "42", $anon_id: "anon2", time: 200 }
		]);
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(1); // only the direct $identify pair; anon1 never links
		expect(assoc[0].properties.$device_id).toBe("anon2");
	});
});

describe("stage: minAssociationRate (fail-closed)", () => {
	test("aborts the stream when assocEmitted / verbsSeen falls below the floor", async () => {
		const records = [
			{ event: "$merge", $distinct_ids: ["a1", "a2"], time: 1 }, // anon-only: no assoc possible
			{ event: "page view", distinct_id: "a1", time: 2 }
		];
		await expect(runStage({ isUserId, minAssociationRate: 0.5 }, records)).rejects.toThrow(/minAssociationRate/);
	});

	test("error message carries telemetry", async () => {
		const records = [{ event: "$merge", $distinct_ids: ["a1", "a2"], time: 1 }];
		try {
			await runStage({ isUserId, minAssociationRate: 0.5 }, records);
			throw new Error("should have rejected");
		}
		catch (err) {
			expect(err.message).toMatch(/verbsSeen/);
			expect(err.message).toMatch(/associationRate/);
		}
	});

	test("passes when the rate clears the floor", async () => {
		const records = [{ event: "$identify", $identified_id: "42", $anon_id: "anonA", time: 1 }];
		const { stats } = await runStage({ isUserId, minAssociationRate: 0.5 }, records);
		expect(stats.associationRate).toBe(1);
	});

	test("rate of 0 (default) never aborts", async () => {
		const records = [{ event: "$merge", $distinct_ids: ["a1", "a2"], time: 1 }];
		const { stats } = await runStage({ isUserId }, records);
		expect(stats.associationRate).toBe(0);
	});
});

describe("stage: denylist", () => {
	test("denylisted ordinary records are dropped and counted; denylisted verb edges are counted", async () => {
		const { out, stats } = await runStage({ isUserId, denylist: ["evil"] }, [
			{ event: "page view", distinct_id: "evil", time: 1 },
			{ event: "$identify", $identified_id: "42", $anon_id: "evil", time: 2 },
			{ event: "page view", distinct_id: "fine-anon", time: 3 }
		]);
		expect(out.length).toBe(1); // just the fine-anon page view
		expect(out[0].distinct_id).toBe("$device:fine-anon");
		expect(stats.denylisted).toBe(2); // 1 dropped record + 1 dropped verb edge
		expect(assocOnly(out).length).toBe(0); // evil never entered the graph
	});

	test("denylist matches prefixed representations too", async () => {
		const { out, stats } = await runStage({ isUserId, denylist: ["evil"] }, [
			{ event: "page view", distinct_id: "$device:evil", time: 1 }
		]);
		expect(out.length).toBe(0);
		expect(stats.denylisted).toBe(1);
	});
});

describe("stage: double-prefix stripping end to end", () => {
	test("double-prefixed ids collapse to one clean device id everywhere", async () => {
		const { out, stats } = await runStage({ isUserId }, [
			{ event: "page view", distinct_id: "$device:$device:dirty", time: 100 },
			{ event: "$identify", $identified_id: "42", $anon_id: "$device:dirty", time: 200 }
		]);
		const ordinary = ordinaryOnly(out);
		expect(ordinary[0].$device_id).toBe("dirty");
		expect(ordinary[0].distinct_id).toBe("$device:dirty");
		const assoc = assocOnly(out);
		expect(assoc.length).toBe(1); // both representations unified on the same node
		expect(assoc[0].properties.$device_id).toBe("dirty");
		expect(assoc[0].properties.$insert_id).toBe(md5("42|dirty"));
		expect(stats.bare.prefixedAlready).toBe(1);
	});
});

describe("stage: graphPath artifact", () => {
	const chain = [
		{ event: "$merge", $distinct_ids: ["anon1", "anon2"], time: 2000 },
		{ event: "$identify", $identified_id: "42", $anon_id: "anon2", time: 4000 },
		{ event: "$merge", $distinct_ids: ["anonP", "anonQ"], time: 100 } // unresolved cluster
	];

	test("writes JSONL pair rows + trailer to a local path (mkdir -p)", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ir-graph-"));
		const graphPath = path.join(dir, "nested", "graph.jsonl");
		await runStage({ isUserId, graphPath }, chain);
		const lines = fs.readFileSync(graphPath, "utf8").trim().split("\n").map((l) => JSON.parse(l));
		const trailer = lines.pop();
		expect(trailer).toEqual({ unresolvedClusters: 1, ambiguousClusters: 0 });
		expect(lines.length).toBe(2); // anon1 + anon2 → 42
		for (const row of lines) {
			expect(row.user).toBe("42");
			expect(["anon1", "anon2"]).toContain(row.device);
			expect(["verb", "closure"]).toContain(row.source);
			expect(row).toHaveProperty("rank");
		}
		fs.rmSync(dir, { recursive: true, force: true });
	});

	test("cloud paths route through destination-writer and never throw (write only if writable)", async () => {
		// no cloud creds in unit tests: the write attempt must degrade gracefully, never a
		// thrown error — the job's data flow is not hostage to the artifact. Cloud uploads
		// are best-effort/async in destination-writer, so only the no-throw contract and
		// telemetry flags are asserted here (data-flow counts are covered by local tests).
		const s3 = await runStage({ isUserId, graphPath: "s3://this-bucket-does-not-exist-mp-import/graph.jsonl" }, chain);
		expect(s3.stats.graphPathWritten ?? s3.stats.graphPathError).toBeDefined();
	});

	test("a directory graphPath gets an auto-generated filename (destination-writer contract)", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ir-graph-"));
		const { out, stats } = await runStage({ isUserId, graphPath: dir }, chain);
		expect(assocOnly(out).length).toBe(2); // anon1 + anon2 → 42; anonP/anonQ unresolved
		expect(stats.graphPathWritten).toBeDefined();
		const files = fs.readdirSync(dir);
		expect(files.length).toBe(1);
		const lines = fs.readFileSync(path.join(dir, files[0]), "utf8").trim().split("\n").map((l) => JSON.parse(l));
		expect(lines.length).toBe(3); // 2 pairs + trailer
		expect(lines[lines.length - 1]).toEqual({ unresolvedClusters: 1, ambiguousClusters: 0 });
		fs.rmSync(dir, { recursive: true, force: true });
	});

	test("unwritable local path warns via telemetry instead of throwing", async () => {
		const { stats } = await runStage({ isUserId, graphPath: "/dev/null/impossible/graph.jsonl" }, chain);
		expect(stats.graphPathError).toBeDefined();
		expect(typeof stats.graphPathError).toBe("string");
	});
});
