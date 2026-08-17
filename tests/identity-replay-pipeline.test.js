// @ts-nocheck
/*
end-to-end pipeline test for identityReplay: the FULL corePipeline (existence filter →
vendor → identityReplay → user transform → flatten → dedupe → helpers → batcher →
destination writer) with destinationOnly — no network, no credentials.
Mirrors the live acceptance run in plans/original-to-simplified/probes/run-04-replay.js.
*/
const fs = require("fs");
const os = require("os");
const path = require("path");
const mpImport = require("../index.js");

/** a miniature original-id-merge export: raw /export shape (nested), with verbs */
function microDataset() {
	const U = "990001", A1 = "aaaa1111-0000-4000-8000-000000000001", A2 = "aaaa2222-0000-4000-8000-000000000002";
	let seq = 0;
	const ev = (event, distinct_id, time, extra = {}) => ({
		event,
		properties: { distinct_id, time, $insert_id: `pipe-${++seq}`, $import: true, $mp_api_endpoint: "api.mixpanel.com", ...extra },
	});
	return [
		ev("page view", A1, 1700000000),
		ev("$create_alias", A1, 1700000100, { alias: A2 }),
		ev("page view", A2, 1700000200),
		ev("$identify", U, 1700000300, { $identified_id: U, $anon_id: A1 }),
		ev("purchase", U, 1700000400),
		// a duplicate verb row, as raw exports really contain (query-time dedupe only)
		ev("$identify", U, 1700000300, { $identified_id: U, $anon_id: A1 }),
	];
}

async function runPipeline(records, identityReplay) {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ir-pipe-"));
	const destination = path.join(dir, "out.jsonl");
	const results = await mpImport(
		{ token: "test-token-not-used" },
		records,
		{
			recordType: "event",
			destination,
			destinationOnly: true,
			fixData: false,
			verbose: false,
			showProgress: false,
			abridged: false,
			identityReplay,
		}
	);
	const lines = fs.readFileSync(destination, "utf8").trim().split("\n").map((l) => JSON.parse(l));
	fs.rmSync(dir, { recursive: true, force: true });
	return { results, lines };
}

describe("identityReplay through the full pipeline (destinationOnly)", () => {
	test("verbs are swallowed, ordinary events rewritten, closure emitted, telemetry lands on results", async () => {
		const { results, lines } = await runPipeline(microDataset(), { isUserId: /^\d+$/ });

		// no verb ever reaches the sink
		expect(lines.some((l) => ["$identify", "$create_alias", "$merge"].includes(l.event))).toBe(false);

		// ordinary events rewritten: anons prefixed, user bare
		const props = (l) => l.properties || l;
		const pageViews = lines.filter((l) => l.event === "page view");
		expect(pageViews.length).toBe(2);
		for (const pv of pageViews) {
			expect(props(pv).distinct_id.startsWith("$device:")).toBe(true);
			expect(props(pv).$device_id).toBeDefined();
			// export junk scrubbed by default
			expect(props(pv).$import).toBeUndefined();
			expect(props(pv).$mp_api_endpoint).toBeUndefined();
		}
		const purchase = lines.find((l) => l.event === "purchase");
		expect(props(purchase).$user_id).toBe("990001");

		// association events: A1 direct (verb), A2 transitive (closure) — duplicate verb deduped
		const assoc = lines.filter((l) => l.event === "identity association");
		expect(assoc.length).toBe(2);
		const byDevice = Object.fromEntries(assoc.map((a) => [props(a).$device_id, a]));
		expect(props(byDevice["aaaa1111-0000-4000-8000-000000000001"]).$id_replay_source).toBe("verb");
		expect(props(byDevice["aaaa2222-0000-4000-8000-000000000002"]).$id_replay_source).toBe("closure");
		for (const a of assoc) expect(props(a).$user_id).toBe("990001");

		// telemetry present on ImportResults
		expect(results.identityReplay).toBeDefined();
		expect(results.identityReplay.verbsSeen).toEqual({ identify: 2, alias: 1, merge: 0 });
		expect(results.identityReplay.assocEmitted).toEqual({ live: 1, closure: 1 });
		expect(results.identityReplay.clusters.resolved).toBe(1);
	});

	test("lite mode (graph:false) emits only the direct pair", async () => {
		const { lines } = await runPipeline(microDataset(), { isUserId: /^\d+$/, graph: false });
		const assoc = lines.filter((l) => l.event === "identity association");
		const props = (l) => l.properties || l;
		// A1 rewritten live per $identify (twice — duplicate verb row, same deterministic
		// $insert_id so query-time dedupe collapses them); A2 (transitive) is MISSED — the
		// documented lite-mode limitation
		expect(assoc.length).toBe(2);
		for (const a of assoc) expect(props(a).$device_id).toBe("aaaa1111-0000-4000-8000-000000000001");
		expect(new Set(assoc.map((a) => props(a).$insert_id)).size).toBe(1);
	});

	test("without identityReplay the feature is fully inert: no stage, no telemetry, verbs pass through", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ir-inert-"));
		const destination = path.join(dir, "out.jsonl");
		const results = await mpImport(
			{ token: "test-token-not-used" },
			microDataset(),
			{ recordType: "event", destination, destinationOnly: true, fixData: false, verbose: false, showProgress: false, abridged: false }
		);
		const lines = fs.readFileSync(destination, "utf8").trim().split("\n").map((l) => JSON.parse(l));
		fs.rmSync(dir, { recursive: true, force: true });
		// verbs NOT swallowed, ids NOT rewritten, no synthetic events, no telemetry
		expect(lines.filter((l) => l.event === "$identify").length).toBe(2);
		expect(lines.filter((l) => l.event === "identity association").length).toBe(0);
		const pv = lines.find((l) => l.event === "page view");
		expect((pv.properties || pv).distinct_id.startsWith("$device:")).toBe(false);
		expect(results.identityReplay).toBeUndefined();
	});

	test("identityReplay forces v2_compat off (mutually exclusive — destination is simplified)", async () => {
		const dir = fs.mkdtempSync(path.join(os.tmpdir(), "ir-v2c-"));
		const destination = path.join(dir, "out.jsonl");
		const results = await mpImport(
			{ token: "test-token-not-used" },
			microDataset(),
			{ recordType: "event", destination, destinationOnly: true, fixData: false, verbose: false, showProgress: false, abridged: false, v2_compat: true, identityReplay: { isUserId: /^\d+$/ } }
		);
		fs.rmSync(dir, { recursive: true, force: true });
		// the job ran the replay (telemetry present) — v2_compat did not interfere
		expect(results.identityReplay).toBeDefined();
		expect(results.identityReplay.verbsSeen.identify).toBe(2);
	});

	test("job construction guards: fastMode throws, wrong recordType throws", () => {
		const guard = (opts) => mpImport({ token: "t" }, [{ event: "x", properties: { distinct_id: "1", time: 1 } }], opts);
		expect(guard({ recordType: "event", fastMode: true, identityReplay: { isUserId: /^\d+$/ }, destinationOnly: true, destination: os.tmpdir() }))
			.rejects.toThrow(/fastMode/);
		expect(guard({ recordType: "user", identityReplay: { isUserId: /^\d+$/ }, destinationOnly: true, destination: os.tmpdir() }))
			.rejects.toThrow(/recordType/);
	});
});
