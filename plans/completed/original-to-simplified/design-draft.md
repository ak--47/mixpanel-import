# Design Draft: `identityReplay`

**Status:** DRAFT v2 — amended with round-1 probe results (research/11-probe-findings-v1.md).
§Post-probe amendments OVERRIDE anything above them. §Pinned interfaces is the build contract.

## Post-probe amendments (2026-08-16 night)

1. **No 'emit' mode.** Simplified /import HARD-REJECTS verbs (400, whole batch). `identityEvents:
   'rewrite' | 'drop'` only — and both mean the raw verb never reaches the sender.
2. **Association events emit at flush only** (not live per-verb). Graph is the single source of
   truth: one assoc event per resolved (device→user) pair, deduped across repeated verbs,
   deterministic `$insert_id = md5(user + '|' + device)`. Retroactive stitching is confirmed in
   both directions (p04/p09/p10), so end-of-stream emission is safe.
3. **Never trust exported distinct_id as the user.** Canonical can be an anon uuid (s07 live).
   Evidence order for the as-ingested id: `$distinct_id_before_identity` ?? `distinct_id`.
   Verb rows use their own props ($identified_id/$anon_id, alias+distinct_id, $distinct_ids) —
   for $create_alias/$merge rows, each side resolved via before_identity-aware helper.
4. **strip('$device:')** applied to every device-side value (ingest normalizes one prefix — p08 —
   but we do not rely on it). One exported function, used everywhere.
5. **Default association event name: `'identity association'`** (proven live; matches
   posthog/heap vendor precedent). Not $-prefixed.
6. **`associationTimestamp: 'original'` (default) | 'floor'.** original = first-seen ts of the
   device node; floor = (min event time seen) − 24h (Vipps keep-out-of-analysis-windows pattern;
   backdating confirmed working, p09).
7. **onAmbiguous applies to multi-user clusters** (union-find makes device-conflicts a cluster
   property): `'drop'` (default: anons in a multi-user cluster get NO assoc events; counted) |
   `'resolve'` (elect winner by evidence rank → latest ts → lexicographic min; link anons to
   winner; losers stay their own identified users) | `'error'` (abort job). Extra users always
   counted + reported regardless.
8. **No dryRun option.** `destinationOnly` (existing) already provides send-nothing runs;
   document the recipe. `graphPath` artifact (default '', local or gs://|s3://) writes the
   resolved pair table + unresolved clusters at flush, only if writable (AK requirement).
9. **v2_compat**: if both set → identityReplay wins, v2_compat disabled with loud warn.
   **fastMode**: throw. **recordType**: only 'event' (and export-import-event, which is
   rewritten to 'event' before corePipeline).
10. **Import error handling**: simplified verb-rejection returns failed_records as STRINGS.
    Not our stage's concern (we never forward verbs) but noted for importers.js robustness.

## Pinned interfaces (build contract)

### components/identity-graph.js
```js
class IdentityGraph {
  constructor(opts = {})            // { maxNodes = 5_000_000 }
  addNode(id, { isUser, ts, rank }) // idempotent; upgrades rank/ts metadata; false if at cap+new
  addEdge(a, b)                     // union by id strings (nodes must be added first); false if either missing
  size                              // node count
  overflowEdges                     // edges skipped because a node was rejected at cap
  clusters()                        // -> Array<{ members: string[], users: Array<{id, rank, ts}> }>
  resolve(id)                       // -> canonical user id | null (after election), for artifact/tests
  electUser(users, mode)            // static-ish helper: rank asc → ts desc → id asc; exported for tests
}
module.exports = { IdentityGraph, stripDevicePrefix }   // stripDevicePrefix(id) removes ALL leading '$device:' repeats
```
Union-find (path compression + union by size), Map<string, …>. Ranks: 0 = hard dual-ID row,
1 = verb edge, 2 = inferred/fallback-prop. Election prefers LOWEST rank.

### components/identity-replay.js
```js
createIdentityReplay(job)  // -> stream.Transform (objectMode), the pipeline stage
// internals (exported for unit tests):
//   normalizeOptions(raw)         -> canonical opts (compiles regex/string isUserId, applies defaults)
//   classifyRecord(record, opts)  -> { kind: 'identify'|'alias'|'merge'|'event', edges: [[a,b,rank]], rewrite }
//   rewriteEvent(record, opts)    -> mutated record (bare-id classification, $device: handling)
//   buildAssociationEvent(user, device, opts, meta) -> event object (deterministic $insert_id)
```
Stage behavior: verbs → absorb into graph, emit nothing (identityEvents 'rewrite') or count
(‘drop’ still builds the graph but emits no assoc events at all — pure translation off);
ordinary events → rewrite + push 1:1 + harvest edges; _flush → emit assoc events per resolved
pair not representable from the events themselves, telemetry onto job, graphPath artifact.

### Options (index.d.ts + job.js)
```js
identityReplay: {
  isUserId,                         // REQUIRED: fn | RegExp | string (compiled)
  graph = true,                     // false = stateless verb rewrite only (lite mode)
  maxGraphSize = 5_000_000,
  onGraphOverflow = 'warn',         // 'warn' | 'abort'
  identityEvents = 'rewrite',       // 'rewrite' | 'drop'
  associationEventName = 'identity association',
  associationTimestamp = 'original',// 'original' | 'floor'
  associationProps = {},            // static props merged onto assoc events (e.g. dataVersion)
  bareDistinctId = 'validate',      // 'validate' | 'passthru'
  userIdFallbackProps = [],         // extra props probed for a user id on ordinary events
  denylist = [],                    // ids excluded from graph + classification (counted)
  onAmbiguous = 'drop',             // 'drop' | 'resolve' | 'error'
  minAssociationRate = 0,           // fail-closed floor: assocEmitted / verbsSeen
  graphPath = '',                   // '' off | local path | gs:// | s3://
}
```
Telemetry appended to results as `identityReplay: {…}` (see design body).

### Files touched (plumbing)
- `components/job.js`: option intake + validation (throw: missing isUserId, fastMode; warn+win: v2_compat)
- `components/pipelines.js`: insert stage between createVendorTransform and createUserTransform
- `index.d.ts`: Options.identityReplay + result typing
- `components/cli.js`: --identity-replay (JSON blob) + --ir-user-id-regex convenience
- `index.js`: no new exports needed (module surface unchanged); results wiring only if required

## Summary

`identityReplay` is a top-level option group on `mpImport()` that translates an
original-ID-merge event stream (raw export shape) into a simplified-ID-merge event stream:

- builds an **identity graph** from every evidence source in the stream (default ON),
- rewrites identity verbs (`$identify`, `$create_alias`, `$merge`) into doc-sanctioned
  dual-ID association events (`$user_id` + `$device_id` on a non-reserved event name),
- classifies bare `distinct_id`s via a customer-supplied `isUserId` predicate
  (function in module mode; regex string on CLI),
- at end of stream, **flushes transitive-closure association events** for every
  anonymous cluster member not directly paired with the canonical user
  (anon1→anon2→anon3→U ⇒ also emit anon1→U, anon2→U),
- reports coverage telemetry and fails closed below a configurable association-rate floor.

## Option shape (v1 draft)

```js
await mpImport(creds, data, {
  recordType: 'event',               // or 'export-import-event' round-trip
  identityReplay: {
    // REQUIRED. Module: (candidate, record) => boolean, or RegExp.
    // CLI: string compiled as RegExp.
    isUserId: (id) => /^\d+$/.test(id),

    // The graph (default ON). false = stateless 1:1 verb rewrite ("lite").
    graph: true,
    maxGraphSize: 5_000_000,         // pairs; at cap: keep streaming, count dropped edges
    onGraphOverflow: 'warn',         // 'warn' | 'abort'

    // Identity verb handling
    identityEvents: 'rewrite',       // 'rewrite' | 'drop' (both never forward the raw verb)
    associationEventName: '$identity_association',   // ⚠️PROBE: pick non-reserved default
    associationTimestamp: 'original',// 'original' (verb time) | 'floor' (min(time)-1d, Vipps style)

    // Bare distinct_id policy for ordinary events
    bareDistinctId: 'validate',      // 'validate' (isUserId → user else $device: prefix) | 'passthru'
    userIdFallbackProps: [],         // e.g. ['$distinct_id_before_identity'] extra evidence
    denylist: [],                    // test-account ids: never user, never device (excluded+counted)

    // Conflict policy: one user per device, evidence-ranked (hard dual-ID row > verb edge >
    // graph-inferred), then most-recent, then lexicographic-min. Ambiguity handling:
    onAmbiguous: 'drop',             // 'drop' | 'first' | 'error'  (multi-user merges, two-user pairs)

    // Fail-closed coverage floor: associations emitted / identity-bearing events seen
    minAssociationRate: 0,           // 0 disables; e.g. 0.01 aborts obviously-broken runs

    // Graph export artifact (AK: default false, local or cloud path, write only if writable)
    graphPath: '',                   // '' = off; './graph.jsonl' | 'gs://bucket/graph.jsonl'

    // Dry run: build graph + telemetry, send nothing (composes with existing destinationOnly)
    dryRun: false,
  }
})
```

Validation rules:
- `identityReplay` without `isUserId` (fn or regex) → throw at job construction.
- Mutually exclusive with `v2_compat` → identityReplay wins, warn loudly.
- Incompatible with `fastMode` → throw (fastMode bypasses all transforms).

## Rewrite semantics

| Input record | Output |
|---|---|
| `$identify {$identified_id: U, $anon_id: A}` | assoc event `{$user_id: U, $device_id: strip(A)}`; edge U↔A into graph |
| `$create_alias {distinct_id: X, alias: Y}` | edge X↔Y into graph; assoc event only if isUserId resolves one side; else defer to closure flush |
| `$merge {$distinct_ids: [a, b]}` | edge a↔b; assoc event if exactly one side is a user; both users → onAmbiguous; neither → defer to closure |
| ordinary event with `$user_id`+`$device_id` | pass through (strip `$device:` from $device_id if present); edge into graph (rank: hard) |
| ordinary event, bare distinct_id, isUserId ✓ | `$user_id = distinct_id` |
| ordinary event, bare distinct_id, isUserId ✗ | `$device_id = strip(id)`, `distinct_id = '$device:'+strip(id)` |
| ordinary event, distinct_id already `$device:`-prefixed | strip once, treat as device (never double-prefix) |
| any record with denylisted id | excluded + counted |

`strip()` = remove `$device:` prefix(es) — ONE function, both directions, unit-tested to
death. (70.7% of the first Vipps seed was corrupted by double-prefixing.)

Association events: deterministic `$insert_id = murmurhash(user + '|' + device)`-derived
(≤36 chars alnum/dash) so re-runs and closure-flush duplicates self-dedupe at query time.
Metadata props on every emitted/rewritten record (AK OK'd):
`$id_replay_source` ('identify'|'alias'|'merge'|'closure'|'dual-row'|'bare-user'|'bare-device'),
`$id_replay_rank` (evidence rank that won, when conflict resolved).

## Graph module (new file: `components/identity-graph.js`)

Union-find (disjoint set) over id strings + per-node metadata:
`{isUser (per isUserId at insert), evidenceRank, lastSeenTs}`. Union-find gives transitive
closure for free at O(α) per op; memory = one Map entry per distinct id (not per pair).
End-of-stream flush walks clusters:
- canonical = the user id in the cluster (evidence-ranked, recency, lexicographic-min tiebreak);
- emit assoc event per anonymous member not already sent live;
- clusters with 2+ users → onAmbiguous per extra user (count, report, drop/first/error);
- clusters with 0 users → members stay `$device:`-anonymous (already correct from per-event
  rewrite); counted as unresolved.

Flush = a Transform stage that emits on `_flush()` — fits stream pipeline; sits between
vendor transform and user transform (pipelines.js:684/686), upstream of flatten (fan-out OK),
dedupe, and helper transforms.

Cap semantics (`maxGraphSize` = node count): at cap, new ids are not added; every skipped
edge increments `graphOverflowEdges` in telemetry; `onGraphOverflow: 'abort'` kills the job.

## Telemetry (added to ImportResults)

```
identityReplay: {
  verbsSeen: {identify, alias, merge}, assocEmitted: {live, closure},
  bare: {user, device, prefixedAlready}, denylisted, ambiguous: {merges, clusters},
  clusters: {total, resolved, anonOnly, multiUser}, unresolvedAnonIds,
  graphOverflowEdges, isUserIdPassRate, associationRate
}
```

## CLI

`--identity-replay` (bool) + `--ir-user-id-regex '<re>'` + a few scalars; function-valued
options module-only. (AK: module-first is fine.)

## Testing

- Unit: graph module (chains, fan-in, merge-of-clusters, multi-user, cap, denylist, strip()).
- Unit: rewrite table above, every row.
- Pipeline: JSONL fixture (generated dataset, small version) through full pipeline with
  `destinationOnly` — assert emitted records byte-for-byte.
- Fixture source: the versioned torture dataset from probes (checked into testData/).
- Live (int.optional): replay ORIGINAL export → SIMPLIFIED project, assert via query API.

## ⚠️PROBE-dependent decisions

- Verb property shapes as they come back from /export (field names post-flatten).
- Whether simplified /import rejects-vs-ignores verbs (decides whether 'drop' needs to exist).
- Association event name + timestamp default (does backdated stitch retroactively?).
- Whether $distinct_id_before_identity is exportable evidence worth a default fallback prop.
- Device-conflict behavior in simplified (first-write-wins ordering implications).
