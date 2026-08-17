# Research Synthesis: Original → Simplified Identity Replay

**Date:** 2026-08-16 · Distilled from research/01–10 (10-agent sweep) + direct reads.
Read this first; drill into 01–10 for sources.

## The single most important finding

**Stateless 1:1 verb translation is measurably insufficient.** Vipps measured the ladder on
the same funnel:

| Strategy | Funnel recovery |
|---|---|
| Drop identity verbs (status quo) | 0% |
| distinct_id-is-user pairs | 15.9% |
| Direct graph edges only (≈ 1:1 verb rewrite) | 65.9% — rejected before load |
| **Cluster expansion (transitive closure)** | **89.51%** vs prod reference 90.5% |
| Structural floor (no derivable user anywhere) | ~19% of pre-login web rows unreachable |

Original-ID-merge clusters are **stars whose head is frequently an anonymous ID**. A `$identify`
(A1→U) tells you about one edge; the sibling anonymous IDs (A2, A3 aliased to A1 or merged into
the cluster) only link to U **transitively**. 1.37M Vipps pairs were reachable no other way.

⇒ AK's "bonus" in-memory ID graph is not a bonus. **It is the core of the feature.** The
per-event streaming rewrite is the supporting act.

## How the target (simplified) actually merges

- Merge trigger: any event carrying both `$device_id` + `$user_id`. One event suffices. Any
  event name EXCEPT the three verbs. Doc-sanctioned "dummy event" pattern.
- `distinct_id` is computed by ingest: `$user_id` if present, else `"$device:" + $device_id`.
  Whatever distinct_id we send is overridden when reserved props are present.
- Bare `distinct_id` fallback: `$device:`-prefixed → device, unprefixed → **user** (the
  phantom-user bug when anon IDs leak through unprefixed).
- `$identify` / `$create_alias` / `$merge` are **silently ignored** per docs ("will be ignored…
  will not trigger identity merging"); one Vipps report observed a hard rejection message via
  some ingress ("identity events are not allowed when project is using simplified identity
  management"). CONFLICT → empirical probe required (probe #3).
- Exactly ONE `$user_id` per cluster, forever, irreversible. Two user IDs can never merge.
  Second identified ID demoted to `$device:`-prefixed anon. Unlimited device_ids (no 500 cap).
- Retroactive stitching works (patch-after-events confirmed at Vipps); propagation up to 24h,
  full compaction observed ~5 days; re-attributed events stamped `$is_reshuffled` +
  `$preshuffle_distinct_id` / `$distinct_id_before_identity`.
- Device→user mapping is **first-write-wins**; concurrent-worker ingest order is
  nondeterministic ⇒ conflicts must be resolved BEFORE sending, one user per device.

## How the source (original) wrote the graph into the stream

- `$identify`: props `$identified_id` + `$anon_id`; `$anon_id` must be UUIDv4 AND never
  previously merged; else no edge (failure mode at ingest undocumented — probe).
- `$create_alias`: props `distinct_id` + `alias`; alias→one distinct_id; daisy-chains allowed;
  fan-in allowed.
- `$merge`: `$distinct_ids` exactly 2, /import-only, merges whole clusters, no format rules —
  the only way to build multi-user clusters and anon↔anon links.
- 500 IDs/cluster hard cap; beyond → "orphaned duplicates" (merge fails silently-ish; probe).
- Canonical distinct_id chosen by Mixpanel, opaque, may be an anon UUID. Only canonical shows
  in queries/EXPORTS.

## What raw export gives us (the replay input)

- `/api/2.0/export` returns resolved **canonical** distinct_id per event; SDK-tracked events
  carry `$device_id`/`$user_id` already; `$distinct_id_before_identity` community-confirmed
  (probe to verify on our projects); verb events' exported property bags UNVERIFIED (probe #1:
  do $identify/$create_alias/$merge rows appear in export? with which props?).
- Exported Mixpanel-added props to strip on re-import: `$import`, `$mp_api_endpoint`,
  `$mp_api_timestamp_ms`, `$mp_event_size`, `mp_processing_time_ms`.
- /import: requires event, time, distinct_id, `$insert_id` (≤36 bytes, alnum+`-`);
  denylist of garbage ids (`anon`, `null`, `-1`, `0`, `true`…); time ≥ 1971-01-01;
  >1h-future silently rewritten to now; 2000 events / 10MB per batch; strict=1 returns
  `failed_records[]` per-record.
- Mixpanel ACCEPTS `$device:$device:X` garbage silently (70.7% of first Vipps seed corrupted).
  The prefix strip/add decision must live in exactly ONE tested function.

## The proven recipe (Vipps v3, generalized)

1. **Build pair table** device→user from four evidence buckets:
   - A: dual-ID rows on real events (hard truth, highest rank)
   - B: bare distinct_id that IS a known user id
   - D: alias-graph direct edges, read in BOTH orientations (~half are stored reversed)
   - E: **cluster expansion** — link every anonymous member of a cluster to the user id found
     anywhere in that cluster (the 65.9→89.5 step)
2. **Resolve one user per device**: rank hard evidence > graph-inferred, then recency, then
   deterministic tiebreak (e.g. min uid). Known defect to avoid: Vipps bucket E joined only the
   alias side; match cluster **head OR alias** (their residual 1.2%).
3. **Emit synthetic dual-ID events**: `$user_id` + stripped `$device_id`, deterministic
   `$insert_id = hash(uid|dev)` (idempotent resends), timestamp OUT of analysis windows
   (Vipps: constant one day before data range).
4. **Rewrite ordinary events**: keep dual-ID rows as-is; bare distinct_id → isUserId(id) ?
   user : `$device:`-prefix; guard: a device_id that is itself a known user id = corrupt, exclude.
5. **Never send a verb event** to the simplified project.

## Required customer inputs (the non-inferable things)

1. `isUserId(candidate, record) => boolean` — per-project shape rule. MUST also support
   serializable forms (regex string / prop paths / denylist) for CLI+UI. Per-project variance is
   real: vipps-prod `^[0-9]{8}$`, merchantportal 36-char UUIDs.
2. Test-account denylist (user_id `1234` merged 404 devices at Vipps; pre-scan for pathological
   device counts).

## Validation techniques worth productizing (from Vipps reports)

- **Dry-run projection before load** (irreversibility!): build the map, report people counts,
  total:unique ratios, conflict census, isUserId pass rate — send nothing. mixpanel-import
  already has `destination`/`destinationOnly` to write locally instead of Mixpanel.
- Cross-identity funnel (anon step → identified step) as THE acceptance metric; user-keyed
  funnels pass even when identity is broken.
- total:unique == 1:1 on high-volume event ⇒ identity loss fingerprint.
- `$is_reshuffled` rate read WITHIN affected event family, after multi-day compaction.
- Reconcile against TARGET project (not source-to-source); whitelist expected deltas.
- Coverage telemetry + fail-closed threshold (`minAssociationRate`-style).

## Codebase facts that shape the build

- Chassis exists: `export-import-event` streams /export → flatten props to root →
  parsers.js:660 rewrites recordType='event', token=secondToken → normal ingest pipeline.
- Stage slot: between `createVendorTransform` (pipelines.js:684) and `createUserTransform`
  (686) — upstream of flatten (fan-out OK), dedupe, and helpers (ezTransforms' badUserIds
  scrub would eat placeholder ids AFTER us, good).
- User `transformFunc` may return arrays (flatten expands) — prototype path.
- Cross-record state precedent: `job.hashTable`, `dimensionMaps` → `job.heavyObjects` (Maps),
  posthog vendor consumes `heavyObjects.people`. Two-pass machinery half-exists.
- `validateToken` (validators.js:20) probes /import and reports idmgmt_v2 vs idmgmt_v3 —
  can auto-verify destination project generation (but inserts a real test event).
- Watch out: fastMode bypasses all transforms; `secondProject` referenced but not settable
  (destination auth is token-only today); vendor transforms are 1:1 by contract (array returns
  break when user transformFunc also set — flatten runs after user transform only).
- NO process-global handlers in components/ (tests/handlers.test.js enforces).

## Probe list (empirical, against 4054680/4054681)

1. Export shape of the three verbs from an original project (props preserved? present at all?)
2. `$distinct_id_before_identity` / `$user_id` / `$device_id` presence in raw export (orig + simpl)
3. Verb events sent to simplified project: ignored vs rejected; strict=1 feedback; stored/billed?
4. Same $device_id later with different $user_id in simplified: first-wins? split? (undocumented)
5. Original: non-UUIDv4 $anon_id via $identify — error or silent no-op? strict mode text?
6. Original: cluster growth past 500 — behavior + how it exports afterwards
7. Custom-named event with both reserved props triggers merge in simplified (rule 1 live check)
8. Timestamp of merge-trigger event: does backdated (pre-range) timestamp affect retroactive stitch?
9. Anon→anon→anon alias chain in original: exports how? canonical = ?
10. Does /import into simplified store the verb event as a regular (billable, queryable) event?

## Query tooling (post-import verification)

Power Tools API: `runQuery` (insights bookmark, math:"unique" — only route to $all_events
uniques), `getSegmentation` (per-event uniques, `where: properties.dataVersion == N` busts the
1h cache per round), `runJQL` (cluster-shape stats via groupByUser), `getActivityStream`
(cluster-membership probe by any member id — returns merged canonical stream), `getProfiles`.
Rate: 5 concurrent, 60/hr — prefer one JQL over N segmentation calls. 400s cached 5 min.
`/crud/createProject` (OAuth) can mint fresh projects if test clusters get polluted.
Caution: getSavedInsight is broken (401) — use direct mixpanel.com API w/ SA basic auth.

## Dataset-generator requirements (phase 3)

Every event: `dataVersion` (int), `scenario` (string), deterministic `$insert_id`.
Canonical users numeric; anon ids uuidv4 (except the deliberately-bad-shape scenario).
Scenario matrix: simple identify; alias chain anon→anon→anon→user; alias fan-in; $merge
user↔user (multi-user cluster); $merge anon↔anon; >500-member cluster; non-UUIDv4 $anon_id;
server-side-identify shape (fresh device per event, identity only via verbs); SDK shape (dual-ID
rows); test-account pathology (1 user, hundreds of devices); device_id colliding with a user id;
normal events interleaved BEFORE and AFTER their identity verbs (retroactivity check); events
whose distinct_id is a mid-chain anon id.

## Open design decisions (for AK)

1. Graph mode default: evidence says transitive closure is the whole ballgame — default ON
   (memory-bounded) vs default OFF (AK's brief said default false)?
2. Scale envelope for in-memory graph: pairs held as Map (Vipps = 41M pairs ≈ multiple GB).
   Bounded-with-hard-fail? Disk spill? Two-pass?
3. API surface: top-level option group (sibling of v2_compat) vs vendor:'mixpanel' vendorOpts
   vs new recordType. CLI-serializable isUserId form.
4. Multi-user-cluster policy default (deterministic winner + report? drop + report?).
5. Profiles in scope for this PR?
6. Association event: name, timestamp policy (pre-range constant vs verb time), metadata props.
