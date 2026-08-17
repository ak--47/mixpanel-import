# Research notes: smarterchild spec — Original → Simplified ID Merge migration

Source: `/Users/ak/code/smarterchild/research/original-simplified-research.md` (484 lines, dated 2026-08-16, author AK w/ Claude).
This file captures the spec in enough detail that a future agent should not need to re-read the source.

## Summary

There is **no existing tool anywhere at Mixpanel** that migrates a project from Original ID Merge
(v2) to Simplified ID Merge (v3). The official docs say automatic conversion is impossible and tell
customers to hand-roll a migration from export/import primitives. Every migration to date (Vipps,
Picklebet, Cosmos, Klover, Extreme Music, Polarsteps, Cardmarket, …) has been rebuilt per-customer
from scratch in BigQuery SQL, and two of them (Vipps v1 and the seed-table attempt) shipped broken
identity data because the `$device:`-prefix decision was made ad hoc in SQL. The spec proposes an
**identity-replay transform in `mixpanel-import`**: read the raw event export (JSON/JSONL), rewrite
the Original ID Merge identity verbs (`$identify`, `$create_alias`, `$merge`) into Simplified
dual-ID association events (`$user_id` + `$device_id` on one event), and classify every bare
`distinct_id` as user-or-device via **one customer-supplied predicate `isUserId(candidate)`** — the
single thing the library cannot infer. A concrete `identity_replay` option shape is sketched
(§6.4 of source), with full per-verb rewrite semantics (§6.5), placement in the existing pipeline
(§6.6), and hazards (§6.7). Every rewrite is 1:1 so the default mode needs no fan-out; only the
optional `'emit'` mode (keep original + emit synthetic association) needs `createFlattenStream`,
which already exists downstream of the user-transform slot.

## Key Facts

- **No prior art.** Dana Ravnur (Senior GTM Engineer, GitHub `danakock`) confirmed directly in DM
  (2024-01-19) that no Original→Simplified script exists; nothing in `mixpanel/analytics`,
  `mixpanel-utils`, `mixpanel-power-tools`, or the docs does it either. The product backend
  (`enable_id_merge` in `analytics/webapp/organization/views.py:2712`) only converts
  Legacy→**Original**, never →Simplified.
- **The two rules Simplified enforces** (from official docs, the entire target contract):
  1. If `$user_id` and/or `$device_id` are present, Mixpanel derives `distinct_id` from them; one
     event carrying both triggers the merge.
  2. If only `distinct_id` is present: `$device:`-prefixed → treated as `$device_id`; unprefixed →
     treated as **`$user_id`**.
  Rule 2 is the Vipps failure mode: unprefixed anonymous IDs each become a phantom user.
- **Original ID Merge already wrote the identity graph into the event stream** as first-class
  events: `$identify` carries `$identified_id` + `$anon_id`; `$create_alias` carries `alias` +
  `distinct_id`; `$merge` carries `$distinct_ids` (openapi pins **exactly 2**: `minItems: 2,
  maxItems: 2`). These are exactly the (user, device) pairs Simplified needs.
- **The one required customer input:** `isUserId(candidate, record) => boolean`. Everything else is
  mechanical. Vipps' hardcoded version was `r'^[A-Za-z0-9]{8}$'` in a BQ view.
- **Replay from raw JSON deletes reverse-schematization** — the problem Dana called "the
  challenge" in 2024 exists only because data round-trips through a schematized BQ warehouse
  export that mangles `$`-prefixed names. Dana himself noted (2023-11-21): *"don't need to worry
  about reversing the schema if using raw json."*
- **Proposed option name:** `identity_replay` (object), sibling of `v2_compat`
  (`components/job.js:361`, `index.d.ts:1022`). Suggested host: `vendor: 'mixpanel'`
  (`vendor/mixpanel.js` already maps warehouse-export rows to Import-API shape).
- **Coverage telemetry is a hard requirement, not a nicety.** Vipps shipped a transform covering
  ~34% of what it should have, twice, and nobody noticed. Job summary must report: events seen,
  associations emitted, bare-distinct_id classifications (user vs device), `isUserId` pass rate,
  ambiguous-`$merge` count. `minAssociationRate` should fail closed.
- **Live demand:** Vipps MobilePay (155.8B rows, two failed attempts, cutover ~2026-08-24), Klover
  (5 years of data, Hightouch), Extreme Music, plus Polarsteps, Picklebet, Cardmarket, Wolt,
  Consensys, Fresco, Mastercard, Jack Henry, Veepee, SimplePractice. "At least three funded
  engagements waiting on it right now."

## Details

### 1. Business problem

- Customers on Original ID Merge (v2) want/need to move to Simplified (v3) because modern
  integrations (Hightouch, Segment-era tooling) are built around the simplified API, and v3 is the
  default for new projects. Klover PS request (2026-08-13): *"Hightouch doesn't integrate with the
  old merge API very well… most Mixpanel integrations are built around the simplified API now."*
- Mixpanel cannot convert a project in place. Official doc
  (`docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system.mdx`): *"It is
  currently not possible to automatically convert an existing project… you would need to set up a
  new empty Mixpanel project"* — then tells you to build your own migration script from
  mixpanel-utils export/import primitives.
- Consequence: every migration is a bespoke ETL — export old project → transform → import into a
  fresh Simplified project. The export/load/entity-cloning halves are solved
  (mixpanel-power-tools, warehouse connectors, snowcat); **the identity transform is the piece
  rebuilt from zero every time**, and the piece that keeps failing.
- Dana explicitly declined to own a general solution (2024-02-05 DM): customers *"will want the
  official thing with slas, docs, etc."* Nothing official ever shipped.

### 2. Why v2→v3 specifically is hard (semantics gap)

- Original (v2): identity is directive-driven. `$identify`, `$create_alias`, `$merge` events
  mutate a server-side identity cluster; events carry a single `distinct_id` that gets resolved
  through the cluster. Clusters can hold up to 500 identities and — via chained `$merge` — can
  contain **multiple genuine user IDs**.
- Simplified (v3): identity is data-driven. One event carrying both `$user_id` and `$device_id`
  performs the merge. One cluster = exactly **one** `$user_id`. `$merge` has no equivalent — Dana
  (2024-01-31): *"you can call `$merge` after the fact in v2 to tie two separate user ids together,
  whereas in v3 you need to send in the same user id from both platforms for a single user."*
- Known Simplified constraints Dana stated (2024-01-30, #mixpanel-consensys):
  - backend conversions (Legacy→Original) capped at <1.5B events/mo; beyond that = ETL to new project
  - anon IDs MUST be UUIDv4 (in the *product's* auto-handling; the replay transform sidesteps this
    by explicit `$device_id`)
  - max 500 identities per cluster
  - up to 24h merge delay; backfill takes a day or two
  - **migration is permanent, no undo**
- Historical linking rule (Dana, #mixpanel-picklebet 2026-02-04, stated once, written down nowhere
  else): *"`device_id` is what's used for historical linking in both Simplified ID Management and
  in the backfill."* This assumption broke on Vipps, where web checkout mints a fresh `device_id`
  per landing-page hit.

### 3. Prior attempts and their outcomes

#### 3.1 Dana's actual code (none of it is a migration)

- **Reverse-schematization pair** (Python, Jack Henry, 2024-01-22): Script A parses a
  `people_schema` JSON to build a `{cleaned_bq_name: original_$name}` map (split on `,"`, strip
  `]"`; spaces/dots→`_`, collapse repeats, trim leading `_`, cap 128 chars). Script B reads
  `INFORMATION_SCHEMA.COLUMNS`, renames columns via the map, wraps them in
  `PARSE_JSON(TO_JSON_STRING(STRUCT(...))) AS event_data`, emits
  `SELECT mp_distinct_id AS distinct_id, <json> FROM …` into a warehouse-connector-shaped table.
  Special case: columns ending `mp_ae_total_app_session_length` are CAST to STRING. This whole
  class of work is what raw-JSON replay eliminates.
- **Veepee union** (2023-11-20): unions 11 `mp_master_event` datasets into one table with a
  `site_id_mapping` provenance tag. No identity logic.
- **The resolution idiom** — Dana's only identity-adjacent code, repeated at lemonme (BQ view,
  2026-03-13), HUL (daily `DELETE`+`MERGE` since ~Feb 2026), Polarsteps (signup query):

  ```sql
  COALESCE(map.resolved_distinct_id, main.distinct_id) AS distinct_id
  -- LEFT JOIN mp_identity_mappings_data_view map ON main.distinct_id = map.distinct_id
  ```

  This collapses an Original cluster to one canonical string. It does **not** produce
  `$user_id`/`$device_id` and does not `$device:`-prefix anonymous IDs. Fed to a Simplified
  project, every unresolvable anon ID becomes a phantom user. **This is the Vipps failure mode in
  four lines of SQL.** (HUL variant also deletes aliased rows where
  `resolved_distinct_id IS NOT NULL AND resolved_distinct_id != distinct_id`.)
- Dana's repos (parquet-dataflow-bigquery, simple-pipelines, GCS-Parquet-Dataflow,
  mixpanel-scripts, internal_testing_environment): ETL plumbing only, zero identity translation.

#### 3.2 Vipps (the instructive failure)

- All transform DDL executed by `ruqia.alzoubi@mixpanel.com` (BQ job history); the *pattern* is
  Dana's, seeded in working sessions Feb 24 – Mar 10 2026. Dana's own footprint in `mixpanel-sa`
  EU: 10 jobs, zero transforms.
- **v1 views** (4 identical, Feb–Mar 2026): the COALESCE resolution idiom + a `user_type: "user"`
  literal. Wrong for Simplified; abandoned for event-seeding but never deleted; AK tripped over
  them 2026-08-13.
- **v2 view** (`vipps.vipps_prod_transformed`, 2026-07-17) — *"the shape of the right answer,
  hardcoded to one customer"*:

  ```sql
  CASE
    WHEN main.user_id IS NOT NULL OR main.device_id IS NOT NULL THEN main.user_id
    WHEN REGEXP_CONTAINS(main.distinct_id, r'^[A-Za-z0-9]{8}$') THEN main.distinct_id
    WHEN REGEXP_CONTAINS(STRING(main.properties['distinct_id_before_identity']), r'^[A-Za-z0-9]{8}$')
      THEN STRING(main.properties['distinct_id_before_identity'])
    ELSE NULL
  END AS user_id,
  CASE
    WHEN main.user_id IS NOT NULL OR main.device_id IS NOT NULL THEN main.device_id
    WHEN <either regex above matches> THEN main.device_id
    ELSE COALESCE(main.distinct_id, STRING(main.properties['distinct_id_before_identity']))
  END AS device_id, ...
  ```

  The regex `^[A-Za-z0-9]{8}$` is Vipps' user-ID format (confirmed with the customer 2026-06-17).
  Generalizing that regex into a pluggable predicate **is** the missing tool.
- **The near-miss:** Ruqia to customer 2026-06-29 — *"~92% of events… already have either a
  `$user_id` or `$device_id` set… or resolvable through the identity graph. No action needed."*
  That sentence *is* the defect (resolution ≠ Simplified split), shipped for customer sign-off.
  Three weeks earlier she had correctly identified the `$device:`-prefix issue
  (*"device IDs… appear in two other formats: raw 36-character UUIDs and `$device:` prefixed
  UUIDs"*) — then the prefix bug was reintroduced in the seed table at ~66% coverage cost.
  Moral: the prefix decision must live in **one testable function**.
- Data point for validation design: 48.9% of Vipps pre-login web rows carry an *older anonymous
  ID* in `distinct_id` (an Original-ID-Merge aliasing artefact) — unprefixed, each becomes its own
  `$user_id`.
- Remaining Vipps estate: `vipps_prod_identity_mapping_v2/_v2_reviewed/_v3` (v3 = 41,128,461 rows,
  built 2026-08-15 by AK, not loaded), `vipps_prod_seed_events/_cleaned`,
  merchantportal id-pair/seed tables, pass-through `*_to_4042347_transformed` views,
  `*_people_updated` profile views.

#### 3.3 cosmos_migration (the only prior work with the right *shape*)

Ruqia, 2026-03-30, `mixpanel-sa.cosmos_migration`: two views —
`v_transformed_events` excludes `event_name = '$identify'` (and renames events to `'V1_Event'`,
stashing the original name in `properties.v1_event_name`); `v_identify_events` isolates
`$identify` rows `ORDER BY time ASC` for separate handling. I.e. isolate identity events, handle
separately, ordered. But it **drops** them rather than replaying them as dual-ID associations.
Per-customer, undocumented, never generalized. (Matches "identity-event drops for simplified ID
merge" in mixpanel/fde PROJECT-LOG.md:133.)

#### 3.4 Picklebet (the one clean precedent, Jan–Mar 2026, Dana-led, no script)

1. Customer stands up new Simplified project (`PICKLEBET-PRODUCTION` 3977292), points Segment at
   it (2026-01-19).
2. 30-day import into a dev project for validation (2026-01-26).
3. Dashboards cloned for old-vs-new comparison (2026-01-27).
4. Sign-off → profiles migrated first, then historical events export→BQ→load (2026-02-06).
5. Ghost/orphan profiles swept afterwards with an ad-hoc script.

This sequencing (validate on a slice → compare dashboards → sign-off → profiles → events → sweep)
is the de facto operational playbook, and matches fde's stated large-migration playbook:
*"validate > sample > dry-run > reconcile > throttled-full-run… scripts customer-specific."*

#### 3.5 Everything that claims to do it (all disconfirmed)

| Artifact | Reality |
|---|---|
| Official migration doc | "not possible to automatically convert"; DIY from mixpanel-utils |
| `mixpanel/mixpanel-utils` | Primitives only; its one migration path is Amplitude→**Original**-only |
| `ak--47/mpMigrate` | Archived Apr 2024; Dana's caveat list leads with "ID Mappings are not applied (no alias table in export)" |
| `ak--47/toMixpanel` | Amplitude→MP, not MP→MP |
| `mixpanel-power-tools customers/vipps/` | Entities only (inventory/nuke/clone/receipts/reshare/sweep-dupes); no events, no identity |
| `analytics` `enable_id_merge` | Legacy→Original only; `ID_MERGE_MODE_SIMPLIFIED` appears 3× org-wide, all enum plumbing |
| `dwe` migration handlers (`export_id_mapping.go`) | Exports the old graph per `IdentityMode`; never translates it |
| Notion Data Backfill/Migration Playbook | 2022 process doc, no code |

### 4. The proposed approach: identity replay

Core idea (§6.1): don't reconstruct the identity graph in SQL — **replay the source event stream
and reinterpret the identity events already in it**. Raw JSON export in → transform in-flight →
Import API out. No warehouse round-trip, so no reverse schematization.

#### 4.1 Proposed option shape (§6.4, verbatim semantics)

```js
await mpImport(creds, './vipps-prod-export/*.jsonl', {
  recordType: 'event',
  vendor: 'mixpanel',
  identity_replay: {
    // REQUIRED — the one thing the library can't infer
    isUserId: (candidate, record) => /^[A-Za-z0-9]{8}$/.test(candidate),

    // $identify / $create_alias / $merge handling:
    //   'rewrite' (default) → become a dual-ID association event
    //   'emit'              → keep original AND emit synthetic association (1→2 fan-out)
    //   'drop'              → discard (matches cosmos_migration behaviour)
    identityEvents: 'rewrite',
    associationEventName: 'Identity Association',

    // Non-identity events with a bare distinct_id:
    //   'validate' (default) → run isUserId; '$device:'-prefix on failure
    //   'passthru'           → leave alone (today's behaviour, unsafe)
    bareDistinctId: 'validate',

    // Extra props to check for a user id before giving up
    // (Vipps needed properties.distinct_id_before_identity)
    userIdFallbackProps: ['distinct_id_before_identity'],

    // $merge is always exactly 2 ids → 1:1 rewrite; ambiguity only when
    // both pass isUserId (two genuine user IDs — unrepresentable in v3)
    // or neither passes (two anon ids — no user to merge to):
    //   'drop' (default, counted+reported) | 'first' | 'error'
    onAmbiguousMerge: 'drop',

    // Fail closed if association coverage is suspiciously low
    minAssociationRate: 0.01
  }
});
```

#### 4.2 Transform semantics table (§6.5)

| Input | Output |
|---|---|
| `$identify` (`$identified_id: U`, `$anon_id: A`) | `{ event: "Identity Association", properties: { $user_id: U, $device_id: A, time, $insert_id } }` |
| `$create_alias` (`alias: X`, `distinct_id: Y`) | whichever of X/Y passes `isUserId` → `$user_id`; the other → `$device_id` |
| `$merge` (`$distinct_ids: [a, b]`, always exactly 2) | passer → `$user_id`, other → `$device_id`; both/neither pass → `onAmbiguousMerge` |
| Ordinary event with `$user_id` and/or `$device_id` | untouched |
| Ordinary event, bare `distinct_id`, **passes** `isUserId` | `$user_id = distinct_id` |
| Ordinary event, bare `distinct_id`, **fails** `isUserId` | `$device_id = distinct_id`; `distinct_id = "$device:" + distinct_id` |
| Ordinary event, `distinct_id` already `$device:`-prefixed | `$device_id = distinct_id.slice(8)`; leave `distinct_id` |

#### 4.3 What already exists in mixpanel-import (§6.3, verified against v3.5.2 / HEAD 722ae9a)

Pipeline (normal mode, `components/pipelines.js:681-691`):
`createExistenceFilter → createVendorTransform (1:1) → createUserTransform (:117) →
createFlattenStream (:145, 1→N fan-out) → createDedupeTransform → createExistenceFilter2 →
createHelperTransforms (:213, strictly 1:1 — v2_compat lives here) → createStringifyCacher →
batcher → http`.

- `v2_compat`: option at `components/job.js:361`, wiring `:417`, registration `:471`;
  implementation `setDistinctIdFromV2Props` at `components/transforms.js:684`. Sets `distinct_id`
  from `$user_id`/`user_id`, else `$device_id`/`device_id`, else `""`. Never overwrites an
  existing `distinct_id`. **Note: no `$device:` prefix** — see hazards.
- `createFlattenStream`: expands array returns from the user transform — fan-out is already
  first-class, one stage after the user-transform slot.
- `vendor/mixpanel.js` `mixpanelEventsToMixpanel` (:19): already maps raw warehouse-export rows
  (`device_id`/`user_id`/`distinct_id`/`insert_id`/`event_name`) to Import-API events with
  `$device_id`/`$user_id`/`$insert_id`. Natural host for the feature.
- `vendor/posthog.js:168-178`: synthesizes v2-shaped identity props (`$identified_id`, `$anon_id`)
  in the reverse direction; its v3 branch (:184) *drops* identify events. Both halves of the
  problem exist in the codebase, unjoined.
- The known gap is written down identically in `vendor/amplitude.js:16-18` and
  `vendor/posthog.js:19`: *"v2_compat sets distinct_id, but will not implicitly join
  user_id/device_id — in order to do this we would need to return `[{ $identify },{ ogEvent }]`
  and pass it down the stream"* — a limitation of the 1:1 vendor slot that
  `createFlattenStream` already solves one stage later.
- `insertIdTuple` already exists for deterministic derived `$insert_id`s.

#### 4.4 Placement (§6.6)

- Default (`'rewrite'`) mode is strictly 1:1 → could live anywhere, incl. `createHelperTransforms`
  next to `v2_compat`.
- `'emit'` mode is 1→2 → must sit **upstream of `createFlattenStream`**;
  `createHelperTransforms` is hard-1:1 (`callback(null, data)` at `pipelines.js:236`) and would
  swallow the second record. Spec's recommendation: put it upstream regardless, keep the option open.
- **Option A (prototype, zero library change):** implement as a `transformFunc` — user slot is
  already followed by flatten, so `'emit'` works too. Right way to test against a real
  Vipps/Klover export.
- **Option B (productize):** new stage between `createVendorTransform` and `createUserTransform`
  (with its own flatten), or register in the user slot ahead of any user-supplied fn. Add
  `identity_replay` to `JobConfig` next to `v2_compat` and to `index.d.ts` (next to
  `v2_compat?: boolean` at `:1022`).

### 5. Hazards and design constraints (§6.7)

1. **Mutual exclusion with `v2_compat`.** `v2_compat` sets bare `distinct_id` from `$device_id`
   with no `$device:` prefix — harmless while `$device_id` is present, but reintroduces phantom
   users if any later stage strips reserved props. Either make the options mutually exclusive or
   let `identity_replay` win and log.
2. **Ordering.** Associations should land before/with the events they merge (`cosmos_migration`
   used `ORDER BY time ASC`). Simplified reshuffles retroactively (`$is_reshuffled`), so strict
   ordering is a nice-to-have — but offer a **seed pass** mode that emits all associations first.
3. **`$insert_id` on synthesized events.** `'rewrite'` inherits the source `$insert_id` (1:1,
   safe). `'emit'` adds a record and needs a deterministic derivative (e.g.
   `hash(insert_id + '$assoc')`) so re-runs dedupe. Use `insertIdTuple`.
4. **Coverage telemetry is the whole point.** Report: events seen, associations emitted,
   bare-distinct_id user/device classification counts, `isUserId` pass rate, ambiguous-`$merge`
   count. `minAssociationRate` fails closed so a regex matching nothing can't quietly ship
   66%-broken data (as Vipps did).
5. **Lossy by construction for multi-user-ID clusters.** v3 = one `$user_id` per cluster; v2
   chained `$merge`s can build clusters with several genuine user IDs (each `$merge` is a pair,
   the cluster is not). Unpreservable. Count and report every ambiguous pair; never silently pick.
   Docs say needing multiple user IDs per user is a reason not to migrate at all.

### 6. Operational context worth keeping

- Migration destination is always a **new, empty** project on Simplified; org default flip only
  applies to new projects (#se-success 2023-10-12). Migration is one-way.
- The proven rollout sequence (Picklebet): 30-day slice into dev project → cloned dashboards for
  old-vs-new comparison → customer sign-off → profiles first → historical events → orphan-profile
  sweep. Any tooling should make the "slice + compare" validation step cheap.
- People/profile data is a separate concern from this transform (Picklebet migrated profiles
  first; Vipps had separate `*_people_updated` views). The spec's proposal covers **events only**.
- A "raw distinct_id/user_id alias mapping table" was requested by Dana in #support (2023-12-06)
  and never built — the replay approach makes it unnecessary because the identity events in the
  export *are* the mapping.
- Key people: Dana Ravnur (`dana.kock@mixpanel.com`, GH `danakock`) — ETL plumbing + resolution
  idiom; Ruqia Alzoubi (`ruqia.alzoubi@mixpanel.com`) — executed all Vipps transforms and the
  cosmos_migration views.

## Gotchas / Limits

- `$merge` carries **exactly two** IDs (identity.openapi.yaml: `minItems: 2, maxItems: 2`) — the
  rewrite is 1:1, never a fan-out. Only ambiguity handling is needed (`onAmbiguousMerge`).
- The `$device:`-prefix rule cuts both ways: strip-side (`distinct_id.slice(8)` → `$device_id`)
  and add-side (failed `isUserId` → prefix). Both must live in one tested function.
- `createHelperTransforms` is strictly 1:1 — parking `'emit'` mode there silently drops the second
  record. Verified: `callback(null, data)` at `pipelines.js:236`.
- `v2_compat` and `identity_replay` conflict on bare-`distinct_id` handling; must be exclusive.
- Simplified constraints that shape output: one `$user_id` per cluster, ≤500 identities per
  cluster, up-to-24h merge latency, irreversible.
- Vendor transforms are 1:1 by contract ("no flatten needed" is asserted at the vendor stage) —
  if the feature lives in `vendor/mixpanel.js`, `'emit'` mode cannot, unless the vendor slot's
  contract changes or the emit happens via the user slot.
- The spec's line-number references were taken at v3.5.2 (HEAD `722ae9a`) and will drift.
- `identity_replay.isUserId` is a **function-valued option** — fine for library use, but the CLI
  and web UI cannot pass functions; a productized version needs a serializable form (regex string,
  prop allowlist) or must be library/`transformFunc`-only at first.
- The source doc contains Slack permalinks and internal BQ dataset names — fine to reference, but
  the transcripts themselves (`smarterchild/tmp/`) are gitignored and may disappear.

## Open Questions

1. **Profiles and group profiles:** the spec covers events only. Do user/group profile migrations
   need identity translation too (they key on the canonical `distinct_id`), or is the existing
   profile-export→import path sufficient once events establish the clusters?
2. **Seed-pass mechanics:** should "emit all associations first" be a separate job/recordType
   (two-pass) or an internal ordering buffer? The spec leaves this as "the option should let you."
3. **CLI/UI surface for `isUserId`:** function in library mode — what's the serializable
   equivalent (regex string? list of fallback props?) for CLI users, if any?
4. **`identityEvents: 'emit'` mode value:** is keeping the original v2 verbs in a v3 project ever
   useful (they'd be inert junk events in Simplified), or is `'rewrite'`/`'drop'` enough?
5. **`$create_alias` directionality:** in v2, alias semantics are directional
   (alias → distinct_id). The spec resolves both sides purely via `isUserId`; is there a case
   where both sides fail the predicate (anon-to-anon alias) — and is that `onAmbiguousMerge`-like
   ('drop'/'count') or a distinct `onAmbiguousAlias` knob?
6. **Interaction with `mp_identity_mappings_data_view`-style resolution:** should the transform
   optionally consume an identity-mappings export as a *supplementary* source of pairs (for
   clusters whose identity events predate the export window or were retention-expired)?
7. **What does the export actually contain?** The design assumes `$identify`/`$create_alias`/
   `$merge` events are present and complete in the raw export for the whole project history —
   needs verification per customer (retention, event TTLs, whether /export returns identity verbs).
8. **`associationEventName` semantics:** does a custom event name carrying `$user_id`+`$device_id`
   reliably trigger the merge in Simplified, or should the synthetic event be a reserved name?
   (Rule 1 says any event with both IDs merges — assumed yes, worth a live test.)
