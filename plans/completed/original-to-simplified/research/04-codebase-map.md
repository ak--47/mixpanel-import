# 04 — Codebase Map: mixpanel-import internals for the original→simplified ID-merge migration pipeline

Research slice for the sprint: "replay events from an original-id-merge Mixpanel project into a new
simplified-id-merge project, translating $identify / $create_alias / $merge instead of dropping them."
All file:line references are against the repo at commit `722ae9a` (v3.5.2 era, 2026-08).

## Summary

The codebase already has an end-to-end "export → re-import" streaming path (`recordType:
'export-import-event'`) that lazily streams raw events out of `/api/2.0/export` and pushes them back
through the normal ingest pipeline — this is the natural chassis for the migration feature. The
pipeline is a linear chain of Node Transform stages assembled per-job in `corePipeline`
(`components/pipelines.js:560`), with a well-defined slot structure: existence filter → vendor
transform → user transform → flatten → dedupe → existence filter 2 → helper transforms → stringify
cacher → smart batcher → HTTP sender → logger. A new identity-translation stage that emits extra
synthetic "graph-completion" events fits cleanly as a new dedicated Transform stage between the
vendor transform and the flatten stage (or as a multi-record-emitting stage anywhere before dedupe),
because `createFlattenStream` expands arrays and dedupe/helpers run downstream of it.

Two important confirm/refute findings: (1) **the claim "today we just filter out $identify + friends"
is REFUTED for the mixpanel vendor and for the export-import path** — `vendor/mixpanel.js` does a
naive field remap and filters nothing; nothing anywhere in the codebase special-cases `$identify`,
`$create_alias`, `$merge`, `$mp_original_distinct_id`, or `$distinct_id_before_identity` on the
mixpanel→mixpanel path (grep confirms zero hits). The only identity-verb handling that exists is in
the **posthog** vendor (converts `$identify` → a synthetic `identity association` event carrying
`$user_id` + `$device_id`; drops `$merge_dangerously`) and the **heap** vendor (same
`identity association` pattern) — these are the in-repo model for what the migration stage should do.
(2) The `ezTransforms` shape-fixer is constructed for export-import record types but is **only
actually executed when `fixData: true`** (job.js:469 gates on `fixData` alone), so export-import jobs
effectively require `fixData: true` to re-nest the flattened export records.

## Key Facts

- Pipeline stage order (normal mode), `components/pipelines.js:680-746`: `createExistenceFilter` (682) → `createVendorTransform` (684) → `createUserTransform` (686) → `createFlattenStream` (687) → `createDedupeTransform` (688) → `createExistenceFilter2` (689) → `createHelperTransforms` (690) → `createStringifyCacher` (691) → `createSmartBatcher` (734) → optional `createTeeStream` (738) → `createHttpSender` (743) → `createLogger` (744).
- Vendor transforms are called 1:1 per record: `job.vendorTransform(data, job.heavyObjects)` (`pipelines.js:96`); returning `null`/`undefined` drops the record (`pipelines.js:99-101`, counted in `job.empty`). Arrays are not expanded until the flatten stage at 687, which sits AFTER the user transform — so a vendor transform returning an array only works when no `transformFunc` is set.
- User transform: `job.transformFunc(data, job.heavyObjects)` (`pipelines.js:124`); may return an array — `createFlattenStream` (`pipelines.js:145-160`) `this.push()`es each element. This is the documented "explode 1 row into N" mechanism (`index.d.ts:1093-1101`).
- `export-import-event` rewiring: `components/parsers.js:660-679` — builds `streamEvents(job)`, then mutates `job.recordType = 'event'`, swaps `job.token = job.secondToken`, clears `job.secret`, sets `job.project = job.secondProject || ""` (note: `secondProject` is never declared anywhere — always empty), recomputes `job.auth`, and applies `job.secondRegion`.
- `streamEvents` (`components/exporters.js:1137-1196`): GET `https://data.mixpanel.com/api/2.0/export` (via `job.url`, region-aware, `job.js:602`), got.stream with `retry: {limit: 50}`, params `from_date`, `to_date`, `limit`, `where` ONLY (`job.params` is ignored on this path, unlike `exportEvents`). Each NDJSON line `{event, properties}` is **flattened**: `{...parsed, ...parsed.properties}` with `delete event.properties` (`exporters.js:1173-1175`).
- `ezTransforms` (`components/transforms.js:204-526`) re-nests flat records into `{event, properties}`, resolves time aliases, adds murmur3 `$insert_id` if missing, `$`-prefixes `user_id`/`device_id`/`source`, stringifies and sanitizes ids (drops `badUserIds` like "null"/"anonymous", `transforms.js:138`), truncates strings to 255 chars. Constructed when `fixData || recordType.includes('export-import')` (`job.js:410`) but pushed into `activeTransforms` only `if (this.fixData)` (`job.js:469`).
- New options flow: `index.d.ts` `Options` type (source of truth) → `components/job.js` constructor (`this.x = opts.x ?? default`) → transform factories in constructor (`job.js:410-483`) or stage factories in `pipelines.js` read `job.x` → CLI flag in `components/cli.js` → env var mapping (optional) in `getEnvVars` (`parsers.js:1221-1238`).
- Cross-record state precedents: `job.hashTable` (a `Set` for dedupe, `job.js:145`), `job.heavyObjects` populated by `job.init()` → `insertHeavyObjects()` → `buildMapFromPath(filePath, keyOne, keyTwo)` (`parsers.js:1251`, returns `Map`), consumed by posthog vendor as `heavyObjects.people` (`vendor/posthog.js:52`).
- `validateToken` (`components/validators.js:20-81`, exported as `mpImport.validateToken`) already detects a project's ID-merge generation by probing `/import`: `idmgmt_v2` (original) vs `idmgmt_v3` (simplified).
- Ordering gotcha: `job.init()` (vendor selection) runs BEFORE `determineDataType` (recordType rewrite) — `index.js:161` vs `index.js:171`. Factories that capture `recordType` at construction see `export-import-event`, not `event`.

## Details

### 1. Orchestration flow (`index.js`)

`main(creds, data, opts, isCLI)` at `index.js:69`:

1. `getEnvVars()` (`parsers.js:1221`) merges `MP_PROJECT/MP_ACCT/MP_PASS/MP_SECRET/MP_TOKEN/MP_TYPE/MP_TABLE_ID/MP_GROUP_KEY/MP_START/MP_END` into creds/opts. Passed-in creds/opts win wholesale (no deep merge) — `index.js:80-90`.
2. `new importJob(finalCreds, finalOpts)` (`index.js:92`) — ALL option plumbing and transform-factory construction happens in this constructor (see §5).
3. `await job.init()` (`index.js:161`) — loads `heavyObjects` from `dimensionMaps` and selects `job.vendorTransform` from the `vendor` switch (`job.js:675-812`).
4. `stream = await determineDataType(data || cliData, job)` (`index.js:171`) — input detection AND the export-import recordType rewrite (see §6).
5. `await corePipeline(stream, job)` (`index.js:185`).
6. `job.summary()` returned as `ImportResults` (`index.js:207`, `job.js:1082`).

Exports (`index.js:340-343`): `main` (default), `main.validateToken`, `mpImport.createMpStream`
(= `pipeInterface`, `index.js:272`, returns the first pipeline stage as a writable Transform), and
`mpImport.destroy` (undici pool teardown). The CLI branch (`require.main === module`,
`index.js:346-381`) is the only place with process-level signal handlers — hard rule, do not add any
in `components/` or `vendor/` (enforced by `tests/handlers.test.js`).

### 2. corePipeline stage assembly (`components/pipelines.js`)

`corePipeline(stream, job, toNodeStream)` at `pipelines.js:560`.

**Non-streaming short-circuits** (`pipelines.js:563-575`) — these NEVER reach the stage pipeline:
`table` → `flushLookupTable`; `export` (string path) → `exportEvents`; `profile-export` (string
path) → `exportProfiles`; `annotations`/`get-annotations`/`delete-annotations` → meta.js;
`profile-delete` → `deleteProfiles`. Note `export-import-*` is NOT in this list — by the time
`corePipeline` runs, parsers.js has already rewritten it to `event`/`user`/`group`, so it flows
through the streaming stages like any ingest job.

**Stage list, normal mode** (fastMode collapses to existence + stringify, `pipelines.js:615-679`):

| # | Stage | File:Line | Behavior relevant to migration |
|---|-------|-----------|-------------------------------|
| 0 | `createMemoryMonitor` | pipelines.js:610-612 | only when `verbose \|\| memoryMonitor` |
| 1 | `createExistenceFilter` | pipelines.js:45-81 | counts `recordsProcessed`, enforces `maxRecords`, drops empties (`isNotEmpty`) |
| 2 | `createVendorTransform` | pipelines.js:88-110 | `job.vendorTransform(data, job.heavyObjects)`; null/undefined return = drop (`job.empty++`) |
| 3 | `createUserTransform` | pipelines.js:117-138 | `job.transformFunc(data, job.heavyObjects)`; null/undefined = drop |
| 4 | `createFlattenStream` | pipelines.js:145-160 | `Array.isArray(data)` → push each item. THE array-expansion point |
| 5 | `createDedupeTransform` | pipelines.js:167-182 | `job.deduper(data)` only when `job.dedupe`; dupes become `{}` |
| 6 | `createExistenceFilter2` | pipelines.js:189-205 | drops `{}` produced by dedupe/white-black-listing |
| 7 | `createHelperTransforms` | pipelines.js:213-240 | iterates `job.activeTransforms` (see §5); `mutates:false` entries reassign `data`, may return null to drop |
| 8 | `createStringifyCacher` | pipelines.js:249-274 | JSON.stringify for byte counting; `bytesCache` WeakMap (`jsonCache` is null/disabled) |
| 9 | `createSmartBatcher` | pipelines.js:283-340 | batches by `recordsPerBatch` (2000 cap) AND `bytesPerBatch*0.985` (~9.65MB) |
| 10 | `createTeeStream` | pipelines.js:738 | only when `job.destination` set (dual-write) |
| 11 | `createHttpSender` | pipelines.js:444-494 | `ParallelTransform` with `job.workers` (default 50); picks `flushToMixpanelWithUndici` vs `flushToMixpanel` by `job.transport` (445); honors `dryRun` (473) and `writeToFile` (479) |
| 12 | `createLogger` | pipelines.js:502-551 | progress + `responseHandler` + dryRun collection |

`createBatcher` (349) and `createSizeBatcher` (391) are deprecated — do not add call sites.

**`destinationOnly`** (`pipelines.js:695-731`): records bypass batching/HTTP entirely and are
written one at a time to the destination stream — useful for a "dry-run the translation to a file"
mode of the migration (also `writeToFile`/`outputFilePath` inside the HTTP sender, and `dryRun`
which collects transformed records into `job.dryRunResults`).

### 3. Where a new identity-translation stage slots in

Constraints derived from the stage order:

- **Must see raw exported records** (with `$identify`/`$merge`/`$create_alias` names and their
  props) → must run before `createHelperTransforms` (ezTransforms renames/sanitizes ids and could
  delete e.g. a literal `"anonymous"` distinct_id via `badUserIds`, transforms.js:291-295).
- **Wants to emit N records for 1 input** (graph-completion events) → either (a) sit before
  `createFlattenStream` and return arrays, or (b) be a dedicated Transform that `this.push()`es
  multiple times (works anywhere).
- **Synthetic events should still get `$insert_id`/time fixups** → emit before
  `createHelperTransforms` and run with `fixData: true` so `ezTransform` post-processes them.
- **Dedupe of identical synthetic events** → emit before `createDedupeTransform` (688) and set
  `dedupe: true`; the deduper murmur-hashes the stable-stringified record (transforms.js:727-740).

Recommended slot: a new `createIdentityTranslator(job)` stage inserted in the `stages.push(...)`
block at `pipelines.js:682-691`, between `createVendorTransform` (684) and `createUserTransform`
(686) — mirroring how vendor transforms are gated (`if (job.vendor && job.vendorTransform)` inside
the stage, so the stage can be pushed unconditionally and no-op when the option is off), or
conditionally pushed like the memory monitor (610). Between vendor and user keeps the contract
"user transformFunc sees what will be imported (including synthetics)"; putting it after flatten
(687) instead would let it also see records exploded by a user transformFunc, but then user
transforms would not see the synthetics. Either works mechanically; pick based on desired UX.

Alternative zero-new-stage prototypes that work today:
- Implement translation as a `transformFunc` (arrays natively supported via flatten). Signature
  `(record, heavyObjects) => record | record[] | {} | null`.
- Implement as a new `vendor: 'mixpanel'` mode via `vendorOpts` — but vendor transforms returning
  arrays break if the user also sets `transformFunc` (the array reaches `transformFunc` unexpanded,
  pipelines.js:124), so a dedicated stage is safer for a first-class feature.

`fastMode` skips stages 2-7 entirely (`pipelines.js:615-620`) — identity translation is incompatible
with `fastMode`; the feature should guard/throw on that combination.

### 4. Transform helpers (`components/transforms.js`)

Factory pattern throughout: `factory(job or params) => (record) => record'`. Conventions:
- Return `{}` to drop (older style; caught by existence filters and `isNotEmpty`, transforms.js:904).
- Return `null` to drop (newer style; `createHelperTransforms` breaks the chain on null,
  pipelines.js:229; vendor/user stages treat null as drop).
- `mutates: true` entries (default) mutate the record in place; `mutates: false` entries return a
  new/replaced record (`job.activeTransforms` metadata, consumed at pipelines.js:220-232).

Key helpers and line refs:
- `ezTransforms(job)` transforms.js:204 — the shape fixer, three branches:
  - Event branch (206-306): matches `recordType.startsWith("event")` OR `=== "export-import-event"`.
    Handles the export shape: wraps loose root keys into `properties` (215-222) — this is what
    re-nests `streamEvents`' flattened records. Time alias resolution (231-239: `timestamp`,
    `event_time`, `ts_utc`, `ts`), string time → epoch ms (242-247), `$insert_id` = murmur3 of
    `event-distinct_id-time` if missing (250-261), `user_id/device_id/source` → `$`-prefixed
    (264-269), `specialProps` promotion (272-281), id stringification (284-288), `badUserIds`
    scrub (291-295, list at 138 — includes `"anon"`, `"anonymous"`, `"null"`, `"undefined"`, `"-1"`,
    `"0"`, the nil UUID…), 255-char truncation (298-302).
  - User branch (309-423): matches `startsWith("user")` OR (`export-import-profile` && !groupKey).
    Wraps into `{$set: {...}, $distinct_id}` honoring `job.directive`; unwraps the engage-export
    `$properties` shape (372-375).
  - Group branch (427-522): matches `startsWith("group")` OR (`export-import-profile` && groupKey).
    NOTE: plain `export-import-group` matches NEITHER branch → noop ezTransform (see Gotchas).
- `matchMixpanelDefaults(job)` transforms.js:168 — explicitly recordType-aware including
  `export-import-*` variants (172-181); good example of handling the pre-rewrite type names.
- `dedupeRecords(job)` transforms.js:727 — murmur3(stable-stringify(record)) against `job.hashTable`.
- `epochFilter(job)` transforms.js:876 — reads `jobConfig.recordType` at CALL time (880), so it
  works after the export-import rewrite. Returns `null` to drop; increments `job.outOfBounds`.
- `addTags` (600), `applyAliases` (626), `addToken` (659) — these capture
  `const type = jobConfig.recordType` at FACTORY time (601/627/660), which for export-import jobs is
  the pre-rewrite name → the `type === "event"` branch never matches → **tags/aliases silently no-op
  on export-import-event jobs**. A new stage must not repeat this pattern (read `job.recordType`
  lazily, or normalize).
- `whiteAndBlackLister` (747) returns `{}` to drop, counts `whiteListSkipped`/`blackListSkipped`.
- `fixTime` (914), `addInsert` (941), `fixJson` (967), `scrubProperties` (1050), `dropColumns`
  (1062), `flattenProperties` (530), `removeNulls` (568), `UTCoffset` (707),
  `setDistinctIdFromV2Props` (684 — `v2_compat`: copies `$user_id`/`$device_id` into `distinct_id`),
  `scdTransform` (1156).
- `isNotEmpty` (904): false for non-objects, `[]`, `{}`, null — arrays with elements pass, so arrays
  survive existence filters and reach flatten.

`activeTransforms` execution order (as pushed, `job.js:467-483`): applyAliases → scdTransform →
**ezTransform** → matchMixpanelDefaults → v2CompatTransform → nullRemover → UTCoffset → addTags →
whiteAndBlackLister → epochFilter → propertyScrubber → columnDropper → flattener → jsonFixer →
insertIdAdder → tokenAdder → timeTransform.

### 5. JobConfig plumbing (`components/job.js`)

Constructor `(creds, opts)` — everything is a flat assignment; no schema validation beyond
recordType checks (539-562: rejects plurals; validates against
`['event','user','group','table','export','scd','export-import-','profile-export','profile-delete']`
using `.includes()`, so any `export-import-*` string passes).

Adding a new option, the established recipe (e.g. `resumeOnStall` as the model):
1. `index.d.ts`: add to `Options` with JSDoc (`@default`, `@example`) — source of truth.
2. `job.js` constructor: `this.myOpt = u.isNil(opts.myOpt) ? DEFAULT : opts.myOpt;` (booleans) or
   `opts.myOpt || DEFAULT` (strings/numbers) — cluster near related options.
3. If the option gates a transform: construct the closure in the "transform conditions" block
   (job.js:410-463) and push into `this.activeTransforms` (466-483) — OR, for a full stage, read
   `job.myOpt` inside `corePipeline`/a new `createX(job)` factory in pipelines.js.
4. `components/cli.js`: `.option("myOpt", {...})` (yargs). CLI ⇢ opts is automatic since `main`
   spreads the parsed argv into both creds and opts (`index.js:83,89`).
5. Optionally `getEnvVars` (`parsers.js:1221`) for an `MP_*` env var.

Relevant existing fields: `secondToken` (creds, job.js:130), `secondRegion` (opts, job.js:265),
`vendor` (267, re-assigned redundantly at 485), `vendorOpts` (369, JSON-string tolerant via
`parse()`), `transformFunc` (387), `heavyObjects` (196), `dimensionMaps` (194) +
`insertHeavyObjects()` (197-206, runs once in `init()`), `dedupe`/`hashTable` (315/145),
`limit`/`whereClause` (211-216), `params` (219 — export-file path only), `start`/`end` (222-235,
default last 30 days), `workers` default 50 (275), `recordsPerBatch` default 2000 (270, hard-capped
at 2000 for event/user/group at 295-297), `bytesPerBatch` 9.8MB (271), `highWater` auto
`min(workers*10, 500)` (277-282).

`init()` (675-812): vendor switch. For `vendor: 'mixpanel'` every recordType (including the not-yet-
rewritten `export-import-event`) gets `mixpanelEventsToMixpanel(this.vendorOpts)` (685-694).
amplitude/ga4/mparticle/posthog/june user-branches force `dedupe = true`. posthog events get
`postHogEventsToMp(this.vendorOpts, this.heavyObjects)` — the only vendor receiving heavyObjects at
factory time (773).

Endpoints map (593-631): per region (us/eu/in) and recordType. `export-import-event` →
`data.mixpanel.com/api/2.0/export`; `export-import-profile`/`-group` →
`mixpanel.com/api/2.0/engage`; after the rewrite `job.url` resolves to the ingest endpoint
(`api.mixpanel.com/import` for events). `resolveProjInfo()` (903-963) builds Basic auth: token-first
for imports, acct/pass service account, secret fallback; throws for exports lacking secret/SA and
for SA-without-project on export types (920-926).

Counters the new stage can/should use: `job.empty`, `job.duplicates`, `job.outOfBounds`,
`job.whiteListSkipped` etc. (488-506); a migration stage would likely add its own counters (e.g.
`identityEventsTranslated`, `syntheticEventsEmitted`) plus `summary()` inclusion (job.js:1091-1139
and the abridged `includeOnly` list at 1163-1193).

### 6. export-import-* rewiring (`components/parsers.js`)

`determineDataType(data, job)` (350) → `handleSpecialRecordTypes(data, job)` (639) runs FIRST:

- `export` (641-658): resolves a local/cloud output path (returned as a string; corePipeline then
  short-circuits to `exportEvents`). Default `./mixpanel-exports/export-<date>-<rand>.ndjson`.
- **`export-import-event` (660-679)**:
  ```
  const exportStream = streamEvents(job);   // built with SOURCE creds/region/url
  job.recordType = 'event';                 // rewrite BEFORE corePipeline
  if (job.secondToken) {
      job.token = job.secondToken;
      job.secret = "";
      job.project = job.secondProject || ""; // secondProject is never set anywhere — always ""
      job.auth = job.resolveProjInfo();      // re-auth as DESTINATION
  }
  if (job.secondRegion) job.region = job.secondRegion;
  return exportStream;
  ```
  The comment at 668-670 documents why project is cleared: a source project_id leaking into the
  destination `/import` URL mismatches the destination token and Mixpanel silently rejects batches.
- **`export-import-profile` / `export-import-group` (681-697)**: `streamProfiles(job)`; becomes
  `group` when it was `export-import-group` OR `dataGroupId` OR `groupKey` set, else `user`; same
  secondToken/secondRegion swap.
- Also here: streams pass through (737-741), in-memory arrays become object streams (744-747).

Sequencing (critical for the new feature): Job constructor → `job.init()` (vendor chosen) →
`determineDataType` (recordType rewritten, auth swapped) → `corePipeline` (stages built; stage
factories that read `job.recordType` now see `event`). Anything keyed off recordType at
construction time must handle the `export-import-*` names explicitly (as ezTransforms and
matchMixpanelDefaults do).

`buildMapFromPath(filePath, keyOne, keyTwo, job)` (1251-1316): loads a local/GCS/S3 `.json/.jsonl/
.ndjson` file fully into memory and returns `Map(keyOne→keyTwo)`. Wired via `opts.dimensionMaps =
[{filePath, keyOne, keyTwo, label}]` → `job.heavyObjects[label]` during `init()`. This is the
existing mechanism for a precomputed identity map (e.g. a first pass over exported data building
canonical-id clusters, then a replay pass consuming the map).

### 7. Raw event export (`components/exporters.js`)

Two distinct paths, both `got`-based (never undici):

- **`exportEvents(filename, job)`** (32-…): `recordType: 'export'` file export. GET
  `data.mixpanel.com/api/2.0/export` with `from_date`, `to_date`, `...job.params`, plus `limit` (57)
  and `where` (59); `project_id` appended only for service-account auth (64). Response is NDJSON;
  each line is written **unmodified** (`{event, properties}` shape preserved) unless
  `job.transformFunc` is set — the transform runs per line and MAY return an array (each element
  written, 150-168). `skipWriteToDisk: true` collects rows in memory instead. Handles gzip +
  GCS/S3 write streams, manual 429 retries.
- **`streamEvents(job)`** (1137-1196): the export-import source. Lazy `got.stream`, retry limit 50.
  Params: `from_date`, `to_date`, `limit`, `where` ONLY — **`job.params` is not merged here**.
  NDJSON parser **flattens each event**: `const event = {...parsed, ...parsed.properties}; delete
  event.properties;` (1172-1175). Malformed lines are silently swallowed (1177-1179). So downstream
  stages see flat records: `{event: "...", time: <epoch-seconds>, distinct_id: "...",
  $insert_id: "...", $device_id?, $user_id?, ...customProps}`. NOTE: flattening means a property
  literally named `event` would collide with the event name (property wins the spread order? —
  properties are spread after the parsed root, so a property named `event` OVERWRITES the event
  name; edge case worth a test).
- **`streamProfiles(job)`** (1205-1271): POST `mixpanel.com/api/2.0/engage` (via `job.url`),
  page/session_id pagination via a pull-based Readable (backpressure defers the next HTTP page);
  flattens `$properties` into the profile root (1259-1263); `filter_by_cohort` / `data_group_id`
  body params (1238-1242).

What the raw export returns for identity verbs (Mixpanel API domain knowledge — NOT encoded
anywhere in this repo): original-ID-merge projects export `$identify` events (props
`$identified_id`, `$anon_id`), `$create_alias` (props `distinct_id`, `alias`), `$merge` (prop
`$distinct_ids: [idA, idB]`); regular events carry the cluster-resolved `distinct_id` plus
`$distinct_id_before_identity` holding the pre-merge id when the event was remapped. The codebase
contains **zero** references to `$distinct_id_before_identity`, `$identified_id`, `$anon_id`,
`$distinct_ids`, or `$mp_original_distinct_id` (verified by grep) — the translation stage owns all
of that logic from scratch.

Time in raw exports is epoch **seconds**; ezTransforms only converts non-numeric time strings
(transforms.js:242-247), so seconds pass through untouched (fine — `/import` accepts both).

### 8. Vendor transform contract + `vendor/mixpanel.js` findings

Contract (from `createVendorTransform`, pipelines.js:88-110, and `job.init()`):
- Factory: `(vendorOpts [, heavyObjects]) => transform(record [, heavyObjects]) => record | null`.
  posthog gets heavyObjects at factory time (job.js:773); all transforms also receive
  `job.heavyObjects` as the 2nd runtime arg (pipelines.js:96).
- 1:1 by convention ("Vendor transforms are 1:1, no flatten needed", pipelines.js:685); `null` drop
  is supported and used (posthog). Arrays technically reach flatten only if no user transformFunc
  intervenes — do not rely on this for a product feature.

**`vendor/mixpanel.js` (all 51 lines)**: `mixpanelEventsToMixpanel(options)` (19-38) destructures
`v2_compat = true` and then never uses it (dead option). The transform copies `mpEvent.properties`,
then lifts top-level `device_id → $device_id`, `distinct_id → distinct_id`, `insert_id →
$insert_id`, `time → time`, `user_id → $user_id`, and renames `event_name → event` (defaulting
"unnamed"). That's it. **It expects the Mixpanel *cloud-storage/data-pipeline* schema** (top-level
un-prefixed `device_id`/`user_id`/`insert_id`/`event_name`), NOT the flattened `/api/2.0/export`
schema (which has `$device_id`/`$user_id`/`$insert_id` and `event`). **It filters nothing** — no
$identify/$create_alias/$merge handling of any kind. Claim "today we just filter out $identify +
friends": REFUTED for this vendor; those events pass through and (in a simplified-ID-merge
destination) are dropped/rejected server-side, which is presumably the behavior the sprint fixes.

The in-repo models for identity translation:
- **posthog** (`vendor/posthog.js:23-224`): `ignore_events` default list includes
  `$merge_dangerously` (36) — dropped via a precompiled regex, `return null` (91). In simplified
  mode (`!v2_compat`, 184-211), `$identify` becomes a synthetic
  `{event: 'identity association', properties: {$user_id: <posthog distinct_id>, $device_id:
  <$device_id || $anon_distinct_id>, $insert_id, time, ...}}`. In `v2_compat` mode (164-181) it
  instead keeps `$identify` with `$identified_id`/`$anon_id` props (the original-ID-merge shape).
  Uses `heavyObjects.people` as a person_id→distinct_id Map (50-58, 142-146).
- **heap** (`vendor/heap.js:123-205`): events with `heapEvent.identity` become
  `event: "identity association"` with `$device_id` + `$user_id = identity` (179-186); optional
  `device_id_file` builds a device→user Map at factory time (124-132).

The comment at `vendor/amplitude.js:17-18` and `vendor/posthog.js:19` ("in order to do this we
would need to return [{ $identify },{ ogEvent }] and pass it down the stream") is the historical
note that multi-event emission from vendors was considered and punted — exactly what the
graph-completion requirement now needs, best served by a dedicated stage.

### 9. Options/type declarations (`index.d.ts`) + CLI

- `RecordType` union at index.d.ts:43-59 — includes `export-import-event/-profile/-group` and also
  `group-export`/`group-delete` (declared in the type but NOT implemented in job.js endpoints —
  aspirational).
- `Creds` (72-141): `secondToken` at 116 ("for export/import (the destination project)").
- `Options` (198-…): `vendor` 590, `vendorOpts` 601 (typed as union of per-vendor opts),
  `transformFunc` 504 (type `transFunc` at 1098-1101: `(data, heavyObjects?) => mpEvent | mpUser |
  mpGroup | Object[] | Object`; `{}` = skip, array = split), `fixData` 512, `secondRegion` 966-970,
  `dimensionMaps` 957, `heavyObjects` 963, `dryRun` 806, `maxRecords` 813, export options
  `start/end/limit/whereClause/params/cohortId/dataGroupId` 828-875, output `writeToFile/
  outputFilePath/destination/destinationOnly/skipWriteToDisk` 887-918. Vendor opts types at
  1454-1520 (`postHogOpts` has `v2_compat`, `ignore_events`, `identify_events`, `ignore_props`).
  New migration options belong in a new `═══` section here, plus a `migrationOpts`-style type if the
  feature takes structured config (mirror `postHogOpts`).
- `validateToken` declared at 35-39 returning `type: "idmgmt_v2" | "idmgmt_v3" | "unknown"`.
- CLI (`components/cli.js`): `secondToken` flag at 51-55; `type/recordType` at 66-72 lists the
  export-import types in its describe; `fix/fixData` defaults **false** at 147-153; `vendor` and
  `vendorOpts` flags exist further down. CLI defaults `workers: 10` (122-127) vs library default 50.

### 10. Auth + validation notes for the migration

- After the parsers.js swap, an export-import job authenticates the *export* leg with
  source `secret` (or SA), and the *import* leg with `secondToken` (token Basic auth needs no
  project id). Without `secondToken`, the same project is both source and destination (the
  round-trip test relies on server-side `$insert_id` dedupe: `tests/int.optional.test.js:318-335`).
- `validateToken(token)` (`validators.js:20-81`) probes with a 4-year-old event lacking
  `distinct_id`: 400 + "'properties.distinct_id' is invalid" → `idmgmt_v2` (original); 200 +
  1 imported → `idmgmt_v3` (simplified). The migration feature can use this to verify the
  destination is actually a simplified project before replaying (note: it *sends a real test
  event* to the project).
- `.env` for integration tests (`tests/int.optional.test.js:2-14, 26-44`): `MP_PROJECT/ACCT/PASS/
  SECRET/TOKEN/TABLE_ID` required; export tests use `MP_EXPORT_PROJECT`/`MP_EXPORT_SECRET`
  (project 3996669 per memory — always pass `limit`); profile export uses
  `MP_PROFILE_EXPORT_SECRET` (project 4017669, secret-only). Base test opts include
  `fixData: true` (line 123) — which is why export-import tests pass despite the fixData gate.

## Gotchas / Limits

1. **ezTransform gate**: `job.js:410` builds ezTransforms for `export-import-*`, but `job.js:469`
   only pushes it into `activeTransforms` when `fixData` is true. Export-import without
   `fixData: true` re-imports FLAT records (no `properties` nesting) which `/import` rejects.
   Effectively `fixData: true` is mandatory on this path today; the migration feature should either
   force it or replicate the re-nesting.
2. **`export-import-group` misses every ezTransforms branch** (transforms.js:206/309/427 check
   `startsWith("group")` and `export-import-profile`+groupKey, neither matches the literal
   `export-import-group`) → noop shape-fixing even with fixData. The wired test avoids this by
   using `export-import-profile` + `groupKey` (int.optional.test.js:363-389).
3. **Factory-time recordType capture**: `addTags`/`applyAliases`/`addToken` capture
   `jobConfig.recordType` before the parsers.js rewrite → they silently no-op for
   export-import-event (`type === "event"` never true). `epochFilter` reads at call time and works.
   Any new stage must read `job.recordType` lazily or handle `export-import-*` names explicitly.
4. **`job.init()` runs before the recordType rewrite** (index.js:161 vs 171): vendor selection for
   `export-import-event` falls into each vendor's `default` switch arm (job.js:691 etc.).
5. **`streamEvents` ignores `job.params`** (exporters.js:1138-1143) — only from_date/to_date/limit/
   where. You cannot pass `event: [...]` filters to the export-import export leg today; use
   `whereClause`. (`exportEvents` for `recordType: 'export'` DOES merge `job.params`,
   exporters.js:39-44.)
6. **`secondProject` is referenced but never settable** (parsers.js:671/691 — not in Creds/Options,
   never assigned) → destination auth is token-only. Fine for `/import` with token Basic auth, but
   a migration wanting destination service-account auth needs to add `secondProject` plumbing.
7. **Flattening collision**: `streamEvents` spreads properties over the root AFTER the parsed line
   (`{...parsed, ...parsed.properties}`), so a custom property named `event` would clobber the
   event name; `$identify`'s `$identified_id`/`$anon_id` etc. land at the record root.
8. **Vendor transforms returning arrays**: only expanded by flatten if no `transformFunc` is set;
   `createUserTransform` would otherwise hand the whole array to the user function. Dedicated stage
   > array-returning vendor for multi-emit.
9. **`badUserIds` scrub** (transforms.js:138, applied at 291-295 under fixData): values like
   `"anonymous"`, `"null"`, `"-1"` are DELETED from `distinct_id`/`$user_id`/`$device_id`. If an
   original-ID-merge project used such placeholder ids, translation must run before ezTransforms or
   the ids vanish. (Translation before helpers — the recommended slot — is safe.)
10. **fastMode bypasses vendor/user/flatten/dedupe/helpers** — incompatible with translation.
11. **dedupe is whole-record hash** (stable stringify) — synthetic graph-completion events that
    differ only by `$insert_id`/time will NOT dedupe; design synthetic-event identity deliberately
    (deterministic `$insert_id` from the id-pair is the natural choice, cf. murmur3 usage at
    transforms.js:257).
12. **Strict-mode `/import` requirements** apply to synthetics: `$insert_id`, `time`, and (v3)
    `$device_id` or `$user_id`; `flushToMixpanel` sends `strict: Number(job.strict)` (importers.js:116),
    default strict=true (job.js:301).
13. **Memory**: an identity graph over a whole project can be large; precedents are the in-memory
    dedupe `Set` and `buildMapFromPath` (loads whole file into memory, parsers.js:1276). A two-pass
    design (pass 1: export identity events → build map; pass 2: replay with map via
    `dimensionMaps`/`heavyObjects`) fits existing machinery with zero new plumbing.
14. **Export API is 100-events-per-distinct_id-per-day?** — no such limit encoded in repo; but
    Mixpanel raw export honors a 730-day retention window (test comments at
    int.optional.test.js:93-101) — migrations of older data must come from cloud-storage backups
    (where `vendor: 'mixpanel'`'s schema actually matches).
15. CLAUDE.md says groups batch-cap is 200, but job.js:295-297 caps event/user/group all at 2000 —
    if the migration emits `group` records anywhere, verify the real API cap independently.

## Open Questions

- Where should translation state (the identity graph) live for the single-pass case — a bounded
  in-memory Map on `job` (like `hashTable`), or mandatory two-pass with `dimensionMaps`? The
  streaming pipeline has no lookahead, and `$merge` events can arrive after the events they affect.
- Should the new stage be gated by a new top-level option (e.g. `identityTranslation: true` /
  `recordType: 'export-import-event'` + flag), a new recordType (e.g. `migrate-identity`), or a new
  vendor mode (`vendor: 'mixpanel'`, `vendorOpts: {simplified: true}`)? The vendor route collides
  with gotcha #8 (no multi-emit with transformFunc) and the dead `v2_compat` option in
  vendor/mixpanel.js suggests it was intended to grow this way.
- What exactly should `$identify`/`$create_alias`/`$merge` translate to? posthog/heap precedent
  says: a synthetic event (name `identity association` there) carrying `$device_id` (anon side) +
  `$user_id` (identified side); simplified ID merge links on any event carrying both. `$merge`
  (two already-canonical ids) has no single-event equivalent — likely needs a chosen canonical id
  plus rewriting of subsequent events' ids (graph completion), confirmed out of repo scope.
- Does the destination check (`validateToken` → `idmgmt_v3`) belong in the flow, given it sends a
  real event into the destination project?
- `secondProject`/`secondRegion` service-account auth for the destination (gotcha #6) — needed?
- Should translation also rewrite `distinct_id` on ordinary events using
  `$distinct_id_before_identity` (replaying the *original* device-level stream), or trust the
  exported cluster-resolved `distinct_id`? This determines whether the stage needs per-event
  rewriting or only verb translation + graph-completion synthesis.
