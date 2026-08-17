# Research 09 — Mixpanel /import and Raw Export APIs (docs deep-dive)

Slice: what the official docs guarantee about `/import` validation and the raw event export
format, with emphasis on identity fields — the contract surface for an
original-ID-merge → simplified-ID-merge replay pipeline.

Sources (fetched 2026-08-16):

- https://docs.mixpanel.com/reference/import-events
- https://docs.mixpanel.com/reference/raw-event-export
- https://docs.mixpanel.com/docs/tracking-methods/id-management
- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-original
- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-simplified
- https://docs.mixpanel.com/docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system
- https://docs.mixpanel.com/docs/data-structure/property-reference/default-properties
- https://docs.mixpanel.com/docs/data-pipelines/schematized-export-pipeline
- Local primary evidence: `mixpanel-exports/export-2026-07-30-77.ndjson` (real raw-export
  output from a simplified project), `testData/mixpanel/mixpanel-export-format.json`
  (data-pipeline-style export rows).

## Summary

`/import` requires four fields per event (`event`, `properties.time`,
`properties.distinct_id`, `properties.$insert_id`), enforces a 36-byte alphanumeric-or-dash
`$insert_id`, accepts events dated any time ≥ 1971-01-01 (future events > 1 hour ahead are
rewritten to ingestion time), and in strict mode returns a 400 with a per-record
`failed_records` array. The raw export API (`data.mixpanel.com/api/2.0/export`) returns JSONL
with the **resolved/canonical** `distinct_id` (docs for Original ID Merge: "only the canonical
`distinct_id` can be used in queries and exports") plus whatever `$device_id` / `$user_id`
properties were on the event at ingest — this is different from schematized Data Pipelines
exports, which keep the as-ingested `distinct_id` and ship a separate identity-mappings table.
In Original ID Merge, merging is driven by three special events (`$identify` with
`$identified_id`/`$anon_id`, `$create_alias` with `distinct_id`/`alias`, `$merge` with
`$distinct_ids` — `$merge` is /import-only). In Simplified ID Merge those events "will be
ignored ... and will not trigger identity merging"; merging is driven solely by `$device_id` +
`$user_id` reserved properties on any event, `distinct_id` is optional and **overridden** by
Mixpanel (`$user_id` if present, else `$device:`-prefixed `$device_id`). The official
migration doc explicitly blesses the Raw Export API → Import API replay path and the "dummy
event with both `$device_id` and `$user_id`" trick for carrying identity mappings over.

## Key Facts

- `/import` required per event: `event`, `properties.time`, `properties.distinct_id`,
  `properties.$insert_id`.
- Time bounds: rejects "time values that are before 1971-01-01 or more than 1 hour in the
  future"; future times are "overwritten with the current present time at ingestion". No
  documented maximum age beyond the 1971 floor (no 5-year rule on /import; the 5-day limit
  belongs to `/track`).
- `$insert_id`: "must be ≤ 36 bytes and contain only alphanumeric characters or '-'".
- Disallowed values for `distinct_id` (verbatim list):
  `00000000-0000-0000-0000-000000000000`, `anon`, `anonymous`, `nil`, `none`, `null`, `n/a`,
  `na`, `undefined`, `unknown`, `<nil>`, `0`, `-1`, `true`, `false`, `[]`, `{}`.
  Empty-string `distinct_id` is allowed ("If the event is not associated with any user, set
  distinct_id to the empty string") but the event is excluded from behavioral analysis.
- Batch limits: "up to 2000 events and 10MB uncompressed per request"; each event
  "1MB of uncompressed JSON"; "fewer than 255 properties" per event; nested objects
  "fewer than 255 keys and a max nesting depth is 3"; arrays "fewer than 255 elements".
- Rate limit (import): "2GB of uncompressed JSON/minute or ~30k events per second, measured on
  a rolling 1 minute basis"; recommended backoff "starting with a backoff of 2s and doubling
  backoff until 60s" (exponential + jitter).
- Strict mode: query param `strict` enum `['0','1']`, **default '1'** per the reference
  schema; strict 400 response carries `failed_records: [{index, $insert_id, field, message}]`
  plus `code`, `error`, `status`, `num_records_imported`.
- Dedupe rule: "Events with identical values for (event, time, distinct_id, $insert_id) are
  considered duplicates and only one of them will be surfaced in queries." Dedup is applied at
  query time — raw exports can still contain the duplicates.
- Raw export: `GET https://data.mixpanel.com/api/2.0/export` (also `data-eu`, `data-in`);
  params `from_date`, `to_date` (both `yyyy-mm-dd`, both inclusive), `limit` (max 100,000),
  `event` (JSON array of names), `where`, `time_in_ms` (default `false`), `project_id`
  (required with service-account auth). Rate limits: "60 queries per hour, 3 queries per
  second, and a maximum of 100 concurrent queries" → 429 on breach. gzip via
  `Accept-Encoding` for responses > 1400 bytes.
- Raw export returns the resolved identity: original-ID-merge docs — "You can use any of the
  IDs in the cluster for ingestion, but **only the canonical `distinct_id` can be used in
  queries and exports**." Simplified docs — canonical `distinct_id` is always the `$user_id`.
- Original ID Merge event shapes: `$identify` → `properties.$identified_id` +
  `properties.$anon_id`; `$create_alias` → `properties.distinct_id` + `properties.alias`;
  `$merge` → `properties.$distinct_ids` (array of exactly 2) and "can only be processed when
  sent via the /import API directly". Cluster hard cap: 500 IDs.
- Simplified ID Merge: `distinct_id` is optional/overridden — `$user_id` wins, else
  `$device:`+`$device_id`; "distinct_id values prefixed with a `$device:` will be used as
  `$device_id`"; unlimited `$device_id`s per cluster; exactly one `$user_id` per cluster ("It
  is not possible to merge 2 `$user_id`s together"); `$identify`/`$create_alias`/`$merge`
  "will be ignored ... and will not trigger identity merging".

## Details

### 1. /import endpoint (docs.mixpanel.com/reference/import-events)

Endpoint: `POST https://api.mixpanel.com/import` (EU: `api-eu`, IN: `api-in`).

**Query params**

| param | notes |
|---|---|
| `strict` | enum `['0','1']`, default `'1'` in the reference schema. `strict=1`: request returns 400 if any event fails validation; **passing events are still ingested**, failures are itemized. `strict=0` behavior is not explicitly documented (historically: silently drop invalid records). |
| `project_id` | `required: true` (needed for service-account auth). |

**Headers**: `Content-Type: application/json` or `application/x-ndjson`;
`Content-Encoding: gzip` (optional, request-body compression).

**Auth** (two options):
1. Service account (Owner/Admin role): HTTP basic auth `serviceaccount_username:secret`, plus
   `project_id` query param.
2. Project token: basic auth with the token as username and empty password.

(Project API secret also historically works like the token path; docs steer to service
accounts.)

**Required fields per event**

```json
{
  "event": "<name>",
  "properties": {
    "time": 1618716477000,
    "distinct_id": "<user id or empty string>",
    "$insert_id": "<= 36 bytes, [A-Za-z0-9-]"
  }
}
```

- `time`: "seconds or milliseconds since epoch" (integer; unit auto-detected).
- Time validation, verbatim: "We will reject events with time values that are before
  1971-01-01 or more than 1 hour in the future ... If the time value is set in the future, it
  will be overwritten with the current present time at ingestion."
- `distinct_id` verbatim: "distinct_id identifies the user who performed the event.
  distinct_id must be specified on every event ... If the event is not associated with any
  user, set distinct_id to the empty string." Empty-string events are excluded from
  behavioral analysis (funnels/flows/retention). If `distinct_id` is omitted entirely,
  "Mixpanel will use the ip address of the incoming request and compute a distinct_id using a
  hash function" — never let the replay pipeline omit it.
- The disallowed-values list applies to `distinct_id` (and the same value list is documented
  as disallowed for `$insert_id`): `00000000-0000-0000-0000-000000000000`, `anon`,
  `anonymous`, `nil`, `none`, `null`, `n/a`, `na`, `undefined`, `unknown`, `<nil>`, `0`, `-1`,
  `true`, `false`, `[]`, `{}`.
- `$insert_id`: "required on all events"; "must be ≤ 36 bytes and contain only alphanumeric
  characters or '-'". Docs do not state whether an over-long `$insert_id` is truncated vs
  rejected, nor whether a missing one is auto-generated in non-strict mode (schema says
  required → expect strict-mode rejection).

**Size/shape limits (verbatim)**: "up to 2000 events and 10MB uncompressed per request";
per event "1MB of uncompressed JSON"; "Fewer than 255 properties" per event; nested objects
"Fewer than 255 keys and a max nesting depth is 3"; arrays "Fewer than 255 elements".

**GeoIP on /import**: "If you supply a property `ip` with an IP address, Mixpanel will
automatically do a GeoIP lookup and replace the 'ip' property with geographic properties
(City, Country, Region)." So: /import does **no** geo enrichment from the request's source IP;
geo only happens if the event body carries `ip`. Replayed events keep their original
`$city`/`mp_country_code`/`$region` properties as plain properties — do not add `ip` unless
you want re-resolution.

**Dedupe**: "Events with identical values for (event, time, distinct_id, $insert_id) are
considered duplicates and only one of them will be surfaced in queries." This is the safety
net that makes replay idempotent — preserve the exported `$insert_id` verbatim on re-import.
Caveat for the pipeline: the tuple includes `distinct_id`, and simplified projects **rewrite**
`distinct_id` at ingest from `$user_id`/`$device_id`; as long as a retried batch carries the
same identity properties it resolves to the same tuple, so idempotency holds within the new
project.

**Response formats**

Success (200):

```json
{"code": 200, "num_records_imported": 2000, "status": "OK"}
```

Strict-mode validation failure (400):

```json
{
  "code": 400,
  "error": "some data points in the request failed validation",
  "status": "Bad Request",
  "num_records_imported": 999,
  "failed_records": [
    {"index": 0, "$insert_id": "...", "field": "properties.time",
     "message": "properties.time' is invalid: must be specified as seconds since epoch"},
    {"index": 7, "$insert_id": "...", "field": "properties.utm_source",
     "message": "properties.utm_source is invalid: string should be valid utf8"}
  ]
}
```

Documented example `message` strings (the docs do not publish an exhaustive error-code
taxonomy — messages are free-text per `field`):
- `properties.time' is invalid: must be specified as seconds since epoch`
- `properties.utm_source is invalid: string should be valid utf8`

Other status codes: `401` unauthorized, `413` payload too large (split the batch and retry),
`429` rate limited (back off 2s → 60s, exponential + jitter).

**Rate limit (verbatim)**: "2GB of uncompressed JSON/minute or ~30k events per second,
measured on a rolling 1 minute basis."

**Identity events on /import**: the /import reference page itself is silent about
`$identify`/`$merge`/`$create_alias`. The authoritative statements live in the ID-management
docs (below). Key: `$merge` "can only be processed when sent via the /import API directly" —
no SDK emits it, so any `$merge` handling in the replay pipeline is /import-shaped by
definition.

### 2. Original ID Merge — identity event shapes (identifying-users-original)

**`$identify`**

```json
{
  "event": "$identify",
  "properties": {
    "$identified_id": "<user_id>",
    "$anon_id": "<device_id>",
    "token": "YOUR_PROJECT_TOKEN"
  }
}
```

Constraint (verbatim): `$anon_id` must be in "UUIDv4 format" and "not previously merged to
another $identified_id". (Non-UUID `$anon_id` values do not merge via `$identify` — a real
constraint on replay when translating; `$merge` has no UUID requirement.)

**`$create_alias`** — "used to link two non-anonymous Distinct IDs":

```json
{
  "event": "$create_alias",
  "properties": {
    "distinct_id": "<existing id already in the cluster>",
    "alias": "<new id>",
    "token": "YOUR_PROJECT_TOKEN"
  }
}
```

Rules: "Multiple alias ID can point to the same Distinct ID" but "the same alias ID cannot
point to multiple different Distinct IDs". Described as "a legacy function for projects not
using ID Merge" (but original-ID-merge projects still ingest/act on it).

**`$merge`**

```json
{
  "event": "$merge",
  "properties": {
    "$distinct_ids": ["user_id_1", "user_id_2"]
  }
}
```

"can only be processed when sent via the /import API directly"; merge fails if it would
produce a cluster over the cap: merging is allowed only if "merging of 2 IDs does not lead to
an ID cluster that exceeds 500 IDs".

**Canonical ID & timing**: "the canonical `distinct_id` is programmatically selected by
Mixpanel ... based on the most optimal merging process"; "This is not user-configurable";
"You can use any of the IDs in the cluster for ingestion, but only the canonical `distinct_id`
can be used in queries and exports." Merges are retroactive; Activity Feed updates in
"less than 1 minute" but "may take up to 24 hours for this mapping to propagate to all other
parts of the system."

**Cluster cap**: 500 distinct IDs per cluster (hard). Frequent `.reset()` can exhaust it;
once hit, new IDs become orphaned.

### 3. Simplified ID Merge semantics (identifying-users-simplified + id-management)

- Merging is driven by reserved event properties `$device_id` and `$user_id` — no special
  events. "When both properties appear together for the first time, a mapping is created to
  merge the `$user_id` and `$device_id` values together, forming an identity cluster."
- `distinct_id` assignment at ingest (Mixpanel "automatically updates or overrides it"):
  - only `$device_id` → `distinct_id = "$device:<device_id>"` (verbatim rule: "Any ID
    provided as `$device_id` will be prefixed with `$device:` in the ID cluster");
  - only `$user_id` → `distinct_id = <user_id>`;
  - both → `distinct_id = <user_id>` and the two IDs are linked.
- Reverse rule (backward compatibility): "distinct_id values prefixed with a `$device:` will
  be used as `$device_id`" — i.e. an incoming bare `distinct_id` of `$device:X` is treated as
  `$device_id = X`.
- Retroactive: "Mixpanel will retroactively set the `$user_id` on any prior events with the
  user's `$device_id` so that both event streams are joined" — prior events' `distinct_id`
  values are updated retroactively.
- No cluster size limit ("retroactively merge an unlimited number of anonymous IDs
  (`$device_id`) to a user (`$user_id`)"), but "does not support multiple identified IDs
  (i.e. User IDs) per ID cluster" and "It is not possible to merge 2 `$user_id`s together
  using the Simplified API."
- Identity events (verbatim): "You should not send `$identify`, `$create_alias`, and `$merge`
  events since they will be ignored in Simplified ID Merge projects and will not trigger
  identity merging." Also "You should not call `alias`". Docs do not say the events error —
  they are "silently ignored" as merge triggers. (Whether an `$identify` event row is still
  stored as a plain event, and whether `$device_id`/`$user_id` props ON an `$identify` event
  still drive property-based merging, is not stated — see Open Questions; modern SDKs in
  simplified mode do still emit `$identify`-named events carrying both props.)
- Org default: simplified is the default for new orgs since April 2024. "You cannot switch
  between the two APIs if your project already contains data in it" → migration requires "a
  new empty Mixpanel project".

### 4. Raw Event Export API (docs.mixpanel.com/reference/raw-event-export)

Endpoint: `GET https://data.mixpanel.com/api/2.0/export` (EU `data-eu.mixpanel.com`, IN
`data-in.mixpanel.com`).

Auth: project secret via HTTP basic auth (secret as username, blank password), or service
account (then `project_id` is required).

Params:

| param | required | notes |
|---|---|---|
| `from_date` | yes | `yyyy-mm-dd`, inclusive, project timezone |
| `to_date` | yes | `yyyy-mm-dd`, inclusive |
| `limit` | no | "cannot exceed 100,000" events |
| `event` | no | JSON array of event names, e.g. `["signup","purchase"]` — this is how a replay pass could pull only `$identify`/`$create_alias`/`$merge` |
| `where` | no | segmentation-expression filter |
| `time_in_ms` | no | default `false` (seconds); `true` → millisecond timestamps |
| `project_id` | with service account | |

Response: JSONL — "one event per line where each line is a valid JSON object". Docs example
(verbatim):

```json
{"event":"Signed up","properties":{"time":1602611311,"$insert_id":"hpuDqcvpltpCjBsebtxwadtEBDnFAdycabFb","mp_processing_time_ms":1602625711874}}
```

Docs claim (verbatim): "The raw export API allows you to download your event data as it is
received and stored within Mixpanel, complete with all event properties (including
distinct_id)".

Rate limits (verbatim): "60 queries per hour, 3 queries per second, and a maximum of 100
concurrent queries. If you exceed the rate limit, a 429 error will be returned."
Compression: send `Accept-Encoding: gzip`; responses over 1400 bytes come back gzipped.

**Observed real export shape** (local primary evidence,
`mixpanel-exports/export-2026-07-30-77.ndjson`, simplified project 3996669, exported by this
repo's `exportEvents`):

```json
{
  "event": "durtle",
  "properties": {
    "time": 1785110733,
    "distinct_id": "b22d6231-125e-542a-b93b-463bae94c4e7",
    "$device_id": "WTdt8fs6BdoXU6xlq6RMD3Oig2eaRwW5I55xd6UHTd",
    "$import": true,
    "$insert_id": "a8d3831f-ba0e-447e-bdaf-81bc8e9f78be",
    "$mp_api_endpoint": "api.mixpanel.com",
    "$mp_api_timestamp_ms": 1785196817966,
    "$mp_event_size": 403,
    "$user_id": "b22d6231-125e-542a-b93b-463bae94c4e7",
    "mp_processing_time_ms": 1785196818451,
    "...custom props...": "..."
  }
}
```

Notables in the real payload:
- `distinct_id` **equals `$user_id`** — the export reflects post-resolution identity (this
  event was ingested with both `$device_id` and `$user_id`; simplified resolution set
  `distinct_id` to the `$user_id`). Anonymous-only events export with
  `distinct_id = "$device:<id>"`.
- Mixpanel-added hidden/system props present in export: `$import` ("Internal Mixpanel
  property set to `true` to indicate that events were sent through /import API"),
  `$mp_api_endpoint` ("Mixpanel property to record the API endpoint the data was sent to"),
  `$mp_api_timestamp_ms` ("UTC timestamp in milliseconds when the event was received by our
  API"), `$mp_event_size`, `mp_processing_time_ms` ("UTC timestamp in milliseconds when the
  event was processed by Mixpanel servers"). **All of these must be stripped before
  re-import** (this repo's `mpTransforms`/`removeNulls` layer is where that happens today).
- Warehouse-sourced events additionally carry `$source`, `$warehouse_import_id`,
  `$warehouse_import_job_id`, `$warehouse_import_run_id`, `$warehouse_type`
  (seen in `testData/mixpanel/mixpanel-export-format.json`, which is a *pipeline-style*
  flattened row: top-level `device_id`/`distinct_id`/`event_name`/`insert_id`/`user_id`
  alongside `properties` — that flat shape is NOT what `/api/2.0/export` returns).

### 5. Identity resolution in exports — the critical distinction

Two export families behave differently:

1. **Raw Export API (`/api/2.0/export`)** — serves data through the query layer:
   original-ID-merge docs state "only the canonical `distinct_id` can be used in queries and
   exports". So the exported `distinct_id` on every event is the **cluster-canonical** ID as
   of export time, regardless of what was originally sent. Confirmed empirically in the local
   simplified-project export (`distinct_id` rewritten to `$user_id`). Consequence for the
   replay pipeline: you cannot recover the *original* per-event `distinct_id` from raw export
   alone; you can recover the anonymous/identified split only via `$device_id` / `$user_id`
   properties (present whenever the source SDK/API call supplied them) and via the exported
   identity events themselves ($identify's `$identified_id`/`$anon_id`, $create_alias's
   `distinct_id`/`alias`).
2. **Schematized Data Pipelines** — the opposite: "Pipelines export event data as they appear
   when Mixpanel ingests them. This means exported event data before sending alias event has
   the original user identifier, **not** the resolved identifier." Pipelines ship a separate
   **identity mappings table** ("Mixpanel automatically exports the ID mapping table when you
   create a people export pipeline from a project with ID merge enabled"), with guidance:
   "use the **resolved** `distinct_id` in place of the non-resolved `distinct_id` whenever
   present. If there is no resolved `distinct_id`, you can then use the `distinct_id` from
   the existing people or events table." Also, in pipeline exports "$"-prefixed properties are
   renamed with an `mp_` prefix.

Hidden identity properties (community-documented, NOT in the official property reference):
- `$distinct_id_before_identity` — "internal Mixpanel property used to track an event's
  original `$distinct_id` before it was updated due to identity merging"; appears on events
  whose `distinct_id` was rewritten by a merge (both Original and Simplified systems). Not in
  the docs' default-properties page; treat as best-effort, verify empirically on a real
  original-project export before depending on it.
- `$mp_original_distinct_id` — referenced in ecosystem tooling as the pipeline-renamed
  (`mp_`-prefixed) original-distinct-id column; not confirmed in the raw export API payload.
- `$failure_reason` / `$failure_description` — community-referenced props explaining why a
  merge did not happen (e.g. cluster cap hit). Unverified in official docs.

The official default-properties page confirms only: `$device_id` ("Autogenerated ID that is
local to the device. Calling reset() regenerates this value."), `$user_id` ("The identified ID
of the user. Calling identify() sets this."), `$insert_id`, `$import`, `$mp_api_endpoint`,
`$mp_api_timestamp_ms`, `mp_processing_time_ms`, `mp_country_code`.

### 6. Official migration guidance (migrating-to-simplified-id-merge-system)

- Simplified cannot be enabled on a project with data: "To adopt Simplified ID Merge, you
  would need to set up a new empty Mixpanel project."
- Server-side guidance: "Update your Import API payload to include `$device_id` and
  `$user_id` properties in the events." Do not send identity events (ignored).
- Backfill section (near-verbatim): backfilling is optional; client SDKs "use the /track API
  endpoint which accepts events up to 5 days old" (hence /import for backfill); "To prevent
  data duplication caused by backfilling, ensure that each imported event includes a
  `$insert_id`"; three blessed paths — "(1) Mixpanel APIs — if Mixpanel is your single source
  of truth, export data from the existing project using Raw Export API and then import it
  into the new project via Import API. (2) Mixpanel Warehouse Connector ... (3) Customer Data
  Platform (CDP) — replay the historical data".
- The identity-mapping trick (verbatim): "If your historical events do not include both
  `$device_id` and `$user_id` that are required in Simplified ID Merge for identity merging,
  check if you can retrieve this ID mapping information from your system through other means.
  **Instrument a dummy event that includes both `$device_id` and `$user_id` based on your ID
  mappings and send that to the new project to enable identity merging.**"
  → This is the doc-sanctioned pattern the replay pipeline should emit for each translated
  `$identify`/`$create_alias`/`$merge`: one synthetic event carrying the pair.
- Historical client-SDK data: "the SDK should have already populated `$device_id` and
  `$user_id` on your events ... These historical events can be directly imported" (true for
  post-ID-merge-era client SDK data; server-side /import data often has bare `distinct_id`
  only).
- Hard limitation: multiple user IDs per person ("alias chains", `$merge` of two identified
  users) has no simplified equivalent — "this functionality is not supported in Simplified ID
  Merge". A cluster gets exactly one `$user_id`; the pipeline must pick a canonical user ID
  per cluster and remap all other identified IDs to it.
- SDK minimums for simplified mode: JavaScript ≥ 2.46.0, Android ≥ 7.3.0, iOS ObjC ≥ 5.0.2,
  Swift ≥ 4.0.5, React Native ≥ 2.2.0, Flutter ≥ 2.1.0; "Mixpanel Unity SDK currently does
  not support Simplified ID Merge."

### 7. Repo touchpoints (for the implementing agent)

- `components/exporters.js` `exportEvents()` sends `from_date: job.start`,
  `to_date: job.end`, spreads `...job.params` into `searchParams` (so `event`, `time_in_ms`
  and friends can already be passed via `params` without code changes), plus `limit` from
  `job.limit`, `where` from `job.whereClause`, and `project_id` only when
  `job.project && job.acct && job.pass`. Retries handled manually (`retry: {limit: 0}`) for
  429s. Same param set reused around line 1139.
- `mixpanel-exports/export-2026-07-30-77.ndjson` is a live fixture of exact raw-export output.
- `testData/gankster/gankster-events-oct-10.ndjson` contains `$identify` events but in
  **PostHog** export format (`$anon_distinct_id`, `person_id`, `uuid`, `$geoip_*`) — do not
  mistake it for Mixpanel raw-export shape.

## Gotchas / Limits

- **Raw export rewrites `distinct_id` to the canonical cluster ID** — the per-event original
  ID is unrecoverable from `/api/2.0/export` unless it survives in `$device_id`/`$user_id`
  props or the hidden `$distinct_id_before_identity`. Plan identity reconstruction around the
  exported `$identify`/`$create_alias`/`$merge` events + `$device_id`/`$user_id` props, not
  around `distinct_id`.
- Exported events include Mixpanel-added props (`$import`, `$mp_api_endpoint`,
  `$mp_api_timestamp_ms`, `$mp_event_size`, `mp_processing_time_ms`) that must be stripped or
  they become garbage literal properties in the target project.
- `$insert_id` from export is the original one — keep it (idempotent replay), but validate
  against the 36-byte / `[A-Za-z0-9-]` rule: exports can contain `$insert_id` values that
  predate current validation (the docs example itself shows a 36-char alpha string).
- `$identify`'s `$anon_id` must be UUIDv4 in Original projects; irrelevant after translation
  (simplified `$device_id` has no format constraint), but explains why some original-project
  `$identify` events may have silently failed to merge historically.
- `$merge` works only via /import; only 2 IDs per `$merge`; original cluster cap is 500 IDs —
  expect orphaned IDs in the source project (events whose merge was refused).
- Simplified: identity events are silently ignored — a replay that forwards
  `$identify`/`$create_alias`/`$merge` untranslated loses all merges with **no error
  signal** (this is exactly the bug the sprint fixes). Strict mode will not catch it.
- Simplified allows only ONE `$user_id` per cluster; alias-chains/user-to-user merges from
  the original project must be collapsed to a single canonical `$user_id` with all other
  identified IDs demoted (e.g. sent as `$device_id`s) or dropped.
- `distinct_id` values already prefixed `$device:` are re-interpreted as `$device_id` on
  ingest — beware double-prefixing (`$device:$device:X`) when replaying a simplified export.
- Future-dated events (> 1 hour ahead) are not rejected — they're silently rewritten to "now",
  which can scramble replayed timelines; pre-filter instead of trusting the server.
- `time_in_ms=true` export + naive re-import is safe (import auto-detects s vs ms), but mixed
  units within one dataset make the dedupe tuple `(event, time, distinct_id, $insert_id)`
  mismatch across passes — pick one unit for the whole replay.
- Import dedupe only "surfaces one in queries" — duplicates still exist in raw storage and
  re-export; don't use export row counts to verify replay counts.
- Export rate limits (60 queries/hour) bite on day-sliced exports of long histories; the
  100,000-event `limit` param is a cap, not pagination — there is no cursor on this API
  (slice by date, or by `where`/`event`).
- 413 responses require client-side batch splitting; this repo's smart batcher already
  batches by 2000 records / ~9.8MB, just under the documented 2000 / 10MB caps.
- Docs do not publish a complete strict-mode error taxonomy — only per-`field` free-text
  `message`s; do not build parsing logic on exact message strings.

## Open Questions

1. In a Simplified project, is an `$identify`-named event carrying `$device_id` + `$user_id`
   props (a) stored as a normal event and (b) still merge-triggering via its props? (Modern
   SDKs in simplified mode do emit these, which suggests yes; the migration doc's "ignored"
   likely refers to the special-event semantics of old-format payloads.) Needs an empirical
   test against a simplified project before choosing the translated event name.
2. Does raw export from an **original**-ID-merge project include
   `$distinct_id_before_identity` (and/or `$device_id`/`$user_id`) on merged events in
   practice? Community says yes for `$distinct_id_before_identity`, official docs are silent.
   Test with a real original-project export.
3. Are `$merge` events returned by `/api/2.0/export` at all (they are UI-hidden)? `$identify`
   and `$create_alias` are queryable/exportable; `$merge` visibility unconfirmed.
4. Exact behavior of `strict=0` for invalid records (silent drop vs partial error) — not
   documented on the current reference page.
5. Is an over-36-byte `$insert_id` rejected or truncated? Docs state only the constraint.
   Matters for replaying old exports with legacy insert_ids.
6. Whether `/import` into a simplified project returns any validation feedback for
   `$merge`/`$create_alias` payloads (silently ignored vs `failed_records` entries) — needs a
   live probe; determines whether the pipeline can detect accidental passthrough.
7. Propagation timing in simplified projects (original docs say "up to 24 hours" for mapping
   propagation) — does the same lag apply to simplified retroactive merges during a
   high-volume backfill, and does import-order (identity event before/after regular events)
   matter for final cluster shape? Docs imply order-independence (retroactive), unverified at
   backfill scale.
