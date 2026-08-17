# 10 — Tooling: Power Tools API + smarterchild inventory

Research slice for the original→simplified ID-merge replay sprint.
Sources: live GET-docs pulls from the Power Tools API (2026-08-16) and a read of
`/Users/ak/code/smarterchild` (CLAUDE.md, `scripts/`, `api/`, `agent/`, `.claude/skills/powertools/`).
No secrets in this file — credential references are env-var **names** only.

## Summary

The Mixpanel Power Tools API (`https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app`) is a
self-documenting proxy over Mixpanel's query/CRUD/macro APIs: **GET any endpoint returns its docs
(no auth); POST executes (Bearer OAuth or Basic service-account auth)**. Its `/query` surface gives
us everything the probe/validate phases need: unique-user counts (`getSegmentation type:"unique"`,
or `runQuery` insights with `math:"unique"` for `$all_events`), arbitrary JQL (`runJQL` — the
best tool for inspecting identity clusters), per-user resolved event timelines
(`getActivityStream`), profile/distinct_id enumeration (`getProfiles`), and property-value
inspection (`getPropertyValues`, `getPropertyDistribution`). Auth for this sprint is
`AK_BEARER_TOKEN` in `mixpanel-import/.env` (confirmed present; value never goes in files).
The smarterchild repo contributes a battle-tested skill doc (`.claude/skills/powertools/SKILL.md`)
with verified curl patterns, a known `getSavedInsight` 401 bug + direct-API workaround, and one
direct-engage-query code pattern (`api/integrations/whoOwns.js`); its own `mixpanel.js` helpers are
track-only observability, not query tooling. The sprint's rough spec also lives in smarterchild:
`research/original-simplified-research.md` (§6 = the identity-replay proposal).

## Key Facts

- Base URL: `https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app` (Cloud Run, us-central1).
- Convention: **GET = docs (no auth), POST = execute (auth required)**. JSON bodies.
- Auth headers: `Authorization: Bearer <oauth_token>` (from `https://mixpanel.com/oauth/access_token`)
  or `Authorization: Basic base64(service_acct:service_secret)`. For this sprint: Bearer via
  `AK_BEARER_TOKEN` in `/Users/ak/code/mixpanel-import/.env`.
- Required on every POST: `project_id` and `client_id` (freeform string — use something like
  `"orig-to-simplified-sprint"`). Optional everywhere: `region` (`US`|`EU`|`IN`, default US).
- Rate limits (Mixpanel query API, enforced through the proxy): **5 concurrent, 60/hour**.
- Server-side caching: **1 hour** default TTL, **15 min** for JQL/runQuery, 5 min for 400 errors.
  Cached responses carry `cache_hit: true`. 429s pass through uncached.
- Four+ surfaces: `/query` (18 methods), `/crud` (~150 endpoints), `/macro` (76 endpoints via
  `/list`), `/agents` (WebSocket AI agents), plus `/ingestion`, `/export`, `/deletion`, `/auth`.
- `/query/getSegmentation` `type:"unique"` = unique-user counts per event; **does NOT support
  `$all_events`** — use `/query/runQuery` (insights bookmark payload, `measurement.math:"unique"`)
  for project-wide uniques.
- `/query/runJQL` executes arbitrary JQL (`script` must contain `function main()`, optional
  `params` object) — the tool for cluster-shape inspection.
- `/query/getActivityStream` takes `distinct_ids` (single or array) → resolved event timeline per
  user; querying by ANY cluster-member id should return the canonical merged stream.
- `/auth` (POST with `project_id`) verifies creds + returns `org_id`, `workspace_id`,
  `has_workspaces`; `include: "token"` additionally returns the project token.
- Sprint test projects (from PROJECT.md): ORIGINAL **4054680**, SIMPLIFIED **4054681**;
  `.env` keys `ORIGINAL_{PROJECT,SECRET,TOKEN}`, `SIMPLIFIED_{PROJECT,SECRET,TOKEN}` all present.
- smarterchild's verified-working reference: `/Users/ak/code/smarterchild/.claude/skills/powertools/SKILL.md`
  (verified 2026-08-05). Known bug: `/query/getSavedInsight` 401s under both auth schemes;
  workaround is direct `https://mixpanel.com/api/query/insights?project_id=&bookmark_id=&workspace_id=`
  with SA Basic auth.
- The rough spec for this sprint: `/Users/ak/code/smarterchild/research/original-simplified-research.md`
  (484 lines; §6.4 = proposed option shape). Related: `research/vipps-migration-postmortem.md`.

## Details

### 1. Power Tools API — service shape

Root GET (`/`) enumerates surfaces:

| Surface | Endpoint | What it is |
|---|---|---|
| Macros | `/` + `/list` | 76 automation endpoints (clone, AI cleanup, bulk ops, generators) |
| Query | `/query` | Direct Mixpanel query methods with caching (the one we care about) |
| CRUD | `/crud` | ~150 direct entity operations (dashboards, cohorts, projects, service accounts, …) |
| Ingestion | `/ingestion` | Data-in via the **mixpanel-import SDK** (this repo!) |
| Export | `/export` | Data-out via mixpanel-import exporters + raw export |
| Deletion | `/deletion` | Lexicon soft-delete + GDPR hard-delete |
| Agents | `/agents` | Conversational AI agents over WebSocket |
| Auth | `/auth`, `/auth/projects` | Credential verification and project enumeration |
| Reference | `/reference/urls` | Canonical Mixpanel URL builders (GET, no auth) |

Global optional params on everything: `user_id` (observability distinct_id), `client_id` (app id).
AI endpoints (`/macro/ai-*`, `/macro/mixpanel-md`) are restricted to Mixpanel employees
(Bearer + @mixpanel.com) or service accounts — AK qualifies. Everything else accepts any valid
Mixpanel credentials and acts with the caller's own permissions.

### 2. `/query` surface — full method inventory

All POST. Required everywhere: `project_id`, `client_id`. Optional everywhere: `region`.
Cache TTL 1h unless noted.

#### Count / segmentation methods (the probe workhorses)

**`/query/getSegmentation`** — segmentation analysis on events.
- Required: `event` (string or array), `from_date`, `to_date` (YYYY-MM-DD).
- Optional: `unit` (day|week|month), **`type` (general|unique|average)** — `unique` gives
  unique-user counts, i.e. post-identity-resolution distinct counts; `on` (segment property, e.g.
  `properties.dataVersion` — exactly what we need for versioned datasets); `where` (filter expr,
  e.g. `properties.dataVersion == 3`); `limit`.
- Response: `data.series` keyed by date (nested by segment value when `on` given), `data.values`.
- **Limitation:** does NOT accept `$all_events`. For all-events uniques use `runQuery`.

**`/query/runQuery`** — arbitrary bookmark/report payload (same JSON as saved reports).
- Required: `bookmark` (payload object). Smart wrapping: pass either
  `{bookmark: {sections: …}}` or the raw `{sections: …}` — both work.
- Optional: `type` (insights|funnels|retention|flows|arb_funnels; default insights),
  `format` (`csv` for CSV).
- Canonical "how many uniques in this project" payload (from the live docs):

```json
{
  "project_id": "4054680",
  "client_id": "orig-to-simplified-sprint",
  "type": "insights",
  "bookmark": {
    "sections": {
      "show": [
        { "name": "# USERS",
          "behavior": { "type": "simple", "resourceType": "events",
            "behaviors": [ { "type": "event", "name": "$all_events", "filters": [] } ],
            "dataset": null },
          "measurement": { "math": "unique" }, "type": "metric" }
      ],
      "time": [ { "dateRangeType": "in the last", "window": { "value": 30, "unit": "day" }, "unit": "day" } ]
    }
  }
}
```

  Add a second `show` entry with `"math": "total"` for event totals in the same call. Filters on
  `dataVersion` go in `behaviors[].filters`. Cache TTL 15 min.
- This is the **A/B comparison instrument** for Phase 7: run identical payloads against 4054680
  and 4054681 and diff unique counts (replay success = uniques match, or simplified < original when
  replay merges clusters original couldn't).

**`/query/getEventFrequency`** — simple counts over time. Required: `event`, `from_date`,
`to_date`; optional `unit`. Internally uses segmentation.

**`/query/getTopEvents`** — events by volume. Optional: `limit` (default 100), `type`
(general|hidden|dropped).

**`/query/getEventNames`** — all event names. Optional: `type`, `limit` (default 1000).

#### Identity-cluster inspection methods

**`/query/runJQL`** — arbitrary JQL. Required: `script` (must contain `function main()`);
optional `params` (object passed into the script). Cache TTL 15 min.
- JQL `Events({from_date, to_date})` yields events with **resolved** `distinct_id` — grouping by
  `distinct_id` and collecting seen id-ish properties reveals cluster shape. Sketch for cluster
  inspection (count distinct pre-merge ids per resolved user):

```javascript
function main() {
  return Events({ from_date: params.from, to_date: params.to })
    .groupByUser(function(acc, events) {
      acc = acc || { count: 0, verbs: {} };
      for (const e of events) {
        acc.count++;
        if (e.name === '$identify' || e.name === '$create_alias' || e.name === '$merge') {
          acc.verbs[e.name] = (acc.verbs[e.name] || 0) + 1;
        }
      }
      return acc;
    });
}
```

  `groupByUser` keys by the resolved user — cluster membership is visible because all member ids
  collapse to one key. `join(Events(), People())` is also available.
- Sibling: **`/macro/ai-jql`** (natural language → generated JQL → executed) and
  **`/query/text-to-jql-query`** — useful for iterating on cluster-inspection scripts quickly.

**`/query/getActivityStream`** — per-user resolved timeline. Required: `distinct_ids` (single id
or array). Optional: `from_date`, `to_date`, `limit` (default 100/user), `event` (name filter).
Response: `results[]` of `{distinct_id, events[]}`.
- **Cluster probe:** query the SAME canonical stream by each member id (device uuid, alias, user
  id) — if resolution worked, every member id returns the merged history. This is the most direct
  "is this id in the cluster?" check the API offers.

**`/query/getProfiles`** — paginated profile query. Optional: `where` (engage expr, e.g.
`properties.$email defined`), `distinct_id`, `distinct_ids` (array), `data_group_id`,
`filter_by_cohort`, `output_properties` (array; shrinks payload), `limit`, and manual pagination
(`session_id` + `page`, `page_size` max 1000, `include_all_users`). Response includes `total`,
`session_id`, `page`.
- Profile count = number of resolved users with profiles; `distinct_ids` lookup tells you which
  canonical id a member id resolves to on the profile store side.

**`/query/streamProfiles`** — same but collects ALL matches in memory (`batch_size` default 1000).
Docs warn >10MB responses; prefer `getProfiles` pagination.

#### Property inspection methods

**`/query/getPropertyValues`** — unique values of a property. Required: `property_name`; optional
`event_name`, `limit` (default 100). Good for verifying `dataVersion` values present, or sampling
`$user_id` / `$device_id` values post-import.

**`/query/getPropertyDistribution`** — property value distribution over time. Required: `event`,
`property`, `from_date`, `to_date`; optional `limit`.

**`/query/getNumericPropertyBuckets`** — numeric bucketing. Required: `event`, `property`
(e.g. `properties.amount`), `from_date`, `to_date`; optional `buckets` (default 10), `type`
(general|unique|average), `unit`, `where`.

**`/query/getTopProperties`** — properties ranked by co-occurrence % with an event. Required:
`event_name`; optional `limit` (default 10).

**`/query/schema`** — events + event properties + user properties with ids (`include_metadata:
true` adds hidden/dropped/displayName/description/tags). First call to understand a project.

#### Report-style methods (less central, available)

- **`getFunnel`** — requires pre-configured `funnel_id` (+ `from_date`, `to_date`; optional
  `interval` conversion window, `on`, `where`, `unit`, `limit`). Use `listFunnels` to discover.
- **`getRetention`** — `birth_event`, `return_event`, `from_date`, `to_date`; optional
  `retention_type` (birth|rolling), `unit`, `interval_count` (default 7), `born_where`, `where`,
  `on`, `limit`.
- **`getFlows`** — optional `events` array, `exclude_event`, dates, `limit`.
- **`listBookmarks`** (optional `workspace_id`, `type`), **`getSavedInsight`** (`bookmark_id`) —
  ⚠️ known 401 bug, see Gotchas. **`listFunnels`**, **`listCohorts`**, **`exportCohort`**.
- **`text-to-mixpanel-query`** — NL → validated bookmark → executed. Required `user_prompt`;
  optional `report_type`, `model` (default gemini-2.5-flash), `schema_json` (pass a pre-fetched
  `/query/schema` result to skip the fetch), `enable_web_search`. **`text-to-jql-query`** same for JQL.

### 3. Other Power Tools surfaces relevant to the sprint

**`/auth`** (GET docs / POST execute) — first call in any probe script:
- POST `{}` → validates creds, returns `user.email`, `auth_type`, `ai_endpoints_allowed`.
- POST `{project_id}` → adds `project: {id, name, org_id, workspace_id, has_workspaces, accessible}`.
- POST `{project_id, include: "token"}` → also returns the project token (sensitive).
- 401 = bad creds; 403 = valid creds but AI-endpoint policy; 429 = wait 15 min.
- **`/auth/projects`** enumerates every project the creds can touch, across orgs.

**`/export`** — three methods (POST, needs `service_acct`/`service_secret` or `access_token` in
body or Authorization header):
- `exportData` — full mixpanel-import exporter: `options.recordType` = `export` |
  `profile-export` | `group-export`; supports `start`/`end`, `where`, `limit`, `cohortId`,
  `dataGroupId`, destination (`local`, `gs://`, `s3://`) or in-memory (`skipWriteToDisk`).
- `exportEvents` — raw export wrapper (`from_date`, `to_date`; optional `events`, `where`,
  `limit`) → parsed event array. Raw export from an ID-merge project is where per-event
  pre-resolution ids surface (`$device_id`, `$user_id`, and — per Mixpanel raw-export behavior —
  `$distinct_id_before_identity`); grouping raw events by resolved `distinct_id` and collecting
  distinct pre-resolution ids reconstructs cluster membership. (Verify field presence on the test
  projects; see Open Questions.)
- `exportProfiles` — engage export (`where` or `cohort_id`).
- For the probe phase we'll likely run mixpanel-import locally instead, but these give a
  remote-callable equivalent.

**`/ingestion`** — `importEvents` / `importUsers` / `importGroups` / `importLookupTable` /
`importData`, all thin wrappers over the mixpanel-import SDK (`data` = array | file/cloud URL |
NDJSON path; `options` = full mixpanel-import config incl. `vendor`, `transformFunc`, `dryRun`).
Once identity-replay ships in this repo and gets deployed there, it becomes remotely invokable
for free.

**`/crud` highlights** (~150 endpoints; full list cached in this slice's pulls):
- `getSettings` — project settings incl. data groups + timezone.
- `getStats` — project usage statistics.
- `createProject` / `deleteProject` / `getProjects` — **createProject requires an OAuth token, not
  a service account**; returns `{id, token, api_secret, workspace_id}`. Useful if we need fresh
  throwaway simplified projects (events can't be deleted, so new rounds may want new projects —
  though PROJECT.md's dataVersion convention is the primary strategy).
- `mintServiceAccount` / `rotateServiceAccount` / `listServiceAccounts`.
- `addGroupKey` / `listGroupKeys`.
- Lexicon ops (`setSchema`, `dropEvents`, `mergeEvents`, …) — schema cleanup post-import if wanted.

**`/list` (macros) highlights**: `/macro/analyze-project` (event volumes, property cardinality,
user activity — a one-shot project fingerprint), `/macro/bulk-delete-profiles` (enumerate +
delete user/group profiles — profiles CAN be reset between rounds even though events can't),
`/macro/clone-project`, `/macro/get-schema`, `/macro/ai-query`, `/macro/ai-jql`.
Also `/macro/dungeon-master` — runs a `@ak--47/dungeon-master` synthetic-data config and ingests
it; potentially relevant to Phase 3 (dataset generator) as prior art for synthetic identity data.

### 4. Auth wiring for this sprint

- `/Users/ak/code/mixpanel-import/.env` keys (names only, verified 2026-08-16):
  `AK_BEARER_TOKEN`, `ORIGINAL_PROJECT`, `ORIGINAL_SECRET`, `ORIGINAL_TOKEN`,
  `SIMPLIFIED_PROJECT`, `SIMPLIFIED_SECRET`, `SIMPLIFIED_TOKEN`, plus the pre-existing
  `MP_*` test-suite keys and `AWS_USER_NAME`.
- Probe-script pattern (never echo the token):

```bash
source /Users/ak/code/mixpanel-import/.env  # or: TOKEN=$(grep '^AK_BEARER_TOKEN' .env | cut -d= -f2)
curl -s -X POST https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app/query/getSegmentation \
  -H "Authorization: Bearer $AK_BEARER_TOKEN" -H 'Content-Type: application/json' \
  -d '{"project_id": 4054680, "client_id": "orig-to-simplified-sprint",
       "event": "some event", "from_date": "2026-08-01", "to_date": "2026-08-16",
       "type": "unique", "where": "properties.dataVersion == 1"}'
```

- OAuth tokens come from `https://mixpanel.com/oauth/access_token`; if `AK_BEARER_TOKEN` ever
  expires (401 from `/auth`), that's where a fresh one is minted.
- Basic-auth alternative: `base64(ORIGINAL_SECRET-style service account)` — but note the sprint's
  `ORIGINAL_SECRET`/`SIMPLIFIED_SECRET` are project API secrets for mixpanel-import ingest/export,
  not necessarily service-account pairs; the Bearer token is the intended Power Tools credential.

### 5. smarterchild inventory (`/Users/ak/code/smarterchild`)

Monorepo of 5 independently deployed Cloud Run services (GCP project `mixpanel-gtm-training`,
us-central1): `agent/` (AI Slack bot, ESM), `api/` (Salesforce directive dispatcher, CJS),
`automation/` (cron VIP flows, ESM), `salesforce-webhooks/`, `slack-webhooks/`. Root
`scripts/prod-logs.mjs` is a multi-service Cloud Run log fetcher (ADC auth via
`gcloud auth application-default login`).

**Directly reusable for this sprint:**

1. **`.claude/skills/powertools/SKILL.md`** — the crown jewel. Verified-working (2026-08-05)
   curl patterns against Power Tools, the two-auth-scheme table, cred-loading one-liners
   (grep from `.env`, never paste values), and gotchas that live docs don't state:
   - `/query/getSavedInsight` returns **401 under BOTH auth schemes** (proxy workspace-handling
     bug). Workaround: hit `https://mixpanel.com/api/query/insights?project_id=…&bookmark_id=…&workspace_id=…`
     directly with SA Basic auth (`curl -u "$SA:$SECRET"`).
   - `getDash` takes `dash_id`, NOT `dashboard_id` (wrong name → "Missing required parameters").
   - Report params are embedded in the board payload at `contents.report[<id>].params`.
   - Short links `mixpanel.com/s/XXXX` resolve to bookmark ids via `curl -sI` → Location header.
   - Its cred env-var names (smarterchild-specific): root `.env` → `inframetrics_access_token`;
     `api/.env` → `MIXPANEL_INFRA_SA` + `MIXPANEL_INFRA_SECRET`.

2. **`api/integrations/whoOwns.js:91` — `getInfraMetrics()`** — the one direct Mixpanel query-API
   call in the codebase: POST `https://mixpanel.com/api/2.0/engage?project_id=1297132&workspace_id=9017`
   with `Content-Type: application/x-www-form-urlencoded`, body `distinct_id=<id>`, and
   `Authorization: Basic base64(SA:SECRET)` from env vars. Pattern to copy when the Power Tools
   proxy is in the way (rate limits, caching, or the getSavedInsight bug): mixpanel.com query APIs
   accept SA Basic auth directly, engage lookups by distinct_id included.

3. **`agent/scripts/query.mjs`** — generic probe-CLI pattern worth cloning for our probe phase:
   `node scripts/query.mjs <service> <method> [args...]`, dotenv at top, a `serviceMap` of module
   paths, dynamic `import()`, `JSON.parse`-with-fallback on argv, pretty-printed JSON out.
   A `probe.mjs` in this sprint's plans dir could follow the same shape with methods =
   Power Tools wrappers (auth from `AK_BEARER_TOKEN`).

**Present but NOT query tooling:**

- `agent/services/mixpanel.js` and `api/modules/mixpanel.js` (plus 3 more) — **track-only
  observability helpers**: hardcoded ingest token, `track(eventName, distinctId, props)` that
  never throws/rejects, common props, dev runs skipped unless `MP_TRACK_IN_DEV=1`. All 5 services
  emit to observability project 4016349. Nothing here queries Mixpanel; nothing identity-related
  beyond using email as distinct_id.
- `agent/services/*` — Salesforce/BigQuery/Slack/Gong/Notion/Calendar/Drive/ChartHop/Linear/Pylon
  wrappers. Notable convention if we build long-running probes: the 24h in-memory + disk cache
  pattern (`./cache/{service}-cache.json`, reference impl `services/charthop.js`).
- `agent/scratch/` — gitignored one-off probes (`probe-sdk.mjs`, `segment-diff.mjs`, …); the repo
  convention is that scrappy scripts go there. Our equivalent: scratch scripts stay OUT of
  `plans/` version control or go under a gitignored dir.

**Sprint-relevant research documents in smarterchild** (read them in their own slices):

- `research/original-simplified-research.md` (484 lines) — THE rough spec. §2 Dana's code
  inventory, §3 Vipps forensics (who wrote the transforms; `cosmos_migration` = "the only place
  anyone wrote the right shape"), §5 Picklebet precedent, §6 the identity-replay proposal
  (§6.2 the two rules Simplified enforces, §6.4 proposed option shape, §6.5 transform semantics,
  §6.6 pipeline placement).
- `research/vipps-migration-postmortem.md` — full Vipps migration/consolidation post-mortem
  (timeline, both failed attempts, the 2022 identity-architecture decision).
- `research/sql-patterns-mixpanel.md` — SQL-in-Mixpanel test-data patterns (dataset-generation
  prior art).

### 6. Probe recipes for Phases 4 & 7 (assembled from the above)

1. **Sanity**: POST `/auth` with each project_id → confirms Bearer works + captures
   `workspace_id`/`org_id` for any direct-API fallback URLs.
2. **Schema check**: `/query/schema` on both projects → event names present, `dataVersion` prop known.
3. **Unique-user count per dataVersion**: `/query/runQuery` insights payload,
   `math:"unique"` on `$all_events` with a `dataVersion` filter → the headline number to compare
   ORIGINAL (4054680) vs SIMPLIFIED (4054681) post-replay. Same payload, two project_ids.
4. **Per-event uniques**: `/query/getSegmentation` `type:"unique"`, `where:
   "properties.dataVersion == N"` — catches partial-merge discrepancies event by event.
5. **Cluster membership**: `/query/getActivityStream` with `distinct_ids: [deviceUuid, alias,
   userId]` — all members returning the same merged stream = cluster resolved. Members returning
   fragments = resolution failure (expected in ORIGINAL for the over-limit torture cases).
6. **Cluster stats in bulk**: `/query/runJQL` with a `groupByUser` script (see §2) → distribution
   of events-per-resolved-user, identity-verb counts per user; compare distributions across
   projects.
7. **Profile-side check**: `/query/getProfiles` with `distinct_ids` → which canonical id each
   member resolves to in engage.
8. **Fallback when proxy caching/limits bite**: direct `mixpanel.com/api/query/*` (Basic SA auth,
   `whoOwns.js` pattern) or local mixpanel-import export runs.

## Gotchas / Limits

- **Server-side caching WILL mask fresh data during iterative rounds**: 1h TTL on most `/query`
  methods, 15 min on JQL/runQuery. During import→probe loops, either wait, vary the query (the
  `dataVersion` filter naturally busts cache per round — one more reason the versioning convention
  is right), or fall back to direct mixpanel.com APIs.
- **Rate limits are the underlying Mixpanel query-API limits: 5 concurrent, 60/hour.** A probe
  suite that fans out per-cluster `getActivityStream` calls can burn the hourly budget fast —
  batch `distinct_ids` (it accepts arrays) and prefer one JQL over N segmentations.
- **`getSegmentation` does not support `$all_events`** — the docs call this out explicitly; use
  `runQuery` insights payloads for all-events math.
- **`/query/getSavedInsight` is broken (401) under both auth schemes** per smarterchild's verified
  skill doc; direct `mixpanel.com/api/query/insights` with SA Basic auth is the workaround.
- **AI endpoints (`/macro/ai-*`, `/macro/mixpanel-md`) are employee/SA-restricted**; check
  `ai_endpoints_allowed` in the `/auth` response before relying on `ai-jql`/`ai-query`. The
  `/query/text-to-*` endpoints are not listed under that restriction but do invoke AI models.
- **`createProject` requires an OAuth token, not a service account.**
- **`client_id` is required on every POST but freeform** — forgetting it is the most common 400.
  Param-name traps exist across `/crud` (`dash_id` not `dashboard_id`).
- **`streamProfiles` collects everything in memory** server-side; use `getProfiles` pagination
  (`page_size` ≤ 1000) for big profile sets.
- 400-error responses are cached 5 min — a malformed query keeps failing from cache even after
  you fix nothing; changing any param busts it.
- smarterchild `.env` files double as production config (deployed with source) — do not treat that
  repo's env conventions as a license to commit env files here; this repo's rule stays "no secrets
  ever" in `plans/`.
- The smarterchild `mixpanel.js` helpers are ingest/track-only — nothing in that repo's runtime
  code queries identity clusters; the query knowledge lives in the skill doc + `whoOwns.js`.

## Open Questions

1. Does raw event export from the test projects include `$distinct_id_before_identity` (and
   per-event `$device_id`/`$user_id`) as expected for ID-merge projects? Needs one live
   `exportEvents`/local mixpanel-import export against 4054680 to confirm the exact field names —
   this determines how the replay transform reads pre-resolution ids.
2. In JQL on an original-ID-merge project, is `distinct_id` in `Events()` fully resolved (cluster
   canonical) at query time, and does resolution reflect merges within the up-to-24h merge delay?
   Affects how soon after import Phase 4 probes are trustworthy.
3. Is `AK_BEARER_TOKEN` long-lived or does it expire? (Mixpanel OAuth access tokens historically
   expire; the skill doc treats `inframetrics_access_token` as a static .env value.) First 401
   from `/auth` answers this; re-mint at `https://mixpanel.com/oauth/access_token`.
4. Do the sprint's `ORIGINAL_SECRET`/`SIMPLIFIED_SECRET` double as service-account credentials for
   Basic auth against Power Tools / direct query APIs, or are they project API secrets only?
   Determines whether the direct-API fallback path needs separate SA minting
   (`/crud/mintServiceAccount` can create one if needed).
5. Can `/query/runQuery` funnels/retention payloads be used to observe merge behavior mid-cluster
   (e.g. conversion across a device→user boundary)? Untested; insights uniques may be sufficient.
6. `/macro/bulk-delete-profiles` could reset profile state between rounds even though events are
   immutable — worth confirming it works on these ephemeral projects if profile-side assertions
   get polluted across dataVersions.
