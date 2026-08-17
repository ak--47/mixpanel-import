# Research 07: Mixpanel Identity API references ($identify / $create_alias / $merge)

Sources fetched 2026-08-16:

- https://docs.mixpanel.com/reference/create-identity
- https://docs.mixpanel.com/reference/identity-create-alias
- https://docs.mixpanel.com/reference/identity-merge
- https://docs.mixpanel.com/docs/tracking-methods/id-management
- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-original
- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-simplified
- https://docs.mixpanel.com/docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system
- https://docs.mixpanel.com/reference/raw-event-export
- Repo corroboration: `vendor/posthog.js`, `vendor/mixpanel.js`

## Summary

Original ID Merge projects express identity graph edges as three special **events**: `$identify`
(anon → identified, sent to `/track`), `$create_alias` (id → alias, sent to `/track`), and `$merge`
(cluster ⇄ cluster, sent only to the authenticated `/import` endpoint). All three are explicitly
documented as having **no functionality in Simplified ID Merge projects** — a simplified project
ingests identity via two reserved event properties instead: `$device_id` and `$user_id`, where a
single event carrying both triggers the merge. Mixpanel's own migration guide confirms there is no
in-place conversion path; the sanctioned procedure is exactly what this sprint is building: raw-export
from the old project, transform, `/import` into a new simplified project. The official docs do **not**
document how identity events appear in raw export output — that must be verified empirically (see
Open Questions), though community sources confirm exported events carry
`$distinct_id_before_identity` preserving the pre-merge distinct_id.

## Key Facts

- `$identify` → POST `/track` (also accepted by `/import`). Required props: `$identified_id`,
  `$anon_id`, `token`. `$anon_id` **must be UUIDv4** and not previously merged.
- `$create_alias` → POST `/track` (also accepted by `/import`). Required props: `distinct_id`,
  `alias`, `token`. "Each alias can only map to one distinct_id." Aliases can be daisy-chained.
- `$merge` → POST `/import` **only** (needs service account / project secret / OAuth). Required:
  `properties.$distinct_ids` — array of **exactly 2** strings (`minItems: 2, maxItems: 2`) — and
  `properties.token`.
- Original ID Merge cluster cap: **500 IDs per cluster**. Merges are **irreversible**.
- Simplified ID Merge: **exactly one `$user_id` per cluster**, **unlimited `$device_id`s**; cannot
  merge two `$user_id`s; `distinct_id` is auto-derived (`$user_id` if present, else
  `$device:<device_id>`).
- All three identity verbs are no-ops in a Simplified project — replay must translate them into a
  regular event carrying both `$device_id` and `$user_id` ("a single instance of such event is
  adequate to trigger identity merging").
- Raw export: `https://data{-eu|-in}.mixpanel.com/api/2.0/export`, basic auth with project secret,
  `from_date`/`to_date` required, `limit` max 100,000, rate limit 60 queries/hour, 3/sec,
  100 concurrent, 429 on excess.
- Retroactive merge propagation in both systems: "may take up to 24 hours … to be fully reflected
  in all Mixpanel reports."

## Details

### 1. `$identify` — Create Identity

- **Endpoint:** `POST https://api.mixpanel.com/track#create-identity` (the `#create-identity`
  fragment is just the docs anchor; the wire endpoint is `/track`). Regional hosts: `api-eu`,
  `api-in`. The docs also say the payload may alternatively be sent to
  `https://api.mixpanel.com/import/`.
- **Content type:** `application/x-www-form-urlencoded`, JSON blob in the `data` parameter
  (standard `/track` envelope).
- **Event name:** exactly `"$identify"`.
- **Properties:**

  | Field | Required | Meaning (doc quotes) |
  |---|---|---|
  | `properties.$identified_id` | yes | "A distinct_id to merge with the $anon_id" |
  | `properties.$anon_id` | yes | "A distinct_id to merge with the $identified_id. The $anon_id must be UUID v4 format and not already merged to an $identified_id" |
  | `properties.token` | yes | project token |
  | `properties.distinct_id` | optional | "The distinct ID post-identification (same as $identified_id - it will be inferred from $identified_id if not included)" |

- **strict:** integer 0/1; when `1`, "Mixpanel will validate the provided records and return a JSON
  object with per-record error messages" (otherwise `/track` returns bare `1`/`0`).
- **Responses:** `200` → plain-text `1` (all objects valid — note: a `1` does *not* confirm the
  token was valid) or `0` (one or more invalid). `401` / `403` → JSON `ErrorResponse`.
- **Auth:** none beyond the in-payload `token` (it is a `/track`-family endpoint).
- **Semantics in Original projects:** this is what SDK `.identify(<user_id>)` emits. "All identify
  calls with a new and valid `$anon_id` will trigger a track `$identify` event, and merge to the
  `$identified_id`." Merge only happens when `$anon_id` is UUIDv4 **and** was not previously merged
  to another `$identified_id` — otherwise the event ingests but creates no graph edge.
- **Doc warning (verbatim):** "The `$identify` event payload is only useful for projects using the
  Original ID Merge system; it has no functionality in other ID management systems."

### 2. `$create_alias`

- **Endpoint:** `POST https://api.mixpanel.com/track` (regional: `api-eu`, `api-in`); same
  form-encoded `data` envelope. Alternative: `/import`.
- **Event name:** exactly `"$create_alias"`.
- **Properties:**

  | Field | Required | Meaning (doc quotes) |
  |---|---|---|
  | `properties.distinct_id` | yes | "A distinct_id to be merged with the alias" (the pre-existing ID) |
  | `properties.alias` | yes | "A new distinct_id to be merged with the original distinct_id. Each alias can only map to one distinct_id." |
  | `properties.token` | yes | project token |

- **strict:** same 0/1 behavior as `$identify`.
- **Responses:** `200` → `1`/`0`; `401`; `403`.
- **Semantics in Original projects:** links two **non-anonymous** distinct IDs (no UUIDv4
  requirement). "Aliases can be daisy-chained; but the same alias ID cannot point to multiple
  different Distinct IDs." SDK `.alias()` emits this; the method is deprecated in Simplified.
- **Doc warning (verbatim):** "The `$create_alias` event payload is only useful for projects using
  the Original ID Merge system and the Legacy ID Management System; it has no functionality in the
  Simplified ID Merge system."
- Note the direction: `alias` is the *new* name, `distinct_id` is the *existing* one. In the Legacy
  system alias was a one-way pointer; in Original ID Merge it becomes a real cluster merge.

### 3. `$merge`

- **Endpoint:** `POST https://api.mixpanel.com/import` (regional: `api-eu`, `api-in`). **Import
  only** — there is no SDK method; it is deliberately gated behind authenticated ingestion.
- **Auth (one of):** Service Account (HTTP Basic), Project Secret (HTTP Basic), OAuth Bearer token.
  With a service account, the `project_id` query param is required.
- **Query params:** `strict` — `'0'` or `'1'`, **default `'1'`**: "When set to 1 (recommended),
  Mixpanel will validate the batch and return errors per event that failed."
- **Body:** JSON array of events (standard `/import` batch):

  ```json
  [
    {
      "event": "$merge",
      "properties": {
        "$distinct_ids": ["id1", "id2"],
        "token": "YOUR_PROJECT_TOKEN"
      }
    }
  ]
  ```

- **`$distinct_ids`:** "The two distinct_ids to merge together" — schema is `minItems: 2,
  maxItems: 2`, items are strings. (The prose page describes it loosely as "array of 2+
  identifiers", but the API schema pins it at exactly two. Treat it as pairwise.)
- **Responses:** `200` → `{"code": 200, "num_records_imported": N, "status": "OK"}`;
  `400` → validation failure with a per-event error array (strict mode); `401` → "Invalid service
  account credentials".
- **Semantics in Original projects:** merges any two ID *clusters* "as long as it does not lead to
  an ID cluster that exceeds 500 IDs." No UUIDv4 restriction, no already-merged restriction — it is
  the unrestricted admin verb. "Merging identities is irreversible"; "You **cannot** unmerge
  `distinct_id`."
- **Doc warning:** only useful in Original ID Merge projects.

### 4. Original ID Merge cluster semantics (what the verbs build)

- Clusters are formed by union of edges from the three verbs. Cap: **500 distinct IDs per
  cluster** — a merge that would exceed 500 is rejected.
- **Canonical distinct_id (verbatim):** "the canonical `distinct_id` is programmatically selected
  by Mixpanel, using one of the IDs inside of the identity cluster, based on the most optimal
  merging process. This means that the canonical distinct_id could be set to a `$device_id` or your
  chosen `user_id`. **This is not user-configurable.**"
- "You can use any of the IDs in the cluster for ingestion, but only the canonical `distinct_id`
  can be used in queries and exports." ← this sentence is the strongest doc-level signal that raw
  export emits the **canonical** cluster ID as each event's `distinct_id`, not the ID the event was
  originally sent with.
- Merging is retroactive; report propagation up to 24 hours.

### 5. Simplified ID Merge semantics (the target system)

- Reserved event properties: `$device_id` (anonymous id), `$user_id` (identified id),
  `distinct_id` (derived).
- Derivation rule (verbatim from migration guide): "`distinct_id` is optional on events because
  Mixpanel automatically updates or overrides it whenever `$user_id` or `$device_id` is present on
  the events. It takes the value of `$user_id` if present; otherwise, it takes `$device_id` and
  prefixes it with `$device:`".
- "Any ID provided as `$device_id` will be prefixed with `$device:` in the ID cluster."
- Merge trigger: first event where both `$user_id` and `$device_id` co-occur. "A single instance of
  such event is adequate to trigger identity merging." A synthetic/"dummy" event with both
  properties is an officially sanctioned merge mechanism.
- Limits: "no limit on the number of `$device_id`s that can be merged into a single `$user_id`";
  "Simplified ID Merge supports only one User ID (`$user_id`) per ID cluster"; "It is not possible
  to merge 2 `$user_id`s together using the Simplified API."
- Canonical id: "For projects using the Simplified ID Merge API, the canonical `distinct_id` is
  always set to the `$user_id`." Anonymous-only clusters query/export as `$device:<device_id>`.
- Retroactive: on identification, "Mixpanel will retroactively set the `$user_id` on any prior
  events with the user's `$device_id`." Activity feed <1 min; other reports up to 24 h.
- Profiles: "User Profiles are set directly on `$distinct_id`s, not on `$user_id`s or
  `$device_id`s"; pre-merge profile properties on the `$device:` profile "are not preserved when
  `$device_id`s are linked to `$user_id`s."

### 6. Migration guide (Original → Simplified) — official position

- **No in-place conversion (verbatim):** "It is currently not possible to automatically convert an
  existing project, already populated with data, from Legacy or Original ID Merge to Simplified ID
  Merge… To adopt Simplified ID Merge, you would need to set up a new empty Mixpanel project."
- Sanctioned backfill paths: (1) "export data from the existing project using Raw Export API and
  then import it into the new project via Import API"; (2) warehouse connector; (3) CDP replay.
- If historical events came from Mixpanel client SDKs, they likely already carry `$device_id` and
  `$user_id` and "can be directly imported into the new Simplified ID Merge project."
- Custom implementations without those props must be transformed before backfill, or merges can be
  triggered with "a dummy event that includes both `$device_id` and `$user_id`."
- Dedupe: "ensure that each imported event includes a `$insert_id`."
- Age limit note: "Mixpanel Client-Side SDKs, by default, use the /track API endpoint which accepts
  events up to 5 days old" — historical backfill must go through `/import` (which this repo already
  does).

### 7. Verb → Simplified translation map (implication for the replay pipeline)

| Original verb | Carries | Simplified equivalent |
|---|---|---|
| `$identify` | `$anon_id` A, `$identified_id` U | one event with `$device_id: A`, `$user_id: U` (A loses UUIDv4 restriction; becomes `$device:A` in cluster) |
| `$create_alias` | `distinct_id` D (existing), `alias` A (new) | **no clean equivalent** — both are typically user-level IDs; simplified allows only one `$user_id` per cluster. Must pick one canonical `$user_id` and demote the other to `$device_id` (accepting the `$device:` prefix), or remap all events to the canonical ID at transform time. |
| `$merge` | `$distinct_ids: [a, b]` | same problem as alias: merging two user IDs is impossible in Simplified. Resolve cluster offline and rewrite event `distinct_id`s / `$user_id`s to one canonical ID. |

The docs implicitly endorse the offline-resolution approach: because raw export already emits the
canonical cluster ID as `distinct_id` (Section 4), an export→import replay of *regular* events may
largely self-resolve — the hard part is choosing `$user_id` vs `$device_id` per event and deciding
whether identity events need replaying at all versus being consumed to build a mapping table.

### 8. Raw Export API (transport for the replay source)

- `GET https://data.mixpanel.com/api/2.0/export` (hosts: `data`, `data-eu`, `data-in`).
- Auth: HTTP Basic with project secret; `project_id` param required when using a service account.
- Required: `from_date`, `to_date` (yyyy-mm-dd, inclusive, project timezone).
- Optional: `event` (JSON array of event names, e.g. `["signup","purchase"]`), `where` (filter
  expression), `limit` (max 100,000), `time_in_ms` (bool, default false = second precision),
  `Accept-Encoding: gzip` (compresses when response > 1400 bytes).
- Rate limits: "60 queries per hour, 3 queries per second, and a maximum of 100 concurrent
  queries"; excess → `429`.
- Output: JSONL — "one event per line where each line is a valid JSON object."
- **The reference page says nothing about identity events** — no statement on whether
  `$identify`/`$create_alias`/`$merge` rows appear by default, must be requested via
  `event=["$identify",…]`, or are excluded entirely.

### 9. How identity/exported events actually look (docs + community + repo evidence)

- Community/AI-answer sources (community.mixpanel.com) describe `$distinct_id_before_identity` as
  "an internal Mixpanel property used to track an event's original `$distinct_id` before it was
  updated due to identity merging… used in both the Original ID Merge and Simplified ID Merge
  systems." I.e., exported events show canonical `distinct_id` plus
  `properties.$distinct_id_before_identity` = the ID the event was originally sent with. Not
  confirmed in official reference docs — verify empirically.
- This repo already encodes the expected shapes:
  - `vendor/posthog.js` (v2 = Original mode) constructs identity events as
    `{ event: "$identify", properties: { $identified_id, $anon_id, ...rest } }`, and in v3
    (Simplified mode) **drops** `$identify` events entirely (`identify_events = ["$identify"]`,
    "don't send identify events in simplified mode") — the exact behavior this sprint replaces.
  - `vendor/mixpanel.js` maps exported-Mixpanel fields `device_id → $device_id`,
    `user_id → $user_id`, `insert_id → $insert_id`, and passes `distinct_id` through — evidence the
    exported property bag uses un-`$`-prefixed `device_id`/`user_id`/`insert_id` keys in some
    export shapes (schematized pipeline naming) while raw `/export` uses `$`-prefixed keys inside
    `properties` (`$device_id`, `$user_id`, `$insert_id`, plus `time`, `distinct_id`).

## Gotchas / Limits

- **Verbs are silent no-ops in Simplified projects** — sending `$identify`/`$create_alias`/`$merge`
  to the new project will not error; it just does nothing for identity. `/track` even returns `1`.
  Strict-mode `/import` of `$merge` into a simplified project may error — unverified.
- **`$anon_id` UUIDv4 gate (Original only):** a replayed `$identify` in an *Original* target only
  merges if `$anon_id` is UUIDv4 and unmerged. Irrelevant for a Simplified target (verb is dead
  there), but critical if anyone reuses the translator to replay into another Original project.
- **`$merge` is exactly 2 IDs** per event (`minItems: 2, maxItems: 2`), `/import`-only, and needs
  real auth (secret/service account), unlike the other two verbs.
- **500-ID cluster cap** exists only in Original; Simplified swaps it for **one `$user_id` per
  cluster** — alias chains and user-to-user merges from the source project can be *unrepresentable*
  as live identity operations in the target and must be resolved at transform time.
- **`$device:` prefix:** any ID demoted to `$device_id` during translation acquires a `$device:`
  prefix in the target's clusters/queries — IDs will not round-trip byte-identical.
- **Merges are irreversible** in both systems; a bad replay cannot be unmerged — test on a
  disposable project first.
- **`/track` vs `/import` age limit:** `/track` accepts events only up to 5 days old; all
  historical identity-event replay must go through `/import` regardless of which endpoint the verb
  "belongs" to (docs confirm `/import` accepts `$identify` and `$create_alias` payloads).
- **`1` from `/track` is not proof of success** — it doesn't even validate the token. Use `/import`
  with `strict=1` for verifiable results.
- **Raw export rate limits (60/hr, 3/sec, 100 concurrent, 429)** bound the export half of the
  pipeline; `limit` caps at 100,000 events per query.
- Profile properties written to anonymous (`$device:`) profiles before a merge are **not carried
  over** to the merged profile in Simplified.
- Up to **24 h** before merges are reflected in reports — post-replay validation queries must wait
  or use the activity feed (<1 min).

## Open Questions

1. **Do `$identify` / `$create_alias` / `$merge` rows appear in raw `/export` output, and under
   which conditions?** Official docs are silent. Verify empirically against project 3996669
   (the repo's standing event-export test project): run an export with and without
   `event=["$identify","$create_alias","$merge"]` and inspect.
2. **Exact exported property bag for each verb** — e.g. does an exported `$identify` retain
   `$anon_id`/`$identified_id` under those names inside `properties`? Does an exported `$merge`
   keep `$distinct_ids`? Needs a sample export.
3. **Is `distinct_id` on exported events always the canonical cluster ID, with the original in
   `$distinct_id_before_identity`?** Strongly implied by "only the canonical `distinct_id` can be
   used in queries and exports" and community sources, but not stated in the export reference.
4. **What does strict `/import` return when a `$merge` event is sent to a Simplified project?**
   (Silent drop vs per-record validation error.) Determines whether the translator must filter
   verbs or can rely on target-side rejection.
5. **`$distinct_ids` arity:** API schema says exactly 2; prose says "any 2 ID clusters… 2+
   identifiers". Assume 2 and split larger sets into pairwise merges if ever generating `$merge`.
6. Does raw export include the `$user_id` / `$device_id` reserved properties on ordinary events in
   an Original-ID-merge project when the SDK supplied them (migration guide implies yes for
   client-SDK data)? Determines whether translation is a pass-through for SDK-sourced events.
