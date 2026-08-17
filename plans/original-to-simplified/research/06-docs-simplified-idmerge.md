# Simplified ID Merge — Official Docs Research

Sources (fetched 2026-08-16):

- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-simplified
- https://docs.mixpanel.com/docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system
- https://docs.mixpanel.com/docs/tracking-methods/id-management (Identity Management Overview)
- https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-original
- https://docs.mixpanel.com/docs/data-structure/property-reference/reserved-properties
- https://docs.mixpanel.com/reference/create-identity.md, /reference/identity-create-alias.md, /reference/identity-merge.md

## Summary

Simplified ID Merge has **no identity verbs at all**. Identity is resolved from two reserved
event properties — `$device_id` (anonymous ID) and `$user_id` (known ID) — and a merge happens
implicitly the **first time** both appear on the same event. `distinct_id` is computed by
Mixpanel: `$user_id` if present, else `"$device:" + $device_id`. The special events `$identify`,
`$create_alias`, and `$merge` are **ignored** in Simplified projects ("will not trigger identity
merging" — not an ingestion error, no 400). There is **no limit** on the number of `$device_id`s
mergeable into one `$user_id` (vs. Original's hard 500-IDs-per-cluster cap), but there is exactly
**one `$user_id` per cluster** — merging two `$user_id`s is impossible, which is the single
biggest translation constraint for an Original→Simplified replay pipeline. Existing projects
cannot be converted in place; migration means a **new empty project** plus export/transform/import,
which is precisely what this sprint's pipeline does.

## Key Facts

- Merge mechanism: "When a `$user_id` and `$device_id` are present in the same event for the
  first time, a mapping is created to merge the `$user_id` and `$device_id` values together,
  forming an identity cluster."
- One dual-ID event is sufficient: "A single instance of such event is adequate to trigger
  identity merging."
- `distinct_id` computation: "It takes the value of `$user_id` if present; otherwise, it takes
  `$device_id` and prefixes it with `$device:`". Mixpanel "automatically updates or overrides"
  any `distinct_id` you send whenever `$device_id`/`$user_id` are present.
- Canonical distinct_id = the `$user_id`, always (user-controlled, unlike Original where it is
  "programmatically selected … random and not user-configurable").
- Verbs in Simplified projects: "You should not send $identify, $create_alias, and $merge events
  since they will be **ignored** in Simplified ID Merge projects and will not trigger identity
  merging." API reference note: the `$identify` payload "is only useful for projects using the
  Original ID Merge system; it has no functionality in other ID management systems" (same wording
  for `$merge`; `$create_alias` note adds Legacy too).
- Cluster limits: Simplified — "There is no limit on the number of `$device_id`s that can be
  merged into a single `$user_id`" / "does not have a limit on the number of identifiers allowed
  in an ID cluster". Original — "There is a limit of 500 IDs that can be merged into a single ID
  cluster"; on reaching it, "any new Distinct ID can no longer be merged … They will then become
  orphaned."
- No user_id↔user_id merge: "It is not possible to merge 2 `$user_id`s together using the
  Simplified API." / "Simplified ID Merge supports only one User ID (`$user_id`) per ID cluster."
- No direct anon↔anon merge mechanism exists either — the only merge trigger is a dual-ID event,
  so two `$device_id`s can only end up together transitively via the same `$user_id`.
- Backward compatibility for events carrying only `distinct_id`: values prefixed `$device:` are
  treated as `$device_id`; unprefixed values are treated as `$user_id`.
- Dummy events are the sanctioned way to inject a mapping: any event name works "except for
  $identify, $create_alias, and $merge".
- Reserved output properties written by identity merging (both systems): `$distinct_id_before_identity`
  and `$is_reshuffled`. Simplified-specific failure property: `$identity_failure_reason`.
- Projects cannot switch systems once any data is ingested: "Once there are any data in your
  project, your Identity API version cannot be changed." New empty project required.
- Simplified is default for new orgs created from **April 2024**.
- Merge propagation latency: Activity Feed <1 min; "It may take up to 24 hours for this mapping
  to propagate to all other parts of the system."

## Details

### Property semantics

- `$device_id` — "unique identifier used to track a device while the user is in anonymous state"
  (reserved-properties doc). Client SDKs auto-generate it and attach it to all events. Any value
  supplied as `$device_id` is prefixed with `$device:` when it becomes a distinct_id / cluster
  member: "Any ID provided as `$device_id` will be prefixed with `$device:` in the ID cluster."
- `$user_id` — "unique identifier used to track a user across devices when user is in identified
  state." Set by calling `identify(<user_id>)` in SDKs; server-side you set the property directly.
- `distinct_id` — optional on ingest in Simplified. Rules, verbatim from docs:
  - "If an event contains a `$device_id` without a `$user_id`, the value of the `$device_id`
    will be set as the `distinct_id`" (concretely, as `$device:<device_id>` — the example tables
    show `distinct_id` = `$device:D1`).
  - "If an event contains a `$user_id`, the value of the `$user_id` will be set as the
    `distinct_id`."
  - "If you choose to manually define the `distinct_id` property, it should be the same value as
    the `$user_id`." Mixpanel overrides it anyway when either reserved property is present.
- Profile updates use `$distinct_id` (events use `distinct_id` — note the `$` difference).
  Profiles attach to distinct_ids, not to `$user_id`/`$device_id`. You *can* write a profile to
  `$distinct_id=$device:<device-id>`, but "user profile properties are not preserved when
  `$device_id`s are linked to `$user_id`s" — anonymous-profile properties must be re-set on
  `$distinct_id=<user-id>` after identification.

### Worked ingestion examples (from migration guide, verbatim payload shapes)

Anonymous event:

```json
{ "event": "View Anonymous Page",
  "properties": { "token": "{{token}}", "$device_id": "anonymous111" } }
```

→ Mixpanel sets `"distinct_id": "$device:anonymous111"`.

Merge-triggering event (any name except the three verbs):

```json
{ "event": "Sign Up",
  "properties": { "token": "{{token}}", "$device_id": "anonymous111", "$user_id": "charlie" } }
```

→ Mixpanel sets `"distinct_id": "charlie"` and creates the mapping anonymous111 → charlie.

Backward-compat (distinct_id only, no reserved props):

```json
{ "event": "Message Sent", "properties": { "token": "{{token}}", "distinct_id": "charlie" } }
```

→ Mixpanel sets `"$user_id": "charlie"`.

```json
{ "event": "App Install", "properties": { "token": "{{token}}", "distinct_id": "$device:anonymous111" } }
```

→ Mixpanel sets `"$device_id": "anonymous111"`.

"Ensure that the `distinct_id` value of an anonymous user's events are always prefixed with
`$device:` if this approach is used."

### Retroactive stitching

- "Mixpanel will retroactively set the `$user_id` on any prior events with the user's
  `$device_id` so that both event streams are joined." Example tables mark prior device-only
  events as "Retroactively updated": `distinct_id` goes `$device:D1 ⇒ U1`.
- Applies across devices/sessions: a second device D2, used anonymously then identified as the
  same U1, gets its pre-login events retroactively re-attributed too ("D1, D2, and U1 are inside
  one ID cluster").
- "Simplified ID Merge can retroactively merge an unlimited number of anonymous IDs
  (`$device_id`) to a user (`$user_id`)."
- After a link exists: "Any data sent with a `distinct_id` set to any of the values in an ID
  cluster will be attributed back to the same user in Mixpanel." So post-link events carrying
  only `$device_id` still resolve to the same user; only the canonical distinct_id (`$user_id`)
  can be used in **queries and exports**, while "you can use any of the IDs in the cluster for
  ingestion."
- Latency: real-time in Activity Feed (<1 minute); up to 24 hours everywhere else. Real-time
  funnels spanning pre/post-login may transiently show drop-offs.

### The three identity verbs (Original) — payload shapes needed for translation

These are the input events the replay pipeline must recognize in exported Original-project data:

- `$identify` — properties: `$identified_id` (required), `$anon_id` (required), `token`;
  `distinct_id` optional ("inferred from $identified_id if not included"). Original-only
  constraint: "$anon_id must be UUID v4 format and not already merged to an $identified_id."
  Sent via `/track#create-identity` or `/import`.
- `$create_alias` — properties: `distinct_id` (required; "A distinct_id to be merged with the
  alias"), `alias` (required; "A new distinct_id to be merged with the original distinct_id.
  Each alias can only map to one distinct_id"), `token`. Aliases "can be daisy-chained; but the
  same alias ID cannot point to multiple different Distinct IDs." Sent via
  `/track#identity-create-alias` or `/import`.
- `$merge` — properties: `$distinct_ids` (required, array, **minItems: 2, maxItems: 2**), `token`.
  "We will only accept `$merge` events that are sent via `https://api.mixpanel.com/import`,
  which is protected by the project api secret." Irreversible. In Original, `$merge` can join
  any two IDs (including two identified IDs) — this is exactly the capability Simplified lacks.
- Original cluster cap: a merge succeeds "as long as the merging of 2 IDs does not lead to an
  ID cluster that exceeds 500 IDs." No explicit error text documented for exceeding it; the
  migration guide describes the result as new IDs becoming "orphaned (duplicate users)."
- In Original, exported events carry the verbs' inputs as reserved props: `$identified_id`,
  `$anon_id` (from `$identify`), `alias` (from `$create_alias`), `$distinct_ids` (from `$merge`).

### Reserved properties relevant to identity (reserved-properties doc, verbatim)

| Property | System | Meaning |
|---|---|---|
| `$device_id` | Simplified | "unique identifier used to track a device while the user is in anonymous state" |
| `$user_id` | Simplified | "unique identifier used to track a user across devices when user is in identified state" |
| `$identified_id` | Original | "Internal Mixpanel property to track the identifier passed into the $identify event" |
| `$anon_id` | Original | "Internal Mixpanel property to track the anonymous ID passed into the $identify event" |
| `alias` | Original | "Internal Mixpanel property to track the alias passed into the $create_alias event" |
| `$distinct_ids` | Original | "Internal Mixpanel property to track the distinct IDs passed into the $merge event" |
| `$distinct_id_before_identity` | **Both** | "Internal Mixpanel property to track an event's original $distinct_id before it was updated due to identity merging" |
| `$is_reshuffled` | **Both** | "Internal Mixpanel property to denote an event was reshuffled (sets to true if original $distinct_id was updated) due to identity merging" |
| `$failure_description` | Original | "property explaining in detail why identity merging was not executed" |
| `$failure_reason` | Original | "property summarizing why identity merging was not executed" |
| `$identity_failure_reason` | **Simplified** | "property summarizing why identity merging was not executed" |
| `mp_original_distinct_id` | both (hot shard) | "Original $distinct_id for an event that was identified as contributing to a hot shard" |

Implication for the pipeline: exported events from the Original project will contain
`$distinct_id_before_identity` on retroactively-updated events — this is the pre-merge
distinct_id and is a first-class signal for reconstructing the original `$device_id` when the
exported event lacks `$device_id`/`$user_id`.

### Migration guidance from the official migration guide (condensed but complete)

- Three ID-management generations: Legacy (pre-March 2020, one-shot `.alias()`), Original ID
  Merge (March 2020, verbs + retroactive merging + 500 cap), Simplified ID Merge (March 2023,
  reserved-props only, no cap).
- Version discovery: Identity Merge setting under Organization Settings (org owners/admins) or
  Project Settings (project owners/admins). Org setting values: Disabled = Legacy,
  Original API, Simplified API. Org setting is the default for new projects; override per
  project **before any data is ingested**.
- "It is currently not possible to automatically convert an existing project, already populated
  with data … To adopt Simplified ID Merge, you would need to set up a new empty Mixpanel
  project." New project = new token, new API secret, new service accounts.
- Live-data guidance: client SDK minimum versions supporting Simplified — JavaScript >= 2.46.0,
  Android >= 7.3.0, iOS Objective-C >= 5.0.2, Swift >= 4.0.5, React Native >= 2.2.0,
  Flutter >= 2.1.0. **Unity SDK does not support Simplified ID Merge.** Don't call `alias`.
- Backfill guidance: export with Raw Export API, import with `/import`; ensure every imported
  event has `$insert_id` for dedup; `/track` only accepts events "up to 5 days old" (backfills
  go through `/import`); `mp_processing_time_ms` = "UTC timestamp of when the event was
  processed by our servers" — use it to find late-arriving events for incremental backfills.
- If historical events already carry `$device_id`/`$user_id` (SDK-instrumented Original
  projects do): "These historical events can be directly imported into the new Simplified ID
  Merge project."
- If they don't (custom/server-side implementations): "derive the reserved properties from
  other relevant properties on the events or from ID mappings maintained in your system" and/or
  "Instrument a dummy event that includes both $device_id and $user_id based on your ID mappings."
- CDP compatibility: Segment works with both APIs unconfigured; Rudderstack has a connection
  setting that must match the project's API version; mParticle needs the "Simplified ID Merge"
  option checked for Simplified projects.
- Non-data entities (boards, cohorts, custom events/properties, lookup tables, Lexicon schemas)
  must be recreated/moved manually; Move Board transfers boards between projects.

### SDK behavior contract (client-side, for reference)

1. `.identify(<user_id>)` on signup/login; send **at least one event after** the identify call —
   "This is necessary to get the $user_id and $device_id to merge."
2. `.reset()` on logout — clears local storage ($user_id + $device_id) and generates a fresh
   `$device_id`, preventing cross-user merges on shared devices.
3. Events before `.identify()` are anonymous, carrying only `$device_id`.

## Gotchas / Limits

- **One `$user_id` per cluster, forever.** No mechanism merges two `$user_id`s, and a `$user_id`
  cannot be changed. Original-project clusters containing multiple identified IDs (built with
  `$merge`/`$create_alias`) **cannot be represented** in Simplified — the pipeline must pick one
  canonical `$user_id` per cluster and rewrite all other identified IDs to it (or demote them).
- **The demotion trap (documented example):** sending
  `{"$device_id": "+6512345678", "$user_id": "charlie"}` to link a second identified ID makes
  `+6512345678` an *anonymous* ID (`$device:+6512345678`). Any later event with
  `$user_id: "+6512345678"` "will not be associated to charlie and would result to creating a
  completely different new ID cluster." Translation must therefore rewrite user_ids
  consistently on *every* event, not just emit mapping events.
- **No anon→anon merge verb.** Two `$device_id`s join a cluster only transitively via a shared
  `$user_id`. A user who is *never* identified can never have their two devices merged in
  Simplified. Original `$merge` events joining two anonymous IDs have no direct equivalent —
  they only translate if the cluster eventually contains a `$user_id`.
- **Verbs are ignored, not rejected.** Importing `$identify`/`$create_alias`/`$merge` into a
  Simplified project produces no error; they just do nothing for identity. (So a pipeline that
  forwards verbs unchanged fails *silently* — user counts fragment with no ingestion errors.)
- **No 500 cap in Simplified, but the cap matters on the source side:** Original clusters at the
  500 limit have orphaned IDs; those orphans exported from the source project are separate users
  there, but the replay can *heal* them if the ID mapping is known.
- **`$device:` prefix is part of the distinct_id**, and shows up in exports/queries as e.g.
  `$device:anonymous111`. When reading exported Simplified data or writing distinct_id-only
  events, the prefix must be preserved (anonymous) or absent (identified) exactly.
- **`distinct_id` vs `$distinct_id`:** events use `distinct_id`, profile (Engage) payloads use
  `$distinct_id`.
- **Anonymous profiles are lossy:** profile props written to `$distinct_id=$device:<id>` are NOT
  carried over when the device links to a user; they must be re-written to the `$user_id`.
- **Propagation delay:** merges take up to 24h to reflect everywhere (Activity Feed <1 min).
  Validation right after a replay may transiently show fragmented users.
- **First-mapping wording:** merge is created when both IDs appear "for the first time" — docs
  do not spell out what happens if the same `$device_id` later appears with a *different*
  `$user_id` without a reset (see Open Questions).
- **Merging can fail silently with an annotation:** `$identity_failure_reason` exists for
  Simplified projects — check for it when validating a backfill.
- **/track vs /import:** `/track` accepts events only up to 5 days old; all backfill goes to
  `/import` (which requires `$insert_id` discipline for dedup). `strict=1` returns per-record
  errors (`failed_records[]` with `index`, `insert_id`, `field`, `message`).
- **Ingest hosts:** `api.mixpanel.com`, `api-eu.mixpanel.com`, `api-in.mixpanel.com` (pick per
  residency).
- **Unity SDK** does not support Simplified ID Merge at all (live-data concern, not replay).

## Open Questions

1. **Device re-mapping / conflict semantics:** if `$device_id` D1 is merged to U1, and later an
   event arrives with `$device_id: D1, $user_id: U2` (no reset), do D1's *anonymous* events move?
   Does the event itself go to U2? The docs only say a mapping is created when both are present
   "for the first time" and recommend `.reset()` to avoid the situation. Not specified on any
   fetched page — needs an empirical test in a scratch Simplified project (directly relevant to
   replay ordering).
2. **Does event ORDER matter for retroactive stitching in a bulk import?** I.e., can the dummy
   mapping events be imported before/after the anonymous events, or interleaved arbitrarily?
   Docs imply retroactivity makes order irrelevant, but never state it for `/import` batches.
   Worth an empirical check.
3. **Are ignored verbs counted/billed?** "Ignored" is stated only for identity merging; whether
   a `$identify` event imported into a Simplified project is stored as a regular (billable)
   event or dropped entirely is not documented. (Either way the pipeline should translate,
   not forward.)
4. **Is `$device:` prefixing case-sensitive / normalized?** Not documented. Exported data uses
   exactly `$device:` — assume byte-exact.
5. **Does `$distinct_id_before_identity` appear in Raw Export payloads reliably** for all
   reshuffled events (and is `$is_reshuffled` exported), or are these query-layer only? The
   reserved-properties doc calls them "Internal Mixpanel property" — verify against a real
   export from the source project.
6. The Original ID Merge docs page (identifying-users-original) is no longer listed in
   docs.mixpanel.com/llms.txt (still reachable directly) — content may be in the process of
   being restructured; re-verify quotes if this becomes load-bearing later.
