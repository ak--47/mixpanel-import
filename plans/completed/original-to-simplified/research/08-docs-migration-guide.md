# Research: Official Mixpanel Migration Guide — Original → Simplified ID Merge

Source: https://docs.mixpanel.com/docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system
Adjacent source (referenced by the guide, fetched for merge semantics): https://docs.mixpanel.com/docs/tracking-methods/id-management/identifying-users-simplified
Fetched: 2026-08-16

## Summary

This is the doc our pipeline effectively automates/overturns. Mixpanel's official position: you **cannot convert a
populated project in place** ("Simplified ID Merge has a very different backend architecture") — you must create a
**new empty project**, flip it to "Simplified API" **before any data arrives**, re-point live tracking, and then
*optionally* backfill history via Raw Export API → Import API (or Warehouse Connector / CDP replay). The guide's
identity-translation story is thin but decisive: `$identify`, `$create_alias`, and `$merge` events "will be ignored
in Simplified ID Merge projects and will not trigger identity merging"; the *only* merge mechanism is an event
carrying both `$device_id` and `$user_id` ("A single instance of such event is adequate to trigger identity
merging"), and the doc explicitly blesses sending a **dummy event** with both properties to carry ID mappings —
named anything **except** `$identify`, `$create_alias`, or `$merge`. The hard wall for our translator is that
Simplified supports **exactly one `$user_id` per ID cluster** (unlimited `$device_id`s), so Original-merge clusters
that used `$create_alias`/`$merge` to join multiple *user* IDs cannot be represented — a canonical ID must be chosen
and the rest remapped or dropped. The doc never specifies what happens when a `$device_id` later appears with a
*different* `$user_id` — that's our biggest open empirical question for replay ordering.

## Key Facts

- **No in-place migration.** Verbatim: "It is currently not possible to automatically convert an existing project,
  already populated with data, from Legacy or Original ID Merge to Simplified ID Merge. This is because Simplified
  ID Merge has a very different backend architecture. **To adopt Simplified ID Merge, you would need to set up a new
  empty Mixpanel project**."
- **The Simplified API toggle must be flipped before any data.** "The new project follows the organization's default
  (Legacy or Original ID Merge). You have to switch the project to Simplified ID Merge *before* sending any data to
  the project." (Project Settings → Identity Merge → "Simplified API".)
- **Identity verbs are dead letters in Simplified projects**: "$identify, $create_alias, and $merge events … will be
  ignored in Simplified ID Merge projects and will not trigger identity merging."
- **Merge trigger**: an event with both `$device_id` and `$user_id`. "A single instance of such event is adequate to
  trigger identity merging."
- **Dummy-event escape hatch (this is our translation target)**: "Instrument a dummy event that includes both
  `$device_id` and `$user_id` based on your ID mappings and send that to the new project to enable identity merging.
  You can choose any name for the dummy event except for $identify, $create_alias, and $merge."
- **`distinct_id` is optional and overridden on ingest**: "Mixpanel automatically updates or overrides it whenever
  `$user_id` or `$device_id` is present on the events. It takes the value of `$user_id` if present; otherwise, it
  takes `$device_id` and prefixes it with `$device:`."
- **Cluster shape**: unlimited `$device_id`s per cluster; "Simplified ID Merge supports only one User ID
  (`$user_id`) per ID cluster, and this User ID will serve as the user's canonical Distinct ID." "It is not possible
  to merge 2 `$user_id`s together using the Simplified API." A `$user_id` also cannot be *changed*.
- **Backward compat**: "Simplified ID Merge still supports events that are only sent with `distinct_id` property
  (i.e. no `$device_id` and `$user_id` properties)."
- **Dedup on backfill**: every imported event must include `$insert_id` (used for deduplication).
- **Merge propagation delay**: up to **24 hours** to reflect in all reports; Activity Feed updates in "less than 1
  minute".
- **Original ID Merge's cap**: **500 Distinct IDs per ID cluster** — the motivating limit removed by Simplified.
- **Client-SDK-tracked Original-merge events already carry `$device_id`/`$user_id`** and "can be directly imported
  into the new Simplified ID Merge project" — the translation problem is confined to identity events and to
  server-side/custom implementations.

## Details

### 1. The three ID management systems (page intro, verbatim)

"Mixpanel currently has three versions of ID management: Prior to March 2020, we only had 1 version that merges a
user's very first anonymous state to the identified state (typically on sign-up when a User ID is created). We will
refer to this as **Legacy ID Management** (or Legacy for short). In March 2020, we released **Original ID Merge**
(formerly known as just 'ID Merge'), which supports retroactive identity merging, enabling the merging of multiple
anonymous states to an identified state across multiple devices and platforms. In March 2023, we released
**Simplified ID Merge** to remove the complexities of having to rely on different identity methods (i.e. $identify,
$create_alias, $merge) for different merging scenarios. This also removed the need to cap Distinct IDs at 500 in an
ID cluster."

How to check which system a project is on: "navigate to **Identity Merge** setting under your Organization Settings
(for Organization Owners/Admins) or Project Settings (for Project Owners/Admins)." Org-level setting is the default
for every *new* project. Values: **Disabled** = Legacy ID Management; **Original API** = Original ID Merge;
**Simplified API** = Simplified ID Merge.

Legacy details:
- "Aliasing on Legacy ID Management can only be done once. Once a User ID is aliased to an Anonymous ID (typically
  on the 1st device where they started using your product), subsequent attempts to alias the same User ID to a
  different Anonymous ID (generated from a different platform or device) will fail." → orphaned anonymous states on
  additional devices.
- "If you are only tracking authenticated users (i.e. don't track events while the user is anonymous), you don't
  need the retroactive identity merging feature in Simplified ID Merge and should not consider the migration."

Original ID Merge details:
- "the main limitation is that each user's ID cluster is limited to a maximum of 500 Distinct IDs. Upon reaching
  this limit, any new Distinct ID can no longer be merged into the same ID cluster."
- "Reaching the 500 Distinct IDs per ID cluster limit is possible when the process of generating new Anonymous IDs
  through the `reset()` call on logout, and adding them to the ID cluster repeats 500 times."
- Original supports "multiple identified IDs (i.e. User IDs) per ID cluster" via "special events such as $merge and
  $create_alias but they are not supported on Simplified ID Merge."

### 2. Why in-place migration is impossible (official reasoning)

Single stated reason, verbatim: "This is because Simplified ID Merge has a very different backend architecture."
No further technical detail is given. The prescribed alternative is the new-project + re-point + optional-backfill
flow below. (Our pipeline is exactly an automation of the "backfill" arm plus the identity translation the doc
leaves to the reader.)

### 3. How Simplified ID Merge works ("Understanding Simplified ID Merge" section)

"Simplified ID Merge only requires including reserved event properties `$device_id` and `$user_id` on the events for
identity merging to take place."

Anonymous event:

```json
{
  "event": "View Anonymous Page",
  "properties": {
    "token": "{{token}}",
    "$device_id": "anonymous111"
  }
}
```

Identified event (this shape *is* the merge trigger — "A single instance of such event is adequate"):

```json
{
  "event": "Sign Up",
  "properties": {
    "token": "{{token}}",
    "$device_id": "anonymous111",
    "$user_id": "charlie"
  }
}
```

`distinct_id` behavior, verbatim: "`distinct_id` is optional on events because Mixpanel automatically updates or
overrides it whenever `$user_id` or `$device_id` is present on the events. It takes the value of `$user_id` if
present; otherwise, it takes `$device_id` and prefixes it with `$device:`."

Resulting stored events:

```json
{
  "event": "Sign Up",
  "properties": {
    "token": "{{token}}",
    "$device_id": "anonymous111",
    "$user_id": "charlie",
    "distinct_id": "charlie"
  }
}
```

```json
{
  "event": "View Anonymous Page",
  "properties": {
    "token": "{{token}}",
    "$device_id": "anonymous111",
    "distinct_id": "$device:anonymous111"
  }
}
```

Cluster limits: "Simplified ID Merge can retroactively merge an unlimited number of anonymous IDs (`$device_id`) to
a user (`$user_id`)." / "You can merge unlimited number of `$device_id` into a `$user_id`" / "Simplified ID Merge
supports only one User ID (`$user_id`) per ID cluster."

From the adjacent implementation page: "When a `$user_id` and `$device_id` are present in the same event for the
first time, a mapping is created to merge the `$user_id` and `$device_id` values together, forming an identity
cluster."

### 4. Considerations before migrating (full list, verbatim where it matters)

1. **One User ID per cluster.** "Simplified ID Merge supports only one User ID (`$user_id`) per ID cluster, and this
   User ID will serve as the user's canonical Distinct ID. If you need an ID management solution that supports
   multiple User IDs per user, such as both a email address and a phone number, it's recommended to remain on Legacy
   ID Management or Original ID Merge which provide methods such as $create_alias or $merge to merge multiple User
   IDs."
2. **Third-party compatibility + backward compat.** "If you are sending events via third-party integrations, ensure
   that they are compatible with Simplified ID Merge by having reserved properties, `$device_id` and `$user_id` on
   the events. For backward compatibility, Simplified ID Merge still supports events that are only sent with
   `distinct_id` property (i.e. no `$device_id` and `$user_id` properties)."
3. **Mobile app-update lag → dual-write window → repeat backfill.** "you'll need to ship a new version of the app
   with the updated ID management implementation, and the new project's token … Without a forced app update, it may
   take awhile for all users to upgrade to the latest app version. During this period, some events will still be
   tracked to the old project. Be prepared for data backfilling if you want these events, as well as the historical
   data to be included in the new project."
4. **24-hour merge propagation.** "it may take up to 24 hours for identity merging (merging 2 unique users into 1
   unique user) to be fully reflected in all Mixpanel reports."
5. **Unity SDK unsupported.** "All Mixpanel **Client-Side SDKs** support Simplified ID Merge except for **Unity
   SDK**."

### 5. Migration steps (the official runbook)

**Set up the new project:**
1. Org Settings → Projects → create new project.
2. New project's Settings → **Identity Merge** → select **"Simplified API"** — *before* sending any data (project
   inherits the org default otherwise).
3. Recreate project scaffolding: invite users / roles & permissions, teams, **group keys** (Group Analytics), data
   views, service accounts, session settings. Replace project tokens, API secrets, and service-account credentials
   everywhere.

**Send live data:**
- Client-side SDK minimum versions for Simplified support: **JavaScript ≥ v2.46.0, Android ≥ v7.3.0, iOS
  (Objective-C) ≥ v5.0.2, Swift ≥ v4.0.5, React Native ≥ v2.2.0, Flutter ≥ v2.1.0**. Initialize with the new token;
  call only `identify` and `reset`; **do not call `alias`**.
- Server-side/API: update credentials; "Do not send `$identify`, `$create_alias`, or `$merge` events—they are
  ignored in Simplified projects." Include `$device_id` and `$user_id` on event payloads, "or send dummy events
  containing both properties at authentication state changes."
- CDPs: update credentials, verify Simplified compatibility.

**Migrate non-data entities (all manual or semi-manual):**
- Cohorts, custom events, custom properties: manually recreate by copying logic.
- Lookup tables: manually re-upload via Lexicon and re-map to properties.
- Boards & reports: **"Move Board"** feature; duplicate boards before moving; recreate dependent
  cohorts/custom-events/properties *first*; verify permissions afterward.
- Lexicon schema definitions: **Lexicon Schemas API** or **CSV Export/Import**.

**Validate:** verify "users who are using your product across multiple platforms, devices, or sessions are being
merged correctly" with the reserved properties populated.

### 6. Backfilling historical data (the section our pipeline automates)

- Framed as **optional**: "If your existing project did not have that much data and you don't mind starting your
  analysis from scratch, you can skip this section on backfilling."
- Scope advice: "It's advisable to migrate only what you need (i.e. recent data actively queried by the team) as
  this is more manageable and resource-efficient."
- **Billing warning**: "Note that backfilling historical data can have significant impact on your billing."
- **Timing / late data**: "Mixpanel Client-Side SDKs, by default, use the /track API endpoint which accepts events
  up to 5 days old, so it is advisable to initiate the backfill process only after the data for a given day has
  stabilized to avoid the need for multiple backfills." Alternative: "consider using `mp_processing_time_ms`
  property (UTC timestamp of when the event was processed by our servers) to identify late-arriving events and
  selectively backfill them into the new project."
- **Dedup**: "ensure that each imported event includes a `$insert_id` which provides a unique identifier for the
  event and is used for deduplication."
- **Three sanctioned data sources:**
  1. *Mixpanel APIs*: "export data from the existing project using Raw Export API and then import it into the new
     project via Import API." Profiles: "You can use Engage API to migrate user data (APIs for both user export
     [engage-query] and batched user import [profile-batch-update] are available)." Suggested tooling: "the export
     and import functions from Mixpanel-utils open source library (github.com/mixpanel/mixpanel-utils)".
  2. *Warehouse Connector* — "supports both events and user data."
  3. *CDP* — "replay the historical data from CDP to Mixpanel."
- **Format requirement**: "Please make sure that the historical data is properly formatted before backfilling it to
  a new project via any of the methods mentioned above." Reader is pointed back at the Understanding-Simplified
  section "to plan out the required data transformation tasks for your historical data."

### 7. Identity translation hints (what the doc says about transforming exported events)

This is everything the doc offers — the rest is left to the implementer (i.e., us):

- **Events missing the reserved props**: "If your historical events do not include both `$device_id` and `$user_id`
  that are required in Simplified ID Merge for identity merging, check if you can retrieve this ID mapping
  information from your system through other means." Then the dummy-event mechanism: "Instrument a dummy event that
  includes both `$device_id` and `$user_id` based on your ID mappings and send that to the new project to enable
  identity merging. You can choose any name for the dummy event except for $identify, $create_alias, and $merge."
- **Original-merge history from client SDKs**: "If you had implemented using Mixpanel Client-Side SDKs (except for
  Unity) and have been calling identify to merge pre and post-login states, the SDK should have already populated
  `$device_id` and `$user_id` on your events (please verify this in your existing Mixpanel project). These
  historical events can be directly imported into the new Simplified ID Merge project as they include reserved
  properties required for identity merging."
- **Original-merge history with multi-user-ID clusters**: "If you are also calling alias or merge (using special
  events, $create_alias or $merge) to merge multiple user IDs per user, it's important to note that this
  functionality is not supported in Simplified ID Merge." (No remediation offered — the doc's answer is "don't
  migrate". Our pipeline needs a canonicalization strategy here.)
- **Legacy history, custom/server-side implementations**: "it's necessary to transform these events before
  backfilling it to the new project. … you can derive the reserved properties from other relevant properties on the
  events or from ID mappings maintained in your system."

So the doc's implicit translation recipe, which our pipeline generalizes:
1. Drop (or rewrite) `$identify` / `$create_alias` / `$merge` — they are ignored anyway.
2. Preserve `$device_id`/`$user_id` where SDKs already stamped them; regular events then merge-trigger on their own.
3. Where mappings exist only in identity events, emit **dummy merge events** with both `$device_id` and `$user_id`
   (any event name other than the three reserved ones).
4. Let ingest recompute `distinct_id` (`$user_id` wins; else `$device:` + `$device_id`).
5. Stamp `$insert_id` for dedup.

### 8. User-profile caveats (from the adjacent implementation page's FAQ, referenced by the guide)

- "User Profiles are set directly on `$distinct_id`s, not on `$user_id`s or `$device_id`s."
- "User profile properties are not preserved when `$device_id`s are linked to `$user_id`s." → profiles written to a
  `$device:`-prefixed distinct_id do not carry over on merge; profile backfill should target the canonical
  `$user_id` distinct_id.
- "$user_id" recommendation: "an ID that is unique to each user and does not change, for example, a database ID.
  … you cannot merge 2 `$user_ids` or change a `$user_id`, so if the user changes their email, they will count as a
  separate user."
- Merge visibility timing: Activity Feed "updated in real-time (less than 1 minute delay)"; "up to 24 hours for this
  mapping to propagate to all other parts of the system."

## Gotchas / Limits

- **One `$user_id` per cluster, forever.** No merge of two user IDs, no changing a user ID, no undo. Any
  Original-merge cluster containing >1 identified ID (built via `$create_alias`/`$merge`) is unrepresentable as-is;
  the pipeline must pick one canonical `$user_id` and rewrite every other identified ID in the cluster to it (the
  doc offers no mechanism — it just says the use case is "not supported").
- **`$identify`/`$create_alias`/`$merge` are silently ignored** in Simplified projects — not rejected. A naive
  replay that forwards them will *appear* to succeed while producing zero merges.
- **Dummy event name restriction**: any name except `$identify`, `$create_alias`, `$merge`.
- **`distinct_id` on imported events is cosmetic**: ingest overrides it whenever `$device_id`/`$user_id` is present
  (`$user_id` wins; else `$device:` prefix is added). Don't fight it; don't rely on preserving an old project's raw
  `distinct_id` values on events that carry the reserved props.
- **`$device:` prefix**: anonymous-only identities get a *different* distinct_id in the new project
  (`$device:anonymous111`) than they had in the old one — anything keyed on old distinct_id values (saved cohorts,
  external joins) won't line up 1:1.
- **`$insert_id` required** for dedup on backfill; multiple backfill passes are expected (5-day /track window,
  app-update lag), so idempotency matters.
- **Toggle timing is unforgiving**: a new project inherits the org default; if any data lands before switching to
  "Simplified API", the project is burned (setting must be flipped "*before* sending any data").
- **Billing**: backfill counts as ingestion — "significant impact on your billing."
- **24-hour report lag** for merges (Activity Feed ~1 min) — validation right after replay will look wrong.
- **Profile properties do not survive device→user linking**; profiles live on distinct_id, so import profiles
  against the canonical `$user_id`, not `$device:` IDs.
- **Everything non-data is manual**: cohorts, custom events/properties, lookup tables (Lexicon re-upload), Lexicon
  schemas (API/CSV), boards ("Move Board", after recreating dependencies). Group keys, service accounts, data views,
  session settings must be reconfigured; all tokens/secrets change.
- **Unity SDK** has no Simplified support at all.
- **Distinct-id-only events remain valid** ("for backward compatibility") — events that can't be mapped can still be
  imported with just `distinct_id`, they just won't participate in merging.

## Open Questions

1. **Device-ID conflict semantics (critical for replay ordering):** neither the migration guide nor the
   identifying-users-simplified page documents what happens when a `$device_id` already merged into cluster A later
   arrives on an event with a different `$user_id` B. First-write-wins? Event attributed to B without cluster merge?
   Needs an empirical test in a scratch Simplified project before we finalize replay ordering / canonicalization.
2. **Does the merge-trigger event's `time` matter?** Merging is retroactive, so presumably order-insensitive within
   a backfill, but the doc never states whether a dummy merge event dated years in the past behaves identically to a
   current-dated one (or whether /import's time-window rules constrain it). Test.
3. **Canonical-ID selection for multi-user-ID clusters:** the doc punts entirely ("not supported"). We must define
   policy: e.g., prefer the cluster's canonical distinct_id from the old project's `/export` (`$distinct_id` field?)
   or the most-recent/most-evented identified ID, and rewrite others — possibly preserving originals in a custom
   property for auditability.
4. **What identity events look like in Raw Export output** (`$identify` with `$identified_id`/`$anon_id`,
   `$create_alias` with `distinct_id`/`alias`, `$merge` with `$distinct_ids`): the migration guide does not show the
   exported shapes; confirm exact exported property names from the Raw Export research slice / real export data.
5. **Rate/size limits of /import for the dummy-event flood** (one merge event per device↔user edge) — covered by the
   import-API research slice, not this doc.
6. **`mp_processing_time_ms` availability in Raw Export** — the doc recommends it for late-event detection; confirm
   it is present on exported events for our incremental/backfill-again story.
