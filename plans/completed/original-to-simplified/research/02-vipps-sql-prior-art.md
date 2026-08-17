# Vipps SQL Prior Art — Original→Simplified ID Patching in BigQuery

Sources read (2026-08-16):

- `/Users/ak/code/bq-mp-inspector/customers/vipps/sql/build-identity-mapping-v2.sql`
- `/Users/ak/code/bq-mp-inspector/customers/vipps/sql/build-identity-mapping-v3.sql` (the definitive one, loaded 2026-08-15)
- `/Users/ak/code/bq-mp-inspector/customers/vipps/sql/de-dupe-gap-candidate-backfill.sql`
- `/Users/ak/code/bq-mp-inspector/customers/vipps/sql/gap-merge-key-check.sql`
- Context (cited by the SQL headers): `customers/vipps/reports/identity-merge-diagnosis.md`, `customers/vipps/reports/AUG-15-UPDATE.md`

Cast of characters: `prod` = Mixpanel project 2733481 (live, **Original ID Merge**), `candidate` =
4042347 (target, **Simplified ID Merge**), `mp_master_event` = BQ export of prod's raw events
(160.5B rows), `mp_identity_mappings_data_view` = BQ export of prod's identity/alias graph.

## Summary

The Vipps migration did **not** replay `$identify` / `$create_alias` / `$merge` into the simplified
project — those verbs were dropped at import time (server-side `$identify` is rejected outright by
Simplified ID Merge). Instead, all identity information was compiled into a **patch table of
synthetic dual-ID events**: one row per `(user_id, device_id)` pair, sent with `$user_id`,
`$device_id`, and a deterministic `$insert_id`, at a constant timestamp *before* the data range.
Simplified ID Merge then **retroactively re-clusters** already-ingested events (Mixpanel stamps
them `$is_reshuffled: true` + `$preshuffle_distinct_id`; compaction takes days).

The core insight: everything expressible in Simplified ID Merge is a **device→user link**. The
translation of the entire original-ID-merge identity graph is therefore "for every device that
appears on a real event, find the one numeric customer its cluster contains, and emit one
device→user pair." Anonymous-to-anonymous merges are **not expressible** and stay fragmented.
The decisive algorithmic step (v3's "cluster expansion") handles the fact that original-ID-merge
clusters are **stars whose head is frequently an anonymous ID** — linking only direct user↔alias
edges orphans every sibling of an anonymous head and left the reference funnel at 65.9% instead of
89.5% (prod reference: 90.5%).

## Key Facts

- **Patch shape:** synthetic events, columns `event_name` (constant, e.g. `identity_mapping_v3`),
  `time` (constant `2022-06-09 00:00:00`, one day *before* the data range so it never pollutes an
  analysis window), `user_id`, `device_id`, `insert_id = TO_HEX(MD5(CONCAT(uid,'|',dev)))`,
  plus provenance columns (`source_buckets`, `last_seen_at`). Loaded via Warehouse Connector with
  user_id→User ID, device_id→Device ID, insert_id→Insert ID.
- **Deterministic insert_id ⇒ idempotent replay.** Re-running the build produces identical
  insert_ids, so a full-table resend dedupes against itself. The "top-up before token flip"
  procedure is literally: re-run the same DDL, resend the whole table; only new pairs land.
- **`$device:` prefix must be stripped before sending.** Mixpanel's identity-mappings export
  stores anonymous IDs in canonical `distinct_id` form (`$device:UUID`). The `$device_id`
  ingestion field expects the RAW id — Mixpanel prepends `$device:` itself. Sending prefixed
  values silently creates phantom `$device:$device:UUID` clusters (70.7% of the original Vipps
  seed had this defect; Mixpanel accepts it without error).
- **One user per device, resolved in SQL, not by ingest order.** Mixpanel's device→user mapping is
  first-write-wins and concurrent workers make ingest order nondeterministic, so the conflict
  winner is chosen in-table: `QUALIFY ROW_NUMBER() OVER (PARTITION BY dev ORDER BY has_truth DESC,
  last_seen DESC, uid) = 1` — hard event evidence beats graph inference, then most-recent, then
  lexical uid for reproducibility.
- **Canonical id choice: always the real (numeric) user id, never the cluster head.** Re-keying
  events onto prod's anonymous canonical was tried on paper and rejected: events already carrying
  `$user_id` win over any patch, so that model collapses (41.6% funnel vs 89.5%).
- **Original-ID-merge graph shape (verified for Vipps):** star clusters, `resolved_distinct_id` =
  head, no multi-hop chains, unique alias keys, no dangling targets; the head is often anonymous;
  edges are stored in *either* orientation (device-as-canonical/user-as-alias occurs ~half the
  time), so both directions must be read.
- **Dropped `$identify` events are redundant with the alias graph.** Mining
  `$distinct_id_before_identity` out of 38.3M dropped `$identify` rows was verified to add nothing:
  the old project built its graph from those very events (sampled pairs all present, reverse
  orientation) — and reading `properties` would have blown the scan from ~13.5 TB to ~90 TB.
- **Event-table dedupe key:** `(insert_id, event_name, DATE(time))`, keep first by `time` via
  `ROW_NUMBER`, staged partition swap. Late-arriving events (event time > 2 days older than
  ingestion time) permanently duplicate under a MERGE whose target window is narrower than its
  source window — the cause of 160M duplicate rows in the materialized table.

## Details

### 1. The problem being patched (context from the diagnosis report)

- Prod (Original ID Merge) events export three id columns: `user_id`, `device_id`,
  `distinct_id`. `distinct_id` is the **old project's resolved person** — for web checkout traffic
  it frequently holds a known numeric customer even when `user_id` is NULL.
- The historical import's transform keyed rows on `user_id`, falling back to promoting an
  8-char `distinct_id` **only when both `user_id` and `device_id` were NULL**. Web checkout mints a
  fresh `device_id` per payment (ephemeral browser UUID appearing on exactly one event), so those
  rows imported anonymous, and the resolved customer in `distinct_id` was discarded → the flagship
  funnel read a hard 0% (the ephemeral UUID never appears on a second event).
- The import also dropped all `$identify` / `$merge` / `$create_alias` rows on the justification
  "every event carries resolved `$user_id`/`$device_id`" — true for app traffic (~90% of rows,
  which imported fine), false for web checkout. Note for the replay sprint: under **server-side
  identify** architectures, "resolved customer in `distinct_id` next to an ephemeral `device_id`"
  is the *normal shape*, not an anomaly.
- Remediation is a patch table, not a re-import: Simplified ID Merge resolves retroactively
  (empirically confirmed — the open question "query-time vs ingestion-time?" was answered by
  loading v2 and observing `$is_reshuffled: true` + `$preshuffle_distinct_id` stamped onto
  previously-ingested events, and funnel numbers moving).

### 2. v2 algorithm (`build-identity-mapping-v2.sql`) — superseded but instructive

Builds `vipps_prod_identity_mapping_v2` from four pair sources ("buckets"), unioned, deduped by
`GROUP BY uid, dev`:

- **A `events_dual_id`** — events where both `user_id` and `device_id` are present. Ground truth:
  the device genuinely belongs to the user.
- **B `events_lost_identity`** — `user_id IS NULL AND device_id IS NOT NULL` and `distinct_id` is
  itself a known user (`known_users` = every distinct non-null `user_id` seen in the source). This
  is "the broken bucket driving the 0% funnel": the import dropped the user but `distinct_id`
  still names it. Emits `(distinct_id, device_id)`.
- **C1 `mappings_fwd`** — old alias graph rows where the *canonical* side
  (`resolved_distinct_id`) is a known user: emits `(resolved_distinct_id, distinct_id-as-device)`.
- **C2 `mappings_rev`** — graph rows where the *alias* side is the known user (canonical is an
  anonymous device — this orientation is common): emits `(distinct_id, resolved_distinct_id-as-device)`.

Every device value is prefix-stripped `REGEXP_REPLACE(x, r'^\$device:', '')` at the source, and
the final WHERE re-guards `NOT STARTS_WITH(...)` on **both** sides. Other filters: uid/dev
non-null, non-empty, `uid != dev`.

Output row: `event_name = 'identity_mapping_v2'`, `time = TIMESTAMP '2022-06-09 00:00:00'`
(one day before the source data range starts 2022-06-10 — deliberately outside every analysis
window and trivially separable from the original seed's 2023-01-01 stamp),
`insert_id = TO_HEX(MD5(CONCAT(uid,'|',dev)))`, `source_buckets = STRING_AGG(DISTINCT bucket)`.

v2 shortcomings that v3 fixed:

1. **No conflict resolution** — 250,225 devices mapped to >1 user, leaving Mixpanel's
   first-write-wins to pick nondeterministically.
2. **No user-id validation** — the test record `1234` appears on 404 devices; shipping it
   collapses 404 people into one identity (irreversibly — see Gotchas).
3. **Pure-graph pairs (C1/C2) are not event-anchored** — 4.4M devices existed only in the graph.
   v3 dropped C1/C2 entirely (low cost: the original seed was graph-built, so those claims were
   already in the project).
4. **Bucket B only matched rows whose `distinct_id` *is* a user**; it never followed the graph for
   rows whose `distinct_id` is anonymous but *resolves* to a user. Measured: the mappings join
   recovers ~1.9× more pairs than B alone (83,853 vs 43,590 rows on a sample day).

Result after loading a reviewed v2 (35M pairs): funnel 0% → 15.9%. Proof of mechanism, not a fix.

### 3. v3 algorithm (`build-identity-mapping-v3.sql`) — the definitive design

CTE chain, in order:

1. **`src`** — `SELECT user_id, device_id, distinct_id, time FROM mp_master_event` over the
   partition range (`2022-06-10` → today).
2. **`known_users`** — distinct `user_id` matching `^[0-9]{8}$`. **Customer-specific rule:** all
   14,376,745 real vipps-prod users are exactly 8 digits (range 10000949–30900698); the only other
   numeric value is test record `1234`, which the regex excludes. 65,594 of 65,601 non-numeric
   "users" in the source `user_id` column are device-shaped UUIDs — exactly the id-management
   damage the migration removes, so they are filtered, not restored. (Merchantportal user ids are
   36-char UUIDs by design — this rule must NOT be reused there; it would delete 392,137 of
   392,138 rows.)
3. **`map`** — the old project's identity-mappings export: `(distinct_id [alias],
   resolved_distinct_id [cluster head])`. **Always read the view**
   (`mp_identity_mappings_data_view`), never the raw suffixed table — the export job
   (`identity-daily-bigquery-json`) drops and recreates the raw table daily at ~22:10 UTC and
   repoints the view; a hardcoded table name goes stale or vanishes.
4. **`canon`** — `alias → ANY_VALUE(head)` grouped by alias. Safe because the graph was verified:
   unique alias keys, **no multi-hop chains**, no dangling targets. (If a graph *did* have chains,
   this step would need transitive closure.)
5. **`members`** — every member of every cluster **including the head itself**, prefix-normalised:
   `(canonical, strip(alias)) UNION DISTINCT (canonical, strip(canonical))`.
6. **`cluster_user`** — for each cluster head, the numeric customer that owns the cluster:
   `MIN(member)` over members matching `^[0-9]{8}$`. MIN is an arbitrary-but-deterministic
   tiebreak for the rare cluster holding more than one numeric user.
7. **`resolver`** — **direct** alias↔user edges in *both orientations* (the graph often stores
   the DEVICE as canonical and the USER as the alias, so one direction misses roughly half):
   `(strip(alias), head)` where head is a known user, UNION `(strip(head), alias)` where alias is
   a known user; self-edges excluded.
8. **`pairs`** — four buckets, each carrying `bucket` (provenance), `has_truth` (bool), and
   `MAX(time) AS last_seen`:
   - **A `events_dual_id`** (`has_truth = TRUE`): event rows with a known 8-digit `user_id` and a
     `device_id`. **No event_name filter — the dropped `$identify` rows are mined here too**: any
     `$identify` that carried both ids contributes its pair like any other event.
   - **B `events_lost_identity`** (`has_truth = TRUE`): `user_id IS NULL`, `device_id` present,
     `distinct_id` is a known user → `(distinct_id, device_id)`.
   - **D `mappings_resolved`** (`has_truth = FALSE`): `user_id IS NULL`, `device_id` present, and
     the (prefix-stripped) `distinct_id` resolves to a user via a **direct** resolver edge →
     `(resolved_user, device_id)`. The graph answers only "which customer is this anonymous
     distinct_id?"; the device always comes from a real event row.
   - **E `cluster_expansion`** (`has_truth = FALSE`) — **the decisive addition**: for any event
     with a `device_id` whose `distinct_id` is an alias in the graph, join
     `canon` (alias→head) then `cluster_user` (head→numeric customer) and emit
     `(cluster's customer, device_id)`. This links a device to the customer **even when the
     cluster head itself is anonymous**. Note E has no `user_id IS NULL` filter — it applies to
     all rows with a device.
9. **`cleaned`** — `GROUP BY uid, dev`; aggregate `source_buckets = STRING_AGG(DISTINCT bucket)`,
   `has_truth = LOGICAL_OR(...)`, `last_seen = MAX(...)`; filters: non-null, non-empty,
   `uid != dev`, and `NOT STARTS_WITH(dev|uid, '$device:')` ("ingestion re-adds the prefix
   itself").
10. **`guarded`** — anti-join: exclude any pair whose `dev` is itself a known user id ("a
    device_id that is itself a known user id is corrupt source data, not a device").
11. **Final projection + conflict resolution** — same output shape as v2 (event_name
    `identity_mapping_v3`, constant pre-range timestamp, MD5 insert_id) plus `last_seen_at`, and:

    ```sql
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dev
      ORDER BY has_truth DESC, last_seen DESC, uid
    ) = 1
    ```

    One user per device. Hard evidence (a real dual-ID event) outranks graph-inferred; then
    most-recent wins; `uid` last makes the build reproducible. Rationale stated in the header:
    "Ingest order cannot be relied on — workers run concurrently — so the winner is decided here."

**Why cluster expansion matters (measured, `Landing Page Viewed → Payment Confirmed`, June 2026,
1-day window):** direct edges only (A+B+D) = 65.9%; + cluster expansion (A+B+D+E) = 91.8%
projected; prod UI reference = 90.5%; actual after load = **89.51%** (the projection method runs
~2.5pp hot vs Mixpanel's windowing — a known, stated bias). Prod's clusters are stars with
frequently-anonymous heads; direct edges link user→head but orphan every sibling of that head,
including all the ephemeral per-payment web device ids.

**Rejected alternative (verified before choosing):** key events onto prod's anonymous canonical
head. Reproduces prod at 93.0% *on paper* but is not patchable in practice — events already
carrying `$user_id` win over any patch, collapsing it to 41.6%.

**Graph trust, quantified:** of 32,246,328 devices with a real dual-ID event, the graph names a
conflicting user on **9**. Applying the graph to prod's own `distinct_id` reproduces prod's funnel
step 1 to within 37 people of 532,700 — i.e. the mappings export *is* prod's clustering, so
faithfully reproducing it also reproduces any over-merging prod contains.

**Build stats:** 41,128,461 pairs; distinct devices == total rows == unique insert_ids (one user
per device verified); 0 non-8-digit users; 0 prefixed devices; 13.49 TB scanned, ~53 min, ~$84.
Bucket `cluster_expansion` alone (reachable no other way): 1,372,737 pairs.

### 4. How `$identify` / `$create_alias` / `$merge` were actually handled

**They were never replayed as identity verbs.** The translation is entirely structural:

- The **identity-mappings export** (`mp_identity_mappings_data_view`) already encodes the net
  effect of every historical `$identify`/`$create_alias`/`$merge` as alias→canonical edges. The
  old project built that graph *from* those events, so the graph is the compiled form of the verbs.
  Verified redundant to mine the 38.3M dropped `$identify` events for
  `$distinct_id_before_identity`: sampled pairs were all already in the graph (reverse
  orientation), and reading `properties` would have raised the scan from ~13.5 TB to ~90 TB.
- Where `$identify` rows do add signal, it is as **ordinary dual-ID events** — bucket A carries no
  event_name filter precisely so dual-ID `$identify` rows contribute their (user, device) pair.
- **Server-side `$identify` is rejected outright by Simplified ID Merge** (per the Aug-15 update).
  The forward-looking prescription for live traffic is the same as the historical patch: emit a
  normal event carrying both `$device_id` and `$user_id` at the moment you would have fired
  `$identify`.
- **`$merge` semantics (anon↔anon) have no simplified equivalent.** "Simplified ID Merge can only
  link a device to a user, not two devices to each other." Anonymous-to-anonymous consolidation
  stays fragmented (for Vipps this was measured to contribute nothing to the target funnel, but
  it is a real expressiveness gap in general).

### 5. How events get "patched"

No historical event row is rewritten. The patch is purely additive:

- Send synthetic events with `$user_id` = the chosen customer id and `$device_id` = the **raw,
  prefix-stripped** device UUID. Mixpanel forms `$device:UUID` internally and merges it into the
  user's cluster.
- Simplified ID Merge then retroactively re-attributes the device's existing anonymous events.
  Monitoring signal: re-attributed events gain `$is_reshuffled: true` and
  `$preshuffle_distinct_id`. Compaction is slow (days; v2's ran ~5 days and was still climbing).
  Watch the reshuffle rate *within the affected event family* — project-wide it is diluted by the
  ~90% of rows that were never misattributed.
- Events that already carry `$user_id` are untouchable — their user assignment wins over any
  patch. (This is both a safety property and the reason the "re-key to anonymous canonical" model
  fails.)

### 6. Dedupe and idempotency

Three distinct dedupe mechanisms appear across the files:

1. **Patch-table idempotency** (v2/v3): `insert_id = TO_HEX(MD5(CONCAT(uid, '|', dev)))`.
   Deterministic across rebuilds → any pair already live in the project dedupes against itself on
   resend. This is what makes "re-run the DDL unchanged just before cutover and resend everything"
   a safe top-up procedure.
2. **Event-table dedupe** (`de-dupe-gap-candidate-backfill.sql`), for the materialized table that
   feeds cutover: dedupe key is `(insert_id, event_name, DATE(time))` — matching Mixpanel's own
   event-dedupe identity. Procedure is a staged partition swap:
   - Step 0: find affected days (`COUNT(*) - COUNT(DISTINCT insert_id|event_name)` per day > 0).
   - Step 1: stage a deduped copy of only the affected partitions
     (`ROW_NUMBER() OVER (PARTITION BY insert_id, event_name, DATE(time) ORDER BY time) = 1`),
     partitioned by `DATE(time)`, clustered by `(event_name, insert_id)`.
   - Step 2: verify counts BEFORE deleting (after == before − surplus).
   - Step 3: `DELETE` the affected partitions from the live table, `INSERT` from the staged copy.
   - Step 4: verify zero remaining duplicate keys, then drop the temp table.
3. **MERGE-window probe** (`gap-merge-key-check.sql`): a cheap 3-partition query that checks
   (a) whether `(insert_id, event_name, DATE(time))` keys are unique within a load window (a
   MERGE fails otherwise) and (b) how many rows are **late arrivals** — event `time` more than
   2 (and 5) days older than ingestion `_PARTITIONTIME`. Late rows are the ones a MERGE with a
   `T.time >= CURRENT_TIMESTAMP - 2 DAY` target predicate re-INSERTs instead of matching →
   **permanent duplicates**. This diagnosed the live defect: daily merge read a 3-day source
   window but matched a 2-day target window → 160,216,433 duplicate rows, compounding daily. Fix
   ordering matters: **widen the target window first (4 days), then dedupe**, or the next
   scheduled run recreates the duplicates.

### 7. Ordering / windowing assumptions (complete list)

- **Patch timestamp is constant and pre-range** (`2022-06-09`, one day before the data window):
  never appears in analysis windows; trivially separable from other seeds by timestamp alone.
- **Retroactive resolution is real**: patches sent *after* events still re-cluster those events
  (empirically proven on v2; this was an explicit open question until then).
- **Ingest order is nondeterministic** (concurrent workers) and **device→user is
  first-write-wins**, so any "which user wins this device" decision must be made in SQL before
  sending — never left to send order.
- **`$user_id` on an event is final** — a patch cannot re-assign an event that already carries one.
- **Original ID Merge caps clusters at 500 anonymous IDs** (prod's graph hard-stops at 501
  members: 14 clusters pinned there, none beyond). Simplified has no cap — v3 emits customers with
  up to 2,730 devices. The cap only binds identities merged *via aliasing*; dual-ID events need no
  alias slot. Its real-world impact was 14 clusters, not the thousands first estimated.
- **Source partition filter** uses `_PARTITIONTIME`, event dedupe uses event `time` — the two
  clocks differ (that gap is precisely what the late-arrival probe measures).
- **The mappings export is a moving target** (rebuilt daily); comparisons between artifacts built
  on different days will show phantom drift (a 12.4M-of-21.3M "mismatch" turned out to be Jul-17
  vs Aug-13 snapshots of the same table).

## Gotchas / Limits

- **NEVER send `$device:`-prefixed values in `$device_id`.** The mappings export stores anonymous
  ids as `$device:UUID` (correct for that table); ingestion expects the raw UUID and prepends the
  prefix itself. Mixpanel accepts `$device:$device:UUID` **without error** — the failure is
  silent phantom clusters. Strip at every source AND guard at output (belt and braces, both
  sides of the pair).
- **Identity damage is irreversible.** Per Mixpanel Support (9 Jul): clusters persist even after
  the underlying events are deleted. A bad merge (e.g. the `1234` test account absorbing 310
  devices) cannot be undone; a badly-seeded project must be abandoned. Hence the standing
  practice: **project the outcome in SQL before loading anything** — a v3 draft was rejected
  pre-load this way, the difference between shipping 65.9% and 89.5%.
- **The 8-digit numeric user rule is Vipps-prod-specific.** It is what makes `known_users`,
  `cluster_user`, and the device-side guard cheap and safe. Any generic pipeline needs a
  per-project "what does a real user id look like" predicate (merchantportal = 36-char UUIDs;
  the numeric rule there deletes everything).
- **Known residual defect in v3 bucket E** (documented, deliberately not fixed pre-cutover):
  `JOIN canon c ON c.alias = s.distinct_id` traverses only the **alias** side. An event whose
  `distinct_id` is itself a **cluster head** matches nothing and its device is never linked.
  Suspected cause of the residual ~1.2% (7,300 of 609,214 people). A general implementation
  should look up `distinct_id` as head OR alias. Second suspect: the one-user-per-device QUALIFY
  can keep bucket-A's user X while the event's cluster names user Y, leaving the two split.
- **~19% of Vipps' pre-login web rows are structurally unrecoverable** — no derivable customer
  exists anywhere in the source. Set that expectation up front; no patch reaches them.
- **Anon-to-anon consolidation is inexpressible** in Simplified ID Merge. Devices that were
  chained together in the original project without any user in the cluster stay separate people.
- **`ANY_VALUE` / `MIN` tiebreaks** assume a well-formed star graph (verified here: no multi-hop
  chains, unique alias keys). A graph with chains needs transitive closure before `canon`;
  a cluster with multiple numeric users gets an arbitrary-but-deterministic owner (`MIN`).
- **BigQuery does not materialize CTEs**: v2's `src` was referenced 3× → three full 160.5B-row
  scans (807.9B records read; billed bytes unaffected, slot time not). For big builds, stage the
  user set once and chunk the main pass with `INSERT INTO`.
- **Projection arithmetic runs ~2.5pp hot** versus Mixpanel's real funnel windowing — calibrate
  any SQL-side funnel simulation against a UI reference before trusting absolute numbers.
- **"Matches the source project" may be the wrong success criterion**: v3 reproduces prod's
  clustering to within 37 people out of 532,700 — *including any over-merges prod contains*.
  Someone must decide whether parity or correctness is the goal.
- **A mechanism being real is not evidence it explains your observation.** Two confident causal
  claims in this workstream (500-cap impact; anon-to-anon loss explaining the residual) were both
  disproved by measurement. Numbers derived under one identity model do not transfer to another.

## Open Questions

- **Generalizing `known_users`:** Vipps had a crisp regex for "real user id". A reusable
  original→simplified pipeline needs this as a required, per-project predicate (regex/allowlist/
  denylist incl. test accounts like `1234`) — what should the mixpanel-import option surface be?
- **Cluster-head lookup symmetry:** the documented bucket-E fix (also match events whose
  `distinct_id` is a cluster head) was never built (a v4 was deliberately skipped). A fresh
  implementation should include it from the start — worth validating on real data that it closes
  the residual as hypothesized.
- **Input source differences:** Vipps worked from a BigQuery event export + the
  identity-mappings BQ export. The mixpanel-import replay pipeline will presumably work from
  `/export` API data (raw `$identify`/`$merge`/`$create_alias` events) — where the graph must be
  *reconstructed* from the verbs rather than read from the mappings export. The Vipps evidence
  that "the graph == the compiled verbs" (sampled pairs all present) supports reconstructing it
  from `$identify` (`$identified_id`/`$anon_id` a.k.a. `distinct_id_before_identity`), `$merge`
  (`$distinct_ids` pair), and `$create_alias` (`alias`/`distinct_id`) — but exact export-side
  property names for each verb were not covered by these SQL files and need separate research.
- **Delivery mechanism:** Vipps loaded patches via Warehouse Connector. Via mixpanel-import the
  equivalent is `/import` with `event`, `properties.$user_id`, `properties.$device_id`,
  `properties.$insert_id`, `properties.time` — confirm nothing about WC's field mapping (e.g.
  reserved-property normalization) differs from raw `/import` for this shape.
- **Retroactivity SLAs:** compaction/reshuffle took ~5 days for a 35M-pair load. Is there any
  documented bound, and does load order (patches before vs after event backfill) change the
  outcome or only the timing? (Vipps proved after-the-fact patching works; before-the-fact was
  the original seed design.)
- **Are `$is_reshuffled` / `$preshuffle_distinct_id` stable, documented properties** safe to
  build validation tooling on, or internal implementation details?
