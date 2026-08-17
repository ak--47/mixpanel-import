# Vipps Migration — Business Context for the Original→Simplified Replay Pipeline

Research notes distilled from four reports in `bq-mp-inspector/customers/vipps/reports/`
(`source-validation.md` 2026-07-22, `gap-backfill-query-review.md` 2026-08-11,
`identity-merge-diagnosis.md` 2026-08-13→15, `AUG-15-UPDATE.md` 2026-08-15).
This is the real-world case study the sprint exists to prevent: a 155.8B-event
original-ID-merge → simplified-ID-merge migration that **dropped** the identity verbs
(`$identify` / `$merge` / `$create_alias`) and paid for it with a hard-0% funnel,
three remediation patch tables, ~$290 of BigQuery spend, and a near-miss on cutover.

## Summary

Vipps (Norwegian payments app) migrated three Mixpanel projects into a new simplified-ID-merge
project (`candidate` = 4042347) because the original project (`prod` = 2733481) had hit the
original-ID-merge 500-anonymous-IDs-per-cluster limit and accumulated identity damage. The
migration transform **dropped `$identify`/`$merge`/`$create_alias`** on the explicit justification
that "every event carries resolved `$user_id`/`$device_id`" — true for ~90% of traffic (app,
`user_id`-keyed) and catastrophically false for web checkout, where the JS SDK mints a **fresh
`device_id` on every payment-page open** and the resolved customer lives only in `distinct_id`
(a server-side-`$identify` architecture Mixpanel itself prescribed in 2022 for their
iframe-embedded checkout). Result: the flagship funnel `Landing Page Viewed → Payment Confirmed`
read **exactly 0%** in the new project vs 90.5% in prod, because each ephemeral device UUID
appears on no second event.

Remediation took three generations of a BigQuery-built device→user pair table synced via
Warehouse Connector as identity-carrying events. v2 (direct `distinct_id`-is-a-user matches only)
recovered 15.9%. A direct-graph-edges v3 was **projected in BQ and rejected before loading** — it
would have landed at 65.9%, because original-ID-merge clusters are *stars whose head is often an
anonymous ID*; direct edges link user→head and orphan every sibling device. v3 **with cluster
expansion** (emit a device→user pair for *every* anonymous member of every user-bearing cluster)
landed the funnel at **89.51%** vs prod's 90.5%. The residual ~1.2% splits into a structural
floor (~19% of pre-login web rows have no derivable customer anywhere in the source) and a
suspected SQL asymmetry (the alias-side-only join misses events whose `distinct_id` is itself a
cluster head).

The decisive lessons for a streaming replay pipeline: (1) naive per-event translation of identity
verbs reproduces the rejected 65.9% "direct edges" model — proper translation requires the whole
graph (cluster expansion), which is at odds with a pure 1:1 streaming transform; (2) simplified
ID merge is **first-write-wins and irreversible**, so identity payloads must be conflict-resolved
deterministically *before* sending and validated by **projection before loading**; (3) validation
must compare against the *target project*, not source-to-source, and needs an agreed definition of
"correct" — parity with an original-ID-merge project may inherit that project's own defects.

## Key Facts

- **Projects:** `prod` = 2733481 (original ID merge, live source), `abandoned` = 3994060 (first
  attempt, ruined by irreversible identity damage — "do not use"), `candidate` = 4042347
  (simplified ID merge, target, EU). Throwaway test project: 4046514.
- **Scale:** vipps-prod 155,756,113,740 rows / 89.5 TB (1,503 day partitions, 2022-06-10 onward);
  merchantportal 825.6M rows; developerdocs 3.5M rows. Unload cost ~$562; remediation ~$290.
- **Identity-verb volume:** ~0.039–0.06% of rows (17 of 43,640 in one sampled file; 72,663 on
  2025-12-15; 38.3M `$identify` events total mined later).
- **The rejection error** (live, from a simplified project):
  `"identity events are not allowed when project is using simplified identity management"`.
- **The kill shot:** `Landing Page Viewed` 2025-12-15 had 56,477 events across 56,476 distinct
  `device_id`s — one device per event — and `distinct_id = device_id` on **zero** rows. Funnel
  keyed on `device_id`: 0.0%. Keyed on old resolved `distinct_id`: 59.9% (BQ arithmetic).
- **Patch generations:** seed (21.3M pairs, 70.7% `$device:`-prefix-corrupted) → v2_reviewed
  (35.0M pairs → 15.9%) → v3 direct-edges (39.8M pairs, **rejected by projection at 65.9%**) →
  v3 + cluster expansion (41,128,461 pairs → **89.51%** actual vs prod 90.5%).
- **Cluster expansion contribution:** 1,372,737 pairs reachable *no other way* than expansion;
  the model difference is 65.9% → 91.8% projected.
- **Simplified-ID-merge semantics observed:** device→user is **first-write-wins**; merges are
  **irreversible** (Mixpanel Support, 9 July: clusters persist even after underlying events are
  deleted); resolution is retroactive (loading pairs *after* events re-attributes them), with
  compaction lag of ~days (~5 days for v2 to reach ~96% of ceiling); re-attributed events are
  stamped `$is_reshuffled: true` + `$preshuffle_distinct_id`.
- **Simplified cannot express anon↔anon links.** A patch/replay can only say "device D belongs
  to user U". Anon-to-anon consolidation from the original graph is inexpressible (though in the
  vipps funnel it measurably contributed **zero** — 0 of 609,214 June LPV IDs sat in anon-only
  clusters; prod holds 15,371 anon-only clusters project-wide).
- **Original-ID-merge cap:** 500 anonymous IDs per cluster (hard stop at 501 members incl. the
  known ID; 14 clusters pinned there in prod, none above). Simplified has no cap — v3 produced
  clusters up to 2,730 devices per customer (211 customers with 501–1,000; 44 above 1,000).
- **`$device:` prefix trap:** Mixpanel's identity-mappings export stores anonymous IDs in
  canonical `distinct_id` form (`$device:XXX`). Feeding that into the `$device_id` ingestion field
  creates phantom `$device:$device:XXX` clusters, **accepted without error**. 15,064,550 of
  21,306,546 seed pairs (70.7%) were corrupted this way; stripping would have raised match
  coverage +66% on the sampled day.
- **Dead-branch property-name bug:** the real event property is `$distinct_id_before_identity`
  (dollar-prefixed); the transform read the bare `distinct_id_before_identity` → always NULL,
  dead for all 155.8B rows. Deliberately left unfixed mid-migration for consistency.
- **Conflict census:** 250,225 of 36.7M devices (0.68%) mapped to >1 user in v2. v3 policy: one
  user per device, hard evidence (real dual-ID event) outranks graph-inferred, then most-recent
  wins — resolved in SQL because concurrent-worker ingest order is nondeterministic.
- **Idempotent top-ups:** deterministic `$insert_id`s on pair events make re-sending the whole
  table a no-op for existing pairs — the pre-cutover top-up is literally "re-run the same build
  and re-sync" (~$84, 53 min).
- **Projection accuracy:** BQ funnel projection predicted step 1 = 539,679 / 91.8% with a stated
  ~2.5pp hot bias vs Mixpanel windowing; actual 540,000 / 89.51% — within 321 people.
- **Graph fidelity:** applying prod's own graph to prod's `distinct_id`s reproduces prod's UI
  step-1 count within **37 of 532,700**. Of 32,246,328 devices with a real dual-ID event, the
  graph names a conflicting user on **9**.
- **Live-tracking consequence:** server-side `$identify` is rejected outright by simplified, so
  post-cutover the client must emit a **normal event carrying both `$device_id` and `$user_id`**
  at the moment it used to fire `$identify`. (Vipps already had this pattern on three events —
  `Ecom Payment Decline Payment Initiated` et al., 100% dual-ID.)

## Details

### 1. The architecture that made dropping identity verbs fatal

Vipps Checkout runs in an **iframe as a third party on merchant domains**. The browser can
neither persist identity nor learn who the payer is, so in Aug 2022 Mixpanel (Merv) prescribed
**server-side `$identify`**: the backend links the ephemeral browser `$device_id` to the customer
after payment resolution. Under that architecture, *a resolved customer in `distinct_id` sitting
next to an ephemeral `device_id` on the same row is the normal shape*, not an anomaly.

Two independent decisions each assumed the opposite:

1. **The transform's identity CASE** (in `vipps_prod_transformed`):
   ```sql
   WHEN main.user_id IS NOT NULL OR main.device_id IS NOT NULL THEN main.user_id
   ```
   Any row with a `device_id` short-circuits here and returns the (NULL) `user_id`; the 8-char
   `distinct_id` promotion below never runs. Fine for app traffic (~90% of rows carry `user_id`).
   Fatal for web checkout, where every row has a fresh `device_id` and the customer is only in
   `distinct_id`. The corrected gate promotes on `user_id IS NULL` alone, and falls back
   `device_id` → `REGEXP_REPLACE(distinct_id, r'^\$device:', '')`.

2. **Dropping the identity verbs.** `source-validation.md` (Jul 22) found `$identify` rejected by
   the target and added the drop, reasoning "this loses nothing — identity seeding is already done
   and every event carries resolved `$user_id`/`$device_id`". The assumption held for app traffic
   and failed for web checkout. The diagnosis report explicitly names this drop as a contributing
   factor (72,663 identity rows dropped on 2025-12-15 alone).

The failure was invisible to every check that was run: event counts reconciled exactly; the
8-char promotion validated; the in-app funnel `Home Viewed → Payment Confirmed` matched prod
(80.23% vs 80.43%). Only a funnel whose *first step is anonymous web traffic* exposed it — and it
exposed it as exactly 0%, not "low", because a per-payment UUID appears on no other event so no
person can ever hold both steps. Secondary signature: people inflation (37,346 imported "people"
at step 1 vs 28,720 real ones) and total:unique = 1:1 on every landing-page event in every month.

Loss was concentrated, not uniform: 0.2–0.7% of the anonymous bucket per sampled day for years,
then a step change to 3.0% (34,943 identities lost on 2026-05-15) when the web landing-page
family rolled out in 2025-11/12. **Per-event-family analysis, not project-wide averages, found
it.**

### 2. Why naive streaming translation of identity verbs is NOT enough — the 65.9% lesson

This is the single most important finding for the sprint. The remediation team effectively
"replayed identity" three ways, and the deltas are measured:

| Identity model | Funnel (Jun 2026, 1-day window) | Interpretation for a replay pipeline |
|---|---|---|
| Drop identity verbs | 0.0% | what the migration did |
| v2: link device→user only where `distinct_id` **is** literally the user | 15.9% | translating only `$identify(user, anon)` events whose target is a real user id |
| v3 direct graph edges only | 65.9% (projected; **rejected before load**) | ≈ **1:1 streaming translation of each identity verb** |
| v3 + **cluster expansion** | 91.8% projected / **89.51% actual** | requires the whole graph, transitively closed |
| prod reference | 90.5% | original-ID-merge UI |

Why direct edges fail: original-ID-merge clusters are **stars whose head is frequently an
anonymous ID**, with the customer's numeric user id as just another member. A stream of verbs
produces edges like `alias(anon_sibling → anon_head)` and `identify(user ↔ anon_head)`. In
simplified terms you can send `(device=head, user=U)` — but `alias(anon_sibling → anon_head)` is
**anon→anon and inexpressible**; the sibling (including every ephemeral payment device) stays
orphaned unless you *resolve the graph first* and emit `(device=anon_sibling, user=U)` for every
sibling. That resolution is a transitive closure over the full event/graph history — inherently a
two-pass (or stateful) computation, not a stateless per-record transform.

Implication for the mixpanel-import pipeline: an original→simplified replay needs either
(a) a **graph-building pass** over the identity verbs (and/or the identity-mappings export)
that computes cluster membership, picks one user per cluster, and emits a device→user pair per
anonymous member — then streams events; or (b) an explicit documented mode that does 1:1 verb
translation with the *measured expectation* that star-shaped anonymous-headed clusters will
under-merge (Vipps: 65.9% vs 89.51%). If events are replayed in chronological order and the
target's own transitive resolution could chain `(D_sibling→?)`, it still can't: simplified never
links two anonymous IDs, so chronology does not rescue model (b).

Also verified redundant: mining the 38.3M dropped `$identify` events for
`$distinct_id_before_identity` added nothing *in this case* because the old project's graph was
built from those very events (4 sampled pairs all present, reverse orientation) — and reading
`properties` blows the scan from ~13.5 TB to ~90 TB. For a replay pipeline without a graph
export, though, the verbs themselves are the graph source; this equivalence is the point.

### 3. Simplified-ID-merge mechanics, empirically established

- **Rejection is hard and per-event**: identity verbs fail with
  `"identity events are not allowed when project is using simplified identity management"` and
  count as skipped rows (3 of 5,000 in the sample import; 27 of 356,492 in the GCS-shard smoke).
- **Linking mechanism**: any event carrying both `$device_id` and `$user_id` links them. The
  patch tables were synced as ordinary events (`identity_mapping_v2` etc.) with mapped columns
  `user_id → User ID`, `device_id → Device ID`, `insert_id → Insert ID`.
- **Retroactive**: pairs loaded *after* the events re-attribute them (open question in the
  diagnosis — "query time vs ingestion time" — settled by v2's observed effect). Re-attributed
  events get `$is_reshuffled: true` and `$preshuffle_distinct_id`. Compaction is slow: ~5 days
  for v2 to reach ~96% of its ceiling; numbers keep climbing after load.
- **First-write-wins, per device**: a device's first user claim sticks. Hence: conflicts must be
  resolved before sending (v3: hard-evidence-outranks-graph, then most-recent; 0.68% of devices
  were contested); ingest concurrency makes "send in the right order" unreliable as a policy.
- **Irreversible**: merges can't be undone even by deleting events (Support, 9 July). This
  killed attempt 1 (project 3994060 abandoned) and is why v3 was *projected and rebuilt* rather
  than loaded on the first attempt. It also means a test account (`user_id = 1234`, 310 devices
  merged) is permanent damage.
- **No anon↔anon edges, no cluster-size cap** (vs original's 500-anon cap).
- **Deterministic `$insert_id` ⇒ idempotent identity loads**: resending the whole pair table is
  a no-op for existing pairs; only new ones land. This is the top-up mechanism before token flip.

### 4. The `$device:` prefix / phantom-cluster failure

The seed was built largely from Mixpanel's **identity-mappings export**
(`mp_identity_mappings_data_*`), whose anonymous IDs are stored in canonical `distinct_id` form —
literally `$device:ABC`. Routing that column into the `$device_id` ingestion field produced
`$device:$device:ABC` phantom IDs matching no real event. Three things made it silent:

1. the source column is named `distinct_id`, giving no cue it's pre-formatted;
2. 5.5M of the values are bare UUIDs (no prefix), so spot checks pass;
3. **Mixpanel accepts `$device:$device:ABC` as a valid device ID without error.**

62.5% of the merchantportal seed had the same bug (245,142 of 392,138 pairs). Every subsequent
patch build asserts `device_id`s with `$device:` prefix = 0 as a pre-load check, and strips via
`REGEXP_REPLACE(device_id, r'^\$device:', '')`.

Pipeline rules to encode: always strip a leading `$device:` (once) from anything destined for
`$device_id`; flag double prefixes; treat identity-mappings-export data as canonical-form,
event-stream data as raw-form.

### 5. The identity-mappings export is a moving target

`mp_identity_mappings_data_<suffix>` is **dropped and recreated daily** (~22:10 UTC, 156 runs
observed); only the view `mp_identity_mappings_data_view` is stable. A hardcoded suffix goes
stale or vanishes. Two practical consequences: (a) always reference the view; (b) two artifacts
built from different days' snapshots will disagree (the seed's 12.4M/21.3M "match rate" against a
27-days-later snapshot was drift, not a defect). Graph structure verified there: unique alias
keys, 0 multi-hop chains, 0 dangling targets — i.e. the export is already flattened alias→canonical
(one hop), which is what makes cluster expansion a group-by rather than an iterative closure.

### 6. Residual gap anatomy (what even a perfect patch cannot fix, and what a v4 could)

Final state: candidate 540,000 → 483,300 (89.51%) vs prod 532,700 → 482,100 (90.5%). Candidate
has *more conversions* than prod; the whole gap is a fatter step-1 denominator of unconsolidated
**non-converters**.

- **Structural floor:** ~19.3% of pre-login web rows have no derivable customer anywhere in the
  source (measured on Landing Page family 2026-06-15: 31.9% fixed by v2 + 48.9% added by v3 +
  19.3% unrecoverable). Set this expectation with the customer *up front*.
- **Anon-to-anon loss:** real in general, measured **zero** for this funnel (0 of 609,214 LPV
  IDs in anon-only clusters). A mechanism being real ≠ it explains your observation.
- **Suspected v4-able defect (~1.2%, 7,300 people):** v3's bucket E joins
  `canon.alias = event.distinct_id` — alias side only. An event whose `distinct_id` is itself a
  **cluster head** matches nothing. Lesson for any graph-resolution code: **look up an ID on
  both the alias side and the head side of the mapping.** Second candidate: the
  one-user-per-device QUALIFY keeping bucket-A's user X while the cluster points at user Y.
  Cheap diagnostic (not run): per LPV distinct_id, compare prod's canonical vs assigned user,
  bucket disagreements by head-vs-alias. Deliberately not chased before cutover — poor value.
- **500-cap devices:** a footnote — 14 clusters project-wide at the cap. The retracted 77k-session
  claim is a class-of-error warning: *a number derived from one identity model does not transfer
  to another without checking* (raw dual-ID event counts vs alias-graph membership are different
  quantities).

### 7. Validation techniques worth building into the pipeline (ranked)

1. **Project before loading.** Compute the target-side funnel/metric against the *proposed*
   identity mapping in SQL/local computation before sending anything, because identity damage is
   irreversible. This is what caught the 65.9% build. Accuracy demonstrated: step 1 within 321 of
   540,000; rate exact once a known ~2.5pp hot bias (BQ arithmetic vs Mixpanel funnel windowing)
   is subtracted. A replay pipeline should ship a "dry-run resolve" mode: apply the computed
   device→user map to the event stream locally and report per-funnel-step people counts.
2. **Pick a cross-identity funnel as the acceptance metric** — first step anonymous, second step
   identified (`Landing Page Viewed → Payment Confirmed`). In-app `user_id`-keyed funnels will
   pass even when identity translation is completely broken.
3. **Total-vs-unique ratio per event.** total:unique == 1:1 on a high-volume event is the
   fingerprint of ephemeral-device keying / identity loss. Post-fix it should drop visibly
   (0.786–0.850 for LPV after v2). Cheap to compute on both sides.
4. **`$is_reshuffled` rate *within the affected event family***, never project-wide. Vipps
   expectations: ~81% ceiling on the landing-page family (~19% structurally unreachable), while
   project-wide can't exceed ~10% because ~90% of the corpus was never misattributed. Compaction
   takes days — schedule the measurement, don't read it immediately after load.
5. **Reconcile against the target project, not source-to-source.** The 392,138 extra
   `identity_resolution` events (residue of a partial uncleaned-table run) were invisible to a
   BQ-to-BQ parity check. Expected deltas to whitelist: `$insert_id` dedup at ingest + dropped/
   translated identity verbs (~0.06%). The shard-level pattern that worked: Mixpanel count ==
   shard rows − source-duplicate `$insert_id`s, exact.
6. **Graph self-fidelity check:** apply the source graph to the source's own IDs and reproduce
   the source UI's numbers (37/532,700). Proves the graph *is* the clustering before using it to
   generate pairs.
7. **Conflict census before sending:** count devices claiming >1 user (0.68% here) and force an
   explicit policy; count devices where graph and dual-ID events disagree (9 of 32.2M here).
8. **ID-shape guards, per project:** vipps user ids are exactly 8-digit numerics — 65,594 of
   65,601 non-numeric "users" were device-shaped UUIDs polluting the `user_id` column and were
   excluded; a device_id that is itself a known user id is corrupt and excluded. **But the rule
   is not portable:** merchantportal user IDs are 36-char UUIDs by design; the numeric filter
   would have deleted 392,137 of 392,138 rows. Shape rules must be configurable, never baked in.
9. **Test/degenerate account detection:** `user_id = 1234` merged 310 devices irreversibly;
   hot-shard account `19805177` previously diagnosed. Pre-scan for IDs with pathological device
   counts and non-conforming shapes before any identity send.
10. **UI-search blind spots:** all seed events were stamped `2023-01-01`, 3.5 years outside any
    default query window — "missing" data that was merely unfindable. Validation queries need
    explicit full-range windows; export-API pulls beat UI search for existence checks.
11. **Define "correct" before validating.** Nobody had stated whether parity with prod was the
    goal — and prod's own numbers carry the defects the migration exists to fix (v3 reproduces
    prod's clustering *including its errors*; if prod over-merges, candidate inherits it). This
    became a named blocking action item. A replay pipeline's docs should force this conversation.
12. **Measure every causal claim.** Two confident, written-down claims (500-cap impact; residual
    gap = anon-to-anon loss) cited real mechanisms and were both disproved by measurement.

### 8. Operational failure modes from the gap/backfill review (steady-state after replay)

The migration doesn't end at the historical load — a daily catch-up feed ran until cutover, and
its review surfaced generic traps:

- **Relative time windows vs midnight-aligned partitions:** `CURRENT_TIMESTAMP() − 19 days`
  evaluated at 04:23 silently excludes the oldest day's partition; the window also drifts with
  every day of delay and isn't reproducible. Pin absolute boundaries
  (`>= TIMESTAMP('2026-07-22')`). The unpinned version silently dropped 387,187,508 rows.
- **Source window wider than target window ⇒ duplicate explosion:** the daily MERGE read a 3-day
  source but matched a 2-day target; rows aged 2–3 days re-inserted — 160,216,433 duplicates
  compounding daily *in the table feeding cutover*. Target match window must exceed source window
  (+ timezone offset). Fix order matters: change the schedule first, then dedupe, or the next
  05:00 run recreates them.
- **Non-unique merge keys:** 0.37% duplicate `(insert_id, event_name, DATE(time))` keys — harmless
  at Mixpanel ingest (it dedupes on `$insert_id`) but fatal to a BQ MERGE. Dedupe with
  `QUALIFY ROW_NUMBER()`.
- **Identity verbs must be filtered on every path.** The snowcat transform dropped them, but the
  gap was considered for Warehouse Connector, which has **no transform layer** — the verbs would
  land as ordinary events named `$identify`. Every ingress path into a simplified project needs
  the filter/translation, not just the main one.
- **Property discontinuities:** adding `user_type = "user"` to gap rows when 155.8B historical
  rows lack the property means `user_type = "user"` filters silently exclude all history; the
  spanning filter is `user_type != "merchant"`. Kept deliberately, but documented. Generic
  lesson: mid-migration schema changes create silent filter cliffs.
- **Deliberate non-fixes:** the dead `distinct_id_before_identity` (bare, not `$`-prefixed)
  branch was left broken because fixing it mid-migration would make gap rows resolve identity
  *differently from history*. Consistency beats correctness mid-stream.
- **Skipped scheduled runs are permanent loss** without a watermark; widen windows or drive from
  a stored watermark.

### 9. Live-tracking (post-cutover) implications

Server-side `$identify` is rejected by simplified, so the *same defect reappears on live traffic
day one* unless the client is reshaped: emit a normal event carrying both `$device_id` and
`$user_id` at the point that used to fire `$identify`. Vipps already does this on three events
(100% dual-ID). This was the longest-lead-time blocking item and was unowned at report time. A
replay pipeline solves history only; the sprint's docs should say so loudly.

### 10. What the customer cared about (priorities as revealed)

1. **Funnel conversion parity** on the flagship checkout funnel — the 0% was the escalation.
2. **A cutover date** (token switch 17–20 Aug, cutover 24 Aug) — remediation value was judged
   against the calendar (the 1.2% residual was explicitly not chased for this reason).
3. **Irreversibility risk** — one project already abandoned; every identity send treated as
   permanent.
4. **Not cost** — ~$290 remediation + ~$562 unload were noted, never a constraint. Engineering
   judgment ("recommend skip", "poor value") was framed in people-and-days, with dollar figures
   as seasoning.
5. **An eventual definition of "correct"** — surfaced late, became a blocker. The reference
   baseline (prod) had no established claim to being right.

## Gotchas / Limits

- **A 1:1 streaming verb translation is the measured 65.9% model.** Cluster expansion — which
  needs global graph state — is worth ~24 points of funnel conversion on real data. Any
  pipeline design must either include a graph pass or document this ceiling honestly.
- **Simplified cannot express anon↔anon.** `$create_alias`/`$merge` between two anonymous IDs has
  no translation; only device→user pairs exist. Budget an "inexpressible" counter and report it.
- **First-write-wins + irreversibility** means a wrong pair sent early beats a right pair sent
  late, forever. Conflict resolution must happen pre-send, deterministically (ingest order under
  concurrent workers is not controllable).
- **Mixpanel silently accepts garbage device IDs** (`$device:$device:X`), so prefix bugs produce
  no errors, only phantom clusters. Assert prefix-free before sending.
- **The identity-mappings export is canonical-form and rebuilt daily** — `$device:`-prefixed
  values are *correct there*; the raw suffixed table is dropped/recreated every ~24h.
- **Alias-side-only graph lookups miss cluster heads** (the suspected 1.2% residual). Resolve IDs
  against both sides of the mapping.
- **ID-shape rules are per-project** (8-digit numeric for vipps, 36-char UUID for merchantportal);
  hardcoding one project's rule destroys another's data.
- **Reshuffle/compaction lag is days** — post-load validation read too early under-reports; the
  reshuffle rate must be read *within the affected event family* and will never reach 100%.
- **BQ-arithmetic funnel projections run ~2.5pp hot** vs Mixpanel's windowing — calibrate the
  bias once against the source UI and carry it.
- **Expected reconciliation deltas** (insert_id dedup, translated/dropped verbs) must be
  distinguished from real loss, or every reconcile "fails" by noise.
- **Original-ID-merge graphs can be truncated at the 500-anon cap** (cluster size 501 = 500 anon
  + known ID); links beyond the cap never existed in the source and cannot be replayed.
- **`$distinct_id_before_identity`** is the dollar-prefixed real property name on events; the
  bare form does not occur. But mining it can be redundant when the alias graph was built from
  those same events.
- Reports mention the loaded historical figure as both 155,756,113,740 (partition snapshot) and
  155,766,332,816 (exported, incl. late-arriving rows) and prose says "155.8B" — snapshot drift,
  not a discrepancy.

## Open Questions

- **Is a two-pass design (graph build → pair emission → event stream) acceptable for
  mixpanel-import**, or does the sprint need a streaming-with-state approach (accumulate the
  graph as verbs flow past, emit/patch pairs at end)? Vipps evidence says pure stateless 1:1
  translation is measurably insufficient for star-shaped anonymous-headed clusters.
- **Which graph source should the pipeline prefer** when both exist: replayed identity verbs from
  the event export, or the identity-mappings export (`/api/2.0/…` → already-flattened
  alias→canonical, one hop, but canonical-form IDs and daily-rebuilt)? Vipps used the mappings
  export; a raw-export-driven replay would reconstruct the graph from verbs instead. Are the two
  provably equivalent in general (they were for the 4 sampled pairs)?
- **Conflict policy default:** vipps chose one-user-per-device, hard-evidence-then-most-recent.
  Should the pipeline expose this as a configurable strategy (first/last/evidence-ranked/drop
  ambiguous) with a mandatory pre-send conflict census?
- **Cluster-head user selection:** when a user-bearing cluster contains *multiple* numeric user
  IDs (account re-registrations, shared devices), which user do all sibling devices link to? The
  reports resolve per-device, not per-cluster; a replay pipeline needs an explicit rule.
- **What is the pipeline's "projection before loading" story?** Vipps did it in BigQuery. For a
  streaming tool the analog might be a dry-run mode that builds the device→user map, applies it
  locally, and emits people-count / total-vs-unique / conflict reports without sending.
- **Retroactivity guarantees:** v2/v3 established that pairs loaded after events re-attribute
  them, but compaction timing is undocumented (~5 days observed). Is events-first-pairs-later a
  supported ordering the pipeline can rely on, or should pairs always precede events?
- **Should the pipeline auto-detect the server-side-`$identify` shape** (resolved customer in
  `distinct_id` alongside an ephemeral `device_id`) and warn — the exact pattern the vipps
  transform destroyed?
- **The 89.51% vs 90.5% question generalizes:** what does the pipeline recommend as the
  acceptance criterion for a migration, given the source project's own numbers may be wrong?
