# PR Draft: `identityReplay` — Original → Simplified ID Merge migration (v3.6.0)

> Draft body for the single PR. Numbers marked ⏳ get finalized from the post-compaction
> verdict table (probes/results/v1-05b-compare-insights.json) before opening the PR.

## What

A new top-level option group, `identityReplay`, that translates an **original ID merge**
event stream into a **simplified ID merge** event stream in-flight — the missing piece for
replaying an existing project into a new simplified project.

```js
await mpImport(creds, './export/*.jsonl', {
  recordType: 'event',            // or 'export-import-event' for direct project→project
  identityReplay: {
    isUserId: (id) => /^\d+$/.test(id),   // the ONE thing the library can't infer
  }
});
```

## Why

Simplified projects **hard-reject** `$identify` / `$create_alias` / `$merge`
(`400: identity events are not allowed when project is using simplified identity management`
— verified live). Today every original→simplified migration hand-builds per-customer SQL,
and the identity transform is the part that keeps failing (Vipps shipped 34–66% coverage
twice). Stateless 1:1 verb rewriting measures at **65.9%** identity recovery; transitive
cluster expansion measured **89.51%** vs a 90.5% prod reference. So the graph is the core
of this feature, not an add-on.

## How

- **`components/identity-graph.js`** — union-find over id strings (path compression, union
  by size), per-node evidence metadata (rank 0 hard dual-ID row / 1 verb / 2 inferred,
  first/last ts), bounded by `maxGraphSize` with fail-loud overflow accounting.
- **`components/identity-replay.js`** — pipeline Transform between the vendor and user
  transforms: absorbs verbs into the graph (never forwards them), rewrites ordinary events
  (`$device:` handling in ONE tested function, evidence order
  `$distinct_id_before_identity` → verb props → predicate), and at flush emits one
  doc-sanctioned dual-ID association event per anonymous cluster member — including
  **transitive closure** (anon1→anon2→anon3→U emits all three pairs). Deterministic
  `$insert_id = md5(user|device)` makes re-runs and raw-export duplicate rows self-dedupe.
  Backpressure-aware flush; multi-user clusters go through `onAmbiguous`
  (drop/resolve/error) with evidence-ranked election; telemetry lands on `ImportResults`
  with a fail-closed `minAssociationRate` floor; optional `graphPath` JSONL artifact
  (local/gs://s3://, best-effort).

## Verified live (projects 4054680 original / 4054681 simplified)

Torture dataset: 15 scenarios, 1216 events, all three verbs, in-limit AND over-limit
(plans/original-to-simplified/probes/generate-dataset.js — versioned + deterministic).

- Replay imported **1241/1241, zero failures, zero verb leaks**.
- Dry-run telemetry matched manifest ground truth exactly (569 associations, 13 clusters).
- **Beats the source project's structural limits** (cluster-membership verified via
  activity stream):
  - anons orphaned by original's **500-id cluster cap** resolve to their user;
  - **non-uuidv4 anon ids** (refused by original's `$identify`) link fine;
  - **anon→anon→anon alias chains** resolve transitively.
- ⏳ Final per-scenario uniques table (post-compaction).

## Empirical findings that shaped the design (plans/original-to-simplified/research/)

- Simplified /import rejects verbs (whole batch, strings-shaped `failed_records`) → no
  "forward verbs" mode exists.
- Merge trigger: any custom-named event with `$device_id`+`$user_id`; backdated association
  events stitch retroactively; device→user is **first-write-wins** → conflicts must be
  resolved pipeline-side, before send.
- Raw export: verb rows persist (with `$identified_id`/`$anon_id`/`alias`/`$distinct_ids`),
  contains duplicate rows, exported `distinct_id` may be re-keyed to an **anonymous**
  cluster head → never trust it as the user.
- Ingest strips one `$device:` prefix from `$device_id`; JQL sees unresolved distinct_ids
  (don't validate with it).

## Tests

`tests/identity-graph.test.js` (~50), `tests/identity-replay.test.js` (~70),
`tests/identity-replay-pipeline.test.js` (full corePipeline, destinationOnly, no network).
All local; no new deps; no process-global handlers (handlers guard passes).

## Docs

README section w/ option table + gotchas; CHANGELOG 3.6.0; CLAUDE.md architecture rows +
stage list; research + probes in `plans/original-to-simplified/` (→ `plans/completed/` on merge).
