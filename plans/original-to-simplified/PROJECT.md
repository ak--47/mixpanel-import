# Original → Simplified ID Merge: Identity Replay

**Sprint start:** 2026-08-16 · **Owner:** AK + Claude (orchestrator)
**Status:** RESEARCH — awaiting clarifying-question round with AK before build

## Goal

A first-class `mixpanel-import` transform that "replays" data from an **original ID merge**
project into a new **simplified ID merge** project, properly translating identity verbs
(`$identify`, `$create_alias`, `$merge`) into dual-ID (`$user_id` + `$device_id`) events
instead of dropping them. Lands as ONE PR: new API + full tests + docs.

Rough spec: `/Users/ak/code/smarterchild/research/original-simplified-research.md` (§6 = proposal).
Prior art: `/Users/ak/code/bq-mp-inspector/customers/vipps/` (sql + reports).

## Test environments (credentials in .env, NOT here)

| Role | Project | Name | .env prefix |
|---|---|---|---|
| ORIGINAL id merge | 4054680 | ephem-bitter-snow-4299 | `ORIGINAL_*` |
| SIMPLIFIED id merge | 4054681 | ephem-delicate-butterfly-9977 | `SIMPLIFIED_*` |

Query via Power Tools API (`https://mixpanel-power-tools-api-lmozz6xkha-uc.a.run.app`),
auth = `AK_BEARER_TOKEN` in .env. Events/clusters can't be deleted → **version every dataset**
(e.g. `dataVersion: N` property on every event) so queries can filter by version and
environments stay reusable across rounds.

## Business requirements (from AK's brief)

1. Canonical user_ids in test data: **numeric**. Anon/session/device ids: **uuidv4**.
2. Pipeline must let the user express which ID becomes canonical/head of cluster
   (simplified doesn't support anon→anon; we need an anchor: the `isUserId` predicate).
3. Test dataset must EXCEED original-id-merge stated limits: clusters >500, non-uuidv4
   anon ids, anon→anon→anon alias chains — prove replay resolves what original could not.
4. BONUS (default false): in-memory id graph built during the pipeline; at stream end,
   trace alias graph and emit transitive-closure association events
   (anon1→anon2→anon3→canon4 ⇒ also emit anon1→canon4, anon2→canon4).
   Must not OOM — bounded memory (circular buffer / LRU?), best-effort.
5. OK to attach metadata props to rewritten events (resolution strategy/bucket/why).

## Design decisions (AK, 2026-08-16)

1. **Graph default ON.** Transitive cluster expansion is the core feature (Vipps: 65.9% stateless
   vs 89.5% graph). Stateless 1:1 rewrite is the opt-out lite mode.
2. **Graph memory: bounded + fail-loud.** In-memory Map, configurable cap; at cap keep going,
   count+report dropped edges, optional hard-fail. No new deps, no silent loss (circular buffer
   rejected — silent correctness loss).
3. **API surface: top-level `identityReplay` option group** (sibling of v2_compat). `isUserId`
   accepts a FUNCTION in module mode (primary use), regex string for CLI. Module-first is fine.
4. **Events only this PR.** Profiles are a separate, smaller follow-up. BUT: option (default
   false) to write the resolved graph as an export artifact — user supplies local or cloud-storage
   path; write only if path writable. Artifact doubles as dry-run projection output.

## Phases

1. **Research sweep** (done via workflow `wf_76b4217e-b2e`) → `research/*.md`
2. **Synthesis + clarifying questions to AK** ← current
3. **Dataset generator** — versioned identity torture-test dataset (normal events + all verbs,
   in-limit and over-limit cases), load into ORIGINAL project 4054680
4. **Probe** — export back out, query both projects via power tools; document where identity
   resolution works/fails; write findings to `research/`
5. **Design** — final option shape + pipeline placement (brainstorm w/ AK's spec §6.4 as base)
6. **Build** — TDD: transform + id-graph module + options + telemetry
7. **Validate** — replay ORIGINAL export → SIMPLIFIED project 4054681, query-compare counts
8. **Ship** — docs, CHANGELOG, single PR; move this dir to `plans/completed/original-to-simplified/`

## Repo conventions established this sprint

- All agent docs + research → `./plans/{feature_name}/`; on completion → `./plans/completed/{feature_name}/`
- Version-controlled, npm-ignored, **no secrets ever**

## Key facts already established

- `vendor/mixpanel.js` `mixpanelEventsToMixpanel` maps warehouse-export rows (device_id/user_id/
  distinct_id/insert_id/event_name) → import events. It does NOT filter $identify today —
  the "we filter $identify + friends" happens in per-customer SQL (cosmos_migration) not in this repo.
  posthog vendor v3 branch DOES drop identify events (vendor/posthog.js:184).
- Simplified rules: bare distinct_id unprefixed → treated as $user_id; `$device:`-prefixed →
  $device_id; event w/ both $user_id+$device_id triggers merge. (Vipps failure = unprefixed anon ids.)
- $merge carries exactly 2 ids (openapi minItems/maxItems 2) → 1:1 rewrite, no fan-out needed.
- Original limits: max 500 ids/cluster, anon must be uuidv4 (per Dana), up to 24h merge delay.
- createFlattenStream (pipelines.js:145) already supports 1→N fan-out downstream of user transform.
- v2_compat (job.js:361, transforms.js:684 setDistinctIdFromV2Props) is the sibling option;
  must be mutually exclusive with identity_replay or identity_replay wins + logs.

## Progress log

- 2026-08-16: Sprint start. .env updated with ORIGINAL_*/SIMPLIFIED_* creds. Research workflow
  launched (10 agents → research/01..10). Spec + vendor/mixpanel.js read directly.
- 2026-08-16: Research complete (10/10 agents, ~755k tokens). **research/00-SYNTHESIS.md written —
  read it first.** Headline: stateless 1:1 verb translation measured at 65.9% recovery at Vipps;
  transitive cluster expansion got 89.51% — the "bonus" id graph is actually the core feature.
  10-item empirical probe list drafted. Awaiting AK answers to 6 open design decisions
  (synthesis §Open design decisions) before building.
- 2026-08-16 night: AK design decisions locked (see §Design decisions). Dataset v1 (1216 events,
  15 scenarios) generated + loaded into ORIGINAL. Simplified probes p01–p10 run.
  **research/11-probe-findings-v1.md** — verbs HARD-REJECTED by simplified /import (no 'emit'
  mode possible); dual-ID custom events merge + stitch retroactively incl. backdated; first-write-
  wins conflicts; ingest strips one $device: prefix; canonical in original can be anon uuid.
  design-draft.md amended to v2 with pinned build interfaces. Build workflow launched
  (wf_9b079b10-59f: graph module, replay stage, plumbing, docs — 4 agents). Steady-state
  re-export scheduled (+60min background task b42pt0pz5). NOTE: per CLAUDE.md we do NOT run
  jest — verification via probes/run-04 live replay + AK runs suites in the morning.
- 2026-08-17 (overnight): AK unblocked test-running (CLAUDE.md updated with live-network/watch-mode
  guidance). Branch feat/identity-replay created; work committed in chunks. Build workflow
  delivered graph + stage + plumbing + docs; suites green (349 pass; 1 pre-existing parquet env
  failure). /code-review (high) found 10 verified issues — ALL fixed + committed (12a8b08):
  dead overflow accounting, unbounded shadow Maps, flush backpressure, cloud graphPath via
  destination-writer, epochStart×floor clamp, ms/s unit detection, zero-verb minAssociationRate,
  denylist double-count + '' entries, rank-2 'verb' mislabel, CLI silent --ir-* discard,
  verbose-gated v2_compat warn, fixData force-on for export-import replay.
  Steady-state export: verbs PERSIST in raw export (round-1 gaps were lag) + export contains
  DUPLICATE rows. Acceptance: dry telemetry matched manifest exactly (569 assoc, 13 clusters);
  LIVE replay → SIMPLIFIED 1241/1241 success 0 fail. Verdict query scheduled (+12min,
  task b86jhq9n6, probes/run-05-compare.js).
