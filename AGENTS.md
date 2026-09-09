# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Important Instructions

- **Tests: run them freely, but know what you're running.** `npm test` is Jest in **watch mode**
  (it never exits — use `npx jest <path> --watchAll=false` for one-shot runs). Some suites make
  **live network calls** and can be slow or flaky: `tests/int.optional.test.js` and
  `tests/sanity.test.js` take a long time, and anything hitting the export APIs is subject to
  rate limits (60 req/hr). Old fixture data (>5 years) can also age out of API time bounds.
  Do not get stuck re-running or "fixing" failures in live-network suites that are
  environmental — note them and move on. Fast local suites (`unit`, `handlers`, the per-module
  suites) are safe to run any time.

## Overview

`mixpanel-import` streams data to Mixpanel's ingestion APIs — events, user profiles, group profiles,
lookup tables — and pulls data back out via the export APIs. Usable as a CLI, as a library, and
through a web UI. Node `>=20.20.0`, npm `>=10`.

Deeper references, kept out of this file on purpose:

- [docs/performance.md](docs/performance.md) — memory/throughput model, BufferQueue, cloud resilience, v3.1.0 refactor history
- [docs/web-ui.md](docs/web-ui.md) — UI routes, HTTP endpoints, WebSocket protocol, logging, deploy
- [docs/websocket-hybrid-pattern.md](docs/websocket-hybrid-pattern.md) — why jobs run over the socket on Cloud Run
- [README.md](README.md) — user-facing docs: data sources, gzip, cloud storage, full options reference
- [CHANGELOG.md](CHANGELOG.md) — release history (currently back to 3.3.2)

## Development Commands

| Command | What it does |
|---|---|
| `npm test` | Jest in **watch** mode |
| `npm run coverage` | Jest with coverage, opens the HTML report |
| `npm run lint` | ESLint (`.eslintrc.json`) |
| `npm run typecheck` | `tsc` against `index.d.ts` + JSDoc |
| `npm run validate` | typecheck + lint |
| `npm run cli:test` | CLI smoke test against `./testData/moarEvents.json` |
| `npm run cli:help` | Print CLI options |
| `npm run generate` | Fake data into `./testData/` via `components/fakeData.js` |
| `npm run benchmark` | Performance suite in `benchmarks/` |
| `npm run prune` | Clear `logs/`, `mixpanel-exports/`, `tmp/`, `benchmarks/results/` |
| `npm start` | Web UI (equivalent to `node index.js --ui`) |
| `npm run ui:dev` | Web UI under nodemon |
| `npm run deploy` | Cloud Build → Cloud Run |
| `npm run dev` | nodemon on `scratch.js` — a gitignored personal scratchpad, not a general dev server |

## Architecture

### Components

| File | Role |
|---|---|
| `index.js` | Entry point, orchestration, CLI branch, exports |
| `components/job.js` | `JobConfig` — credentials, options, defaults, running stats |
| `components/pipelines.js` | `corePipeline` — stream assembly and record-type dispatch |
| `components/parsers.js` | Input detection (file, dir, array, stream, `gs://`, `s3://`) → readable streams |
| `components/transforms.js` | Built-in transforms and validation helpers |
| `components/importers.js` | Ingest HTTP layer (undici pools + legacy `got`), pool lifecycle |
| `components/exporters.js` | Export/annotation/SCD/lookup-table operations (uses `got`) |
| `components/validators.js` | Credential and token validation |
| `components/cli.js` | yargs CLI definition |
| `components/buffer-queue.js` | Memory-aware throttle between cloud sources and the pipeline |
| `components/resilient-source.js` | Idle-watchdog + range-read resume for cloud reads |
| `components/destination-writer.js` | Write/tee output to a destination instead of, or alongside, Mixpanel |
| `components/smart-config.js` | Memory monitor stage and adaptive config |
| `components/identity-graph.js` | Union-find identity graph (no internal imports) for identityReplay |
| `components/identity-replay.js` | Original→Simplified id-merge translation stage (`identityReplay` option) |
| `components/constants.js` | Shared constants; no internal `require`s (avoids cycles) |
| `components/jsonl.js`, `logs.js`, `meta.js`, `fakeData.js` | JSONL helpers, logger config, project metadata, test-data generator |
| `vendor/` | Source-platform transforms: amplitude, ga4, heap, june, mixpanel, mparticle, posthog |
| `ui/` | Express + `ws` server and static front-end |

### Data flow

1. `parsers.js` detects the input type and builds a readable stream.
2. `corePipeline` in `pipelines.js` diverts non-streaming record types, then assembles Transform stages.
3. Records are batched by count **and** bytes (`createSmartBatcher`).
4. `importers.js` sends batches concurrently.
5. Responses are aggregated onto the `JobConfig` and returned as `ImportResults`.

### Pipeline stages

Stage list is **branch-dependent**, not fixed. `corePipeline` first short-circuits non-streaming
record types (`table`, `export`, `profile-export`, `annotations`, `get-annotations`,
`delete-annotations`, `profile-delete`) to `exporters.js` / `flushLookupTable`. Remaining types
build a stage array:

- **Prelude** (conditional): `createMemoryMonitor` when `verbose` or `memoryMonitor`.
- **Normal mode:** `createExistenceFilter` → `createVendorTransform` →
  [`createIdentityReplay` when `identityReplay` is set] → `createUserTransform` →
  `createFlattenStream` → `createDedupeTransform` → `createExistenceFilter2` →
  `createHelperTransforms` → `createStringifyCacher`.
  Only one flatten stage exists, after the user transform — vendor transforms are 1:1.
  The identityReplay stage swallows `$identify`/`$create_alias`/`$merge` verbs and emits
  synthetic `identity association` events at flush — a user `transformFunc` downstream must
  tolerate those nested records.
- **`fastMode`:** collapses the above to `createExistenceFilter` → `createStringifyCacher`.
- **Sink:** `createSmartBatcher` → optional `createTeeStream(destinationStream)` →
  `createHttpSender` → `createLogger`. With `destinationOnly`, records bypass batching and the HTTP
  sender entirely and are written one at a time to the destination stream.

`createBatcher` and `createSizeBatcher` still exist but are **deprecated** — `createSmartBatcher`
does count and size batching in one pass. Don't add new call sites.

### Transport

`job.transport` selects the ingest client: **`undici` is the default**, `got` is the legacy path
(`components/job.js` → `this.transport = opts.transport || 'undici'`). `pipelines.js` picks
`flushToMixpanelWithUndici` vs `flushToMixpanel` accordingly. `exporters.js` and
`validators.js` always use `got`. Both flush functions must stay behaviorally equivalent —
`tests/pools.test.js` exercises the undici path against local servers.

### Authentication

Service account (preferred) or legacy API secret:

- Service account: `acct`, `pass`, `project`
- API secret: `secret`
- Extra, by record type: `token` (profiles/groups), `groupKey` (groups), `lookupTableId` (tables)

Which credentials a given record type actually needs is not uniform — export operations in
particular differ from ingest. `tests/int.optional.test.js` is the working reference for real
per-record-type credential combinations.

### Process-Global Handlers: Hard Rule (v3.5.1)

**Library code must never call `process.on` / `process.once` / `process.prependListener`.** No
`uncaughtException`, `unhandledRejection`, `exit`, `SIGINT`, or `SIGTERM` listeners in
`components/`, `vendor/`, or the module scope of `index.js`.

Why this is a rule and not a preference:
- `uncaughtException` / `unhandledRejection` listeners **replace** Node's default crash behavior. A
  library that registers one silently disables crash-on-error for every embedding application.
- Signal listeners run **synchronously in registration order**. A library registered during the
  import chain always wins the race, so a `process.exit()` in its handler means the application's
  own graceful-shutdown handler never runs — `prependListener` does not help, because any realistic
  drain is asynchronous and returns before its work completes.

Through v3.5.0, `components/importers.js` registered all five. They were removed in 3.5.1.

**Where process-level policy belongs instead:**
- CLI-only concerns → inside the `require.main === module` branch in `index.js`, and specifically
  inside the non-`--ui` arm, so `ui/server.js` can own its own shutdown.
- Resource cleanup → the exported `destroy()` lifecycle API (see below).
- Transport-level errors → scoped listeners on the resource that failed (e.g.
  `pool.on('connectionError')`), never process-wide.

**Guard:** `tests/handlers.test.js` enforces this. It asserts in child processes that requiring the
library adds zero global listeners, and statically scans `components/` and `vendor/` for
`process.on(`. If you add a handler, that suite fails — that is intentional, do not weaken it.

### Pool Lifecycle & `destroy()`

Undici pools live in a `Map` keyed by origin in `components/importers.js`, created lazily by
`getPool(url)` on first request. Origin is derived from the request URL via `new URL(url).origin`,
so each host gets a pool pointed at itself.

Only **ingest** urls reach `getPool()` — `api.mixpanel.com`, `api-eu.mixpanel.com`,
`api-in.mixpanel.com`. `corePipeline` diverts `export`, `profile-export`, `profile-delete`,
`annotations`, and `table` record types to `exporters.js` (which uses `got`) before the undici
sender runs, and `export-import-*` reassigns `recordType` to `event`/`user`/`group` in
`parsers.js`. The `data.mixpanel.com` / `mixpanel.com` export hosts never touch the undici pools.

`destroy()` closes all pools and clears the map. It is exported from `components/importers.js` and
re-exported as `mpImport.destroy` in `index.js`. It is idempotent and re-entrant: pools are
re-created on demand, so a consumer may call it after every job. It is optional — the CLI does not
need it, since `index.js` force-exits on completion.

## Configuration

`index.d.ts` is the source of truth for the `Options` type. Frequently touched:

- `recordType` — 17 values, not 4. Ingest: `event`, `user`, `group`, `table`. Export/ops: `export`,
  `profile-export`, `profile-delete`, `group-export`, `group-delete`, `scd`, `annotations`,
  `get-annotations`, `delete-annotations`. Round-trip: `export-import-event`,
  `export-import-profile`, `export-import-group` (rewritten to `event`/`user`/`group` in `parsers.js`).
- `transport` — `'undici'` (default) | `'got'`
- `workers` — concurrency, **default 50** (alias: `concurrency`)
- `recordsPerBatch` — default 2000, capped at 2000 for events/users and 200 for groups
- `bytesPerBatch` — default 9.8 MB
- `vendor` / `vendorOpts` — source-platform transform
- `transformFunc` — user transform; may return an array, which the flatten stage expands
- `compress` — gzip request bodies (events only)
- `fastMode`, `destination`, `destinationOnly` — pipeline shape switches
- Throttle and resilience options — see [docs/performance.md](docs/performance.md)

## Gotchas

- **`jsonCache` is disabled.** `pipelines.js` sets it to `null` and threads it through only for
  signature compatibility; `bytesCache` (a `WeakMap`) is the live cache. Any comment or doc claiming
  JSON stringification is cached is describing the disabled path.
- **Local `.parquet.gz` is not supported.** Gzip auto-detection covers `.json.gz`, `.jsonl.gz`,
  `.csv.gz`, `.ndjson.gz`, `.txt.gz`, `.tsv.gz`; Parquet gzip works from GCS/S3 only. `isGzip: true`
  forces decompression for files without a `.gz` extension.
- **Multiple input files must share one format and one compression state.**
- **Missing cloud files are skipped, not fatal** — the pipeline continues.
- `components/constants.js` must stay free of internal `require`s; it exists to break a cycle.
- The `--ui` CLI arm installs no signal handlers by design. Don't "fix" that.

## Testing

Jest, config in `jest.config.json`. `npm test` runs in watch mode.

| Suite | Covers |
|---|---|
| `tests/unit.test.js` | Job config, transforms, parsers |
| `tests/handlers.test.js` | Process-global handler guard (child processes + static scan) |
| `tests/pools.test.js` | Undici pool lifecycle and retry counts against local servers |
| `tests/cloud.test.js`, `cloud-failure.test.js` | GCS/S3 paths and failure handling |
| `tests/vendor.test.js` | Vendor transforms |
| `tests/identity-graph.test.js`, `identity-replay.test.js`, `identity-replay-pipeline.test.js` | identityReplay: graph module, stage semantics, full-pipeline integration (all local, no network) |
| `tests/sanity.test.js`, `jsdocTests.js` | Smoke tests and JSDoc examples |
| `tests/int.optional.test.js` | Live-API integration — despite the name it matches `testMatch` and runs with the rest; needs a `.env` (format documented at the top of the file) |
