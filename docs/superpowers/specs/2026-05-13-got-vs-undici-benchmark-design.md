# Transport Benchmark: got vs undici

**Date:** 2026-05-13
**Status:** Draft

## Goal

Determine which HTTP transport (`got` or `undici`) yields higher throughput when streaming gzipped JSONL event data into a real Mixpanel project. Output: aggregate eps, mbps, rps per (transport × workers × file).

## Background

`components/importers.js` ships two transport implementations:

- `flushToMixpanel` — `got` HTTP client with `https.Agent({ keepAlive: true, maxSockets: 50 })`
- `flushToMixpanelWithUndici` — shared `undici.Pool` (100 connections, 20 pipelining) per region

`components/pipelines.js:439` selects between them based on `job.transport`. Default is `'undici'` (`components/job.js:177`).

The existing `benchmarks/archives/benchmark.js` runs with `dryRun: true`, which `pipelines.js:467-471` short-circuits before any HTTP call. It measures pipeline overhead, not transport. A fresh benchmark must run live against Mixpanel.

## Inputs

Four gzipped JSONL files in `benchmarks/testdata/`:

| File | Compressed | Records | Avg Event |
|------|-----------|---------|-----------|
| `1m-events-TINY-150MB-RAW.json.gz` | 21M | ~1M | ~150B |
| `5m-events-TINY-700MB-RAW.json.gz` | 105M | ~5M | ~140B |
| `300k-events-DENSE-1GB-RAW.json.gz` | 63M | ~300k | ~3.5KB |
| `1m-events-DENSE-4GB-RAW.json.gz` | 278M | ~1M | ~4KB |

Tiny files stress request rate (small batches → many requests). Dense files stress byte throughput.

## Matrix

- **transports**: `['got', 'undici']`
- **workers**: `[25, 50, 100]`
- **files**: all 4 above

Total: 4 × 2 × 3 = **24 runs**, executed sequentially (one at a time, no concurrency between runs).

## Fixed Configuration

Per `mp()` call:

```js
{
  recordType: 'event',
  streamFormat: 'jsonl',
  isGzip: true,
  fixData: true,
  recordsPerBatch: 2000,
  compress: true,
  verbose: false,
  abridged: true,
  logs: false,
  showProgress: false,
  dryRun: false,             // MUST be false to exercise HTTP
  transport,                 // matrix axis
  workers,                   // matrix axis
  highWater: workers * 10
}
```

Credentials: `{ token: process.env.MP_TOKEN }`. Loaded via `dotenv` from `.env` at repo root.

## Components

### `benchmarks/transport-bench.js`

Single entry point. Responsibilities:

1. Load `.env` for `MP_TOKEN`. Fail fast if missing.
2. Verify all 4 test data files exist. Fail fast if any missing.
3. Build run plan: nested loop `files → transports → workers`. 24 runs total.
4. Optional warmup: 1 small unmeasured run with each transport against `1m-events-TINY-150MB-RAW.json.gz` at workers=25 to prime DNS/TLS/sockets.
5. For each measured run:
   - Print run header `[i/24] file=... transport=... workers=...`
   - Call `mp(creds, file, opts)`, time wall-clock and capture result
   - Pull `eps`, `mbps`, `rps`, `duration`, `requests`, `retries`, `rateLimited`, `serverErrors`, `clientErrors`, `success`, `failed`, peak heap from job result
   - Print one-line summary
   - Push result to in-memory array
   - Sleep 30s cooldown (let undici/got pools settle, avoid Mixpanel rate-limit spillover)
6. After all runs: print summary tables, write JSON.

### Sequential guarantee

Plain `for` loop with `await mp(...)`. No `Promise.all`, no parallelism. One pipeline at a time. Cooldown `await new Promise(r => setTimeout(r, 30000))` between runs.

### Output

- **Live console**: per-run one-liner with eps/mbps/rps/duration/retries
- **JSON**: `benchmarks/results/transport-bench-<ISO-timestamp>.json` containing `system` info (node, platform, mem, cores), full run plan, per-run metrics
- **Final summary tables**:
  - Per file: transport × workers grid showing eps, mbps, rps
  - Overall winner per file (higher eps)
  - Overall winner across matrix

### Failure handling

- If single run errors (network, auth, exception): record `error` field in result, print red error line, **continue** to next run. Do not abort the matrix.
- If `MP_TOKEN` missing or test files missing: abort before any runs.
- Ctrl+C: write partial results to JSON before exit.

## Non-Goals

- p50/p95/p99 latency
- Memory profiling beyond peak heap from job result
- HTTP/2 toggle
- compress=false comparison
- Any code change to `components/importers.js` or `components/pipelines.js`
- npm script wiring (run directly with `node benchmarks/transport-bench.js`)

## Risks

- **Mixpanel rate limits** — `dense-4GB` at workers=100 may trip 429s. The benchmark records `retries` and `rateLimited`, so degraded throughput is visible rather than silent. 30s cooldown mitigates carry-over throttling.
- **Long total runtime** — 24 sequential runs over 5+ GB raw data could take 1–3 hours. Acceptable for a one-shot run.
- **Network variance** — single trial per cell. Documented as a limitation; no repeat runs in scope.

## Success Criteria

- Script completes 24 runs against real Mixpanel and emits a results JSON.
- Per-cell metrics let user see which transport wins per file size and worker count.
- Zero modifications to production code paths under test.
