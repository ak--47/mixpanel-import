# Performance & Memory Model

Reference for how throughput and memory behave, and why the architecture is shaped the way it is.
Operational tuning advice for end users lives in README §Performance & Scale; this document is the
internal rationale.

## Throughput model

- **No artificial throttling** to Mixpanel — the pipeline streams as fast as the API accepts.
- **Backoff only on 429**, with exponential retry. Retriable status codes and error codes are listed
  in [components/importers.js](../components/importers.js).
- Metrics tracked on the Job: `eps` (events/sec), `rps` (requests/sec), `mbps` (MB/sec).
  `percentQuota` was removed in v3.1.0.
- Concurrency is `job.workers` (default **50**, aliased by `concurrency`). Stream `highWater`
  defaults to `min(workers * 10, 500)` unless set explicitly.
- Batches are bounded by both count and bytes: `recordsPerBatch` (default 2000, hard-capped at 2000
  for events/users, 200 for groups) and `bytesPerBatch` (default 9.8 MB). `createSmartBatcher`
  targets 98.5% of `bytesPerBatch` to stay clear of the 10 MB API limit.

## Memory model

Constant memory regardless of input size, achieved by backpressure end to end:

- Native Node.js Transform streams throughout, composed with `stream/promises`.
- Bounded queues via each stage's `highWaterMark`.
- `bytesCache` is a `WeakMap` keyed on the record object, so byte counts computed during
  stringification are reused by the batcher without mutating records.
- **`jsonCache` is currently disabled** (`const jsonCache = null` in
  [components/pipelines.js](../components/pipelines.js)). It is still threaded through
  `createStringifyCacher` / `createHttpSender` for API compatibility. Caching the serialized JSON
  roughly doubled resident memory, which cost more than the re-serialization it saved. Re-enabling
  it means re-measuring both axes, not just throughput.
- `createMemoryMonitor` ([components/smart-config.js](../components/smart-config.js)) is inserted as
  the first stage when `verbose` or `memoryMonitor` is set.
- `aggressiveGC` adds periodic GC every 30s plus an emergency pass at 90% heap, but only when Node
  is started with `--expose-gc`; otherwise it logs and no-ops.

## BufferQueue (cloud storage throttling)

**Problem:** GCS/S3 deliver at 100 MB/s+ while the pipeline drains at ~10 MB/s. Without
intervention the delta accumulates in memory and the process dies on large files.

**Solution:** [components/buffer-queue.js](../components/buffer-queue.js) decouples the fast source
from the slow sink with an internal queue that pauses and resumes the source stream.

```
GCS/S3 (100MB/s) → BufferQueue → Pipeline → Mixpanel (10MB/s)
                      ↑      ↓
                   Pause   Resume
                  at 1.5GB  at 1GB
```

| Option | Default | Effect |
|---|---|---|
| `throttleMemory` | off | Enable the BufferQueue (`throttleGCS` is a deprecated alias) |
| `throttlePauseMB` | 1500 | Pause the source when the buffer exceeds this |
| `throttleResumeMB` | 1000 | Resume when the buffer drains below this; must be < pause |
| `throttleMaxBufferMB` | 2000 | Hard ceiling on buffer size |

Wired into [components/parsers.js](../components/parsers.js) for GCS/S3 JSON, JSONL, and CSV
streams. Records stay in order; nothing is dropped. Typical steady state is 1–2 GB heap for an input
of any size.

## Cloud read resilience (v3.5.0)

[components/resilient-source.js](../components/resilient-source.js) wraps cloud reads with an idle
watchdog and range-read resume, so a stalled or reset connection restarts from the last byte
received rather than failing the job.

| Option | Meaning |
|---|---|
| `resumeOnStall` | Enable watchdog + range-read resume |
| `cloudResumeAttempts` | Max resume attempts; counter resets after a successful resume |
| `cloudRetryBackoffMs` | Backoff between attempts |

Counters surfaced on the Job: `stallsDetected`, `resumesAttempted`, `resumesSucceeded`,
`bytesResumed`.

## History: v3.1.0 native streams refactor

Highland.js was removed and the pipeline rewritten on native Transform streams.

**Breaking changes**
- Highland streams are no longer accepted as input — convert to a native Node.js stream first.
- `percentQuota` was removed; use `eps` / `rps` / `mbps`.
- The external API was otherwise unchanged (drop-in for most callers).

**Measured effect at the time**
- ~10x lower heap on large files (≈500 MB → ≈50 MB for a 1 GB input).
- 10–20% higher throughput from removing Highland's per-record overhead.
- OOM on large cloud files eliminated, since backpressure now propagates from the HTTP sender back
  to the source.

These figures are from the v3.1.0 measurement and are kept as rationale, not as a current benchmark.
Live numbers come from `npm run benchmark`
([benchmarks/benchmark.js](../benchmarks/benchmark.js)); see also
[docs/superpowers/specs/2026-05-13-got-vs-undici-benchmark-design.md](superpowers/specs/2026-05-13-got-vs-undici-benchmark-design.md)
for the transport comparison design.
