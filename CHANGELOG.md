# Changelog

## 3.5.2

### Fixed

- **CLI: `--data-group-id` no longer corrupts large IDs.** The flag was declared `type: 'number'`,
  so yargs coerced the value through a JavaScript double. Mixpanel data group IDs are 19-digit
  integers — above `Number.MAX_SAFE_INTEGER` — so any real-world ID lost precision (e.g.
  `3928097563762691601` became `3928097563762691600`) and group profile exports silently queried a
  nonexistent group. The flag is now `type: 'string'`, which `JobConfig` already handled. Library
  callers passing `dataGroupId` as a string were never affected; pass a string there too.
- **`export-import-group` is now actually wired up.** The record type has been declared in
  `index.d.ts` since round-trip types were added, but `parsers.js` had no branch for it — using the
  literal string fell through the type dispatch and crashed. It now behaves as
  `export-import-profile` in group mode: exports group profiles from `/api/2.0/engage` (pass
  `dataGroupId`) and imports them into the destination project as `group` records (pass `groupKey`,
  and `secondToken` when the destination is a different project). Previously the only way to
  round-trip groups was `export-import-profile` + `dataGroupId`, which still works.

## 3.5.1

### Removed — please read before upgrading

- **The library no longer registers process-global handlers.** Requiring `mixpanel-import` used to
  install five listeners as an import side effect (`components/importers.js`): `unhandledRejection`,
  `uncaughtException`, `exit`, `SIGINT`, and `SIGTERM`. All five are gone. A library has no business
  setting process-level policy for the application embedding it.

  **What changes for you, in order of how much it matters:**

  1. **Your process will crash on uncaught exceptions and unhandled rejections again.** This is the
     intended fix, and it is the one that can surprise you. The old `uncaughtException` /
     `unhandledRejection` listeners logged to `console.error` and *continued*, which suppressed
     Node's default crash. Any consumer that has been silently coasting through errors since adding
     this dependency will start seeing those errors terminate the process. **Those errors were
     always happening — only the reporting changes.** If you want the old behavior, register your
     own handlers; that is now a decision you get to make.
  2. **Graceful shutdown works for consumers now.** Node runs signal listeners synchronously in
     registration order, and the old `SIGTERM`/`SIGINT` handlers called `process.exit(0)` on their
     last line. Because they were registered during the import chain, they nearly always ran first,
     so an application's own shutdown handler never got a turn — including asynchronous drains
     scheduled via `process.prependListener`. Servers with WebSocket or connection draining (notably
     on Cloud Run, which sends `SIGTERM` then `SIGKILL`s 10s later) can now shut down cleanly.
  3. **CLI exit status on Ctrl-C is now `130` instead of `0`.** A supervisor can once again tell
     "asked to stop" apart from "completed successfully."

  The removed hooks accomplished nothing they appeared to. `Pool.close()` is asynchronous and
  `process.exit()` ran synchronously on the very next line, so no pool ever finished closing; the
  `exit` hook was strictly worse, since no async work of any kind can run during the `exit` event.
  Normal CLI completion was already handled by an explicit `process.exit(0)` in `index.js`.

  Reported by the dm4 team, 2026-07-30, after a graceful WebSocket drain silently failed for months.

### Added

- **`mp.destroy()` — lifecycle API for long-lived processes.** Releases the shared undici HTTP
  connection pools. Not required for CLI use or short-lived scripts; useful for servers and workers
  that run occasional imports and want to free sockets between jobs. Safe to call repeatedly, and
  safe to call before further imports — pools are re-created on demand.

  ```js
  const mp = require('mixpanel-import');
  await mp(creds, data, opts);
  await mp.destroy();
  ```

- **CLI-only signal handling.** `SIGINT`/`SIGTERM` during a CLI import now print
  `received <SIGNAL>; aborting import...` and exit `130`. This lives inside the `require.main ===
  module` branch and is deliberately *not* installed for `--ui`, so `ui/server.js` can own its own
  shutdown.

- **Regression guards.** `tests/handlers.test.js` asserts (in child processes) that requiring the
  library adds zero global listeners, that crash semantics are intact, and that a consumer's own
  signal and `uncaughtException` handlers run — including an asynchronous drain. A static scan
  fails the suite if any file under `components/` or `vendor/` reintroduces a `process.on`.
  `tests/pools.test.js` covers the pool lifecycle and retry counts against local HTTP servers; new
  `job config` cases in `tests/unit.test.js` cover the option and endpoint fixes below.

### Fixed

These are long-standing bugs, unrelated to the handler removal above, found while working on it.

- **`maxRetries: 0` no longer silently becomes 10 (or 5).** The option was read with `||`, so an
  explicit `0` — the natural way to say "do not retry" — was falsy and replaced by the default. A
  batch you asked to fail fast would instead retry ten times with exponential backoff. Fixed at all
  six read sites (`job.js`, `importers.js` ×2, `exporters.js` ×3) by using `??`.
- **`compressionLevel: 0` no longer silently becomes 6.** Same `||` bug. Level `0` is a valid gzip
  setting (store, no compression) and was impossible to select.
- **India-region SCD imports no longer go to the EU host.** `endpoints.in.scd` pointed at
  `https://api-eu.mixpanel.com/import` while every other `in` endpoint used `api-in` — a
  copy-paste slip that sent India-region SCD batches to the wrong region. **If you run SCD imports
  with `region: 'IN'`, this changes where your data lands** (correctly, to `api-in`).

### Changed

- **Undici pools are created lazily, keyed by origin.** Previously three `Pool` objects were
  allocated at import time and never closed. They are now created on first use and reachable via
  `destroy()`.
- **Pool origin is derived from the request URL** (`new URL(url).origin`) rather than
  pattern-matching `api-eu` / `api-in` with a hardcoded US default. **No behavior change** — only
  ingest urls reach this code path, so it resolves to the same three origins as before. This is a
  simplification, not a bug fix: the undici transport is never used for exports (`corePipeline`
  routes those to `exporters.js`, which uses `got`), and the export endpoints on
  `data.mixpanel.com` / `mixpanel.com` were always correct.
- **Undici socket failures are reported through a pool-scoped `connectionError` listener** instead
  of a process-global handler.

### Notes

- Consumers who deliberately relied on the suppressed-crash behavior should register their own
  `uncaughtException` handler. Note that continuing after an uncaught exception leaves the process
  in an undefined state; Node's default is to crash for good reason.
- Graceful WebSocket shutdown in `ui/server.js` is unblocked by this release but not yet
  implemented — tracked as follow-up work.

## 3.5.0

### Added
- **GCS network resilience for cloud reads.** New `resumeOnStall` option (default `false`): when a
  mid-stream stall kills a GCS read, the source is reopened at the last-received compressed byte
  offset (generation-pinned range read) and streaming continues through the same pipeline. Gzip
  resume sits upstream of gunzip, so the zlib trailer remains the integrity net — any offset
  uncertainty fails loudly instead of guessing. JSONL/NDJSON GCS reads only in this release.
  - `cloudResumeAttempts` (default 3) bounds consecutive no-progress resumes; delivering 10MB after
    a resume resets the budget so large flaky files can complete.
  - `cloudRetryBackoffMs` (default 1000) tunes the exponential backoff.
  - `cloudStreamCallback` receives serializable per-file telemetry events: `stall`,
    `resume-attempt`, `resume-success`, `resume-fail`, `file-skip-missing`, `open-retry`.
  - New result counters: `stallsDetected`, `resumesAttempted`, `resumesSucceeded`,
    `filesSkippedMissing`, `bytesResumed`.

### Changed
- **Multi-file GCS/S3 reads never silently skip files.** Transient creation-time errors are
  retried (3x exponential backoff) and then fail the job with the file path and preserved error
  code. Cleanly missing files (404) are still skipped, but are now counted in
  `filesSkippedMissing` and surfaced via `cloudStreamCallback`.
- **1-element array inputs route through the single-file path.** A missing file in a 1-element
  array now throws (single-file semantics) instead of resolving empty.
- **S3 multi-file reads now respect backpressure.** Multi-file concatenation was rewritten as
  for-await loops with proper drain handling (previously unbounded buffering).
- **Dependency refresh (CJS-safe bumps only).** `@google-cloud/storage` ^7.21.0,
  `@aws-sdk/client-s3` ^3.1095.0, `pino` ^10, `undici` ^7, `ws`, `express` ^5, `multer` ^2, and a
  `uuid` override for a transitive vulnerability. Production audit: 0 vulnerabilities (was
  3 critical / 18 high). `engines.node` is now `>= 20.20.0`.

### Notes
- Resume offsets are stored-object offsets: exact for plain `.gz` objects, but objects uploaded
  with `contentEncoding: gzip` metadata may be transcoded by GCS, which ignores range requests —
  canary against your real objects before relying on resume.
- The GCS client skips checksum validation on ranged reads; for gzip the CRC32/ISIZE trailer
  covers integrity, while plain JSONL resume relies on offset exactness alone.

## 3.4.1

### Fixed
- **Wrapped multi-file cloud read failures now expose the original transport error code.** The
  fatal errors produced by mid-stream read failures in multi-file GCS/S3 imports
  (`Multi-file GCS read failed ...` / `Multi-file S3 read failed ...`) now carry the original
  error's `code` property (e.g. `ECONNRESET`), so callers' retry logic can classify them as
  transient. Message text and failure semantics are unchanged.

## 3.3.2

### Changed
- **`progressCallback` now fires independently of console verbosity.** Previously the import
  `progressCallback` was only invoked as a side effect of the CLI progress printer, so it never
  fired unless `verbose` or `showProgress` was enabled. It now fires whenever provided, regardless
  of `verbose`/`showProgress`, so UI/programmatic consumers get progress updates without stdout spam.
  - Throttled by `LOG_INTERVAL` (default 100ms) — not called per-record.
  - The stdout progress bar remains gated behind `verbose`/`showProgress`.
  - Callback signature is unchanged: `(recordType, processed, requests, eps, bytesProcessed)`.
  - A final `progressCallback` now also fires at pipeline flush so consumers can render a terminal 100% tick.
  - This is consumed by [@ak--47/dungeon-master](https://github.com/ak--47/dungeon-master) for UI import progress.
  - Note: the export/download `progressCallback` (different arg shape) is unaffected.
