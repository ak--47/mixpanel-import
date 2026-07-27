# Changelog

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
