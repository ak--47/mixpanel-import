# Changelog

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
