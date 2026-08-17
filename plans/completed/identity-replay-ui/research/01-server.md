# Server-Side Recon: ui/server.js (2295 lines) — identityReplay for v3.6.1

## Summary

The import routes pass options through **verbatim** — `opts = JSON.parse(options)` goes straight to
`mixpanelImport()` with no allowlist, so an `identityReplay` key in the options JSON already reaches
the library untouched on `/job/prepare` + WS `start_job`, legacy `/job`, and `/dry-run`. **Only two
server changes are strictly required:**

1. **`filterResultForClient` (ui/server.js:418–435)** — the results allowlist does NOT include
   `identityReplay`, so replay telemetry is stripped from every `job-complete` payload and from
   `GET /job/:jobId/status`. Add `'identityReplay'` to `allowedFields` (line 419–425). The library's
   own abridged-mode filter already keeps it (components/job.js:1216–1217), so this is the only
   place telemetry dies.
2. **`/export` (ui/server.js:1443–1469) and `/export-dry-run` (1763–1778)** build `opts` explicitly
   field-by-field from `exportData` — unknown keys are silently dropped. For
   `export-import-event` + replay, add `identityReplay: exportData.identityReplay` to both
   constructions.

`transformCode` is compiled with a bare `eval("(" + transformCode + ")")` — no sandbox. Do **not**
add another eval site for `isUserId`: the library compiles regex **strings** itself
(components/identity-replay.js:61–92), so the UI should send
`options.identityReplay.isUserId` as a plain JSON string.

## Details

### How /job/prepare + WS start_job receive {credentials, options, transformCode}

- `POST /job/prepare` (ui/server.js:677–722): multipart upload only. Parses `options` just to peek
  at file-source info (:682), stores `jobStatuses.set(jobId, { status: "prepared", filePaths,
  files, ... })` (:698–704), returns `{ jobId }` (:710). **Options/credentials/transformCode are
  NOT stored here** — they arrive later over the socket. No server change needed at prepare.
- WS message handler (:292–350): `data.type === "start_job"` destructures
  `{ jobId, credentials, options, cloudPaths, transformCode }` (:315) and calls
  `executeJobOverWebSocket(ws, jobId, credentials, options, cloudPaths, transformCode, jobLogger)` (:336).
- `executeJobOverWebSocket` (:116–286):
  - `creds = JSON.parse(credentials)`, `opts = JSON.parse(options)` (:123–124). **No allowlist,
    no key validation** — `opts` is handed to `mixpanelImport(creds, data, opts)` as-is (:181).
    An `identityReplay` object in the options JSON flows through unmodified.
  - Production forcing: `opts.abridged = true` if undefined (:127–129); `opts.manualGc = true`
    (:133–135); `dryRun` capped to `maxRecords = 100` (:139–141).
  - `opts.progressCallback = createProgressCallback(jobId)` (:148).
  - Data source: `cloudPaths` JSON (:161–164) or `jobStatuses.get(jobId).filePaths` from prepare
    (:166–175).
  - Result: logged (:183–199), silent-failure heuristic (:204–213), then
    `filterResultForClient(result)` (:216) → `updateJobStatus(jobId, "completed", null, filteredResult)`
    (:219) → `ws.send({ type: "job-complete", result: filteredResult })` (:222–227).

### transformCode compilation — exact mechanism, all three sites

`opts.transformFunc = eval(`(${transformCode})`)` — raw eval of a user-authored function string,
wrapped in parens to support arrow/anonymous functions. **No vm module, no sandbox, no timeout;
runs with full server privileges.** Errors are caught and surfaced as
`Transform function error: <msg>`.

| Site | Line | Error handling |
|---|---|---|
| WS `executeJobOverWebSocket` | ui/server.js:154 | throws → job-error over WS (:229–241) |
| Legacy `POST /job` | ui/server.js:776 | 400 JSON (:777–783). A commented-out `new Function('data','heavy', code)` alternative sits at :775 |
| `POST /dry-run` | ui/server.js:1233 | 400 JSON (:1234–1239) |

Pattern to imitate for identityReplay: **don't**. `isUserId` should travel as a regex string inside
`options.identityReplay` (JSON-safe); `normalizeOptions` in components/identity-replay.js:61–92
compiles it — function passthrough (:72–74), RegExp with g/y flags stripped (:75–79), string via
`new RegExp(...)` with a friendly error (:80–89). Predicate *functions* are module-API-only, same
stance as the CLI (components/cli.js:423–424: "the CLI cannot pass an isUserId function").

### Option whitelisting — the critical asymmetry

- **Import routes: NO allowlist.** `/job` (:745), WS path (:124), `/dry-run` (:1220), `/sample`
  (:979), `/columns` (:1099) all do `JSON.parse(options || "{}")` and pass through. `identityReplay`
  survives. (`/sample` and `/columns` then force-override transform-ish keys (:983–999, :1103–1119)
  but never strip unknown keys.)
- **Export routes: explicit construction = implicit allowlist.**
  - `/export` creds built field-by-field (:1422–1431, includes `secondToken`); opts built
    field-by-field (:1443–1469): recordType, region, workers, start, end, epochStart, epochEnd,
    whereClause, limit, logs, verbose, showProgress, writeToFile, where, outputFilePath, abridged,
    compress, compressionLevel, gcpProjectId, gcsCredentials, s3Region, s3Key, s3Secret. **Anything
    else in the POST body is dropped.** `identityReplay` must be added here explicitly.
  - `/export-dry-run` same pattern (:1751–1778), plus forces `dryRun: true`, `limit ≤ 100`,
    `writeToFile: false`.
  - **Existing proof of the hazard:** `secondRegion` is collected by the client (export.js) and
    logged by the server (:1659, :1703 — `destRegion: exportData.secondRegion || opts.region`) but
    is **never set on `opts`**, even though components/job.js:265 reads `opts.secondRegion`. The
    explicit construction already silently dropped a real option once.

### How /dry-run works (ui/server.js:1213–1330)

- Forces `opts.dryRun = true`, `opts.maxRecords = 100` (:1225–1226). Transform eval at :1233.
- Runs the import **twice**:
  1. Raw pass (:1279–1287): `rawOpts = { ...opts }` (shallow copy) with `transformFunc = null`,
     `fixData = false`, `removeNulls = false`, `flattenData = false`, `vendor = ""`,
     `maxRecords = 100` → `rawResult = await mixpanelImport(creds, data, rawOpts)`.
  2. Transformed pass (:1290): `transformedResult = await mixpanelImport(creds, data, opts)`.
- Response (:1304–1309): `{ success: true, result: transformedResult, previewData:
  transformedResult.dryRun || [], rawData: rawResult.dryRun || [] }`.
- **`result` here is the FULL unfiltered ImportResults** — `filterResultForClient` is not applied
  to /dry-run. So once identityReplay is set on the options, replay telemetry
  (`result.identityReplay`, set at the stage's `_flush`) should already appear in dry-run
  responses with zero server changes (verify at build time — dryRun collects records pre-batcher,
  telemetry lands at flush).
- Dry-run records preview = `previewData` (array of post-transform records) vs `rawData`
  (pre-transform), rendered by import.js as a before/after diff.

### Result filtering before job-complete

`filterResultForClient(result)` (ui/server.js:418–435). Exact `allowedFields` (:419–425):

```
'recordType', 'total', 'success', 'failed', 'empty', 'outOfBounds',
'duplicates', 'startTime', 'endTime', 'durationHuman', 'bytesHuman',
'requests', 'retries', 'rateLimit', 'wasStream', 'eps', 'rps', 'mbps',
'badRecords', 'vendor', 'vendorOpts', 'errors', 'responses', 'files', 'downloadUrl',
'stallsDetected', 'resumesAttempted', 'resumesSucceeded', 'filesSkippedMissing', 'bytesResumed'
```

- `identityReplay` is absent → `results.identityReplay` telemetry does **not** survive
  `job-complete` today. Add it to this array.
- Call sites: `executeJobOverWebSocket` (:216) and `signalJobComplete` (:438–465, filter at :440).
  `signalJobComplete` serves both legacy `/job` completions (:892) and all `/export` completions
  including export-import (:1617, :1709). One array edit covers every completion path, plus
  `GET /job/:jobId/status` (:1988–2009), which returns the stored (filtered) result.
- Doc location: docs/web-ui.md:60–70 ("Response Filtering") lists Included/Excluded keys — it is
  **already stale** (missing the five resilience keys at server.js:424). Update the doc's Included
  list when adding `identityReplay`.
- Library side is already done: components/job.js:1167–1168 sets
  `summary.identityReplay = this.identityReplayStats`, and the abridged `includeOnly` list keeps
  `"identityReplay"` (components/job.js:1216–1217) — production's forced `abridged: true` is safe.

### /export route — export-import handling (ui/server.js:1416–1741)

- `fileProducingTypes` (:1513) = `["export", "profile-export", "profile-delete", "group-export",
  "group-delete", "annotations", "get-annotations"]`. `export-import-*` types fall to the **else
  branch** (:1647–1731): child logger (:1650), `opts.progressCallback` wired (:1653), `res.json({
  jobId })` sent immediately (:1668–1672), then `await mixpanelImport(creds, null, opts)` (:1675)
  — note `data = null`; parsers build the export stream and rewrite recordType to `event`.
- Completion: `signalJobComplete(jobId, result)` (:1709) → filtered. Failure: `job-error` over WS
  (:1710–1730). Silent-destination-failure warn at :1698–1707.
- For replay-on-migration (`export-import-event` + identityReplay), this else branch is the code
  path; the explicit `opts` object (:1443–1469) is where `identityReplay` must be threaded.
- File-producing branch (:1516–1646) is irrelevant to replay except as the pattern for
  progress/complete plumbing.

### Library-side validation the server will surface as job-error

components/job.js:365–380 (JobConfig constructor):
- `identityReplay + fastMode` → throw (`identityReplay is incompatible with fastMode`).
- recordType must be `'event'` or `'export-import-event'` → throw otherwise (:369–371).
- `isUserId` required → throw (:372).
- `v2_compat` silently disabled when both set (:373–377).
- `export-import-event` forces `fixData = true` (:378–380).

These throws happen inside `mixpanelImport(...)`, so on the WS path they surface as a `job-error`
message (:229–241) — acceptable UX, but the client should pre-validate to avoid a round trip.

### CLI preview parity (for the front-end's live CLI-command string)

components/cli.js: `--identity-replay` (boolean, alias `identityReplay`, :390–396),
`--ir-user-id-regex` (string, :397–402), `--ir-graph-path` (:403–408), `--ir-on-ambiguous`
(choices drop|resolve|error, :409–414). Cross-flag checks at :422–429. Flags composed into the
`identityReplay` option group at :438–446. Only these four are CLI-exposed; everything else in
`identityReplayOpts` (index.d.ts:1155–1256: graph, maxGraphSize, onGraphOverflow, identityEvents,
associationEventName, associationTimestamp, associationProps, scrubExportProps, scrubJunkIds,
electionScope, bareDistinctId, userIdFallbackProps, denylist, onAmbiguous, minAssociationRate,
graphPath) is module-API. The UI can expose more than the CLI since options JSON passes through,
but the CLI-preview string can only render the four `--ir-*` flags.

### Misc plumbing facts

- Body parsing: `express.json({ limit: "2000mb" })` (:577) for `/export`* JSON bodies; import
  routes are multipart via multer disk storage into `tmpDir` (:537–552), 30 MB/file cap in
  production (:533–535).
- `updateJobStatus` (:369–410) stores only status + result (never progress) in `jobStatuses`;
  progress is WS-ephemeral via `createProgressCallback` (:468–486) — callback signature
  `(recordType, processed, requests, eps, bytesProcessed, downloadMessage)`.
- Job cleanup: `cleanupOldJobs` every 5 min (:2041–2132), 1-hour max age, maps pruned at >100.

## Gotchas

1. **`filterResultForClient` is the one place replay telemetry dies** (ui/server.js:419–425). Add
   `'identityReplay'` there; every completion path (WS, legacy /job, /export, export-import, REST
   status polling) is covered by that single edit.
2. **`/export` and `/export-dry-run` strip unknown option keys** by construction (:1443–1469,
   :1763–1778). `identityReplay` must be explicitly added to both. Precedent: `secondRegion` is
   already silently dropped there today (collected client-side, logged at :1659/:1703, never set
   on opts even though components/job.js:265 reads `opts.secondRegion`) — consider fixing that
   pre-existing gap in the same PR since export-import is exactly the replay use case.
3. **/dry-run's raw pass shallow-copies opts** (:1279) — it nulls transforms but would keep
   `identityReplay`, so the replay stage (and its identity graph) would run in BOTH passes:
   doubled memory, misleading `rawData`, and doubled work. Add `rawOpts.identityReplay = null`
   next to `rawOpts.transformFunc = null`.
4. **Send `isUserId` as a regex string in options JSON.** Functions do not survive
   `JSON.parse`; the library compiles strings (components/identity-replay.js:80–89). Never route
   isUserId through the transformCode `eval` path.
5. **recordType constraint**: components/job.js:369–371 throws unless `event` or
   `export-import-event`. On the import UI, replay only makes sense with recordType=event; on the
   export UI, only with export-import-event. Guard client-side; server needs no guard (throw
   becomes a job-error).
6. **`/dry-run` returns the FULL unfiltered result** (:1306) — replay telemetry appears there for
   free, and so does everything the job-complete filter normally excludes. Don't "fix" this by
   filtering, the import.js dry-run panel may rely on extra keys.
7. **docs/web-ui.md:62–70 (Response Filtering) is stale** — the code's allowlist already has five
   resilience keys the doc lacks. Update the doc's Included list alongside the code change.
8. **Production forces `abridged: true`** (:127) — safe: the library's abridged `includeOnly`
   already whitelists `identityReplay` (components/job.js:1216–1217).
9. `/job/prepare` stores only files, never options — no changes there; options ride the WS
   `start_job` message.
