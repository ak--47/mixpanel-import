# Web UI

Browser front-end for imports and exports. Served by [ui/server.js](../ui/server.js) (Express 5 + `ws`),
static assets in [ui/public/](../ui/public/).

**Start:** `npm start`, `node index.js --ui`, or `npx mixpanel-import --ui`.
**Dev (nodemon):** `npm run ui:dev`.

The `--ui` arm of [index.js](../index.js) deliberately installs **no** signal handlers, so
`ui/server.js` owns its own graceful shutdown. See the process-global handler rule in
[CLAUDE.md](../CLAUDE.md).

## Tools

| Route | Tool | Purpose |
|---|---|---|
| `/` | Landing page | Choose import or export |
| `/import` | E.T.L | Events, user profiles, group profiles, lookup tables |
| `/export` | L.T.E | Event exports, profile export/delete, annotations, SCD, export-import |

Import supports local upload (drag & drop), `gs://` and `s3://` paths; JSON, JSONL, CSV, Parquet,
each with optional `.gz`. Transforms are authored in an embedded Monaco editor. Both tools render a
live CLI-command preview and support dry runs.

## HTTP Endpoints

| Method | Path | Notes |
|---|---|---|
| GET | `/` `/import` `/export` | Pages |
| POST | `/job/prepare` | Multipart upload; returns `jobId`. Preferred path — see [websocket-hybrid-pattern.md](websocket-hybrid-pattern.md) |
| POST | `/job` | Legacy full import over HTTP; kept for backward compatibility |
| POST | `/sample` | Raw preview, 500 records max, no transforms |
| POST | `/columns` | Column detection for the field mapper |
| POST | `/dry-run` | Import dry run, 100 records max |
| POST | `/export` | Run an export; returns `jobId`, progress over WebSocket |
| POST | `/export-dry-run` | Export dry run, 100 records max |
| POST | `/snowcat/request` | Snowcat job submission (`jobType`: `import` \| `export`) |
| GET | `/download/:jobId/:filename` | Download one exported file |
| GET | `/download/:jobId` | Download the job's export directory contents |
| GET | `/job/:jobId/status` | Poll job status (REST alternative to WebSocket) |
| DELETE | `/job/:jobId` | Manual job cleanup |
| GET | `/health` | Health check; reports active/tracked jobs and GC availability |
| GET | `/memory` | Heap stats; `?gc=true` triggers GC when `--expose-gc` is set |
| GET | `/browse-gcs` | List objects under `etl_ui_jobs/` in the `snowcat` bucket only |

## WebSocket Messages

| Direction | Type | Payload |
|---|---|---|
| Client → Server | `register-job` | `{ jobId }` — subscribe to an already-running job |
| Client → Server | `start_job` | `{ jobId, credentials, options, cloudPaths, transformCode }` — run the job on this socket |
| Server → Client | `progress` | events/sec, records processed, memory usage |
| Server → Client | `job-complete` | filtered result payload |
| Server → Client | `job-error` | error details |

The socket doubles as the Cloud Run keep-alive: an open WebSocket is an active HTTP request, so the
container cannot be suspended mid-job. Full rationale in
[websocket-hybrid-pattern.md](websocket-hybrid-pattern.md).

## Response Filtering

Result payloads are trimmed before being sent to the client.

**Included:** `recordType`, `total`, `success`, `failed`, `empty`, `outOfBounds`, `duplicates`,
`startTime`, `endTime`, `durationHuman`, `bytesHuman`, `requests`, `retries`, `rateLimit`,
`wasStream`, `eps`, `rps`, `mbps`, `badRecords`, `vendor`, `vendorOpts`, `errors`, `responses`,
`files`, `downloadUrl`

**Excluded:** `memory`, `memoryHuman`, `duration`, `avgBatchLength`, `percentQuota`, `transport`,
`batches`, `serverErrors`, `clientErrors`, `version`, `workers`, `dryRun`

## Logging

Pino, configured per environment in [components/logs.js](../components/logs.js) and `ui/server.js`.

| Env | Level | Format |
|---|---|---|
| Production | `info` | GCP structured logging |
| Local | `debug` | `pino-pretty`, colorized |
| Test | `warn` | minimal |

Levels: `debug` operational detail · `info` lifecycle milestones · `warn` non-fatal (cleanup
failures) · `error` fatal.

Every job uses a child logger carrying `jobId`, so all lines for one job correlate:

```
jsonPayload.jobId="job-1234567890-abc"
```

## Deployment

Cloud Run, via `npm run deploy` (`gcloud builds submit --config cloudbuild.yaml`). Set the service
timeout to match the longest expected job (max 60 min):

```bash
gcloud run services update mixpanel-import --timeout=3600 --region=us-central1
```

`tmp/` holds uploads and is ephemeral in serverless environments.
