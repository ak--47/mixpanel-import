# WebSocket Hybrid Pattern for Cloud Run

## Problem Solved

Cloud Run containers can be suspended when there's no active network activity. The previous approach sent HTTP responses immediately, then ran jobs asynchronously, which could lead to container suspension mid-job.

## Solution: Hybrid Upload + WebSocket Execution

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      LOCAL FILE MODE                         │
│                                                              │
│  1. POST /job/prepare (multipart upload)                    │
│     → Returns jobId immediately                              │
│     → Files saved to tmp directory                           │
│                                                              │
│  2. WebSocket connect                                        │
│     → Send "start_job" with jobId                           │
│                                                              │
│  3. Server runs import over WebSocket                       │
│     → Progress updates keep connection alive                │
│     → Job completes via WebSocket                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                   CLOUD STORAGE MODE                         │
│                                                              │
│  1. WebSocket connect (no file upload!)                     │
│     → Client generates jobId                                 │
│     → Send "start_job" with cloudPaths                      │
│                                                              │
│  2. Server streams from cloud storage                       │
│     → Progress updates keep connection alive                │
│     → Job completes via WebSocket                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Key Changes

### Server Side (`ui/server.js`)

**1. Endpoint: `POST /job/prepare`**
- Handles file uploads (multipart/form-data)
- Returns `jobId` immediately
- Stores file paths in `jobStatuses` map
- HTTP response completes quickly

**2. WebSocket handler**
- `start_job` message type handler
- Receives: `{ type: 'start_job', jobId, credentials, options, cloudPaths, transformCode }`
- Calls `executeJobOverWebSocket()`

**3. `executeJobOverWebSocket()`**
- Handles both local files and cloud storage
- Local mode: retrieves file paths from `jobStatuses.get(jobId)`
- Cloud mode: uses `cloudPaths` directly
- Runs the entire import over WebSocket (keeps container alive)
- Sends progress updates via WebSocket
- Sends completion/error via WebSocket
- Cleans up temp files after completion

### Client Side (`ui/public/import.js`)

**1. Submit flow**
```javascript
// Dry runs - use the dry-run endpoint
if (isDryRun) {
  await fetch('/dry-run', { method: 'POST', body: formData });
}

// Local file mode
else if (fileSource === 'local') {
  // Step 1: Upload files
  const { jobId } = await fetch('/job/prepare', {
    method: 'POST',
    body: formData
  }).then(r => r.json());

  // Step 2: Execute via WebSocket
  this.executeJobViaWebSocket(jobId, fileSource);
}

// Cloud storage mode
else {
  // Direct to WebSocket (no upload!)
  this.executeJobViaWebSocket(null, fileSource);
}
```

**2. `executeJobViaWebSocket()`**
- Connects WebSocket
- Generates `jobId` if not provided (cloud mode)
- Collects credentials and options
- Sends `start_job` message with all job data
- Receives progress updates and completion

**3. Helper methods**
- `collectCredentials()` - extracts credentials without FormData
- `collectOptions()` - extracts options without FormData

## Why This Works on Cloud Run

### WebSocket IS the Long-Running HTTP Request

According to [Cloud Run WebSocket docs](https://cloud.google.com/run/docs/triggering/websockets):

> "WebSockets requests are treated as long-running HTTP requests in Cloud Run. They are subject to request timeouts (currently up to 60 minutes)"

**Key insight:** The WebSocket connection itself counts as an active HTTP request.

### Flow Timeline

```
0s   → Client uploads files to /job/prepare
      → HTTP response sent with jobId
      → HTTP request DONE (container could suspend...)

0s   → BUT client immediately opens WebSocket
      → WebSocket handshake = new HTTP request
      → Client sends "start_job" message

0s   → Server receives start_job
      → Begins running import
      → WebSocket connection stays OPEN

5s   → Progress update sent via WebSocket (activity!)
10s  → Progress update sent via WebSocket (activity!)
15s  → Progress update sent via WebSocket (activity!)
...
300s → Job completes
      → Completion sent via WebSocket
      → WebSocket closes
      → Container can now suspend (job done!)
```

### Why the Container Stays Alive

1. **WebSocket handshake** = HTTP upgrade request (long-running)
2. **Progress updates** = bidirectional messages (network activity)
3. **Cloud Run sees an active HTTP request** = container not suspended
4. **Job runs to completion**

## Benefits

- **Cloud Run compatible** - WebSocket counts as an active HTTP request
- **Real-time progress** - client sees live updates
- **Efficient file upload** - uses multipart/form-data (not base64)
- **Cloud storage optimized** - no file upload needed for GCS/S3
- **Backward compatible** - the older `POST /job` endpoint still works
- **Clean architecture** - separation of upload vs execution

## Configuration

Ensure the Cloud Run timeout is set appropriately:

```bash
gcloud run services update mixpanel-import \
  --timeout=3600 \
  --region=us-central1
```

## Testing

### Local Files
1. Select local file
2. Fill in credentials
3. Click "Import"
4. See files upload → WebSocket connect → progress updates → completion

### Cloud Storage
1. Enter GCS/S3 path
2. Fill in credentials (including cloud creds)
3. Click "Import"
4. See WebSocket connect → progress updates → completion (no upload)

## Migration Notes

- **`POST /job`** - still works, kept for backward compatibility
- **Dry runs** - continue using `POST /dry-run`
- **Sample/preview** - continue using `POST /sample`
- **This pattern** - only affects real imports (not dry runs or previews)

## Logging

Jobs log with `jobId` correlation:

```javascript
const jobLogger = logger.child({ jobId });
jobLogger.info("job started via websocket");
```

Filter logs by jobId in Cloud Logging:
```
jsonPayload.jobId="job-1234567890-abc"
```
