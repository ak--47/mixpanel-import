# Performance Tuning Guide for mixpanel-import

## Understanding the Problem

When importing large datasets, especially from cloud storage (GCS/S3), memory issues can occur due to:

1. **Speed mismatch**: Cloud storage can stream at 100MB/s+ while Mixpanel APIs accept ~5-10MB/s
2. **Event density**: Large events (>10KB) require different settings than small events (<1KB)
3. **Transform overhead**: Vendor transforms (like PostHog) can significantly increase event size

## Key Performance Parameters

### `workers` (default: 10)
Controls the number of concurrent HTTP requests to Mixpanel.
- **More workers (20-50)**: Better for small events, higher throughput
- **Fewer workers (5-10)**: Better for large/dense events, prevents memory accumulation
- **Memory impact**: Each worker buffers ~3 batches worth of data

### `highWater` (default: min(workers * 10, 100))
Controls the stream buffer size (number of objects) between pipeline stages.
- **Lower values (16-50)**: Less memory usage, better for dense events
- **Higher values (100-500)**: Better throughput for small events
- **Memory impact**: Directly affects how much data is buffered in memory

### `recordsPerBatch` (default: 2000)
Number of records per HTTP request.
- **Smaller batches (500-1000)**: Better for large events, reduces memory per request
- **Larger batches (2000)**: Better for small events, fewer HTTP requests
- **Limit**: Max 2000 for events, 200 for groups

### `adaptive` (default: false)
Automatically adjusts workers and batch size based on event sampling.
- **Enable**: For unknown or variable event sizes
- **Disable**: When you know your event characteristics or using vendor transforms
- **Note**: Samples AFTER transforms, may not work well with transforms that increase size

### `compress` (default: true)
Gzip compression for HTTP requests.
- **Always enable**: Reduces network payload by 70-90%
- **Only disable**: For debugging or if Mixpanel endpoint doesn't support it

### `dedupe` (default: false for events)
Deduplication based on event hash.
- **WARNING**: Can cause memory leaks with large datasets (stores all hashes)
- **Only use**: For small datasets or user/group profiles
- **Never use**: For event imports with millions of records

## Configuration Examples

### Dense Events (PostHog, >10KB each)
```javascript
{
  workers: 10,           // Moderate concurrency
  highWater: 50,         // Small buffer
  recordsPerBatch: 500,  // Small batches
  compress: true,        // Always compress
  adaptive: false,       // Disable (transforms confuse it)
  dedupe: false          // Never for events
}
```

### Small Events (<1KB each)
```javascript
{
  workers: 30,           // High concurrency
  highWater: 200,        // Larger buffer
  recordsPerBatch: 2000, // Max batch size
  compress: true,        // Always compress
  adaptive: true,        // Can auto-tune
  dedupe: false          // Avoid for large datasets
}
```

### Memory-Constrained Environment
```javascript
{
  workers: 5,            // Minimal concurrency
  highWater: 16,         // Minimal buffer
  recordsPerBatch: 500,  // Small batches
  compress: true,        // Reduce payload
  adaptive: false,       // Manual control
  forceStream: true      // Always stream files
}
```

### Unknown Event Size (Let It Auto-Tune)
```javascript
{
  adaptive: true,        // Enable auto-tuning
  compress: true,        // Always compress
  // Don't set workers/highWater - let adaptive scaling handle it
}
```

## Memory Management Best Practices

1. **Use `abridged: true`** for production imports
   - Prevents storing all responses/errors in memory
   - Essential for large datasets

2. **Process files sequentially** if hitting memory limits
   - Instead of passing array of files, process one at a time
   - Add delays between files for garbage collection

3. **Monitor memory usage**
   - Run with `--expose-gc` flag to enable manual GC
   - Use `node --max-old-space-size=4096` to increase heap limit

4. **Avoid memory leaks**
   - Never use `dedupe: true` for large event imports
   - Always use `abridged: true` for production
   - Ensure vendor transforms don't accumulate state

## Troubleshooting

### Out of Memory (OOM) Errors
1. Reduce `workers` to 5-10
2. Reduce `highWater` to 16-50
3. Reduce `recordsPerBatch` to 500
4. Enable `compress: true`
5. Use `abridged: true`
6. Process files one at a time

### Slow Performance
1. Increase `workers` (if memory allows)
2. Increase `highWater` (if memory allows)
3. Increase `recordsPerBatch` to 2000
4. Ensure `compress: true`
5. Use `transport: 'undici'` (more efficient)

### GCS/S3 Overwhelming Pipeline
The issue: Cloud storage downloads too fast, overwhelming slower Mixpanel API.
Solutions:
1. Reduce `workers` to slow processing
2. Reduce `highWater` to limit buffering
3. Process files sequentially
4. Consider implementing custom throttling

## Performance Metrics

Monitor these metrics to tune performance:
- **eps** (events per second): Target throughput
- **mem** (memory usage): Should stay under 2GB
- **proc** (processed bytes): Cumulative data processed
- **rps** (requests per second): HTTP request rate
- **mbps** (megabytes per second): Network throughput

## Advanced: Understanding the Pipeline

The data flows through these stages:
1. **Source Stream** (GCS/S3/File) → Downloads data
2. **Parse Stream** → Converts to JSON objects
3. **Vendor Transform** → Applies vendor-specific transforms
4. **User Transform** → Applies custom transforms
5. **Helper Transforms** → Fixes time, aliases, etc.
6. **Dedupe** (if enabled) → Removes duplicates
7. **Batcher** → Groups into batches
8. **HTTP Sender** → Sends to Mixpanel (parallel workers)

Each stage has its own buffer controlled by `highWater`.

## Rule of Thumb

**Memory Usage ≈ workers × recordsPerBatch × avgEventSize × 3**

The `×3` factor accounts for:
- Records in worker queue
- Records being processed
- HTTP payload being sent

Example: 10 workers × 2000 records × 10KB × 3 = ~600MB

Keep this under 50% of available heap for safety.