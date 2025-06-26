# GOAL: Improve the performance of the mixpanel-import module

Several key bottlenecks exist in the current codebase

  Primary Bottlenecks:

  1. HTTP Client (importers.js:1-5)
  - Uses got instead of undici (ak-fetch's choice)
  - Single HTTP agent with maxSockets: 100
  - No connection pooling optimization

  2. Stream Processing Overhead (pipelines.js:6)
  - Highland.js streams add significant overhead vs native streams
  - Multiple sequential transform operations (lines 80-224)
  - Each record passes through 8+ transform functions

  3. Low Concurrency (job.js:131)
  - Default workers: 10 vs ak-fetch's sophisticated concurrency
  - Conservative highWater settings
  - No dynamic queue resizing

  4. Inefficient Batching
  - Batches created after all transforms complete
  - No early batching to reduce memory pressure
  - Missing ak-fetch's circular buffer approach

  Specific Performance Killers:

  // importers.js - Individual request processing
  const req = await got(options); // Sequential, no connection reuse optimization

  // pipelines.js - Multiple transform steps
  _.filter() -> _.map() -> _.flatten() -> _.map() -> _.filter() -> _.batch()

  // job.js - Conservative defaults  
  this.workers = opts.workers || 10; // Too low

  Recommendations:

  1. Replace got with undici - immediate 2-3x performance gain
  2. Replace Highland.js with native streams - reduce transform overhead
  3. Increase default concurrency to 50-100 workers
  4. Implement connection pooling similar to ak-fetch
  5. Batch earlier in pipeline to reduce memory pressure
  6. Consider integrating ak-fetch directly as the HTTP layer

