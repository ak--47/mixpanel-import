// $ parsers
const { chunkForSize } = require("./parsers.js");

// $ native node streams
const { Transform, PassThrough, pipeline } = require('stream');
const { pipeline: pipelinePromise } = require('stream/promises');
const ParallelTransform = require('parallel-transform');

// $ garbage collection
const v8 = require('v8');

// $ networking + filesystem
const { exportEvents, exportProfiles, deleteProfiles } = require('./exporters');
const { flushLookupTable, flushToMixpanel, flushToMixpanelWithUndici } = require('./importers.js');
const { createEventSampler, createMemoryMonitor, applySmartDefaults } = require('./smart-config.js');
const { replaceAnnotations, getAnnotations, deleteAnnotations } = require('./meta.js');
const fs = require('fs');

// $ env
const cliParams = require('./cli.js');
const counter = cliParams.showProgress;
const { logger } = require('../components/logs.js');

// $ transforms
const { isNotEmpty } = require('./transforms.js');

// $ utils
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);

/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */
/** @typedef {import('../index').Options} Options */
/** @typedef {import('../index').Creds} Creds */
/** @typedef {import('../index').ImportResults} ImportResults */

/**
 * Creates a transform stream that checks existence and enforces maxRecords
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createExistenceFilter(job) {
	let terminated = false;
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(chunk, encoding, callback) {
			// If we've already terminated, don't process any more
			if (terminated) {
				return callback();
			}

			// Check maxRecords limit BEFORE processing
			if (job.maxRecords !== null && job.recordsProcessed >= job.maxRecords) {
				// Terminate the stream
				terminated = true;
				// Don't call this.end() - let the stream end naturally when source ends
				return callback();
			}

			// Count ALL records we process, including empty ones
			job.recordsProcessed++;

			// very small chance of mem sampling
			Math.random() <= 0.00005 ? job.memSamp() : null;

			const exists = isNotEmpty(chunk);
			if (exists) {
				callback(null, chunk);
			} else {
				job.empty++;
				callback(); // Skip this item but continue
			}
		}
	});
}

/**
 * Creates a transform stream for vendor transforms
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createVendorTransform(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			try {
				if (job.vendor && job.vendorTransform) {
					data = job.vendorTransform(data, job.heavyObjects);
				}
				callback(null, data);
			} catch (err) {
				callback(err);
			}
		}
	});
}

/**
 * Creates a transform stream for user-defined transforms
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createUserTransform(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			try {
				if (job.transformFunc) {
					data = job.transformFunc(data, job.heavyObjects);
				}
				callback(null, data);
			} catch (err) {
				callback(err);
			}
		}
	});
}

/**
 * Creates a transform stream that flattens arrays
 * Handles "exploded" transforms [{},{},{}] to emit single events {}
 * @returns {Transform}
 */
function createFlattenStream(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			if (Array.isArray(data)) {
				for (const item of data) {
					this.push(item);
				}
				callback();
			} else {
				callback(null, data);
			}
		}
	});
}

/**
 * Creates a transform stream for deduplication
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createDedupeTransform(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			try {
				if (job.dedupe) {
					data = job.deduper(data);
				}
				callback(null, data);
			} catch (err) {
				callback(err);
			}
		}
	});
}

/**
 * Creates a transform stream for post-transform existence filter
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createExistenceFilter2(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			const exists = isNotEmpty(data);
			if (exists) {
				callback(null, data);
			} else {
				job.empty++;
				callback(); // Skip but continue
			}
		}
	});
}

/**
 * Creates a transform stream for all helper transforms
 * Uses pre-computed array of active transforms for better performance
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createHelperTransforms(job) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			try {
				// Iterate through pre-computed active transforms
				for (const transform of job.activeTransforms) {
					// Some transforms mutate data, others return new data
					if (transform.mutates === false) {
						data = transform.fn(data);
					} else {
						transform.fn(data);
					}
				}
				callback(null, data);
			} catch (err) {
				callback(err);
			}
		}
	});
}

/**
 * Creates a transform stream that caches JSON stringification
 * @param {JobConfig} job
 * @param {WeakMap} jsonCache
 * @param {WeakMap} bytesCache
 * @returns {Transform}
 */
function createStringifyCacher(job, jsonCache, bytesCache) {
	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			const exists = isNotEmpty(data);
			if (!exists) {
				job.empty++;
				callback(); // Skip
				return;
			}

			// Cache JSON stringification and byte count using WeakMaps
			const jsonString = JSON.stringify(data);
			const byteLength = Buffer.byteLength(jsonString, 'utf-8');
			job.bytesProcessed += byteLength;

			// Store in WeakMaps (doesn't modify original object)
			jsonCache.set(data, jsonString);
			bytesCache.set(data, byteLength);

			callback(null, data);
		}
	});
}

/**
 * Creates a smart batching transform that respects BOTH count and size limits
 * Batches by 2000 records OR 9.85MB (whichever comes first)
 * @param {JobConfig} job
 * @param {WeakMap} bytesCache
 * @returns {Transform}
 */
function createSmartBatcher(job, bytesCache) {
	let buffer = [];
	let currentSize = 0;
	const maxCount = job.recordsPerBatch;
	const maxBytes = Math.floor(job.bytesPerBatch * 0.985); // Target 9.85MB to avoid going over 10MB

	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(chunk, encoding, callback) {
			// Stop batching if we've hit maxRecords
			if (job.maxRecords !== null && job.recordsProcessed > job.maxRecords) {
				return callback();
			}

			// Get cached size or calculate it
			const chunkSize = bytesCache.get(chunk) || Buffer.byteLength(JSON.stringify(chunk), 'utf-8');

			// Check if adding this chunk would exceed limits
			if (buffer.length > 0 && (
				buffer.length >= maxCount ||
				(currentSize + chunkSize > maxBytes)
			)) {
				// Emit current batch
				const batch = buffer;
				buffer = [];
				currentSize = 0;
				this.push(batch);
			}

			// Add chunk to buffer
			buffer.push(chunk);
			currentSize += chunkSize;

			// Check if we've hit the count limit after adding
			if (buffer.length >= maxCount) {
				const batch = buffer;
				buffer = [];
				currentSize = 0;
				callback(null, batch);
			} else {
				callback();
			}
		},
		flush(callback) {
			if (buffer.length > 0) {
				// Only flush if we haven't exceeded maxRecords
				if (job.maxRecords === null || job.recordsProcessed <= job.maxRecords) {
					callback(null, buffer);
				} else {
					callback();
				}
			} else {
				callback();
			}
		}
	});
}

/**
 * Creates a transform stream that batches records by count (DEPRECATED - use createSmartBatcher)
 * @param {number} batchSize
 * @param {number} highWater
 * @param {JobConfig} job
 * @returns {Transform}
 */
function createBatcher(batchSize, highWater = 16, job = null) {
	let buffer = [];
	let recordCount = 0;
	return new Transform({
		objectMode: true,
		highWaterMark: highWater,
		transform(chunk, encoding, callback) {
			// Stop batching if we've hit maxRecords
			if (job && job.maxRecords !== null && job.recordsProcessed > job.maxRecords) {
				return callback();
			}

			buffer.push(chunk);
			if (buffer.length >= batchSize) {
				const batch = buffer;
				buffer = [];
				callback(null, batch);
			} else {
				callback();
			}
		},
		flush(callback) {
			if (buffer.length > 0) {
				// Only flush if we haven't exceeded maxRecords
				if (!job || job.maxRecords === null || job.recordsProcessed <= job.maxRecords) {
					callback(null, buffer);
				} else {
					callback();
				}
			} else {
				callback();
			}
		}
	});
}

/**
 * Creates a transform stream that batches by size using cached bytes
 * @param {JobConfig} job
 * @param {WeakMap} bytesCache
 * @returns {Transform}
 */
function createSizeBatcher(job, bytesCache) {
	let currentBatch = [];
	let currentSize = 0;
	const maxBytes = job.bytesPerBatch; // Use the actual bytesPerBatch setting (10MB)

	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(batch, encoding, callback) {
			// If it's already a batch from the count batcher
			if (Array.isArray(batch)) {
				// Check if this batch needs to be split by size
				const chunks = [];
				let chunk = [];
				let chunkSize = 0;

				for (const item of batch) {
					const itemSize = bytesCache.get(item) || Buffer.byteLength(JSON.stringify(item), 'utf-8');

					if (chunkSize + itemSize > maxBytes && chunk.length > 0) {
						chunks.push(chunk);
						chunk = [];
						chunkSize = 0;
					}

					chunk.push(item);
					chunkSize += itemSize;
				}

				if (chunk.length > 0) {
					chunks.push(chunk);
				}

				// Push all chunks
				for (const c of chunks) {
					this.push(c);
				}
				callback();
			} else {
				// Single item case (shouldn't happen with our pipeline)
				callback(null, [batch]);
			}
		}
	});
}

/**
 * Creates a parallel transform stream for HTTP requests
 * @param {JobConfig} job
 * @param {WeakMap} jsonCache
 * @param {fs.WriteStream} fileStream
 * @param {number} gcThreshold
 * @returns {Transform}
 */
function createHttpSender(job, jsonCache, fileStream, gcThreshold) {
	const flush = job.transport === 'undici' ? flushToMixpanelWithUndici : flushToMixpanel;
	let batchId = 0;

	return new ParallelTransform(job.workers, {
		objectMode: true,
		highWaterMark: job.workers  // One batch per worker for proper backpressure
	}, async function(batch, callback) {
		try {
			const thisBatchId = ++batchId;
			job.requests++;
			job.batches++;
			job.addBatchLength(batch.length);

			// Trigger manual GC if enabled and threshold exceeded
			if (gcThreshold && process.memoryUsage().heapUsed > gcThreshold) {
				global.gc();
			}

			if (job.dryRun) {
				// Add batch ID for debugging
				batch._batchId = thisBatchId;
				return callback(null, [null, batch]);
			}

			if (job.writeToFile) {
				batch.forEach(item => {
					const cachedJson = jsonCache.get(item);
					fileStream.write((cachedJson || JSON.stringify(item)) + '\n');
				});
				return callback(null, [null, batch]);
			}

			const result = await flush(batch, job);
			callback(null, result);
		} catch (err) {
			callback(err);
		}
	});
}

/**
 * Creates a passthrough stream for logging/verbose output
 * @param {JobConfig} job
 * @param {number} LOG_INTERVAL
 * @returns {Transform}
 */
function createLogger(job, LOG_INTERVAL = 100) {
	let lastLogUpdate = Date.now();

	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(result, encoding, callback) {
			const [response, batch] = result;

			if (job.responseHandler && typeof job.responseHandler === 'function') {
				job.responseHandler(response, batch);
			}

			if (job.dryRun && batch) {
				if (Array.isArray(batch)) {
					// In dry run mode, collect individual records
					batch.forEach(data => {
						// Only push actual data objects, not metadata
						if (data && typeof data === 'object' && data !== batch) {  // Don't push the batch itself
							job.dryRunResults.push(data);
						}
					});
				}
			} else {
				const now = Date.now();
				if ((job.verbose || job.showProgress) && (now - lastLogUpdate >= LOG_INTERVAL)) {
					counter(job.recordType, job.recordsProcessed, job.requests, job.getEps(), job.success, job.failed, job.bytesProcessed, job.progressCallback);
					lastLogUpdate = now;
				}
			}

			callback(null, result);
		}
	});
}

/**
 * the core pipeline
 * @param {ReadableStream | null} stream
 * @param {JobConfig} job
 * @param {boolean} toNodeStream
 * @returns {Promise<ImportResults> | Transform} a promise or stream
 */
async function corePipeline(stream, job, toNodeStream = false) {
	const l = logger(job);

	// Special handling for non-streaming operations
	// @ts-ignore
	if (job.recordType === 'table') return flushLookupTable(stream, job);
	// @ts-ignore
	if (job.recordType === 'export' && typeof stream === 'string') return exportEvents(stream, job);
	// @ts-ignore
	if (job.recordType === 'profile-export' && typeof stream === 'string') return exportProfiles(stream, job);
	// @ts-ignore
	if (job.recordType === 'annotations') return replaceAnnotations(stream, job);

	if (job.recordType === 'get-annotations') return getAnnotations(job);
	if (job.recordType === 'delete-annotations') return deleteAnnotations(job);
	if (job.recordType === 'profile-delete') return deleteProfiles(job);

	// Manual GC setup (if enabled)
	let gcThreshold = null;
	if (job.manualGc && global.gc) {
		const heapStats = v8.getHeapStatistics();
		gcThreshold = heapStats.heap_size_limit * 0.85; // Trigger GC at 85% heap usage
		l(`Manual GC enabled: will trigger at ${(gcThreshold / 1024 / 1024).toFixed(0)} MB (85% of ${(heapStats.heap_size_limit / 1024 / 1024).toFixed(0)} MB heap)`);
	} else if (job.manualGc && !global.gc) {
		l(`Manual GC requested but global.gc not available. Start Node with --expose-gc flag.`);
	}

	let fileStream;
	if (job.writeToFile) {
		fileStream = fs.createWriteStream(job.outputFilePath, { flags: 'a', highWaterMark: job.highWater });
	}

	// Cache for JSON strings to avoid re-stringifying
	const jsonCache = new WeakMap();
	const bytesCache = new WeakMap();

	// Create base transform stages
	const stages = [];

	// Only add adaptive stages if enabled (opt-in)
	if (job.adaptive === true) {
		applySmartDefaults(job);
		stages.push(createEventSampler(job)); // Sample events to determine optimal configuration
		stages.push(createMemoryMonitor(job)); // Monitor memory usage
	}

	// Add core transform stages
	stages.push(
		createExistenceFilter(job),
		createVendorTransform(job),
		// Note: Vendor transforms are 1:1, no flatten needed
		createUserTransform(job),
		createFlattenStream(job), // User transforms can explode 1 row to multiple
		createDedupeTransform(job),
		createExistenceFilter2(job),
		createHelperTransforms(job),
		createStringifyCacher(job, jsonCache, bytesCache),
		createSmartBatcher(job, bytesCache), // Combined count + size batching
		createHttpSender(job, jsonCache, fileStream, gcThreshold),
		createLogger(job)
	);

	// For createMpStream - return the entry point of the pipeline
	if (toNodeStream) {
		// Create the pipeline by chaining stages
		const pipelineStream = stages.reduce((prev, curr, index) => {
			if (index === 0) return curr;
			return prev.pipe(curr);
		}, null);

		return stages[0]; // Return the first transform in the chain
	}

	// Pipe the input stream through our pipeline
	if (stream) {
		// Create an array to collect results
		const results = [];

		// Get the last stage to listen for results
		const lastStage = stages[stages.length - 1];

		// Log exactly once, on the very first data record
		lastStage.once('data', () => {
			l(`\n\nDATA FLOWING\n`);
		});

		// Set up result collection from the last stage
		lastStage.on('data', (result) => {
			results.push(result);
		});

		// Use the promise-based pipeline for proper error handling and completion
		await pipelinePromise(
			stream,
			...stages
		);

		// Clean up file stream if used
		if (fileStream) {
			fileStream.end();
		}

		return results;
	}

	// Should not reach here
	throw new Error('Stream is required for pipeline');
}

module.exports = {
	corePipeline
};