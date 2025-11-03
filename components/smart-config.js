/**
 * Smart configuration system that samples events early and adjusts settings
 * BEFORE creating the pipeline to ensure optimal performance
 */

const { Transform } = require('stream');

/**
 * Configurable event size categories and their worker limits
 * Easily adjust these thresholds based on your environment
 */
const EVENT_SIZE_CATEGORIES = {
    tiny: {
        maxBytes: 500,
        maxWorkers: 50,
        description: '< 500 bytes'
    },
    small: {
        maxBytes: 2048, // 2KB
        maxWorkers: 30,
        description: '500 bytes - 2KB'
    },
    medium: {
        maxBytes: 5120, // 5KB
        maxWorkers: 15,
        description: '2KB - 5KB'
    },
    large: {
        maxBytes: 10240, // 10KB
        maxWorkers: 8,
        description: '5KB - 10KB'
    },
    dense: {
        maxBytes: Infinity,
        maxWorkers: 5,
        description: '> 10KB'
    }
};

/**
 * Calculate optimal workers based on event size
 * This provides a resilient default that prevents OOM
 */
function calculateOptimalWorkers(avgEventSize, requestedWorkers = 10) {
    // Memory safety calculations
    const heapLimit = require('v8').getHeapStatistics().heap_size_limit;
    const safeHeapUsage = heapLimit * 0.6; // Use max 60% of heap for safety

    // Calculate memory per worker
    // Account for: batch in memory + HTTP payload + parallel-transform queue
    const eventsPerBatch = Math.min(2000, Math.floor(10 * 1024 * 1024 / avgEventSize));
    const memoryPerBatch = eventsPerBatch * avgEventSize;
    const memoryPerWorker = memoryPerBatch * 3; // 3x for safety (queue, processing, HTTP)

    // Calculate max safe workers
    const maxSafeWorkers = Math.floor(safeHeapUsage / memoryPerWorker);

    // Determine category based on event size
    let category = 'tiny';
    let recommendedWorkers = requestedWorkers;

    for (const [catName, catConfig] of Object.entries(EVENT_SIZE_CATEGORIES)) {
        if (avgEventSize <= catConfig.maxBytes) {
            category = catName;
            recommendedWorkers = Math.min(requestedWorkers, catConfig.maxWorkers);
            break;
        }
    }

    // Apply memory safety limit
    const finalWorkers = Math.max(1, Math.min(recommendedWorkers, maxSafeWorkers));

    return {
        workers: finalWorkers,
        category,
        batchSize: eventsPerBatch,
        memoryPerWorker: memoryPerWorker / 1024 / 1024, // In MB
        reasoning: finalWorkers < requestedWorkers
            ? `Limited from ${requestedWorkers} to ${finalWorkers} workers due to ${category} events (${(avgEventSize/1024).toFixed(1)}KB avg)`
            : `Using ${finalWorkers} workers for ${category} events (${(avgEventSize/1024).toFixed(1)}KB avg)`
    };
}

/**
 * Create a sampling transform that analyzes the first N events
 * and configures the job optimally
 */
function createEventSampler(job, sampleSize = 100) {
    let samples = [];
    let configured = false;
    let eventCount = 0;

    return new Transform({
        objectMode: true,
        highWaterMark: 16,
        transform(data, encoding, callback) {
            eventCount++;

            // Sample events until we have enough
            if (!configured && samples.length < sampleSize) {
                const size = Buffer.byteLength(JSON.stringify(data), 'utf8');
                samples.push(size);

                // Once we have enough samples, configure
                if (samples.length === sampleSize || eventCount >= sampleSize * 2) {
                    configured = true;

                    // Calculate statistics
                    const sum = samples.reduce((a, b) => a + b, 0);
                    const avgSize = Math.ceil(sum / samples.length);
                    const maxSize = Math.max(...samples);
                    const minSize = Math.min(...samples);

                    // Store in job for later use
                    job.detectedEventSize = avgSize;
                    job.eventSizeStats = { avg: avgSize, max: maxSize, min: minSize, samples: samples.length };

                    // Calculate optimal configuration
                    const config = calculateOptimalWorkers(avgSize, job.originalWorkers || job.workers);

                    // Apply configuration if adaptive scaling is enabled
                    if (job.adaptiveScaling !== false) {
                        job.workers = config.workers;
                        job.adaptiveWorkers = config.workers;
                        job.recordsPerBatch = Math.min(job.recordsPerBatch || 2000, config.batchSize);

                        // Adjust high water mark based on event size
                        if (avgSize > 5120) {
                            // For large events, use smaller buffers
                            job.highWater = Math.min(job.workers * 5, 50);
                        } else {
                            // For small events, can use larger buffers
                            job.highWater = Math.min(job.workers * 20, 200);
                        }

                        // Log the configuration
                        console.log(`
╔══════════════════════════════════════════════════════════════════╗
║                    ADAPTIVE CONFIGURATION                         ║
╠══════════════════════════════════════════════════════════════════╣
║ Event Analysis (sample: ${samples.length} events)                              ║
║   • Average size: ${(avgSize / 1024).toFixed(2)} KB                              ║
║   • Range: ${(minSize / 1024).toFixed(2)} KB - ${(maxSize / 1024).toFixed(2)} KB                       ║
║   • Category: ${config.category.toUpperCase()}                                       ║
║                                                                    ║
║ Optimized Settings:                                               ║
║   • Workers: ${config.workers} (requested: ${job.originalWorkers || job.workers})                           ║
║   • Batch size: ${job.recordsPerBatch} events                              ║
║   • Buffer size: ${job.highWater} objects                                 ║
║   • Memory/worker: ~${config.memoryPerWorker.toFixed(1)} MB                          ║
║                                                                    ║
║ ${config.reasoning}${' '.repeat(Math.max(0, 66 - config.reasoning.length))}║
╚══════════════════════════════════════════════════════════════════╝
                        `);
                    } else {
                        console.log(`Event size detected: ${(avgSize / 1024).toFixed(2)} KB average (adaptive scaling disabled)`);
                    }

                    // Clear samples to free memory
                    samples = null;
                }
            }

            // Pass through the data
            callback(null, data);
        }
    });
}

/**
 * Create a memory monitor that can trigger emergency measures
 */
function createMemoryMonitor(job) {
    const CHECK_INTERVAL = 5000; // Check every 5 seconds
    let lastCheck = Date.now();
    let warningCount = 0;

    return new Transform({
        objectMode: true,
        highWaterMark: job.highWater,
        transform(data, encoding, callback) {
            const now = Date.now();

            if (now - lastCheck >= CHECK_INTERVAL) {
                lastCheck = now;

                const memUsage = process.memoryUsage();
                const heapUsed = memUsage.heapUsed;
                const heapLimit = require('v8').getHeapStatistics().heap_size_limit;
                const heapPercent = (heapUsed / heapLimit) * 100;

                // Emergency brake at 90% heap
                if (heapPercent > 90) {
                    console.error(`
🚨 CRITICAL MEMORY WARNING: ${heapPercent.toFixed(1)}% heap used!
   Implement these immediately:
   1. Reduce workers to 1-2
   2. Reduce batch size
   3. Enable compression
   4. Consider filtering unnecessary properties
                    `);

                    // Force GC if available
                    if (global.gc) {
                        global.gc();
                        console.log('🗑️  Forced garbage collection');
                    }

                    // Slow down processing
                    setTimeout(() => callback(null, data), 500);
                    return;
                }

                // Warning at 75% heap
                if (heapPercent > 75) {
                    warningCount++;
                    if (warningCount % 10 === 1) { // Don't spam warnings
                        console.warn(`⚠️  Memory pressure: ${heapPercent.toFixed(1)}% heap used. Consider reducing workers.`);
                    }
                }

                // Log memory status periodically in verbose mode
                if (job.verbose && heapPercent > 50) {
                    console.log(`Memory: ${heapPercent.toFixed(1)}% heap, ${(heapUsed / 1024 / 1024).toFixed(0)} MB used`);
                }
            }

            callback(null, data);
        }
    });
}

/**
 * Apply smart configuration to a job before pipeline creation
 */
function applySmartDefaults(job) {
    // Store original worker count
    job.originalWorkers = job.workers;

    // Enable adaptive scaling by default
    if (job.adaptiveScaling === undefined) {
        job.adaptiveScaling = true;
    }

    // If user provided average event size, use it immediately
    if (job.avgEventSize) {
        const config = calculateOptimalWorkers(job.avgEventSize, job.workers);
        if (job.adaptiveScaling) {
            job.workers = config.workers;
            job.adaptiveWorkers = config.workers;
            console.log(`Using provided event size (${(job.avgEventSize/1024).toFixed(1)}KB): ${config.reasoning}`);
        }
    }

    // Set memory-aware defaults
    const heapLimit = require('v8').getHeapStatistics().heap_size_limit;
    const heapLimitMB = heapLimit / 1024 / 1024;

    // Adjust defaults based on available memory
    if (heapLimitMB < 512) {
        // Low memory environment
        job.workers = Math.min(job.workers, 5);
        job.highWater = Math.min(job.highWater, 50);
        console.log(`Low memory environment detected (${heapLimitMB.toFixed(0)}MB). Limiting to ${job.workers} workers.`);
    } else if (heapLimitMB < 2048) {
        // Medium memory environment
        job.workers = Math.min(job.workers, 20);
    }

    return job;
}

module.exports = {
    calculateOptimalWorkers,
    createEventSampler,
    createMemoryMonitor,
    applySmartDefaults
};