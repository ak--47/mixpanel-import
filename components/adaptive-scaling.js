/**
 * Adaptive scaling system for automatic worker and batch size adjustment
 * Ensures resilient processing of any file size or event density
 */

class AdaptiveScaler {
    constructor(job) {
        this.job = job;
        this.samples = [];
        this.sampleSize = 100; // Sample first 100 events
        this.eventCount = 0;

        // Memory thresholds
        this.MAX_HEAP_USAGE = 0.8; // 80% of available heap
        this.TARGET_HEAP_USAGE = 0.5; // Target 50% heap usage
        this.MIN_HEAP_USAGE = 0.3; // Can scale up if below 30%

        // Worker limits
        this.MIN_WORKERS = 1;
        this.MAX_WORKERS = job.workers || 10; // User's desired max
        this.currentWorkers = Math.min(3, this.MAX_WORKERS); // Start conservative

        // Event size tracking
        this.avgEventSize = null;
        this.maxEventSize = 0;
        this.minEventSize = Infinity;

        // Memory per worker targets
        this.MAX_MEMORY_PER_WORKER = 50 * 1024 * 1024; // 50MB per worker
        this.TARGET_MEMORY_PER_WORKER = 30 * 1024 * 1024; // 30MB target

        // Batch size limits
        this.MIN_BATCH_SIZE = 100;
        this.MAX_BATCH_SIZE = 2000;

        // Performance tracking
        this.lastAdjustmentTime = Date.now();
        this.adjustmentInterval = 10000; // Adjust every 10 seconds max
        this.memoryHistory = [];
        this.throughputHistory = [];
    }

    /**
     * Sample an event to calculate average size
     */
    sampleEvent(event) {
        if (this.samples.length < this.sampleSize) {
            const size = Buffer.byteLength(JSON.stringify(event), 'utf8');
            this.samples.push(size);
            this.maxEventSize = Math.max(this.maxEventSize, size);
            this.minEventSize = Math.min(this.minEventSize, size);

            // Calculate average after collecting enough samples
            if (this.samples.length === this.sampleSize) {
                this.calculateOptimalSettings();
            }
        }
        this.eventCount++;
    }

    /**
     * Calculate optimal settings based on sampled events
     */
    calculateOptimalSettings() {
        // Calculate average event size
        const sum = this.samples.reduce((a, b) => a + b, 0);
        this.avgEventSize = Math.ceil(sum / this.samples.length);

        // Add 20% buffer for variance
        const bufferedSize = this.avgEventSize * 1.2;

        // Calculate memory requirements
        const memoryPerBatch = bufferedSize * this.job.recordsPerBatch;
        const memoryPerWorker = memoryPerBatch * 2; // Account for queuing

        // Calculate optimal worker count
        const heapLimit = require('v8').getHeapStatistics().heap_size_limit;
        const availableMemory = heapLimit * this.TARGET_HEAP_USAGE;

        let optimalWorkers = Math.floor(availableMemory / memoryPerWorker);
        optimalWorkers = Math.max(this.MIN_WORKERS, Math.min(optimalWorkers, this.MAX_WORKERS));

        // Calculate optimal batch size
        let optimalBatchSize = Math.floor(this.MAX_MEMORY_PER_WORKER / bufferedSize);
        optimalBatchSize = Math.max(this.MIN_BATCH_SIZE, Math.min(optimalBatchSize, this.MAX_BATCH_SIZE));

        // Adjust settings
        this.currentWorkers = optimalWorkers;
        this.job.recordsPerBatch = optimalBatchSize;

        // Log the decision
        console.log(`
=== Adaptive Scaling Analysis ===
Event Statistics:
  Average size: ${(this.avgEventSize / 1024).toFixed(2)} KB
  Max size: ${(this.maxEventSize / 1024).toFixed(2)} KB
  Min size: ${(this.minEventSize / 1024).toFixed(2)} KB

Optimal Configuration:
  Workers: ${this.currentWorkers} (max: ${this.MAX_WORKERS})
  Batch size: ${this.job.recordsPerBatch} events
  Memory per worker: ${(memoryPerWorker / 1024 / 1024).toFixed(1)} MB
  Total memory footprint: ${(memoryPerWorker * this.currentWorkers / 1024 / 1024).toFixed(1)} MB
================================
        `);

        return {
            workers: this.currentWorkers,
            batchSize: this.job.recordsPerBatch,
            avgEventSize: this.avgEventSize
        };
    }

    /**
     * Monitor memory and adjust workers dynamically
     */
    monitorAndAdjust() {
        const now = Date.now();
        if (now - this.lastAdjustmentTime < this.adjustmentInterval) {
            return this.currentWorkers; // Too soon to adjust
        }

        const memUsage = process.memoryUsage();
        const heapUsed = memUsage.heapUsed;
        const heapTotal = memUsage.heapTotal;
        const heapUsageRatio = heapUsed / heapTotal;

        // Track memory history
        this.memoryHistory.push({ time: now, heapUsed, heapUsageRatio });
        if (this.memoryHistory.length > 10) this.memoryHistory.shift();

        // Check if we need to adjust
        let newWorkers = this.currentWorkers;

        if (heapUsageRatio > this.MAX_HEAP_USAGE) {
            // Memory pressure - reduce workers
            newWorkers = Math.max(this.MIN_WORKERS, Math.floor(this.currentWorkers * 0.7));
            console.warn(`⚠️  Memory pressure detected (${(heapUsageRatio * 100).toFixed(1)}% heap). Reducing workers: ${this.currentWorkers} → ${newWorkers}`);
        } else if (heapUsageRatio < this.MIN_HEAP_USAGE && this.currentWorkers < this.MAX_WORKERS) {
            // Memory available - can increase workers
            const avgThroughput = this.getAverageThroughput();
            if (avgThroughput > 0) { // Only scale up if we're actually processing
                newWorkers = Math.min(this.MAX_WORKERS, this.currentWorkers + 1);
                console.log(`✓ Memory usage low (${(heapUsageRatio * 100).toFixed(1)}% heap). Increasing workers: ${this.currentWorkers} → ${newWorkers}`);
            }
        }

        if (newWorkers !== this.currentWorkers) {
            this.currentWorkers = newWorkers;
            this.lastAdjustmentTime = now;
        }

        return this.currentWorkers;
    }

    /**
     * Track throughput for scaling decisions
     */
    recordThroughput(eventsPerSecond) {
        this.throughputHistory.push({
            time: Date.now(),
            eps: eventsPerSecond
        });
        if (this.throughputHistory.length > 10) this.throughputHistory.shift();
    }

    /**
     * Get average throughput over recent history
     */
    getAverageThroughput() {
        if (this.throughputHistory.length === 0) return 0;
        const sum = this.throughputHistory.reduce((a, b) => a + b.eps, 0);
        return sum / this.throughputHistory.length;
    }

    /**
     * Get current configuration
     */
    getConfig() {
        return {
            workers: this.currentWorkers,
            batchSize: this.job.recordsPerBatch,
            avgEventSize: this.avgEventSize,
            heapUsage: (process.memoryUsage().heapUsed / process.memoryUsage().heapTotal * 100).toFixed(1) + '%'
        };
    }

    /**
     * Emergency brake - drastically reduce workers if OOM is imminent
     */
    emergencyBrake() {
        const memUsage = process.memoryUsage();
        const heapUsed = memUsage.heapUsed;
        const heapLimit = require('v8').getHeapStatistics().heap_size_limit;

        if (heapUsed > heapLimit * 0.95) {
            this.currentWorkers = 1;
            this.job.recordsPerBatch = Math.min(100, this.job.recordsPerBatch);
            console.error('🚨 EMERGENCY: Near OOM! Reducing to 1 worker and small batches');

            // Force garbage collection if available
            if (global.gc) {
                global.gc();
                console.log('🗑️  Forced garbage collection');
            }

            return true;
        }
        return false;
    }
}

/**
 * Create an adaptive transform that samples events and adjusts settings
 */
function createAdaptiveTransform(scaler) {
    const { Transform } = require('stream');

    return new Transform({
        objectMode: true,
        highWaterMark: 16, // Keep small for responsiveness
        transform(data, encoding, callback) {
            // Sample events for size calculation
            scaler.sampleEvent(data);

            // Check for emergency memory situation
            if (scaler.emergencyBrake()) {
                // Pause briefly to let GC run
                setTimeout(() => callback(null, data), 100);
            } else {
                callback(null, data);
            }
        }
    });
}

/**
 * Create a monitoring transform that tracks throughput
 */
function createMonitoringTransform(scaler, job) {
    const { Transform } = require('stream');
    let lastCheck = Date.now();
    let eventsSinceLastCheck = 0;

    return new Transform({
        objectMode: true,
        highWaterMark: job.highWater,
        transform(data, encoding, callback) {
            eventsSinceLastCheck++;

            const now = Date.now();
            if (now - lastCheck >= 1000) { // Check every second
                const eps = eventsSinceLastCheck / ((now - lastCheck) / 1000);
                scaler.recordThroughput(eps);
                eventsSinceLastCheck = 0;
                lastCheck = now;

                // Adjust workers if needed
                const newWorkers = scaler.monitorAndAdjust();
                if (newWorkers !== job.workers) {
                    // This would need to be communicated to parallel-transform
                    // For now, just track it
                    job.adaptiveWorkers = newWorkers;
                }
            }

            callback(null, data);
        }
    });
}

module.exports = {
    AdaptiveScaler,
    createAdaptiveTransform,
    createMonitoringTransform
};