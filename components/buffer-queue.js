/**
 * Buffer Queue for decoupling fast sources from slow sinks
 *
 * Problem: When GCS is paused, the entire pipeline stops due to backpressure
 * Solution: Buffer data in memory/disk to allow pipeline to continue draining
 */

const { Transform, Writable, Readable } = require('stream');
const { EventEmitter } = require('events');

/**
 * In-memory buffer queue that decouples source from sink
 * Allows pausing source while sink continues processing buffered data
 */
class BufferQueue extends EventEmitter {
	constructor(options = {}) {
		super();

		// Queue configuration
		this.maxSizeMB = options.maxSizeMB || 2000; // Max 2GB buffer by default
		this.pauseThresholdMB = options.pauseThresholdMB || 1500; // Pause at 1.5GB
		this.resumeThresholdMB = options.resumeThresholdMB || 1000; // Resume at 1GB
		this.verbose = options.verbose !== undefined ? options.verbose : true;

		// Queue state
		this.queue = [];
		this.queueSizeBytes = 0;
		this.isPaused = false;
		this.sourceStream = null;
		this.sinkStream = null;
		this.objectsQueued = 0;
		this.objectsDequeued = 0;

		// Memory monitoring
		this.lastMemCheck = Date.now();
		this.memCheckInterval = 100; // Check every 100ms
		this.pausedAt = 0;
		this.checkCount = 0;

		// Stream state
		this.sourceEnded = false;
		this.processing = false;
	}

	/**
	 * Create input stream that adds to queue
	 */
	createInputStream() {
		const self = this;

		const inputStream = new Writable({
			objectMode: true,
			highWaterMark: 16,

			write(chunk, encoding, callback) {
				// Add to queue
				const size = self._getObjectSize(chunk);
				self.queue.push({ data: chunk, size });
				self.queueSizeBytes += size;
				self.objectsQueued++;

				// Check if we should pause source
				const queueSizeMB = self.queueSizeBytes / 1024 / 1024;
				if (queueSizeMB > self.pauseThresholdMB && !self.isPaused) {
					self._pauseSource(queueSizeMB);
				}

				// Trigger processing
				self._processQueue();

				// Always accept data (buffering internally)
				callback();
			},

			final(callback) {
				self.sourceEnded = true;
				self._processQueue();
				callback();
			}
		});

		return inputStream;
	}

	/**
	 * Create output stream that reads from queue
	 */
	createOutputStream() {
		const self = this;

		const outputStream = new Readable({
			objectMode: true,
			highWaterMark: 16,

			read() {
				self._processQueue();
			}
		});

		this.sinkStream = outputStream;
		return outputStream;
	}

	/**
	 * Set the source stream to control (pause/resume)
	 */
	setSourceStream(stream) {
		this.sourceStream = stream;
		return this;
	}

	/**
	 * Process queue - move data from queue to output
	 */
	_processQueue() {
		if (this.processing) return;
		this.processing = true;

		try {
			// Process while we have data and sink wants it
			while (this.queue.length > 0 && this.sinkStream && !this.sinkStream.readableEnded) {
				const item = this.queue[0];

				// Try to push to output
				if (this.sinkStream.push(item.data)) {
					// Successfully pushed, remove from queue
					this.queue.shift();
					this.queueSizeBytes -= item.size;
					this.objectsDequeued++;

					// Check if we should resume source
					const queueSizeMB = this.queueSizeBytes / 1024 / 1024;
					if (queueSizeMB < this.resumeThresholdMB && this.isPaused) {
						this._resumeSource(queueSizeMB);
					}
				} else {
					// Sink is full, stop processing
					break;
				}
			}

			// If queue is empty and source ended, end the output
			if (this.queue.length === 0 && this.sourceEnded && this.sinkStream) {
				this.sinkStream.push(null);
			}

			// Log status periodically while paused
			if (this.isPaused && this.verbose) {
				const now = Date.now();
				if (now - this.lastMemCheck > 1000) {
					this.lastMemCheck = now;
					this._logPausedStatus();
				}
			}

		} finally {
			this.processing = false;
		}
	}

	/**
	 * Pause the source stream
	 */
	_pauseSource(queueSizeMB) {
		this.isPaused = true;
		this.pausedAt = Date.now();
		this.checkCount = 0;

		if (this.verbose) {
			console.log('');
			console.log(`🛑 BUFFER QUEUE: Pausing GCS input`);
			console.log(`    ├─ Queue size: ${queueSizeMB.toFixed(0)}MB > ${this.pauseThresholdMB}MB threshold`);
			console.log(`    ├─ Queue depth: ${this.queue.length.toLocaleString()} objects`);
			console.log(`    ├─ Objects: ${this.objectsQueued.toLocaleString()} queued, ${this.objectsDequeued.toLocaleString()} sent`);
			console.log(`    └─ Pipeline continues draining buffered data...`);
		}

		// Pause the actual source stream
		if (this.sourceStream && typeof this.sourceStream.pause === 'function') {
			this.sourceStream.pause();
		}
	}

	/**
	 * Resume the source stream
	 */
	_resumeSource(queueSizeMB) {
		const pausedDuration = Math.floor((Date.now() - this.pausedAt) / 1000);
		const minutes = Math.floor(pausedDuration / 60);
		const seconds = pausedDuration % 60;
		const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${pausedDuration}s`;

		this.isPaused = false;

		if (this.verbose) {
			console.log('');
			console.log(`▶️  BUFFER QUEUE: Resuming GCS input`);
			console.log(`    ├─ Queue size: ${queueSizeMB.toFixed(0)}MB < ${this.resumeThresholdMB}MB threshold`);
			console.log(`    ├─ Duration: Paused for ${timeStr}`);
			console.log(`    ├─ Queue depth: ${this.queue.length.toLocaleString()} objects remaining`);
			console.log(`    └─ Objects: ${(this.objectsDequeued - (this.objectsQueued - this.queue.length)).toLocaleString()} processed while paused`);
		}

		// Resume the actual source stream
		if (this.sourceStream && typeof this.sourceStream.resume === 'function') {
			this.sourceStream.resume();
		}

		this.pausedAt = 0;
		this.checkCount = 0;
	}

	/**
	 * Log status while paused
	 */
	_logPausedStatus() {
		this.checkCount++;

		// Log every 10 seconds
		if (this.checkCount % 10 !== 0) return;

		const pausedDuration = Math.floor((Date.now() - this.pausedAt) / 1000);
		const minutes = Math.floor(pausedDuration / 60);
		const seconds = pausedDuration % 60;
		const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${pausedDuration}s`;

		const queueSizeMB = this.queueSizeBytes / 1024 / 1024;
		const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

		console.log('');
		console.log(`    ⏸️  GCS PAUSED - ${timeStr} | QUEUE DRAINING`);
		console.log(`    ├─ Queue: ${queueSizeMB.toFixed(0)}MB (${this.queue.length.toLocaleString()} objects)`);
		console.log(`    ├─ Memory: Heap ${heapUsed.toFixed(0)}MB`);
		console.log(`    ├─ Progress: ${this.objectsDequeued.toLocaleString()} / ${this.objectsQueued.toLocaleString()} objects sent`);
		console.log(`    └─ Target: Resume when queue < ${this.resumeThresholdMB}MB`);

		// If queue is actively draining, show rate
		if (this.objectsDequeued > 0) {
			const drainRate = this.objectsDequeued / (pausedDuration || 1);
			console.log(`    └─ Drain rate: ${drainRate.toFixed(1)} objects/sec to Mixpanel`);
		}
	}

	/**
	 * Get approximate size of an object in bytes
	 */
	_getObjectSize(obj) {
		// Use cached size if available (from stringification)
		if (obj._cachedSize) return obj._cachedSize;

		// Otherwise estimate
		try {
			return JSON.stringify(obj).length;
		} catch {
			return 1000; // Default estimate
		}
	}

	/**
	 * Get queue statistics
	 */
	getStats() {
		return {
			queueLength: this.queue.length,
			queueSizeMB: (this.queueSizeBytes / 1024 / 1024).toFixed(2),
			objectsQueued: this.objectsQueued,
			objectsDequeued: this.objectsDequeued,
			isPaused: this.isPaused
		};
	}
}

module.exports = { BufferQueue };