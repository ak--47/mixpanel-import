/**
 * Buffer Queue for decoupling fast sources from slow sinks
 *
 * Problem: When GCS is paused, the entire pipeline stops due to backpressure
 * Solution: Buffer data in memory/disk to allow pipeline to continue draining
 */

const { Transform, Writable, Readable } = require('stream');
const { EventEmitter } = require('events');
const u = require('ak-tools');

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
		this.pendingCallbacks = []; // Array of callbacks waiting to be called when buffer drains

		// Memory monitoring
		this.lastMemCheck = Date.now();
		this.memCheckInterval = 100; // Check every 100ms
		this.pausedAt = 0;
		this.checkCount = 0;
		this.memoryAtPause = 0; // Track memory when pause starts
		this.objectsDequeuedAtPause = 0; // Track objects sent when pause starts

		// Stream state
		this.sourceEnded = false;
		this.processing = false;
	}

	/**
	 * Create input stream that adds to queue
	 * @param {boolean} objectMode - Whether to operate in object mode (default: false for byte streams)
	 */
	createInputStream(objectMode = false) {
		const self = this;

		const inputStream = new Writable({
			objectMode: objectMode,
			highWaterMark: objectMode ? 16 : 64 * 1024, // 16 objects or 64KB for bytes

			write(chunk, encoding, callback) {
				// Add to queue
				const size = self._getObjectSize(chunk);
				self.queue.push({ data: chunk, size });
				self.queueSizeBytes += size;
				self.objectsQueued++;

				// Check ACTUAL HEAP MEMORY, not just queue size
				// This ensures we pause based on total memory pressure, not just buffered data
				const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;
				const queueSizeMB = self.queueSizeBytes / 1024 / 1024;

				// Pause if EITHER heap memory OR queue size exceeds threshold
				if (heapUsedMB > self.pauseThresholdMB || queueSizeMB > self.pauseThresholdMB) {
					if (!self.isPaused) {
						self._pauseSource(heapUsedMB);
					}

					// Store callback to call later when buffer drains
					if (!self.pendingCallbacks) {
						self.pendingCallbacks = [];
					}
					self.pendingCallbacks.push(callback);

					// DON'T call callback yet - this creates backpressure
					// The callback will be called when the buffer drains
				} else {
					// Buffer has space, accept more data
					callback();
				}

				// Trigger processing
				self._processQueue();
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
	 * @param {boolean} objectMode - Whether to operate in object mode (default: false for byte streams)
	 */
	createOutputStream(objectMode = false) {
		const self = this;

		const outputStream = new Readable({
			objectMode: objectMode,
			highWaterMark: objectMode ? 16 : 64 * 1024, // 16 objects or 64KB for bytes

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
					const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;

					// Resume if BOTH heap memory AND queue size are below resume threshold
					if (heapUsedMB < this.resumeThresholdMB && queueSizeMB < this.resumeThresholdMB) {
						// Call pending callbacks to resume data flow
						if (this.pendingCallbacks && this.pendingCallbacks.length > 0) {
							const callback = this.pendingCallbacks.shift();
							callback(); // Resume one pending write
						}

						if (this.isPaused && this.pendingCallbacks && this.pendingCallbacks.length === 0) {
							this._resumeSource(queueSizeMB);
						}
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
	_pauseSource(heapUsedMB) {
		this.isPaused = true;
		this.pausedAt = Date.now();
		this.checkCount = 0;
		// Track memory at pause start to show how much we free
		this.memoryAtPause = heapUsedMB;
		this.objectsDequeuedAtPause = this.objectsDequeued;

		const queueSizeMB = this.queueSizeBytes / 1024 / 1024;

		if (this.verbose) {
			console.log('');
			console.log(`🛑 BUFFER QUEUE: Pausing GCS input`);
			// Show both heap memory and queue size, indicating which triggered the pause
			if (heapUsedMB > this.pauseThresholdMB) {
				console.log(`    ├─ Heap memory: ${u.bytesHuman(heapUsedMB * 1024 * 1024)} > ${u.bytesHuman(this.pauseThresholdMB * 1024 * 1024)} threshold (TRIGGERED PAUSE)`);
			} else {
				console.log(`    ├─ Heap memory: ${u.bytesHuman(heapUsedMB * 1024 * 1024)}`);
			}
			if (queueSizeMB > this.pauseThresholdMB) {
				console.log(`    ├─ Queue size: ${u.bytesHuman(this.queueSizeBytes)} > ${u.bytesHuman(this.pauseThresholdMB * 1024 * 1024)} threshold (TRIGGERED PAUSE)`);
			} else {
				console.log(`    ├─ Queue size: ${u.bytesHuman(this.queueSizeBytes)}`);
			}
			console.log(`    ├─ Queue depth: ${this.queue.length.toLocaleString()} objects`);
			console.log(`    ├─ Objects: ${this.objectsQueued.toLocaleString()} queued, ${this.objectsDequeued.toLocaleString()} sent`);
			console.log(`    └─ Pipeline continues draining buffered data to Mixpanel...`);
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

		const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;
		const memoryFreed = this.memoryAtPause - heapUsed;
		const objectsSentWhilePaused = this.objectsDequeued - this.objectsDequeuedAtPause;

		this.isPaused = false;

		if (this.verbose) {
			console.log('');
			console.log(`▶️  BUFFER QUEUE: Resuming GCS input`);
			console.log(`    ├─ Heap memory: ${u.bytesHuman(heapUsed * 1024 * 1024)} < ${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)} threshold`);
			console.log(`    ├─ Queue size: ${u.bytesHuman(this.queueSizeBytes)} < ${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)} threshold`);
			console.log(`    ├─ Duration: Paused for ${timeStr}`);
			console.log(`    ├─ Memory freed: ${u.bytesHuman(memoryFreed * 1024 * 1024)} (from ${u.bytesHuman(this.memoryAtPause * 1024 * 1024)} to ${u.bytesHuman(heapUsed * 1024 * 1024)})`);
			console.log(`    ├─ Queue depth: ${this.queue.length.toLocaleString()} objects remaining`);
			console.log(`    └─ Sent to Mixpanel while paused: ${objectsSentWhilePaused.toLocaleString()} objects`);
		}

		// Resume the actual source stream
		if (this.sourceStream && typeof this.sourceStream.resume === 'function') {
			this.sourceStream.resume();
		}

		this.pausedAt = 0;
		this.checkCount = 0;
		this.memoryAtPause = 0;
		this.objectsDequeuedAtPause = 0;
	}

	/**
	 * Log status while paused
	 */
	_logPausedStatus() {
		this.checkCount++;

		// Log every 30 seconds
		if (this.checkCount % 30 !== 0) return;

		const pausedDuration = Math.floor((Date.now() - this.pausedAt) / 1000);
		const minutes = Math.floor(pausedDuration / 60);
		const seconds = pausedDuration % 60;
		const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${pausedDuration}s`;

		const queueSizeMB = this.queueSizeBytes / 1024 / 1024;
		const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

		// Calculate memory freed since pause
		const memoryFreed = this.memoryAtPause - heapUsed;
		const objectsSentWhilePaused = this.objectsDequeued - this.objectsDequeuedAtPause;

		console.log('');
		console.log(`    ⏸️  GCS PAUSED - ${timeStr} | ACTIVELY DRAINING TO MIXPANEL`);
		console.log(`    ├─ Heap memory: ${u.bytesHuman(heapUsed * 1024 * 1024)} (freed ${u.bytesHuman(memoryFreed * 1024 * 1024)} since pause)`);
		console.log(`    ├─ Queue: ${u.bytesHuman(this.queueSizeBytes)} (${this.queue.length.toLocaleString()} objects remaining)`);
		console.log(`    ├─ Progress: ${objectsSentWhilePaused.toLocaleString()} objects sent to Mixpanel while paused`);
		console.log(`    ├─ Total: ${this.objectsDequeued.toLocaleString()} / ${this.objectsQueued.toLocaleString()} objects sent overall`);
		console.log(`    └─ Target: Resume when heap < ${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)} AND queue < ${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)}`);

		// Show drain rate if actively draining
		if (objectsSentWhilePaused > 0) {
			const drainRate = objectsSentWhilePaused / (pausedDuration || 1);
			const mbDrainRate = (this.memoryAtPause - heapUsed) / (pausedDuration || 1) * 60; // MB per minute
			console.log(`    └─ Rates: ${drainRate.toFixed(1)} objects/sec | ${u.bytesHuman(mbDrainRate * 1024 * 1024)}/min memory freed`);
		}
	}

	/**
	 * Get approximate size of an object or buffer in bytes
	 */
	_getObjectSize(chunk) {
		// If it's a Buffer, use its actual byte length
		if (Buffer.isBuffer(chunk)) {
			return chunk.length;
		}

		// Use cached size if available (from stringification)
		if (chunk._cachedSize) return chunk._cachedSize;

		// Otherwise estimate for objects
		try {
			return JSON.stringify(chunk).length;
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