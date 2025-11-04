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
		this.baselineMemoryMB = null; // Track baseline pipeline memory
		this.emptyQueueCheckCount = 0; // Track how long queue has been empty

		// Stream state
		this.sourceEnded = false;
		this.processing = false;
		this.finalCallback = null; // Stores the final callback when source wants to end but has pending data
		this.checkInterval = null; // Timer for periodic checks while paused
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

					if (self.verbose) {
						console.log(`    🔄 BufferQueue: Storing callback #${self.pendingCallbacks.length}, not calling it (creates backpressure)`);
					}

					// DON'T call callback yet - this creates backpressure
					// The callback will be called when the buffer drains
					// But DO keep processing the queue so pipeline drains to Mixpanel!
					self._processQueue(); // Keep draining even while paused
					return; // Return after triggering process
				} else {
					// Buffer has space, accept more data
					callback();
				}

				// Trigger processing only if we're not paused
				if (!self.isPaused) {
					self._processQueue();
				}
			},

			final(callback) {
				// Don't mark as ended if we're paused OR have pending callbacks
				if (self.isPaused || (self.pendingCallbacks && self.pendingCallbacks.length > 0)) {
					if (self.verbose) {
						console.log(`    📋 BufferQueue: Source wants to end but we're ${self.isPaused ? 'paused' : 'have pending callbacks'} - deferring end`);
						console.log(`       └─ Paused: ${self.isPaused}, Pending callbacks: ${self.pendingCallbacks ? self.pendingCallbacks.length : 0}`);
					}
					// Store the final callback to call when we resume and drain
					self.finalCallback = callback;
					self._processQueue();
				} else {
					// Not paused and no pending callbacks, safe to mark as ended
					if (self.verbose) {
						console.log(`    ✓ BufferQueue: Source ended cleanly, not paused and no pending callbacks`);
					}
					self.sourceEnded = true;
					self._processQueue();
					callback();
				}
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
		// Allow re-entrance for resuming pending callbacks
		if (this.processing && !this.isPaused) return;
		this.processing = true;

		try {
			// First check if we should resume pending callbacks even if queue is empty
			if (this.isPaused && this.pendingCallbacks && this.pendingCallbacks.length > 0) {
				const heapUsedMB = process.memoryUsage().heapUsed / 1024 / 1024;
				const queueSizeMB = this.queueSizeBytes / 1024 / 1024;

				if (heapUsedMB < this.resumeThresholdMB && queueSizeMB < this.resumeThresholdMB) {
					// Resume pending callbacks
					const callback = this.pendingCallbacks.shift();
					callback(); // Resume one pending write

					if (this.verbose) {
						console.log(`    ♻️ BufferQueue: Resumed pending callback (${this.pendingCallbacks.length} remaining)`);
					}

					// If no more pending callbacks, resume source
					if (this.pendingCallbacks.length === 0) {
						this._resumeSource(queueSizeMB);

						// If we had a final callback waiting and no more pending, mark as ended now
						if (this.finalCallback) {
							if (this.verbose) {
								console.log(`    ✅ BufferQueue: All pending callbacks processed, now ending source`);
							}
							this.sourceEnded = true;
							const finalCb = this.finalCallback;
							this.finalCallback = null;
							finalCb();
						}
					} else {
						// More callbacks to process, schedule another check
						setImmediate(() => this._processQueue());
					}
					this.processing = false; // Clear flag before returning
					return; // Exit early to let the resumed callback do its work
				}
				this.processing = false; // Clear flag if conditions not met
			}

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

							// Schedule next process to continue draining callbacks
							if (this.pendingCallbacks.length > 0) {
								setImmediate(() => this._processQueue());
							}
						}

						if (this.isPaused && this.pendingCallbacks && this.pendingCallbacks.length === 0) {
							this._resumeSource(queueSizeMB);

							// If we had a final callback waiting and no more pending, mark as ended now
							if (this.finalCallback) {
								if (this.verbose) {
									console.log(`    ✅ BufferQueue: All pending callbacks processed, now ending source`);
								}
								this.sourceEnded = true;
								const callback = this.finalCallback;
								this.finalCallback = null;
								callback();
							}
						}
					}
				} else {
					// Sink is full, stop processing
					break;
				}
			}

			// Only end if queue is empty, source ended, AND no pending callbacks
			// Pending callbacks mean we're still expecting data once memory pressure reduces
			if (this.queue.length === 0 && this.sourceEnded &&
			    (!this.pendingCallbacks || this.pendingCallbacks.length === 0) &&
			    this.sinkStream) {
				this.sinkStream.push(null);
			}

			// Log status periodically while paused (the check timer handles resuming)
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
			console.log(`    ├─ Pending callbacks: ${this.pendingCallbacks ? this.pendingCallbacks.length : 0} (paused data waiting)`);
			console.log(`    ├─ Objects: ${this.objectsQueued.toLocaleString()} queued, ${this.objectsDequeued.toLocaleString()} sent`);
			console.log(`    └─ Pipeline continues draining buffered data to Mixpanel...`);
		}

		// Pause the actual source stream
		if (this.sourceStream && typeof this.sourceStream.pause === 'function') {
			this.sourceStream.pause();
		}

		// Start periodic checks to resume when memory drops
		if (!this.checkInterval) {
			this.checkInterval = setInterval(() => {
				// Only check if we're still paused
				if (!this.isPaused) {
					clearInterval(this.checkInterval);
					this.checkInterval = null;
					return;
				}

				// Check if we can resume
				const currentHeapMB = process.memoryUsage().heapUsed / 1024 / 1024;
				const currentQueueMB = this.queueSizeBytes / 1024 / 1024;

				// Track baseline memory when queue is empty
				if (this.queueSizeBytes === 0) {
					this.emptyQueueCheckCount++;
					// After 5 checks with empty queue, consider this baseline memory
					if (this.emptyQueueCheckCount >= 5 && !this.baselineMemoryMB) {
						this.baselineMemoryMB = currentHeapMB;
						if (this.verbose) {
							console.log(`    📊 BufferQueue: Detected baseline pipeline memory: ${u.bytesHuman(this.baselineMemoryMB * 1024 * 1024)}`);
							if (this.baselineMemoryMB > this.resumeThresholdMB) {
								console.log(`    ⚠️  WARNING: Baseline memory (${u.bytesHuman(this.baselineMemoryMB * 1024 * 1024)}) exceeds resume threshold (${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)})`);
								console.log(`       └─ Pipeline will be stuck! Increase resumeThresholdMB to at least ${Math.ceil(this.baselineMemoryMB + 100)}MB`);
							}
						}
					}

					// Force garbage collection when queue is empty and memory is high
					if (this.emptyQueueCheckCount % 5 === 0 && currentHeapMB > this.resumeThresholdMB) {
						if (global.gc) {
							const beforeGC = process.memoryUsage().heapUsed / 1024 / 1024;
							global.gc();
							const afterGC = process.memoryUsage().heapUsed / 1024 / 1024;
							const freed = beforeGC - afterGC;
							if (this.verbose) {
								if (freed > 10) {
									console.log(`    ♻️  BufferQueue: Forced GC freed ${u.bytesHuman(freed * 1024 * 1024)} (${u.bytesHuman(beforeGC * 1024 * 1024)} → ${u.bytesHuman(afterGC * 1024 * 1024)})`);
								} else {
									console.log(`    ♻️  BufferQueue: Forced GC freed minimal memory (${freed.toFixed(1)}MB) - pipeline holding legitimate data`);
								}
							}
						} else if (this.verbose && this.emptyQueueCheckCount === 5) {
							console.log(`    ⚠️  BufferQueue: Cannot force GC - run with 'node --expose-gc' flag`);
						}
					}
				} else {
					this.emptyQueueCheckCount = 0;
				}

				// Log status every 10 seconds
				if (this.verbose) {
					console.log(`    ⏳ BufferQueue: Checking if can resume - Heap: ${u.bytesHuman(currentHeapMB * 1024 * 1024)}, Queue: ${u.bytesHuman(this.queueSizeBytes)}`);

					// Log detailed status every 10 seconds
					if (this.checkCount % 10 === 0) {
						const memoryGap = currentHeapMB - this.resumeThresholdMB;
						if (memoryGap > 0) {
							console.log(`       ├─ Need heap < ${u.bytesHuman(this.resumeThresholdMB * 1024 * 1024)} to resume (currently ${u.bytesHuman(memoryGap * 1024 * 1024)} above)`);
							if (this.queueSizeBytes === 0) {
								console.log(`       ├─ BufferQueue is EMPTY (0 bytes) - memory held elsewhere in pipeline`);
								console.log(`       ├─ Pending callbacks: ${this.pendingCallbacks ? this.pendingCallbacks.length : 0}`);

								// Show memory breakdown
								const memUsage = process.memoryUsage();
								console.log(`       ├─ Memory breakdown:`);
								console.log(`       │  ├─ Heap Used: ${u.bytesHuman(memUsage.heapUsed)}`);
								console.log(`       │  ├─ Heap Total: ${u.bytesHuman(memUsage.heapTotal)}`);
								console.log(`       │  ├─ RSS: ${u.bytesHuman(memUsage.rss)}`);
								console.log(`       │  └─ External: ${u.bytesHuman(memUsage.external)}`);
								console.log(`       └─ Pipeline is processing data through 10 HTTP workers + 13 transform stages`);
							}
						}
					}
				}
				this.checkCount++;

				if (currentHeapMB < this.resumeThresholdMB && currentQueueMB < this.resumeThresholdMB) {
					// Memory dropped enough, clear timer and trigger ONE resume
					clearInterval(this.checkInterval);
					this.checkInterval = null;

					if (this.verbose) {
						console.log(`    ✨ BufferQueue: Memory dropped below threshold, triggering resume...`);
					}
					this._processQueue();
				}
			}, 1000); // Check every second
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

		// Stop the periodic check timer
		if (this.checkInterval) {
			clearInterval(this.checkInterval);
			this.checkInterval = null;
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
		console.log(`    ├─ Pending callbacks: ${this.pendingCallbacks ? this.pendingCallbacks.length : 0} (paused data waiting)`);
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