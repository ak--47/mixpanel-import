/**
 * GCS Stream Throttling Mechanism
 *
 * Problem: GCS downloads at 100MB/s+ but the pipeline can only process at ~10MB/s
 * Solution: Implement proper backpressure and pause/resume control
 */

const { Transform } = require('stream');

/**
 * Memory-aware throttle that actually pauses upstream
 * This works by applying backpressure when memory is high
 */
class MemoryThrottle extends Transform {
	constructor(options = {}) {
		super({
			objectMode: true,
			// CRITICAL: Small highWaterMark to trigger backpressure quickly
			highWaterMark: options.highWaterMark || 1, // Only buffer 1 object!
		});

		this.maxHeapMB = options.maxHeapMB || 1500;
		this.pauseThresholdMB = options.pauseThresholdMB || 1200;
		this.resumeThresholdMB = options.resumeThresholdMB || 800;
		this.checkInterval = options.checkInterval || 100; // Check every 100ms
		this._isPaused = false; // Internal state tracker (renamed to avoid conflict with Transform.isPaused())
		this.lastCheck = Date.now();
		this.objectCount = 0;
		this.pauseCount = 0;
		this.pendingCallback = null; // Store callback when paused
		this.pendingChunk = null; // Store chunk when paused
		this.memCheckTimer = null; // Timer for memory checks while paused
	}

	_checkMemoryAndResume() {
		const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

		// Log status while paused
		console.log(`⏸️  Throttle: Memory check (${heapUsed.toFixed(0)}MB / ${this.resumeThresholdMB}MB to resume)`);

		// Resume if memory dropped enough
		if (heapUsed < this.resumeThresholdMB) {
			this._isPaused = false;
			console.log(`▶️  Throttle: Resuming GCS (memory: ${heapUsed.toFixed(0)}MB < ${this.resumeThresholdMB}MB)`);

			// Clear the timer
			if (this.memCheckTimer) {
				clearInterval(this.memCheckTimer);
				this.memCheckTimer = null;
			}

			// Process the pending chunk if we have one
			if (this.pendingCallback) {
				const callback = this.pendingCallback;
				const chunk = this.pendingChunk;
				this.pendingCallback = null;
				this.pendingChunk = null;
				callback(null, chunk);
			}
		}
	}

	_transform(chunk, encoding, callback) {
		this.objectCount++;
		const now = Date.now();

		// Check memory periodically
		if (now - this.lastCheck >= this.checkInterval) {
			this.lastCheck = now;
			const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

			// Start pausing if memory is too high
			if (heapUsed > this.pauseThresholdMB && !this._isPaused) {
				this._isPaused = true;
				this.pauseCount++;
				console.log(`🛑 Throttle: Pausing GCS (memory: ${heapUsed.toFixed(0)}MB > ${this.pauseThresholdMB}MB) [pause #${this.pauseCount}]`);
				console.log(`   Pipeline has ~${this.objectCount} objects buffered, draining to Mixpanel...`);

				// Start checking memory every second to see if we can resume
				if (!this.memCheckTimer) {
					this.memCheckTimer = setInterval(() => this._checkMemoryAndResume(), 1000);
				}
			}

			// Resume when memory drops (also handled in _checkMemoryAndResume)
			if (heapUsed < this.resumeThresholdMB && this._isPaused) {
				this._isPaused = false;
				console.log(`▶️  Throttle: Resuming GCS (memory: ${heapUsed.toFixed(0)}MB < ${this.resumeThresholdMB}MB)`);

				if (this.memCheckTimer) {
					clearInterval(this.memCheckTimer);
					this.memCheckTimer = null;
				}
			}
		}

		// CRITICAL: Apply backpressure by NOT calling callback when paused
		// This prevents GCS from reading more data while pipeline drains
		if (this._isPaused) {
			// Store the callback and chunk - we'll call it when memory drops
			this.pendingCallback = callback;
			this.pendingChunk = chunk;
			// Don't call callback - this applies backpressure upstream!
		} else {
			// Normal flow - pass through immediately
			callback(null, chunk);
		}
	}

	_final(callback) {
		// Clean up timer if still running
		if (this.memCheckTimer) {
			clearInterval(this.memCheckTimer);
			this.memCheckTimer = null;
		}

		// Process any pending callback
		if (this.pendingCallback) {
			const pendingCallback = this.pendingCallback;
			const pendingChunk = this.pendingChunk;
			this.pendingCallback = null;
			this.pendingChunk = null;
			pendingCallback(null, pendingChunk);
		}

		console.log(`📊 Throttle stats: ${this.objectCount} objects, ${this.pauseCount} pauses`);
		callback();
	}
}

/**
 * Rate-limited throttle that limits objects per second
 */
class RateLimitThrottle extends Transform {
	constructor(options = {}) {
		super({
			objectMode: true,
			highWaterMark: 1, // Minimal buffer
		});

		this.objectsPerSecond = options.objectsPerSecond || 1000;
		this.delayMs = 1000 / this.objectsPerSecond;
		this.lastEmit = 0;
	}

	async _transform(chunk, encoding, callback) {
		const now = Date.now();
		const timeSinceLastEmit = now - this.lastEmit;

		if (timeSinceLastEmit < this.delayMs) {
			// Need to wait
			const waitTime = this.delayMs - timeSinceLastEmit;
			await new Promise(resolve => setTimeout(resolve, waitTime));
		}

		this.lastEmit = Date.now();
		callback(null, chunk);
	}
}

/**
 * Chunked reader that uses GCS range requests
 * Reads file in chunks to control download speed
 */
class ChunkedGCSReader {
	constructor(file, options = {}) {
		this.file = file;
		this.chunkSize = options.chunkSize || 10 * 1024 * 1024; // 10MB chunks
		this.delayBetweenChunks = options.delayBetweenChunks || 100; // 100ms between chunks
		this.currentOffset = 0;
		this.metadata = null;
	}

	async getMetadata() {
		if (!this.metadata) {
			[this.metadata] = await this.file.getMetadata();
		}
		return this.metadata;
	}

	createReadStream() {
		const { Readable } = require('stream');
		const self = this;

		return new Readable({
			objectMode: false,
			highWaterMark: 64 * 1024, // 64KB

			async read() {
				try {
					const metadata = await self.getMetadata();
					const fileSize = parseInt(metadata.size);

					if (self.currentOffset >= fileSize) {
						this.push(null); // EOF
						return;
					}

					// Calculate chunk boundaries
					const start = self.currentOffset;
					const end = Math.min(start + self.chunkSize - 1, fileSize - 1);

					// Read chunk with range request
					const stream = self.file.createReadStream({
						start,
						end,
						decompress: false,
						validation: false
					});

					// Collect chunk data
					const chunks = [];
					for await (const chunk of stream) {
						chunks.push(chunk);
					}
					const data = Buffer.concat(chunks);

					// Update offset
					self.currentOffset = end + 1;

					// Push data downstream
					this.push(data);

					// Delay before next chunk (throttling)
					if (self.delayBetweenChunks > 0 && self.currentOffset < fileSize) {
						await new Promise(r => setTimeout(r, self.delayBetweenChunks));
					}

				} catch (error) {
					this.destroy(error);
				}
			}
		});
	}
}

module.exports = {
	MemoryThrottle,
	RateLimitThrottle,
	ChunkedGCSReader
};