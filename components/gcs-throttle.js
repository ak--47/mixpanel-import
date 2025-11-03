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
		this.verbose = options.verbose !== undefined ? options.verbose : true; // Default to verbose
		this._isPaused = false; // Internal state tracker (renamed to avoid conflict with Transform.isPaused())
		this.lastCheck = Date.now();
		this.objectCount = 0;
		this.pauseCount = 0;
		this.pendingCallback = null; // Store callback when paused
		this.pendingChunk = null; // Store chunk when paused
		this.memCheckTimer = null; // Timer for memory checks while paused
		this.checkCount = 0; // Count checks to reduce logging
		this.lastLoggedMem = 0; // Track last logged memory to detect changes
		this.pausedAt = 0; // Timestamp when paused
		this.objectsAtPause = 0; // Objects processed when paused
	}

	_checkMemoryAndResume() {
		const memUsage = process.memoryUsage();
		const heapUsed = memUsage.heapUsed / 1024 / 1024;
		const heapTotal = memUsage.heapTotal / 1024 / 1024;
		const rss = memUsage.rss / 1024 / 1024;
		const external = memUsage.external / 1024 / 1024;
		this.checkCount++;

		// Log immediately on first check, then every 30 seconds, or if memory changed significantly
		const shouldLog = this.verbose && (this.checkCount === 1 || this.checkCount % 30 === 0 || Math.abs(heapUsed - this.lastLoggedMem) > 100);
		if (shouldLog) {
			const pausedDuration = Math.floor((Date.now() - this.pausedAt) / 1000);
			const minutes = Math.floor(pausedDuration / 60);
			const seconds = pausedDuration % 60;
			const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${pausedDuration}s`;

			console.log('');  // Blank line for readability
			console.log(`    ⏸️  THROTTLE PAUSED - ${timeStr}`);
			console.log(`    ├─ Memory: Heap ${heapUsed.toFixed(0)}/${heapTotal.toFixed(0)}MB | RSS ${rss.toFixed(0)}MB | External ${external.toFixed(0)}MB`);
			console.log(`    ├─ Target: Resume when heap < ${this.resumeThresholdMB}MB (currently ${(heapUsed - this.resumeThresholdMB).toFixed(0)}MB over)`);
			console.log(`    ├─ Objects: ${this.objectCount.toLocaleString()} processed total (${(this.objectCount - this.objectsAtPause).toLocaleString()} since pause)`);

			this.lastLoggedMem = heapUsed;

			// Force garbage collection if available to help memory drop
			if (global.gc && this.checkCount % 10 === 0) {  // GC every 10 seconds
				console.log(`    └─ Running GC...`);
				global.gc();
				const newHeap = process.memoryUsage().heapUsed / 1024 / 1024;
				const freed = heapUsed - newHeap;
				if (freed > 0) {
					console.log(`       ✓ Freed ${freed.toFixed(0)}MB → Now at ${newHeap.toFixed(0)}MB`);
				} else {
					console.log(`       • No memory freed (stable at ${newHeap.toFixed(0)}MB)`);
				}
			}
		}

		// Resume if memory dropped enough
		if (heapUsed < this.resumeThresholdMB) {
			this._isPaused = false;
			const pausedDuration = Math.floor((Date.now() - this.pausedAt) / 1000);
			const minutes = Math.floor(pausedDuration / 60);
			const seconds = pausedDuration % 60;
			const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${pausedDuration}s`;

			if (this.verbose) {
				console.log('');
				console.log(`▶️  THROTTLE: Resuming GCS downloads`);
				console.log(`    ├─ Memory: ${heapUsed.toFixed(0)}MB < ${this.resumeThresholdMB}MB threshold`);
				console.log(`    ├─ Duration: Paused for ${timeStr}`);
				console.log(`    └─ Objects: ${(this.objectCount - this.objectsAtPause).toLocaleString()} processed while paused`);
			}

			// Clear the timer
			if (this.memCheckTimer) {
				clearInterval(this.memCheckTimer);
				this.memCheckTimer = null;
			}

			// Reset counters
			this.checkCount = 0;
			this.lastLoggedMem = 0;
			this.pausedAt = 0;
			this.objectsAtPause = 0;

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
				this.pausedAt = Date.now();
				this.objectsAtPause = this.objectCount;

				if (this.verbose) {
					console.log('');  // Blank line for visibility
					console.log(`🛑 THROTTLE: Pausing GCS downloads`);
					console.log(`    ├─ Memory: ${heapUsed.toFixed(0)}MB > ${this.pauseThresholdMB}MB threshold`);
					console.log(`    ├─ Objects: ${this.objectCount.toLocaleString()} processed so far`);
					console.log(`    └─ Action: Draining pipeline to Mixpanel...`);
				}

				// Start checking memory every second to see if we can resume
				if (!this.memCheckTimer) {
					this.memCheckTimer = setInterval(() => this._checkMemoryAndResume(), 1000);
				}
			}

			// Resume when memory drops (also handled in _checkMemoryAndResume)
			if (heapUsed < this.resumeThresholdMB && this._isPaused) {
				this._isPaused = false;
				if (this.verbose) {
					console.log(`▶️  Throttle: Resuming GCS (memory: ${heapUsed.toFixed(0)}MB < ${this.resumeThresholdMB}MB)`);
				}

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

		if (this.verbose) {
			console.log(`📊 Throttle stats: ${this.objectCount} objects, ${this.pauseCount} pauses`);
		}
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