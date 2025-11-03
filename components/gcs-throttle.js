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
		this.isPaused = false;
		this.lastCheck = Date.now();
		this.objectCount = 0;
		this.pauseCount = 0;
	}

	_transform(chunk, encoding, callback) {
		this.objectCount++;
		const now = Date.now();

		// Check memory periodically
		if (now - this.lastCheck >= this.checkInterval) {
			this.lastCheck = now;
			const heapUsed = process.memoryUsage().heapUsed / 1024 / 1024;

			// Pause if memory is too high
			if (heapUsed > this.pauseThresholdMB && !this.isPaused) {
				this.isPaused = true;
				this.pauseCount++;

				console.log(`🛑 Throttle: Pausing GCS (memory: ${heapUsed.toFixed(0)}MB > ${this.pauseThresholdMB}MB) [pause #${this.pauseCount}]`);

				// Apply backpressure by delaying callback
				// This will cause upstream to pause
				const checkMemory = setInterval(() => {
					const currentHeap = process.memoryUsage().heapUsed / 1024 / 1024;

					if (currentHeap < this.resumeThresholdMB) {
						clearInterval(checkMemory);
						this.isPaused = false;
						console.log(`▶️  Throttle: Resuming GCS (memory: ${currentHeap.toFixed(0)}MB < ${this.resumeThresholdMB}MB)`);
						callback(null, chunk);
					}
				}, 200); // Check every 200ms while paused

				return; // Don't call callback yet - this applies backpressure!
			}
		}

		// Normal flow - pass through immediately
		callback(null, chunk);
	}

	_final(callback) {
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