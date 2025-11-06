/**
 * Memory monitoring for the pipeline
 * Provides real-time memory usage feedback and warnings
 */

const { Transform } = require('stream');
const v8 = require('v8');
const u = require('ak-tools');

/**
 * Create a memory monitor that tracks memory usage and provides warnings
 * @param {Object} job - The job configuration object
 * @returns {Transform} A transform stream that monitors memory
 */
function createMemoryMonitor(job) {
	const CHECK_INTERVAL = 5000; // Check every 5 seconds
	let lastCheck = Date.now();
	let warningCount = 0;
	let lastGC = Date.now();
	const GC_INTERVAL = job.aggressiveGC ? 30000 : 60000; // GC every 30s if aggressive, else 60s

	return new Transform({
		objectMode: true,
		highWaterMark: job.highWater,
		transform(data, encoding, callback) {
			const now = Date.now();

			if (now - lastCheck >= CHECK_INTERVAL) {
				lastCheck = now;

				const memUsage = process.memoryUsage();
				const heapUsed = memUsage.heapUsed;
				const heapStats = v8.getHeapStatistics();
				const heapLimit = heapStats.heap_size_limit;
				const heapPercent = (heapUsed / heapLimit) * 100;

				// Emergency brake at 90% heap
				if (heapPercent > 90) {
					console.error(`
🚨 CRITICAL MEMORY WARNING: ${heapPercent.toFixed(1)}% heap used (${u.bytesHuman(heapUsed)})!
   Immediate actions:
   1. Reduce workers to 1-2
   2. Reduce batch size
   3. Enable compression
   4. Consider filtering unnecessary properties
					`);

					// Force GC if available
					if (global.gc) {
						const beforeGC = process.memoryUsage().heapUsed;
						global.gc();
						const afterGC = process.memoryUsage().heapUsed;
						console.log(`🗑️  Emergency GC: ${u.bytesHuman(beforeGC)} → ${u.bytesHuman(afterGC)} (freed ${u.bytesHuman(beforeGC - afterGC)})`);
					}

					// Slow down processing to allow memory to be freed
					setTimeout(() => callback(null, data), 500);
					return;
				}

				// Warning at 75% heap
				if (heapPercent > 75) {
					warningCount++;
					if (warningCount % 10 === 1) { // Don't spam warnings
						console.warn(`⚠️  Memory pressure: ${heapPercent.toFixed(1)}% heap used (${u.bytesHuman(heapUsed)}). Consider reducing workers.`);
					}
				}

				// Aggressive GC mode - periodic garbage collection
				if (job.aggressiveGC && global.gc && (now - lastGC >= GC_INTERVAL)) {
					lastGC = now;
					if (heapPercent > 50) { // Only GC if memory usage is notable
						const beforeGC = process.memoryUsage().heapUsed;
						global.gc();
						const afterGC = process.memoryUsage().heapUsed;
						if (job.verbose) {
							console.log(`♻️  Periodic GC: ${u.bytesHuman(beforeGC)} → ${u.bytesHuman(afterGC)} (freed ${u.bytesHuman(beforeGC - afterGC)})`);
						}
					}
				}

				// Log memory status periodically in verbose mode
				if (job.verbose && heapPercent > 30 && warningCount % 20 === 0) {
					console.log(`📊 Memory: ${heapPercent.toFixed(1)}% heap (${u.bytesHuman(heapUsed)} / ${u.bytesHuman(heapLimit)})`);
				}
			}

			callback(null, data);
		}
	});
}

module.exports = {
	createMemoryMonitor
};