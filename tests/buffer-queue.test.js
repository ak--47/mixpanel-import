/**
 * Test for BufferQueue implementation
 * Verifies that the queue properly decouples source from sink
 */

const { BufferQueue } = require('../components/buffer-queue');
const { Readable, Writable } = require('stream');

describe('BufferQueue', () => {
	jest.setTimeout(10000);

	test('decouples fast source from slow sink', async () => {
		const bufferQueue = new BufferQueue({
			pauseThresholdMB: 0.001,  // Pause at 1KB for testing
			resumeThresholdMB: 0.0005, // Resume at 0.5KB
			verbose: false
		});

		// Track metrics
		let sourceReadCount = 0;
		let sinkWriteCount = 0;
		let sourcePaused = false;
		const sourceData = [];
		const sinkData = [];

		// Create a fast source (100 items/sec)
		const source = new Readable({
			objectMode: true,
			read() {
				if (sourceReadCount < 100) {
					const data = { id: sourceReadCount, data: 'x'.repeat(100) };
					sourceData.push(data);
					this.push(data);
					sourceReadCount++;
				} else {
					this.push(null);
				}
			}
		});

		// Monitor source pause/resume
		source.on('pause', () => { sourcePaused = true; });
		source.on('resume', () => { sourcePaused = false; });

		// Create a slow sink (10 items/sec)
		const sink = new Writable({
			objectMode: true,
			highWaterMark: 1,
			async write(chunk, encoding, callback) {
				// Simulate slow processing
				await new Promise(resolve => setTimeout(resolve, 10));
				sinkData.push(chunk);
				sinkWriteCount++;
				callback();
			}
		});

		// Set source stream for pause/resume control
		bufferQueue.setSourceStream(source);

		// Connect: source -> queue input
		const queueInput = bufferQueue.createInputStream();
		const queueOutput = bufferQueue.createOutputStream();

		// Pipe the streams
		source.pipe(queueInput);
		queueOutput.pipe(sink);

		// Wait for completion
		await new Promise((resolve, reject) => {
			sink.on('finish', resolve);
			sink.on('error', reject);
		});

		// Verify results
		expect(sourceReadCount).toBe(100);
		expect(sinkWriteCount).toBe(100);
		expect(sourceData).toEqual(sinkData);
		expect(sourcePaused).toBe(true); // Source should have been paused at some point
	});

	test('resumes source when buffer drains', async () => {
		const bufferQueue = new BufferQueue({
			pauseThresholdMB: 0.001,  // Pause at 1KB
			resumeThresholdMB: 0.0005, // Resume at 0.5KB
			verbose: false
		});

		let pauseCount = 0;
		let resumeCount = 0;

		// Create source
		const source = new Readable({
			objectMode: true,
			read() {
				for (let i = 0; i < 10; i++) {
					this.push({ data: 'x'.repeat(200) }); // Each item is 200+ bytes
				}
				this.push(null);
			}
		});

		// Track pause/resume events
		source.on('pause', () => { pauseCount++; });
		source.on('resume', () => { resumeCount++; });

		// Create sink
		const sink = new Writable({
			objectMode: true,
			async write(chunk, encoding, callback) {
				await new Promise(resolve => setTimeout(resolve, 5));
				callback();
			}
		});

		// Connect
		bufferQueue.setSourceStream(source);
		const queueInput = bufferQueue.createInputStream();
		const queueOutput = bufferQueue.createOutputStream();

		source.pipe(queueInput);
		queueOutput.pipe(sink);

		// Wait for completion
		await new Promise((resolve) => {
			sink.on('finish', resolve);
		});

		// Should have paused and resumed at least once
		expect(pauseCount).toBeGreaterThan(0);
		expect(resumeCount).toBeGreaterThan(0);
	});

	test('handles source completion correctly', async () => {
		const bufferQueue = new BufferQueue({
			pauseThresholdMB: 10, // High threshold so no pausing
			verbose: false
		});

		const data = [];

		// Create source that ends immediately after 5 items
		const source = new Readable({
			objectMode: true,
			read() {
				for (let i = 0; i < 5; i++) {
					this.push({ id: i });
				}
				this.push(null);
			}
		});

		// Create sink
		const sink = new Writable({
			objectMode: true,
			write(chunk, encoding, callback) {
				data.push(chunk);
				callback();
			}
		});

		// Connect
		const queueInput = bufferQueue.createInputStream();
		const queueOutput = bufferQueue.createOutputStream();

		source.pipe(queueInput);
		queueOutput.pipe(sink);

		// Wait for completion
		await new Promise((resolve) => {
			sink.on('finish', resolve);
		});

		// All data should have been transmitted
		expect(data).toHaveLength(5);
		expect(data[0]).toEqual({ id: 0 });
		expect(data[4]).toEqual({ id: 4 });
	});

	test('getStats returns correct statistics', () => {
		const bufferQueue = new BufferQueue({
			verbose: false
		});

		// Initial stats
		let stats = bufferQueue.getStats();
		expect(stats.queueLength).toBe(0);
		expect(stats.objectsQueued).toBe(0);
		expect(stats.objectsDequeued).toBe(0);
		expect(stats.isPaused).toBe(false);

		// Add some data to queue
		bufferQueue.queue.push({ data: 'test', size: 100 });
		bufferQueue.queueSizeBytes += 100;
		bufferQueue.objectsQueued++;

		stats = bufferQueue.getStats();
		expect(stats.queueLength).toBe(1);
		expect(stats.objectsQueued).toBe(1);
		expect(parseFloat(stats.queueSizeMB)).toBeCloseTo(0.0001, 4);
	});
});