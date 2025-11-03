// @ts-nocheck
/* eslint-disable no-undef */
/* eslint-disable no-unused-vars */

const {
	calculateOptimalWorkers,
	createEventSampler,
	createMemoryMonitor,
	applySmartDefaults
} = require('../components/smart-config.js');

// Mock v8 module for heap statistics
jest.mock('v8', () => ({
	getHeapStatistics: () => ({
		heap_size_limit: 2048 * 1024 * 1024  // 2GB heap for testing
	})
}));

describe('smart-config', () => {
	describe('calculateOptimalWorkers', () => {
		test('tiny events (< 500 bytes) allow maximum workers', () => {
			const result = calculateOptimalWorkers(300, 100);
			expect(result.category).toBe('tiny');
			expect(result.workers).toBeLessThanOrEqual(50); // Category max
			expect(result.workers).toBeGreaterThan(0);
			expect(result.batchSize).toBeGreaterThan(0);
		});

		test('small events (500B - 2KB) use moderate workers', () => {
			const result = calculateOptimalWorkers(1500, 100);
			expect(result.category).toBe('small');
			expect(result.workers).toBeLessThanOrEqual(30); // Category max
		});

		test('medium events (2KB - 5KB) reduce workers', () => {
			const result = calculateOptimalWorkers(3500, 100);
			expect(result.category).toBe('medium');
			expect(result.workers).toBeLessThanOrEqual(15); // Category max
		});

		test('large events (5KB - 10KB) use few workers', () => {
			const result = calculateOptimalWorkers(8000, 100);
			expect(result.category).toBe('large');
			expect(result.workers).toBeLessThanOrEqual(8); // Category max
		});

		test('dense events (> 10KB) use minimal workers', () => {
			const result = calculateOptimalWorkers(15000, 100);
			expect(result.category).toBe('dense');
			expect(result.workers).toBeLessThanOrEqual(5); // Category max
		});

		test('respects requested worker count as maximum', () => {
			const result = calculateOptimalWorkers(300, 10);
			expect(result.workers).toBeLessThanOrEqual(10);
		});

		test('returns at least 1 worker', () => {
			const result = calculateOptimalWorkers(100000, 100); // Huge events
			expect(result.workers).toBeGreaterThanOrEqual(1);
		});

		test('calculates reasonable batch size', () => {
			const result = calculateOptimalWorkers(1000, 10);
			expect(result.batchSize).toBeGreaterThan(0);
			expect(result.batchSize).toBeLessThanOrEqual(2000); // Mixpanel max
		});

		test('provides reasoning in response', () => {
			const result = calculateOptimalWorkers(12000, 50);
			expect(result.reasoning).toContain('workers');
			expect(result.reasoning).toContain('dense');
		});
	});

	describe('applySmartDefaults', () => {
		test('stores original worker count', () => {
			const job = { workers: 20 };
			applySmartDefaults(job);
			expect(job.originalWorkers).toBe(20);
		});

		test('enables adaptive scaling by default', () => {
			const job = { workers: 10 };
			applySmartDefaults(job);
			expect(job.adaptiveScaling).toBe(true);
		});

		test('respects explicit adaptiveScaling setting', () => {
			const job = { workers: 10, adaptiveScaling: false };
			applySmartDefaults(job);
			expect(job.adaptiveScaling).toBe(false);
		});

		test('uses avgEventSize hint if provided', () => {
			const job = {
				workers: 50,
				avgEventSize: 12000, // Dense event
				adaptiveScaling: true
			};
			applySmartDefaults(job);
			expect(job.workers).toBeLessThanOrEqual(5); // Dense category max
			expect(job.adaptiveWorkers).toBeLessThanOrEqual(5);
		});

		test('limits workers in low memory environment', () => {
			// Mock low memory
			jest.resetModules();
			jest.doMock('v8', () => ({
				getHeapStatistics: () => ({
					heap_size_limit: 400 * 1024 * 1024  // 400MB heap
				})
			}));
			const { applySmartDefaults } = require('../components/smart-config.js');

			const job = { workers: 100, highWater: 200 };
			applySmartDefaults(job);
			expect(job.workers).toBeLessThanOrEqual(5);
			expect(job.highWater).toBeLessThanOrEqual(50);
		});

		test('limits workers in medium memory environment', () => {
			// Mock medium memory
			jest.resetModules();
			jest.doMock('v8', () => ({
				getHeapStatistics: () => ({
					heap_size_limit: 1024 * 1024 * 1024  // 1GB heap
				})
			}));
			const { applySmartDefaults } = require('../components/smart-config.js');

			const job = { workers: 100 };
			applySmartDefaults(job);
			expect(job.workers).toBeLessThanOrEqual(20);
		});
	});

	describe('createEventSampler transform', () => {
		test('creates a transform stream', () => {
			const job = { originalWorkers: 10, workers: 10 };
			const sampler = createEventSampler(job);
			expect(sampler).toBeDefined();
			expect(sampler._transform).toBeDefined();
			expect(sampler._transform).toBeInstanceOf(Function);
		});

		test('samples events and stores statistics', (done) => {
			const job = {
				originalWorkers: 10,
				workers: 10,
				adaptiveScaling: true,
				recordsPerBatch: 2000
			};
			const sampler = createEventSampler(job, 2); // Sample just 2 events

			// Small test object
			const testData = { test: 'data', value: 123 };

			// Process first event
			sampler._transform(testData, 'utf8', (err, result) => {
				expect(err).toBeNull();
				expect(result).toBe(testData); // Should pass through

				// Process second event - should trigger configuration
				sampler._transform(testData, 'utf8', (err2, result2) => {
					expect(err2).toBeNull();
					expect(result2).toBe(testData);

					// Check that configuration was applied
					expect(job.detectedEventSize).toBeGreaterThan(0);
					expect(job.eventSizeStats).toBeDefined();
					expect(job.eventSizeStats.avg).toBeGreaterThan(0);
					expect(job.eventSizeStats.samples).toBe(2);

					done();
				});
			});
		});

		test('only configures once', (done) => {
			const job = {
				originalWorkers: 10,
				workers: 10,
				adaptiveScaling: false  // Disabled
			};
			const sampler = createEventSampler(job, 1); // Sample just 1 event

			const testData = { test: 'data' };
			const originalWorkers = job.workers;

			// Process events
			sampler._transform(testData, 'utf8', () => {
				sampler._transform(testData, 'utf8', () => {
					sampler._transform(testData, 'utf8', () => {
						// Workers should not change when adaptive scaling is disabled
						expect(job.workers).toBe(originalWorkers);
						done();
					});
				});
			});
		});
	});

	describe('createMemoryMonitor transform', () => {
		test('creates a transform stream', () => {
			const job = { highWater: 100 };
			const monitor = createMemoryMonitor(job);
			expect(monitor).toBeDefined();
			expect(monitor._transform).toBeDefined();
			expect(monitor._transform).toBeInstanceOf(Function);
		});

		test('passes data through', (done) => {
			const job = { highWater: 100, verbose: false };
			const monitor = createMemoryMonitor(job);
			const testData = { test: 'data' };

			monitor._transform(testData, 'utf8', (err, result) => {
				expect(err).toBeNull();
				expect(result).toBe(testData);
				done();
			});
		});

		test('monitors memory periodically', (done) => {
			const job = { highWater: 100, verbose: true };
			const monitor = createMemoryMonitor(job);
			const testData = { test: 'data' };

			// Mock memory usage
			const originalMemoryUsage = process.memoryUsage;
			process.memoryUsage = () => ({
				heapUsed: 1000 * 1024 * 1024,  // 1GB used
				heapTotal: 1500 * 1024 * 1024,
				rss: 2000 * 1024 * 1024,
				external: 100 * 1024 * 1024,
				arrayBuffers: 50 * 1024 * 1024
			});

			// Spy on console methods
			const consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation();
			const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();

			monitor._transform(testData, 'utf8', (err, result) => {
				expect(err).toBeNull();
				expect(result).toBe(testData);

				// Restore
				process.memoryUsage = originalMemoryUsage;
				consoleWarnSpy.mockRestore();
				consoleLogSpy.mockRestore();
				done();
			});
		});
	});

	describe('integration', () => {
		test('smart config can be disabled', () => {
			const job = {
				workers: 100,
				adaptive: false,  // Disabled
				adaptiveScaling: false  // Explicitly disable
			};

			// This would normally be called by pipeline
			applySmartDefaults(job);

			// With 2GB heap, medium memory environment limits to 20 workers
			// This is a safety limit that applies regardless of adaptive scaling
			expect(job.workers).toBeLessThanOrEqual(20);
			expect(job.adaptiveScaling).toBe(false);
		});

		test('smart config respects user hints', () => {
			const job = {
				workers: 50,
				avgEventSize: 15000,  // Dense hint
				adaptive: true
			};

			applySmartDefaults(job);

			// Should respect the hint and reduce workers
			expect(job.workers).toBeLessThanOrEqual(5);
		});
	});
});