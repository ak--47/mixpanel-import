const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');
const HTTP_AGENT = new https.Agent({ keepAlive: true, maxSockets: 50 });

// Undici imports for high-performance HTTP
const { Pool } = require('undici');

// Add global error handlers to catch undici issues
process.on('unhandledRejection', (reason, promise) => {
	console.error('[UNDICI] Unhandled Rejection at:', promise, 'reason:', reason);
});

process.on('uncaughtException', (error) => {
	console.error('[UNDICI] Uncaught Exception:', error);
	process.exit(1);
});

// Optimized undici pool settings for high throughput
// Note: These are shared across all jobs, so we set high defaults
// Ideal formula: connections = workers * 3-5, pipelining = workers / 2
// Current settings support up to ~30-50 workers efficiently
const poolConfig = {
	connections: 100, // Supports 20-30 workers with 3-5 connections each
	pipelining: 20,   // HTTP/2 multiplexing - allows 20 requests per connection
	keepAliveTimeout: 30000,
	keepAliveMaxTimeout: 60000,
	headersTimeout: 60000,
	bodyTimeout: 60000,
	connectTimeout: 10000
};

// Shared undici pool for maximum connection reuse and performance
const UNDICI_POOL = new Pool('https://api.mixpanel.com', poolConfig);

// Shared undici pool for EU region
const UNDICI_POOL_EU = new Pool('https://api-eu.mixpanel.com', poolConfig);

// Shared undici pool for IN region
const UNDICI_POOL_IN = new Pool('https://api-in.mixpanel.com', poolConfig);

// Cleanup pools on process exit
process.on('exit', () => {
	UNDICI_POOL.close();
	UNDICI_POOL_EU.close();
	UNDICI_POOL_IN.close();
});

process.on('SIGINT', () => {
	UNDICI_POOL.close();
	UNDICI_POOL_EU.close();
	UNDICI_POOL_IN.close();
	process.exit(0);
});

process.on('SIGTERM', () => {
	UNDICI_POOL.close();
	UNDICI_POOL_EU.close();
	UNDICI_POOL_IN.close();
	process.exit(0);
});

/** @typedef {import('./job')} JobConfig */




/**
 * @param  {Object[]} batch
 * @param  {JobConfig} job
 */
async function flushToMixpanel(batch, job) {
	try {
		/** @type {Buffer | string} */
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (job.recordType === 'event' && job.compress) {
			body = await gzip(body, { level: job.compressionLevel || 6 });
			job.encoding = 'gzip';
		}

		/** @type {got.Options} */
		const options = {
			url: job.url,
			searchParams: {
				ip: 0,
				verbose: 1,
				strict: Number(job.strict)
			},
			method: job.reqMethod || 'POST',
			retry: {
				limit: job.maxRetries || 10,
				statusCodes: [429, 500, 501, 503, 524, 502, 408, 504],
				errorCodes: [
					`ETIMEDOUT`,
					`ECONNRESET`,
					`EADDRINUSE`,
					`ECONNREFUSED`,
					`EPIPE`,
					`ENOTFOUND`,
					`ENETUNREACH`,
					`EAI_AGAIN`,
					`ESOCKETTIMEDOUT`,
					`ECONNABORTED`,
					`EHOSTUNREACH`,
					`EPROTO`,
					`ETLSHANDSHAKE`
				],
				methods: ['POST']
			},
			headers: {
				"Authorization": `${job.auth}`,
				"Content-Type": job.contentType,
				"Content-Encoding": job.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			//consider timeout + agent timeout
			agent: {
				https: HTTP_AGENT
			},
			http2: false,
			timeout: {
				request: 30000,  // 30 second total request timeout
				response: 10000,  // 10 second to start receiving response
			},
			hooks: {
				// @ts-ignore
				beforeRetry: [(req, error, count) => {
					try {
						// @ts-ignore
						l(`got ${error.message}...retrying request...#${count}`);
					}
					catch (e) {
						//noop
					}
					job.retries++;
					job.requests++;
					if (error?.response?.statusCode?.toString() === "429") {
						job.rateLimited++;
					}
					else if (error?.response?.statusCode?.toString()?.startsWith("5")) {
						job.serverErrors++;
					}
					else {
						job.clientErrors++;
					}
				}],

			},
			body
		};

		if (job.http2) {
			options.http2 = true;
			delete options.headers?.Connection;

		}

		// @ts-ignore
		if (job.project) options.searchParams.project_id = job.project;

		let res, success;
		try {
			// @ts-ignore
			const req = await got(options);
			res = JSON.parse(req.body);
			success = true;
		}

		catch (e) {
			if (u.isJSONStr(e?.response?.body)) {
				res = JSON.parse(e.response.body);
			}
			else {
				res = e;
			}
			success = false;



		}

		if (job.recordType === 'event' || job.recordType === "scd") {
			job.success += res.num_records_imported || 0;
			job.failed += res?.failed_records?.length || 0;
			if (!job.abridged && res?.failed_records?.length) {
				for (const error of res.failed_records) {
					const { index, message } = error;
					job.addBadRecord(message, batch[index]); // Use bounded method
				}
			}
		}
		else if (job.recordType === 'user' || job.recordType === 'group') {
			if (!res.error || res.status) {
				if (res.num_good_events) {
					job.success += res.num_good_events;
				}
				else {
					job.success += job.lastBatchLength;
				}
			}
			if (res.error || !res.status) job.failed += job.lastBatchLength;
		}

		job.store(res, success);

		// MEMORY FIX: Return minimal response data, not full response object
		// The full response object was being stored in parallel-transform's buffer causing memory leaks
		// We only need success/fail counts for logging, everything else is already handled above

		// Extract only essential data for logging
		const minimalResponse = {
			success: res.num_records_imported || res.num_good_events || 0,
			failed: res?.failed_records?.length || 0,
			error: res.error || null,
			status: res.status || success
		};

		// Only return batch if needed (dry run or custom response handler)
		// This prevents memory accumulation in production imports
		if (job.dryRun || job.responseHandler) {
			return [res, batch];  // Keep full response for dry run/custom handlers
		}
		return [minimalResponse, null];
	}

	catch (e) {
		try {
			// @ts-ignore
			l(`\nBATCH FAILED: ${e.message}\n`);
		}
		catch (e) {
			//noop
		}
	}
}

/**
 * High-performance Mixpanel flush using undici
 * Drop-in replacement for flushToMixpanel with better performance
 * @param  {Object[]} batch
 * @param  {JobConfig} job
 */
async function flushToMixpanelWithUndici(batch, job) {
	try {

		/** @type {Buffer | string} */
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (job.recordType === 'event' && job.compress) {
			body = await gzip(body, { level: job.compressionLevel || 6 });
			job.encoding = 'gzip';
		}

		// Build search params manually for better performance
		const searchParams = new URLSearchParams({
			ip: '0',
			verbose: '1',
			strict: Number(job.strict).toString()
		});
		if (job.project) {
			searchParams.set('project_id', String(job.project));
		}

		// Build headers
		const headers = {
			"Authorization": `${job.auth}`,
			"Content-Type": job.contentType,
			"Accept": "application/json"
		};

		// Add encoding header if compressed
		if (job.encoding) {
			headers["Content-Encoding"] = job.encoding;
		}

		// Add connection header for HTTP/1.1 (unless HTTP/2)
		if (!job.http2) {
			headers["Connection"] = "keep-alive";
		}

		// Select appropriate pool based on job URL (more efficient)
		let pool = UNDICI_POOL; // Default to US
		if (job.url.includes('api-eu.mixpanel.com')) {
			pool = UNDICI_POOL_EU;
		} else if (job.url.includes('api-in.mixpanel.com')) {
			pool = UNDICI_POOL_IN;
		}

		// Get pathname from job URL efficiently
		const url = new URL(job.url);
		const pathname = url.pathname + '?' + searchParams.toString();

		// Retry configuration matching original
		const retryConfig = {
			maxRetries: job.maxRetries || 10,
			retryStatusCodes: new Set([429, 500, 501, 503, 524, 502, 408, 504]),
			retryErrorCodes: new Set([
				'ETIMEDOUT', 'ECONNRESET', 'EADDRINUSE', 'ECONNREFUSED',
				'EPIPE', 'ENOTFOUND', 'ENETUNREACH', 'EAI_AGAIN',
				'ESOCKETTIMEDOUT', 'ECONNABORTED', 'EHOSTUNREACH',
				'EPROTO', 'ETLSHANDSHAKE', 'UND_ERR_CONNECT_TIMEOUT',
				'UND_ERR_SOCKET', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT'
			])
		};

		let retryCount = 0;
		let lastError;
		let res, success = false;

		// Retry loop
		while (retryCount <= retryConfig.maxRetries) {
			try {
				// Make request directly on pool for maximum performance
				const response = await pool.request({
					path: pathname,
					method: job.reqMethod || 'POST',
					headers,
					body,
					blocking: false // Enable pipelining for better performance
					// Note: throwOnError is not valid for pool.request(), only for global request()
				});

				// Read response body
				const responseBody = await response.body.text();
				
				// Parse JSON response
				if (u.isJSONStr(responseBody)) {
					res = JSON.parse(responseBody);
				} else {
					res = { error: 'Invalid JSON response', raw: responseBody };
				}

				// Check if we should retry based on status code
				if (retryConfig.retryStatusCodes.has(response.statusCode) && retryCount < retryConfig.maxRetries) {
					// Handle retry logging and stats
					try {
						// @ts-ignore
						l(`undici got status ${response.statusCode}...retrying request...#${retryCount + 1}`);
					} catch (e) {
						// noop
					}
					
					job.retries++;
					job.requests++;
					
					if (response.statusCode === 429) {
						job.rateLimited++;
					} else if (response.statusCode >= 500) {
						job.serverErrors++;
					} else {
						job.clientErrors++;
					}
					
					retryCount++;
					continue;
				}

				success = response.statusCode >= 200 && response.statusCode < 300;
				break;

			} catch (error) {
				lastError = error;
				
				// Enhanced error logging for debugging
				console.error(`[UNDICI ERROR] ${error.message}`, {
					code: error.code,
					name: error.name,
					retryCount,
					batchSize: Array.isArray(batch) ? batch.length : 'unknown'
				});
				
				// Check if we should retry based on error code
				const shouldRetry = retryConfig.retryErrorCodes.has(error.code) && retryCount < retryConfig.maxRetries;
				
				if (shouldRetry) {
					// Handle retry logging and stats
					try {
						// @ts-ignore
						l(`undici got ${error.message}...retrying request...#${retryCount + 1}`);
					} catch (e) {
						// noop
					}
					
					job.retries++;
					job.requests++;
					job.clientErrors++;
					
					retryCount++;
					
					// Add exponential backoff for retries
					await new Promise(resolve => setTimeout(resolve, Math.min(1000 * Math.pow(2, retryCount), 5000)));
					continue;
				}

				// No more retries, handle the error
				if (error.response && u.isJSONStr(error.response.body)) {
					res = JSON.parse(error.response.body);
				} else {
					res = error;
				}
				success = false;
				break;
			}
		}

		// Handle case where all retries exhausted
		if (retryCount > retryConfig.maxRetries && !success) {
			if (lastError) {
				res = lastError;
			}
			success = false;
		}

		// Update job stats based on record type (same logic as original)
		if (job.recordType === 'event' || job.recordType === "scd") {
			job.success += res.num_records_imported || 0;
			job.failed += res?.failed_records?.length || 0;
			if (!job.abridged && res?.failed_records?.length) {
				for (const error of res.failed_records) {
					const { index, message } = error;
					job.addBadRecord(message, batch[index]); // Use bounded method
				}
			}
		}
		else if (job.recordType === 'user' || job.recordType === 'group') {
			if (!res.error || res.status) {
				if (res.num_good_events) {
					job.success += res.num_good_events;
				}
				else {
					job.success += job.lastBatchLength;
				}
			}
			if (res.error || !res.status) job.failed += job.lastBatchLength;
		}

		job.store(res, success);

		// MEMORY FIX: Return minimal response data, not full response object
		// The full response object was being stored in parallel-transform's buffer causing memory leaks
		// We only need success/fail counts for logging, everything else is already handled above

		// Extract only essential data for logging
		const minimalResponse = {
			success: res.num_records_imported || res.num_good_events || 0,
			failed: res?.failed_records?.length || 0,
			error: res.error || null,
			status: res.status || success
		};

		// Only return batch if needed (dry run or custom response handler)
		// This prevents memory accumulation in production imports
		if (job.dryRun || job.responseHandler) {
			return [res, batch];  // Keep full response for dry run/custom handlers
		}
		return [minimalResponse, null];
	}

	catch (e) {
		try {
			// @ts-ignore
			l(`\nBATCH FAILED: ${e.message}\n`);
		}
		catch (e) {
			// noop
		}
	}
}


/**
 * @param  {any} csvString
 * @param  {JobConfig} config
 */
async function flushLookupTable(csvString, config) {
	const res = await flushToMixpanel(csvString, config);
	config.recordsProcessed = csvString.split('\n').length - 1;
	config.success = config.recordsProcessed;
	return res;
}


module.exports = {
	flushToMixpanel,
	flushToMixpanelWithUndici,
	flushLookupTable
};