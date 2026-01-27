const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');
const HTTP_AGENT = new https.Agent({ keepAlive: true, maxSockets: 50 });

// Undici imports for high-performance HTTP
const { Pool } = require('undici');

// Add global error handlers to catch undici issues
// IMPORTANT: Log but DON'T exit - let the application handle errors gracefully
let unhandledRejectionCount = 0;
let uncaughtExceptionCount = 0;

process.on('unhandledRejection', (reason, promise) => {
	unhandledRejectionCount++;
	console.error(`\n❌ [ERROR #${unhandledRejectionCount}] Unhandled Promise Rejection:`);
	console.error('Reason:', reason);
	console.error('Promise:', promise);
	// @ts-ignore - reason might have a stack property if it's an Error
	console.error('Stack:', reason?.stack || 'No stack trace available');
	console.error('This error was caught but the process will continue.\n');

	// Only exit if we get too many errors in a short time (likely fatal)
	if (unhandledRejectionCount > 10) {
		console.error('❌ Too many unhandled rejections (>10). Exiting to prevent corruption.');
		process.exit(1);
	}
});

process.on('uncaughtException', (error) => {
	uncaughtExceptionCount++;
	console.error(`\n❌ [ERROR #${uncaughtExceptionCount}] Uncaught Exception:`);
	console.error('Error:', error);
	console.error('Stack:', error?.stack || 'No stack trace available');
	console.error('This error was caught but the process will continue.\n');

	// Only exit if we get too many errors in a short time (likely fatal)
	if (uncaughtExceptionCount > 10) {
		console.error('❌ Too many uncaught exceptions (>10). Exiting to prevent corruption.');
		process.exit(1);
	}
});

// Undici pool settings - shared across all jobs
// Formula: connections = workers * 3-5, pipelining = workers / 2
const poolConfig = {
	connections: 100, // 20-30 workers @ 3-5 connections each
	pipelining: 20,   // HTTP/2 multiplexing
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

		// Only add project_id if using service account auth (not secret auth)
		// Secret-based auth doesn't want project_id in the URL
		// @ts-ignore
		if (job.project && !job.secret) options.searchParams.project_id = job.project;

		let res, success;
		try {
			// @ts-ignore
			const { body } = await got(options);
			res = JSON.parse(body);
			success = true;
		}

		catch (e) {
			if (u.isJSONStr(e?.response?.body)) {
				res = JSON.parse(e.response.body);
			}
			else {
				// Extract minimal error info to prevent memory leak from full error object
				res = {
					error: e?.message || e?.code || 'Request failed',
					status: false,
					code: e?.response?.statusCode || e?.code || 500,
					// Include stack trace only in verbose mode for debugging
					...(process.env.VERBOSE && { stack: e?.stack })
				};
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

		// MEMORY FIX: Store abbreviated responses to prevent memory issues
		// Even in unabridged mode, we store only essential fields for monitoring
		if (!job.abridged) {
			// Store abbreviated version for monitoring without memory bloat
			const abbreviatedForStorage = {
				num_records_imported: res.num_records_imported || 0,
				num_failed: res?.failed_records?.length || 0,
				// Don't include generic error if we have specific failed_records
				// The generic error "some data points in the request failed validation"
				// is just a wrapper - the real errors are in failed_records
				error: res?.failed_records?.length ? null : (res.error || null),
				status: res.status !== undefined ? res.status : success,
				code: res.code || (success ? 200 : 400)
			};
			// Don't pass batch to prevent memory leaks - badRecords feature is opt-in now
			job.store(abbreviatedForStorage, success, null);
		} else {
			// Abridged mode - don't store anything
			job.store(null, success, null);
		}

		// Return minimal response to prevent memory leaks in parallel-transform buffer
		const minimalResponse = {
			num_records_imported: res.num_records_imported || 0,
			num_good_events: res.num_good_events || 0,
			failed_records: res?.failed_records ? [] : undefined,  // Empty array to prevent iteration errors
			error: res.error || null,
			status: res.status !== undefined ? res.status : success,
			code: res.code || (success ? 200 : 400)
		};

		// Return batch only for dry run or custom handlers
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
		// Must return array on error (prevents "result is not iterable")
		return [{ error: e.message || 'Unknown error' }, null];
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
		// Only add project_id if using service account auth (not secret auth)
		if (job.project && !job.secret) {
			searchParams.set('project_id', String(job.project));
		}

		if (job.project && job.acct && job.secret) {
			if (atob(job.auth.split("Basic ")?.pop())?.split(":")?.pop().length > 2) {
				//probably service account auth; need project_id
				searchParams.set('project_id', String(job.project));
			}
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
			if (res?.failed_records?.length) {
				for (const error of res.failed_records) {
					const { index, message } = error;
					// Update error counts (this was missing!)
					if (!job.errors[message]) job.errors[message] = 0;
					job.errors[message]++;

					// Store bad records if not in abridged mode
					if (!job.abridged) {
						job.addBadRecord(message, batch[index]); // Use bounded method
					}
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

		// MEMORY FIX: Store abbreviated responses to prevent memory issues
		// Even in unabridged mode, we store only essential fields for monitoring
		if (!job.abridged) {
			// Store abbreviated version for monitoring without memory bloat
			const abbreviatedForStorage = {
				num_records_imported: res.num_records_imported || 0,
				num_failed: res?.failed_records?.length || 0,
				// Don't include generic error if we have specific failed_records
				// The generic error "some data points in the request failed validation"
				// is just a wrapper - the real errors are in failed_records
				error: res?.failed_records?.length ? null : (res.error || null),
				status: res.status !== undefined ? res.status : success,
				code: res.code || (success ? 200 : 400)
			};
			// Don't pass batch to prevent memory leaks - badRecords feature is opt-in now
			job.store(abbreviatedForStorage, success, null);
		} else {
			// Abridged mode - don't store anything
			job.store(null, success, null);
		}

		// Return minimal response to prevent memory leaks in parallel-transform buffer
		const minimalResponse = {
			num_records_imported: res.num_records_imported || 0,
			num_good_events: res.num_good_events || 0,
			failed_records: res?.failed_records ? [] : undefined,  // Empty array to prevent iteration errors
			error: res.error || null,
			status: res.status !== undefined ? res.status : success,
			code: res.code || (success ? 200 : 400)
		};

		// Return batch only for dry run or custom handlers
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