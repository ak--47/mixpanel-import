const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');
const HTTP_AGENT = new https.Agent({ keepAlive: true, maxSockets: 100 });

// Undici imports for high-performance HTTP
const { Pool } = require('undici');

// NOTE: this module registers NO process-global handlers (uncaughtException,
// unhandledRejection, exit, SIGINT, SIGTERM). It is a library; process-level policy belongs to
// the application embedding it. See tests/handlers.test.js for the regression guard.

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

// Shared undici pools, keyed by origin; created lazily on first use so that requiring this
// module allocates nothing, and so destroy() can be followed by further imports.
/** @type {Map<string, Pool>} */
const UNDICI_POOLS = new Map();

/**
 * resolve a mixpanel API url to the origin its pool should target.
 *
 * only ingest urls reach this function: corePipeline diverts export/annotation/table record types
 * to exporters.js (which uses `got`) before the undici sender runs, and export-import reassigns
 * recordType to event/user/group first. so in practice this yields exactly the three ingest
 * origins the module used to hardcode — us, eu, in.
 *
 * deriving the origin from the url instead of pattern-matching two hostnames is simply less to get
 * wrong: a pool is always pointed at the host its requests are addressed to, with no default-case
 * assumption to revisit if a new endpoint is ever routed through this transport.
 * @param  {string} url
 * @returns {string}
 */
function originFor(url) {
	try {
		return new URL(url).origin;
	} catch (e) {
		return 'https://api.mixpanel.com';
	}
}

/**
 * get (or lazily create) the shared undici pool for a given mixpanel API url
 * @param  {string} url
 * @returns {Pool}
 */
function getPool(url) {
	const origin = originFor(url);
	let pool = UNDICI_POOLS.get(origin);
	if (!pool) {
		pool = new Pool(origin, poolConfig);
		// scoped replacement for the process-global handlers this module used to install;
		// surfaces socket-level failures without touching process-wide crash semantics
		pool.on('connectionError', (_origin, _targets, error) => {
			try {
				// @ts-ignore
				l(`undici connection error to ${origin}: ${error?.message || error}`);
			} catch (e) {
				// noop; l() is only global in CLI mode
			}
		});
		UNDICI_POOLS.set(origin, pool);
	}
	return pool;
}

/**
 * release all shared HTTP connection pools.
 *
 * long-lived processes that import occasionally may call this to free sockets between jobs; it is
 * NOT required for CLI use or short-lived scripts. safe to call repeatedly, and safe to call
 * before further imports (pools are re-created on demand).
 * @returns {Promise<void>}
 */
async function destroy() {
	const pools = [...UNDICI_POOLS.values()];
	UNDICI_POOLS.clear();
	await Promise.all(pools.map(pool => pool.close()));
}

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
				limit: job.maxRetries ?? 10,
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

		// Select appropriate pool based on job URL (lazily created, keyed by origin)
		const pool = getPool(job.url);

		// Get pathname from job URL efficiently
		const url = new URL(job.url);
		const pathname = url.pathname + '?' + searchParams.toString();

		// Retry configuration matching original
		const retryConfig = {
			maxRetries: job.maxRetries ?? 10,
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
	flushLookupTable,
	destroy
};