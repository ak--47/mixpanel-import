/*
----
CLOUD READ RESILIENCE
----
Primitives for surviving transient network failures on cloud-storage reads:

- classifyCloudError(): retriable vs not-found vs fatal
- retryCloudOp(): bounded exponential-backoff retry for creation-time steps
  (metadata probes, stream opens) — safe because no bytes have entered the
  pipeline yet. Mid-stream failures must NOT be retried this way.
- armIdleTimeout(): transport idle watchdog (moved from parsers.js)
- createResilientGCSSource(): a byte-mode PassThrough that is a drop-in
  replacement for a raw GCS read stream, but reconnects on stalls/resets by
  reopening the object at the last-received byte offset via a range read.
  Because it sits UPSTREAM of gunzip, the decompressor's zlib state survives
  reconnects untouched — it just sees a pause in data. A wrong offset would
  corrupt the gzip framing and error out downstream, which is the safety net:
  we either resume exactly, or the job fails loudly like today.
*/

const stream = require('stream');

/** codes that indicate a transient transport problem worth retrying */
const RETRIABLE_CODES = new Set([
	'ECONNRESET', 'ETIMEDOUT', 'ESOCKETTIMEDOUT', 'ECONNABORTED', 'ECONNREFUSED',
	'EPIPE', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH',
	'ESTALL', // idle watchdog (armIdleTimeout below)
	'EEARLYEOF' // clean 'end' before all object bytes arrived
]);

/** HTTP statuses (GCS ApiError puts these in err.code) worth retrying */
const RETRIABLE_STATUSES = new Set([408, 429, 500, 502, 503, 504]);

/** a resumed connection must deliver this many bytes before we reset the
 * consecutive-no-progress attempt counter — big flaky files complete, while
 * a truly dead read still fails after `cloudResumeAttempts` fruitless tries */
const RESUME_PROGRESS_RESET_BYTES = 10 * 1024 * 1024;

/**
 * Classify a cloud-storage error for retry/skip/fail decisions.
 * Unknown errors are fatal — never retry what we don't understand.
 * @param {any} err
 * @returns {'retriable' | 'not-found' | 'fatal'}
 */
function classifyCloudError(err) {
	if (!err) return 'fatal';
	const code = err.code;
	// normalize an HTTP status from the shapes GCS/teeny-request/gaxios/AWS SDK
	// produce: numeric err.code (ApiError), numeric-string err.code, err.status,
	// err.response.statusCode, err.$metadata.httpStatusCode (AWS SDK v3)
	let status;
	if (typeof code === 'number') status = code;
	else if (typeof code === 'string' && /^\d{3}$/.test(code)) status = Number(code);
	else if (typeof err.status === 'number') status = err.status;
	else if (typeof err?.response?.statusCode === 'number') status = err.response.statusCode;
	else if (typeof err?.$metadata?.httpStatusCode === 'number') status = err.$metadata.httpStatusCode;
	if (code === 'ENOENT' || status === 404) return 'not-found';
	if (err.name === 'NoSuchKey' || err.name === 'NotFound' || err.name === 'NoSuchBucket') return 'not-found'; // AWS SDK v3
	if (typeof err.message === 'string' && /file not found/i.test(err.message)) return 'not-found';
	if (typeof code === 'string' && RETRIABLE_CODES.has(code)) return 'retriable';
	if (status !== undefined && RETRIABLE_STATUSES.has(status)) return 'retriable';
	return 'fatal';
}

/**
 * Bounded retry with exponential backoff for CREATION-TIME cloud operations
 * (exists/getMetadata/stream open). Not-found and fatal errors rethrow
 * immediately; only retriable transport errors are retried.
 * @param {() => Promise<any>} fn
 * @param {{attempts?: number, baseMs?: number, label?: string, onRetry?: (err: any, attempt: number) => void}} [opts]
 * @returns {Promise<any>}
 */
async function retryCloudOp(fn, opts = {}) {
	const { attempts = 3, baseMs = 1000, label = '', onRetry = null } = opts;
	let lastErr;
	for (let attempt = 1; attempt <= attempts; attempt++) {
		try {
			return await fn();
		} catch (err) {
			lastErr = err;
			if (classifyCloudError(err) !== 'retriable' || attempt === attempts) throw err;
			if (onRetry) {
				try { onRetry(err, attempt); } catch (e) { /* user callback must not break the retry loop */ }
			}
			await new Promise(resolve => setTimeout(resolve, baseMs * 2 ** (attempt - 1)));
		}
	}
	throw lastErr; // unreachable, but keeps static analysis happy
}

/**
 * Transport-agnostic idle watchdog: if no bytes arrive for `ms`, destroy the
 * stream with an error. Catches silent half-open / stalled cloud downloads
 * (TCP connection that stops delivering bytes but never sends FIN/RST) that a
 * request-level `timeout` can miss. Because the stream is part of a pipeline,
 * destroy(err) propagates cleanly to the destination.
 * @param {import('stream').Readable} readable
 * @param {number} ms idle threshold in milliseconds (<=0 disables)
 * @param {string} label included in the error message for diagnosis
 * @param {any} [job] JobConfig — when provided, stalls increment job.stallsDetected
 *   so the counter is meaningful even with resumeOnStall off (that's exactly
 *   when users need it to decide whether to enable the flag)
 * @returns {import('stream').Readable} the same stream (for chaining)
 */
function armIdleTimeout(readable, ms, label, job = null) {
	if (!ms || ms <= 0) return readable;
	let timer;
	const reset = () => {
		clearTimeout(timer);
		timer = setTimeout(() => {
			const err = new Error(`cloud read stalled: no data for ${ms}ms (${label})`);
			// @ts-ignore - tag so retry/resume logic can recognize a stall
			err.code = 'ESTALL';
			if (job) job.stallsDetected++;
			readable.destroy(err);
		}, ms);
	};
	const clear = () => clearTimeout(timer);
	readable.on('data', reset);
	readable.once('end', clear);
	readable.once('close', clear);
	readable.once('error', clear);
	reset();
	return readable;
}

/**
 * Fire the job's cloudStreamCallback (if any) with a serializable event.
 * A throwing user callback must never break the pipeline.
 * @param {any} job
 * @param {{type: string, file: string, byteOffset: number, attempt: number, error?: {message: string, code?: string|number}}} event
 */
function emitCloudEvent(job, event) {
	if (typeof job?.cloudStreamCallback !== 'function') return;
	try { job.cloudStreamCallback(event); } catch (e) { /* swallow */ }
}

/** @param {any} err @returns {{message: string, code?: string|number}} */
function serializeError(err) {
	const out = { message: err?.message || String(err) };
	if (err?.code !== undefined) out.code = err.code;
	return out;
}

/**
 * Create a resilient GCS byte source: drop-in replacement for
 * `gcsFile.createReadStream(readOpts)` + `armIdleTimeout(...)` that reconnects
 * on stalls and retriable transport errors by range-reading from the last
 * byte offset that was handed downstream.
 *
 * Contract (best-effort, per the change request):
 * - Offsets count RAW (compressed, wire) bytes; the wrapper sits upstream of
 *   any gunzip stage.
 * - The object's generation is pinned on the first metadata probe; a reopen
 *   can never read a different version of the object (overwrite mid-read →
 *   ranged reopen 404s → fatal).
 * - Any uncertainty (more bytes than the object holds, unsafe transcoding)
 *   → fail the job exactly like today. Never guess an offset.
 * - Bounded attempts: `job.cloudResumeAttempts` consecutive no-progress
 *   resumes → fail. Delivering ≥ RESUME_PROGRESS_RESET_BYTES after a resume
 *   resets the counter.
 * - A deliberate destroy from downstream (maxRecords abort, gunzip/parse
 *   error, pipeline teardown) ABORTS the source — it never triggers resume.
 *
 * @param {object} params
 * @param {any} params.storage Storage client (from the google-cloud storage SDK)
 * @param {string} params.bucketName
 * @param {string} params.filePath object path within the bucket
 * @param {string} params.gcsPath full gs:// path (for labels/events)
 * @param {object} params.readOpts options for createReadStream (decompress/validation/timeout)
 * @param {number} params.idleMs idle watchdog threshold (<=0 disables)
 * @param {any} params.job JobConfig (counters + cloudStreamCallback + resume options)
 * @param {number} [params.initialOffset] resume-from byte offset (forward-compat for caller-persisted offsets; 0 = start of object)
 * @returns {Promise<stream.PassThrough>} byte-mode stream of the object's raw contents
 */
async function createResilientGCSSource({ storage, bucketName, filePath, gcsPath, readOpts, idleMs, job, initialOffset = 0 }) {
	const maxResumeAttempts = job.cloudResumeAttempts ?? 3;
	const backoffBaseMs = job.cloudRetryBackoffMs ?? 1000;

	// --- OPENING: one metadata probe (bounded retry) replaces exists() and
	// yields the generation to pin + the object size for offset sanity checks.
	let metadata;
	try {
		const [meta] = await retryCloudOp(() => storage.bucket(bucketName).file(filePath).getMetadata(), {
			baseMs: backoffBaseMs,
			label: gcsPath,
			onRetry: (err, attempt) => emitCloudEvent(job, { type: 'open-retry', file: gcsPath, byteOffset: 0, attempt, error: serializeError(err) })
		});
		metadata = meta;
	} catch (err) {
		if (classifyCloudError(err) === 'not-found') {
			const notFound = new Error(`File not found: ${gcsPath}`);
			// @ts-ignore
			notFound.code = 'ENOENT';
			throw notFound;
		}
		throw err;
	}

	const generation = metadata?.generation;
	const objectSize = Number(metadata?.size);
	const sizeKnown = Number.isFinite(objectSize);
	// GCS decompressive transcoding (objects stored with contentEncoding: gzip)
	// can ignore/transform range requests — stored-object offsets would not map
	// to received bytes. And without a generation to pin, a reopen could read a
	// different object version at the old offset — for plain JSONL there is no
	// integrity net (gzip's trailer CRC only covers .gz). Resume is unsafe in
	// both cases; stall stays fatal as today.
	const rangeSafe = metadata?.contentEncoding !== 'gzip' && Boolean(generation);
	const pinnedFile = generation
		? storage.bucket(bucketName).file(filePath, { generation })
		: storage.bucket(bucketName).file(filePath);

	const out = new stream.PassThrough({ highWaterMark: 2 ** 20 }); // 1MB byte-mode buffer

	// --- state machine: STREAMING → (RECONNECTING ↔ STREAMING) → ENDED | FAILED | ABORTED
	let state = 'streaming';
	let raw = null; // current underlying GCS read stream attempt
	let bytesEmitted = initialOffset; // raw bytes counted-then-written downstream, exactly once each
	let attemptIndex = 0; // total reopen count for this file (event `attempt` field)
	let attemptsSinceProgress = 0; // consecutive resumes without RESUME_PROGRESS_RESET_BYTES of progress
	let progressBytes = 0; // bytes delivered since the last stall/reset
	let idleTimer = null;
	let backoffTimer = null;
	let pausedForBackpressure = false;

	const stopWatchdog = () => { clearTimeout(idleTimer); idleTimer = null; };
	const kickWatchdog = () => {
		if (!idleMs || idleMs <= 0) return;
		clearTimeout(idleTimer);
		idleTimer = setTimeout(() => {
			const err = new Error(`cloud read stalled: no data for ${idleMs}ms (${gcsPath})`);
			// @ts-ignore
			err.code = 'ESTALL';
			if (raw && !raw.destroyed) raw.destroy(err); // surfaces via raw 'error' → handleRawError
			else handleRawError(err);
		}, idleMs);
	};

	const teardownRaw = () => {
		stopWatchdog();
		clearTimeout(backoffTimer);
		backoffTimer = null;
		if (raw && !raw.destroyed) {
			raw.removeAllListeners('data');
			raw.removeAllListeners('end');
			raw.removeAllListeners('error');
			// the aborted transport can still surface an async error (socket
			// hang-up on abort) — sink it or Node crashes on unhandled 'error'
			raw.on('error', () => { });
			raw.destroy();
		}
		raw = null;
	};

	const fail = (err) => {
		if (state !== 'streaming') return;
		state = 'failed';
		teardownRaw();
		if (!out.destroyed) out.destroy(err);
	};

	const finish = () => {
		if (state !== 'streaming') return;
		state = 'ended';
		teardownRaw();
		out.end();
	};

	const handleRawError = (err) => {
		if (state !== 'streaming') return;
		const isStall = err?.code === 'ESTALL';
		if (isStall) {
			job.stallsDetected++;
			emitCloudEvent(job, { type: 'stall', file: gcsPath, byteOffset: bytesEmitted, attempt: attemptIndex });
		}

		// All raw bytes arrived but the server never FIN'd — the object is
		// complete; end cleanly (for gzip, the trailer CRC is the integrity net).
		if (sizeKnown && bytesEmitted === objectSize) return finish();

		if (!job.resumeOnStall || !rangeSafe) return fail(err);
		if (classifyCloudError(err) !== 'retriable') return fail(err);
		// More bytes than the object holds = offset bookkeeping is wrong.
		// Never resume on uncertainty — a clean whole-job retry beats corruption.
		if (sizeKnown && bytesEmitted > objectSize) return fail(err);
		if (attemptsSinceProgress >= maxResumeAttempts) {
			emitCloudEvent(job, { type: 'resume-fail', file: gcsPath, byteOffset: bytesEmitted, attempt: attemptIndex, error: serializeError(err) });
			return fail(err);
		}

		// RECONNECTING: backoff, then reopen at the last-received offset.
		attemptsSinceProgress++;
		attemptIndex++;
		progressBytes = 0;
		job.resumesAttempted++;
		emitCloudEvent(job, { type: 'resume-attempt', file: gcsPath, byteOffset: bytesEmitted, attempt: attemptIndex, error: serializeError(err) });
		teardownRaw();
		const delay = backoffBaseMs * 2 ** (attemptsSinceProgress - 1);
		backoffTimer = setTimeout(() => {
			if (state === 'streaming') openAttempt(bytesEmitted);
		}, delay);
	};

	const openAttempt = (offset) => {
		const isResume = attemptIndex > 0;
		const opts = { ...readOpts };
		// Omit `start` at offset 0 so the first attempt is byte-identical to the
		// non-resilient path (ranged reads skip the GCS client's checksum validation).
		if (offset > 0) opts.start = offset;
		let sawData = false;

		try {
			raw = pinnedFile.createReadStream(opts);
		} catch (err) {
			return handleRawError(err);
		}

		raw.on('data', (chunk) => {
			if (state !== 'streaming') return;
			if (isResume && !sawData) {
				sawData = true;
				job.resumesSucceeded++;
				emitCloudEvent(job, { type: 'resume-success', file: gcsPath, byteOffset: offset, attempt: attemptIndex });
			}
			// count-then-write, synchronously: bytesEmitted always equals bytes
			// handed downstream, so a reopen offset is exact by construction.
			bytesEmitted += chunk.length;
			if (isResume) job.bytesResumed += chunk.length;
			if (attemptsSinceProgress > 0) {
				progressBytes += chunk.length;
				if (progressBytes >= RESUME_PROGRESS_RESET_BYTES) attemptsSinceProgress = 0;
			}
			kickWatchdog();
			if (!out.write(chunk)) {
				// Backpressure pause is not a stall: park the watchdog until drain,
				// otherwise a slow sink would burn resume attempts for nothing.
				pausedForBackpressure = true;
				stopWatchdog();
				raw.pause();
			}
		});

		raw.once('end', () => {
			if (state !== 'streaming') return;
			if (sizeKnown && bytesEmitted < objectSize) {
				// Clean FIN before all bytes arrived — a silently-truncated read.
				// Retriable: resume picks up at the received offset.
				const err = new Error(`cloud read ended early: received ${bytesEmitted} of ${objectSize} bytes (${gcsPath})`);
				// @ts-ignore
				err.code = 'EEARLYEOF';
				return handleRawError(err);
			}
			finish();
		});

		raw.once('error', handleRawError);
		kickWatchdog();
	};

	out.on('drain', () => {
		if (pausedForBackpressure && state === 'streaming' && raw && !raw.destroyed) {
			pausedForBackpressure = false;
			raw.resume();
			kickWatchdog();
		}
	});

	// Deliberate destroy from downstream (pipeline teardown, maxRecords abort,
	// gunzip/parse error) → ABORT: kill the transport, never attempt resume.
	const origDestroy = out._destroy.bind(out);
	out._destroy = (err, cb) => {
		if (state === 'streaming') {
			state = err ? 'failed' : 'aborted';
			teardownRaw();
		}
		origDestroy(err, cb);
	};

	openAttempt(bytesEmitted);
	return out;
}

module.exports = {
	classifyCloudError,
	retryCloudOp,
	armIdleTimeout,
	emitCloudEvent,
	createResilientGCSSource,
	RESUME_PROGRESS_RESET_BYTES
};
