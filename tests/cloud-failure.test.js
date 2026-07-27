// @ts-nocheck
/* eslint-disable no-undef */
/*
 * Mocked failure-mode tests for cloud (GCS/S3) source readers.
 *
 * These DO NOT hit real buckets — @google-cloud/storage and @aws-sdk/client-s3
 * are mocked so we can deterministically simulate mid-stream errors, stalled
 * (half-open) reads, and multi-file behavior. They guard the v3.4.0 fix that
 * makes cloud reads fail fast + propagate instead of hanging forever.
 *
 * All imports run with { dryRun: true } so the pipeline fully consumes the
 * source (errors propagate) but never makes a real Mixpanel HTTP request.
 */

const { Readable } = require("stream");
const zlib = require("zlib");

// ──────────────────────────────────────────────────────────────────────────
// Mock @google-cloud/storage. The factory may only reference `mock`-prefixed
// outer vars (jest hoisting rule), so per-test behavior is injected via mockGcs.
// ──────────────────────────────────────────────────────────────────────────
const mockGcs = {
	exists: null,            // (filePath) => Promise<[boolean]>
	getMetadata: null,       // (filePath) => Promise<[{generation, size, contentEncoding?}]>
	createReadStream: null,  // (filePath, opts) => Readable
	opened: [],              // filePaths for which createReadStream was called
	openCalls: [],           // { filePath, opts, generation } per createReadStream call
	streams: [],             // the Readables we handed back (to assert destroyed)
};

jest.mock("@google-cloud/storage", () => ({
	Storage: jest.fn().mockImplementation(() => ({
		bucket: () => ({
			file: (filePath, fileOpts) => ({
				exists: async () => mockGcs.exists(filePath),
				getMetadata: async () => mockGcs.getMetadata(filePath),
				createReadStream: (opts) => {
					mockGcs.opened.push(filePath);
					mockGcs.openCalls.push({ filePath, opts, generation: fileOpts && fileOpts.generation });
					const s = mockGcs.createReadStream(filePath, opts);
					mockGcs.streams.push(s);
					return s;
				},
			}),
		}),
	})),
}));

// ──────────────────────────────────────────────────────────────────────────
// Mock @aws-sdk/client-s3. send() delegates to a per-test impl.
// ──────────────────────────────────────────────────────────────────────────
const mockS3 = { impl: null };

jest.mock("@aws-sdk/client-s3", () => ({
	S3Client: jest.fn().mockImplementation(() => ({
		send: (...args) => mockS3.impl(...args),
	})),
	GetObjectCommand: jest.fn().mockImplementation((input) => ({ input })),
	PutObjectCommand: jest.fn(),
	DeleteObjectCommand: jest.fn(),
}));

// Import AFTER mocks are registered.
const mp = require("../index.js");

const CREDS = { token: "test-token", secret: "test-secret", project: "1234" };
const BASE_OPTS = {
	recordType: "event",
	dryRun: true,
	verbose: false,
	abortOnError: false,
};

// ── stream factories ───────────────────────────────────────────────────────

/** Readable that emits `bytesBefore` bytes then errors (never ends). */
function makeErroringStream(bytesBefore = 0, errMsg = "read ECONNRESET") {
	const s = new Readable({ read() {} });
	process.nextTick(() => {
		if (bytesBefore > 0) s.push(Buffer.from("x".repeat(bytesBefore)));
		process.nextTick(() =>
			s.destroy(Object.assign(new Error(errMsg), { code: "ECONNRESET" }))
		);
	});
	return s;
}

/** Readable that emits one line then goes permanently silent (no end/error). */
function makeStallStream(line = '{"event":"ping","properties":{"time":1700000000000}}\n') {
	const s = new Readable({ read() {} });
	process.nextTick(() => s.push(Buffer.from(line)));
	return s; // intentionally never pushes null / never errors
}

/** Readable that emits valid NDJSON records then ends cleanly. */
function makeGoodStream(n = 2) {
	const s = new Readable({ read() {} });
	process.nextTick(() => {
		for (let i = 0; i < n; i++) {
			const rec = { event: "test", properties: { time: 1700000000000, distinct_id: `u${i}` } };
			s.push(Buffer.from(JSON.stringify(rec) + "\n"));
		}
		s.push(null);
	});
	return s;
}

/** Async-iterable body that yields bytes then throws (for S3 response.Body). */
async function* erroringBody(bytesBefore = 0, errMsg = "read ECONNRESET") {
	if (bytesBefore > 0) yield Buffer.from("x".repeat(bytesBefore));
	throw Object.assign(new Error(errMsg), { code: "ECONNRESET" });
}

/** Async-iterable body that yields valid NDJSON then completes. */
async function* goodBody(n = 2) {
	for (let i = 0; i < n; i++) {
		const rec = { event: "test", properties: { time: 1700000000000, distinct_id: `u${i}` } };
		yield Buffer.from(JSON.stringify(rec) + "\n");
	}
}

/** NDJSON fixture of n distinct records; returns { buf, n } */
function ndjsonFixture(n = 20, prefix = "evt") {
	let out = "";
	for (let i = 0; i < n; i++) {
		out += JSON.stringify({ event: `${prefix}_${i}`, properties: { time: 1700000000000 + i, distinct_id: `u${i}` } }) + "\n";
	}
	return { buf: Buffer.from(out), n };
}

/** Readable that emits buf.slice(0, k) then goes permanently silent. */
function makeStallAtByteStream(buf, k) {
	const s = new Readable({ read() { } });
	process.nextTick(() => s.push(buf.subarray(0, k)));
	return s; // never ends, never errors — the idle watchdog must fire
}

/** Readable that emits buf.slice(start) then ends cleanly (a range read). */
function makeRangeStream(buf, start = 0) {
	const s = new Readable({ read() { } });
	process.nextTick(() => {
		s.push(buf.subarray(start));
		s.push(null);
	});
	return s;
}

/** Readable that never emits anything (dead reopen). */
function makeSilentStream() {
	return new Readable({ read() { } });
}

beforeEach(() => {
	mockGcs.exists = async () => [true];
	mockGcs.getMetadata = async () => [{ generation: "1" }]; // size intentionally unknown by default
	mockGcs.createReadStream = () => makeGoodStream();
	mockGcs.opened = [];
	mockGcs.openCalls = [];
	mockGcs.streams = [];
	mockS3.impl = async () => ({ Body: goodBody() });
});

// ════════════════════════════════════════════════════════════════════════════
// GCS
// ════════════════════════════════════════════════════════════════════════════
describe("GCS source failures", () => {
	test("mid-stream error rejects (does not hang) and names the file", async () => {
		const path = "gs://bucket/part-0001.jsonl";
		mockGcs.createReadStream = () => makeErroringStream(8);

		await expect(mp(CREDS, path, BASE_OPTS)).rejects.toThrow(/ECONNRESET/);
	});

	test("on failure, the source stream is destroyed (no fd/socket leak)", async () => {
		const path = "gs://bucket/part-0001.jsonl";
		mockGcs.createReadStream = () => makeErroringStream(8);

		await expect(mp(CREDS, path, BASE_OPTS)).rejects.toThrow();
		expect(mockGcs.streams.length).toBeGreaterThan(0);
		for (const s of mockGcs.streams) expect(s.destroyed).toBe(true);
	});

	test("stalled (half-open) read rejects via idle watchdog", async () => {
		const path = "gs://bucket/part-0001.jsonl";
		mockGcs.createReadStream = () => makeStallStream();

		await expect(
			mp(CREDS, path, { ...BASE_OPTS, cloudReadIdleTimeout: 400 })
		).rejects.toThrow(/stalled/);
	});

	test("happy path single file resolves", async () => {
		const path = "gs://bucket/part-0001.jsonl";
		const res = await mp(CREDS, path, BASE_OPTS);
		expect(res).toBeDefined();
		expect(res.failed).toBe(0);
	});

	test("multi-file: a mid-stream read error fails the job and stops opening later files", async () => {
		const paths = Array.from({ length: 10 }, (_, i) => `gs://bucket/part-${String(i).padStart(4, "0")}.jsonl`);
		const FAIL_AT = 3; // 0-indexed → 4th file
		mockGcs.createReadStream = (filePath) => {
			const idx = paths.findIndex((p) => p.endsWith(filePath));
			return idx === FAIL_AT ? makeErroringStream(8) : makeGoodStream();
		};

		await expect(mp(CREDS, paths, BASE_OPTS)).rejects.toThrow(/Multi-file GCS read failed/);

		// files after the failing one must never be opened (sequential + fail-fast)
		const openedSuffixes = mockGcs.opened;
		for (let i = FAIL_AT + 1; i < paths.length; i++) {
			const suffix = `part-${String(i).padStart(4, "0")}.jsonl`;
			expect(openedSuffixes).not.toContain(suffix);
		}
	});

	test("multi-file: fatal error preserves the original error code (so callers can retry)", async () => {
		const paths = ["gs://bucket/part-0000.jsonl", "gs://bucket/part-0001.jsonl"];
		mockGcs.createReadStream = () => makeErroringStream(8);

		await expect(mp(CREDS, paths, BASE_OPTS)).rejects.toMatchObject({
			message: expect.stringMatching(/Multi-file GCS read failed/),
			code: "ECONNRESET",
		});
	});

	test("multi-file: a genuinely-absent file is skipped (not fatal) and counted", async () => {
		const paths = [
			"gs://bucket/part-0000.jsonl",
			"gs://bucket/part-0001.jsonl", // will be 'missing'
			"gs://bucket/part-0002.jsonl",
		];
		mockGcs.exists = async (filePath) => [!filePath.endsWith("part-0001.jsonl")];
		const events = [];

		const res = await mp(CREDS, paths, { ...BASE_OPTS, cloudStreamCallback: (e) => events.push(e) });
		expect(res).toBeDefined();
		expect(res.failed).toBe(0);
		// the missing file must never be opened for reading
		expect(mockGcs.opened).not.toContain("part-0001.jsonl");
		// R1: skip is surfaced in results + event hook, not silent
		expect(res.filesSkippedMissing).toBe(1);
		const skips = events.filter((e) => e.type === "file-skip-missing");
		expect(skips.length).toBe(1);
		expect(skips[0].file).toBe("gs://bucket/part-0001.jsonl");
	});
});

// ════════════════════════════════════════════════════════════════════════════
// S3 (parity)
// ════════════════════════════════════════════════════════════════════════════
describe("S3 source failures", () => {
	const S3_OPTS = { ...BASE_OPTS, s3Region: "us-east-1" };

	test("mid-stream error rejects (does not hang)", async () => {
		mockS3.impl = async () => ({ Body: erroringBody(8) });
		await expect(mp(CREDS, "s3://bucket/part-0001.jsonl", S3_OPTS)).rejects.toThrow(/ECONNRESET/);
	});

	test("happy path single file resolves", async () => {
		mockS3.impl = async () => ({ Body: goodBody() });
		const res = await mp(CREDS, "s3://bucket/part-0001.jsonl", S3_OPTS);
		expect(res).toBeDefined();
		expect(res.failed).toBe(0);
	});

	test("multi-file: fatal error preserves the original error code", async () => {
		const paths = ["s3://bucket/part-0.jsonl", "s3://bucket/part-1.jsonl"];
		const counts = {};
		mockS3.impl = async (command) => {
			const key = command.input.Key;
			counts[key] = (counts[key] || 0) + 1;
			if (counts[key] === 1) return { Body: goodBody(0) }; // existence probe
			return { Body: erroringBody(8) };
		};

		await expect(mp(CREDS, paths, S3_OPTS)).rejects.toMatchObject({
			message: expect.stringMatching(/Multi-file S3 read failed/),
			code: "ECONNRESET",
		});
	});

	test("multi-file: a mid-stream read error fails the job", async () => {
		const paths = Array.from({ length: 5 }, (_, i) => `s3://bucket/part-${i}.jsonl`);
		const FAIL_AT = 2;
		// createMultiS3Stream calls send() once per file for existence, then
		// createS3Stream calls send() again to read. Track per-key call count.
		const counts = {};
		mockS3.impl = async (command) => {
			const key = command.input.Key;
			counts[key] = (counts[key] || 0) + 1;
			if (counts[key] === 1) return { Body: goodBody(0) }; // existence probe
			const idx = paths.findIndex((p) => p.endsWith(key));
			return { Body: idx === FAIL_AT ? erroringBody(8) : goodBody() };
		};

		await expect(mp(CREDS, paths, S3_OPTS)).rejects.toThrow(/Multi-file S3 read failed/);
	});
});

// ════════════════════════════════════════════════════════════════════════════
// GCS creation-time retry (R1, v3.5.0): never silently skip a file
// ════════════════════════════════════════════════════════════════════════════
describe("GCS creation retry (R1)", () => {
	test("transient probe failure is retried and recovers (open-retry event)", async () => {
		let calls = 0;
		mockGcs.exists = async () => {
			if (++calls === 1) throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
			return [true];
		};
		const events = [];

		const res = await mp(CREDS, "gs://bucket/part-0001.jsonl", {
			...BASE_OPTS,
			cloudRetryBackoffMs: 10,
			cloudStreamCallback: (e) => events.push(e),
		});
		expect(res.failed).toBe(0);
		expect(calls).toBe(2);
		const retries = events.filter((e) => e.type === "open-retry");
		expect(retries.length).toBe(1);
		expect(retries[0].error.code).toBe("ECONNRESET");
	});

	test("persistent probe failure rejects loudly after bounded retries, code preserved", async () => {
		let calls = 0;
		mockGcs.exists = async () => {
			calls++;
			throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
		};

		await expect(
			mp(CREDS, "gs://bucket/part-0001.jsonl", { ...BASE_OPTS, cloudRetryBackoffMs: 10 })
		).rejects.toMatchObject({ code: "ECONNRESET" });
		expect(calls).toBe(3); // bounded: exactly `attempts` probes, then loud failure
	});

	test("multi-file: persistent transient open failure is FATAL, not a silent skip", async () => {
		const paths = ["gs://bucket/part-0000.jsonl", "gs://bucket/part-0001.jsonl"];
		mockGcs.exists = async () => {
			throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
		};

		await expect(mp(CREDS, paths, { ...BASE_OPTS, cloudRetryBackoffMs: 10 })).rejects.toMatchObject({
			message: expect.stringMatching(/Multi-file GCS read failed/),
			code: "ECONNRESET",
		});
		// nothing was ever opened for reading — and nothing was skipped-as-success
		expect(mockGcs.opened.length).toBe(0);
	});
});

// ════════════════════════════════════════════════════════════════════════════
// 1-element array routing (R3, v3.5.0): ["gs://…"] ≡ "gs://…"
// ════════════════════════════════════════════════════════════════════════════
describe("GCS 1-element array routing", () => {
	test("1-element array produces identical results to a single string path", async () => {
		const single = await mp(CREDS, "gs://bucket/part-0001.jsonl", BASE_OPTS);
		const singleOpens = [...mockGcs.opened];

		mockGcs.opened = [];
		mockGcs.openCalls = [];
		const arr = await mp(CREDS, ["gs://bucket/part-0001.jsonl"], BASE_OPTS);

		expect(arr.total).toBe(single.total);
		expect(arr.failed).toBe(single.failed);
		expect(arr.filesSkippedMissing).toBe(0);
		// exactly one open, same as the string form (no multi-file wrapper overhead)
		expect(mockGcs.opened).toEqual(singleOpens);
	});

	test("missing file in a 1-element array rejects (single-file semantics)", async () => {
		mockGcs.exists = async () => [false];
		await expect(mp(CREDS, ["gs://bucket/nope.jsonl"], BASE_OPTS)).rejects.toThrow(/File not found/);
	});
});

// ════════════════════════════════════════════════════════════════════════════
// GCS stall resume (R2, v3.5.0): flag-gated range-read reconnects
// ════════════════════════════════════════════════════════════════════════════
describe("GCS stall resume (resumeOnStall)", () => {
	const RESUME_OPTS = {
		...BASE_OPTS,
		resumeOnStall: true,
		cloudReadIdleTimeout: 250,
		cloudRetryBackoffMs: 10,
	};

	test("gzip JSONL killed mid-transfer resumes at exact compressed offset — zero dup, zero missing", async () => {
		const { buf, n } = ndjsonFixture(50);
		const gzBuf = zlib.gzipSync(buf);
		const k = Math.floor(gzBuf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: gzBuf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start ? makeRangeStream(gzBuf, opts.start) : makeStallAtByteStream(gzBuf, k);
		const events = [];

		const res = await mp(CREDS, "gs://bucket/data.jsonl.gz", {
			...RESUME_OPTS,
			cloudStreamCallback: (e) => events.push(e),
		});

		// zero duplicates / zero missing: every distinct record exactly once
		expect(res.total).toBe(n);
		expect(res.dryRun.length).toBe(n);
		expect(new Set(res.dryRun.map((r) => r.event)).size).toBe(n);
		expect(res.failed).toBe(0);

		// exact range reopen against the pinned generation
		expect(mockGcs.openCalls.length).toBe(2);
		expect(mockGcs.openCalls[0].opts.start).toBeUndefined(); // first attempt byte-identical to today
		expect(mockGcs.openCalls[0].generation).toBe("g1");
		expect(mockGcs.openCalls[1].opts.start).toBe(k);
		expect(mockGcs.openCalls[1].generation).toBe("g1");

		// counters (R4)
		expect(res.stallsDetected).toBe(1);
		expect(res.resumesAttempted).toBe(1);
		expect(res.resumesSucceeded).toBe(1);
		expect(res.bytesResumed).toBe(gzBuf.length - k);
		expect(res.filesSkippedMissing).toBe(0);

		// event hook (R4): serializable, in order
		expect(events.map((e) => e.type)).toEqual(["stall", "resume-attempt", "resume-success"]);
		expect(events[0].byteOffset).toBe(k);
		expect(events[1].byteOffset).toBe(k);
		expect(() => JSON.stringify(events)).not.toThrow();
	});

	test("plain JSONL stalled mid-record resumes without corruption", async () => {
		const { buf, n } = ndjsonFixture(30);
		// cut in the MIDDLE of a record, not at a line boundary
		const k = buf.indexOf(0x0a, Math.floor(buf.length / 2)) + 5;
		mockGcs.getMetadata = async () => [{ generation: "g7", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start ? makeRangeStream(buf, opts.start) : makeStallAtByteStream(buf, k);

		const res = await mp(CREDS, "gs://bucket/data.jsonl", RESUME_OPTS);
		expect(res.total).toBe(n);
		expect(res.dryRun.length).toBe(n);
		expect(new Set(res.dryRun.map((r) => r.event)).size).toBe(n);
		expect(mockGcs.openCalls[1].opts.start).toBe(k);
		expect(res.resumesSucceeded).toBe(1);
	});

	test("resume works under BufferQueue throttling (throttleMemory: true)", async () => {
		const { buf, n } = ndjsonFixture(50);
		const gzBuf = zlib.gzipSync(buf);
		const k = Math.floor(gzBuf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: gzBuf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start ? makeRangeStream(gzBuf, opts.start) : makeStallAtByteStream(gzBuf, k);

		const res = await mp(CREDS, "gs://bucket/data.jsonl.gz", { ...RESUME_OPTS, throttleMemory: true });
		expect(res.total).toBe(n);
		expect(res.resumesSucceeded).toBe(1);
	});

	test("clean EOF before all bytes (truncated read) is treated as retriable and resumed", async () => {
		const { buf, n } = ndjsonFixture(30);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start
				? makeRangeStream(buf, opts.start)
				: makeRangeStream(buf.subarray(0, k)); // partial bytes then clean 'end'

		const res = await mp(CREDS, "gs://bucket/data.jsonl", RESUME_OPTS);
		expect(res.total).toBe(n);
		expect(res.stallsDetected).toBe(0); // early EOF is not a stall
		expect(res.resumesAttempted).toBe(1);
		expect(res.resumesSucceeded).toBe(1);
		expect(mockGcs.openCalls[1].opts.start).toBe(k);
	});

	test("all reopens stall → fails after cloudResumeAttempts consecutive no-progress attempts", async () => {
		const { buf } = ndjsonFixture(20);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start ? makeSilentStream() : makeStallAtByteStream(buf, k);
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl", {
				...RESUME_OPTS,
				cloudReadIdleTimeout: 150,
				cloudStreamCallback: (e) => events.push(e),
			})
		).rejects.toThrow(/stalled/);

		// job rejected, so counters come from the event hook
		expect(events.filter((e) => e.type === "stall").length).toBe(4); // initial + 3 dead reopens
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(3);
		expect(events.filter((e) => e.type === "resume-success").length).toBe(0);
		expect(events.filter((e) => e.type === "resume-fail").length).toBe(1);
		// every reopen targeted the same exact offset — no progress, no drift
		for (const call of mockGcs.openCalls.slice(1)) expect(call.opts.start).toBe(k);
	});

	test("object overwritten mid-read (ranged reopen 404s) fails — never restarts from zero", async () => {
		const { buf } = ndjsonFixture(20);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) => {
			if (opts && opts.start) {
				const s = new Readable({ read() { } });
				process.nextTick(() =>
					s.destroy(Object.assign(new Error("No such object: bucket/data.jsonl#g1"), { code: 404 }))
				);
				return s;
			}
			return makeStallAtByteStream(buf, k);
		};
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl", { ...RESUME_OPTS, cloudStreamCallback: (e) => events.push(e) })
		).rejects.toThrow(/No such object/);

		expect(events.filter((e) => e.type === "resume-success").length).toBe(0);
		expect(mockGcs.openCalls.length).toBe(2); // one reopen, then fatal — no restart at offset 0
		expect(mockGcs.openCalls[1].generation).toBe("g1"); // reopen was pinned to the original generation
	});

	test("corrupt gzip rejects with a decompression error and never attempts resume", async () => {
		const { buf } = ndjsonFixture(20);
		const gzBuf = zlib.gzipSync(buf);
		const mid = Math.floor(gzBuf.length / 2);
		for (let i = 0; i < 8; i++) gzBuf[mid + i] ^= 0xff; // corrupt the deflate stream, not header/trailer
		mockGcs.getMetadata = async () => [{ generation: "g1", size: gzBuf.length }];
		mockGcs.createReadStream = () => makeRangeStream(gzBuf, 0);
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl.gz", { ...RESUME_OPTS, cloudStreamCallback: (e) => events.push(e) })
		).rejects.toThrow();

		// downstream parse/decompress errors ABORT the source — they must not reconnect
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(0);
		expect(events.filter((e) => e.type === "stall").length).toBe(0);
	});

	test("stall AFTER all bytes received (missing FIN) ends cleanly, no resume", async () => {
		const { buf, n } = ndjsonFixture(20);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = () => makeStallAtByteStream(buf, buf.length); // all bytes, never 'end'

		const res = await mp(CREDS, "gs://bucket/data.jsonl", RESUME_OPTS);
		expect(res.total).toBe(n);
		expect(res.failed).toBe(0);
		expect(res.stallsDetected).toBe(1);
		expect(res.resumesAttempted).toBe(0);
	});

	test("resume is DISABLED for objects with contentEncoding: gzip (decompressive transcoding)", async () => {
		const { buf } = ndjsonFixture(20);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length, contentEncoding: "gzip" }];
		mockGcs.createReadStream = () => makeStallAtByteStream(buf, k);

		// stored-object offsets don't map to received bytes → stall stays fatal
		await expect(mp(CREDS, "gs://bucket/data.jsonl", RESUME_OPTS)).rejects.toThrow(/stalled/);
		expect(mockGcs.openCalls.length).toBe(1); // no reopen was ever attempted
	});

	test("default (resumeOnStall off) keeps today's behavior: stall rejects", async () => {
		mockGcs.createReadStream = () => makeStallStream();
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl", {
				...BASE_OPTS,
				cloudReadIdleTimeout: 300,
				cloudStreamCallback: (e) => events.push(e),
			})
		).rejects.toThrow(/stalled/);
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(0);
	});

	test("clean run reports zeroed resilience counters (full and abridged summaries)", async () => {
		const res = await mp(CREDS, "gs://bucket/part-0001.jsonl", RESUME_OPTS);
		expect(res.stallsDetected).toBe(0);
		expect(res.resumesAttempted).toBe(0);
		expect(res.resumesSucceeded).toBe(0);
		expect(res.filesSkippedMissing).toBe(0);
		expect(res.bytesResumed).toBe(0);

		const abridged = await mp(CREDS, "gs://bucket/part-0001.jsonl", { ...RESUME_OPTS, abridged: true });
		expect(abridged.stallsDetected).toBe(0);
		expect(abridged.resumesAttempted).toBe(0);
		expect(abridged.resumesSucceeded).toBe(0);
		expect(abridged.filesSkippedMissing).toBe(0);
		expect(abridged.bytesResumed).toBe(0);
	});
});

// ════════════════════════════════════════════════════════════════════════════
// Resume hardening (v3.5.0 adversarial-review regressions)
// ════════════════════════════════════════════════════════════════════════════
describe("GCS resume hardening (review regressions)", () => {
	const RESUME_OPTS = {
		...BASE_OPTS,
		resumeOnStall: true,
		cloudReadIdleTimeout: 250,
		cloudRetryBackoffMs: 10,
	};

	test("more bytes delivered than the object holds → fatal, never a resume (offset uncertainty)", async () => {
		const { buf } = ndjsonFixture(20);
		// metadata claims the object is HALF the size of what actually arrived
		mockGcs.getMetadata = async () => [{ generation: "g1", size: Math.floor(buf.length / 2) }];
		mockGcs.createReadStream = () => makeStallAtByteStream(buf, buf.length);
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl", { ...RESUME_OPTS, cloudStreamCallback: (e) => events.push(e) })
		).rejects.toThrow(/stalled/);

		// bytesEmitted > objectSize means our accounting can't be trusted — never guess an offset
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(0);
		expect(mockGcs.openCalls.length).toBe(1);
	});

	test("metadata without a generation disables resume (no integrity net for plain JSONL)", async () => {
		const { buf } = ndjsonFixture(20);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ size: buf.length }]; // no generation
		mockGcs.createReadStream = () => makeStallAtByteStream(buf, k);

		await expect(mp(CREDS, "gs://bucket/data.jsonl", RESUME_OPTS)).rejects.toThrow(/stalled/);
		expect(mockGcs.openCalls.length).toBe(1); // unpinned resume is never attempted
	});

	test("numeric 404 from getMetadata classifies as not-found (rejects File not found, not retry loop)", async () => {
		let calls = 0;
		mockGcs.getMetadata = async () => {
			calls++;
			throw Object.assign(new Error("No such object: bucket/nope.jsonl"), { code: 404 });
		};

		await expect(mp(CREDS, "gs://bucket/nope.jsonl", RESUME_OPTS)).rejects.toThrow(/File not found/);
		expect(calls).toBe(1); // not-found is rethrown immediately, never retried
	});

	test("resume budget resets after ≥10MB of progress (choppy links complete big files)", async () => {
		const MB = 1024 * 1024;
		// ~36MB of JSONL: 36 records with ~1MB payloads (few records, big bytes)
		let text = "";
		for (let i = 0; i < 36; i++) {
			text += JSON.stringify({
				event: `big_${i}`,
				properties: { time: 1700000000000 + i, distinct_id: `u${i}`, pad: "x".repeat(MB) },
			}) + "\n";
		}
		const buf = Buffer.from(text);
		// three stalls, each after ~10.8MB of fresh progress (> RESUME_PROGRESS_RESET_BYTES)
		const stallPoints = [Math.floor(buf.length * 0.3), Math.floor(buf.length * 0.6), Math.floor(buf.length * 0.9)];
		let open = 0;
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) => {
			const start = (opts && opts.start) || 0;
			const stallAt = stallPoints[open++];
			if (stallAt === undefined) return makeRangeStream(buf, start); // final open completes
			const s = new Readable({ read() { } });
			process.nextTick(() => s.push(buf.subarray(start, stallAt)));
			return s; // silent after its segment — watchdog fires
		};

		// cloudResumeAttempts=2 but 3 resumes are needed — only possible because
		// each resumed segment delivers ≥10MB, resetting the no-progress counter
		const res = await mp(CREDS, "gs://bucket/big.jsonl", { ...RESUME_OPTS, cloudResumeAttempts: 2 });
		expect(res.total).toBe(36);
		expect(res.stallsDetected).toBe(3);
		expect(res.resumesAttempted).toBe(3);
		expect(res.resumesSucceeded).toBe(3);
	});

	test("cloudResumeAttempts: 0 is honored (not coerced to the default)", async () => {
		const { buf } = ndjsonFixture(20);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = () => makeStallAtByteStream(buf, k);
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl", {
				...RESUME_OPTS,
				cloudResumeAttempts: 0,
				cloudStreamCallback: (e) => events.push(e),
			})
		).rejects.toThrow(/stalled/);
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(0);
		expect(mockGcs.openCalls.length).toBe(1);
	});

	test("a throwing cloudStreamCallback never breaks the pipeline", async () => {
		const { buf, n } = ndjsonFixture(30);
		const k = Math.floor(buf.length / 2);
		mockGcs.getMetadata = async () => [{ generation: "g1", size: buf.length }];
		mockGcs.createReadStream = (filePath, opts) =>
			opts && opts.start ? makeRangeStream(buf, opts.start) : makeStallAtByteStream(buf, k);

		const res = await mp(CREDS, "gs://bucket/data.jsonl", {
			...RESUME_OPTS,
			cloudStreamCallback: () => {
				throw new Error("user callback exploded");
			},
		});
		expect(res.total).toBe(n);
		expect(res.resumesSucceeded).toBe(1);
	});

	test("late transport error after teardown does not crash the process (error sink)", async () => {
		const { buf } = ndjsonFixture(20);
		const gzBuf = zlib.gzipSync(buf);
		const mid = Math.floor(gzBuf.length / 2);
		for (let i = 0; i < 8; i++) gzBuf[mid + i] ^= 0xff; // corrupt → gunzip fails downstream
		mockGcs.getMetadata = async () => [{ generation: "g1", size: gzBuf.length }];
		mockGcs.createReadStream = () => {
			const s = new Readable({
				read() { },
				destroy(err, cb) {
					// real transports can surface one more async error after abort
					setImmediate(() =>
						s.emit("error", Object.assign(new Error("socket hang up"), { code: "ECONNRESET" }))
					);
					cb(err);
				},
			});
			process.nextTick(() => s.push(gzBuf)); // all bytes, never 'end' → stays live until torn down
			return s;
		};

		await expect(mp(CREDS, "gs://bucket/data.jsonl.gz", RESUME_OPTS)).rejects.toThrow();
		// let the late error land — without the teardown sink this crashes the worker
		await new Promise((r) => setTimeout(r, 50));
	});

	test("throttled mode: downstream error destroys the source (no zombie download / resume loop)", async () => {
		const { buf } = ndjsonFixture(20);
		const gzBuf = zlib.gzipSync(buf);
		const quarter = Math.floor(gzBuf.length / 4);
		for (let i = 0; i < 8; i++) gzBuf[quarter + i] ^= 0xff; // corrupt EARLY in the deflate stream
		const k = Math.floor(gzBuf.length * 0.6); // transport delivers 60% then goes silent
		mockGcs.getMetadata = async () => [{ generation: "g1", size: gzBuf.length }];
		mockGcs.createReadStream = () => makeStallAtByteStream(gzBuf, k);
		const events = [];

		await expect(
			mp(CREDS, "gs://bucket/data.jsonl.gz", {
				...RESUME_OPTS,
				throttleMemory: true,
				cloudStreamCallback: (e) => events.push(e),
			})
		).rejects.toThrow();

		// BufferQueue never destroys its source; the close-hook must. Without it the
		// watchdog fires after the job already failed and starts a resume loop.
		await new Promise((r) => setTimeout(r, 600));
		expect(events.filter((e) => e.type === "resume-attempt").length).toBe(0);
		expect(events.filter((e) => e.type === "stall").length).toBe(0);
		for (const s of mockGcs.streams) expect(s.destroyed).toBe(true);
	});

	test("multi-file with resumeOnStall: stalls resume per file, missing files still counted", async () => {
		const f0 = ndjsonFixture(10, "a");
		const f2 = ndjsonFixture(10, "c");
		const k = Math.floor(f2.buf.length / 2);
		mockGcs.getMetadata = async (filePath) => {
			if (filePath.endsWith("part-1.jsonl"))
				throw Object.assign(new Error("No such object: bucket/part-1.jsonl"), { code: 404 });
			return [{ generation: "g1", size: filePath.endsWith("part-0.jsonl") ? f0.buf.length : f2.buf.length }];
		};
		mockGcs.createReadStream = (filePath, opts) => {
			if (filePath.endsWith("part-0.jsonl")) return makeRangeStream(f0.buf, (opts && opts.start) || 0);
			return opts && opts.start ? makeRangeStream(f2.buf, opts.start) : makeStallAtByteStream(f2.buf, k);
		};
		const events = [];

		const res = await mp(
			CREDS,
			["gs://bucket/part-0.jsonl", "gs://bucket/part-1.jsonl", "gs://bucket/part-2.jsonl"],
			{ ...RESUME_OPTS, cloudStreamCallback: (e) => events.push(e) }
		);

		expect(res.total).toBe(f0.n + f2.n);
		expect(new Set(res.dryRun.map((r) => r.event)).size).toBe(f0.n + f2.n); // zero dup across files
		expect(res.filesSkippedMissing).toBe(1);
		expect(res.resumesSucceeded).toBe(1);
		expect(events.filter((e) => e.type === "file-skip-missing").length).toBe(1);
	});
});

// ════════════════════════════════════════════════════════════════════════════
// S3 multi-file R1 parity (v3.5.0): retried probes, counted skips, loud failures
// ════════════════════════════════════════════════════════════════════════════
describe("S3 multi-file R1 parity", () => {
	const S3_OPTS = { ...BASE_OPTS, s3Region: "us-east-1", cloudRetryBackoffMs: 10 };

	test("happy path: files concatenate and the stream ends", async () => {
		const res = await mp(CREDS, ["s3://bucket/a.jsonl", "s3://bucket/b.jsonl"], S3_OPTS);
		expect(res.total).toBe(4); // 2 records per file via goodBody()
		expect(res.failed).toBe(0);
		expect(res.filesSkippedMissing).toBe(0);
	});

	test("missing file (NoSuchKey) is skipped, counted, and surfaced via the event hook", async () => {
		const counts = {};
		mockS3.impl = async (command) => {
			const key = command.input.Key;
			counts[key] = (counts[key] || 0) + 1;
			if (key === "gone.jsonl") {
				const err = new Error("The specified key does not exist.");
				err.name = "NoSuchKey";
				err.$metadata = { httpStatusCode: 404 };
				throw err;
			}
			if (counts[key] === 1) return { Body: goodBody(0) }; // existence probe
			return { Body: goodBody() };
		};
		const events = [];

		const res = await mp(
			CREDS,
			["s3://bucket/a.jsonl", "s3://bucket/gone.jsonl", "s3://bucket/b.jsonl"],
			{ ...S3_OPTS, cloudStreamCallback: (e) => events.push(e) }
		);

		expect(res.total).toBe(4); // both present files fully read
		expect(res.filesSkippedMissing).toBe(1);
		const skips = events.filter((e) => e.type === "file-skip-missing");
		expect(skips.length).toBe(1);
		expect(skips[0].file).toBe("s3://bucket/gone.jsonl");
	});

	test("transient probe failure is retried with backoff, then FATAL — never a silent skip", async () => {
		let probeCalls = 0;
		mockS3.impl = async (command) => {
			if (command.input.Key === "a.jsonl") {
				probeCalls++;
				throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
			}
			return { Body: goodBody() };
		};

		await expect(mp(CREDS, ["s3://bucket/a.jsonl", "s3://bucket/b.jsonl"], S3_OPTS)).rejects.toMatchObject({
			message: expect.stringMatching(/Multi-file S3 read failed/),
			code: "ECONNRESET",
		});
		expect(probeCalls).toBe(3); // bounded retries, then loud failure
	});

	test("transient probe failure that recovers emits open-retry and succeeds", async () => {
		const counts = {};
		mockS3.impl = async (command) => {
			const key = command.input.Key;
			counts[key] = (counts[key] || 0) + 1;
			if (key === "a.jsonl" && counts[key] === 1) {
				throw Object.assign(new Error("socket hang up"), { code: "ECONNRESET" });
			}
			// a.jsonl: call2=probe-retry-success, call3=read; b.jsonl: call1=probe, call2=read
			if ((key === "a.jsonl" && counts[key] === 2) || (key === "b.jsonl" && counts[key] === 1)) {
				return { Body: goodBody(0) };
			}
			return { Body: goodBody() };
		};
		const events = [];

		const res = await mp(CREDS, ["s3://bucket/a.jsonl", "s3://bucket/b.jsonl"], {
			...S3_OPTS,
			cloudStreamCallback: (e) => events.push(e),
		});
		expect(res.total).toBe(4);
		const retries = events.filter((e) => e.type === "open-retry");
		expect(retries.length).toBe(1);
		expect(retries[0].error.code).toBe("ECONNRESET");
	});
});
