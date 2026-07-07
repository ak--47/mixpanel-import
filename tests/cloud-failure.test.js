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

// ──────────────────────────────────────────────────────────────────────────
// Mock @google-cloud/storage. The factory may only reference `mock`-prefixed
// outer vars (jest hoisting rule), so per-test behavior is injected via mockGcs.
// ──────────────────────────────────────────────────────────────────────────
const mockGcs = {
	exists: null,            // (filePath) => Promise<[boolean]>
	createReadStream: null,  // (filePath) => Readable
	opened: [],              // filePaths for which createReadStream was called
	streams: [],             // the Readables we handed back (to assert destroyed)
};

jest.mock("@google-cloud/storage", () => ({
	Storage: jest.fn().mockImplementation(() => ({
		bucket: () => ({
			file: (filePath) => ({
				exists: async () => mockGcs.exists(filePath),
				createReadStream: () => {
					mockGcs.opened.push(filePath);
					const s = mockGcs.createReadStream(filePath);
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

beforeEach(() => {
	mockGcs.exists = async () => [true];
	mockGcs.createReadStream = () => makeGoodStream();
	mockGcs.opened = [];
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

	test("multi-file: a genuinely-absent file is skipped (not fatal)", async () => {
		const paths = [
			"gs://bucket/part-0000.jsonl",
			"gs://bucket/part-0001.jsonl", // will be 'missing'
			"gs://bucket/part-0002.jsonl",
		];
		mockGcs.exists = async (filePath) => [!filePath.endsWith("part-0001.jsonl")];

		const res = await mp(CREDS, paths, BASE_OPTS);
		expect(res).toBeDefined();
		expect(res.failed).toBe(0);
		// the missing file must never be opened for reading
		expect(mockGcs.opened).not.toContain("part-0001.jsonl");
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
