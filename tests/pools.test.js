// @ts-nocheck
/* eslint-disable no-undef */

/*
Functional coverage for the lazy undici pool lifecycle introduced in 3.5.1.

These tests run against LOCAL http servers — no Mixpanel credentials, no network egress. They
prove the three things the refactor could plausibly have broken:
  1. requests still go out and come back through a lazily-created pool
  2. pools are reused across calls (keep-alive is preserved, not re-dialed per batch)
  3. each origin gets its own pool pointed at itself (multi-region: us/eu/in never share a pool)
and that destroy() actually releases them and can be followed by more work.
*/

const http = require("http");
const { flushToMixpanelWithUndici, destroy } = require("../components/importers.js");

jest.setTimeout(30000);

/** spin up a local http server that records what it received */
function startServer(handler) {
	return new Promise((resolve) => {
		const received = [];
		const connections = [];
		const server = http.createServer((req, res) => {
			let body = "";
			req.on("data", (c) => { body += c; });
			req.on("end", () => {
				received.push({ url: req.url, method: req.method, headers: req.headers, body });
				if (handler) return handler(req, res, body);
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ code: 200, num_records_imported: 1, status: 1 }));
			});
		});
		server.on("connection", (socket) => connections.push(socket));
		server.listen(0, "127.0.0.1", () => {
			const { port } = server.address();
			resolve({
				server,
				port,
				origin: `http://127.0.0.1:${port}`,
				received,
				connections,
				close: () => new Promise((r) => {
					for (const s of connections) s.destroy();
					server.close(() => r());
				})
			});
		});
	});
}

/** minimal JobConfig stand-in: only the fields flushToMixpanelWithUndici touches */
function makeJob(url, overrides = {}) {
	return {
		url,
		recordType: "event",
		compress: false,
		strict: true,
		http2: false,
		auth: "Basic dGVzdDp0ZXN0",
		contentType: "application/json",
		maxRetries: 0,
		retries: 0,
		requests: 0,
		clientErrors: 0,
		serverErrors: 0,
		success: 0,
		failed: 0,
		empty: 0,
		errors: {},
		abridged: true,
		lastBatchLength: 1,
		responses: [],
		store() { },
		addBadRecord() { },
		addBatchLength() { },
		...overrides
	};
}

const BATCH = [{ event: "test", properties: { time: 1, distinct_id: "a", $insert_id: "b" } }];

afterEach(async () => {
	await destroy();
});

describe("lazy undici pools", () => {

	test("a request succeeds through a lazily-created pool", async () => {
		const srv = await startServer();
		try {
			const job = makeJob(`${srv.origin}/import`);
			const res = await flushToMixpanelWithUndici(BATCH, job);

			expect(srv.received.length).toBe(1);
			expect(srv.received[0].method).toBe("POST");
			expect(srv.received[0].url).toContain("/import");
			expect(JSON.parse(srv.received[0].body)).toEqual(BATCH);

			// flushToMixpanelWithUndici resolves to a [response, batch] tuple
			const [response] = res;
			expect(response.num_records_imported).toBe(1);
			expect(response.code).toBe(200);
			expect(job.success).toBe(1);
		} finally {
			await destroy();
			await srv.close();
		}
	});

	test("the pool is reused across calls to the same origin (keep-alive preserved)", async () => {
		const srv = await startServer();
		try {
			const job = makeJob(`${srv.origin}/import`);
			await flushToMixpanelWithUndici(BATCH, job);
			await flushToMixpanelWithUndici(BATCH, job);
			await flushToMixpanelWithUndici(BATCH, job);

			expect(srv.received.length).toBe(3);
			// one pool, keep-alive: all three requests share a single TCP connection
			expect(srv.connections.length).toBe(1);
		} finally {
			await destroy();
			await srv.close();
		}
	});

	test("each origin gets its own pool and requests are not misrouted", async () => {
		// multi-region guard: two origins must not share a pool, or one region's batches would be
		// delivered to the other region's host
		const a = await startServer();
		const b = await startServer();
		try {
			await flushToMixpanelWithUndici(BATCH, makeJob(`${a.origin}/import`));
			await flushToMixpanelWithUndici(BATCH, makeJob(`${b.origin}/engage`));

			expect(a.received.length).toBe(1);
			expect(b.received.length).toBe(1);
			expect(a.received[0].url).toContain("/import");
			expect(b.received[0].url).toContain("/engage");
		} finally {
			await destroy();
			await a.close();
			await b.close();
		}
	});

	test("destroy() closes pools, and further imports still work afterwards", async () => {
		const srv = await startServer();
		try {
			await flushToMixpanelWithUndici(BATCH, makeJob(`${srv.origin}/import`));
			expect(srv.connections.length).toBe(1);

			await destroy();

			// a fresh pool is created on demand => a new TCP connection to the same server
			await flushToMixpanelWithUndici(BATCH, makeJob(`${srv.origin}/import`));
			expect(srv.received.length).toBe(2);
			expect(srv.connections.length).toBe(2);
		} finally {
			await destroy();
			await srv.close();
		}
	});

	test("destroy() is idempotent and safe with no pools open", async () => {
		await expect(destroy()).resolves.toBeUndefined();
		await expect(destroy()).resolves.toBeUndefined();
	});

	test("maxRetries: 0 sends exactly one request and does not retry", async () => {
		// 503 is in retryStatusCodes, so this batch would retry if maxRetries were not honored.
		// before the `||` -> `??` fix, a literal 0 was falsy and became 10 retries.
		const srv = await startServer((req, res, body) => {
			res.writeHead(503, { "Content-Type": "application/json" });
			res.end(JSON.stringify({ error: "service unavailable" }));
		});
		try {
			const job = makeJob(`${srv.origin}/import`, { maxRetries: 0 });
			await flushToMixpanelWithUndici(BATCH, job);

			expect(srv.received.length).toBe(1);
			expect(job.retries).toBe(0);
		} finally {
			await destroy();
			await srv.close();
		}
	});

	test("maxRetries: 2 retries exactly twice before giving up", async () => {
		const srv = await startServer((req, res) => {
			res.writeHead(503, { "Content-Type": "application/json" });
			res.end(JSON.stringify({ error: "service unavailable" }));
		});
		try {
			const job = makeJob(`${srv.origin}/import`, { maxRetries: 2 });
			await flushToMixpanelWithUndici(BATCH, job);

			// initial attempt + 2 retries
			expect(srv.received.length).toBe(3);
			expect(job.retries).toBe(2);
		} finally {
			await destroy();
			await srv.close();
		}
	});

	test("socket-level failures surface via the pool-scoped connectionError listener", async () => {
		// this listener is the scoped replacement for the process-global handlers removed in 3.5.1;
		// if undici ever stops emitting 'connectionError' on Pool, this test fails rather than the
		// diagnostic silently disappearing
		const originalL = global.l;
		const seen = [];
		global.l = (msg) => seen.push(String(msg));
		try {
			// port 1 has nothing listening => ECONNREFUSED at the socket layer
			const job = makeJob("http://127.0.0.1:1/import", { maxRetries: 0 });
			await flushToMixpanelWithUndici(BATCH, job);
			await new Promise((r) => setTimeout(r, 250)); // listener fires async

			const connErrors = seen.filter((m) => m.includes("undici connection error"));
			expect(connErrors.length).toBeGreaterThan(0);
			expect(connErrors[0]).toContain("127.0.0.1:1");
		} finally {
			global.l = originalL;
			await destroy();
		}
	});
});
