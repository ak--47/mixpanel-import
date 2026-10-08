// @ts-nocheck
/* eslint-disable no-undef */

/**
 * Mixpanel's /export server can end a 200 response cleanly with a cut-off
 * record followed by the bare text `terminated early`. exportEvents wrote the
 * body, counted the broken line (or skipped it as unparsable), and finalized
 * the file: most of the day was missing and nothing failed. Found on the
 * snowcat Menards Legacy export (2024-05-03, 08-20, 08-21, 09-06).
 *
 * Local http server only; GCS is a double whose Close commits unless the
 * stream was destroyed first, which is the storage client's rule.
 */

const http = require("http");
const fs = require("fs");
const os = require("os");
const path = require("path");
const mockCommitted = {};
const committed = mockCommitted;
jest.mock("@google-cloud/storage", () => ({
	Storage: class {
		bucket(b) {
			return {
				file: (n) => ({
					createWriteStream: () => {
						const bufs = [];
						const { Writable } = require("stream");
						return new Writable({
							write(chunk, _e, cb) { bufs.push(chunk); cb(); },
							final(cb) { mockCommitted[`gs://${b}/${n}`] = Buffer.concat(bufs); cb(); }
						});
					}
				})
			};
		}
	}
}));

const { exportEvents } = require("../components/exporters.js");

jest.setTimeout(30000);

const GOOD = '{"event":"a","properties":{"time":1}}\n{"event":"b","properties":{"time":2}}\n';
const CUT = '{"event":"a","properties":{"time":1}}\n{"event":"b","properties":{"Item ID":"1524810607507","Item Name":terminated early';
const OWN_LINE = '{"event":"a","properties":{"time":1}}\nterminated early\n';

function serve(body) {
	return new Promise((resolve) => {
		const server = http.createServer((_req, res) => {
			res.writeHead(200, { "Content-Type": "application/x-ndjson" });
			res.end(body);
		});
		server.listen(0, "127.0.0.1", () => resolve({ url: `http://127.0.0.1:${server.address().port}/export`, close: () => new Promise((r) => server.close(r)) }));
	});
}

function job(url, over = {}) {
	return {
		url, start: "2024-05-03", end: "2024-05-03", params: {}, reqMethod: "GET", auth: "Basic x",
		compress: false, verbose: false, maxRetries: 0, requests: 0, failed: 0, rateLimited: 0,
		recordsProcessed: 0, success: 0, dryRunResults: [], gcpProjectId: "p",
		store() {}, ...over
	};
}

const tmpFile = () => path.join(fs.mkdtempSync(path.join(os.tmpdir(), "exp-")), "day.ndjson");

describe("exportEvents: a stream the server terminated early fails", () => {
	let srv;
	afterEach(async () => { await srv?.close(); });

	test.each([["cut-off record", CUT], ["trailer on its own line", OWN_LINE]])("local file, verbatim (%s): throws, leaves no file", async (_label, body) => {
		srv = await serve(body);
		const out = tmpFile();
		await expect(exportEvents(out, job(srv.url))).rejects.toMatchObject({ code: "EXPORT_TERMINATED_EARLY" });
		expect(fs.existsSync(out)).toBe(false);
	});

	test("local file with a transform: throws instead of skipping the line as unparsable", async () => {
		srv = await serve(CUT);
		const out = tmpFile();
		await expect(exportEvents(out, job(srv.url, { transformFunc: (r) => r }))).rejects.toMatchObject({ code: "EXPORT_TERMINATED_EARLY" });
		expect(fs.existsSync(out)).toBe(false);
	});

	test.each([[false], [true]])("GCS (compress %s): throws and commits nothing", async (compress) => {
		srv = await serve(CUT);
		const where = `gs://bucket/day-${compress}.ndjson`;
		await expect(exportEvents(where, job(srv.url, { compress }))).rejects.toMatchObject({ code: "EXPORT_TERMINATED_EARLY" });
		expect(Object.keys(committed).filter((k) => k.includes(`day-${compress}`))).toEqual([]);
	});

	test("a whole stream still exports, to a local file and to GCS", async () => {
		srv = await serve(GOOD);
		const out = tmpFile();
		const j = job(srv.url);
		await exportEvents(out, j);
		expect(fs.readFileSync(out, "utf8")).toBe(GOOD);
		expect(j.success).toBe(2);

		const g = job(srv.url);
		await exportEvents("gs://bucket/good.ndjson", g);
		expect(committed["gs://bucket/good.ndjson"].toString()).toBe(GOOD);
		expect(g.success).toBe(2);
	});
});
