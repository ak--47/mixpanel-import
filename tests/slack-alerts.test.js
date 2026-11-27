// @ts-nocheck
/* eslint-disable no-undef */

// Server-only Slack alerts (ui/slack-alerts.js + ui/alert-format.js). Local, no network.

const path = require("path");
const fs = require("fs");
const { formatAlert, logsExplorerUrl } = require("../ui/alert-format.js");
const { SlackAlerts, alertRecord, scrub, logMethod, CAP_PER_HOUR, DEDUPE_WINDOW_MS } = require("../ui/slack-alerts.js");

const AT = Date.parse("2026-11-27T04:15:00Z"); // 11/26/26 @ 11:15 PM ET
const PROD = { NODE_ENV: "production", K_SERVICE: "etl-lte", SLACK_ALERT_WEBHOOK_URL: "https://hooks.slack.com/services/T/B/x" };

function make(env = PROD, start = AT) {
	let t = start;
	const posts = [];
	const fetchImpl = jest.fn(async (url, init) => {
		posts.push(JSON.parse(init.body).text);
		return { ok: true, status: 200 };
	});
	const a = new SlackAlerts({ env, now: () => t, fetchImpl });
	return { a, posts, fetchImpl, tick: ms => { t += ms; } };
}

function boom() {
	const err = new TypeError("Cannot read properties of undefined (reading 'events')");
	err.stack = "TypeError: Cannot read properties of undefined (reading 'events')\n" +
		"    at executeJobOverWebSocket (/app/ui/server.js:181:18)\n" +
		"    at async WebSocket.<anonymous> (/app/ui/server.js:336:6)\n" +
		"    at x (/app/node_modules/ws/lib/websocket.js:1:1)";
	return err;
}

describe("alert-format (CJS port of fleet reference)", () => {
	test("reference sample renders identically", () => {
		const err = new TypeError("Cannot read properties of undefined (reading 'events')");
		err.stack = "TypeError: Cannot read properties of undefined (reading 'events')\n    at buildEvents (/app/server/v5/runner.js:212:18)\n    at async run (/app/server/v5/runner.js:88:5)\n    at x (/app/node_modules/express/lib/router.js:1:1)";
		const text = formatAlert({
			service: "DM4", title: "[runner] dungeon run failed", userId: "aaron@mixpanel.com", clientId: "3276012",
			component: "runner", path: "POST /api/run", status: 500, stack: err.stack, atMs: AT,
			notes: ["+3 more since 11:02 PM"], logsUrl: "L"
		});
		expect(text).toBe([
			":rotating_light: *DM4 — [runner] dungeon run failed*",
			"*WHO:* aaron@mixpanel.com / 3276012",
			"*WHAT:* runner / POST /api/run / 500",
			"*WHERE:* server/v5/runner.js @ 212",
			"*WHEN:* 11/26/26 @ 11:15 PM ET · +3 more since 11:02 PM",
			"*WHY:*",
			"> TypeError: Cannot read properties of undefined (reading 'events')",
			">     at buildEvents (/app/server/v5/runner.js:212:18)",
			">     at async run (/app/server/v5/runner.js:88:5)",
			">     at x (/app/node_modules/express/lib/router.js:1:1)",
			"<L|View logs in GCP>"
		].join("\n"));
	});

	test("missing fields render as dashes and text is escaped", () => {
		expect(formatAlert({ service: "Express Proxy", title: "upstream <timeout> & retry", atMs: AT })).toBe([
			":rotating_light: *Express Proxy — upstream &lt;timeout&gt; &amp; retry*",
			"*WHO:* —",
			"*WHAT:* —",
			"*WHERE:* —",
			"*WHEN:* 11/26/26 @ 11:15 PM ET",
			"*WHY:*",
			"> upstream &lt;timeout&gt; &amp; retry"
		].join("\n"));
	});

	test("logs url is pinned to alert time in mixpanel-gtm-training", () => {
		const u = logsExplorerUrl({ project: "mixpanel-gtm-training", service: "etl-lte", needle: "x", atMs: AT });
		expect(u).toContain("?project=mixpanel-gtm-training");
		expect(decodeURIComponent(u)).toContain('resource.labels.service_name="etl-lte"');
		expect(decodeURIComponent(u)).toContain("cursorTimestamp=2026-11-27T04:15:00.000Z");
	});
});

describe("slack-alerts gate", () => {
	test.each([
		["local", { ...PROD, NODE_ENV: "local" }],
		["not on cloud run", { NODE_ENV: "production", SLACK_ALERT_WEBHOOK_URL: PROD.SLACK_ALERT_WEBHOOK_URL }],
		["no webhook", { NODE_ENV: "production", K_SERVICE: "etl-lte" }]
	])("never posts when %s", async (_n, env) => {
		const { a, fetchImpl } = make(env);
		expect(a.enabled()).toBe(false);
		a.notify({ message: "job execution failed", err: boom() });
		await a.flush();
		expect(fetchImpl).not.toHaveBeenCalled();
	});

	test("posts in prod on Cloud Run (service or job) with webhook", () => {
		expect(make().a.enabled()).toBe(true);
		const { K_SERVICE, ...rest } = PROD;
		expect(make({ ...rest, CLOUD_RUN_JOB: "j" }).a.enabled()).toBe(true);
	});

	test("default instance is inert under jest", () => {
		const { slackAlerts } = require("../ui/slack-alerts.js");
		expect(slackAlerts.enabled()).toBe(false);
	});
});

describe("slack-alerts dedupe + cap", () => {
	test("renders the standard format for a server error log", async () => {
		const { a, posts } = make();
		a.notify(alertRecord([{ err: boom() }, "job execution failed"]));
		await a.flush();
		expect(posts).toHaveLength(1);
		const lines = posts[0].split("\n");
		expect(lines.slice(0, 7)).toEqual([
			":rotating_light: *Mixpanel Import (etl-lte) — job execution failed*",
			"*WHO:* —",
			"*WHAT:* —",
			"*WHERE:* ui/server.js @ 181",
			"*WHEN:* 11/26/26 @ 11:15 PM ET",
			"*WHY:*",
			"> TypeError: Cannot read properties of undefined (reading 'events')"
		]);
		expect(lines[lines.length - 1]).toMatch(/^<https:\/\/console\.cloud\.google\.com\/logs\/query;.*\|View logs in GCP>$/);
	});

	test("dedupes per message|component for 30 min and reports the repeat count", async () => {
		const { a, posts, tick } = make();
		a.notify({ message: "job execution failed", err: boom() });
		tick(60_000);
		a.notify({ message: "job execution failed", err: boom() });
		a.notify({ message: "job execution failed", err: boom() });
		a.notify({ message: "job execution failed", component: "http", err: boom() }); // different key
		expect(posts).toHaveLength(2);
		tick(DEDUPE_WINDOW_MS);
		a.notify({ message: "job execution failed", err: boom() });
		await a.flush();
		expect(posts).toHaveLength(3);
		expect(posts[2]).toContain("*WHEN:* 11/26/26 @ 11:46 PM ET · +2 more since 11:15 PM");
	});

	test("caps at 20 posts per rolling hour and reports suppressed count", async () => {
		const { a, posts, tick } = make();
		for (let i = 0; i < CAP_PER_HOUR + 5; i++) a.notify({ message: `err ${i}` });
		expect(posts).toHaveLength(CAP_PER_HOUR);
		tick(61 * 60_000);
		a.notify({ message: "after" });
		await a.flush();
		expect(posts[CAP_PER_HOUR]).toContain("+5 other alerts held by the rate cap");
	});

	test("never throws; failed post warns once without the webhook url", async () => {
		const warn = jest.spyOn(console, "warn").mockImplementation(() => {});
		const a = new SlackAlerts({
			env: PROD,
			fetchImpl: async () => { throw new Error(`Failed to fetch ${PROD.SLACK_ALERT_WEBHOOK_URL}`); }
		});
		expect(() => a.notify({ message: "x" })).not.toThrow();
		await a.flush();
		expect(warn).toHaveBeenCalledTimes(1);
		expect(warn.mock.calls[0][0]).not.toContain("hooks.slack.com");
		warn.mockRestore();
	});
});

describe("slack-alerts redaction + record extraction", () => {
	test("scrubs credentials from text", () => {
		const s = scrub('auth failed https://acct.sa:p4ss@mixpanel.com/x Authorization: Basic YWJjOmRlZg== ' +
			'{"secret":"abc","password":"hunter2"} api_key=XYZ token: tok123 0123456789abcdef0123456789abcdef');
		for (const bad of ["p4ss", "YWJjOmRlZg", "hunter2", "\"abc\"", "XYZ", "tok123", "0123456789abcdef0123456789abcdef"]) {
			expect(s).not.toContain(bad);
		}
		expect(scrub("check token / project_id / region")).toBe("check token / project_id / region");
		expect(scrub(`Unexpected token 'a', "{"acct":"sa"... is not valid JSON`)).toBe("Unexpected token 'a', <redacted> is not valid JSON");
	});

	test("strips exported record tails, JSON slices, and cloud object paths", async () => {
		const tail = '{"event":"Purchase","properties":{"$email":"bob@acme.com","amount":12';
		const msg = `Export stream terminated early: the last line is not a JSON record ("${tail}"); the rest of the range is missing`;
		expect(scrub(msg)).toBe("Export stream terminated early: the last line is not a JSON record (<redacted>); the rest of the range is missing");
		expect(scrub('Response code 400: {"error":"bad $email bob@acme.com"}')).toBe("Response code 400: <redacted>");
		expect(scrub('parse failed on [{"distinct_id":"u1"}]')).toBe("parse failed on <redacted>");
		expect(scrub("could not write gs://cust-bucket/acme/exports/2026/file.json")).toBe("could not write gs://cust-bucket/<path>");
		expect(scrub("NoSuchKey s3://b/x/y.ndjson.gz (retrying)")).toBe("NoSuchKey s3://b/<path> (retrying)");

		const { a, posts } = make();
		const err = new Error(msg);
		err.code = "EXPORT_TERMINATED_EARLY";
		a.notify(alertRecord([{ err }, "export handler error"]));
		await a.flush();
		expect(posts[0]).not.toMatch(/bob@acme\.com|Purchase|\$email/);
	});

	test("alert text never includes secrets in the error", async () => {
		const { a, posts } = make();
		const err = new Error("request failed for secret=deadbeefdeadbeefdeadbeefdeadbeef");
		a.notify({ message: "job execution failed", err });
		await a.flush();
		expect(posts[0]).not.toContain("deadbeef");
	});

	test("alertRecord keeps only message, err, and safe context keys", () => {
		const err = boom();
		const rec = alertRecord([{ err, jobId: "j1", credentials: { secret: "s" }, path: "POST /job", status: 500 }, "job error"]);
		expect(rec).toEqual({ message: "job error", err, path: "POST /job", status: 500 });
		expect(alertRecord(["plain"])).toEqual({ message: "plain" });
		expect(alertRecord([err]).message).toBe(err.message);
	});

	test("logMethod passes args through unchanged at every level", () => {
		const method = jest.fn();
		const args = [{ a: 1 }, "m"];
		logMethod.call({}, args, method, 30);
		logMethod.call({}, args, method, 50);
		expect(method).toHaveBeenCalledTimes(2);
		expect(method.mock.calls[1]).toEqual(args);
	});
});

describe("library isolation", () => {
	const ROOT = path.resolve(__dirname, "..");
	test("index.js and components/ never require the alert modules", () => {
		const files = ["index.js", ...fs.readdirSync(path.join(ROOT, "components")).filter(f => f.endsWith(".js")).map(f => `components/${f}`)];
		for (const f of files) {
			expect(fs.readFileSync(path.join(ROOT, f), "utf8")).not.toMatch(/slack-alerts|alert-format/);
		}
	});
});
