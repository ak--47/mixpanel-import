// @ts-nocheck
/* eslint-disable no-undef */

/*
REGRESSION GUARD: mixpanel-import is a library. Requiring it MUST NOT register process-global
handlers (uncaughtException, unhandledRejection, exit, SIGINT, SIGTERM) on behalf of the embedding
application. Prior to 3.5.1 components/importers.js registered five of them as an import side
effect, which suppressed Node's crash semantics and made graceful shutdown impossible for
consumers (their signal handler could never win the race against a synchronous process.exit).

Every test here runs in a CHILD PROCESS on purpose: jest itself installs global handlers, so the
assertions are meaningless in-process. No test in this file touches the network.
*/

const { spawn, spawnSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const ROOT = path.resolve(__dirname, "..");
const ENTRY = JSON.stringify(path.join(ROOT, "index.js"));
const IMPORTERS = JSON.stringify(path.join(ROOT, "components", "importers.js"));

jest.setTimeout(60000);

/** run a snippet of node in a child process; returns {status, signal, stdout, stderr} */
function runNode(code) {
	const res = spawnSync(process.execPath, ["--no-warnings", "-e", code], {
		cwd: ROOT,
		encoding: "utf8",
		timeout: 45000
	});
	return res;
}

/** spawn a long-lived child, wait for it to print `readyToken`, then hand it back */
function spawnUntilReady(code, readyToken) {
	return new Promise((resolve, reject) => {
		const child = spawn(process.execPath, ["--no-warnings", "-e", code], {
			cwd: ROOT,
			stdio: ["ignore", "pipe", "pipe"]
		});
		let stdout = "";
		let stderr = "";
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`child never printed "${readyToken}"; stdout=${stdout} stderr=${stderr}`));
		}, 40000);

		child.stdout.on("data", (d) => {
			stdout += d.toString();
			if (stdout.includes(readyToken)) {
				clearTimeout(timer);
				resolve({
					child,
					getStdout: () => stdout,
					getStderr: () => stderr,
					exited: new Promise((res) => child.on("exit", (code, signal) => res({ code, signal })))
				});
			}
		});
		child.stderr.on("data", (d) => { stderr += d.toString(); });
		child.on("error", (e) => { clearTimeout(timer); reject(e); });
	});
}

const SIGNALS = ["SIGINT", "SIGTERM", "exit", "uncaughtException", "unhandledRejection"];

describe("no process-global handlers", () => {

	test("requiring the library registers zero global listeners", () => {
		// this is the test that would have caught the original bug, and the one that keeps it out
		const code = `
			const events = ${JSON.stringify(SIGNALS)};
			const before = events.map(e => process.listeners(e).length);
			require(${ENTRY});
			const after = events.map(e => process.listeners(e).length);
			console.log(JSON.stringify({ events, before, after,
				added: events.filter((e,i) => after[i] > before[i]) }));
		`;
		const res = runNode(code);
		expect(res.stderr).toBe("");
		expect(res.status).toBe(0);
		const { before, after, added } = JSON.parse(res.stdout.trim().split("\n").pop());
		expect(added).toEqual([]);
		expect(after).toEqual(before);
	});

	test("requiring components/importers.js directly registers zero global listeners", () => {
		// belt and braces: the offending module, loaded on its own
		const code = `
			const events = ${JSON.stringify(SIGNALS)};
			const before = events.map(e => process.listeners(e).length);
			require(${IMPORTERS});
			const after = events.map(e => process.listeners(e).length);
			console.log(JSON.stringify({ before, after }));
		`;
		const res = runNode(code);
		expect(res.status).toBe(0);
		const { before, after } = JSON.parse(res.stdout.trim().split("\n").pop());
		expect(after).toEqual(before);
	});

	test("no source file under components/ or vendor/ registers a process handler", () => {
		// static guard: catches a listener added inside a rarely-loaded module, which the dynamic
		// require test above would miss
		const offenders = [];
		const dirs = ["components", "vendor"];
		for (const dir of dirs) {
			const base = path.join(ROOT, dir);
			if (!fs.existsSync(base)) continue;
			for (const file of fs.readdirSync(base, { recursive: true })) {
				if (!String(file).endsWith(".js")) continue;
				const full = path.join(base, String(file));
				if (!fs.statSync(full).isFile()) continue;
				const src = fs.readFileSync(full, "utf8");
				const lines = src.split("\n");
				lines.forEach((line, i) => {
					if (/process\s*\.\s*(on|once|prependListener|prependOnceListener)\s*\(/.test(line)) {
						offenders.push(`${dir}/${file}:${i + 1}: ${line.trim()}`);
					}
				});
			}
		}
		expect(offenders).toEqual([]);
	});
});

describe("node crash semantics are restored", () => {

	test("an uncaught exception crashes the process with exit code 1", () => {
		const code = `
			require(${ENTRY});
			setInterval(() => {}, 1000); // keep the loop alive so a swallowed error would hang
			setTimeout(() => { throw new Error('boom-uncaught'); }, 10);
		`;
		const res = runNode(code);
		expect(res.status).toBe(1);
		expect(res.stderr).toContain("boom-uncaught");
	});

	test("an unhandled rejection crashes the process with exit code 1", () => {
		const code = `
			require(${ENTRY});
			setInterval(() => {}, 1000);
			setTimeout(() => { Promise.reject(new Error('boom-rejection')); }, 10);
		`;
		const res = runNode(code);
		expect(res.status).toBe(1);
		expect(res.stderr).toContain("boom-rejection");
	});
});

describe("consumers can own process-level policy", () => {

	test("a consumer's own SIGTERM handler runs and is not pre-empted", async () => {
		const code = `
			const mp = require(${ENTRY});
			process.on('SIGTERM', () => { console.log('MINE RAN'); process.exit(0); });
			setInterval(() => {}, 1000);
			console.log('READY');
		`;
		const { child, getStdout, exited } = await spawnUntilReady(code, "READY");
		child.kill("SIGTERM");
		const { code: exitCode } = await exited;
		expect(getStdout()).toContain("MINE RAN");
		expect(exitCode).toBe(0);
	});

	test("a consumer's own SIGTERM handler may run asynchronously (graceful drain)", async () => {
		// the realistic shape: handler schedules async work and returns. before 3.5.1 the library's
		// listener would fire next and process.exit(0) before this ever resolved.
		const code = `
			require(${ENTRY});
			process.on('SIGTERM', () => {
				setTimeout(() => { console.log('DRAIN COMPLETE'); process.exit(0); }, 250);
			});
			setInterval(() => {}, 1000);
			console.log('READY');
		`;
		const { child, getStdout, exited } = await spawnUntilReady(code, "READY");
		child.kill("SIGTERM");
		const { code: exitCode } = await exited;
		expect(getStdout()).toContain("DRAIN COMPLETE");
		expect(exitCode).toBe(0);
	});

	test("a consumer's own uncaughtException handler receives the error", () => {
		const code = `
			require(${ENTRY});
			process.on('uncaughtException', (e) => { console.log('CONSUMER SAW: ' + e.message); process.exit(7); });
			setInterval(() => {}, 1000);
			setTimeout(() => { throw new Error('mine'); }, 10);
		`;
		const res = runNode(code);
		expect(res.stdout).toContain("CONSUMER SAW: mine");
		expect(res.status).toBe(7);
	});
});

describe("destroy() lifecycle API", () => {

	test("is exported from the top level and from components/importers.js", () => {
		const mp = require("../index.js");
		const importers = require("../components/importers.js");
		expect(typeof mp.destroy).toBe("function");
		expect(typeof importers.destroy).toBe("function");
		expect(mp.destroy).toBe(importers.destroy);
	});

	test("resolves when no pools were ever created, and is idempotent", async () => {
		const importers = require("../components/importers.js");
		await expect(importers.destroy()).resolves.toBeUndefined();
		await expect(importers.destroy()).resolves.toBeUndefined();
	});

	test("pools are lazy: requiring the library opens no sockets and the process exits naturally", () => {
		// no explicit process.exit() here. if requiring the module allocated eager pools that held
		// the event loop open, this child would hang and spawnSync would time out (null status).
		const code = `
			const mp = require(${ENTRY});
			mp.destroy().then(() => console.log('CLEAN EXIT'));
		`;
		const res = runNode(code);
		expect(res.status).toBe(0);
		expect(res.stdout).toContain("CLEAN EXIT");
	});
});
