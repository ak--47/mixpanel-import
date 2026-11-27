// @ts-check

/**
 * Slack alerts for the etl-lte Cloud Run service (the `--ui` web server).
 *
 * SERVER-ONLY. This module is required by ui/server.js and nothing else. It is
 * not exported from the package, index.js and components/ never require it, and
 * it is inert anywhere except a production Cloud Run service with a webhook set.
 *
 * Ported from dm4 server/utils/slack-alerts.js (minus dungeon keys, recoverable
 * hold, sweep alert). Wired through pino's hooks.logMethod in ui/server.js, so
 * every ERROR/fatal log line from the server posts; the crash handlers in
 * startUI() await flush() before exit.
 *
 *   - Gate: NODE_ENV=production AND on Cloud Run (K_SERVICE or CLOUD_RUN_JOB)
 *     AND SLACK_ALERT_WEBHOOK_URL set. Local dev, tests, and `npx mixpanel-import
 *     --ui` never post.
 *   - Dedupe: once per `message | component` per 30 minutes; repeats are counted
 *     and reported on the key's next post.
 *   - Cap: 20 posts per rolling hour; posts over the cap are counted and
 *     reported on the next post that goes out.
 *   - Redaction: only the log message and the error's message/stack are used
 *     (never request bodies, credentials, options, or cloud paths), and those are
 *     scrubbed of secrets (basic-auth URLs, Basic/Bearer headers, key=value
 *     secrets, PEM keys, 32-hex API secrets, Slack webhook URLs, the input
 *     slice V8 quotes in JSON.parse errors).
 *   - Never throws. A failed post writes one console.warn line; this module never
 *     calls the app logger (an ERROR from here would loop back into Slack).
 *
 * @module ui/slack-alerts
 */

const { formatAlert, logsExplorerUrl, etTime } = require('./alert-format.js');

const DEDUPE_WINDOW_MS = 30 * 60_000;
const CAP_PER_HOUR = 20;
const HOUR_MS = 60 * 60_000;
const POST_TIMEOUT_MS = 5_000;
const MAX_KEYS = 500;
const GCP_PROJECT = 'mixpanel-gtm-training';
const DEFAULT_SERVICE = 'etl-lte';
const SERVICE_NAME = 'Mixpanel Import (etl-lte)';

const R = '<redacted>';

/**
 * Remove credentials from free text before it can reach Slack.
 * @param {unknown} v
 * @returns {string}
 */
function scrub(v) {
	let s = String(v ?? '');
	s = s.replace(/https?:\/\/hooks\.slack\.com\/\S*/g, '<webhook>');
	// V8 JSON.parse errors quote a slice of the input (could be credentials or customer rows).
	s = s.replace(/(?:\.\.\.)?"[^\n]*?"(?:\.\.\.)? is not valid JSON/g, `${R} is not valid JSON`);
	s = s.replace(/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?(-----END [A-Z ]*PRIVATE KEY-----|$)/g, R);
	s = s.replace(/(\b[a-z][a-z0-9+.-]*:\/\/)[^\s/@:]+:[^\s/@]+@/gi, `$1${R}@`);
	s = s.replace(/\b(Basic|Bearer)\s+[A-Za-z0-9._~+/=-]+/g, `$1 ${R}`);
	s = s.replace(
		/(["']?)\b(api[_-]?secret|secret(?:[_-]?access[_-]?key)?|client[_-]?secret|password|passwd|pass|token|api[_-]?key|access[_-]?key(?:[_-]?id)?|private[_-]?key|authorization|acct|credentials?)\1(\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,&;}\]]+)/gi,
		(_m, q, key, sep) => `${q}${key}${q}${sep}${R}`
	);
	s = s.replace(/\b[a-f0-9]{32}\b/gi, R);
	return s;
}

/**
 * Turn pino logMethod args into a flat alert record. Uses only the message, the
 * error, and a few known-safe context keys; everything else is ignored.
 * pino call shapes: (msg), (obj, msg), (err), (err, msg).
 * @param {any[]} args
 * @returns {Record<string, any>}
 */
function alertRecord(args) {
	const [first, second] = args;
	/** @type {Record<string, any>} */
	const obj = first && typeof first === 'object' ? first : {};
	const err = first instanceof Error ? first
		: obj.err instanceof Error ? obj.err
			: obj.error instanceof Error ? obj.error
				: null;
	let message = typeof first === 'string' ? first : typeof second === 'string' ? second : '';
	if (!message && err) message = err.message;
	if (!message && typeof obj.msg === 'string') message = obj.msg;
	if (!message && typeof obj.message === 'string') message = obj.message;
	/** @type {Record<string, any>} */
	const rec = { message };
	if (err) rec.err = err;
	for (const k of ['component', 'path', 'status', 'user', 'project']) {
		if (typeof obj[k] === 'string' || typeof obj[k] === 'number') rec[k] = obj[k];
	}
	return rec;
}

class SlackAlerts {
	/**
	 * @param {{fetchImpl?: typeof fetch, now?: () => number, env?: NodeJS.ProcessEnv}} [deps]
	 */
	constructor({ fetchImpl, now, env } = {}) {
		this.fetchImpl = fetchImpl || ((url, init) => fetch(url, init));
		this.now = now || Date.now;
		this.env = env || process.env;
		/** @type {Map<string, {lastSentAt: number, suppressed: number}>} */
		this.keys = new Map();
		/** @type {number[]} send times inside the rolling hour */
		this.sentTimes = [];
		this.capSuppressed = 0;
		/** @type {Set<Promise<void>>} */
		this.pending = new Set();
	}

	url() {
		return this.env.SLACK_ALERT_WEBHOOK_URL || '';
	}

	/** True when this process should post: prod, on Cloud Run, URL set. */
	enabled() {
		return Boolean(
			this.env.NODE_ENV === 'production' &&
			(this.env.K_SERVICE || this.env.CLOUD_RUN_JOB) &&
			this.url()
		);
	}

	/**
	 * Queue one post for an error record ({message, err?, component?, path?, status?}).
	 * Never throws.
	 * @param {Record<string, any>} info
	 */
	notify(info) {
		try {
			if (!this.enabled()) return;
			const now = this.now();
			const message = String(info.message ?? '').split('\n')[0].trim();
			const component = typeof info.component === 'string' ? info.component : '';
			const key = `${message} | ${component}`;

			const state = this.keys.get(key);
			if (state && now - state.lastSentAt < DEDUPE_WINDOW_MS) {
				state.suppressed += 1;
				return;
			}

			this.sentTimes = this.sentTimes.filter(t => now - t < HOUR_MS);
			if (this.sentTimes.length >= CAP_PER_HOUR) {
				this.capSuppressed += 1;
				return;
			}

			const notes = [];
			if (state && state.suppressed > 0) notes.push(`+${state.suppressed} more since ${etTime(state.lastSentAt, false)}`);
			if (this.capSuppressed > 0) notes.push(`+${this.capSuppressed} other alerts held by the rate cap`);
			this.capSuppressed = 0;

			this.keys.set(key, { lastSentAt: now, suppressed: 0 });
			this.sentTimes.push(now);
			this.pruneKeys(now);

			this.track(this.post(this.format(info, { message, notes, atMs: now })));
		} catch (err) {
			console.warn(JSON.stringify({ severity: 'WARNING', message: '[slack-alerts] notify failed', error: scrub(err instanceof Error ? err.message : err) }));
		}
	}

	/**
	 * Build the standard v1 alert text (see ui/alert-format.js).
	 * @param {Record<string, any>} info
	 * @param {{message: string, notes: string[], atMs: number}} parts
	 */
	format(info, { message, notes, atMs }) {
		const err = info.err instanceof Error ? info.err : null;
		const stack = err && typeof err.stack === 'string' ? scrub(err.stack) : undefined;
		const detail = err && !stack ? scrub(err.message) : undefined;
		const title = scrub(message);
		return formatAlert({
			service: SERVICE_NAME,
			title,
			userId: info.user != null ? scrub(info.user) : undefined,
			clientId: info.project != null ? scrub(info.project) : undefined,
			component: info.component,
			path: info.path,
			status: info.status,
			stack,
			detail,
			atMs,
			notes,
			logsUrl: logsExplorerUrl({
				project: GCP_PROJECT,
				service: this.env.K_SERVICE || DEFAULT_SERVICE,
				job: this.env.CLOUD_RUN_JOB || undefined,
				needle: message,
				atMs
			})
		});
	}

	/**
	 * Post and report failure on the console. Resolves either way.
	 * @param {string} text
	 * @returns {Promise<void>}
	 */
	async post(text) {
		try {
			const res = await this.send(text);
			if (!res.ok) {
				console.warn(JSON.stringify({ severity: 'WARNING', message: '[slack-alerts] post failed', status: res.status }));
			}
		} catch (err) {
			let msg = scrub(err instanceof Error ? err.message : err);
			const url = this.url();
			if (url) msg = msg.split(url).join('<webhook>');
			console.warn(JSON.stringify({ severity: 'WARNING', message: '[slack-alerts] post failed', error: msg }));
		}
	}

	/**
	 * One raw post to the webhook. Rejects on network error or timeout.
	 * @param {string} text
	 * @returns {Promise<{ok: boolean, status: number}>}
	 */
	async send(text) {
		const url = this.url();
		if (!url) throw new Error('SLACK_ALERT_WEBHOOK_URL is not set');
		const res = await this.fetchImpl(url, {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ text }),
			signal: AbortSignal.timeout(POST_TIMEOUT_MS)
		});
		return { ok: res.ok, status: res.status };
	}

	/** @param {Promise<void>} p */
	track(p) {
		this.pending.add(p);
		p.finally(() => this.pending.delete(p));
	}

	/**
	 * Resolve when pending posts finish or `timeoutMs` passes. Crash handlers
	 * await this so the crash alert leaves the process before exit.
	 * @param {number} [timeoutMs]
	 */
	async flush(timeoutMs = 3000) {
		if (this.pending.size === 0) return;
		/** @type {ReturnType<typeof setTimeout>|undefined} */
		let timer;
		const timeout = new Promise(resolve => { timer = setTimeout(resolve, timeoutMs); });
		await Promise.race([Promise.allSettled([...this.pending]), timeout]);
		clearTimeout(timer);
	}

	/**
	 * Drop expired dedupe keys once the map grows large.
	 * @param {number} now
	 */
	pruneKeys(now) {
		if (this.keys.size <= MAX_KEYS) return;
		for (const [k, v] of this.keys) {
			if (now - v.lastSentAt >= DEDUPE_WINDOW_MS) this.keys.delete(k);
		}
	}
}

const slackAlerts = new SlackAlerts();

/**
 * pino `hooks.logMethod`: route every ERROR (50) / fatal (60) record to Slack,
 * then log as usual. Never throws, never changes what is logged.
 * @param {any[]} args
 * @param {(...a: any[]) => void} method
 * @param {number} level
 * @this {any}
 */
function logMethod(args, method, level) {
	if (level >= 50) {
		try {
			slackAlerts.notify(alertRecord(args));
		} catch (err) {
			console.warn(JSON.stringify({ severity: 'WARNING', message: '[slack-alerts] could not build alert', error: scrub(String(err)) }));
		}
	}
	return method.apply(this, args);
}

module.exports = {
	SlackAlerts,
	slackAlerts,
	logMethod,
	alertRecord,
	scrub,
	DEDUPE_WINDOW_MS,
	CAP_PER_HOUR,
	SERVICE_NAME
};
