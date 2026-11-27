// @ts-check
/**
 * Canonical Slack alert format (v1) — shared across Aaron's Cloud Run fleet.
 *
 *   :rotating_light: *DM4 — dungeon run failed*
 *   *WHO:* aaron@mixpanel.com / 3276012
 *   *WHAT:* runner / POST /api/run / 500
 *   *WHERE:* server/v5/runner.js @ 212
 *   *WHEN:* 11/26/26 @ 11:15 PM ET · +3 more since 11:02 PM
 *   *WHY:*
 *   > TypeError: Cannot read properties of undefined (reading 'events')
 *   >     at buildEvents (server/v5/runner.js:212:18)
 *   <https://console.cloud.google.com/logs/...|View logs in GCP>
 *
 * Rules: every label always present ('—' when unknown). No SHA, PR, run id,
 * revision, or request bodies in Slack — those stay in Cloud Logging. Pure
 * function, no I/O, never throws on odd input. Copy this file verbatim into a
 * repo (CommonJS/TS/Go ports must produce byte-identical text for the same input).
 *
 * CommonJS port for mixpanel-import. SERVER-ONLY: required by ui/slack-alerts.js,
 * which only ui/server.js requires. Never imported by index.js or components/.
 */

const DASH = '—';
const TITLE_MAX = 120;
const FIELD_MAX = 160;
const WHY_MAX_CHARS = 1500;
const WHY_MAX_LINES = 8;

/** Slack mrkdwn escape. @param {string} s */
function esc(s) {
	return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/** One line, trimmed, truncated with …. @param {unknown} v @param {number} max */
function oneLine(v, max) {
	const s = String(v ?? '').replace(/\s+/g, ' ').trim();
	return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

/** Join present parts with ' / ', or '—'. @param {unknown[]} parts */
function slashJoin(parts) {
	const kept = parts.map(p => oneLine(p, FIELD_MAX)).filter(Boolean);
	return kept.length ? esc(kept.join(' / ')) : DASH;
}

/**
 * '11/26/26 @ 11:15 PM' in America/New_York.
 * @param {number} ms
 * @param {boolean} [withDate]
 */
function etTime(ms, withDate = true) {
	const d = new Date(ms);
	const parts = Object.fromEntries(
		new Intl.DateTimeFormat('en-US', {
			timeZone: 'America/New_York', month: '2-digit', day: '2-digit', year: '2-digit',
			hour: 'numeric', minute: '2-digit', hour12: true
		}).formatToParts(d).map(p => [p.type, p.value])
	);
	const time = `${parts.hour}:${parts.minute} ${String(parts.dayPeriod).toUpperCase()}`;
	return withDate ? `${parts.month}/${parts.day}/${parts.year} @ ${time}` : time;
}

/**
 * First stack frame that is app code (not node_modules / node: internals).
 * Returns {file, line} with the path made repo-relative ('/app/' or cwd stripped).
 * @param {string | undefined} stack
 * @returns {{file: string, line: string} | null}
 */
function whereFromStack(stack) {
	if (typeof stack !== 'string') return null;
	for (const raw of stack.split('\n').slice(1)) {
		const m = raw.match(/\(?((?:file:\/\/)?[^\s()]+?):(\d+):\d+\)?\s*$/);
		if (!m) continue;
		let file = m[1].replace(/^file:\/\//, '');
		if (/node_modules|^node:|^internal\//.test(file)) continue;
		const cwd = typeof process !== 'undefined' && process.cwd ? process.cwd() + '/' : '';
		if (cwd && file.startsWith(cwd)) file = file.slice(cwd.length);
		file = file.replace(/^\/(app|workspace|usr\/src\/app)\//, '');
		return { file, line: m[2] };
	}
	return null;
}

/**
 * @typedef {object} AlertInput
 * @property {string} service        Human service name, e.g. 'DM4', 'Snowcat Worker'
 * @property {string} title          Short error (log message first line)
 * @property {string} [userId]
 * @property {string} [clientId]     project id / customer / client id
 * @property {string} [component]    module / subarea / macro
 * @property {string} [path]         'POST /api/run' or job/task name
 * @property {string|number} [status] HTTP status or exit code
 * @property {string} [file]         WHERE file (else derived from stack)
 * @property {string|number} [line]
 * @property {string} [stack]        Error stack (used for WHERE and WHY)
 * @property {string} [detail]       Longer error text (used for WHY when no stack, or prepended)
 * @property {number} atMs
 * @property {string[]} [notes]      dedupe/cap notes: '+3 more since 11:02 PM'
 * @property {string} [logsUrl]
 * @property {string} [envTag]       e.g. 'staging' — shown as [staging] before service; omit in prod
 */

/** @param {AlertInput} a @returns {string} */
function formatAlert(a) {
	const title = oneLine(a.title, TITLE_MAX) || 'error';
	const env = a.envTag ? `[${esc(a.envTag)}] ` : '';
	const where = a.file ? { file: String(a.file), line: a.line != null ? String(a.line) : '' } : whereFromStack(a.stack);
	const whereText = where ? esc(`${where.file}${where.line ? ` @ ${where.line}` : ''}`) : DASH;
	const when = [etTime(a.atMs) + ' ET', ...(a.notes || []).filter(Boolean)].map(esc).join(' · ');

	// WHY: detail first, then stack (dropping a duplicated first line), capped.
	const whyLines = [];
	const detail = String(a.detail ?? '').trim();
	if (detail) whyLines.push(...detail.split('\n'));
	if (typeof a.stack === 'string' && a.stack.trim()) {
		const st = a.stack.trim().split('\n');
		if (whyLines.length && st[0].trim() === whyLines[0].trim()) st.shift();
		whyLines.push(...st);
	}
	let why = whyLines.map(l => l.replace(/\s+$/, '')).filter(Boolean).slice(0, WHY_MAX_LINES).join('\n');
	if (why.length > WHY_MAX_CHARS) why = why.slice(0, WHY_MAX_CHARS - 1) + '…';
	if (!why) why = title;
	const quoted = why.split('\n').map(l => `> ${esc(l)}`).join('\n');

	const lines = [
		`:rotating_light: ${env}*${esc(oneLine(a.service, 60))} — ${esc(title)}*`,
		`*WHO:* ${slashJoin([a.userId, a.clientId])}`,
		`*WHAT:* ${slashJoin([a.component, a.path, a.status])}`,
		`*WHERE:* ${whereText}`,
		`*WHEN:* ${when}`,
		`*WHY:*`,
		quoted
	];
	if (a.logsUrl) lines.push(`<${a.logsUrl}|View logs in GCP>`);
	return lines.join('\n');
}

/**
 * Logs Explorer link pinned to the alert time (−1h / +30m).
 * @param {{project: string, service?: string, job?: string, needle?: string, atMs: number}} o
 */
function logsExplorerUrl({ project, service, job, needle, atMs }) {
	const q = [
		job ? 'resource.type="cloud_run_job"' : 'resource.type="cloud_run_revision"',
		job ? `resource.labels.job_name="${job}"` : service ? `resource.labels.service_name="${service}"` : '',
		'severity>=ERROR',
		needle ? `jsonPayload.message:"${needle.slice(0, 120).replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"` : ''
	].filter(Boolean).join('\n');
	const from = new Date(atMs - 3600_000).toISOString();
	const to = new Date(atMs + 1800_000).toISOString();
	return 'https://console.cloud.google.com/logs/query' +
		`;query=${encodeURIComponent(q)}` +
		`;timeRange=${encodeURIComponent(`${from}/${to}`)}` +
		`;cursorTimestamp=${encodeURIComponent(new Date(atMs).toISOString())}` +
		`?project=${encodeURIComponent(project)}`;
}

module.exports = { esc, etTime, whereFromStack, formatAlert, logsExplorerUrl };
