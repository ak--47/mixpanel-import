// @ts-nocheck
/* eslint-env browser */

// Shared identityReplay UI module — loaded by BOTH /import and /export pages via <script>.
// Exposes window.IdentityReplayUI = { fieldDefs, renderSection, collect, validate, cliFlags, refresh }.
//
// Wire contract (see plans/identity-replay-ui/design.md): collect() returns ONE nested
// identityReplay object (or null when disabled); default-valued keys are omitted; regex mode
// sends isUserId (string), function mode sends isUserIdCode (source string) and never both.
// Uses the page's native markup: import = .collapsible-section, export = .form-group blocks.
// Never loads Monaco itself — uses window.monaco only if the page already loaded it.

(function () {
	'use strict';

	const MAX_SAMPLE_CHIPS = 200;

	const DEFAULT_FUNCTION_CODE = [
		'(id, record) => {',
		'\t// return true ONLY when `id` is one of YOUR user ids',
		'\treturn /^\\d+$/.test(id); // example: numeric ids are users',
		'}'
	].join('\n');

	// ---------------------------------------------------------------------------
	// Field definitions — one array drives rendering, collection, and CLI preview.
	// Tooltips are the adoption surface: plain language, defaults stated, 1-2 sentences.
	// (associationProps is deliberately NOT exposed — module-only, per the build contract.)
	// ---------------------------------------------------------------------------
	const fieldDefs = [
		{
			key: 'isUserId',
			id: 'ir-user-id-regex',
			label: 'User ID Regex',
			type: 'regex',
			default: '',
			required: true,
			section: 'core',
			tooltip: 'A regular expression that matches YOUR user IDs and nothing else — the one thing the tool cannot infer. Test it against real samples below.'
		},
		{
			key: 'electionScope',
			id: 'ir-election-scope',
			label: 'Election Scope',
			type: 'select',
			default: 'cluster',
			section: 'core',
			options: [
				{ value: 'cluster', label: 'cluster — one elected user per cluster (default)' },
				{ value: 'device', label: 'device — direct evidence per device' }
			],
			tooltip: 'How anonymous IDs get linked when a cluster contains more than one user: cluster (the default) links every anonymous ID to one elected winner; device links each anonymous ID to the user it has direct evidence with, falling back to the ambiguity policy for the rest — the usual choice for shared-device data.'
		},
		{
			key: 'onAmbiguous',
			id: 'ir-on-ambiguous',
			label: 'Multi-User Clusters',
			type: 'select',
			default: 'drop',
			section: 'core',
			options: [
				{ value: 'drop', label: 'drop — no association events for that cluster (default)' },
				{ value: 'resolve', label: 'resolve — elect a single winner' },
				{ value: 'error', label: 'error — abort the job' }
			],
			tooltip: 'What to do when two or more users end up in the same identity cluster. drop (the default) emits no association events for that cluster, resolve elects a single winner, error aborts. In simplified projects a device binds to its first user forever, so this choice matters.'
		},
		{
			key: 'associationTimestamp',
			id: 'ir-association-timestamp',
			label: 'Association Timestamp',
			type: 'timestamp',
			default: 'original',
			section: 'core',
			options: [
				{ value: 'original', label: 'original — first-seen time of each device (default)' },
				{ value: 'floor', label: 'floor — 24h before the earliest event' },
				{ value: 'epoch', label: 'pinned epoch — for chunked / multi-run replays' }
			],
			tooltip: 'When the emitted association events are timestamped. original (the default) uses the first time each device was seen; floor backdates them to 24 hours before the earliest event; a pinned epoch number keeps events deduplicating across chunked or repeated runs.'
		},
		{
			key: 'graphPath',
			id: 'ir-graph-path',
			label: 'Graph Audit File',
			type: 'text',
			default: '',
			section: 'core',
			placeholder: './identity-graph.jsonl',
			tooltip: 'Optional audit artifact: writes the resolved device-to-user pair table plus any unresolved clusters when the job flushes. Accepts a local path, gs://, or s3:// URL; leave empty (the default) to skip.'
		},
		{
			key: 'minAssociationRate',
			id: 'ir-min-association-rate',
			label: 'Min Association Rate',
			type: 'number',
			default: 0,
			min: 0,
			max: 1,
			step: 0.01,
			section: 'core',
			tooltip: 'Fail-closed safety floor between 0 and 1: abort the job if fewer than this fraction of identity verbs turn into association events. 0 (the default) disables the check.'
		},

		// --- advanced: only sent when changed from defaults ---
		{
			key: 'graph',
			id: 'ir-graph',
			label: 'Build Identity Graph',
			type: 'toggle',
			default: true,
			section: 'advanced',
			tooltip: 'Build the full identity graph so transitive links (anon → anon → user) are discovered at end of stream. On by default; turn off for stateless lite mode that only rewrites verbs one-for-one.'
		},
		{
			key: 'maxGraphSize',
			id: 'ir-max-graph-size',
			label: 'Max Graph Size',
			type: 'number',
			default: 5000000,
			min: 1,
			step: 1,
			section: 'advanced',
			tooltip: 'Maximum distinct IDs held in memory while building the graph (default 5,000,000). Budget roughly 1GB of heap per million distinct IDs.'
		},
		{
			key: 'onGraphOverflow',
			id: 'ir-on-graph-overflow',
			label: 'On Graph Overflow',
			type: 'select',
			default: 'warn',
			section: 'advanced',
			options: [
				{ value: 'warn', label: 'warn — keep streaming, count skipped edges (default)' },
				{ value: 'abort', label: 'abort — stop the job' }
			],
			tooltip: 'What happens when the graph hits the max size cap: warn (the default) keeps streaming and counts the skipped edges; abort stops the job.'
		},
		{
			key: 'identityEvents',
			id: 'ir-identity-events',
			label: 'Identity Verbs',
			type: 'select',
			default: 'rewrite',
			section: 'advanced',
			options: [
				{ value: 'rewrite', label: 'rewrite — verbs become association events (default)' },
				{ value: 'drop', label: 'drop — feed the graph but emit nothing' }
			],
			tooltip: 'rewrite (the default) turns $identify / $create_alias / $merge verbs into association events; drop still uses them to build the graph but emits nothing — useful when an earlier run already sent the associations.'
		},
		{
			key: 'associationEventName',
			id: 'ir-association-event-name',
			label: 'Association Event Name',
			type: 'text',
			default: 'identity association',
			section: 'advanced',
			placeholder: 'identity association',
			tooltip: 'Event name given to the emitted association events (default: identity association). Any non-reserved event name works.'
		},
		{
			key: 'bareDistinctId',
			id: 'ir-bare-distinct-id',
			label: 'Bare distinct_id Handling',
			type: 'select',
			default: 'validate',
			section: 'advanced',
			options: [
				{ value: 'validate', label: 'validate — classify with your user ID matcher (default)' },
				{ value: 'passthru', label: 'passthru — leave bare distinct_ids alone' }
			],
			tooltip: 'validate (the default) classifies bare distinct_ids on ordinary events: matches become $user_id, everything else becomes a $device:-prefixed $device_id — preventing anonymous UUIDs from being promoted to phantom users. passthru leaves them untouched.'
		},
		{
			key: 'userIdFallbackProps',
			id: 'ir-user-id-fallback-props',
			label: 'User ID Fallback Props',
			type: 'text',
			parse: 'list',
			default: '',
			section: 'advanced',
			placeholder: 'user_id, email',
			tooltip: 'Extra property names to probe for a user ID on ordinary events, comma-separated (default: none).'
		},
		{
			key: 'denylist',
			id: 'ir-denylist',
			label: 'Denylist',
			type: 'textarea',
			parse: 'list',
			default: '',
			section: 'advanced',
			placeholder: 'test-account-1\ntest-account-2',
			tooltip: 'IDs to exclude entirely — records carrying them are dropped and counted in telemetry. Typically test accounts; one per line (default: none).'
		},
		{
			key: 'scrubJunkIds',
			id: 'ir-scrub-junk-ids',
			label: 'Scrub Junk IDs',
			type: 'toggle',
			default: true,
			section: 'advanced',
			tooltip: 'Neutralize well-known junk IDs (anonymous, null, the all-zero UUID, ...) so a shared junk device ID cannot fuse unrelated users into one mega-cluster. On by default.'
		},
		{
			key: 'scrubExportProps',
			id: 'ir-scrub-export-props',
			label: 'Scrub Raw-Export Props',
			type: 'toggle',
			default: true,
			section: 'advanced',
			tooltip: 'Strip the properties Mixpanel adds to raw exports ($import, $mp_api_endpoint, $mp_api_timestamp_ms, ...) before re-import. On by default; harmless when the source is not a raw export.'
		}
	];

	// keys the CLI can express (components/cli.js); everything else is module-API only
	const CLI_EXPRESSIBLE_KEYS = ['isUserId', 'graphPath', 'onAmbiguous'];

	// module state (per page — each page loads its own copy)
	const state = {
		page: 'import',
		rendered: false,
		monacoEditor: null // live monaco instance when mounted; textarea is always source of truth
	};

	// --------------------------------------------------------------------------
	// null-safe DOM helpers (same spirit as getElementValue/getElementChecked)
	// --------------------------------------------------------------------------
	function val(id, defaultValue = '') {
		const el = document.getElementById(id);
		return el ? el.value : defaultValue;
	}

	function checked(id) {
		const el = document.getElementById(id);
		return el ? el.checked : false;
	}

	function esc(str) {
		return String(str)
			.replace(/&/g, '&amp;')
			.replace(/</g, '&lt;')
			.replace(/>/g, '&gt;')
			.replace(/"/g, '&quot;');
	}

	function shellQuote(str) {
		return `'${String(str).replace(/'/g, `'\\''`)}'`;
	}

	function parseList(raw) {
		return String(raw || '')
			.split(/[\n,]/)
			.map(s => s.trim())
			.filter(Boolean);
	}

	function isFunctionMode() {
		const radio = document.getElementById('ir-mode-function');
		return radio ? radio.checked : false;
	}

	function getFunctionCode() {
		// the hidden/visible textarea is always the source of truth (monaco syncs into it)
		return val('ir-function-code');
	}

	// --------------------------------------------------------------------------
	// rendering
	// --------------------------------------------------------------------------
	function renderField(def) {
		const help = `<small>${esc(def.tooltip)}</small>`;
		switch (def.type) {
			case 'select': {
				const opts = def.options
					.map(o => `<option value="${esc(o.value)}"${o.value === def.default ? ' selected' : ''}>${esc(o.label)}</option>`)
					.join('\n\t\t\t\t');
				return `<div class="form-group">
					<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}</label>
					<select id="${def.id}" name="${def.id}">
						${opts}
					</select>
					${help}
				</div>`;
			}
			case 'number': {
				const attrs = [
					def.min !== undefined ? `min="${def.min}"` : '',
					def.max !== undefined ? `max="${def.max}"` : '',
					def.step !== undefined ? `step="${def.step}"` : ''
				].filter(Boolean).join(' ');
				return `<div class="form-group">
					<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}</label>
					<input type="number" id="${def.id}" name="${def.id}" value="${esc(def.default)}" ${attrs} autocomplete="off">
					${help}
				</div>`;
			}
			case 'text': {
				return `<div class="form-group">
					<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}</label>
					<input type="text" id="${def.id}" name="${def.id}" value="${esc(def.default)}" placeholder="${esc(def.placeholder || '')}" autocomplete="off" spellcheck="false">
					${help}
				</div>`;
			}
			case 'textarea': {
				return `<div class="form-group">
					<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}</label>
					<textarea id="${def.id}" name="${def.id}" rows="3" placeholder="${esc(def.placeholder || '')}" spellcheck="false" style="font-family: var(--font-mono); font-size: 14px;"></textarea>
					${help}
				</div>`;
			}
			case 'toggle': {
				return `<div class="form-group">
					<label class="checkbox-label" title="${esc(def.tooltip)}">
						<input type="checkbox" id="${def.id}" name="${def.id}"${def.default ? ' checked' : ''}>
						${esc(def.label)}
					</label>
					${help}
				</div>`;
			}
			default:
				return '';
		}
	}

	function renderTimestampRow(def) {
		const opts = def.options
			.map(o => `<option value="${esc(o.value)}"${o.value === 'original' ? ' selected' : ''}>${esc(o.label)}</option>`)
			.join('\n\t\t\t\t');
		return `<div class="form-row">
			<div class="form-group">
				<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}</label>
				<select id="${def.id}" name="${def.id}">
					${opts}
				</select>
				<small>${esc(def.tooltip)}</small>
			</div>
			<div class="form-group" id="ir-association-timestamp-epoch-group" style="display: none;">
				<label for="ir-association-timestamp-epoch">Pinned Epoch</label>
				<input type="number" id="ir-association-timestamp-epoch" name="ir-association-timestamp-epoch" min="1" step="1" placeholder="1700000000" autocomplete="off">
				<small>Unix epoch stamped on every association event — the same number across every chunk of a multi-run replay, so the events dedupe.</small>
			</div>
		</div>`;
	}

	function renderIsUserIdBlock(def) {
		return `<div class="auth-toggle" id="ir-mode-toggle">
			<label class="toggle-label" title="A regex string — works in the UI, the CLI, and the module API.">
				<input type="radio" name="ir-isuserid-mode" id="ir-mode-regex" value="regex" checked>
				Regex (recommended)
			</label>
			<label class="toggle-label" title="A JavaScript predicate (id, record) => boolean — module API only; the CLI cannot express it.">
				<input type="radio" name="ir-isuserid-mode" id="ir-mode-function" value="function">
				JavaScript Function (module API)
			</label>
		</div>

		<div id="ir-regex-wrap">
			<div class="form-group">
				<label for="${def.id}" title="${esc(def.tooltip)}">${esc(def.label)}*</label>
				<input type="text" id="${def.id}" name="${def.id}" placeholder="^\\d+$" autocomplete="off" spellcheck="false" style="font-family: var(--font-mono);">
				<small>${esc(def.tooltip)}</small>
				<small id="ir-regex-error" class="error-text" style="display: none;"></small>
			</div>
			<div class="form-group">
				<label for="ir-sample-ids" title="Paste real distinct_ids from your project, one per line — the chips show how each one will be classified. Nothing is sent anywhere.">Test Against Real IDs</label>
				<textarea id="ir-sample-ids" name="ir-sample-ids" rows="4" placeholder="paste sample distinct_ids, one per line" spellcheck="false" style="font-family: var(--font-mono); font-size: 14px;"></textarea>
				<small>✓ matches your regex → becomes <code>$user_id</code> · ✗ no match → becomes a <code>$device:</code>-prefixed <code>$device_id</code>. Pure preview — nothing is sent anywhere.</small>
				<div id="ir-sample-results" style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px;"></div>
			</div>
		</div>

		<div id="ir-function-wrap" style="display: none;">
			<div class="form-group">
				<label for="ir-function-code">User ID Function*</label>
				<div id="ir-function-editor" style="display: none; height: 180px; border: 1px solid var(--border-primary); border-radius: 8px; overflow: hidden;"></div>
				<textarea id="ir-function-code" name="ir-function-code" rows="6" spellcheck="false" style="font-family: var(--font-mono); font-size: 13px; resize: vertical; tab-size: 2;"></textarea>
				<small>Receives <code>(id, record)</code> and must return <code>true</code> only for YOUR user IDs. Function mode is module-API only — the CLI preview cannot express it.</small>
			</div>
		</div>`;
	}

	// chunk pairable fields into .form-row pairs; full-width types flush on their own line
	function renderFieldList(defs) {
		const parts = [];
		let pending = [];
		const flushPending = () => {
			if (!pending.length) return;
			if (pending.length === 2) {
				parts.push(`<div class="form-row">\n${pending.join('\n')}\n</div>`);
			}
			else {
				parts.push(pending[0]);
			}
			pending = [];
		};
		for (const def of defs) {
			if (def.type === 'regex') {
				flushPending();
				parts.push(renderIsUserIdBlock(def));
			}
			else if (def.type === 'timestamp') {
				flushPending();
				parts.push(renderTimestampRow(def));
			}
			else if (def.type === 'toggle' || def.type === 'textarea') {
				flushPending();
				parts.push(renderField(def));
			}
			else {
				pending.push(renderField(def));
				if (pending.length === 2) flushPending();
			}
		}
		flushPending();
		return parts.join('\n');
	}

	function renderBody(page) {
		const coreDefs = fieldDefs.filter(d => d.section === 'core');
		const advancedDefs = fieldDefs.filter(d => d.section === 'advanced');

		const enableToggle = page === 'import'
			? `<div class="processing-option">
				<label class="checkbox-label" title="Translate original ID-merge identity verbs into the dual-ID association events simplified projects require.">
					<input type="checkbox" id="ir-enabled" name="ir-enabled">
					Enable Identity Replay
				</label>
				<div class="example">
					<code>{"event": "$merge", ...}</code> →
					<code class="after-transform">{"event": "identity association", "properties": {"$user_id": "...", "$device_id": "..."}}</code>
					<small>Simplified ID-merge projects hard-reject $identify / $create_alias / $merge — this rewrites them into association events the destination accepts</small>
				</div>
			</div>`
			: `<div class="form-group">
				<label class="checkbox-label" title="Translate original ID-merge identity verbs into the dual-ID association events simplified projects require.">
					<input type="checkbox" id="ir-enabled" name="ir-enabled">
					Enable Identity Replay
				</label>
				<small>Simplified ID-merge projects hard-reject $identify / $create_alias / $merge — this rewrites them into dual-ID association events the destination accepts.</small>
			</div>`;

		const transformNote = page === 'import'
			? `<p class="section-description" style="margin-top: 16px; margin-bottom: 0;">⚠️ Using a custom transform too? It must pass the synthetic <code>identity association</code> events through — returning <code>null</code> or <code>{}</code> for rows it does not recognize silently kills identity stitching.</p>`
			: '';

		return `${enableToggle}
		<div id="ir-fields" style="display: none;">
			${renderFieldList(coreDefs)}

			<div class="collapsible-section">
				<div class="section-header"
					onclick="toggleSection('ir-advanced-content')"
					aria-expanded="false"
					title="Rarely-needed knobs — the defaults are right for most migrations">
					<span class="section-icon">🎛️</span>
					<h3>Advanced</h3><span class="subtext">defaults are right for most migrations</span>
					<span class="toggle-icon">▼</span>
				</div>
				<div id="ir-advanced-content" class="collapsible-content" style="display: none;">
					${renderFieldList(advancedDefs)}
				</div>
			</div>
			${transformNote}
		</div>`;
	}

	function renderSection(containerEl, opts = {}) {
		if (!containerEl) return;
		state.page = opts.page === 'export' ? 'export' : 'import';
		const body = renderBody(state.page);

		if (state.page === 'import') {
			// native import-page pattern: .collapsible-section (import.html:435-443)
			containerEl.innerHTML = `<div class="collapsible-section" id="ir-section">
				<div class="section-header"
					onclick="toggleSection('ir-section-content')"
					aria-expanded="false"
					title="Translate original ID-merge identity verbs into simplified dual-ID association events">
					<span class="section-icon">🔀</span>
					<h3>Identity Replay</h3><span class="subtext">original → simplified ID merge migration</span>
					<span class="toggle-icon">▼</span>
				</div>
				<div id="ir-section-content" class="collapsible-content" style="display: none;">
					<p class="section-description">Migrating events into a simplified ID-merge project? This builds an identity graph from the stream, rewrites identity verbs into association events, and classifies bare distinct_ids — so the destination accepts everything.</p>
					${body}
				</div>
			</div>`;
		}
		else {
			// native export-page pattern: .form-group blocks under a subsection title
			containerEl.innerHTML = `<div id="ir-section">
				<h3 class="subsection-title">🔀 Identity Replay</h3>
				<p class="section-description">Migrating into a simplified ID-merge project? This builds an identity graph from the export stream, rewrites identity verbs into association events, and classifies bare distinct_ids — so the destination accepts everything.</p>
				${body}
			</div>`;
		}

		const functionCodeEl = document.getElementById('ir-function-code');
		if (functionCodeEl && !functionCodeEl.value) functionCodeEl.value = DEFAULT_FUNCTION_CODE;

		wireEvents();
		state.rendered = true;
		refresh();
	}

	// --------------------------------------------------------------------------
	// events + visibility sync
	// --------------------------------------------------------------------------
	function wireEvents() {
		const enabledEl = document.getElementById('ir-enabled');
		if (enabledEl) enabledEl.addEventListener('change', syncEnabledVisibility);

		const modeRegex = document.getElementById('ir-mode-regex');
		const modeFunction = document.getElementById('ir-mode-function');
		if (modeRegex) modeRegex.addEventListener('change', syncModeVisibility);
		if (modeFunction) modeFunction.addEventListener('change', syncModeVisibility);

		const regexEl = document.getElementById('ir-user-id-regex');
		if (regexEl) regexEl.addEventListener('input', renderSampleChips);

		const samplesEl = document.getElementById('ir-sample-ids');
		if (samplesEl) samplesEl.addEventListener('input', renderSampleChips);

		const tsEl = document.getElementById('ir-association-timestamp');
		if (tsEl) tsEl.addEventListener('change', syncTimestampVisibility);
	}

	function syncEnabledVisibility() {
		const fields = document.getElementById('ir-fields');
		if (fields) fields.style.display = checked('ir-enabled') ? 'block' : 'none';
	}

	function syncModeVisibility() {
		const functionMode = isFunctionMode();
		const regexWrap = document.getElementById('ir-regex-wrap');
		const functionWrap = document.getElementById('ir-function-wrap');
		if (regexWrap) regexWrap.style.display = functionMode ? 'none' : 'block';
		if (functionWrap) functionWrap.style.display = functionMode ? 'block' : 'none';
		if (functionMode) mountFunctionEditor();
	}

	function syncTimestampVisibility() {
		const epochGroup = document.getElementById('ir-association-timestamp-epoch-group');
		if (epochGroup) epochGroup.style.display = val('ir-association-timestamp') === 'epoch' ? 'block' : 'none';
	}

	// mount Monaco over the textarea IF the page already loaded it (import page only);
	// the textarea stays the source of truth — monaco edits sync back into it, so
	// sessionStorage persistence and collect() work identically in both modes
	function mountFunctionEditor() {
		if (state.monacoEditor) return;
		const host = document.getElementById('ir-function-editor');
		const textarea = document.getElementById('ir-function-code');
		if (!host || !textarea) return;
		if (state.page !== 'import' || typeof window.monaco === 'undefined' || !window.monaco.editor) return; // keep the textarea fallback

		try {
			host.style.display = 'block';
			textarea.style.display = 'none';
			const editor = window.monaco.editor.create(host, {
				value: textarea.value,
				language: 'javascript',
				theme: 'vs-dark',
				fontSize: 13,
				minimap: { enabled: false },
				scrollBeyondLastLine: false,
				automaticLayout: true,
				wordWrap: 'on'
			});
			editor.onDidChangeModelContent(() => {
				textarea.value = editor.getValue();
			});
			state.monacoEditor = editor;
		}
		catch (error) {
			console.warn('Identity Replay: Monaco mount failed, using textarea fallback', error);
			host.style.display = 'none';
			textarea.style.display = 'block';
		}
	}

	// regex live-tester: one chip per sample id, re-evaluated on every keystroke — pure client
	function renderSampleChips() {
		const out = document.getElementById('ir-sample-results');
		const errorEl = document.getElementById('ir-regex-error');
		if (!out) return;
		out.innerHTML = '';

		const regexStr = val('ir-user-id-regex').trim();
		let re = null;
		let regexError = null;
		if (regexStr) {
			try {
				re = new RegExp(regexStr);
			}
			catch (e) {
				regexError = e.message;
			}
		}

		if (errorEl) {
			errorEl.style.display = regexError ? 'block' : 'none';
			errorEl.textContent = regexError ? `Invalid regex: ${regexError}` : '';
		}

		const lines = val('ir-sample-ids')
			.split('\n')
			.map(s => s.trim())
			.filter(Boolean);
		if (!lines.length) return;

		if (!re) {
			const hint = document.createElement('span');
			hint.className = 'muted-text';
			hint.style.cssText = 'font-size: 13px;';
			hint.textContent = regexError ? 'fix the regex to test your samples' : 'enter a regex above to test your samples';
			out.appendChild(hint);
			return;
		}

		lines.slice(0, MAX_SAMPLE_CHIPS).forEach(id => {
			const match = re.test(id);
			const chip = document.createElement('span');
			chip.className = `ir-chip ${match ? 'success-text' : 'error-text'}`;
			chip.dataset.match = String(match);
			chip.title = match ? 'matches → becomes $user_id' : 'no match → becomes $device:-prefixed $device_id';
			chip.style.cssText = `font-family: var(--font-mono); font-size: 13px; padding: 2px 10px; border-radius: 12px; border: 1px solid ${match ? 'var(--success)' : 'var(--error)'}; background: ${match ? 'var(--success-bg)' : 'var(--error-bg)'};`;
			chip.textContent = `${match ? '✓' : '✗'} ${id}`;
			out.appendChild(chip);
		});
		if (lines.length > MAX_SAMPLE_CHIPS) {
			const more = document.createElement('span');
			more.className = 'muted-text';
			more.style.cssText = 'font-size: 13px;';
			more.textContent = `+${lines.length - MAX_SAMPLE_CHIPS} more not shown`;
			out.appendChild(more);
		}
	}

	// re-sync all conditional visibility from current control state — call after
	// sessionStorage form restore (checkbox restores do not dispatch change events)
	function refresh() {
		syncEnabledVisibility();
		syncModeVisibility();
		syncTimestampVisibility();
		renderSampleChips();
	}

	// --------------------------------------------------------------------------
	// collect() — the nested identityReplay object per the wire contract, or null
	// --------------------------------------------------------------------------
	function collect() {
		if (!checked('ir-enabled')) return null;

		const ir = {};

		// isUserId: function mode wins and clears regex — never send both
		if (isFunctionMode()) {
			const code = getFunctionCode().trim();
			if (code) ir.isUserIdCode = code;
		}
		else {
			ir.isUserId = val('ir-user-id-regex').trim();
		}

		// associationTimestamp: 'original' is the default (omitted); number input → epoch
		const tsMode = val('ir-association-timestamp', 'original');
		if (tsMode === 'floor') {
			ir.associationTimestamp = 'floor';
		}
		else if (tsMode === 'epoch') {
			const epoch = Number(val('ir-association-timestamp-epoch'));
			if (Number.isFinite(epoch) && epoch > 0) ir.associationTimestamp = epoch;
		}

		// everything else is def-driven; default-valued keys are omitted
		for (const def of fieldDefs) {
			if (def.type === 'regex' || def.type === 'timestamp') continue;
			const el = document.getElementById(def.id);
			if (!el) continue;

			if (def.type === 'toggle') {
				if (el.checked !== def.default) ir[def.key] = el.checked;
			}
			else if (def.parse === 'list') {
				const list = parseList(el.value);
				if (list.length) ir[def.key] = list;
			}
			else if (def.type === 'number') {
				const raw = String(el.value).trim();
				if (raw === '') continue;
				const num = Number(raw);
				if (Number.isFinite(num) && num !== def.default) ir[def.key] = num;
			}
			else {
				const value = String(el.value).trim();
				if (value && value !== def.default) ir[def.key] = value;
			}
		}

		return ir;
	}

	// --------------------------------------------------------------------------
	// validate() — {ok, errors[]}; cheap client-side wins before job.js throws
	// --------------------------------------------------------------------------
	function validate() {
		const errors = [];
		if (!checked('ir-enabled')) return { ok: true, errors };

		if (isFunctionMode()) {
			if (!getFunctionCode().trim()) {
				errors.push('Identity Replay: function mode is selected but the code editor is empty.');
			}
		}
		else {
			const regexStr = val('ir-user-id-regex').trim();
			if (!regexStr) {
				errors.push('Identity Replay: a user ID regex is required — it is the one thing the tool cannot infer.');
			}
			else {
				try {
					new RegExp(regexStr);
				}
				catch (e) {
					errors.push(`Identity Replay: the user ID regex does not compile: ${e.message}`);
				}
			}
		}

		const rateRaw = val('ir-min-association-rate').trim();
		if (rateRaw !== '') {
			const rate = Number(rateRaw);
			if (!Number.isFinite(rate) || rate < 0 || rate > 1) {
				errors.push('Identity Replay: minimum association rate must be a number between 0 and 1.');
			}
		}

		if (val('ir-association-timestamp') === 'epoch') {
			const epoch = Number(val('ir-association-timestamp-epoch'));
			if (!Number.isFinite(epoch) || epoch <= 0) {
				errors.push('Identity Replay: a pinned association timestamp must be a positive epoch number.');
			}
		}

		return { ok: errors.length === 0, errors };
	}

	// --------------------------------------------------------------------------
	// cliFlags() — fragments for the live CLI preview; only 4 real flags exist
	// (components/cli.js: --identity-replay, --ir-user-id-regex, --ir-graph-path,
	// --ir-on-ambiguous) — anything else gets the module-API comment fragment
	// --------------------------------------------------------------------------
	function cliFlags() {
		if (!checked('ir-enabled')) return [];

		const ir = collect() || {};
		const flags = ['--identity-replay'];
		const functionMode = isFunctionMode();

		if (!functionMode) {
			flags.push(`--ir-user-id-regex ${shellQuote(ir.isUserId || '<your-user-id-regex>')}`);
		}
		if (ir.graphPath) flags.push(`--ir-graph-path ${shellQuote(ir.graphPath)}`);
		if (ir.onAmbiguous) flags.push(`--ir-on-ambiguous ${ir.onAmbiguous}`);

		const moduleOnly = functionMode || Object.keys(ir).some(key => !CLI_EXPRESSIBLE_KEYS.includes(key));
		if (moduleOnly) flags.push('# identityReplay: some options require the module API');

		return flags;
	}

	// --------------------------------------------------------------------------
	// public API
	// --------------------------------------------------------------------------
	window.IdentityReplayUI = {
		fieldDefs,
		renderSection,
		collect,
		validate,
		cliFlags,
		refresh
	};
})();
