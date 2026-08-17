# Recon: import.html + import.js (E.T.L front-end) — identityReplay UI slice

## Summary

The import tool is a single 1108-line HTML form (`ui/public/import.html`) driven by one class,
`MixpanelImportUI`, in `ui/public/import.js` (3285 lines, `// @ts-nocheck`, vanilla JS, no framework).
Everything a new option section needs already has an exact pattern to copy:

- **Option sections** are `.collapsible-section` blocks inside the "⚙️ Import Configuration" card
  (import.html:335-926), toggled by the global `toggleSection(id)` (import.js:3232).
- **Options collection** happens in **two places** that must both be touched:
  `collectOptions()` (import.js:1560, the WebSocket/real-job path) and `collectFormData()`
  (import.js:1681, the dry-run//sample//columns path — it builds its *own reduced* options object).
- **CLI preview** is `updateCLICommand()` (import.js:1066) — flag-by-flag string building, fired on
  every form `input`/`change` (import.js:832-833).
- **Monaco** transform code goes up as raw source text (`this.editor.getValue()`) in the
  `start_job` WS message (import.js:47, 62-69); the server `eval`s it (ui/server.js:154).
- **Results** render as syntax-highlighted JSON of the (server-filtered) result object
  (`showResults`, import.js:2144). identityReplay telemetry will need a server allowlist change
  (cross-slice: ui/server.js:418 `filterResultForClient`) before it can appear here.
- Backend needs **zero changes to accept the option itself**: the server does
  `JSON.parse(options)` and passes through (ui/server.js:124), and
  `components/identity-replay.js` `normalizeOptions()` (line 61-90) compiles a **regex string**
  `isUserId` into a predicate — so the UI just adds
  `options.identityReplay = { isUserId: '<regex string>', ... }`.

## Details

### Page structure: how an option section is built

Grid of cards: `<form id="importForm" class="import-form">` contains four `<section class="card">`
blocks — Data Files (import.html:65), Credentials (:191), Import Configuration (:335), Transform
(:929) — then the CLI Command card (:972), the `.actions` button row (:992), the loading overlay
(:1016), and the results div (:1029).

**Collapsible section skeleton** (the exact pattern to imitate — this is the "Processing" section,
import.html:435-443):

```html
<div class="collapsible-section">
    <div class="section-header"
        onclick="toggleSection('data-processing-section')"
        aria-expanded="false"
        title="tooltip text shown on hover">
        <span class="section-icon">🔄</span>
        <h3>Processing</h3><span class="subtext">macros + transforms for fixing data</span>
        <span class="toggle-icon">▼</span>
    </div>
    <div id="data-processing-section" class="collapsible-content" style="display: none;">
        ...content...
    </div>
</div>
```

- `toggleSection(sectionId)` is a **global function** (import.js:3232) — flips
  `style.display` between `none`/`block` and sets `aria-expanded` on the header.
- `toggleAllSections()` (import.js:3248) hits every `.collapsible-content`, so a new section
  participates in Expand All for free.
- Sections live inside `#import-config-content` (import.html:342), which is itself collapsible
  but starts `display: block`.
- Existing sections in order: Column Mapping (`column-mapper-wrapper`), Processing
  (`data-processing-section`), Filtering (`filtering-section`), Time & Date (`time-section`),
  Performance (`performance-section`), Meta (`expert-section`). An "Identity Replay" section slots
  naturally between Processing and Filtering (or after Filtering).

**Form field patterns inside a section:**

- Two-up rows: `<div class="form-row">` containing two `<div class="form-group">` (import.html:349).
- A field: `<label for="id">Label</label>` + `<input id="id" name="id" autocomplete="off">` +
  `<small>help text</small>` (e.g. timeOffset, import.html:561-570).
- Checkboxes with worked examples use `.processing-option` > `.checkbox-label` > `.example`
  (import.html:448-461) — the `.example` div holds `<code>before</code> → <code>after</code>
  <small>explanation</small>`. This is the best pattern for identityReplay toggles because it
  carries a before/after illustration.
- Plain checkbox grids: `<div class="checkbox-grid">` of `.checkbox-label`s (import.html:890).
- Select with explanatory option labels: the `directive` dropdown (import.html:582-596) — note it
  sits in a `form-row` with `id="directive-row"` and `style="display: none"`, shown only for
  matching record types (see conditional visibility below).

### Tooltip / help-text pattern

Three mechanisms, all in use:

1. `<small>` under the input inside `.form-group` — primary help text (styled at style.css:233).
2. `title="..."` attribute on `.section-header` (import.html:439) or on a `<label>`
   (import.html:757) — hover tooltip.
3. `.example` blocks with before → after `<code>` snippets under checkboxes (import.html:456-460;
   CSS at style.css:424-441, `.example code.after-transform` variant exists).

There is no JS tooltip library; do not add one.

### Conditional visibility by record type (identityReplay requires recordType 'event')

`updateFieldVisibility()` (import.js:992-1028) runs on every `recordType` change
(listener at import.js:816-819). Pattern for the directive row (import.js:1006-1009):

```js
const directiveRow = document.getElementById('directive-row');
if (directiveRow) {
    directiveRow.style.display = (recordType === 'user' || recordType === 'group') ? 'block' : 'none';
}
```

identityReplay should copy this: show its section only when `recordType === 'event'`
(the UI's recordType select offers only `event`/`user`/`group`, import.html:196-204 —
`export-import-event` is on the export page, out of this slice). `job.js:367-372` throws for
non-event record types and for missing `isUserId`, so the UI must gate both.

### Monaco transform editor wiring

- Container: `<div id="monaco-editor" class="code-editor">` inside the Transform card
  (import.html:943).
- Init: `initializeMonacoEditor()` (import.js:1399) lazy-loads the AMD loader from cdnjs
  (0.44.0), then `createMonacoEditor()` (import.js:1456) makes the editor with
  `this.getDefaultTransformFunction()` (import.js:1440) as the starting value. On CDN failure,
  `createFallbackEditor()` (import.js:1469) substitutes a `<textarea>` wrapped in a
  `{getValue, setValue}` shim — **always access code via `this.editor.getValue()`**, never
  Monaco APIs directly.
- Code storage: only in the editor + sessionStorage (`saveFormState()` persists it under key
  `'monaco-editor'` if it differs from the default, import.js:417-423).
- Path to start_job: `executeJobViaWebSocket()` sends
  `transformCode: this.editor ? this.editor.getValue() : null` in the `start_job` message
  (import.js:47, 68). Server side: `opts.transformFunc = eval('(' + transformCode + ')')`
  (ui/server.js:154). The HTTP/dry-run path attaches it via
  `formData.append('transformCode', transformCode)` in `collectFormData()` (import.js:1812-1815),
  skipped when unchanged from default.
- README gotcha relevant to UI copy: `transformFunc` sees identityReplay's **synthetic association
  events too** — a user transform returning `{}`/null for unrecognized rows silently kills identity
  stitching. Worth a `<small>` warning in the new section or in the editor-help block
  (import.html:945-953).

### Options collection — the exact functions

**`collectOptions()` (import.js:1560-1679)** — used by the WebSocket job path (import.js:46) and by
`generateSnowcatJob()` (import.js:3026). Builds the options object from form fields with the
established idioms:

- checkbox → `if (this.getElementChecked('fixTime')) options.fixTime = true;` (import.js:1634)
- text → `const x = this.getElementValue('id'); if (x) options.x = ...` (import.js:1596-1601)
- JSON-typed text field → `try { options.tags = JSON.parse(tags) } catch { console.warn }`
  (import.js:1644-1650) — this is the pattern for `associationProps`.
- Helper accessors: `getElementValue(id, default)` (import.js:270), `getElementChecked(id)`
  (import.js:276) — both null-safe.

For identityReplay, append a block like:

```js
if (this.getElementChecked('identityReplayEnabled') && recordType === 'event') {
    const ir = { isUserId: this.getElementValue('irUserIdRegex') };
    // + optional scalars: onAmbiguous, associationEventName, associationTimestamp,
    //   identityEvents, electionScope, graphPath, minAssociationRate, graph:false, ...
    options.identityReplay = ir;
}
```

`isUserId` as a **string regex is fully supported downstream**: server passes options through
(`JSON.parse`, ui/server.js:124), `components/identity-replay.js` `normalizeOptions()`
compiles string → RegExp with a clear error on invalid regex (identity-replay.js:80-89).

**`collectFormData()` (import.js:1681-1818)** — used by `/dry-run`, `/sample`, `/columns`, and the
legacy `/job` path. **It does NOT call `collectOptions()`** — it builds its own *reduced* options
object (import.js:1780-1807: recordType, workers, recordsPerBatch, region, compress, fixData,
strict, verbose, directive, vendor, aliases, showProgress only). identityReplay must be added here
too or **dry runs won't exercise the replay** — and dry-run is the README-recommended way to preview
this feature. (Alternatively refactor collectFormData to spread `...this.collectOptions()`, but
that changes dry-run behavior for every extended option — bigger blast radius, separate decision.)

### Live CLI-command preview

`updateCLICommand()` (import.js:1066-1337), output to `<pre id="cli-command">` (import.html:986).
Fired by form-level `input`/`change` listeners (import.js:832-833) — a new section's fields get
live preview for free as long as they're inside `#importForm`.

Patterns to imitate:

- boolean flag table: `booleanFlags = [['fixData', 'fix'], ...]` → `--flag` when checked
  (import.js:1158-1178)
- text option table: `textOptions` (import.js:1207-1229); JSON-ish values are single-quoted
  (`--tags '{"a":1}'`, import.js:1223-1224)
- conditional-by-recordType flag: the `--directive` block (import.js:1194-1198)

The real CLI flags to emit (components/cli.js:390-415, composition at cli.js:435-447):
`--identity-replay`, `--ir-user-id-regex '<regex>'`, `--ir-graph-path <path>`,
`--ir-on-ambiguous <drop|resolve|error>`. **Only those four exist on the CLI** — the other
identityReplay options (associationTimestamp, electionScope, associationProps, …) are module-only,
so the CLI preview can only reflect the enable-toggle + regex + graphPath + onAmbiguous; if the UI
exposes module-only scalars, the CLI preview cannot represent them (either omit them from the
preview or add CLI flags — out of scope here). cli.js also enforces `--identity-replay` requires
`--ir-user-id-regex` (cli.js:423) and rejects orphan `--ir-*` flags (cli.js:427). The quote style
for the regex should be single quotes (`--ir-user-id-regex '^\d+$'` per README).

### Dry-run flow and preview rendering

- Trigger: `#dry-run-btn` → `submitJob(true)` (import.js:766-769).
- `submitJob(isDryRun=true)` skips credential validation (import.js:1997), POSTs
  `collectFormData()` to `/dry-run` (import.js:2014-2031).
- Server returns `{ success, previewData, rawData }` (ui/server.js:1307-1308 — two dry runs, raw +
  transformed).
- `showResults(result, true)` (import.js:2144) → when both `rawData` and `previewData` exist,
  `showSideBySideComparison()` (import.js:2178) renders paginated raw-vs-transformed panels with
  scroll sync. **This comparison is where a user would verify verb rewriting** — association events
  appear in the transformed panel if identityReplay runs during dry-run (which requires the
  collectFormData change above; also note server caps dry runs at 100 records, so the closure flush
  operates on a tiny graph — telemetry numbers will be small but the rewrite is visible).

### Job execution + results/telemetry rendering

- Real jobs: local files → POST `/job/prepare` for a jobId, then `executeJobViaWebSocket(jobId, ...)`
  (import.js:2040-2061); cloud paths → straight to WebSocket with a client-generated jobId
  (import.js:2036-2038, 27-102).
- Progress: `handleWebSocketMessage` (import.js:152) → `updateProgressDisplay` (import.js:189)
  renders `.progress-stats` stat-items (Duration/Records/Requests/EPS/Memory) into
  `.loading-details`. Progress payload comes from the server's progress callback — no
  identityReplay data flows here.
- Completion: `job-complete` → `showResults(data.result, false)` (import.js:165-172) → dumps the
  whole filtered result object as highlighted JSON into `#results-data` via `highlightJSON()`
  (import.js:2394). So **telemetry rendering is automatic** once the server includes the
  `identityReplay` block — no bespoke front-end telemetry widget is required (though a dedicated
  stats panel could be added by imitating `.progress-stats`).
- **Cross-slice blocker:** `filterResultForClient()` (ui/server.js:418-435) allowlists result keys;
  `identityReplay` is **not** in the list, so telemetry is currently stripped before reaching the
  browser. The server slice must add `'identityReplay'` to `allowedFields`. (docs/web-ui.md's
  Response Filtering section will need the same doc update.)
- `this.lastResults` powers the Download JSON button (import.js:1365-1397).

### Vendor picker pattern (identityReplay interacts with vendor)

- Vendor is a plain `<select id="vendor">` in the Core Settings form-row (import.html:361-374),
  collected as `if (vendor) options.vendor = vendor;` (import.js:1584-1585), CLI as
  `--vendor x` (import.js:1153-1155). `vendorOpts` is a JSON text input in the Meta section
  (import.html:879-886).
- There is **no existing UI cross-option validation** between vendor and anything else — the
  enable/disable choreography for identityReplay (event-type-only) will be the first of its kind on
  this page; the closest precedent is `updateFieldVisibility()`'s recordType-driven show/hide.
- Pipeline ordering fact for help text: the identity-replay stage runs inside the normal pipeline
  (pipelines.js:688-690) and is skipped in fastMode (job.js throws on the combo anyway). The UI has
  no fastMode control, so no UI guard is needed for that; `v2_compat` is likewise not exposed in
  this UI.

### style.css conventions for a new section

Dark theme, CSS custom properties at style.css:10-33: backgrounds `--bg-primary/secondary/tertiary/
card/elevated`, text `--text-primary/secondary/muted/accent`, borders `--border-primary/secondary/
focus`, status `--success`, `--error` (+ `-bg` translucent variants), brand `--mp-purple`.

Classes a new section reuses (no new CSS should be needed):

| Class | Defined at | Use |
|---|---|---|
| `.collapsible-section` | style.css:308 | outer wrapper, bordered rounded box |
| `.section-header` (+ `h3`, `.section-icon`, `.subtext`, `.toggle-icon`) | style.css:316-387 | clickable header row |
| `.collapsible-content` | (display toggled inline) | body, starts `style="display: none"` |
| `.form-row` / `.form-group` | style.css:186 / :182 | 2-col rows / field stacks |
| `.checkbox-label` | style.css:245 | checkbox + text pill, hover bg |
| `.processing-option` + `.example` | style.css:411 / :424 | checkbox with before→after code demo |
| `.checkbox-grid` | style.css:288 | grid of plain checkboxes |
| `.section-description` | style.css:174 | muted intro paragraph |
| `small` under inputs | style.css:233 | help text |

Responsive overrides for `.processing-option`/`.example` exist at style.css:1085/:1103 — nothing
to do if the standard classes are used. Emoji serve as icons everywhere (no icon font); pick one
for the section header (e.g. 🔀, matching the README section's emoji).

### Session persistence

`saveFormState()`/`loadFormState()` (import.js:389-483) persist all `input[type=text|password|
date|number]`, `select`, `textarea` **by element id**, checkboxes as `checkbox-<id>`, radios as
`radio-<name>` into sessionStorage. New fields participate automatically **if they have ids** and
live inside `#importForm`. `resetForm()` (import.js:325) uses `form.reset()` — give new inputs
proper `value`/`checked` HTML defaults so reset restores them.

## Gotchas

1. **Two options collectors.** `collectOptions()` (WS jobs + Snowcat) and `collectFormData()`
   (dry-run/sample/columns/legacy) build options independently. identityReplay must be added to
   both, or dry-run — the flagship preview flow for this feature — won't run the replay.
2. **Telemetry is server-filtered out today.** `filterResultForClient` (ui/server.js:418) has no
   `identityReplay` key; without the server-slice change the results panel and the tracked
   `import completed` event will never show replay telemetry. Front-end work should assume the key
   arrives at `data.result.identityReplay`.
3. **`job.js` throws, WS reports it as job-error.** Enabling identityReplay with
   recordType user/group or without `isUserId` throws in the JobConfig constructor
   (job.js:367-372) — the UI should prevent this client-side (hide section for non-event, require
   the regex field when the toggle is on, mirroring `validateRequiredFields()` at import.js:1030).
4. **CLI preview can't express module-only options.** Only `--identity-replay`,
   `--ir-user-id-regex`, `--ir-graph-path`, `--ir-on-ambiguous` exist (cli.js:390-415). If the UI
   exposes more (associationTimestamp, electionScope, associationProps…), the CLI preview will
   under-represent the job. Decide: limit UI fields to the CLI four, or accept the mismatch.
5. **Regex strings survive the JSON round-trip; functions don't.** The UI must send `isUserId` as
   a plain string (compiled server-side by identity-replay.js:80-89). Don't try to route it through
   the Monaco transform or eval path.
6. **Invalid regex fails at job start, not at typing time.** `new RegExp(str)` throws inside
   normalizeOptions → job-error over WS. Cheap client-side win: `try { new RegExp(v) } catch` on
   input and show inline error before submit.
7. **`updateCLICommand()` reads many non-existent ids** (secret, bearer, start, end, scdType…)
   guarded by null checks — copied from export.js. Follow the same null-guarded style; don't assume
   an element exists.
8. **Duplicate `id="preview-content"`** exists in import.html (:175 and :965) — `getElementById`
   grabs the first. Don't add a third; use unique ids for anything new.
9. **Snowcat path inherits `collectOptions()`** (import.js:3026) — an enabled identityReplay will
   flow into Snowcat job JSON automatically. Probably desirable, but the Snowcat queuer must
   tolerate the extra key; the modal's editable JSON lets users delete it either way.
10. **`checkbox-label` state restoration quirk:** `loadFormState` restores checkbox state but does
    not dispatch `change` events for checkboxes (only radios get `dispatchEvent`), so any
    show/hide logic keyed to the identityReplay enable-toggle must also run once after
    `loadFormState()` (constructor order: `setupEventListeners()` ends with `loadFormState()`,
    import.js:878).
11. **Transform + replay interaction warning belongs in the UI copy:** user transforms that drop
    unrecognized rows will drop synthetic association events (README gotcha). Put a `<small>`
    note in the new section and/or the editor-help list.
12. **transport select default mismatch:** the UI treats `got` as default (import.js:1670,
    `!== 'got'`), but the library default is `undici` since v3.x. Pre-existing inconsistency —
    don't imitate it for new fields; compare against the real library default.
