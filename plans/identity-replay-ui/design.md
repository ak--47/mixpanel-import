# Build Contract: identityReplay in the Web UI (v3.6.1)

Decisions (AK): both pages · regex field + live tester + optional function editor ·
core + advanced accordion · records-preview only (no telemetry card).

## Wire contract (client → server)

The client composes ONE nested object and includes it in the existing options/form JSON
**only when the section's enable toggle is on**:

```js
identityReplay: {
  isUserId: '<regex string>',        // regex mode (default)
  isUserIdCode: '(id) => ...',       // function mode: source string; wins over isUserId
  electionScope, onAmbiguous, associationTimestamp,  // number input → epoch; else 'original'|'floor'
  graphPath, minAssociationRate,
  // advanced accordion values only when changed from defaults:
  graph, maxGraphSize, onGraphOverflow, identityEvents, associationEventName,
  bareDistinctId, userIdFallbackProps, denylist, scrubJunkIds, scrubExportProps
}
```

Rules: omit unset/default keys; never send both isUserId and isUserIdCode (function mode
clears regex); `associationProps` NOT exposed in UI (module-only, YAGNI).

## Server (ui/server.js) — 6 edits

1. `compileIdentityReplayOpts(opts, logger)` helper: if `opts.identityReplay?.isUserIdCode`
   is a non-empty string → `opts.identityReplay.isUserId = eval('(' + code + ')')` in
   try/catch (throw a clear job-error on syntax error), `delete isUserIdCode`. Mirrors the
   existing transformCode eval sites exactly (same trust model — the UI user runs their own
   server; do NOT invent a sandbox).
2. Call the helper at ALL FIVE option-ingestion sites: WS `executeJobOverWebSocket` (~:154),
   POST /job (~:776), POST /dry-run (~:1233), POST /export (after edit 3), POST
   /export-dry-run (after edit 4).
3. POST /export opts construction (~:1443-1469): `if (exportData.identityReplay)
   opts.identityReplay = exportData.identityReplay;` AND fix the pre-existing drop:
   `if (exportData.secondRegion) opts.secondRegion = exportData.secondRegion;`
4. POST /export-dry-run opts (~:1763-1778): same two additions.
5. `filterResultForClient` allowedFields (~:419-425): add `'identityReplay'` (telemetry
   rides the existing results JSON view — no new card, per AK).
6. POST /dry-run raw pass (~:1279-1287): `rawOpts.identityReplay = null;` next to the
   existing `rawOpts.transformFunc = null` (otherwise the graph runs twice and the "raw"
   preview shows replayed data).

## Shared front-end module: ui/public/identity-replay-ui.js (NEW)

Both pages load it via `<script>`. Exposes `window.IdentityReplayUI`:

- `fieldDefs` — one array of {key, label, type, default, options?, tooltip} driving ALL
  rendering: core = isUserId(+mode toggle), electionScope, onAmbiguous, associationTimestamp,
  graphPath, minAssociationRate; advanced = the rest (minus associationProps). Tooltips are
  the adoption surface — write them from the README option table (plain language, one or two
  sentences, mention defaults).
- `renderSection(containerEl, {page: 'import'|'export'})` — builds the section DOM: enable
  toggle → core fields → "Advanced" sub-accordion → regex live-tester (textarea for sample
  ids, one per line; renders ✓/✗ chips per id against the current regex — pure client, no
  network) → function-mode toggle revealing a code input (import page: Monaco via the page's
  existing loader/fallback shim; export page: a monospace <textarea> — Monaco is not loaded
  there and one predicate does not justify it). Uses the page's native section markup:
  import = .collapsible-section pattern (import.html:435-443), export = field-group pattern.
- `collect()` — returns the nested identityReplay object per the wire contract, or null
  when disabled.
- `validate()` — returns {ok, errors[]}: regex compiles (new RegExp try/catch), function
  mode has non-empty code, minAssociationRate ∈ [0,1], numeric associationTimestamp > 0.
- `cliFlags()` — array of CLI fragments for the preview: `--identity-replay
  --ir-user-id-regex '<re>' [--ir-graph-path <p>] [--ir-on-ambiguous <v>]`; in function
  mode or when module-only options are set, still emit the expressible flags and append
  `# identityReplay: some options require the module API` as a trailing comment fragment
  (both pages' previews render plain text).

## Import page (import.html + import.js)

- Insert the section container after the transform section; `IdentityReplayUI.renderSection`.
- Visibility: only when recordType === 'event' (updateFieldVisibility, directive-row
  precedent import.js:1006).
- BOTH collectors gain the object: `collectOptions()` (:1560) and `collectFormData()`
  (:1681) — `const ir = IdentityReplayUI.collect(); if (ir) options.identityReplay = ir;`
- `updateCLICommand()` (:1066): append `IdentityReplayUI.cliFlags()`.
- Submit-path validation: call `IdentityReplayUI.validate()` alongside existing checks.

## Export page (export.html + export.js)

- New `identityReplay-group` after the secondToken/secondRegion groups; add to the
  allGroups hide-list (export.js:661-665) AND show it ONLY in the export-import-event case
  (:719) — job.js throws for export-import-profile/group.
- `collectFormData()` (:1163): attach `identityReplay` object (nested value in the flat
  JSON body is fine).
- `updateCLICommand()` (:939): append cliFlags.
- Client validation (pattern :853-907): run validate() before submit — dry runs currently
  skip validation; run it there too for a friendly error instead of a server 500.
- sessionStorage persistence: verify restored state re-triggers section visibility.

## Docs + version

- package.json → 3.6.1; CHANGELOG 3.6.1 entry (UI support, secondRegion export fix,
  response-filter addition).
- docs/web-ui.md: response-filtering table gains identityReplay AND the five stale
  resilience keys the code already includes (stallsDetected, resumesAttempted,
  resumesSucceeded, filesSkippedMissing, bytesResumed); one paragraph on the UI section.
- README UI feature bullets: mention identity replay configurable from both tools.

## Verification (orchestrator, post-build)

Playwright against `npm start`: import page — enable section, bad regex shows validation,
regex tester chips work, CLI preview shows --ir flags, upload small torture JSONL +
dry-run shows rewritten `$device:` rows + `identity association` records; export page —
section appears only for export-import-event, hidden for others. No jest UI suite exists;
library suites must stay green (no components/ changes in this sprint).
