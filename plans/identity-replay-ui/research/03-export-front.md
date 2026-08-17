# Recon: L.T.E Export Front-End (export.html + export.js) for identityReplay UI

Slice 03 of the identity-replay-ui sprint. Maps the export-tool front-end so the identityReplay
section (v3.6.1) can be slotted into the export-import flow.

## Summary

- The export page is a single static form (`ui/public/export.html`, 717 lines) driven by one class,
  `MixpanelExportUI` in `ui/public/export.js` (2148 lines), instantiated at the bottom and exposed
  as `window.app` (export.js:2148-2149). No build step, no framework, no Monaco, no dropzone use
  (the dropzone CSS/JS is loaded in `<head>` but unused on this page).
- **Everything is a flat JSON POST.** `collectFormData()` (export.js:1163-1221) reads every field
  by element id into one flat object, deletes empty keys, and POSTs it to `/export` (real run) or
  `/export-dry-run` (dry run) — see `submitExport()` (export.js:1082-1161). There is no multipart,
  no `/job/prepare`, no `start_job` WebSocket message: the WebSocket is used *only* to subscribe to
  progress (`register-job`, export.js:14-53) after the server returns a `jobId`.
- **export-import-* is already fully wired** front and back: record-type options
  (export.html:224-226), field visibility (export.js:719-758), validation (export.js:853-907),
  source creds + `secondToken`/`secondRegion` destination fields (export.html:326-394), and the
  non-file-producing async branch in `ui/server.js` (/export, lines 1647-1731).
- **The natural identityReplay slot** is a new conditional group shown only for
  `export-import-event`, following the exact `secondToken-group` pattern: hidden div in
  export.html's credentials card (or its own card), id added to the `allGroups` hide-list
  (export.js:661-665), `style.display = 'block'` added inside the `case 'export-import-event':`
  branch of `updateFieldVisibility()` (export.js:719-730), fields read in `collectFormData()`,
  flags appended in `updateCLICommand()`, and — critically — **whitelisted server-side** (see
  Gotchas: the server drops unknown fields).

## Details

### Page structure (export.html) vs the import page

Order of sections inside `<form id="exportForm">`:

1. **Export Destination** card (export.html:64-202) — radio toggle `destinationType`
   (local/gcs/s3) with three swap-in divs `local-destination`, `gcs-destination`,
   `s3-destination`; toggled by `toggleDestinationType()` (export.js:625-653).
2. **Type of Export** card (export.html:205-230) — the `recordType` `<select>`. Live options:
   `export`, `profile-export`, `group-export`, `export-import-event`, `export-import-profile`,
   `export-import-group` (lines 215-226). `profile-delete`, `group-delete`, `scd`, annotations
   types exist but are **commented out** (lines 217-223).
3. **Credentials** card `#credentials-section` (export.html:232-395) — hidden until a recordType
   is chosen. Contains every credential group; visibility is per-recordType.
4. **Export Configuration** card (export.html:397-582) — Core Settings (workers, source `region`)
   plus three collapsible sections: Time & Date Filters (`time-section`, start/end/epochStart/
   epochEnd), Advanced Filtering (`filtering-section`, whereClause/limit), Output Options
   (`output-section`, logs/verbose/showProgress/compress checkboxes).
5. **CLI Command Preview** card (export.html:584-602) — `<pre id="cli-command">` + copy button.
6. **Actions** (export.html:604-632) — Dry Run, Test Run (1000 records), Start Export, and a
   conditionally shown Snowcat button.
7. Loading overlay, results div, Snowcat modal, GCS browse modal (export.html:634-710).

Differences from the import page (`import.html` / `import.js`):

- No file upload / dropzone section (import's first card is "📁 Data Files", import.html:66);
  export's first card is the *destination*.
- No Monaco transform editor (import.html:933 "🔧 Data Transform"), no field mapper, no vendor
  select, no `/sample` / `/columns` preview endpoints.
- Import uses the hybrid `/job/prepare` + WebSocket `start_job` flow; export uses plain
  `POST /export` returning `jobId` then `register-job` over the socket.
- Both share `style.css`, the collapsible-section helpers (`toggleSection` /
  `toggleAllSections`, export.js:2095-2145, duplicated in import.js), the CLI-preview card
  pattern, sessionStorage form persistence, and the dev-key 🔑 / reset 🔄 header buttons.

### How export-import-event / export-import-profile are configured

**Fields shown** (`updateFieldVisibility()`, export.js:655-775). For `export-import-event`
(case at 719-730) and `export-import-profile` (case at 732-743) the same set is displayed:

- `project-group` — source Project ID (export.html:241-251)
- `token-group` — "Project Token", small-text says "Required for re-import operations
  (destination project)" (export.html:326-337)
- `auth-toggle` + `service-auth` — service acct vs API secret radio; `toggleAuthMethod()`
  (export.js:606-623) swaps `service-auth`/`secret-auth` and hides `project-group` when secret
  is chosen
- Destination sub-header: `destination-title` ("Destination Project (Optional)") +
  `destination-description` ("Leave empty to reimport into the same project")
  (export.html:363-369)
- `secondToken-group` — Destination Project Token (export.html:371-381)
- `secondRegion-group` — Destination Data Residency select US/EU/IN (export.html:383-394)

`export-import-group` (case at 745-758) adds `groupKey-group` + `dataGroupId-group` on top.

**Validation** (`validateRequiredFields()`, export.js:777-937): for `export-import-event` /
`export-import-profile` (853-875) — project ID always required, then acct+pass (service) or
secret. `export-import-group` (877-907) additionally requires groupKey + dataGroupId. Cloud
destination paths validated at 913-934. Validation is **skipped for dry runs**
(export.js:1085-1093).

**Collection** (`collectFormData()`, export.js:1163-1221): flat object with `recordType`,
creds (`project`, `token`, `secret`, `acct`, `pass`, `groupKey`, `dataGroupId`, `secondToken`,
`secondRegion`), config (`region`, `workers`), time filters, `whereClause`/`limit`, output
booleans, and cloud destination fields. Empty strings/null are deleted (1213-1218).

**Server side** (`ui/server.js` POST `/export`, 1416-1741): builds `creds` (1422-1431, includes
`secondToken`) and `opts` (1443-1469) from an **explicit field whitelist**, then calls
`mixpanelImport(creds, null, opts)`. export-import-* types fall into the "non-file-producing"
`else` branch (1647-1731): responds `{success, jobId}` immediately, runs async, wires
`opts.progressCallback = createProgressCallback(jobId)`, signals `job-complete` via
`signalJobComplete()` → `filterResultForClient()` (418-435).

### How export dry-run works

- Front-end: Dry Run button → `submitExport(true)` (export.js:530-533); Test Run button →
  `submitExport(false, 1000)` which clamps `formData.limit` (1116-1118). Dry run POSTs to
  `/export-dry-run` and renders the JSON response synchronously via `showResults(result, true)`
  (1133-1139) — no WebSocket, no jobId.
- Server (`/export-dry-run`, server.js:1745-1799): builds the same creds, forces
  `dryRun: true`, `limit: Math.min(limit || 100, 100)`, `writeToFile: false`, `logs: false`,
  runs `mixpanelImport` **synchronously** and returns `{success, result, previewData: result.dryRun}`.
  Note its `opts` whitelist (1763-1778) is even narrower than `/export`'s.
- This dry-run path is a genuinely good preview vehicle for identityReplay: the library's
  pipeline runs for real (graph build + verb rewrite on up to 100 records) with nothing sent —
  README also recommends `destinationOnly` + `destination` for full-fidelity rehearsal, but for
  UI purposes the 100-record dry run showing rewritten association events in `previewData`
  is the natural fit.

### CLI preview on this page

`updateCLICommand()` (export.js:939-1060), re-run on every form `input`/`change`
(export.js:558-559) and at the end of `updateFieldVisibility()` (771):

- Starts `npx mixpanel-import --type <recordType>`.
- `project`/`token` included only if their `.form-group` is visible and non-empty (956-964) —
  a good pattern to copy for IR fields.
- Secrets are redacted: `--pass [password]`, `--secret [api-secret]`,
  `--secondToken [destination-token]`, `--gcsCredentials [gcs-credentials]`,
  `--s3Secret [s3-secret]` (966-978, 1003-1007, 1024, 1034).
- Scalar options via `optionsMap` `{fieldId: flag}` appended as `--flag "value"` (981-1001);
  booleans via `booleanFlags` (1038-1050).
- For identityReplay the real CLI flags are (components/cli.js:390-417, composition at
  435-447): `--identity-replay` (boolean), `--ir-user-id-regex '<regex>'` (required with it),
  `--ir-graph-path`, `--ir-on-ambiguous {drop|resolve|error}`. cli.js `.check()` (423-431)
  enforces that `--ir-*` flags require `--identity-replay` and vice versa — the preview should
  emit them together.

### Where an identityReplay section slots in

Recommended shape, imitating the existing destination-subsection pattern exactly:

1. **export.html** — inside `#credentials-section` after `secondRegion-group` (i.e. after
   line 394), or as its own card between Credentials and Export Configuration: an
   `<h3 class="subsection-title" id="identity-replay-title">` + description + a wrapper div
   `id="identityReplay-group"` (all `style="display: none;"`) containing: an enable checkbox,
   `irUserIdRegex` text input (the one required field), and optionally `irOnAmbiguous`
   select + `irGraphPath` text input + `associationProps`-style extras. A collapsible-section
   inside the card also works (`toggleSection()` pattern, export.html:445-452).
2. **export.js `updateFieldVisibility()`** — add the new ids to `allGroups` (661-665) and show
   them **only in `case 'export-import-event':`** (719-730). Do NOT show for
   `export-import-profile`/`export-import-group` — `components/job.js:367-371` throws unless
   recordType is `event`/`export-import-event`.
3. **export.js `validateRequiredFields()`** — in the `export-import-event` case: if IR enabled,
   require a non-empty `irUserIdRegex` (mirrors cli.js check) and ideally `new RegExp()` it in a
   try/catch for early feedback.
4. **export.js `collectFormData()`** — either send flat fields (`identityReplay: true`,
   `irUserIdRegex`, `irGraphPath`, `irOnAmbiguous`) and compose the object server-side (matches
   how cli.js composes at 438-446), or compose the nested
   `identityReplay: { isUserId, graphPath, onAmbiguous }` object client-side. Flat + server-side
   composition is safer given the empty-value stripping loop (1213-1218) only handles scalars.
5. **export.js `updateCLICommand()`** — append `--identity-replay --ir-user-id-regex "<v>"`
   (+ optional `--ir-graph-path`, `--ir-on-ambiguous`) when enabled and recordType is
   `export-import-event`.
6. **Results**: telemetry lands on results as `result.identityReplay`
   (components/job.js:1167-1168) — the results renderer (`showResults`, export.js:1266-1303)
   just JSON-dumps, so it will display automatically **once the server allows the field
   through** (see Gotchas).

### index.html landing page — third tile?

`ui/public/index.html` (757 lines) is a self-contained page: two `<a class="tool-card">` tiles
(E.T.L → `/import` at 394-403, L.T.E → `/export` at 406-414) in a
`grid-template-columns: 1fr 1fr` container (91-96), plus a purely decorative center `.divider`
(249-258, absolutely positioned, hidden on mobile) and emoji/word-cycling scripts (422-753).

Structurally a third tile is easy — change the grid to `1fr 1fr 1fr` (and the 768px breakpoint
already collapses to `1fr`), add a third card with its own gradient/glow keyframes, and drop or
reposition the `.divider` (it assumes exactly two columns; `left: 50%` lands mid-tile with
three). Each tile also has its own emoji-cycling array + interval and the ETL card carries
`border-right: 2px solid` (116-119) which the middle tile of three would need too. **However**,
identityReplay is an *option of the export-import-event flow*, not a separate tool with its own
route — server.js only serves `/`, `/import`, `/export`. Unless the sprint adds a dedicated
`/migrate` route + page, the better landing-page move is to keep two tiles and, at most, update
the L.T.E tagline ("export data from mixpanel and re-import data", line 412) to mention identity
migration. A third tile only makes sense if v3.6.1 ships a dedicated wizard page.

## Gotchas

1. **The server whitelists everything — client-side work alone is invisible.** Both
   `POST /export` (`opts` at ui/server.js:1443-1469) and `POST /export-dry-run` (opts at
   1763-1778) build options from explicit field lists. An `identityReplay` key sent by
   `collectFormData()` is silently dropped. Both endpoints need the field added (and the
   dry-run endpoint is the narrower of the two).
2. **`identityReplay` telemetry is stripped from results.** `filterResultForClient()`
   (ui/server.js:418-435) has a hard `allowedFields` list; `identityReplay` is not on it, so
   `job-complete` payloads and `/export-dry-run` full results won't carry the telemetry block
   until `'identityReplay'` is appended to that list (docs/web-ui.md's Included/Excluded table
   should be updated too).
3. **Pre-existing bug worth fixing in the same pass: `secondRegion` is a no-op.** The UI
   collects it (export.js:1177) but the server only uses `exportData.secondRegion` in two log
   lines (server.js:1659, 1703) — it is never placed on `opts`, so
   `parsers.js:676/695` (`if (job.secondRegion) job.region = job.secondRegion`) never fires.
   Destination-region selection in the UI currently does nothing.
4. **Only `export-import-event` may show the IR section.** `components/job.js:369-371` throws
   for any other recordType (including `export-import-profile`/`-group`); job.js:368 also
   throws on `fastMode` (not surfaced on this page, so no conflict) and disables `v2_compat`
   with a warning (also not on this page).
5. **`isUserId` is a regex *string* over the wire.** Function predicates are module-only; the
   README confirms strings are compiled as RegExp. The UI field should be labeled as a regex
   and validated with `new RegExp()` client-side; cli.js:423-425 refuses
   `--identity-replay` without `--ir-user-id-regex`, so the CLI preview must always pair them.
6. **`collectFormData()` deletes falsy-empty values but not `false` booleans explicitly** —
   checkboxes come through as `true`/`false` (booleans survive the strip loop since only
   `''`/null/undefined are deleted, export.js:1213-1218). A flat `identityReplay: false` would
   therefore be transmitted; compose/gate accordingly server-side (cli.js only composes when
   truthy, components/cli.js:438).
7. **Dry runs skip client validation entirely** (export.js:1085-1093) — a dry run with IR
   enabled but an empty/invalid regex will reach `job.js` and throw
   (`identityReplay requires isUserId`); the server returns 500 with the message, which the
   page alerts. Acceptable, but a client-side check gives a nicer failure.
8. **Form state persistence:** sessionStorage save/restore (export.js:301-375) is generic over
   inputs/radios/checkboxes with ids — new IR fields get persistence for free, but the restore
   fires `change` events on radios only; a restored IR *checkbox* won't re-trigger visibility
   logic by itself (visibility is driven by `recordType`'s change event, which restore does
   dispatch for radios/selects via the generic input restore path — verify after wiring).
9. **`fillDevValues()` (export.js:185-259) contains hardcoded dev credentials** (service
   account + password for project 3730336) that get committed with this file — be careful not
   to add more secrets there when extending it for IR testing, and note it only exercises the
   `export` recordType today.
10. **Two update hooks must both be touched** when adding fields: `updateFieldVisibility()`'s
    `allGroups` hide-list (export.js:661-665) *and* the per-case show logic — forgetting the
    hide-list leaves IR fields visible after switching away from `export-import-event`.
