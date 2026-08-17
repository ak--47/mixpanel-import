# identityReplay in the Web UI (v3.6.1)

**Sprint start:** 2026-08-17 · **Owner:** AK + Claude (orchestrator)
**Status:** RECON → design questions → build

## Goal

Expose the v3.6.0 `identityReplay` feature in the web UI (`ui/`) so migrations can be
configured entirely from the GUI — with helpful descriptions/tooltips — shipping as patch
**3.6.1**. AK: identityReplay is "probably its own section in the GUI"; adoption is the point.

## Context

- Feature docs: README "Original → Simplified ID Merge Migration (identityReplay)";
  option group in index.d.ts (~17 sub-options); prior sprint: plans/completed/original-to-simplified/
- UI: Express + ws server (ui/server.js), two tools: /import (E.T.L) + /export (L.T.E),
  Monaco editor for transform code (user functions cross the boundary as strings → isUserId
  function support is feasible via the same mechanism), live CLI preview, dry-run endpoints.
- Replay's flagship flow (export-import-event project→project) lives on the EXPORT page;
  file-based replay lives on the IMPORT page.

## Design decisions (AK, 2026-08-17)

1. **Both pages** — one shared section on /import and /export.
2. **isUserId: regex field (with live pass/fail tester on sample ids) + optional
   "use a function" Monaco toggle** (same string→function mechanism as transformCode).
3. **Core + advanced accordion.** Visible: isUserId, electionScope, onAmbiguous,
   associationTimestamp, graphPath, minAssociationRate. Advanced: graph, maxGraphSize,
   onGraphOverflow, identityEvents, associationEventName, associationProps, bareDistinctId,
   userIdFallbackProps, denylist, scrubJunkIds, scrubExportProps.
4. **Records preview only** for dry-run/completion — NO dedicated telemetry card, no
   response-whitelist changes. Users eyeball rewritten events + synthetic associations in
   the existing dry-run table.

## Progress log

- 2026-08-17: Sprint start. Recon workflow launched (wf_94793be4-8eb: server plumbing,
  import front-end, export front-end → research/01-03).
