# Probe Findings — dataset v1, round 1 (2026-08-16 evening)

Status: PARTIAL — Mixpanel-side merge propagation in flight (AK confirmed latency expectation).
Items marked ⏳ need steady-state re-export/re-query (scheduled). Probe scripts in `../probes/`.

## CONFIRMED (design-grade facts)

### Simplified project (4054681) behavior
1. **Verbs are HARD-REJECTED by /import, not ignored.** HTTP 400,
   `failed_records: ["identity events are not allowed when project is using simplified identity management"]`
   for all three of $identify/$create_alias/$merge, strict=1. Whole batch fails
   (num_records_imported: 0). ⇒ `identityEvents: 'emit'` mode is IMPOSSIBLE; rewrite-or-drop only.
   ⇒ Also: failed_records here is an array of STRINGS (docs show objects) — importer error
   handling must tolerate both shapes.
2. **Custom-named dual-ID event triggers merge** (p04): "identity association" with
   $device_id+$user_id merged the cluster; prior $device:-keyed page view retroactively
   re-keyed to the user (befId=$device:..., reshuffle visible within minutes).
3. **First-write-wins on device conflicts** (p05): D→U51 sent before D→U52; device's bare
   events resolved to U51. U52's association stored as an event but did not steal the device.
   ⇒ conflict resolution MUST happen pipeline-side, pre-send (confirms Vipps).
4. **Backdated association events work** (p09): assoc timestamped 30 days before the anon
   events still stitched them. ⇒ `associationTimestamp: 'floor'` strategy is viable.
5. **Bare unprefixed uuid distinct_id becomes a $user_id** (p07) — phantom-user bug live:
   exported with $user_id = the uuid. This is exactly what `bareDistinctId: 'validate'` prevents.
6. **Bare '$device:X' distinct_id → device** (p06). Exported distinct_id keeps prefix,
   $device_id = stripped X.
7. **Ingest strips ONE '$device:' prefix from $device_id** (p08): sent $device_id='$device:uuid',
   exported $device_id='uuid', cluster keyed correctly. (Modern /import normalizes; we still
   strip ourselves — don't rely on undocumented behavior. Vipps' $device:$device: corruption
   presumably came via a different ingress or era.)
8. **Raw export can contain duplicate rows** (p05 assoc row appeared twice) — dedupe is
   query-time; replay reads of simplified exports must expect dupes.

### Original project (4054680) behavior
9. **All 1216 events accepted, zero strict errors** — including 510-id cluster builder and
   non-uuidv4 $anon_id. Original enforcement (uuid shape, 500 cap) is SILENT, post-ingest.
10. **Verb rows export with full props**: $identify keeps $identified_id+$anon_id;
    $create_alias keeps alias+distinct_id; $merge keeps $distinct_ids. ⏳ confirm they persist
    at steady state (some verb rows currently missing from export).
11. **Exported distinct_id is re-keyed to cluster canonical, and canonical can be an ANON id**:
    s07 $identify row exported with distinct_id=<anon uuid>, $distinct_id_before_identity=<user id>.
    Confirms star-with-anonymous-head; replay CANNOT trust exported distinct_id as "the user".
12. **$distinct_id_before_identity is real and exported** (189 rows so far) — records the
    as-ingested distinct_id of re-keyed rows. Useful evidence prop (userIdFallbackProps default?).
13. **Original does NOT link from $device_id/$user_id props** (s10): activity stream for the
    device id is empty; props stored inertly. Verbs are the only edge source in original.
    (Dual-id rows in exports remain graph EVIDENCE for us, but expect them only from SDK data.)
14. **Query-time resolution runs ahead of export materialization**: segmentation already shows
    s01/s03/s04/s06/s12 = 1 unique while export still shows unresolved distinct_ids. Anon↔anon
    $merge (s04) and user↔user $merge (s06) both resolved to single users — original supports both.
15. **500-cap effects visible** ⏳: s07 page-view uniques = 103 (would be 1 if fully merged) —
    large orphan population consistent with the cap. Needs steady-state census (JQL) for exact split.

## Mid-propagation (⏳ re-check at steady state)
- s01 export unresolved; s02 chain partial (A2 collapsed into A1, A3+U separate); s05 3 uniques;
  s09 identify rows missing from export; s11 8 uniques (expect 1); s07 missing ~310 rows.
- Total exported 901/1216 — determine whether missing rows are propagation lag or permanent
  (identify-row compaction?). CRITICAL for "can replay rely on exported verbs" question.

## v2 dataset TODOs (fixes to generator)
- s10: add anon-side events (distinct_id=A) so dual-id linkage is testable via export, not
  just activity stream.
- Add scenario: $device:$device: double-prefix garbage into ORIGINAL (does original store it?).
- Add scenario: $identify where $anon_id was ALREADY merged to another user (docs say refused).
- Consider: event with distinct_id ≠ $user_id ≠ $device_id all set (conflicting hints).

## Immediate design consequences
- identityEvents: 'rewrite' | 'drop' only (no 'emit'/forward — hard-rejected).
- Conflict resolution + one-user-per-device election MUST be pipeline-side (first-write-wins).
- Backdated assoc timestamps viable → keep 'floor' option (Vipps pattern) + 'original' default.
- strip('$device:') both directions; never trust exported distinct_id as user; prefer
  $user_id/$device_id props, verbs, $distinct_id_before_identity as evidence in that order.
- Export dupes exist → deterministic assoc $insert_id is required, not nice-to-have.
