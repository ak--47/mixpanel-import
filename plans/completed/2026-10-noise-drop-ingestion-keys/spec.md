# filterMixpanelNoise: drop fourteen ingestion keys (3.7.1)

Status: SHIPPED in commit `cdb14e7` on `main`, 2026-10-29. AK publishes 3.7.1 to npm.
Snowcat is not updated yet (see Follow-up).

## Decision

AK, 2026-10-29: these fourteen keys move from `watch` to `drop` in
`components/mixpanel-noise.json`. Mixpanel stamps them on ingestion. They arrive in source data
mostly from a Mixpanel export. We almost never want to send them on an import.

| Key | Old group | Why drop |
|---|---|---|
| `$mp_api_endpoint` | overwritten | Source-project ingestion metadata |
| `$mp_api_timestamp_ms` | overwritten | Source-project ingestion metadata |
| `$mp_event_size` | overwritten | Source-project ingestion metadata |
| `mp_processing_time_ms` | overwritten | Source-project ingestion metadata |
| `$import` | overwritten | Source-project ingestion metadata |
| `$mp_actor_type` | overwritten | Source-project ingestion metadata |
| `$mp_raw_user_agent` | overwritten | Source-project ingestion metadata |
| `$warehouse_import_id` | warehouse | Names a warehouse sync the destination does not have |
| `$warehouse_import_job_id` | warehouse | Same |
| `$warehouse_import_run_id` | warehouse | Same |
| `$warehouse_table` | warehouse | Same |
| `$warehouse_type` | warehouse | Same |
| `mp_original_distinct_id` | warehouse | Source-project ingestion metadata |
| `mp_original_event_name` | warehouse | Source-project ingestion metadata |

The first draft of this spec had nine keys (no `$import`, `$mp_actor_type`,
`$mp_raw_user_agent`, `mp_original_distinct_id`, `mp_original_event_name`) and targeted 3.8.0.
AK added the five keys and changed the version to 3.7.1 during implementation.

`filterMixpanelNoise` stays default `true` for event jobs, so every event job drops them.
`false` still keeps every key.

This changes the tier rule. Before: drop a key only when it makes the destination hide an
event. After: also drop source-project ingestion metadata, by owner decision.

## What shipped

`noiseFilter` in `components/transforms.js` reads `list.drop` generically. No code changes.

1. `components/mixpanel-noise.json`: the fourteen entries move into `drop`. The nine first-draft
   keys come first, then the five added keys in the order AK listed them. Each has
   `added: "2026-10-29"`, keeps its old `why` and `evidence`, and adds "owner decision AK
   2026-10-29". The `about` text allows an owner decision. `watch` keeps 14 keys; `watch_events`
   keeps `$delete`.
2. `tests/fixtures/noise/expected.ndjson` and `expected-counts.json`: rebuilt from the
   unchanged `input.ndjson`. The keys leave records 10 (`overwritten`), 13 (`warehouse`), and
   the `$import` key on record 19 (`mixed`). They move from `noise_seen` to `noise_removed`.
3. `tests/noise.test.js`, "list shape": `DROP` equals `$preshuffle_distinct_id` plus the
   fourteen keys. Test renamed.
4. `README.md` "Mixpanel noise filter": names the fourteen keys and states the rule: a key is
   deleted when it hides an event in the destination, or when it is source-project ingestion
   metadata.
5. `index.d.ts` `filterMixpanelNoise` doc comment and the `components/cli.js` help text: same
   wording change.
6. `CHANGELOG.md`: 3.7.1 entry, marked as a behaviour change.
7. Version 3.7.1 in `package.json` and `package-lock.json`.

The copies in `files/` are the nine-key first draft. The shipped versions are the repo files
above.

## Verified

- `tests/noise.test.js`: 30 of 30 pass, including the golden fixture.
- `npm run validate` passes.
- Full local run (without `int.optional` and `sanity`): 7 failures, all parquet and posthog
  tests with a `hyparquet` dynamic-import error. The same 7 fail on `main` before this change.
- `npm pack --dry-run` builds 3.7.1 and ships `components/mixpanel-noise.json`.

## Follow-up

- Snowcat: copy `components/mixpanel-noise.json` over
  `~/code/snowcat/go-worker/internal/transform/mixpanel-noise.json`. The snowcat copy has only
  the nine-key draft list. `tests/noiseParity.test.js` needs the files byte-identical.
- Snowcat: change `NEEDS` in `tests/noiseParity.test.js` from `"3.8.0"` to `"3.7.1"`. Upgrade
  `mixpanel-import` to `^3.7.1` in `package.json` and `worker/package.json`.
- Open question: drop the rest of the `watch` list too? AK is not sure yet. Dropping
  `$is_deleted` and the `$delete_*` keys can make deleted events show up in the destination.
