# filterMixpanelNoise: drop `$mp_session_record` events and `$mp_replay_id`, list version 3

Status: SHIPPED in 3.7.3 on `main`, 2026-11-09. AK publishes to npm. Written 2026-11-09. The Go
worker (snowcat `go-worker/`) gets the same change; its spec is
`~/code/snowcat/plans/2026-11-noise-drop-session-record/spec.md`. The list file this spec named
(`files/mixpanel-noise.json`) is now `components/mixpanel-noise.json`.

## Problem

Mixpanel's raw `/export` returns `$mp_session_record` events. Each one is a Session Replay
checkpoint and points, through `$mp_replay_id`, at a recording that lives only in the source
project. `/import` accepts these events. In the destination project the replay list then shows
sessions that cannot play, because no recording exists there.

SDKs also put `$mp_replay_id` on ordinary events sent while a recording runs. That links each
event to the same recording, which does not exist in the destination either.

Evidence:

- Mixpanel builds a project's replay list from `$mp_session_record`: it is "the one event
  guaranteed to exist for every replay" (analytics `backend/replays/query_utils.py:41, 229-233`).
- Ingestion knows the event by name and checks only `replay_length_ms` on it
  (`go/src/mixpanel.com/ingestion/api/verify/verify.go:450-457`). Nothing refuses an imported one.
- Sandbox 4081039 (fde-toolkit `labs/klover-replay-test`, 2026-11-09): one Klover export file sent
  544,966 events, and 70 of them were `$mp_session_record`. All 70 were accepted.

Example record (from that export):

```json
{ "event": "$mp_session_record",
  "properties": { "time": 1768532009, "distinct_id": "$device:-8422725226034097984",
    "$mp_replay_id": "D00457CE-8669-492E-9F8F-FBB979473141", "$mp_replay_retention_period": 30,
    "mp_lib": "swift-sr", "replay_length_ms": 2, "replay_start_time": 1768532009.032448,
    "replay_region": "us", "replay_version": "1", "seq_no": 0, "...": "..." } }
```

## Decision

AK, 2026-11-09. `filterMixpanelNoise` drops every `$mp_session_record` event, and deletes the
`$mp_replay_id` key from every event it keeps. This fits the
filter's goal: project B shows what project A showed, with nothing that cannot work in B.

- An export keeps both. Only the import side drops them.
- `filterMixpanelNoise: false` keeps both, like every other part of the filter.

## The list: `components/mixpanel-noise.json` version 3

Replace it with `files/mixpanel-noise.json` from this folder (now `components/mixpanel-noise.json`). It must stay byte-identical with
the Go copy: snowcat's `tests/noiseParity.test.js` compares the bytes.

- `version: 3`. The `about` text names the new section.
- `drop_events` (new section, after `drop`): one entry, `{ event, why, evidence, added }`, for
  `$mp_session_record`.
- `drop`: 24 keys. The 23 from version 2, then `$mp_replay_id` (last).
- `delete_keys`, `watch`, `watch_events`: unchanged.

A new section, not a `watch_events` entry: `watch_events` only counts, and it counts `$delete` only
when deletes are not applied. `drop_events` always drops. One section, one meaning.

## Behaviour

### `$mp_replay_id`

An ordinary `drop` key. The existing `noiseFilter` step deletes it and counts
`noiseRemoved['$mp_replay_id']`, as it does for every `drop` key. No code change beyond the list.
The other replay keys in the sample (`$mp_replay_retention_period`, `replay_*`) are on
`$mp_session_record`, which is dropped whole, so they get no entry. If one turns up on ordinary
events, it is a new `drop` key with its own evidence.

### `$mp_session_record`

1. **When.** The noise filter is active (`jobConfig.noiseFilter` is set: `event` or
   `export-import-event`, not `fastMode`) **and** no `vendor` is set. A vendor record is not in
   Mixpanel's shape before the vendor transform, so a name test on it means nothing. This is the
   same `vendor` rule the delete prescan uses (`parsers.js` `prescanDeletes`).
2. **Not tied to the prescan.** The drop needs only the record's own name. It runs for a caller's
   stream and for `export-import-event` too, where deletes are not applied.
3. **Where.** A new stage in `pipelines.js`, on the raw record: after the first existence filter
   and after `createDeleteApplier` (when that runs), before `createVendorTransform`. A later
   transform can rename the event; the test must see the source's name. After the delete applier
   so that a `$delete` that targets a session record still counts as `event:deleted`, as today.
4. **Match.** `typeof record.event === 'string'` and the name is in `drop_events`, exact and
   case-sensitive. Build a `Set` once from the list.
5. **Count.** `noiseRemoved['event:$mp_session_record'] += 1` and `job.empty += 1`. So
   `total - empty` still counts what was sent, and snowcat's `recordsWritten()` stays right.
6. **Destination-only runs** (`destination` + `destinationOnly`) drop the event from the output
   file too. The filter runs the same way whether or not records are sent.

`fastMode` skips every transform, so session records pass under it. That is the existing rule for
the whole filter; do not change it.

## Code

- `components/mixpanel-noise.json`: the version 3 file.
- `components/pipelines.js`: `createNoiseEventDropper(job)` (name free), pushed as in Behaviour 3.
  Keep it a small object-mode `Transform`, like `createDeleteApplier`.
- `components/job.js`: no new option. Expose the set (for example `this.noiseDropEvents`) only if
  the stage needs it from the job.
- `components/transforms.js` `noiseFilter`: unchanged. It never drops a record, and its doc
  comment says so; keep that true.
- Types (`index.d.ts`): the `noise_removed` doc names `event:$mp_session_record`.

## Tests

- A unit test for the new stage: drops `$mp_session_record`, keeps `$mp_session_recording` and
  `$MP_SESSION_RECORD`, keeps a record with no `event`, counts `empty` and `noise_removed`.
- The stage does not run under `fastMode`, with `filterMixpanelNoise: false`, or with `vendor`.
- It runs for a stream input (deletes not applied).
- `noiseFilter` deletes `$mp_replay_id` from an ordinary event and counts it.
- The noise golden fixture (shared with Go: snowcat
  `go-worker/internal/transform/testdata/noise/input.ndjson` and `expected.ndjson`) gets one
  `$mp_session_record` row in the input and none in the expected output, and one ordinary event
  with `$mp_replay_id`, which the expected output holds without that key. Both engines must
  reproduce it; snowcat's parity test runs this engine against the Go golden files.

## Docs

- README "Mixpanel noise filter": one bullet, "drops every `$mp_session_record` event (a Session
  Replay checkpoint that points at a recording only the source project has)", and add
  `event:$mp_session_record` to the `noise_removed` bullet. Say that `$mp_replay_id` is deleted from
  every other event.
- README options table row for `filterMixpanelNoise`: add "drop Session Replay checkpoints".
- CHANGELOG entry.

## Release

A patch release, as 3.7.1 and 3.7.2 were for list changes: **3.7.3**. AK publishes to npm. Then
snowcat sets `mixpanel-import` to `^3.7.3` in `package.json` and `worker/package.json`, copies the
list into `go-worker/`, and raises `NEEDS` in `tests/noiseParity.test.js`.

## Rejected

- **Drop in `noiseFilter`** (the last transform step). It runs after user transforms, so a
  `transformFunc` that renames events changes what is dropped, and the step's contract is "never
  drops a record".
- **Use `eventBlacklist`.** That puts the burden on every caller and counts in
  `blackListSkipped`, not in the noise counts snowcat shows.
