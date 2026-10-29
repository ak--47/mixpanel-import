# filterMixpanelNoise: apply the source's deletes, list version 2 (3.7.2)

Status: SHIPPED in 3.7.2 on `main`, 2026-10-29. AK publishes to npm. Snowcat is not updated
yet: set `mixpanel-import` to `^3.7.2` there and run `tests/noiseParity.test.js`.

## Changes from this spec (AK, 2026-10-29)

- **`export-import-event` does not apply deletes.** Its input is a live `/export` call that cannot
  be read twice, and `determineDataType` rewrites the job's `recordType` and credentials. It is
  treated like a caller's stream: the `$delete` rows and delete keys are kept and counted.
- **Jobs with `vendor` set skip the prescan.** The flow is for events that came from Mixpanel.
  The Go worker prescans vendor jobs; output differs only if vendor input carries `$delete` rows.
- **The prescan decodes in full.** No `"$delete"` byte filter for JSON lines. Same output and
  counts; more CPU on the first pass.
- **Where it lives:** `components/deletes.js` (`DeleteIndex`, `timeMillis`),
  `parsers.js` `prescanDeletes` (reads through a shadow of the job, so no counter or callback
  fires), `pipelines.js` `createDeleteApplier` (after the first existence filter), and
  `index.js` calls the prescan before `determineDataType`.
- **Web UI:** a "Filter Mixpanel Noise" checkbox in Processing (event imports, on by default);
  the raw side of a preview sets `filterMixpanelNoise: false`.

## Decision

AK, 2026-10-29. The goal of `filterMixpanelNoise` is that project B shows what project A
showed, with nothing that Mixpanel stamps on ingestion. Two changes follow:

1. **Apply the source's deletes before import. Never send them.** Drop every `$delete` event,
   every event that a valid `$delete` deletes, and every event with `$is_deleted: true`. Then
   delete the delete-mechanism keys from the events that are kept.
2. **Drop the rest of the old `watch` list.** `watch` is now empty.

Why not send the `$delete` rows to B and let Mixpanel apply them: B has no warehouse import.
Mixpanel's query layer then assumes B holds no `$delete` rows, and a delete can show and hide
again as files compact. Each `$delete` also makes identity calls, and B can reach a query
process exit (`~/code/fde-toolkit/research/synth-delete/REPORT.md` §4 and §6). Skipping only the
`$delete` rows is wrong too: their targets would show in B but stay hidden in A.

## The list: `components/mixpanel-noise.json` version 2

Replace it with `files/mixpanel-noise.json` from this folder. It must stay byte-identical:
snowcat's `tests/noiseParity.test.js` compares the bytes.

- `version: 2`. The `about` text describes the new behaviour.
- `drop`: 23 keys. The 15 from 3.7.1, then `$is_reshuffled`, `$original_distinct_id`,
  `$identity_failure_reason`, `$had_persisted_distinct_id`, `$version`, `$possible_duplicate`,
  `$sampling_factor`, `$bucket`.
- `delete_keys` (new section): `$is_deleted`, `$delete_event_name`, `$delete_insert_id`,
  `$delete_inserted_before`, `$inserted_at`, `$distinct_id_before_identity`.
- `watch`: `[]`. Keep the key, so the file keeps one shape.
- `watch_events`: `$delete`, with a new `why`.

## Behaviour

`filterMixpanelNoise` keeps its activation rule: default `true` for `event` and
`export-import-event`, off under `fastMode`, an explicit `true` elsewhere throws.

### Deletes applied: the input can be read twice

The input can be read twice when it is a path, a folder, an array of paths, a `gs://` or `s3://`
URI or array of them, or an array of objects. A caller's stream cannot.

1. **Prescan.** Before `corePipeline`, read the input once and build the delete index. Call
   `determineDataType` a second time to get a fresh stream for the main pass. The prescan must
   change **no** job counter and fire no callback that counts: an unparsable line is skipped, not
   counted, and `maxRecords` does not apply. A missing object in a multi-object read must be
   counted once over both reads. For JSON lines, decode only the lines that contain the bytes
   `"$delete"`. Other formats decode in full.
2. **Index.** A record goes into the index only when it is a valid `$delete`:
   - `event === "$delete"`, and `properties` is an object;
   - `$delete_event_name` and `$delete_insert_id` are strings;
   - `$delete_inserted_before` is a number;
   - `distinct_id` is a string;
   - `time` is a time (rule below).

   Key: `(target name = $delete_event_name, distinct_id, target insert id = $delete_insert_id,
   time in ms)`. One key can hold several `$delete` rows; keep each row's
   `$delete_inserted_before` and a `matched` flag.
3. **Main pass, on the raw record.** Run this right after the first existence filter
   (`pipelines.js` ~45), before the vendor transform. A transform after it can change the name,
   the ids or the time, and the match must see the source's values. In this order:
   - `event === "$delete"`: drop. `noise_removed["event:$delete"] += 1`. This applies to every
     `$delete` event, valid or not: an invalid one hid nothing in A.
   - `properties.$is_deleted === true` (boolean only): drop.
     `noise_removed["event:$is_deleted"] += 1`.
   - `properties` holds a string `$insert_id`, a string `distinct_id` and a valid `time`, and the
     index holds that key under the event's name: take the key's rows in insertion order. A row
     deletes the event when `$inserted_at` is absent (`!('$inserted_at' in properties)`), or is a
     number `< $delete_inserted_before`. Any other `$inserted_at` (string, null, equal or higher)
     skips that row. On the first row that deletes: set `matched`, drop the event,
     `noise_removed["event:deleted"] += 1`.

   **Every drop also increments `empty`**, so `total - empty` still counts what was sent. Snowcat's
   `recordsWritten()` relies on this.
4. **After the last record:** for each indexed row with `matched === false`,
   `noise_seen["event:$delete:no_target"] += 1`.
5. **`noiseFilter`** (still last in the chain): delete each `drop` key as today, then delete each
   `delete_keys` key and count it in `noise_removed`. Count `watch` keys in `noise_seen` (none
   today). `watch_events` cannot match, because no `$delete` event reaches the chain.

This is Mixpanel's query-time rule (analytics `7437f23a9a1`,
`backend/libquery/event_reader/delete_event_reader.cpp:137-256`, and
`backend/arb/reader/main.cpp:3016` for `$is_deleted`).

### Deletes not applied: a caller's stream

Do not prescan. Apply nothing. `noiseFilter` deletes the `drop` keys. It counts each
`delete_keys` key and each `watch_events` name (`event:$delete`) in `noise_seen` and leaves them
in the record. Then the `$delete` rows reach the destination complete and work there.

### Time

The same rule as Mixpanel ingestion (`ingestion/api/apitime/time.go`, `MaxSeconds = 1 << 31`).
Only a JS number is a time; a string is not.
- `v <= 0`, `NaN`, `±Infinity`: not a time.
- `v > 2 ** 31`: milliseconds, `Math.trunc(v)`.
- otherwise: seconds, `Math.trunc(v * 1000)`.

Go does the same in float64, so both engines produce the same key.

### Scope

The index covers one run. A `$delete` whose target is in another run's input is dropped, and its
target is sent; `event:$delete:no_target` counts these. An export day file holds both, because a
`$delete` carries its target's `time`.

## Tests

1. `tests/fixtures/noise/`: replace `input.ndjson`, `expected.ndjson` and `expected-counts.json`
   with `files/fixtures/`. The input gains 11 rows (21-31) that cover:
   - a `$delete` before its target and one after it;
   - the strict `<` on `$inserted_at` (a replacement row survives);
   - a target 1 ms off;
   - an invalid `$delete` (string `$delete_inserted_before`);
   - a target whose `$inserted_at` is a string.

   Expected: 31 in, 22 out, `empty` 9, three `no_target`.
2. The golden test must run the whole pipeline (as snowcat's parity test does: `mp()` with
   `destination`, `destinationOnly: true`, `fixData: false`, `removeNulls: false`). Assert
   `total = 31` and `total - empty = 22` as well as the records and both maps. The output keeps
   each record's key order.
3. `tests/noise.test.js`, "list shape": `DROP` equals the 23 keys, `delete_keys` equals the six,
   `watch` is empty. Port the unit cases from snowcat
   `go-worker/internal/transform/deletes_test.go`: the match table, invalid `$delete` rows not
   indexed but still dropped, counts with two `$delete` rows for one key, and the time table.
4. Port `go-worker/mixpanel/noise_test.go`:
   - a stream input applies nothing and counts the `delete_keys` and `event:$delete` as seen;
   - the prescan counts nothing (one unparsable line gives `unparsable: 1`);
   - `filterMixpanelNoise: false` writes all 31 rows and counts nothing.
5. Any test that pins per-read counts on a re-readable cloud input (opens, resumes, stalls,
   `cloudStreamCallback` events) sets `filterMixpanelNoise: false`. The prescan reads each object
   twice. Snowcat's Go port did this in `internal/gcs/fake_test.go` `baseOpts`.

## Docs

- `README.md` "Mixpanel noise filter": the goal; the delete application and its counts; the
  stream case; one-run scope; the input is read twice.
- `index.d.ts` and the `components/cli.js` help text: the same.
- `CHANGELOG.md` 3.7.2, a behaviour change: event jobs drop `$delete` events and the events
  they delete, the drop list grows by 8, and the input is read twice.

## Verify

- `npx jest tests/noise.test.js --watchAll=false` passes.
- `cmp components/mixpanel-noise.json ~/code/snowcat/go-worker/internal/transform/mixpanel-noise.json`
  prints nothing.
- `cmp` each fixture file with `~/code/snowcat/go-worker/internal/transform/testdata/noise/`.
- Publish 3.7.2. In snowcat, set `mixpanel-import` to `^3.7.2` in `package.json` and
  `worker/package.json`, then run `npx vitest run tests/noiseParity.test.js`. It skips below 3.7.2.
  On 3.7.2 both tests must run and pass: the bytes of the list, and the node engine reproducing
  the Go golden files.
