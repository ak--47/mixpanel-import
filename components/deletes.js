/*
----
DELETE INDEX
----
filterMixpanelNoise applies a source project's deletes before import, so the destination shows
the events the source showed and no `$delete` event ever reaches it. It needs an input that can
be read twice: a prescan adds every `$delete` (parsers.js prescanDeletes), then the main pass asks
drop() about every record, on the raw record, before any other transform
(pipelines.js createDeleteApplier).

The rule is Mixpanel's query-time rule (analytics 7437f23a9a1,
backend/libquery/event_reader/delete_event_reader.cpp:137-256): a `$delete` is valid when
`$delete_event_name` and `$delete_insert_id` are strings, `$delete_inserted_before` is a number,
`distinct_id` is a string and `time` is a time. It deletes an event with the same name,
`distinct_id`, `$insert_id` and millisecond `time` whose `$inserted_at` is absent, or a number
below `$delete_inserted_before`. Any other `$inserted_at` keeps the event. Every query also hides
an event with `$is_deleted` true (backend/arb/reader/main.cpp:3016).

Scope is one run: a `$delete` whose target is in another run's input is dropped, its target is
not, and the `$delete` is counted as `event:$delete:no_target`.

parity: snowcat go-worker/internal/transform/deletes.go

This module has no internal imports and no process-global side effects.
*/

/** splits seconds from milliseconds, as Mixpanel ingestion does (apitime/constants.go:8) */
const MAX_SECONDS = 2 ** 31;

/**
 * a JS number (or bigint, from parquet) as a finite number; anything else is not a number
 * @param {any} v
 * @returns {number | null}
 */
function toNumber(v) {
	if (typeof v === 'bigint') return Number(v);
	if (typeof v !== 'number' || Number.isNaN(v)) return null;
	return v;
}

/**
 * a numeric `time` as epoch milliseconds, the way Mixpanel ingestion reads it
 * (apitime/time.go unixTimeFloat64): above 2^31 it is already milliseconds, otherwise seconds;
 * the fraction below a millisecond is cut. Zero, negative, infinite and non-numeric times
 * (strings included) are not times.
 * @param {any} v
 * @returns {number | null}
 */
function timeMillis(v) {
	const n = toNumber(v);
	if (n === null || n <= 0 || !Number.isFinite(n)) return null;
	if (n > MAX_SECONDS) return Math.trunc(n);
	return Math.trunc(n * 1000);
}

/** @param {any} p */
const isObject = p => p !== null && typeof p === 'object' && !Array.isArray(p);

/**
 * @param {string} name
 * @param {string} distinctId
 * @param {string} insertId
 * @param {number} timeMs
 */
const keyOf = (name, distinctId, insertId, timeMs) => JSON.stringify([name, distinctId, insertId, timeMs]);

/**
 * @typedef {{ before: number, matched: boolean }} DeleteRow
 * @typedef {{ noiseRemoved: Record<string, number>, noiseSeen: Record<string, number> }} NoiseCounts
 */

class DeleteIndex {
	constructor() {
		/** @type {Map<string, DeleteRow[]>} */
		this.byKey = new Map();
		/** @type {DeleteRow[]} */
		this.rows = [];
	}

	/** the number of valid `$delete` events indexed */
	get size() {
		return this.rows.length;
	}

	/**
	 * index a record when it is a valid `$delete`; anything else is ignored
	 * @param {any} record
	 */
	add(record) {
		if (record?.event !== '$delete') return;
		const props = record.properties;
		if (!isObject(props)) return;
		const target = props.$delete_event_name;
		const insertId = props.$delete_insert_id;
		const before = toNumber(props.$delete_inserted_before);
		const distinctId = props.distinct_id;
		const timeMs = timeMillis(props.time);
		if (typeof target !== 'string' || typeof insertId !== 'string' || before === null) return;
		if (typeof distinctId !== 'string' || timeMs === null) return;
		const row = { before, matched: false };
		const k = keyOf(target, distinctId, insertId, timeMs);
		const rows = this.byKey.get(k);
		if (rows) rows.push(row);
		else this.byKey.set(k, [row]);
		this.rows.push(row);
	}

	/**
	 * whether a record must not be sent; counts the reason in `noise_removed`:
	 * `event:$delete` for every `$delete` event (valid or not), `event:$is_deleted` for an event
	 * with `$is_deleted` true, `event:deleted` for an event a `$delete` deletes
	 * @param {NoiseCounts} job
	 * @param {any} record
	 * @returns {boolean}
	 */
	drop(job, record) {
		const removed = job.noiseRemoved;
		const name = record?.event;
		if (name === '$delete') {
			removed['event:$delete'] = (removed['event:$delete'] || 0) + 1;
			return true;
		}
		const props = record?.properties;
		if (!isObject(props)) return false;
		if (props.$is_deleted === true) {
			removed['event:$is_deleted'] = (removed['event:$is_deleted'] || 0) + 1;
			return true;
		}
		if (this.rows.length === 0) return false;
		const insertId = props.$insert_id;
		const distinctId = props.distinct_id;
		const timeMs = timeMillis(props.time);
		if (typeof insertId !== 'string' || typeof distinctId !== 'string' || timeMs === null) return false;
		// a missing name matches as "", as Go's rec["event"].(string) does
		const rows = this.byKey.get(keyOf(typeof name === 'string' ? name : '', distinctId, insertId, timeMs));
		if (!rows) return false;
		const present = Object.prototype.hasOwnProperty.call(props, '$inserted_at');
		const insertedAt = present ? toNumber(props.$inserted_at) : null;
		for (const row of rows) {
			if (present && (insertedAt === null || insertedAt >= row.before)) continue;
			row.matched = true;
			removed['event:deleted'] = (removed['event:deleted'] || 0) + 1;
			return true;
		}
		return false;
	}

	/**
	 * count, in `noise_seen` as `event:$delete:no_target`, every valid `$delete` that deleted
	 * nothing in this run; call once, after the last drop()
	 * @param {NoiseCounts} job
	 */
	countOrphans(job) {
		const seen = job.noiseSeen;
		for (const row of this.rows) {
			if (!row.matched) seen['event:$delete:no_target'] = (seen['event:$delete:no_target'] || 0) + 1;
		}
	}
}

module.exports = { DeleteIndex, timeMillis };
