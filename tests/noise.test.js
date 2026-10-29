// @ts-nocheck
/* eslint-disable no-undef */
const { noiseFilter, MIXPANEL_NOISE } = require('../components/transforms.js');

const jobStub = () => ({ noiseRemoved: {}, noiseSeen: {} });
const DROP = MIXPANEL_NOISE.drop.map(e => e.key);
const DELETE_KEYS = MIXPANEL_NOISE.delete_keys.map(e => e.key);
const WATCH = MIXPANEL_NOISE.watch.map(e => e.key);

describe('noiseFilter', () => {
	test('list shape: version 2, 23 drop keys, six delete_keys, watch empty; every entry has evidence', () => {
		expect(MIXPANEL_NOISE.version).toBe(2);
		expect(DROP).toEqual([
			'$preshuffle_distinct_id', '$mp_api_endpoint', '$mp_api_timestamp_ms', '$mp_event_size',
			'mp_processing_time_ms', '$warehouse_import_id', '$warehouse_import_job_id',
			'$warehouse_import_run_id', '$warehouse_table', '$warehouse_type', '$import',
			'$mp_actor_type', '$mp_raw_user_agent', 'mp_original_distinct_id', 'mp_original_event_name',
			'$is_reshuffled', '$original_distinct_id', '$identity_failure_reason', '$had_persisted_distinct_id',
			'$version', '$possible_duplicate', '$sampling_factor', '$bucket'
		]);
		expect(DELETE_KEYS).toEqual([
			'$is_deleted', '$delete_event_name', '$delete_insert_id', '$delete_inserted_before', '$inserted_at',
			'$distinct_id_before_identity'
		]);
		expect(WATCH).toEqual([]);
		for (const e of [...MIXPANEL_NOISE.drop, ...MIXPANEL_NOISE.delete_keys, ...MIXPANEL_NOISE.watch_events]) {
			expect(typeof e.why).toBe('string');
			expect(e.evidence.length).toBeGreaterThan(0);
			expect(e.added).toMatch(/^\d{4}-\d{2}-\d{2}$/);
		}
		expect(MIXPANEL_NOISE.watch_events.map(e => e.event)).toEqual(['$delete']);
	});

	test('deletes drop keys, keeps every other key and value byte-identical', () => {
		const job = jobStub();
		const rec = { event: 'e', properties: { distinct_id: 'u1', time: 1, $preshuffle_distinct_id: 'old', $is_reshuffled: true, $is_deleted: false, a: { b: [1, null] } } };
		const expected = JSON.parse(JSON.stringify(rec));
		delete expected.properties.$preshuffle_distinct_id;
		delete expected.properties.$is_reshuffled;
		const out = noiseFilter(job)(rec);
		expect(JSON.stringify(out)).toBe(JSON.stringify(expected));
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 1, $is_reshuffled: 1 });
		expect(job.noiseSeen).toEqual({ $is_deleted: 1 });
	});

	const mixedBatch = () => [
		{ event: 'a', properties: { $preshuffle_distinct_id: 'x', $is_reshuffled: true } },
		{ event: 'a', properties: { $preshuffle_distinct_id: 'y' } },
		{ event: '$delete', properties: { $delete_event_name: 'a', $delete_insert_id: 'i' } },
		{ event: 'b', properties: { plain: 1 } }
	];

	test('deletes not applied: counts are exact over a mixed batch; delete_keys and $delete are seen and kept', () => {
		const job = jobStub();
		const f = noiseFilter(job);
		const recs = mixedBatch();
		recs.forEach(r => f(r));
		expect(recs[2].properties).toEqual({ $delete_event_name: 'a', $delete_insert_id: 'i' });
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 2, $is_reshuffled: 1 });
		expect(job.noiseSeen).toEqual({ $delete_event_name: 1, $delete_insert_id: 1, 'event:$delete': 1 });
	});

	test('deletes applied: delete_keys are deleted like drop keys', () => {
		const job = { ...jobStub(), noiseDeletesApplied: true };
		const f = noiseFilter(job);
		const recs = mixedBatch();
		recs.forEach(r => f(r));
		expect(recs[2].properties).toEqual({});
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 2, $is_reshuffled: 1, $delete_event_name: 1, $delete_insert_id: 1 });
		expect(job.noiseSeen).toEqual({ 'event:$delete': 1 });
	});

	test.each([null, '', 0, false])('presence alone counts: value %p', (v) => {
		const job = jobStub();
		const rec = { event: 'e', properties: { $preshuffle_distinct_id: v, $is_deleted: v } };
		noiseFilter(job)(rec);
		expect('$preshuffle_distinct_id' in rec.properties).toBe(false);
		expect(rec.properties.$is_deleted).toBe(v);
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 1 });
		expect(job.noiseSeen).toEqual({ $is_deleted: 1 });
	});

	test.each([undefined, null, 'str', 7, [{ $preshuffle_distinct_id: 'x' }]])('properties %p: untouched, nothing counted', (props) => {
		const job = jobStub();
		const rec = props === undefined ? { event: '$delete' } : { event: '$delete', properties: props };
		const before = JSON.stringify(rec);
		expect(noiseFilter(job)(rec)).toBe(rec);
		expect(JSON.stringify(rec)).toBe(before);
		expect(job.noiseRemoved).toEqual({});
		expect(job.noiseSeen).toEqual({});
	});

	test('nested and root-level listed keys are untouched', () => {
		const job = jobStub();
		const rec = { event: 'e', $preshuffle_distinct_id: 'root', properties: { meta: { $preshuffle_distinct_id: 'nested', $is_deleted: true } } };
		const before = JSON.stringify(rec);
		noiseFilter(job)(rec);
		expect(JSON.stringify(rec)).toBe(before);
		expect(job.noiseRemoved).toEqual({});
		expect(job.noiseSeen).toEqual({});
	});

	test('inherited keys are not own keys: untouched', () => {
		const job = jobStub();
		const props = Object.create({ $preshuffle_distinct_id: 'proto' });
		props.a = 1;
		noiseFilter(job)({ event: 'e', properties: props });
		expect(props.$preshuffle_distinct_id).toBe('proto');
		expect(job.noiseRemoved).toEqual({});
	});

	test('a custom list replaces the default', () => {
		const job = jobStub();
		const list = { drop: [{ key: 'zap' }], watch: [], watch_events: [] };
		const rec = { event: 'e', properties: { zap: 1, $preshuffle_distinct_id: 'x' } };
		noiseFilter(job, list)(rec);
		expect(rec.properties).toEqual({ $preshuffle_distinct_id: 'x' });
		expect(job.noiseRemoved).toEqual({ zap: 1 });
	});
});

const os = require('os');
const fs = require('fs');
const path = require('path');
const Job = require('../components/job.js');
const mp = require('../index.js');
const creds = { token: 'dummy-token-nothing-is-sent' };

const tmpFile = (name) => path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'noise-')), name);
const readNdjson = (p) => fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
const events = () => [
	{ event: 'a', properties: { distinct_id: 'u1', time: 1700000000, $insert_id: 'i1', $preshuffle_distinct_id: 'old', $is_deleted: false } },
	{ event: 'b', properties: { distinct_id: 'u2', time: 1700000001, $insert_id: 'i2' } }
];
const run = (opts, data = events()) => {
	const dest = tmpFile('out.ndjson');
	return mp(creds, data, { recordType: 'event', destination: dest, destinationOnly: true, ...opts })
		.then(summary => ({ summary, out: readNdjson(dest) }));
};

describe('filterMixpanelNoise option', () => {
	test('default is on: step is last, maps start empty', () => {
		const job = new Job(creds, { recordType: 'event', fixTime: true });
		expect(job.filterMixpanelNoise).toBe(true);
		expect(job.activeTransforms.at(-1).name).toBe('noiseFilter');
		expect(job.noiseRemoved).toEqual({});
		expect(job.noiseSeen).toEqual({});
	});

	test('export-import-event gets the step', () => {
		const job = new Job(creds, { recordType: 'export-import-event' });
		expect(job.activeTransforms.at(-1).name).toBe('noiseFilter');
	});

	test.each([[{ recordType: 'user' }], [{ recordType: 'group' }], [{ recordType: 'event', fastMode: true }]])('default does nothing on %p', (opts) => {
		const job = new Job(creds, opts);
		expect(job.activeTransforms.map(t => t.name)).not.toContain('noiseFilter');
	});

	test('explicit true throws with fastMode and with a non-event recordType', () => {
		expect(() => new Job(creds, { recordType: 'event', fastMode: true, filterMixpanelNoise: true })).toThrow(/filterMixpanelNoise/);
		expect(() => new Job(creds, { recordType: 'user', filterMixpanelNoise: true })).toThrow(/filterMixpanelNoise/);
		expect(() => new Job(creds, { recordType: 'event', filterMixpanelNoise: true })).not.toThrow();
	});

	test('option absent: keys removed, counts in the full and abridged summary', async () => {
		const { summary, out } = await run({});
		expect(out.find(r => r.event === 'a').properties).not.toHaveProperty('$preshuffle_distinct_id');
		expect(out.find(r => r.event === 'a').properties).not.toHaveProperty('$is_deleted');
		expect(summary.noise_removed).toEqual({ $preshuffle_distinct_id: 1, $is_deleted: 1 });
		expect(summary.noise_seen).toEqual({});
		const ab = await run({ abridged: true });
		expect(ab.summary.noise_removed).toEqual({ $preshuffle_distinct_id: 1, $is_deleted: 1 });
		expect(ab.summary.noise_seen).toEqual({});
		const keys = Object.keys(ab.summary);
		expect(keys.indexOf('noise_removed')).toBe(keys.indexOf('errors') + 1);
		expect(keys.indexOf('noise_seen')).toBe(keys.indexOf('errors') + 2);
	});

	test('false: output identical to input, maps empty', async () => {
		const input = events();
		const { summary, out } = await run({ filterMixpanelNoise: false }, JSON.parse(JSON.stringify(input)));
		expect(out).toEqual(input);
		expect(summary.noise_removed).toEqual({});
		expect(summary.noise_seen).toEqual({});
	});

	test('user records with the default: unchanged, maps empty', async () => {
		const users = [{ $distinct_id: 'u1', $set: { $preshuffle_distinct_id: 'x' }, properties: { $preshuffle_distinct_id: 'y' } }];
		const { summary, out } = await run({ recordType: 'user' }, JSON.parse(JSON.stringify(users)));
		expect(out[0].properties).toEqual({ $preshuffle_distinct_id: 'y' });
		expect(summary.noise_removed).toEqual({});
	});

	test('runs after transformFunc: the transform still reads the key, and a key it adds is removed', async () => {
		const transformFunc = (r) => { r.properties.saw = r.properties.$preshuffle_distinct_id || 'none'; r.properties.$preshuffle_distinct_id = 'added'; return r; };
		const { summary, out } = await run({ transformFunc });
		expect(out.map(r => r.properties.saw)).toEqual(['old', 'none']);
		expect(out.every(r => !('$preshuffle_distinct_id' in r.properties))).toBe(true);
		expect(summary.noise_removed).toEqual({ $preshuffle_distinct_id: 2, $is_deleted: 1 });
	});

	test('a record epochFilter drops is not counted', async () => {
		const { summary, out } = await run({ epochStart: 1700000001 });
		expect(out.map(r => r.event)).toEqual(['b']);
		expect(summary.noise_removed).toEqual({});
		expect(summary.noise_seen).toEqual({});
	});

	test('CLI does not send an explicit value by default', () => {
		const cliSrc = fs.readFileSync(path.join(__dirname, '../components/cli.js'), 'utf8');
		const block = cliSrc.slice(cliSrc.indexOf('filterMixpanelNoise'), cliSrc.indexOf('filterMixpanelNoise') + 400);
		expect(block).toMatch(/default:\s*undefined/);
	});
});

describe('filterMixpanelNoise with export-import-event', () => {
	test('fixData re-nests a flat exported row; the step then cleans properties', () => {
		const job = new Job(creds, { recordType: 'export-import-event', fixData: true });
		let data = { event: 'a', distinct_id: 'u1', time: 1700000000, $insert_id: 'i1', $preshuffle_distinct_id: 'old', $is_deleted: false };
		for (const t of job.activeTransforms) {
			if (t.mutates === false) data = t.fn(data);
			else t.fn(data);
			if (!data) break;
		}
		expect(data.properties).toBeDefined();
		expect(data.properties).not.toHaveProperty('$preshuffle_distinct_id');
		expect(data.properties.$is_deleted).toBe(false);
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 1 });
		expect(job.noiseSeen).toEqual({ $is_deleted: 1 });
	});
});

describe('noise golden fixture', () => {
	const dir = path.join(__dirname, 'fixtures/noise');
	test('the engine reproduces expected.ndjson and expected-counts.json', async () => {
		const dest = tmpFile('golden.ndjson');
		const summary = await mp(creds, path.join(dir, 'input.ndjson'), { recordType: 'event', streamFormat: 'jsonl', destination: dest, destinationOnly: true, fixData: false, removeNulls: false });
		const expected = readNdjson(path.join(dir, 'expected.ndjson'));
		const counts = JSON.parse(fs.readFileSync(path.join(dir, 'expected-counts.json'), 'utf8'));
		const input = readNdjson(path.join(dir, 'input.ndjson'));
		expect(readNdjson(dest)).toEqual(expected);
		expect({ records: expected.length, noise_removed: summary.noise_removed, noise_seen: summary.noise_seen }).toEqual(counts);
		// total counts every input record; the source's deletes are tallied as empty
		expect(summary.total).toBe(input.length);
		expect(summary.total - summary.empty).toBe(expected.length);
		// the output keeps each record's key order
		expect(fs.readFileSync(dest, 'utf8').trim()).toBe(fs.readFileSync(path.join(dir, 'expected.ndjson'), 'utf8').trim());
	});

	test('npm pack ships components/mixpanel-noise.json', () => {
		const { execSync } = require('child_process');
		const out = execSync('npm pack --dry-run --json', { cwd: path.join(__dirname, '..'), encoding: 'utf8' });
		const files = JSON.parse(out)[0].files.map(f => f.path);
		expect(files).toContain('components/mixpanel-noise.json');
	});
});

// parity: snowcat go-worker/internal/transform/deletes_test.go
describe('DeleteIndex', () => {
	const { DeleteIndex, timeMillis } = require('../components/deletes.js');
	const del = (name, distinctId, insertId, time, before) => ({
		event: '$delete', properties: {
			distinct_id: distinctId, time, $insert_id: 'd-' + insertId,
			$delete_event_name: name, $delete_insert_id: insertId, $delete_inserted_before: before
		}
	});
	const ev = (name, distinctId, insertId, time, extra = {}) => ({ event: name, properties: { distinct_id: distinctId, time, $insert_id: insertId, ...extra } });

	test.each([
		['exact match, no $inserted_at', ev('buy', 'u1', 'i1', 1760000000), true],
		['match on ms time against seconds', ev('buy', 'u1', 'i1', 1760000000000), true],
		['$inserted_at below before', ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: 99 }), true],
		['$inserted_at equal to before (strict <)', ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: 100 }), false],
		['$inserted_at above before', ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: 101 }), false],
		['$inserted_at string', ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: '1' }), false],
		['$inserted_at null', ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: null }), false],
		['time off by 1 ms', ev('buy', 'u1', 'i1', 1760000000001), false],
		['other name', ev('view', 'u1', 'i1', 1760000000), false],
		['other distinct_id', ev('buy', 'u2', 'i1', 1760000000), false],
		['other insert_id', ev('buy', 'u1', 'i2', 1760000000), false],
		['no insert_id', { event: 'buy', properties: { distinct_id: 'u1', time: 1760000000 } }, false],
		['$is_deleted true', ev('x', 'u9', 'i9', 1, { $is_deleted: true }), true],
		['$is_deleted false', ev('x', 'u9', 'i9', 1, { $is_deleted: false }), false],
		['$is_deleted string', ev('x', 'u9', 'i9', 1, { $is_deleted: 'true' }), false],
		['a $delete itself', del('buy', 'u1', 'i1', 1760000000, 100), true]
	])('match rule: %s', (_name, rec, drop) => {
		const x = new DeleteIndex();
		x.add(del('buy', 'u1', 'i1', 1760000000, 100));
		expect(x.drop(jobStub(), rec)).toBe(drop);
	});

	test('invalid $delete rows are not indexed, and are still dropped', () => {
		const bad = [
			del('buy', 'u1', 'i1', 1760000000, '100'), // before not a number
			del('buy', 'u1', 'i1', '1760000000', 100), // time not a number
			del('buy', 'u1', 'i1', 0, 100), // time not a time
			{ event: '$delete', properties: 'nope' }, // properties not an object
			{ event: '$delete', properties: { distinct_id: 'u1', time: 1760000000, $delete_event_name: 'buy', $delete_inserted_before: 100 } },
			{ event: 'buy', properties: { distinct_id: 'u1', time: 1760000000, $delete_event_name: 'buy', $delete_insert_id: 'i1', $delete_inserted_before: 100 } }
		];
		const x = new DeleteIndex();
		bad.forEach(r => x.add(r));
		expect(x.size).toBe(0);
		const job = jobStub();
		for (const r of bad.slice(0, 5)) expect(x.drop(job, r)).toBe(true);
		expect(x.drop(job, ev('buy', 'u1', 'i1', 1760000000))).toBe(false);
		expect(job.noiseRemoved).toEqual({ 'event:$delete': 5 });
	});

	test('counts: two $delete rows for one key, orphans, $is_deleted', () => {
		const x = new DeleteIndex();
		x.add(del('buy', 'u1', 'i1', 1760000000, 100));
		x.add(del('buy', 'u1', 'i1', 1760000000, 200)); // two deletes, one target
		x.add(del('gone', 'u2', 'i2', 1760000001, 100));
		const job = jobStub();
		[
			del('buy', 'u1', 'i1', 1760000000, 100),
			ev('buy', 'u1', 'i1', 1760000000),
			ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: 150 }), // only the 200 delete removes it
			ev('buy', 'u1', 'i1', 1760000000, { $inserted_at: 250 }), // the replacement row stays
			ev('x', 'u3', 'i3', 1, { $is_deleted: true })
		].forEach(r => x.drop(job, r));
		x.countOrphans(job);
		expect(job.noiseRemoved).toEqual({ 'event:$delete': 1, 'event:deleted': 2, 'event:$is_deleted': 1 });
		expect(job.noiseSeen).toEqual({ 'event:$delete:no_target': 1 });
	});

	test.each([
		[1760000000, 1760000000000],
		[1760000000.1239, 1760000000123],
		[BigInt(1760000000), 1760000000000],
		[1760000000123, 1760000000123],
		[1760000000123.9, 1760000000123],
		[2 ** 31, 2 ** 31 * 1000],
		[0, null],
		[-5, null],
		[NaN, null],
		[Infinity, null],
		['1760000000', null],
		[null, null]
	])('timeMillis(%p) = %p', (v, want) => {
		expect(timeMillis(v)).toBe(want);
	});
});

// parity: snowcat go-worker/mixpanel/noise_test.go
describe('filterMixpanelNoise applies the source deletes', () => {
	const { Readable } = require('stream');
	const { prescanDeletes } = require('../components/parsers.js');
	const deleteLine = '{"event":"$delete","properties":{"distinct_id":"u","time":1760000000,"$insert_id":"d","$delete_event_name":"b","$delete_insert_id":"i","$delete_inserted_before":5}}';

	test('a caller\'s stream: nothing applied; delete_keys and event:$delete are seen and kept', async () => {
		const text = [deleteLine, '{"event":"b","properties":{"distinct_id":"u","time":1760000000,"$insert_id":"i","$inserted_at":1}}'].join('\n');
		const dest = tmpFile('out.ndjson');
		const summary = await mp(creds, Readable.from([Buffer.from(text)], { objectMode: false }), { recordType: 'event', streamFormat: 'jsonl', destination: dest, destinationOnly: true });
		expect(readNdjson(dest)).toHaveLength(2);
		expect(summary.noise_seen).toEqual({ 'event:$delete': 1, $delete_event_name: 1, $delete_insert_id: 1, $delete_inserted_before: 1, $inserted_at: 1 });
		expect(summary.noise_removed).toEqual({});
	});

	test('the prescan counts nothing: one unparsable line gives unparsable 1', async () => {
		const src = tmpFile('in.ndjson');
		fs.writeFileSync(src, [
			deleteLine,
			'{"$delete" not json',
			'{"event":"b","properties":{"distinct_id":"u","time":1760000000,"$insert_id":"i"}}',
			'{"event":"c","properties":{"distinct_id":"u","time":1760000000,"$insert_id":"j"}}'
		].join('\n') + '\n');
		const dest = tmpFile('out.ndjson');
		const summary = await mp(creds, src, { recordType: 'event', destination: dest, destinationOnly: true });
		expect(summary.unparsable).toBe(1);
		const out = readNdjson(dest);
		expect(out.map(r => r.event)).toEqual(['c']);
		expect(summary.noise_removed).toEqual({ 'event:$delete': 1, 'event:deleted': 1 });
	});

	test('filterMixpanelNoise: false writes every row and counts nothing', async () => {
		const input = path.join(__dirname, 'fixtures/noise/input.ndjson');
		const dest = tmpFile('out.ndjson');
		const summary = await mp(creds, input, { recordType: 'event', destination: dest, destinationOnly: true, filterMixpanelNoise: false });
		expect(readNdjson(dest)).toHaveLength(readNdjson(input).length);
		expect(summary.noise_removed).toEqual({});
		expect(summary.noise_seen).toEqual({});
	});

	test('an array of objects is read twice: deletes applied', async () => {
		const input = readNdjson(path.join(__dirname, 'fixtures/noise/input.ndjson'));
		const expected = readNdjson(path.join(__dirname, 'fixtures/noise/expected.ndjson'));
		const { out } = await run({ fixData: false, removeNulls: false }, input);
		expect(out).toEqual(expected);
	});

	test('no prescan for vendor jobs, export-import-event, streams, or with the filter off', async () => {
		const input = path.join(__dirname, 'fixtures/noise/input.ndjson');
		const cases = [
			new Job(creds, { recordType: 'event', vendor: 'amplitude' }),
			new Job(creds, { recordType: 'export-import-event' }),
			new Job(creds, { recordType: 'event', filterMixpanelNoise: false }),
			new Job(creds, { recordType: 'event', fastMode: true })
		];
		for (const job of cases) {
			expect(await prescanDeletes(input, job)).toBe(false);
			expect(job.deleteIndex).toBeNull();
			expect(job.noiseDeletesApplied).toBe(false);
		}
		const job = new Job(creds, { recordType: 'event' });
		expect(await prescanDeletes(Readable.from([]), job)).toBe(false);
		expect(await prescanDeletes(input, job)).toBe(true);
		expect(job.deleteIndex.size).toBeGreaterThan(0);
	});
});
