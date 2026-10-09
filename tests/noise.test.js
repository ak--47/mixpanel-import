// @ts-nocheck
/* eslint-disable no-undef */
const { noiseFilter, MIXPANEL_NOISE } = require('../components/transforms.js');

const jobStub = () => ({ noiseRemoved: {}, noiseSeen: {} });
const DROP = MIXPANEL_NOISE.drop.map(e => e.key);
const WATCH = MIXPANEL_NOISE.watch.map(e => e.key);

describe('noiseFilter', () => {
	test('list shape: drop is exactly $preshuffle_distinct_id; every entry has evidence', () => {
		expect(DROP).toEqual(['$preshuffle_distinct_id']);
		for (const e of [...MIXPANEL_NOISE.drop, ...MIXPANEL_NOISE.watch, ...MIXPANEL_NOISE.watch_events]) {
			expect(typeof e.why).toBe('string');
			expect(e.evidence.length).toBeGreaterThan(0);
			expect(e.added).toMatch(/^\d{4}-\d{2}-\d{2}$/);
		}
		expect(WATCH).toContain('$is_reshuffled');
		expect(MIXPANEL_NOISE.watch_events.map(e => e.event)).toEqual(['$delete']);
	});

	test('deletes drop keys, keeps every other key and value byte-identical', () => {
		const job = jobStub();
		const rec = { event: 'e', properties: { distinct_id: 'u1', time: 1, $preshuffle_distinct_id: 'old', $is_reshuffled: true, a: { b: [1, null] } } };
		const expected = JSON.parse(JSON.stringify(rec));
		delete expected.properties.$preshuffle_distinct_id;
		const out = noiseFilter(job)(rec);
		expect(JSON.stringify(out)).toBe(JSON.stringify(expected));
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 1 });
		expect(job.noiseSeen).toEqual({ $is_reshuffled: 1 });
	});

	test('counts are exact over a mixed batch, including watched event names', () => {
		const job = jobStub();
		const f = noiseFilter(job);
		const recs = [
			{ event: 'a', properties: { $preshuffle_distinct_id: 'x', $is_reshuffled: true } },
			{ event: 'a', properties: { $preshuffle_distinct_id: 'y' } },
			{ event: '$delete', properties: { $delete_event_name: 'a', $delete_insert_id: 'i' } },
			{ event: 'b', properties: { plain: 1 } }
		];
		recs.forEach(r => f(r));
		expect(job.noiseRemoved).toEqual({ $preshuffle_distinct_id: 2 });
		expect(job.noiseSeen).toEqual({ $is_reshuffled: 1, $delete_event_name: 1, $delete_insert_id: 1, 'event:$delete': 1 });
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
