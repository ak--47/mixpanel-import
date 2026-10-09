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

const os = require('os');
const fs = require('fs');
const path = require('path');
const Job = require('../components/job.js');
const mp = require('../index.js');
const creds = { token: 'dummy-token-nothing-is-sent' };

const tmpFile = (name) => path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'noise-')), name);
const readNdjson = (p) => fs.readFileSync(p, 'utf8').trim().split('\n').filter(Boolean).map(l => JSON.parse(l));
const events = () => [
	{ event: 'a', properties: { distinct_id: 'u1', time: 1700000000, $insert_id: 'i1', $preshuffle_distinct_id: 'old', $is_reshuffled: true } },
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
		expect(out.find(r => r.event === 'a').properties.$is_reshuffled).toBe(true);
		expect(summary.noise_removed).toEqual({ $preshuffle_distinct_id: 1 });
		expect(summary.noise_seen).toEqual({ $is_reshuffled: 1 });
		const ab = await run({ abridged: true });
		expect(ab.summary.noise_removed).toEqual({ $preshuffle_distinct_id: 1 });
		expect(ab.summary.noise_seen).toEqual({ $is_reshuffled: 1 });
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
		expect(summary.noise_removed).toEqual({ $preshuffle_distinct_id: 2 });
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
