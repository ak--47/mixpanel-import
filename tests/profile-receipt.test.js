// @ts-nocheck
/* eslint-disable no-undef */

/**
 * Profile receipts must count the batch they describe.
 *
 * `/engage` answers `{status: 1}` with no per-record count, so for
 * `recordType: 'user'` and `'group'` the importer fell back to
 * `job.success += job.lastBatchLength`. That is ONE scalar on the job,
 * overwritten by `addBatchLength()` at every batch DISPATCH. With `workers > 1`
 * every batch is in flight before any response lands, so each response credited
 * whichever batch was queued last.
 *
 * Measured over the 7 days to 2026-09-16 against 20 production runs from DM4:
 * `success === ceil(E / 2000) * (E mod 2000 || 2000)` in all 20. A 5,000-user
 * import with `failed: 0` and every response `200` reported `success: 3`.
 * It reconciled only when there was a single batch, or when every batch was
 * exactly full.
 */

const got = require('got');

jest.mock('got', () => jest.fn());

const { flushToMixpanel } = require('../components/importers.js');

/** A job double carrying only what the profile branch reads and writes. */
function jobDouble(recordType = 'user') {
	return {
		recordType,
		url: 'https://api.mixpanel.com/engage',
		reqMethod: 'POST',
		strict: true,
		maxRetries: 0,
		abridged: true,
		success: 0,
		failed: 0,
		lastBatchLength: 0,
		errors: {},
		responses: [],
		timer: { start() {}, end() {} },
		store() {},
		addBatchLength(length) { this.lastBatchLength = length; },
		addBadRecord() {},
		getEncoding() { return 'json'; },
	};
}

function batchOf(size, offset = 0) {
	return Array.from({ length: size }, (_, index) => ({ $distinct_id: `user-${offset + index}`, $set: {} }));
}

beforeEach(() => {
	got.mockReset();
	got.mockResolvedValue({ body: JSON.stringify({ status: 1 }) });
});

describe('profile receipts under concurrency', () => {
	/**
	 * Every batch is dispatched before any response lands. `lastBatchLength`
	 * therefore holds the FINAL batch's length by the time the first response is
	 * accounted for, which is exactly the production failure.
	 */
	async function sendConcurrently(eligible, batchSize = 2000, recordType = 'user') {
		const job = jobDouble(recordType);
		const batches = [];
		for (let sent = 0; sent < eligible; sent += batchSize) {
			batches.push(batchOf(Math.min(batchSize, eligible - sent), sent));
		}
		for (const batch of batches) job.addBatchLength(batch.length);
		await Promise.all(batches.map((batch) => flushToMixpanel(batch, job)));
		return { job, batches };
	}

	it.each([1, 1999, 2000, 2001, 4000, 4415, 5000])('accounts for every one of %i eligible profiles', async (eligible) => {
		const { job } = await sendConcurrently(eligible);
		expect(job.success + job.failed).toBe(eligible);
		expect(job.failed).toBe(0);
	});

	it('does not reproduce the batches x lastBatchLength formula', async () => {
		const eligible = 4415;
		const wrong = Math.ceil(eligible / 2000) * (eligible % 2000 || 2000);
		expect(wrong).toBe(1245);
		const { job } = await sendConcurrently(eligible);
		expect(job.success).not.toBe(wrong);
		expect(job.success).toBe(eligible);
	});

	it('counts a failed response against its own batch', async () => {
		got.mockResolvedValue({ body: JSON.stringify({ status: 0, error: 'bad token' }) });
		const { job } = await sendConcurrently(4415);
		expect(job.failed).toBe(4415);
		expect(job.success).toBe(0);
	});

	it('applies the same rule to group profiles', async () => {
		const { job } = await sendConcurrently(3522, 2000, 'group');
		expect(job.success).toBe(3522);
	});

	it('still prefers a per-record count when the endpoint reports one', async () => {
		got.mockResolvedValue({ body: JSON.stringify({ status: 1, num_good_events: 7 }) });
		const { job, batches } = await sendConcurrently(4415);
		expect(job.success).toBe(7 * batches.length);
	});
});
