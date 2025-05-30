const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');
const HTTP_AGENT = new https.Agent({ keepAlive: true, maxSockets: 100 });

/** @typedef {import('./job')} JobConfig */




/**
 * @param  {Object[]} batch
 * @param  {JobConfig} job
 */
async function flushToMixpanel(batch, job) {
	try {
		/** @type {Buffer | string} */
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (job.recordType === 'event' && job.compress) {
			body = await gzip(body, { level: job.compressionLevel || 6 });
			job.encoding = 'gzip';
		}

		/** @type {got.Options} */
		const options = {
			url: job.url,
			searchParams: {
				ip: 0,
				verbose: 1,
				strict: Number(job.strict)
			},
			method: job.reqMethod || 'POST',
			retry: {
				limit: job.maxRetries || 10,
				statusCodes: [429, 500, 501, 503, 524, 502, 408, 504],
				errorCodes: [
					`ETIMEDOUT`,
					`ECONNRESET`,
					`EADDRINUSE`,
					`ECONNREFUSED`,
					`EPIPE`,
					`ENOTFOUND`,
					`ENETUNREACH`,
					`EAI_AGAIN`,
					`ESOCKETTIMEDOUT`,
					`ECONNABORTED`,
					`EHOSTUNREACH`,
					`EPROTO`,
					`ETLSHANDSHAKE`
				],
				methods: ['POST']
			},
			headers: {
				"Authorization": `${job.auth}`,
				"Content-Type": job.contentType,
				"Content-Encoding": job.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			//consider timeout + agent timeout
			agent: {
				https: HTTP_AGENT
			},
			http2: false,
			hooks: {
				// @ts-ignore
				beforeRetry: [(req, error, count) => {
					try {
						// @ts-ignore
						l(`got ${error.message}...retrying request...#${count}`);
					}
					catch (e) {
						//noop
					}
					job.retries++;
					job.requests++;
					if (error?.response?.statusCode?.toString() === "429") {
						job.rateLimited++;
					}
					else if (error?.response?.statusCode?.toString()?.startsWith("5")) {
						job.serverErrors++;
					}
					else {
						job.clientErrors++;
					}
				}],

			},
			body
		};

		if (job.http2) {
			options.http2 = true;
			delete options.headers?.Connection;

		}

		// @ts-ignore
		if (job.project) options.searchParams.project_id = job.project;

		let req, res, success;
		try {
			// @ts-ignore
			req = await got(options);
			res = JSON.parse(req.body);
			success = true;
		}

		catch (e) {
			if (u.isJSONStr(e?.response?.body)) {
				res = JSON.parse(e.response.body);
			}
			else {
				res = e;
			}
			success = false;



		}

		if (job.recordType === 'event' || job.recordType === "scd") {
			job.success += res.num_records_imported || 0;
			job.failed += res?.failed_records?.length || 0;
			if (!job.abridged && res?.failed_records?.length) {
				for (const error of res.failed_records) {
					const { index, message } = error;
					if (!job.badRecords[message]) {
						job.badRecords[message] = [];
					}
					job.badRecords[message].push(batch[index]);
				}
			}
		}
		else if (job.recordType === 'user' || job.recordType === 'group') {
			if (!res.error || res.status) {
				if (res.num_good_events) {
					job.success += res.num_good_events;
				}
				else {
					job.success += job.lastBatchLength;
				}
			}
			if (res.error || !res.status) job.failed += job.lastBatchLength;
		}

		job.store(res, success);
		return res;
	}

	catch (e) {
		try {
			// @ts-ignore
			l(`\nBATCH FAILED: ${e.message}\n`);
		}
		catch (e) {
			//noop
		}
	}
}

/**
 * @param  {any} csvString
 * @param  {JobConfig} config
 */
async function flushLookupTable(csvString, config) {
	const res = await flushToMixpanel(csvString, config);
	config.recordsProcessed = csvString.split('\n').length - 1;
	config.success = config.recordsProcessed;
	return res;
}


module.exports = {
	flushToMixpanel,
	flushLookupTable
};