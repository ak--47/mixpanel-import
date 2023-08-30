const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');
const HTTP_AGENT = new https.Agent({ keepAlive: true, maxSockets: 100 })

/** @typedef {import('./job')} JobConfig */

/**
 * @param  {Object[]} batch
 * @param  {JobConfig} jobConfig
 */
async function flushToMixpanel(batch, jobConfig) {
	try {
		/** @type {Buffer | string} */
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (jobConfig.recordType === 'event' && jobConfig.compress) {
			body = await gzip(body, { level: jobConfig.compressionLevel || 6 });
			jobConfig.encoding = 'gzip';
		}

		/** @type {got.Options} */
		const options = {
			url: jobConfig.url,
			searchParams: {
				ip: 0,
				verbose: 1,
				strict: Number(jobConfig.strict)
			},
			method: jobConfig.reqMethod || 'POST',
			retry: {
				limit: jobConfig.maxRetries || 10,
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
				"Authorization": `${jobConfig.auth}`,
				"Content-Type": jobConfig.contentType,
				"Content-Encoding": jobConfig.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			//consider timeout + agent timeout
			agent: {
				https: HTTP_AGENT
			},
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
					jobConfig.retries++;
					jobConfig.requests++;
					if (error?.response?.statusCode?.toString() === "429") {
						jobConfig.rateLimited++;
					}
					else if (error?.response?.statusCode?.toString()?.startsWith("5")) {
						jobConfig.serverErrors++;
					}
					else {
						jobConfig.clientErrors++;
					}
				}],

			},
			body
		};
		// @ts-ignore
		if (jobConfig.project) options.searchParams.project_id = jobConfig.project;

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

		if (jobConfig.recordType === 'event') {
			jobConfig.success += res.num_records_imported || 0;
			jobConfig.failed += res?.failed_records?.length || 0;
		}
		if (jobConfig.recordType === 'user' || jobConfig.recordType === 'group') {
			if (!res.error || res.status) jobConfig.success += batch.length;
			if (res.error || !res.status) jobConfig.failed += batch.length;
		}

		jobConfig.store(res, success);
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