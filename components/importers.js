const got = require('got');
const https = require('https');
const { gzip } = require('node-gzip');
const u = require('ak-tools');

/**
 * @param  {Object[]} batch
 * @param  {Object} config
 */
async function flushToMixpanel(batch, config) {
	try {
		let body = typeof batch === 'string' ? batch : JSON.stringify(batch);
		if (config.recordType === 'event' && config.compress) {
			body = await gzip(body, { level: config.compressionLevel || 6 });
			config.encoding = 'gzip';
		}

		/** @type {got.Options} */
		const options = {
			url: config.url,
			searchParams: {
				ip: 0,
				verbose: 1,
				strict: Number(config.strict)
			},
			method: config.reqMethod,
			retry: {
				limit: config.maxRetries || 10,
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
				"Authorization": `${config.auth}`,
				"Content-Type": config.contentType,
				"Content-Encoding": config.encoding,
				'Connection': 'keep-alive',
				'Accept': 'application/json'
			},
			//consider timeout + agent timeout
			agent: {
				https: new https.Agent({ keepAlive: true, maxSockets: 100 })
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
					config.retries++;
					config.requests++;
					if (error?.response?.statusCode?.toString() === "429") {
						config.rateLimited++;
					}
					else if (error?.response?.statusCode?.toString()?.startsWith("5")) {
						config.serverErrors++;
					}
					else {
						config.clientErrors++;
					}
				}],

			},
			body
		};
		// @ts-ignore
		if (config.project) options.searchParams.project_id = config.project;

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

		if (config.recordType === 'event') {
			config.success += res.num_records_imported || 0;
			config.failed += res?.failed_records?.length || 0;
		}
		if (config.recordType === 'user' || config.recordType === 'group') {
			if (!res.error || res.status) config.success += batch.length;
			if (res.error || !res.status) config.failed += batch.length;
		}

		config.store(res, success);
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

async function flushLookupTable(stream, config) {
	const res = await flushToMixpanel(stream, config);
	config.recordsProcessed = stream.split('\n').length - 1;
	config.success = config.recordsProcessed;
	return res;
}


module.exports = {
	flushToMixpanel,
	flushLookupTable
};