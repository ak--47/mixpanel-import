const { flushToMixpanel } = require('./importers.js');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const Job = require('./job');
dayjs.extend(utc);


/**
 * Validates a mixpanel token; kinda experimental
 * @param  {string} token
 * @returns {Promise<{token: string, valid: boolean, type: string}>} details about the token; type is either 'idmgmt_v2', 'idmgmt_v3', or 'unknown'
 */
async function validateToken(token) {
	if (!token) {
		throw new Error('No Token');
	}
	if (typeof token !== 'string') {
		throw new Error('Token must be a string');
	}
	if (token.length !== 32) {
		throw new Error('Token must be 32 characters');
	}

	// this event is only valid in v3
	// ? https://docs.mixpanel.com/docs/tracking-methods/id-management/migrating-to-simplified-id-merge-system#legacy-id-management
	const veryOldEventBatch = [{
		event: 'test',
		properties: {
			time: dayjs.utc().subtract(4, 'year').valueOf(),
			$device_id: 'knock',
			$user_id: 'knock',
			$insert_id: 'whose',
			there: "..."
		}
	}];

	const result = {
		token,
		valid: false,
		type: ''
	}

	const config = new Job({ token }, { recordType: 'event', strict: true, verbose: false, logs: false, compress: false, region: 'US' });
	const res = await flushToMixpanel(veryOldEventBatch, config);
	
	//v2_compat requires distinct_id
	if (res.code === 400) {
		if (res.failed_records[0].message === `'properties.distinct_id' is invalid: must not be missing`) {
			result.valid = true;
			result.type = 'idmgmt_v2';
		}

		else {
			result.valid = false;
			result.type = 'unknown';		
		}
	}

	//v3 does not require distinct_id
	if (res.code === 200) {
		if (res.num_records_imported === 1) {
			result.valid = true;
			result.type = 'idmgmt_v3';
		}

		else {
			result.valid = false;
			result.type = 'unknown';
		}
	}

	return result;
}


module.exports = {
	validateToken
};