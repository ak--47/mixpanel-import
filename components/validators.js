const { flushToMixpanel } = require('./importers.js');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const Job = require('./job');
dayjs.extend(utc);
const got = require('got');

/** @typedef {import('./job')} JobConfig */


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
	};

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

/**
 * enriches the jobConfig with SCD specific details
 * @param  {JobConfig} job
 */
async function prepareSCD(job) {
	const { acct = '', pass = '', project = '', token = '',
		scdKey = '', scdLabel = '', scdType = 'string'
	} = job;
	if (!acct || !pass) {
		throw new Error('Missing Credentials; both `acct` and `pass` are required');
	}

	if (!scdKey || !scdLabel) {
		throw new Error('Missing SCD Key or SCD Label');
	}

	const auth = { username: acct, password: pass };


	if (!project) {
		/** @type {got.Options} */
		const requestData = {
			url: "https://mixpanel.com/api/app/me/?include_workspace_users=false",
			...auth

		};
		const meReq = await got(requestData).json();
		const meData = meReq?.results;

		if (!meData) {
			throw new Error('Invalid Credentials');
		}

		// const orgId = Object.keys(meData.organizations)[0];
		// config.org = orgId;

		const projectId = Object.keys(meData.workspaces)[0];
		// const projectDetails = meData.workspaces[projectId]
		job.project = projectId;
	}

	if (!token) {
		/** @type {got.Options} */
		const requestData = {
			url: `https://mixpanel.com/settings/project/${job.project}/metadata`,
			...auth
		};
		const { results: metadata } = await got(requestData).json();
		const { secret, token } = metadata;
		job.secret = secret;
		job.token = token;
	}

	// DATA DFNs API
	//https://mixpanel.com/api/app/projects/{{ _.project_id }}/data-definitions/events

	/** @type {got.Options} */
	const dataDfnReq = {
		url: `https://mixpanel.com/api/app/projects/${job.project}/data-definitions/events`,
		method: "PATCH",
		json: {
			"name": scdLabel,
			"isScd": true
		},
		...auth
	};

	const dataDfn = await got(dataDfnReq).json();
	const { results: dfnResults } = dataDfn;
	const scdId = dfnResults.id
	if (!scdId) {
		throw new Error('SCD not created');
	}
	job.scdId = scdId;
	/** @type {got.Options} */
	const propsReq = {
		url: `https://mixpanel.com/api/app/projects/${job.project}/data-definitions/properties`,
		method: "PATCH",
		json: {
			"name": `$scd:${scdKey}`,
			"resourceType": "User",
			"scdEvent": scdId,
			"type": scdType
		},
		...auth
	};

	if (job.dataGroupId) propsReq.body.dataGroupId = job.dataGroupId;

	const propDfn = await got(propsReq).json();
	const { results: propResults } = propDfn;
	const propId = propResults.id;
	if (!propId) {
		throw new Error('Property not created');
	}
	job.scdPropId = propId;
	

	return job;
}


module.exports = {
	validateToken,
	prepareSCD,
};