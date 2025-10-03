const { flushToMixpanel } = require('./importers.js');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const Job = require('./job');
dayjs.extend(utc);
const got = require('got');
const fs = require('fs');
const { Storage } = require('@google-cloud/storage');
const StreamArray = require('stream-json/streamers/StreamArray');
const readline = require('readline');

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
 * 
 * 
 * @param  {JobConfig} job
 */
async function prepareSCD(job) {
	const { acct = '', pass = '', project = '', token = '',
		scdKey = '', scdLabel = '', scdType = 'string',
		groupKey = "", dataGroupId = ""
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


		// config.org = orgId;

		const projectId = Object.keys(meData.projects)[0];
		const workspaceId = Object.keys(meData.workspaces)[0];
		const orgId = Object.keys(meData.organizations)[0];
		// const projectDetails = meData.workspaces[projectId]
		job.project = projectId;
		job.workspace = workspaceId;
		job.org = orgId;

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

	if (groupKey && !dataGroupId) {
		/** @type {got.Options} */
		const requestData = {
			url: `https://mixpanel.com/api/app/projects/${job.project}/data-groups/`,
			...auth
		};
		const { results: metadata } = await got(requestData).json();
		const groupEntry = metadata.find((group) => group?.property_name === groupKey);
		const foundDataGroupId = groupEntry?.data_group_id;

		if (!foundDataGroupId) {
			debugger;
			throw new Error(`could not find dataGroupId for ${groupKey}`);
		}
		job.dataGroupId = foundDataGroupId;


	}

	// DATA DFNs API
	//https://mixpanel.com/api/app/projects/{{ _.project_id }}/data-definitions/events

	/** @type {got.Options} */
	const dataDfnReq = {
		url: `https://mixpanel.com/api/app/projects/${job.project}/data-definitions/events`,
		method: "PATCH",
		json: {
			"name": scdLabel,
			"isScd": true,
			"hidden": true
		},
		...auth
	};

	const dataDfn = await got(dataDfnReq).json();
	const { results: dfnResults } = dataDfn;
	const scdId = dfnResults.id;
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

	if (job.dataGroupId) propsReq.json.dataGroupId = job.dataGroupId;

	const propDfn = await got(propsReq).json();
	const { results: propResults } = propDfn;
	const propId = propResults.id;
	if (!propId) {
		throw new Error('Property not created');
	}
	job.scdPropId = propId;


	return job;
}

/**
 * Load and parse data from a file (local or GCS)
 * @param {string} filePath - Path to file (local path or gs:// URL)
 * @param {boolean} [isJson=true] - Whether to parse as JSON/JSONL (true) or return as string (false)
 * @returns {Promise<Array|string>} Parsed objects array if isJson=true, string if isJson=false
 */
async function loadFile(filePath, isJson = true) {
	if (typeof filePath !== 'string') {
		throw new Error('File path must be a string');
	}

	// Handle Google Cloud Storage paths
	if (filePath.startsWith('gs://')) {
		return await loadGCSFile(filePath, isJson);
	}

	// Handle local files
	return await loadLocalFile(filePath, isJson);
}

/**
 * Load data from Google Cloud Storage
 * @param {string} gcsPath - GCS path in format gs://bucket/path/to/file
 * @param {boolean} isJson - Whether to parse as JSON/JSONL
 * @returns {Promise<Array|string>} Parsed data
 */
async function loadGCSFile(gcsPath, isJson) {
	const storage = new Storage();
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	
	if (!matches) {
		throw new Error(`Invalid GCS path format: ${gcsPath}. Expected gs://bucket/path/to/file`);
	}

	const [, bucketName, fileName] = matches;
	const bucket = storage.bucket(bucketName);
	const file = bucket.file(fileName);

	// Check if file exists
	const [exists] = await file.exists();
	if (!exists) {
		throw new Error(`File not found: ${gcsPath}`);
	}

	if (!isJson) {
		// Return as string
		const [buffer] = await file.download();
		return buffer.toString('utf8');
	}

	// Parse as JSON/JSONL
	const data = [];
	const stream = file.createReadStream();
	
	// Check if file is gzipped based on extension
	const isGzipped = fileName.endsWith('.gz');
	let processStream = stream;
	
	if (isGzipped) {
		const zlib = require('zlib');
		processStream = stream.pipe(zlib.createGunzip());
	}

	// Detect if it's JSON array or JSONL
	const firstChunk = await new Promise((resolve, reject) => {
		let firstData = '';
		const tempStream = file.createReadStream({ start: 0, end: 1023 });
		
		if (isGzipped) {
			const zlib = require('zlib');
			tempStream.pipe(zlib.createGunzip()).on('data', chunk => {
				firstData += chunk.toString();
			}).on('end', () => resolve(firstData.trim())).on('error', reject);
		} else {
			tempStream.on('data', chunk => {
				firstData += chunk.toString();
			}).on('end', () => resolve(firstData.trim())).on('error', reject);
		}
	});

	if (firstChunk.startsWith('[')) {
		// JSON array format
		return new Promise((resolve, reject) => {
			const jsonStream = StreamArray.withParser();
			processStream
				.pipe(jsonStream)
				.on('data', (item) => {
					data.push(item.value);
				})
				.on('end', () => resolve(data))
				.on('error', reject);
		});
	} else {
		// JSONL format
		return new Promise((resolve, reject) => {
			const rl = readline.createInterface({
				input: processStream,
				crlfDelay: Infinity
			});

			rl.on('line', (line) => {
				if (line.trim()) {
					try {
						data.push(JSON.parse(line));
					} catch (e) {
						reject(new Error(`Invalid JSON on line: ${line.substring(0, 100)}...`));
					}
				}
			});

			rl.on('close', () => resolve(data));
			rl.on('error', reject);
		});
	}
}

/**
 * Load data from local file
 * @param {string} filePath - Local file path
 * @param {boolean} isJson - Whether to parse as JSON/JSONL
 * @returns {Promise<Array|string>} Parsed data
 */
async function loadLocalFile(filePath, isJson) {
	// Check if file exists
	if (!fs.existsSync(filePath)) {
		throw new Error(`File not found: ${filePath}`);
	}

	if (!isJson) {
		// Return as string
		return fs.readFileSync(filePath, 'utf8');
	}

	// Parse as JSON/JSONL
	const data = [];
	
	// Check if file is gzipped
	const isGzipped = filePath.endsWith('.gz');
	let stream = fs.createReadStream(filePath);
	
	if (isGzipped) {
		const zlib = require('zlib');
		stream = stream.pipe(zlib.createGunzip());
	}

	// Peek at first few bytes to determine format
	const firstChunk = await new Promise((resolve, reject) => {
		const chunks = [];
		let bytesRead = 0;
		const maxBytes = 1024;
		
		const tempStream = fs.createReadStream(filePath, { start: 0, end: maxBytes - 1 });
		
		if (isGzipped) {
			const zlib = require('zlib');
			tempStream.pipe(zlib.createGunzip()).on('data', chunk => {
				chunks.push(chunk);
				bytesRead += chunk.length;
			}).on('end', () => {
				resolve(Buffer.concat(chunks).toString('utf8').trim());
			}).on('error', reject);
		} else {
			tempStream.on('data', chunk => {
				chunks.push(chunk);
				bytesRead += chunk.length;
			}).on('end', () => {
				resolve(Buffer.concat(chunks).toString('utf8').trim());
			}).on('error', reject);
		}
	});

	if (firstChunk.startsWith('[')) {
		// JSON array format
		return new Promise((resolve, reject) => {
			const jsonStream = StreamArray.withParser();
			stream
				.pipe(jsonStream)
				.on('data', (item) => {
					data.push(item.value);
				})
				.on('end', () => resolve(data))
				.on('error', reject);
		});
	} else {
		// JSONL format
		return new Promise((resolve, reject) => {
			const rl = readline.createInterface({
				input: stream,
				crlfDelay: Infinity
			});

			rl.on('line', (line) => {
				if (line.trim()) {
					try {
						data.push(JSON.parse(line));
					} catch (e) {
						reject(new Error(`Invalid JSON on line: ${line.substring(0, 100)}...`));
					}
				}
			});

			rl.on('close', () => resolve(data));
			rl.on('error', reject);
		});
	}
}


module.exports = {
	validateToken,
	prepareSCD,
	loadFile,
};