const readline = require('readline');
const stream = require('stream');
const got = require('got');
const https = require('https');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const u = require('ak-tools');
const showProgress = require('./cli').showProgress;
const { Transform, Readable } = require('stream');

const { Storage } = require('@google-cloud/storage');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

let mainFunc;
function getMain() {
	if (!mainFunc) {
		mainFunc = require('../index.js');
	}
	return mainFunc;
}


/** @typedef {import('./job')} jobConfig */

/**
 * @param  {string} filename
 * @param  {jobConfig} job
 */
async function exportEvents(filename, job) {
	const pipeline = promisify(stream.pipeline);
	const { skipWriteToDisk = false, limit, whereClause } = job;

	/** @type {got.Options} */
	const options = {
		url: job.url,
		searchParams: {
			from_date: job.start,
			to_date: job.end,
			// Merge in arbitrary params from job.params
			...job.params
		},
		method: job.reqMethod,
		retry: { limit: 50 },
		headers: {
			"Authorization": `${job.auth}`
		},
		agent: {
			https: new https.Agent({ keepAlive: true })
		},
		hooks: {
			// @ts-ignore
			beforeRetry: [(err, count) => {
				// @ts-ignore
				l(`retrying request...#${count}`);
				job.retries++;
			}]
		},

	};

	// @ts-ignore
	if (limit && typeof limit === 'number') options.searchParams.limit = limit;
	// @ts-ignore
	if (whereClause && typeof whereClause === 'string') options.searchParams.where = whereClause;

	// @ts-ignore
	if (job.project) options.searchParams.project_id = job.project;

	// @ts-ignore
	const request = got.stream(options);

	request.on('response', (res) => {
		job.requests++;
		job.responses.push({
			status: res.statusCode,
			ip: res.ip,
			url: res.requestUrl,
			...res.headers
		});
	});

	request.on('error', (e) => {
		job.failed++;
		job.responses.push({
			status: e.statusCode,
			ip: e.ip,
			url: e.requestUrl,
			...e.headers,
			message: e.message
		});
		throw e;

	});

	request.on('downloadProgress', (progress) => {
		downloadProgress(progress.transferred);
	});

	// Define streams upfront
	const cloudInfo = detectCloudDestination(filename);
	let fileStream;

	if (cloudInfo.isCloud) {
		if (cloudInfo.provider === 'gcs') {
			fileStream = createGCSWriteStream(filename, job);
		} else if (cloudInfo.provider === 's3') {
			fileStream = createS3WriteStream(filename, job);
		}
	} else {
		fileStream = fs.createWriteStream(filename);
	}

	// Create a unified processing stream that handles both memory and file/cloud output
	let buffer = "";
	const processingStream = new stream.Writable({
		write(chunk, encoding, callback) {
			// Convert the chunk to a string and append it to the buffer
			buffer += chunk.toString();

			// Split the buffer into lines
			const lines = buffer.split("\n");

			// Keep the last partial line in the buffer (if any)
			buffer = lines.pop() || "";

			// Process each complete line
			lines.forEach(line => {
				if (!line.trim()) return;

				try {
					let row = JSON.parse(line.trim());

					// Apply transform function if provided
					if (job.transformFunc && typeof job.transformFunc === 'function') {
						try {
							const transformed = job.transformFunc(row);
							// Handle case where transform returns array (explosion)
							if (Array.isArray(transformed)) {
								transformed.forEach(item => {
									if (skipWriteToDisk) {
										allResults.push(item);
									} else {
										fileStream.write(JSON.stringify(item) + '\n');
									}
								});
								return;
							} else if (transformed) {
								row = transformed;
							}
						} catch (transformError) {
							// Log transform error but continue processing
							if (job.verbose) {
								console.warn(`Transform error on record: ${transformError.message}`);
							}
							// Use original row if transform fails
						}
					}

					// Write the (possibly transformed) record
					if (skipWriteToDisk) {
						allResults.push(row);
					} else {
						fileStream.write(JSON.stringify(row) + '\n');
					}
				}
				catch (parseError) {
					// Skip malformed lines
					if (job.verbose) {
						console.warn(`Parse error on line: ${parseError.message}`);
					}
				}
			});

			callback();
		},

		final(callback) {
			// Process the remaining data in the buffer as the last line
			if (buffer.trim()) {
				try {
					let row = JSON.parse(buffer.trim());

					// Apply transform function if provided
					if (job.transformFunc && typeof job.transformFunc === 'function') {
						try {
							const transformed = job.transformFunc(row);
							if (Array.isArray(transformed)) {
								transformed.forEach(item => {
									if (skipWriteToDisk) {
										allResults.push(item);
									} else {
										fileStream.write(JSON.stringify(item) + '\n');
									}
								});
								// Ensure cloud streams are properly finalized
								if (!skipWriteToDisk && cloudInfo.isCloud) {
									fileStream.end();
								}
								callback();
								return;
							} else if (transformed) {
								row = transformed;
							}
						} catch (transformError) {
							if (job.verbose) {
								console.warn(`Transform error on final record: ${transformError.message}`);
							}
						}
					}

					if (skipWriteToDisk) {
						allResults.push(row);
					} else {
						fileStream.write(JSON.stringify(row) + '\n');
					}
				}
				catch (parseError) {
					if (job.verbose) {
						console.warn(`Parse error on final line: ${parseError.message}`);
					}
				}
			}

			// Ensure cloud streams are properly finalized
			if (!skipWriteToDisk && cloudInfo.isCloud) {
				fileStream.end();
			}
			callback();
		}
	});

	const allResults = [];

	// Choose the appropriate stream based on whether transforms are needed
	let outputStream;
	if (skipWriteToDisk || (job.transformFunc && typeof job.transformFunc === 'function')) {
		// Use processing stream when we need transforms or memory mode
		outputStream = processingStream;
	} else {
		// Use direct file stream when no transforms (more efficient for cloud)
		outputStream = fileStream;
	}

	// Use the chosen stream in the pipeline
	await pipeline(request, outputStream);
	if (job.verbose) console.log('\n\ndownload finished\n\n');
	if (skipWriteToDisk) {
		job.recordsProcessed += allResults.length;
		job.success += allResults.length;
		job.dryRunResults.push(...allResults);
		return allResults;
	}

	if (cloudInfo.isCloud) {
		// For cloud storage, we can't easily count lines after upload
		// Count lines from the memory buffer if available, or estimate
		const lines = allResults.length || 0;
		job.recordsProcessed += lines;
		job.success += lines;
		job.file = filename;
		return filename;
	} else {
		// For local files, count lines from the file
		const lines = await countFileLines(filename);
		job.recordsProcessed += lines;
		job.success += lines;
		job.file = filename;
		return filename;
	}

}

/**
 * @param  {string} folder
 * @param  {jobConfig} job
 */
async function exportProfiles(folder, job) {
	const auth = job.auth;
	const { skipWriteToDisk = false } = job;
	// EITHER be a list of files ^ OR a list of objects in memory
	const allResults = [];
	let entityName = `users`;
	if (job.dataGroupId) entityName = `group`;

	const cloudInfo = detectCloudDestination(folder);
	let iterations = 0;
	let fileName = `${entityName}-${iterations}.json`;
	let file;

	if (cloudInfo.isCloud) {
		// For cloud storage, treat 'folder' as a prefix
		// Ensure it ends with / for proper prefix behavior
		const prefix = folder.endsWith('/') ? folder : folder + '/';
		file = `${prefix}${fileName}`;
	} else {
		// For local storage, use path.resolve as before
		file = path.resolve(`${folder}/${fileName}`);
	}

	/** @type {got.Options} */
	const options = {
		method: 'POST',
		url: job.url,
		headers: {
			Authorization: auth,
			'content-type': 'application/x-www-form-urlencoded'
		},
		searchParams: {
			...job.params || {}
		},
		responseType: 'json',
		retry: { limit: 50 }
	};
	// @ts-ignore
	if (job.project) options.searchParams.project_id = job.project;

	if (job.cohortId) {
		options.body = `filter_by_cohort={"id": ${job.cohortId}}&include_all_users=false`;
		options.body = encodeURIComponent(options.body);
	}

	if (job.whereClause) {
		// @ts-ignore
		options.body = `filter_by_cohort=${JSON.stringify(job.whereClause)}&include_all_users=false`;
		options.body = encodeURIComponent(options.body);
	}
	// if (job.dataGroupId) options.body = `data_group_id=${job.dataGroupId}`;
	// @ts-ignore

	if (job.dataGroupId) {
		const encodedParams = new URLSearchParams();
		encodedParams.set('data_group_id', job.dataGroupId);
		options.body = encodedParams.toString();
	}

	// @ts-ignore
	let request = await got(options).catch(e => {
		job.failed++;
		job.responses.push({
			status: e.statusCode,
			ip: e.ip,
			url: e.requestUrl,
			...e.headers,
			message: e.message
		});
		throw e;
	});
	let response = request.body;



	//grab values for recursion
	let { page, page_size, session_id } = response;
	let lastNumResults = response.results.length;

	// write first page of profiles
	let profiles = response.results;

	// Apply transforms if provided
	profiles = applyTransformToRecords(profiles, job);

	let firstFile, nextFile;
	if (skipWriteToDisk) {
		allResults.push(...profiles);
	}
	if (!skipWriteToDisk) {
		if (cloudInfo.isCloud) {
			firstFile = await writeCloudJSON(file, profiles, job);
		} else {
			firstFile = await writeLocalJSONL(file, profiles);
		}
		allResults.push(firstFile);
	}



	//update config
	job.recordsProcessed += profiles.length;
	job.success += profiles.length;
	job.requests++;
	job.responses.push({
		status: request.statusCode,
		ip: request.ip,
		url: request.requestUrl,
		...request.headers
	});

	if (job.verbose || job.showProgress) showProgress("profile", job.success, iterations + 1);


	// recursively consume all profiles
	// https://developer.mixpanel.com/reference/engage-query
	while (lastNumResults >= page_size) {
		page++;
		iterations++;

		fileName = `${entityName}-${iterations}.json`;
		if (cloudInfo.isCloud) {
			const prefix = folder.endsWith('/') ? folder : folder + '/';
			file = `${prefix}${fileName}`;
		} else {
			file = path.resolve(`${folder}/${fileName}`);
		}
		// @ts-ignore
		options.searchParams.page = page;
		// @ts-ignore
		options.searchParams.session_id = session_id;

		// @ts-ignore
		request = await got(options).catch(e => {
			job.failed++;
			job.responses.push({
				status: e.statusCode,
				ip: e.ip,
				url: e.requestUrl,
				...e.headers,
				message: e.message
			});
		});
		response = request.body;

		//update config
		job.requests++;
		job.responses.push({
			status: request.statusCode,
			ip: request.ip,
			url: request.requestUrl,
			...request.headers
		});
		job.success += profiles.length;
		job.recordsProcessed += profiles.length;
		if (job.verbose || job.showProgress) showProgress("profile", job.success, iterations + 1);

		profiles = response.results;

		// Apply transforms if provided
		profiles = applyTransformToRecords(profiles, job);

		if (skipWriteToDisk) {
			allResults.push(...profiles);
		}
		if (!skipWriteToDisk) {
			if (cloudInfo.isCloud) {
				nextFile = await writeCloudJSON(file, profiles, job);
			} else {
				nextFile = await writeLocalJSONL(file, profiles);
			}
			allResults.push(nextFile);
		}

		// update recursion
		lastNumResults = response.results.length;

	}

	if (job.verbose) console.log('\n\ndownload finished\n\n');

	// @ts-ignore
	if (skipWriteToDisk) {
		job.dryRunResults.push(...allResults);
	}
	if (!skipWriteToDisk) {
		// @ts-ignore
		job.file = allResults;
		if (cloudInfo.isCloud) {
			job.folder = folder; // Keep the cloud prefix as folder
		} else {
			job.folder = folder;
		}
	}


	return allResults;

}


async function deleteProfiles(job) {
	if (!job?.creds?.token) throw new Error("missing token");
	const { token } = job.creds;
	let recordType = "user";
	let deleteIdentityKey = "$distinct_id";
	const exportOptions = { skipWriteToDisk: true, recordType: "profile-export", verbose: false };
	if (job.dataGroupId) {
		recordType = "group";
		exportOptions.dataGroupId = job.dataGroupId;
		if (job.groupKey) deleteIdentityKey = job.groupKey;
		else throw new Error("missing groupKey");
	}
	const exportJob = new job.constructor({ ...job.creds }, exportOptions);
	const exportedProfiles = await exportProfiles("", exportJob);
	const deleteObjects = exportedProfiles.map(profile => {
		// ? https://developer.mixpanel.com/reference/delete-profile
		const deleteObj = {
			$token: job.token,
			$delete: "null"

		};
		if (recordType === "user") {
			deleteObj.$ignore_alias = false;
			deleteObj.$distinct_id = profile.$distinct_id;
		}
		if (recordType === "group") {
			deleteObj.$group_key = deleteIdentityKey;
			deleteObj.$group_id = profile.$distinct_id;
		}
		return deleteObj;
	});
	getMain();
	const deleteOpts = { recordType };
	if (job.groupKey) deleteOpts.groupKey = job.groupKey;
	const deleteJob = await mainFunc({ token }, deleteObjects, deleteOpts);
	job.dryRunResults = deleteJob;
	return deleteJob;
}

/**
 * @param  {number} amount
 */
function downloadProgress(amount) {
	if (amount < 1000000) {
		//noop
	}
	else {
		// @ts-ignore
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`\tdownloaded: ${u.bytesHuman(amount, 2, true)}    \t`);
	}
}


async function countFileLines(filePath) {
	return new Promise((resolve, reject) => {
		let lineCount = 0;
		fs.createReadStream(filePath)
			.on("data", (buffer) => {
				let idx = -1;
				lineCount--; // Because the loop will run once for idx=-1
				do {
					// @ts-ignore
					idx = buffer.indexOf(10, idx + 1);
					lineCount++;
				} while (idx !== -1);
			}).on("end", () => {
				resolve(lineCount);
			}).on("error", reject);
	});
}

/**
 * Detect if a path is a cloud storage path
 * @param {string} destination 
 * @returns {{isCloud: boolean, provider: 'gcs'|'s3'|null}}
 */
function detectCloudDestination(destination) {
	if (destination.startsWith('gs://')) {
		return { isCloud: true, provider: 'gcs' };
	}
	if (destination.startsWith('s3://')) {
		return { isCloud: true, provider: 's3' };
	}
	return { isCloud: false, provider: null };
}

/**
 * Create a writable stream to GCS
 * @param {string} gcsPath - gs://bucket/file path
 * @param {jobConfig} job - Job configuration
 * @returns {stream.Writable}
 */
function createGCSWriteStream(gcsPath, job) {
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];

	const storageConfig = {
		projectId: job.gcpProjectId
	};

	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}

	const storage = new Storage(storageConfig);
	const file = storage.bucket(bucketName).file(filePath);

	return file.createWriteStream({
		metadata: {
			contentType: 'application/x-ndjson'
		},
		resumable: false
	});
}

/**
 * Create a writable stream to S3
 * @param {string} s3Path - s3://bucket/file path  
 * @param {jobConfig} job - Job configuration
 * @returns {stream.Writable}
 */
function createS3WriteStream(s3Path, job) {
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];

	const s3ClientConfig = {
		region: job.s3Region
	};

	if (job.s3Key && job.s3Secret) {
		s3ClientConfig.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}

	if (!job.s3Region) {
		throw new Error('S3 region is required. Please specify s3Region in job config.');
	}

	const s3Client = new S3Client(s3ClientConfig);

	// Create a custom writable stream that buffers data and uploads on end
	const chunks = [];

	return new stream.Writable({
		write(chunk, encoding, callback) {
			chunks.push(chunk);
			callback();
		},

		async final(callback) {
			try {
				const body = Buffer.concat(chunks);
				await s3Client.send(new PutObjectCommand({
					Bucket: bucketName,
					Key: key,
					Body: body,
					ContentType: 'application/x-ndjson'
				}));
				callback();
			} catch (error) {
				callback(error);
			}
		}
	});
}

/**
 * Apply transform function to an array of records with error handling
 * @param {Array} records - array of records to transform
 * @param {jobConfig} job - job configuration
 * @returns {Array} - transformed records
 */
function applyTransformToRecords(records, job) {
	if (!job.transformFunc || typeof job.transformFunc !== 'function') {
		return records;
	}

	const transformedRecords = [];

	for (const record of records) {
		try {
			const transformed = job.transformFunc(record);
			// Handle case where transform returns array (explosion)
			if (Array.isArray(transformed)) {
				transformedRecords.push(...transformed);
			} else if (transformed) {
				transformedRecords.push(transformed);
			}
			// If transform returns null/undefined, skip the record
		} catch (transformError) {
			// Log transform error but continue processing
			if (job.verbose) {
				console.warn(`Transform error on profile record: ${transformError.message}`);
			}
			// Use original record if transform fails
			transformedRecords.push(record);
		}
	}

	return transformedRecords;
}

/**
 * Write data to local file as JSONL (newline-delimited JSON)
 * @param {string} filePath - local file path
 * @param {Array} data - data to write as JSONL
 * @returns {Promise<string>} - the file path that was written to
 */
async function writeLocalJSONL(filePath, data) {
	// Convert to JSONL format (newline-delimited JSON)
	const jsonlData = data.map(item => JSON.stringify(item)).join('\n') + '\n';
	
	// Ensure directory exists
	const dir = path.dirname(filePath);
	if (!fs.existsSync(dir)) {
		fs.mkdirSync(dir, { recursive: true });
	}
	
	// Write file
	await fs.promises.writeFile(filePath, jsonlData, 'utf8');
	
	return filePath;
}

/**
 * Write data to cloud storage as JSONL (newline-delimited JSON)
 * @param {string} cloudPath - cloud storage path
 * @param {Array} data - data to write as JSONL
 * @param {jobConfig} job - job configuration
 * @returns {Promise<string>} - the cloud path that was written to
 */
async function writeCloudJSON(cloudPath, data, job) {
	const cloudInfo = detectCloudDestination(cloudPath);

	if (!cloudInfo.isCloud) {
		throw new Error(`Expected cloud path, got local path: ${cloudPath}`);
	}

	// Convert to JSONL format (newline-delimited JSON) instead of JSON array
	const jsonlData = data.map(item => JSON.stringify(item)).join('\n') + '\n';

	if (cloudInfo.provider === 'gcs') {
		const matches = cloudPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
		if (!matches) {
			throw new Error(`Invalid GCS path: ${cloudPath}`);
		}

		const bucketName = matches[1];
		const filePath = matches[2];

		const storageConfig = {
			projectId: job.gcpProjectId
		};

		if (job.gcsCredentials) {
			storageConfig.keyFilename = job.gcsCredentials;
		}

		const storage = new Storage(storageConfig);
		const file = storage.bucket(bucketName).file(filePath);

		await file.save(jsonlData, {
			metadata: {
				contentType: 'application/json'
			}
		});

		return cloudPath;

	} else if (cloudInfo.provider === 's3') {
		const matches = cloudPath.match(/^s3:\/\/([^\/]+)\/(.+)$/);
		if (!matches) {
			throw new Error(`Invalid S3 path: ${cloudPath}`);
		}

		const bucketName = matches[1];
		const key = matches[2];

		const s3ClientConfig = {
			region: job.s3Region
		};

		if (job.s3Key && job.s3Secret) {
			s3ClientConfig.credentials = {
				accessKeyId: job.s3Key,
				secretAccessKey: job.s3Secret
			};
		}

		if (!job.s3Region) {
			throw new Error('S3 region is required. Please specify s3Region in job config.');
		}

		const s3Client = new S3Client(s3ClientConfig);

		await s3Client.send(new PutObjectCommand({
			Bucket: bucketName,
			Key: key,
			Body: jsonlData,
			ContentType: 'application/json'
		}));

		return cloudPath;
	}

	throw new Error(`Unsupported cloud provider: ${cloudInfo.provider}`);
}




/**
 * Lazily streams Mixpanel events as JS objects.
 * @param {jobConfig} job 
 * @returns {Readable} object-mode stream
 */
function streamEvents(job) {
	/** @type {got.Options} */
	const options = {
		url: job.url,
		method: 'GET',
		searchParams: {
			from_date: job.start,
			to_date: job.end,
			limit: job.limit,
			where: job.whereClause,
			project_id: job.project
		},
		retry: { limit: 50 },
		headers: { Authorization: job.auth },
		agent: { https: new https.Agent({ keepAlive: true }) }
	};

	const request = got.stream(options);

	// ------- NDJSON → objects -------------------------------------------------
	const ndjsonParser = new Transform({
		readableObjectMode: true,
		transform(chunk, _enc, cb) {
			this._buf = (this._buf || '') + chunk.toString();
			const lines = this._buf.split('\n');
			this._buf = lines.pop() || '';
			for (const line of lines) {
				if (!line.trim()) continue;
				try {
					const parsed = JSON.parse(line);
					const event = { ...parsed, ...parsed.properties };
					delete event.properties;
					this.push(event);
				}
				catch (e) {
					/* swallow malformed lines */
				}
			}
			cb();
		},
		flush(cb) {                               // last partial line
			if (this._buf) {
				try { this.push(JSON.parse(this._buf)); }
				catch { /* ignore */ }
			}
			cb();
		}
	});

	// expose pipeline errors on the resulting stream
	request.on('error', err => ndjsonParser.destroy(err));

	return request.pipe(ndjsonParser);
}

/**
 * Streams Mixpanel user or group profiles page-by-page.
 * Each object is emitted individually before the next request is made.
 * Back-pressure automatically defers the next HTTP call.
 * @param {jobConfig} job
 * @returns {Readable} object-mode stream
 */
function streamProfiles(job) {
	const url = job.url || 'https://mixpanel.com/api/2.0/engage/query';
	const auth = job.auth;
	return new Readable({
		objectMode: true,
		async read() {
			try {
				// on first call initialize pagination state on the stream instance
				if (!this._page) {
					this._page = 0;
					this._session_id = null;
					this._buffer = []; // holds objects not yet pushed
				}

				// If we still have buffered rows, push and return immediately
				if (this._buffer.length) {
					return this.push(this._buffer.shift());
				}

				const searchParams = {
					page: this._page,
					session_id: this._session_id
				};
				if (job.project) searchParams.project_id = job.project;
				const res = await got({
					method: 'POST',
					url,
					headers: {
						Authorization: auth,
						'content-type': 'application/x-www-form-urlencoded'
					},
					searchParams,
					body: new URLSearchParams(
						job.cohortId ? { filter_by_cohort: `{"id":${job.cohortId}}`, include_all_users: 'true' } :
							job.dataGroupId ? { data_group_id: job.dataGroupId } :
								{}
					).toString(),
					agent: { https: new https.Agent({ keepAlive: true }) },
					retry: { limit: 50 },
					responseType: 'json'
				}).json();

				// capture pagination tokens for the *next* call
				this._page = (res.page || 0) + 1;
				this._session_id = res.session_id;

				// No more results → end stream
				if (!res.results?.length) {
					return this.push(null);             // EOS
				}

				// Buffer results and push the first one now
				this._buffer = res.results
					.map(profile => {
						profile = { ...profile, ...profile.$properties };
						delete profile.$properties;
						return profile;
					});
				this.push(this._buffer.shift());

			} catch (err) {
				this.destroy(err);
			}
		}
	});
}


module.exports = { exportEvents, exportProfiles, deleteProfiles, streamEvents, streamProfiles };