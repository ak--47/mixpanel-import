const readline = require('readline');
const stream = require('stream');
const got = require('got');
const https = require('https');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');
const { promisify } = require('util');
const u = require('ak-tools');
const showProgress = require('./cli').showProgress;
const { Transform, Readable } = require('stream');
const { COMPRESSION_CONFIG } = require('./constants');

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

	// Add project_id when using service account auth (acct + pass + project)
	// Secret-based auth doesn't need project_id in the URL
	// @ts-ignore
	if (job.project && job.acct && job.pass) options.searchParams.project_id = job.project;
	

	// @ts-ignore
	const request = got.stream(options);

	request.on('response', (res) => {
		job.requests++;
		// Use job.store() to respect abridged mode
		job.store({
			status: res.statusCode,
			ip: res.ip,
			url: res.requestUrl,
			...res.headers
		}, true);
	});

	request.on('error', (e) => {
		job.failed++;
		// Use job.store() to respect abridged mode
		job.store({
			status: e.statusCode,
			ip: e.ip,
			url: e.requestUrl,
			...e.headers,
			message: e.message
		}, false);
		// Don't throw here - the pipeline() will handle stream errors
		// Throwing inside an event handler causes unhandled exceptions and hangs
	});

	request.on('downloadProgress', (progress) => {
		downloadProgress(progress.transferred, job);
	});

	// Auto-generate filename if cloud path ends with / or is a bucket/directory without a filename
	const cloudInfo = detectCloudDestination(filename);
	const shouldCompress = job.compress !== false; // Default true for cloud exports

	if (cloudInfo.isCloud) {
		// Check if this appears to be a directory (no file extension after the last /)
		const lastSlashIndex = filename.lastIndexOf('/');
		const afterLastSlash = filename.substring(lastSlashIndex + 1);

		// If there's no extension or it ends with /, generate a filename
		if (!afterLastSlash.includes('.') || filename.endsWith('/')) {
			// Generate filename based on date range
			const startDate = job.start || 'unknown';
			const endDate = job.end || 'unknown';

			// Convention: .json.gz if compressed, .ndjson if not
			const extension = shouldCompress ? '.json.gz' : '.ndjson';
			const generatedFilename = `events-${startDate}--${endDate}${extension}`;

			// Add trailing slash if not present and path doesn't end with /
			if (!filename.endsWith('/')) {
				filename += '/';
			}

			filename = filename + generatedFilename;
			if (job.verbose) {
				console.log(`Auto-generated filename for cloud export: ${filename}`);
			}
		} else {
			// User provided a filename - normalize the extension based on compression setting
			// Remove any existing .gz or compression-related suffixes and apply correct extension
			let basePath = filename;

			// Strip existing extensions to get base name
			if (basePath.endsWith('.gz')) {
				basePath = basePath.slice(0, -3);
			}
			if (basePath.endsWith('.ndjson')) {
				basePath = basePath.slice(0, -7);
			} else if (basePath.endsWith('.json')) {
				basePath = basePath.slice(0, -5);
			} else if (basePath.endsWith('.jsonl')) {
				basePath = basePath.slice(0, -6);
			}

			// Apply the correct extension based on compression setting
			// Convention: .json.gz if compressed, .ndjson if not
			filename = basePath + (shouldCompress ? '.json.gz' : '.ndjson');

			if (job.verbose) {
				console.log(`Normalized cloud export filename: ${filename}`);
			}
		}
	}

	// Define streams upfront
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

	// Processing stream for memory and file/cloud output
	let buffer = "";
	const processingStream = new stream.Writable({
		write(chunk, encoding, callback) {
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
										recordCount++;
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
						recordCount++;
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
										recordCount++;
									}
								});
								// Cloud stream finalization is handled after the pipeline completes
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
						recordCount++;
					}
				}
				catch (parseError) {
					if (job.verbose) {
						console.warn(`Parse error on final line: ${parseError.message}`);
					}
				}
			}

			// Note: Cloud stream finalization is handled after the pipeline completes
			// to ensure we properly wait for the upload to finish
			callback();
		}
	});

	const allResults = [];
	let recordCount = 0; // Track record count for cloud storage

	// Choose the appropriate stream based on whether transforms are needed
	let outputStream;
	if (skipWriteToDisk || (job.transformFunc && typeof job.transformFunc === 'function') || cloudInfo.isCloud) {
		// Use processing stream when we need transforms, memory mode, or cloud storage (for record counting)
		outputStream = processingStream;
	} else {
		// Use direct file stream only for local files with no transforms
		outputStream = fileStream;
	}

	// Use the chosen stream in the pipeline
	try {
		await pipeline(request, outputStream);
	}
	catch (e) {
		if (job.verbose) console.warn(`Pipeline error: ${e.message}`);
	}
	if (job.verbose) console.log('\n\ndownload finished\n\n');
	if (skipWriteToDisk) {
		job.recordsProcessed += allResults.length;
		job.success += allResults.length;
		job.dryRunResults.push(...allResults);
		return allResults;
	}

	if (cloudInfo.isCloud) {
		// Wait for the cloud stream to finish uploading
		// The fileStream may be a gzip stream piped to a GCS/S3 stream
		await new Promise((resolve, reject) => {
			// Get the actual cloud path (may have .gz appended)
			const actualPath = fileStream._gcsPath || fileStream._s3Path || filename;

			// For GCS with compression, we have a gzip stream piped to a GCS stream
			// We need to wait for the underlying GCS stream to finish, not just the gzip stream
			const underlyingStream = fileStream._underlyingStream;
			const streamToWaitOn = underlyingStream || fileStream;

			// The 'finish' event fires when all data has been flushed to the destination
			streamToWaitOn.on('finish', () => {
				if (job.verbose) console.log(`Cloud stream finished: ${actualPath}`);
				resolve();
			});
			streamToWaitOn.on('error', (err) => {
				if (job.verbose) console.error(`Cloud stream error: ${err.message}`);
				reject(err);
			});

			// Also listen on the outer stream for errors
			if (underlyingStream) {
				fileStream.on('error', (err) => {
					if (job.verbose) console.error(`Compression stream error: ${err.message}`);
					reject(err);
				});
			}

			// End the stream if not already ended (triggers flush)
			if (!fileStream.writableEnded) {
				fileStream.end();
			}
		});

		// Get the actual cloud path (may have .gz appended by createGCSWriteStream/createS3WriteStream)
		const actualCloudPath = fileStream._gcsPath || fileStream._s3Path || filename;

		// For cloud storage, use the record count we tracked during streaming
		job.recordsProcessed += recordCount;
		job.success += recordCount;
		job.file = actualCloudPath;
		if (job.verbose) console.log(`Exported ${recordCount} records to cloud storage: ${actualCloudPath}`);
		return actualCloudPath;
	} else {
		// For local files, ensure the file stream is properly closed
		// This is necessary when using processingStream (transforms enabled)
		if (outputStream !== fileStream && fileStream && !fileStream.writableEnded) {
			await new Promise((resolve, reject) => {
				fileStream.on('finish', resolve);
				fileStream.on('error', reject);
				fileStream.end();
			});
		}

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
		// Profile exports create multiple files, so we need a folder path, not a filename
		// Strip any filename from the path if user provided one
		let prefix = folder;
		const fileExtensions = ['.json.gz', '.ndjson.gz', '.jsonl.gz', '.json', '.ndjson', '.jsonl', '.gz'];
		const lowerFolder = folder.toLowerCase();
		for (const ext of fileExtensions) {
			if (lowerFolder.endsWith(ext)) {
				// Strip the filename - find the last / before the extension
				const lastSlash = folder.lastIndexOf('/');
				if (lastSlash > 0) {
					prefix = folder.substring(0, lastSlash + 1);
					if (job.verbose) console.log(`Profile export: converted file path to folder: ${folder} -> ${prefix}`);
				}
				break;
			}
		}
		// Ensure it ends with / for proper prefix behavior
		prefix = prefix.endsWith('/') ? prefix : prefix + '/';
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
	// Add project_id when using service account auth (acct + pass + project)
	// Secret-based auth doesn't need project_id in the URL
	// @ts-ignore
	if (job.project && job.acct && job.pass) options.searchParams.project_id = job.project;

	// Build form data for POST body
	const encodedParams = new URLSearchParams();

	if (job.cohortId) {
		encodedParams.set('filter_by_cohort', JSON.stringify({ id: job.cohortId }));
		encodedParams.set('include_all_users', 'false');
	}

	if (job.whereClause) {
		encodedParams.set('where', job.whereClause);
	}

	if (job.dataGroupId) {
		encodedParams.set('data_group_id', job.dataGroupId);
	}

	// Only set body if we have form parameters
	if (encodedParams.toString()) {
		options.body = encodedParams.toString();
	}

	// Retry logic for initial request - critical for getting session_id
	let request;
	let retryCount = 0;
	const maxRetries = 5;
	let lastError = null;

	while (retryCount <= maxRetries) {
		try {
			// @ts-ignore
			request = await got(options);
			break; // Success, exit retry loop
		} catch (e) {
			lastError = e;
			const isRateLimit = e.statusCode === 429;
			const isServerError = e.statusCode >= 500;
			const shouldRetry = isRateLimit || isServerError || retryCount < maxRetries;

			if (job.verbose) {
				console.warn(`Profile export initial request failed (attempt ${retryCount + 1}/${maxRetries + 1}): ${e.message}`);
			}

			if (!shouldRetry) {
				job.failed++;
				job.store({
					status: e.statusCode,
					ip: e.ip,
					url: e.requestUrl,
					...e.headers,
					message: e.message
				}, false);
				throw e;
			}

			// Exponential backoff: 1s, 2s, 4s, 8s, 16s
			const backoffMs = Math.min(1000 * Math.pow(2, retryCount), 30000);
			if (job.verbose) {
				console.log(`Retrying in ${backoffMs}ms... (${isRateLimit ? 'rate limit' : isServerError ? 'server error' : 'network error'})`);
			}
			await new Promise(resolve => setTimeout(resolve, backoffMs));
			retryCount++;
		}
	}

	// If we exhausted retries, request will still be undefined
	if (!request) {
		job.failed++;
		job.store({
			status: lastError?.statusCode,
			ip: lastError?.ip,
			url: lastError?.requestUrl,
			...lastError?.headers,
			message: lastError?.message || 'Request failed after retries'
		}, false);
		throw lastError || new Error('Profile export initial request failed after retries');
	}

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
	// Use job.store() to respect abridged mode
	job.store({
		status: request.statusCode,
		ip: request.ip,
		url: request.requestUrl,
		...request.headers
	}, true);

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

		// Retry logic for pagination requests - critical for maintaining session
		let retryCount = 0;
		const maxRetries = 5;
		let lastError = null;

		while (retryCount <= maxRetries) {
			try {
				// @ts-ignore
				request = await got(options);
				break; // Success, exit retry loop
			} catch (e) {
				lastError = e;
				const isRateLimit = e.statusCode === 429;
				const isServerError = e.statusCode >= 500;
				const shouldRetry = isRateLimit || isServerError || retryCount < maxRetries;

				if (job.verbose) {
					console.warn(`Profile pagination request failed (page ${page}, attempt ${retryCount + 1}/${maxRetries + 1}): ${e.message}`);
				}

				if (!shouldRetry) {
					job.failed++;
					job.store({
						status: e.statusCode,
						ip: e.ip,
						url: e.requestUrl,
						...e.headers,
						message: e.message
					}, false);
					throw e;
				}

				// Exponential backoff: 1s, 2s, 4s, 8s, 16s
				const backoffMs = Math.min(1000 * Math.pow(2, retryCount), 30000);
				if (job.verbose) {
					console.log(`Retrying in ${backoffMs}ms... (${isRateLimit ? 'rate limit' : isServerError ? 'server error' : 'network error'})`);
				}
				await new Promise(resolve => setTimeout(resolve, backoffMs));
				retryCount++;
			}
		}

		// If we exhausted retries, request will still be undefined
		if (!request) {
			job.failed++;
			job.store({
				status: lastError?.statusCode,
				ip: lastError?.ip,
				url: lastError?.requestUrl,
				...lastError?.headers,
				message: lastError?.message || 'Request failed after retries'
			}, false);
			throw lastError || new Error('Profile pagination request failed after retries');
		}

		response = request.body;

		//update config
		job.requests++;
		// Use job.store() to respect abridged mode
		job.store({
			status: request.statusCode,
			ip: request.ip,
			url: request.requestUrl,
			...request.headers
		}, true);
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
 * @param  {jobConfig} [job]
 */
function downloadProgress(amount, job = null) {
	if (amount < 1000000) {
		//noop
	}
	else {
		// @ts-ignore
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`\tdownloaded: ${u.bytesHuman(amount, 2, true)}    \t`);

		// Send WebSocket update if progressCallback exists
		if (job && job.progressCallback && typeof job.progressCallback === 'function') {
			// Create a download progress message
			const downloadMessage = `downloaded: ${u.bytesHuman(amount, 2, true)}`;
			// Don't send processed count during download (we don't have line counts yet)
			// Pass null for processed to avoid showing misleading numbers
			// @ts-ignore
			job.progressCallback('download', null, 0, 0, amount, downloadMessage);
		}
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

	// Handle compression - default true for cloud exports
	// Note: Extension normalization is handled in exportEvents, so we use the path as-is
	const shouldCompress = job.compress !== false;

	const storageConfig = {
		projectId: job.gcpProjectId
	};

	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}

	const storage = new Storage(storageConfig);
	const file = storage.bucket(bucketName).file(filePath);

	// Create GCS write stream with appropriate metadata
	const gcsWriteOptions = {
		metadata: {
			contentType: 'application/x-ndjson'
		},
		resumable: false
	};

	// Add content encoding for gzip
	if (shouldCompress) {
		gcsWriteOptions.metadata.contentEncoding = 'gzip';
	}

	const gcsStream = file.createWriteStream(gcsWriteOptions);

	if (shouldCompress) {
		// Create gzip transform stream
		const gzipStream = zlib.createGzip({
			level: job.compressionLevel || COMPRESSION_CONFIG.GZIP_LEVEL,
			memLevel: COMPRESSION_CONFIG.GZIP_MEM_LEVEL
		});

		// Pipe gzip through to GCS
		gzipStream.pipe(gcsStream);

		// Expose the final path for logging
		gzipStream._gcsPath = `gs://${bucketName}/${filePath}`;
		// Store reference to underlying GCS stream for proper finish detection
		gzipStream._underlyingStream = gcsStream;
		return gzipStream;
	}

	gcsStream._gcsPath = `gs://${bucketName}/${filePath}`;
	return gcsStream;
}

/**
 * Create a writable stream to S3 with optional gzip compression
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

	// Handle compression - default true for cloud exports
	// Note: Extension normalization is handled in exportEvents, so we use the path as-is
	const shouldCompress = job.compress !== false;

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

	const writeStream = new stream.Writable({
		write(chunk, encoding, callback) {
			chunks.push(chunk);
			callback();
		},

		async final(callback) {
			try {
				let body = Buffer.concat(chunks);

				// Compress if needed
				if (shouldCompress) {
					body = zlib.gzipSync(body, {
						level: job.compressionLevel || COMPRESSION_CONFIG.GZIP_LEVEL,
						memLevel: COMPRESSION_CONFIG.GZIP_MEM_LEVEL
					});
				}

				const putParams = {
					Bucket: bucketName,
					Key: key,
					Body: body,
					ContentType: 'application/x-ndjson'
				};

				// Add content encoding for gzip
				if (shouldCompress) {
					putParams.ContentEncoding = 'gzip';
				}

				await s3Client.send(new PutObjectCommand(putParams));
				callback();
			} catch (error) {
				callback(error);
			}
		}
	});

	writeStream._s3Path = `s3://${bucketName}/${key}`;
	return writeStream;
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
 * Write data to cloud storage as JSONL (newline-delimited JSON) with optional gzip compression
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

	// Handle compression - default true for cloud exports
	const shouldCompress = job.compress !== false;

	// Auto-append .gz extension if compressing and not already present
	let finalPath = cloudPath;
	if (shouldCompress && !cloudPath.endsWith('.gz')) {
		finalPath = cloudPath + '.gz';
	}

	// Convert to JSONL format (newline-delimited JSON) instead of JSON array
	const jsonlData = data.map(item => JSON.stringify(item)).join('\n') + '\n';

	// Compress if needed
	let bodyData = Buffer.from(jsonlData);
	if (shouldCompress) {
		bodyData = zlib.gzipSync(bodyData, {
			level: job.compressionLevel || COMPRESSION_CONFIG.GZIP_LEVEL,
			memLevel: COMPRESSION_CONFIG.GZIP_MEM_LEVEL
		});
	}

	if (cloudInfo.provider === 'gcs') {
		const matches = finalPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
		if (!matches) {
			throw new Error(`Invalid GCS path: ${finalPath}`);
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

		const saveOptions = {
			metadata: {
				contentType: 'application/x-ndjson'
			}
		};

		if (shouldCompress) {
			saveOptions.metadata.contentEncoding = 'gzip';
		}

		await file.save(bodyData, saveOptions);

		return finalPath;

	} else if (cloudInfo.provider === 's3') {
		const matches = finalPath.match(/^s3:\/\/([^\/]+)\/(.+)$/);
		if (!matches) {
			throw new Error(`Invalid S3 path: ${finalPath}`);
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

		const putParams = {
			Bucket: bucketName,
			Key: key,
			Body: bodyData,
			ContentType: 'application/x-ndjson'
		};

		if (shouldCompress) {
			putParams.ContentEncoding = 'gzip';
		}

		await s3Client.send(new PutObjectCommand(putParams));

		return finalPath;
	}

	throw new Error(`Unsupported cloud provider: ${cloudInfo.provider}`);
}




/**
 * Lazily streams Mixpanel events as JS objects.
 * @param {jobConfig} job 
 * @returns {Readable} object-mode stream
 */
function streamEvents(job) {
	const searchParams = {
		from_date: job.start,
		to_date: job.end,
		limit: job.limit,
		where: job.whereClause
	};

	// Add project_id when using service account auth (acct + pass + project)
	if (job.project && job.acct && job.pass) {
		searchParams.project_id = job.project;
	}

	/** @type {got.Options} */
	const options = {
		url: job.url,
		method: 'GET',
		searchParams,
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
				// Add project_id when using service account auth (acct + pass + project)
				if (job.project && job.acct && job.pass) searchParams.project_id = job.project;
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


module.exports = { exportEvents, exportProfiles, deleteProfiles, streamEvents, streamProfiles, detectCloudDestination };