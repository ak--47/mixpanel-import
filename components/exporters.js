const readline = require('readline');
const stream = require('stream');
const got = require('got');
const https = require('https');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const u = require('ak-tools');
const showProgress = require('./cli').showProgress;

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
			to_date: job.end
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
	const fileStream = fs.createWriteStream(filename);
	let buffer = "";
	const memoryStream = new stream.Writable({
		write(chunk, encoding, callback) {

			// Convert the chunk to a string and append it to the buffer
			buffer += chunk.toString();

			// Split the buffer into lines
			const lines = buffer.split("\n");

			// Keep the last partial line in the buffer (if any)
			buffer = lines.pop() || "";

			// Push each complete line into the results array
			lines.forEach(line => {
				try {
					const row = JSON.parse(line.trim());
					allResults.push(row);
				}
				catch (e) {
					// console.log(e);
				}
			});

			callback();
		},

		final(callback) {
			// Push the remaining data in the buffer as the last line
			if (buffer) {
				try {
					allResults.push(JSON.parse(buffer.trim()));
				}
				catch (e) {
					// console.log
				}
			}
			callback();
		}
	});

	const allResults = [];

	// Choose the appropriate stream
	const outputStream = skipWriteToDisk ? memoryStream : fileStream;

	// Use the chosen stream in the pipeline
	await pipeline(request, outputStream);
	console.log('\n\ndownload finished\n\n');
	if (skipWriteToDisk) {
		job.recordsProcessed += allResults.length;
		job.success += allResults.length;
		job.dryRunResults.push(...allResults);
		return allResults;
	}

	const lines = await countFileLines(filename);
	job.recordsProcessed += lines;
	job.success += lines;
	job.file = filename;


	return filename;

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

	let iterations = 0;
	let fileName = `${entityName}-${iterations}.json`;
	let file = path.resolve(`${folder}/${fileName}`);

	/** @type {got.Options} */
	const options = {
		method: 'POST',
		url: job.url,
		headers: {
			Authorization: auth,
			'content-type': 'application/x-www-form-urlencoded'
		},
		searchParams: {},
		responseType: 'json',
		retry: { limit: 50 }
	};
	// @ts-ignore
	if (job.project) options.searchParams.project_id = job.project;

	if (job.cohortId) {
		options.body = `filter_by_cohort={"id": ${job.cohortId}}&include_all_users=true`;
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
	let firstFile, nextFile;
	if (skipWriteToDisk) {
		allResults.push(...profiles);
	}
	if (!skipWriteToDisk) {
		firstFile = await u.touch(file, profiles, true);
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
		file = path.resolve(`${folder}/${fileName}`);
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

		if (skipWriteToDisk) {
			allResults.push(...profiles);
		}
		if (!skipWriteToDisk) {
			nextFile = await u.touch(file, profiles, true);
			allResults.push(nextFile);
		}

		// update recursion
		lastNumResults = response.results.length;

	}

	console.log('\n\ndownload finished\n\n');

	// @ts-ignore
	if (skipWriteToDisk) {
		job.dryRunResults.push(...allResults);
	}
	if (!skipWriteToDisk) {
		// @ts-ignore
		job.file = allResults;
		job.folder = folder;
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

module.exports = { exportEvents, exportProfiles, deleteProfiles };