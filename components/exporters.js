const readline = require('readline');
const stream = require('stream');
const got = require('got');
const https = require('https');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const u = require('ak-tools');
const showProgress = require('./cli').showProgress;

/** @typedef {import('./job')} jobConfig */

/**
 * @param  {string} filename
 * @param  {jobConfig} jobConfig
 */
async function exportEvents(filename, jobConfig) {
	const pipeline = promisify(stream.pipeline);

	/** @type {got.Options} */
	const options = {
		url: jobConfig.url,
		searchParams: {
			from_date: jobConfig.start,
			to_date: jobConfig.end
		},
		method: jobConfig.reqMethod,
		retry: { limit: 50 },
		headers: {
			"Authorization": `${jobConfig.auth}`
		},
		agent: {
			https: new https.Agent({ keepAlive: true })
		},
		hooks: {
			// @ts-ignore
			beforeRetry: [(err, count) => {
				// @ts-ignore
				l(`retrying request...#${count}`);
				jobConfig.retries++;
			}]
		},

	};

	// @ts-ignore
	if (jobConfig.project) options.searchParams.project_id = jobConfig.project;

	// @ts-ignore
	const request = got.stream(options);

	request.on('response', (res) => {
		jobConfig.requests++;
		jobConfig.responses.push({
			status: res.statusCode,
			ip: res.ip,
			url: res.requestUrl,
			...res.headers
		});
	});

	request.on('error', (e) => {
		jobConfig.failed++;
		jobConfig.responses.push({
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

	await pipeline(
		request,
		fs.createWriteStream(filename)
	);

	console.log('\n\ndownload finished\n\n');

	const lines = await countFileLines(filename);
	jobConfig.recordsProcessed += lines;
	jobConfig.success += lines;
	jobConfig.file = filename;

	return null;
}

/**
 * @param  {string} folder
 * @param  {jobConfig} jobConfig
 */
async function exportProfiles(folder, jobConfig) {
	const auth = jobConfig.auth;
	const allFiles = [];

	let iterations = 0;
	let fileName = `people-${iterations}.json`;
	let file = path.resolve(`${folder}/${fileName}`);

	/** @type {got.Options} */
	const options = {
		method: 'POST',
		url: jobConfig.url,
		headers: {
			Authorization: auth
		},
		searchParams: {},
		responseType: 'json',
		retry: { limit: 50 }
	};
	// @ts-ignore
	if (jobConfig.project) options.searchParams.project_id = jobConfig.project;

	// @ts-ignore
	let request = await got(options).catch(e => {
		jobConfig.failed++;
		jobConfig.responses.push({
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
	const firstFile = await u.touch(file, profiles, true);
	let nextFile;
	allFiles.push(firstFile);

	//update config
	jobConfig.recordsProcessed += profiles.length;
	jobConfig.success += profiles.length;
	jobConfig.requests++;
	jobConfig.responses.push({
		status: request.statusCode,
		ip: request.ip,
		url: request.requestUrl,
		...request.headers
	});

	showProgress("profile", jobConfig.success, iterations + 1);


	// recursively consume all profiles
	// https://developer.mixpanel.com/reference/engage-query
	while (lastNumResults >= page_size) {
		page++;
		iterations++;

		fileName = `people-${iterations}.json`;
		file = path.resolve(`${folder}/${fileName}`);
		// @ts-ignore
		options.searchParams.page = page;
		// @ts-ignore
		options.searchParams.session_id = session_id;

		// @ts-ignore
		request = await got(options).catch(e => {
			jobConfig.failed++;
			jobConfig.responses.push({
				status: e.statusCode,
				ip: e.ip,
				url: e.requestUrl,
				...e.headers,
				message: e.message
			});
		});
		response = request.body;

		//update config
		jobConfig.requests++;
		jobConfig.responses.push({
			status: request.statusCode,
			ip: request.ip,
			url: request.requestUrl,
			...request.headers
		});
		jobConfig.success += profiles.length;
		jobConfig.recordsProcessed += profiles.length;
		showProgress("profile", jobConfig.success, iterations + 1);

		profiles = response.results;

		nextFile = await u.touch(file, profiles, true);
		allFiles.push(nextFile);

		// update recursion
		lastNumResults = response.results.length;

	}

	console.log('\n\ndownload finished\n\n');

	// @ts-ignore
	jobConfig.file = allFiles;
	jobConfig.folder = folder;

	return null;

}

/**
 * @param  {number} amount
 */
function downloadProgress(amount) {
	if (amount < 1000000) {
		//noop
	}
	else {
		if (process) {
			readline.cursorTo(process.stdout, 0);
			process.stdout.write(`\tdownloaded: ${u.bytesHuman(amount, 2, true)}    \t`);
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

module.exports = { exportEvents, exportProfiles };