const readline = require('readline');
const stream = require('stream');
const got = require('got');
const https = require('https');
const path = require('path');
const fs = require('fs');
const { promisify } = require('util');
const u = require('ak-tools')


async function exportEvents(filename, config) {
	const pipeline = promisify(stream.pipeline);

	/** @type {got.Options} */
	const options = {
		url: config.url,
		searchParams: {
			from_date: config.start,
			to_date: config.end
		},
		method: config.reqMethod,
		retry: { limit: 50 },
		headers: {
			"Authorization": `${config.auth}`
		},
		agent: {
			https: new https.Agent({ keepAlive: true })
		},
		hooks: {
			// @ts-ignore
			beforeRetry: [(err, count) => {
				// @ts-ignore
				l(`retrying request...#${count}`);
				config.retries++;
			}]
		},

	};

	// @ts-ignore
	if (config.project) options.searchParams.project_id = config.project;

	// @ts-ignore
	const request = got.stream(options);

	request.on('response', (res) => {
		config.requests++;
		config.responses.push({
			status: res.statusCode,
			ip: res.ip,
			url: res.requestUrl,
			...res.headers
		});
	});

	request.on('error', (e) => {
		config.failed++;
		config.responses.push({
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

	const exportedData = await pipeline(
		request,
		fs.createWriteStream(filename)
	);

	console.log('\n\ndownload finished\n\n');

	const lines = await countFileLines(filename);
	config.recordsProcessed += lines;
	config.success += lines;
	config.file = filename;

	return exportedData;
}

async function exportProfiles(folder, config) {
	const auth = config.auth;
	const allFiles = [];

	let iterations = 0;
	let fileName = `people-${iterations}.json`;
	let file = path.resolve(`${folder}/${fileName}`);

	/** @type {got.Options} */
	const options = {
		method: 'POST',
		url: config.url,
		headers: {
			Authorization: auth
		},
		searchParams: {},
		responseType: 'json'
	};
	// @ts-ignore
	if (config.project) options.searchParams.project_id = config.project;

	// @ts-ignore
	let request = await got(options).catch(e => {
		config.failed++;
		config.responses.push({
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
	config.recordsProcessed += profiles.length;
	config.success += profiles.length;
	config.requests++;
	config.responses.push({
		status: request.statusCode,
		ip: request.ip,
		url: request.requestUrl,
		...request.headers
	});

	showProgress("profile", config.success, iterations + 1);


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
			config.failed++;
			config.responses.push({
				status: e.statusCode,
				ip: e.ip,
				url: e.requestUrl,
				...e.headers,
				message: e.message
			});
		});
		response = request.body;

		//update config
		config.requests++;
		config.responses.push({
			status: request.statusCode,
			ip: request.ip,
			url: request.requestUrl,
			...request.headers
		});
		config.success += profiles.length;
		config.recordsProcessed += profiles.length;
		showProgress("profile", config.success, iterations + 1);

		profiles = response.results;

		nextFile = await u.touch(file, profiles, true);
		allFiles.push(nextFile);

		// update recursion
		lastNumResults = response.results.length;

	}

	console.log('\n\ndownload finished\n\n');

	config.file = allFiles;
	config.folder = folder;

	return folder;

}

function downloadProgress(amount) {
	if (amount < 1000000) {
		//noop
	}
	else {
		readline.cursorTo(process.stdout, 0);
		process.stdout.write(`\tdownloaded: ${u.bytesHuman(amount, 2, true)}    \t`);
	}
}

function showProgress(record, processed, requests) {
	const { rss, heapTotal, heapUsed } = process.memoryUsage();
	const percentHeap = (heapUsed / heapTotal) * 100;
	const percentRSS = (heapUsed / rss) * 100;
	const line = `${record}s: ${u.comma(processed)} | batches: ${u.comma(requests)} | memory: ${u.bytesHuman(heapUsed)} (heap: ${u.round(percentHeap)}% total:${u.round(percentRSS)}%)\t\t`;
	readline.cursorTo(process.stdout, 0);
	process.stdout.write(line);
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