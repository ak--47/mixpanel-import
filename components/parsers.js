const readline = require('readline');
const stream = require('stream');
const { Transform } = require('stream');
const MultiStream = require('multistream');
const path = require('path');
const fs = require('fs');
const os = require("os");
const _ = require('highland');
const { pick } = require('underscore');
const StreamArray = require('stream-json/streamers/StreamArray');
const JsonlParser = require('./jsonl');
const u = require('ak-tools');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
dayjs.extend(utc);
const dateFormat = `YYYY-MM-DD`;
const Papa = require('papaparse');
const { prepareSCD } = require('./validators.js');


/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */


/**
 * @param  {any} data
 * @param  {JobConfig} job
 */
async function determineDataType(data, job) {

	//exports are saved locally
	if (job.recordType === 'export') {
		if (job.where) {
			return path.resolve(job.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return filename;
	}

	if (job.recordType === 'profile-export') {
		if (job.where) {
			return path.resolve(job.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		return path.resolve(folder);
	}

	// lookup tables are not streamed
	if (job.recordType === 'table') {
		job.wasStream = false;
		if (fs.existsSync(path.resolve(data))) return await u.load(data);
		return data;
	}

	// scd props need a whole crazy slew of things
	if (job.recordType === 'scd') {
		try {
		await prepareSCD(job);
		}
		catch (e) {			
			throw new Error(`SCD preparation failed: ${e.message}`);
		}

	}

	// data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		job.wasStream = true;
		if (data.readableObjectMode) return data;
		return _(existingStreamInterface(data));
	}

	// data is an object in memory
	if (Array.isArray(data) && data.every(item => typeof item === 'object' && item !== null)) {
		job.wasStream = true;
		return stream.Readable.from(data, { objectMode: true, highWaterMark: job.highWater });
	}

	try {
		const { lineByLineFileExt, objectModeFileExt, tableFileExt, supportedFileExt, streamFormat, forceStream, highWater } = job;
		let isArrayOfFileNames = false; // !ugh ... so disorganized 
		//data might be an array of filenames
		if (Array.isArray(data) && data.every(item => typeof item === 'string')) {
			if (data.every(filePath => fs.existsSync(path.resolve(filePath)))) {
				isArrayOfFileNames = true;
			}
		}

		// data refers to file/folder on disk
		if (typeof data === 'string' && !isArrayOfFileNames) {
			if (fs.existsSync(path.resolve(data))) {
				if (fs.lstatSync(path.resolve(data)).isFile()) {
					const fileInfo = fs.lstatSync(path.resolve(data));
					//it's a file
					let parsingCase = '';
					if (streamFormat === 'jsonl' || lineByLineFileExt.includes(path.extname(data))) parsingCase = 'jsonl';
					else if (streamFormat === 'json' || objectModeFileExt.includes(path.extname(data))) parsingCase = 'json';
					else if (streamFormat === 'csv' || tableFileExt.includes(path.extname(data))) parsingCase = 'csv';

					let loadIntoMemory = false;
					if (fileInfo.size < os.freemem() * .50) loadIntoMemory = true;
					if (forceStream) loadIntoMemory = false;

					if (parsingCase === 'jsonl') {
						if (loadIntoMemory) {
							job.wasStream = false;
							try {
								const file = /** @type {string} */ (await u.load(path.resolve(data)));
								const parsed = file.trim().split('\n').map(line => JSON.parse(line));
								return stream.Readable.from(parsed, { objectMode: true, highWaterMark: highWater });
							}
							catch (e) {
								// probably a memory crash, so we'll try to stream it
							}
						}
						job.wasStream = true;
						return itemStream(path.resolve(data), "jsonl", job);
					}

					if (parsingCase === 'json') {
						if (loadIntoMemory) {
							try {
								job.wasStream = false;
								const file = await u.load(path.resolve(data), true);
								// @ts-ignore
								return stream.Readable.from(file, { objectMode: true, highWaterMark: highWater });
							}
							catch (e) {
								// probably a memory crash, so we'll try to stream it
							}
						}

						//otherwise, stream it
						job.wasStream = true;
						return itemStream(path.resolve(data), "json", job);
					}

					//csv case
					if (parsingCase === 'csv') {
						if (loadIntoMemory) {
							try {
								job.wasStream = false;
								return await csvMemory(path.resolve(data), job);
							}
							catch (e) {
								// probably a memory crash, so we'll try to stream it
							}

						}
						job.wasStream = true;
						return csvStreamer(path.resolve(data), job);

					}
				}

			}
		}

		//folder or array of files case
		if (isArrayOfFileNames || (fs.existsSync(path.resolve(data)) && fs.lstatSync(path.resolve(data)).isDirectory())) {
			job.wasStream = true;
			let files;
			let exampleFile;
			let parsingCase;
			if (isArrayOfFileNames && Array.isArray(data)) {
				//array of files case
				files = data.map(filePath => path.resolve(filePath));
				exampleFile = path.extname(files[0]);
			}
			else {
				//directory case
				const enumDir = await u.ls(path.resolve(data));
				files = enumDir.filter(filePath => supportedFileExt.includes(path.extname(filePath)));
				exampleFile = path.extname(files[0]);
			}

			if (streamFormat === 'jsonl' || lineByLineFileExt.includes(exampleFile)) parsingCase = 'jsonl';
			else if (streamFormat === 'json' || objectModeFileExt.includes(exampleFile)) parsingCase = 'json';
			else if (streamFormat === 'csv' || tableFileExt.includes(exampleFile)) parsingCase = 'csv';

			switch (parsingCase) {
				case 'jsonl':
					return itemStream(files, "jsonl", job);
				case 'json':
					return itemStream(files, "json", job);
				case 'csv':
					return csvStreamArray(files, job);
				default:
					return itemStream(files, "jsonl", job);
			}
		}
	}


	catch (e) {

		//noop
	}

	// data is a string, and we have to guess what it is
	if (typeof data === 'string') {

		//stringified JSON
		try {
			return stream.Readable.from(JSON.parse(data), { objectMode: true, highWaterMark: job.highWater });
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			// @ts-ignore
			return stream.Readable.from(data.trim().split('\n').map(JSON.parse), { objectMode: true, highWaterMark: job.highWater });
		}

		catch (e) {
			//noop
		}

		//CSV or TSV
		try {
			return stream.Readable.from(Papa.parse(data, { header: true, skipEmptyLines: true }).data, { objectMode: true, highWaterMark: job.highWater });
		}
		catch (e) {
			//noop
		}
	}

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string... (i could not determine it's type)`);
	process.exit(1);

}



/**
 * @param  {import("stream").Readable} stream
 */
function existingStreamInterface(stream) {
	const rl = readline.createInterface({
		input: stream,
		crlfDelay: Infinity
	});

	const generator = (push, next) => {
		rl.on('line', line => {
			push(null, JSON.parse(line));
		});
		rl.on('close', () => {
			next();
			push(null, _.nil); //end of stream

		});
	};

	return generator;
}

// Create a transform stream factory to ensure newline at the end of each file.
function createEnsureNewlineTransform() {
	return new Transform({
		transform(chunk, encoding, callback) {
			this.push(chunk);
			callback();
		},
		flush(callback) {
			this.push('\n');
			callback();
		}
	});
}

/**
 * @param  {string | string[]} filePath
 * @param  {import('../index').SupportedFormats} type="jsonl"
 * @param {JobConfig} job
 */
function itemStream(filePath, type = "jsonl", job) {
	let stream;
	let parsedStream;
	const parser = type === "jsonl" ? JsonlParser.parser : StreamArray.withParser;
	const streamOpts = {
		highWaterMark: job.highWater,
		autoClose: true,
		emitClose: true

	};
	//parsing folders
	if (Array.isArray(filePath)) {

		if (type === "jsonl") {
			stream = new MultiStream(filePath.map((file) => fs.createReadStream(file, streamOpts).pipe(createEnsureNewlineTransform())), streamOpts);
			// @ts-ignore
			parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: job.parseErrorHandler, ...streamOpts })).map(token => token.value);
			return parsedStream;

		}
		if (type === "json") {
			stream = filePath.map((file) => fs.createReadStream(file, streamOpts));
			// @ts-ignore
			parsedStream = MultiStream.obj(stream.map(s => s.pipe(parser(streamOpts)).map(token => token.value)));
			return parsedStream;
		}
	}

	//parsing files
	else {
		stream = fs.createReadStream(filePath, streamOpts);
		// @ts-ignore
		parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: job.parseErrorHandler, ...streamOpts })).map(token => token.value);
	}

	return parsedStream;

}

/**
 * wraps csvStream with MultiStream to turn a folder of csv files into a single stream
 * @param  {string[]} filePaths
 * @param  {JobConfig} jobConfig
 */
function csvStreamArray(filePaths, jobConfig) {
	const streams = filePaths.map((filePath) => {
		return csvStreamer(filePath, jobConfig);
	});
	return MultiStream.obj(streams);
}

/**
 * streamer for csv files
 * @param  {string} filePath
 * @param {JobConfig} jobConfig
 */
function csvStreamer(filePath, jobConfig) {
	const fileStream = fs.createReadStream(path.resolve(filePath));
	const mappings = Object.entries(jobConfig.aliases);
	const csvParser = Papa.parse(Papa.NODE_STREAM_INPUT, {
		header: true,
		skipEmptyLines: true,

		//rename's header keys to match aliases
		transformHeader: (header) => {
			const mapping = mappings.filter(pair => pair[0] === header).pop();
			if (mapping) header = mapping[1];
			return header;
		}

	});
	const transformer = new stream.Transform({
		objectMode: true, highWaterMark: jobConfig.highWater, transform: (chunk, encoding, callback) => {
			const { distinct_id = "", $insert_id = "", time = 0, event, ...props } = chunk;
			const mixpanelEvent = {
				event,
				properties: {
					distinct_id,
					$insert_id,
					time: dayjs.utc(time).valueOf(),
					...props
				}
			};
			if (!distinct_id) delete mixpanelEvent.properties.distinct_id;
			if (!$insert_id) delete mixpanelEvent.properties.$insert_id;
			if (!time) delete mixpanelEvent.properties.time;

			callback(null, mixpanelEvent);
		}
	});
	const outStream = fileStream.pipe(csvParser).pipe(transformer);
	return outStream;
}

/**
 * streamer for csv files
 * @param  {string} filePath
 * @param {JobConfig} jobConfig
 */
async function csvMemory(filePath, jobConfig) {
	/** @type {string} */
	// @ts-ignore
	const fileContents = await u.load(filePath, false);
	const mappings = Object.entries(jobConfig.aliases);
	const csvParser = Papa.parse(fileContents, {
		header: true,
		skipEmptyLines: true,

		//rename's header keys to match aliases
		transformHeader: (header) => {
			const mapping = mappings.filter(pair => pair[0] === header).pop();
			if (mapping) header = mapping[1];
			return header;
		}

	});
	const data = csvParser.data.map((chunk) => {
		const { distinct_id = "", $insert_id = "", time = 0, event, ...props } = chunk;
		const mixpanelEvent = {
			event,
			properties: {
				distinct_id,
				$insert_id,
				time: dayjs.utc(time).valueOf(),
				...props
			}
		};
		if (!distinct_id) delete mixpanelEvent.properties.distinct_id;
		if (!$insert_id) delete mixpanelEvent.properties.$insert_id;
		if (!time) delete mixpanelEvent.properties.time;
		return mixpanelEvent;
	});
	return stream.Readable.from(data, { objectMode: true, highWaterMark: jobConfig.highWater });

}

/**
 * @param  {JobConfig} jobConfig
 */
function chunkForSize(jobConfig) {
	let pending = [];
	let totalSize = 0; // maintain a running total of size

	return (err, x, push, next) => {
		const maxBatchSize = jobConfig.bytesPerBatch;
		const maxBatchCount = jobConfig.recordsPerBatch;

		if (err) {
			push(err);
			next();
		} else if (x === _.nil) {
			if (pending.length > 0) {
				push(null, pending);
				pending = [];
				totalSize = 0; // reset total size
			}
			push(null, x);
		} else {
			for (const item of x) {
				const itemSize = Buffer.byteLength(JSON.stringify(item), 'utf-8');

				// Check for individual items exceeding size
				if (itemSize > maxBatchSize) {
					console.warn('Dropping an oversized record.');
					continue;
				}

				pending.push(item);
				totalSize += itemSize;

				// Check size and count constraints
				while (totalSize > maxBatchSize || pending.length > maxBatchCount) {
					const chunk = [];
					let size = 0;

					while (pending.length > 0) {
						const item = pending[0];
						const itemSize = Buffer.byteLength(JSON.stringify(item), 'utf-8');

						if (size + itemSize > maxBatchSize || chunk.length >= maxBatchCount) {
							break;
						}

						size += itemSize;
						totalSize -= itemSize; // reduce from total size
						chunk.push(item);
						pending.shift();
					}

					push(null, chunk);
				}
			}

			next();
		}
	};
}

function getEnvVars() {
	const envVars = pick(process.env, `MP_PROJECT`, `MP_ACCT`, `MP_PASS`, `MP_SECRET`, `MP_TOKEN`, `MP_TYPE`, `MP_TABLE_ID`, `MP_GROUP_KEY`, `MP_START`, `MP_END`);
	const envKeyNames = {
		MP_PROJECT: "project",
		MP_ACCT: "acct",
		MP_PASS: "pass",
		MP_SECRET: "secret",
		MP_TOKEN: "token",
		MP_TYPE: "recordType",
		MP_TABLE_ID: "lookupTableId",
		MP_GROUP_KEY: "groupKey",
		MP_START: "start",
		MP_END: "end"
	};
	const envCreds = u.rnKeys(envVars, envKeyNames);

	return envCreds;
}

module.exports = {
	JsonlParser,
	determineDataType,
	existingStreamInterface,
	StreamArray,
	itemStream,
	getEnvVars,
	chunkForSize
};
