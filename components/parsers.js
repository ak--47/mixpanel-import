const readline = require('readline');
const stream = require('stream');
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


/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */


/**
 * @param  {any} data
 * @param  {JobConfig} jobConfig
 */
async function determineDataType(data, jobConfig) {
	//exports are saved locally
	if (jobConfig.recordType === 'export') {
		if (jobConfig.where) {
			return path.resolve(jobConfig.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return filename;
	}

	if (jobConfig.recordType === 'peopleExport') {
		if (jobConfig.where) {
			return path.resolve(jobConfig.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		return path.resolve(folder);
	}

	// lookup tables are not streamed
	if (jobConfig.recordType === 'table') {
		if (fs.existsSync(path.resolve(data))) return await u.load(data);
		return data;
	}

	// data is already a stream
	if (data.pipe || data instanceof stream.Stream) {
		if (data.readableObjectMode) return data;
		return _(existingStreamInterface(data));
	}

	// data is an object in memory
	if (Array.isArray(data)) {
		return stream.Readable.from(data, { objectMode: true, highWaterMark: jobConfig.highWater });
	}

	//todo: support array of files
	try {

		// data refers to file/folder on disk
		if (fs.existsSync(path.resolve(data))) {
			const fileOrDir = fs.lstatSync(path.resolve(data));

			//file case			
			if (fileOrDir.isFile()) {
				//check for jsonl first... many jsonl files will have the same extension as json
				if (jobConfig.streamFormat === 'jsonl' || jobConfig.lineByLineFileExt.includes(path.extname(data))) {
					// !! todo... make DRY
					if (fileOrDir.size < os.freemem() * .50 && !jobConfig.forceStream) {
						const file = /** @type {string} */ (await u.load(path.resolve(data)));
						const parsed = file.trim().split('\n').map(line => JSON.parse(line));
						return stream.Readable.from(parsed, { objectMode: true, highWaterMark: jobConfig.highWater });
					}

					return itemStream(path.resolve(data), "jsonl", jobConfig);
				}

				if (jobConfig.streamFormat === 'json' || jobConfig.objectModeFileExt.includes(path.extname(data))) {
					// !! ugh
					if (fileOrDir.size < os.freemem() * .50 && !jobConfig.forceStream) {
						const file = await u.load(path.resolve(data), true);
						return stream.Readable.from(file, { objectMode: true, highWaterMark: jobConfig.highWater });
					}

					//otherwise, stream it
					return itemStream(path.resolve(data), "json", jobConfig);
				}

				//csv case
				// todo: refactor this inside the itemStream function
				if (jobConfig.streamFormat === 'csv') {
					const fileStream = fs.createReadStream(path.resolve(data));
					const mappings = Object.entries(jobConfig.aliases);
					const csvParser = Papa.parse(Papa.NODE_STREAM_INPUT, {
						header: true,
						skipEmptyLines: true,
						transformHeader: (header) => {
							const mapping = mappings.filter(pair => pair[0] === header).pop();
							if (mapping) header = mapping[1];
							return header;
						}
					});
					const transformer = new stream.Transform({
						// @ts-ignore
						objectMode: true, highWaterMark: jobConfig.highWater, transform: (chunk, encoding, callback) => {
							const { distinct_id = "", $insert_id = "", time, event, ...props } = chunk;
							const mixpanelEvent = {
								event,
								properties: {
									distinct_id,
									$insert_id,
									time: dayjs.utc(time).valueOf(),
									...props
								}
							};
							callback(null, mixpanelEvent);
						}
					});
					const outStream = fileStream.pipe(csvParser).pipe(transformer);
					return outStream;
				}
			}

			//folder case
			if (fileOrDir.isDirectory()) {
				const enumDir = await u.ls(path.resolve(data));
				const files = enumDir.filter(filePath => jobConfig.supportedFileExt.includes(path.extname(filePath)));
				if (jobConfig.streamFormat === 'jsonl' || jobConfig.lineByLineFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "jsonl", jobConfig);
				}
				if (jobConfig.streamFormat === 'json' || jobConfig.objectModeFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "json", jobConfig);
				}
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
			return stream.Readable.from(JSON.parse(data), { objectMode: true, highWaterMark: jobConfig.highWater });
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			// @ts-ignore
			return stream.Readable.from(data.trim().split('\n').map(JSON.parse), { objectMode: true, highWaterMark: jobConfig.highWater });
		}

		catch (e) {
			//noop
		}

		//CSV or TSV
		try {
			return stream.Readable.from(Papa.parse(data, { header: true, skipEmptyLines: true }));
		}
		catch (e) {
			//noop
		}
	}

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string... (i could not determine it's type)`);
	process.exit(1);

}



/**
 * @param  { import("stream").Readable} stream
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

/**
 * @param  {string} filePath
 * @param  {import('../index').SupportedFormats} type="jsonl"
 * @param {JobConfig} jobConfig
 */
function itemStream(filePath, type = "jsonl", jobConfig) {
	let stream;
	let parsedStream;
	const parser = type === "jsonl" ? JsonlParser.parser : StreamArray.withParser;
	const streamOpts = {
		highWaterMark: jobConfig.highWater,
		autoClose: true,
		emitClose: true

	};
	//parsing folders
	if (Array.isArray(filePath)) {

		if (type === "jsonl") {
			stream = new MultiStream(filePath.map((file) => { return fs.createReadStream(file, streamOpts); }), streamOpts);
			parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: jobConfig.parseErrorHandler, ...streamOpts })).map(token => token.value);
			return parsedStream;

		}
		if (type === "json") {
			stream = filePath.map((file) => fs.createReadStream(file));
			parsedStream = MultiStream.obj(stream.map(s => s.pipe(parser(streamOpts)).map(token => token.value)));
			return parsedStream;
		}
	}

	//parsing files
	else {
		stream = fs.createReadStream(filePath, streamOpts);
		parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: jobConfig.parseErrorHandler, ...streamOpts })).map(token => token.value);
	}

	return parsedStream;

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
