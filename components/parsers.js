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






async function determineDataType(data, config) {
	//exports are saved locally
	if (config.recordType === 'export') {
		if (config.where) {
			return path.resolve(config.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return filename;
	}

	if (config.recordType === 'peopleExport') {
		if (config.where) {
			return path.resolve(config.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		return path.resolve(folder);
	}

	// lookup tables are not streamed
	if (config.recordType === 'table') {
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
		return stream.Readable.from(data, { objectMode: true, highWaterMark: config.highWater });
	}

	//todo: support array of files
	try {

		// data refers to file/folder on disk
		if (fs.existsSync(path.resolve(data))) {
			const fileOrDir = fs.lstatSync(path.resolve(data));

			//file case			
			if (fileOrDir.isFile()) {
				//check for jsonl first... many jsonl files will have the same extension as json
				if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75 && !config.forceStream) {
						const file = await u.load(path.resolve(data));
						const parsed = file.trim().split('\n').map(JSON.parse);
						return stream.Readable.from(parsed, { objectMode: true, highWaterMark: config.highWater });
					}

					return itemStream(path.resolve(data), "jsonl", config.highWater);
				}

				if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(data))) {
					// !! if the file is small enough; just load it into memory (is this ok?)
					if (fileOrDir.size < os.freemem() * .75 && !config.forceStream) {
						const file = await u.load(path.resolve(data), true);
						return stream.Readable.from(file, { objectMode: true, highWaterMark: config.highWater });
					}

					//otherwise, stream it
					return itemStream(path.resolve(data), "json", config.highWater);
				}

				//csv case
				// todo: refactor this inside the itemStream function
				if (config.streamFormat === 'csv') {
					const fileStream = fs.createReadStream(path.resolve(data));
					const mappings = Object.entries(config.aliases);
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
						objectMode: true, highWaterMark: config.highWater, transform: (chunk, encoding, callback) => {
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
				const files = enumDir.filter(filePath => config.supportedFileExt.includes(path.extname(filePath)));
				if (config.streamFormat === 'jsonl' || config.lineByLineFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "jsonl", config.highWater);
				}
				if (config.streamFormat === 'json' || config.objectModeFileExt.includes(path.extname(files[0]))) {
					return itemStream(files, "json", config.highWater);
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
			return stream.Readable.from(JSON.parse(data), { objectMode: true, highWaterMark: config.highWater });
		}
		catch (e) {
			//noop
		}

		//stringified JSONL
		try {
			// @ts-ignore
			return stream.Readable.from(data.trim().split('\n').map(JSON.parse), { objectMode: true, highWaterMark: config.highWater });
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

function itemStream(filePath, type = "jsonl", highWater) {
	let stream;
	let parsedStream;
	const parser = type === "jsonl" ? JsonlParser.parser : StreamArray.withParser;
	const streamOpts = {
		highWaterMark: highWater,
		autoClose: true,
		emitClose: true

	};
	//parsing folders
	if (Array.isArray(filePath)) {

		if (type === "jsonl") {
			stream = new MultiStream(filePath.map((file) => { return fs.createReadStream(file, streamOpts); }), streamOpts);
			parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: undefined, ...streamOpts })).map(token => token.value);
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
		parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: undefined, ...streamOpts })).map(token => token.value);
	}

	return parsedStream;

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

//! GPT implementation
/**
 * @param  {import('./config')} config
 */
function chunkForSize(config) {
	let pending = [];
	let totalSize = 0; // maintain a running total of size

	return (err, x, push, next) => {
		const maxBatchSize = config.bytesPerBatch;
		const maxBatchCount = config.recordsPerBatch;

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


module.exports = {
	JsonlParser,
	determineDataType,
	existingStreamInterface,
	StreamArray,
	itemStream,
	getEnvVars,
	chunkForSize
};
