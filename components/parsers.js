const readline = require('readline');
const stream = require('stream');
const { Transform, Readable } = require('stream');
const duckdb = require('duckdb');
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
const { console } = require('inspector');


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

	if (job.recordType === 'profile-delete') return null;

	if (job.recordType === 'annotations') return data;
	if (job.recordType === 'get-annotations') return null;
	if (job.recordType === 'delete-annotations') return null;


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
					if (lineByLineFileExt.includes(path.extname(data))) parsingCase = 'jsonl';
					else if (objectModeFileExt.includes(path.extname(data))) parsingCase = 'json';
					else if (tableFileExt.includes(path.extname(data))) parsingCase = 'csv';
					else if (data?.endsWith('.parquet')) parsingCase = 'parquet';
					if (['jsonl', 'json', 'csv', 'parquet'].includes(streamFormat)) parsingCase = streamFormat;

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

					//parquet case
					if (parsingCase === 'parquet') {
						return await parquetStream(path.resolve(data), job);
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

			if (lineByLineFileExt.includes(exampleFile)) parsingCase = 'jsonl';
			else if (objectModeFileExt.includes(exampleFile)) parsingCase = 'json';
			else if (tableFileExt.includes(exampleFile)) parsingCase = 'csv';
			else if (exampleFile?.endsWith('.parquet')) parsingCase = 'parquet';
			if (['jsonl', 'json', 'csv', 'parquet'].includes(streamFormat)) parsingCase = streamFormat;

			switch (parsingCase) {
				case 'jsonl':
					return itemStream(files, "jsonl", job);
				case 'json':
					return itemStream(files, "json", job);
				case 'csv':
					return csvStreamArray(files, job);
				case 'parquet':
					return parquetStreamArray(files, job);
				default:
					return itemStream(files, "jsonl", job);
			}
		}
	}


	catch (e) {
		debugger;
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
			if (data.length > 420) {
				return stream.Readable.from(Papa.parse(data, { header: true, skipEmptyLines: true }).data, { objectMode: true, highWaterMark: job.highWater });
			}
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
 * Creates an object-mode stream that reads Parquet files row by row.
 * @param {string} filename - Path to the Parquet file.
 * @param {JobConfig} job
 * @returns {Promise<Readable>} - A readable stream emitting Parquet rows.
 */
async function parquetStream_OLD_IMPLEMENTATION(filename, job) {
	const filePath = path.resolve(filename);
	let reader = null;
	let cursor = null;
	let isReading = false; // To prevent concurrent reads

	try {
		reader = await parquet.ParquetReader.openFile(filePath);
		cursor = reader.getCursor();
	} catch (err) {
		// console.error(`Failed to open parquet file ${filePath}:`, err);
		// Return an empty stream if we can't open the file
		return new Readable({
			objectMode: true,
			read() {
				this.push(null);
			}
		});
	}

	const fileErrorHandler = function (err) {
		console.error(`Error processing ${filePath}:`, err?.message || err);
		return null;
	};

	const rowErrorHandler = job?.parseErrorHandler || function (err, record) {
		return {};
	};

	const stream = new Readable({
		objectMode: true,
		read() {
			if (isReading) return; // Prevent concurrent reads
			isReading = true;

			(async () => {
				try {
					let record;
					try {
						record = await cursor.next();
					} catch (err) {
						const handledError = fileErrorHandler(err);
						this.push(handledError);
						isReading = false;
						return; // Continue with next read
					}

					if (record) {
						try {
							for (const key in record) {
								const value = record[key];
								if (value instanceof Date) {
									record[key] = dayjs.utc(value).toISOString();
								}
								//big int
								if (typeof value === 'bigint') {
									try {
										// Check if the bigint is within safe integer range
										if (value <= Number.MAX_SAFE_INTEGER && value >= Number.MIN_SAFE_INTEGER) {
											record[key] = Number(value);
										} else {
											record[key] = value.toString();
										}
									} catch {
										record[key] = value.toString();
									}
								}

								if (Buffer.isBuffer(value)) {
									record[key] = value.toString('utf-8'); // or 'base64' if needed
								}

								if (value === undefined) {
									record[key] = null;
								}
							}
							this.push(record);
						} catch (err) {
							const handledError = rowErrorHandler(err);
							this.push(handledError);

						}
					} else {
						// End of file reached
						await reader.close();
						reader = null;
						this.push(null);
					}
				} catch (err) {
					console.error(`Fatal error processing ${filePath}:`, err);
					this.destroy(err);
				} finally {
					isReading = false;
				}
			})();
		},
		async destroy(err, callback) {
			try {
				if (reader) {
					await reader.close();
					reader = null;
				}
				callback(err);
			} catch (closeErr) {
				callback(closeErr || err);
			}
		},
	});

	return stream;
}

/**
 * Creates a factory function for MultiStream that lazily creates parquet streams
 * @param {string[]} filePaths 
 * @param {JobConfig} job
 * @returns {(callback: (error: Error | null, stream: Readable | null) => void) => void}
 */
function createParquetFactory(filePaths, job) {
	let currentIndex = 0;

	return function factory(callback) {
		if (currentIndex >= filePaths.length) {
			callback(null, null); // Signal end of all files
			return;
		}

		const currentPath = filePaths[currentIndex];
		currentIndex++;
		// console.log(`Processing parquet file: ${currentPath}`);
		parquetStream(currentPath, job)
			.then(stream => {
				// Add error handler to prevent stream from breaking
				stream.on('error', (err) => {
					console.error(`Error processing ${currentPath}:`, err);
					// Stream will automatically move to next file
				});
				callback(null, stream);
			})
			.catch(err => {
				console.error(`Failed to create stream for ${currentPath}:`, err);
				// Call factory again to move to next file
				factory(callback);
			});
	};
}

/**
 * wraps parquetStream with MultiStream to turn a folder of parquet files into a single stream
 * @param  {string[]} filePaths
 * @param {JobConfig} job
 */
function parquetStreamArray(filePaths, job) {
	// @ts-ignore
	const lazyStreamGen = createParquetFactory(filePaths, job);
	return MultiStream.obj(lazyStreamGen);
}

/**
 * Streams rows from a Parquet file via DuckDB's each-row API, but
 * detects EOF by first querying COUNT(*) and then counting callbacks.
 *
 * @param {string}   filename – path to the Parquet file
 * @param {object}   [job]    – may include parseErrorHandler/fileErrorHandler
 * @returns {Promise<Readable>} – object-mode Readable of sanitized rows
 */
async function parquetStream(filename, job = {}) {
	const filePath = path.resolve(filename);
	const db = new duckdb.Database(':memory:');
	const conn = db.connect();

	// SQL pieces, with single-quotes escaped
	const esc = filePath.replace(/'/g, "''");
	const dataSQL = `SELECT * FROM read_parquet('${esc}')`;
	const countSQL = `SELECT COUNT(*) AS cnt FROM read_parquet('${esc}')`;

	// Handlers
	const fileErrorHandler = job.fileErrorHandler || (err => {
		console.error(`Error reading ${filePath}:`, err.message || err);
		throw new Error(`Error reading ${filePath}: ${err.message || err}`);
		return null;
	});
	const rowErrorHandler = job.parseErrorHandler || ((err, row) => {
		console.error(`Error parsing row from ${filePath}:`, err);
		return {};
	});

	// 1) Get total row count (cheap, returns a single number)
	let total = await new Promise(resolve => {
		conn.all(countSQL, (err, rows) => {
			if (err) {
				console.error(`Failed to count ${filePath}:`, err);
				resolve(0);
			} else {
				resolve(rows[0]?.cnt ?? 0);
			}
		});
	});
	total = Number(total); // Ensure it's a number

	// 2) Build our output Readable
	const out = new Readable({
		objectMode: true,
		read() { }
	});
	out._destroy = (err, cb) => db.close(() => cb(err));

	// 3) Zero-row shortcut
	if (total === 0) {
		process.nextTick(() => db.close(() => out.push(null)));
		return out;
	}

	// 4) Stream rows, counting as we go
	let seen = 0;
	conn.each(
		dataSQL,
		(err, row) => {
			seen++;
			if (err) {
				out.push(fileErrorHandler(err));
			} else {
				// sanitize in-place
				for (const k of Object.keys(row)) {
					const v = row[k];
					if (v?.toISOString) row[k] = dayjs.utc(v).toISOString();
					else if (typeof v === 'bigint') row[k] =
						(v <= Number.MAX_SAFE_INTEGER && v >= Number.MIN_SAFE_INTEGER)
							? Number(v)
							: v.toString();
					else if (Buffer.isBuffer(v)) row[k] = v.toString('utf-8');
					else if (v === undefined) row[k] = null;
				}
				out.push(row);
			}

			// If we've now emitted the last row, close & EOF
			if (seen >= total) {
				db.close(closeErr => {
					if (closeErr) out.destroy(closeErr);
					else out.push(null);
				});
			}
		}
		// no completion callback here ☝️
	);

	return out;
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


/**
 * @param {string} filePath  Path to either:
 *   • a JSON file containing an array of {id, distinct_id} objects, OR
 *   • an NDJSON file (one JSON object per line)
 * @returns {Map<string,string>}  Maps item.id → item.distinct_id
 */
function buildDeviceIdMap(filePath, keyOne = "distinct_id", keyTwo = "person_id") {

	if (!filePath) {
		return new Map();
	}

	const fileContents = fs.readFileSync(path.resolve(filePath), "utf-8");
	let records;

	// Try parsing as a JSON array first…
	try {
		if (!fileContents.startsWith('[')) throw new Error("probably not a json array");
		const parsed = JSON.parse(fileContents);
		if (!Array.isArray(parsed)) {
			throw new Error("Not an array");
		}
		records = parsed;
	} catch {
		// Fallback to NDJSON: one JSON object per line
		records = fileContents
			.split(/\r?\n/)
			.filter(line => line.trim().length > 0)
			.map((line, idx) => {
				try {
					return JSON.parse(line);
				} catch (e) {
					throw new Error(`Invalid JSON on line ${idx + 1}: ${e.message}`);
				}
			});
	}

	// Build the Map<id, distinct_id>
	const idMap = records.reduce((map, item) => {
		if (item[keyOne] == null || item[keyTwo] == null) {
			// you can choose to warn or skip silently
			return map;
		}
		map.set(item[keyOne], item[keyTwo]);
		return map;
	}, new Map());

	return idMap;
}


module.exports = {
	buildDeviceIdMap,
	JsonlParser,
	determineDataType,
	existingStreamInterface,
	StreamArray,
	itemStream,
	getEnvVars,
	chunkForSize
};
