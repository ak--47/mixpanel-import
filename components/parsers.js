const readline = require('readline');
const stream = require('stream');
const { Transform, Readable } = require('stream');
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
const { streamEvents, streamProfiles } = require('../components/exporters.js');
const zlib = require('zlib');
// const { logger } = require('../components/logs.js');
const { NODE_ENV = "unknown" } = process.env;

/**
 * Determine if file is gzipped and extract base format
 * @param {string} filePath - Path to file
 * @param {JobConfig} job - Job configuration
 * @returns {{isGzipped: boolean, baseFormat: string, parsingCase: string}}
 */
function analyzeFileFormat(filePath, job) {
	const { lineByLineFileExt, objectModeFileExt, tableFileExt, gzippedLineByLineFileExt, gzippedObjectModeFileExt, gzippedTableFileExt, isGzip } = job;

	// isGzip option overrides extension detection
	if (isGzip) {
		// When forcing gzip, determine format from base filename (remove .gz if present)
		const baseFileName = filePath.endsWith('.gz') ? filePath.slice(0, -3) : filePath;
		const baseExt = path.extname(baseFileName);

		let parsingCase = '';
		if (lineByLineFileExt.includes(baseExt)) parsingCase = 'jsonl';
		else if (objectModeFileExt.includes(baseExt)) parsingCase = 'json';
		else if (tableFileExt.includes(baseExt)) parsingCase = 'csv';
		else if (baseFileName.endsWith('.parquet')) parsingCase = 'parquet';

		return { isGzipped: true, baseFormat: baseExt, parsingCase };
	}

	// Check for gzipped extensions first
	if (gzippedLineByLineFileExt.some(ext => filePath.endsWith(ext))) {
		// Extract the base format from the gzipped filename
		const baseFormat = filePath.endsWith('.gz') ? path.extname(filePath.slice(0, -3)) : path.extname(filePath);
		return { isGzipped: true, baseFormat, parsingCase: 'jsonl' };
	}
	if (gzippedObjectModeFileExt.some(ext => filePath.endsWith(ext))) {
		// Extract the base format from the gzipped filename
		const baseFormat = filePath.endsWith('.gz') ? path.extname(filePath.slice(0, -3)) : path.extname(filePath);
		return { isGzipped: true, baseFormat, parsingCase: 'json' };
	}
	if (gzippedTableFileExt.some(ext => filePath.endsWith(ext))) {
		// Extract the base format from the gzipped filename  
		const baseFormat = filePath.endsWith('.gz') ? path.extname(filePath.slice(0, -3)) : path.extname(filePath);
		return { isGzipped: true, baseFormat, parsingCase: 'csv' };
	}
	if (filePath.endsWith('.parquet.gz')) {
		return { isGzipped: true, baseFormat: '.parquet', parsingCase: 'parquet' };
	}

	// Check for regular extensions
	let parsingCase = '';
	if (lineByLineFileExt.includes(path.extname(filePath))) parsingCase = 'jsonl';
	else if (objectModeFileExt.includes(path.extname(filePath))) parsingCase = 'json';
	else if (tableFileExt.includes(path.extname(filePath))) parsingCase = 'csv';
	else if (filePath.endsWith('.parquet')) parsingCase = 'parquet';

	return { isGzipped: false, baseFormat: path.extname(filePath), parsingCase };
}

const { Storage } = require('@google-cloud/storage');
const { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');

// Lazy load hyparquet since it's an ES module
let parquetRead = null;
const getParquetRead = async () => {
	if (!parquetRead) {
		try {
			const hyparquet = await import('hyparquet');
			parquetRead = hyparquet.parquetRead;
		} catch (error) {
			throw new Error(`Failed to load hyparquet: ${error.message}. Make sure hyparquet is installed: npm install hyparquet`);
		}
	}
	return parquetRead;
};

// ====================================================================
// PARSER PERFORMANCE TUNING CONSTANTS
// ====================================================================
// All configurable parameters for optimizing parsing performance
// Adjust these values based on your hardware and data characteristics
//
// PERFORMANCE TUNING GUIDE:
// - High Throughput: Increase buffer multipliers, memory thresholds, worker counts  
// - Memory Constrained: Decrease buffer sizes, lower memory threshold
// - Large Files: Increase chunk sizes, stream multipliers
// - Small Files: Use memory loading, smaller buffers
// - Reliable Data: Disable validation, increase error limits
// - Noisy Data: Lower error limits, enable more validation
//
// TOP IMPACT TUNING PARAMETERS:
// 1. MEMORY_CONFIG.FREE_MEMORY_THRESHOLD (0.5 → 0.8 for dedicated processing)
// 2. GCS_STREAMING_CONFIG.GCS_BUFFER_MULTIPLIER (100 → 200 for large files)
// 3. STREAM_CONFIG.OBJECT_MODE_MULTIPLIER (2 → 10 for high throughput)
// 4. JSON_CONFIG.OBJECT_STREAM_HIGH_WATER (1000 → 5000 for large datasets)

// GCS Streaming Configuration
const GCS_STREAMING_CONFIG = {
	// Buffer sizes for high-performance streaming
	GCS_BUFFER_MULTIPLIER: 100,      // Multiply job.highWater by this for GCS reads
	OBJECT_STREAM_MULTIPLIER: 10,    // Multiply job.highWater by this for object stream

	// Gzip decompression settings
	GZIP_CHUNK_SIZE: 64 * 1024,      // 64KB chunks for decompression
	GZIP_WINDOW_BITS: 15,            // Standard gzip window size
	GZIP_MEM_LEVEL: 8,               // Memory vs speed tradeoff (1-9, 8 is balanced)

	// Error handling
	MAX_PARSE_ERRORS: 1000,          // Stop logging parse errors after this many

	// GCS-specific optimizations
	DISABLE_VALIDATION: false,       // Set to true for maximum speed on trusted files
	DECOMPRESS: false                // Let us handle compression manually
};

// S3 Streaming Configuration
const S3_STREAMING_CONFIG = {
	// Buffer sizes for high-performance streaming
	S3_BUFFER_MULTIPLIER: 100,       // Multiply job.highWater by this for S3 reads
	OBJECT_STREAM_MULTIPLIER: 10,    // Multiply job.highWater by this for object stream

	// Gzip decompression settings
	GZIP_CHUNK_SIZE: 64 * 1024,      // 64KB chunks for decompression
	GZIP_WINDOW_BITS: 15,            // Standard gzip window size
	GZIP_MEM_LEVEL: 8,               // Memory vs speed tradeoff (1-9, 8 is balanced)

	// Error handling
	MAX_PARSE_ERRORS: 1000,          // Stop logging parse errors after this many

	// S3-specific optimizations
	REQUEST_TIMEOUT: 30000,          // 30 second timeout for S3 requests
	MAX_RETRY_ATTEMPTS: 3,           // Number of retry attempts for failed requests
	PART_SIZE: 5 * 1024 * 1024,      // 5MB part size for streaming

	// Default region (can be overridden)
	DEFAULT_REGION: 'us-east-1'      // Default AWS region if none specified
};

// Memory Management Configuration
const MEMORY_CONFIG = {
	FREE_MEMORY_THRESHOLD: 0.50,     // Load files into memory only if under 50% of free RAM
	MEMORY_SAMPLE_RATE: 0.00005,     // Memory sampling probability (0.005%)
	MAX_MEMORY_SAMPLES: 100          // Limit memory samples in circular buffer (from job.js)
};

// File Processing Thresholds
const FILE_PROCESSING_CONFIG = {
	CSV_MIN_LENGTH: 420,             // Minimum string length for CSV parsing attempts
	PARQUET_CHUNK_SIZE: 1000,        // Records per chunk for Parquet processing

	// Stream vs Memory decision points
	SMALL_FILE_THRESHOLD: 50 * 1024 * 1024,  // 50MB - files smaller load into memory
	LARGE_FILE_STREAM_FORCE: true,    // Always stream files larger than memory threshold
};

// Stream Configuration (prepared for future use)
const _STREAM_CONFIG = {
	DEFAULT_HIGH_WATER_MARK: 16,     // Default stream buffer size (can be overridden by job.highWater)
	OBJECT_MODE_MULTIPLIER: 2,       // Multiply buffer size for object mode streams  
	AUTO_CLOSE: true,                // Auto-close file streams
	EMIT_CLOSE: true,                // Emit close events

	// File stream optimizations
	FILE_STREAM_FLAGS: 'r',          // Read-only file access
	FILE_STREAM_ENCODING: null       // Binary mode for maximum performance
};

// JSON/JSONL Processing Configuration
const JSON_CONFIG = {
	PARSE_ERROR_LIMIT: 1000,         // Maximum parse errors before stopping
	LINE_BUFFER_SIZE: 8 * 1024,      // 8KB line buffer for JSONL processing
	OBJECT_STREAM_HIGH_WATER: 1000,  // Object mode stream buffer size

	// Memory optimizations
	STRINGIFY_SPACE: 0,              // No pretty printing for performance
	REVIVER_FUNCTION: null           // No custom JSON parsing (performance)
};

// CSV Processing Configuration  
const CSV_CONFIG = {
	header: true,                 // First row contains headers
	skipEmptyLines: true,         // Skip blank lines
	fastMode: false,              // Disable fast mode for better error handling
	worker: false,                // Main thread processing (workers add overhead for small files)
	chunk: undefined,             // Process entire file at once for memory files
	dynamicTyping: false,         // Disable type inference for performance
	encoding: 'utf8'              // Default encoding
};

// Compression Configuration
const COMPRESSION_CONFIG = {
	GZIP_LEVEL: 6,                   // Default compression level (1-9, 6 is balanced)
	GZIP_WINDOW_BITS: 15,            // Standard gzip window
	GZIP_MEM_LEVEL: 8,               // Memory usage level (1-9)
	GZIP_CHUNK_SIZE: 16 * 1024,      // 16KB chunks for general gzip operations

	// Detection patterns
	GZIP_EXTENSIONS: ['.gz', '.gzip'],
	COMPRESSION_THRESHOLD: 1024       // Minimum bytes to consider compression
};

// Error Handling Configuration (prepared for future use)
const _ERROR_CONFIG = {
	MAX_PARSE_ERRORS: 1000,          // Stop after this many parse errors
	MAX_RETRY_ATTEMPTS: 3,           // File operation retries
	ERROR_SAMPLE_RATE: 0.1,          // Log only 10% of similar errors

	RECOVERABLE_ERRORS: [
		'ENOENT', 'EACCES', 'EMFILE', 'ECONNRESET'
	]
};

/** @typedef {InstanceType<typeof import('./job')>} JobConfig */
/** @typedef {import('../index').Data} Data */


/**
 * @param  {any} data
 * @param  {JobConfig} job
 */
async function determineDataType(data, job) {
	// const l = logger(job);
	//exports are saved locally or to cloud storage
	if (job.recordType === 'export') {
		if (job.where) {
			// Don't resolve cloud paths - return them as-is
			if (job.where.startsWith('gs://') || job.where.startsWith('s3://')) {
				return job.where;
			}
			return path.resolve(job.where);
		}
		const folder = u.mkdir('./mixpanel-exports');
		const filename = path.resolve(`${folder}/export-${dayjs().format(dateFormat)}-${u.rand()}.ndjson`);
		await u.touch(filename);
		return filename;
	}

	if (job.recordType === 'export-import-events') {
		const exportStream = streamEvents(job);
		job.recordType = 'event';

		if (job.secondToken) {
			job.token = job.secondToken;
			job.secret = "";
			job.auth = job.resolveProjInfo();
		}

		// @ts-ignore
		if (job.secondRegion) job.region = job.secondRegion;

		return exportStream;
	}

	if (job.recordType === 'export-import-profiles') {
		const exportStream = streamProfiles(job);
		if (job.dataGroupId || job.groupKey) job.recordType = 'group';
		else job.recordType = 'user';

		if (job.secondToken) {
			job.token = job.secondToken;
			job.secret = "";
			job.auth = job.resolveProjInfo();
		}
		// @ts-ignore
		if (job.secondRegion) job.region = job.secondRegion;
		return exportStream;
	}

	if (job.recordType === 'profile-export') {
		if (job.where) {
			// Don't resolve cloud paths - return them as-is  
			if (job.where.startsWith('gs://') || job.where.startsWith('s3://')) {
				return job.where;
			}
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

	// CLOUD STORAGE PARSING

	// Handle Google Cloud Storage URLs (gs://)
	if (typeof data === 'string' && data.startsWith('gs://')) {
		job.wasStream = true;
		return createGCSStream(data, job);
	}

	// Handle array of Google Cloud Storage URLs
	if (Array.isArray(data) && data.every(item => typeof item === 'string' && item.startsWith('gs://'))) {
		job.wasStream = true;
		return createMultiGCSStream(data, job);
	}

	// Handle Amazon S3 URLs (s3://)
	if (typeof data === 'string' && data.startsWith('s3://')) {
		job.wasStream = true;
		return createS3Stream(data, job);
	}

	// Handle array of Amazon S3 URLs
	if (Array.isArray(data) && data.every(item => typeof item === 'string' && item.startsWith('s3://'))) {
		job.wasStream = true;
		return createMultiS3Stream(data, job);
	}


	// ALL OTHER PARSING
	let parsingError;
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
					const { isGzipped, baseFormat, parsingCase: detectedCase } = analyzeFileFormat(data, job);
					let parsingCase = detectedCase;

					// Allow streamFormat to override detected format
					if (['jsonl', 'strict_json', 'csv', 'parquet'].includes(streamFormat)) parsingCase = streamFormat;

					let loadIntoMemory = false;
					if (fileInfo.size < os.freemem() * MEMORY_CONFIG.FREE_MEMORY_THRESHOLD) loadIntoMemory = true;
					if (forceStream) loadIntoMemory = false;
					// Gzipped files must be streamed - cannot load into memory without decompression
					if (isGzipped) loadIntoMemory = false;

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
						return itemStream(path.resolve(data), "jsonl", job, isGzipped);
					}

					if (parsingCase === 'strict_json') {
						if (loadIntoMemory) {
							try {
								job.wasStream = false;
								const file = await u.load(path.resolve(data), true);
								let fileContents;
								if (Array.isArray(file)) fileContents = file;
								else fileContents = [file];
								// @ts-ignore
								return stream.Readable.from(fileContents, { objectMode: true, highWaterMark: highWater });
							}
							catch (e) {
								// probably a memory crash, so we'll try to stream it
							}
						}

						//otherwise, stream it
						job.wasStream = true;
						// @ts-ignore
						return itemStream(path.resolve(data), "json", job, isGzipped);
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
						return csvStreamer(path.resolve(data), job, isGzipped);
					}

					//parquet case
					if (parsingCase === 'parquet') {
						return await parquetStream(path.resolve(data), job, isGzipped);
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
			let isGzipped = false;

			if (isArrayOfFileNames && Array.isArray(data)) {
				//array of files case
				files = data.map(filePath => path.resolve(filePath));
				exampleFile = files[0];
			}
			else {
				//directory case
				const enumDir = await u.ls(path.resolve(data));
				files = Array.isArray(enumDir) ? enumDir.filter(filePath => supportedFileExt.includes(path.extname(filePath))) : [];
				exampleFile = files[0] || '';
			}

			// Analyze format using the first file as example
			if (exampleFile) {
				const analysis = analyzeFileFormat(exampleFile, job);
				parsingCase = analysis.parsingCase;
				isGzipped = analysis.isGzipped;

				// Validate all files have same format (basic check)
				if (files.length > 1) {
					const allSameFormat = files.every(file => {
						const fileAnalysis = analyzeFileFormat(file, job);
						return fileAnalysis.parsingCase === parsingCase && fileAnalysis.isGzipped === isGzipped;
					});
					if (!allSameFormat) {
						throw new Error('All files in array/directory must have the same format and compression (gzipped or not gzipped)');
					}
				}
			}

			if (['jsonl', 'strict_json', 'csv', 'parquet'].includes(streamFormat)) parsingCase = streamFormat;

			switch (parsingCase) {
				case 'jsonl':
					return itemStream(files, "jsonl", job, isGzipped);
				case 'strict_json':
					return itemStream(files, "strict_json", job, isGzipped);
				case 'csv':
					return csvStreamArray(files, job, isGzipped);
				case 'parquet':
					return parquetStreamArray(files, job, isGzipped);
				default:
					return itemStream(files, "jsonl", job, isGzipped);
			}
		}
	}


	catch (e) {
		if (NODE_ENV === "dev") debugger;
		parsingError = e;

	}



	// data is a string, and we have to guess what it is
	if (typeof data === 'string') {
		// Special check for gzipped parquet files - they should not be processed as regular strings
		if (data.endsWith('.parquet.gz')) {
			throw new Error(`Gzipped parquet files (${data}) are not yet supported for local files. Please decompress the file first or use cloud storage which supports .parquet.gz files.`);
		}
		
		// If we have a parsing error from file processing and the data looks like a file path, 
		// throw the error instead of trying to parse the path as data
		if (parsingError && (data.includes('/') || data.includes('\\') || data.includes('.'))) {
			throw parsingError;
		}

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
			if (data.length > FILE_PROCESSING_CONFIG.CSV_MIN_LENGTH) {
				// @ts-ignore
				return stream.Readable.from(Papa.parse(data, CSV_CONFIG).data, { objectMode: true, highWaterMark: job.highWater });
			}
		}
		catch (e) {
			//noop
		}
	}

	console.error(`ERROR:\n\t${data} is not a file, a folder, an array, a stream, or a string... (i could not determine it's type)`);
	if (parsingError) {
		throw parsingError;
	}
	else {
		throw new Error('a very unusual error has occurred');
	}
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
		transform(chunk, _encoding, callback) {
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
function itemStream(filePath, type = "jsonl", job, isGzipped = false) {
	let stream;
	let parsedStream;
	const parser = type === "jsonl" ? JsonlParser.parser : StreamArray.withParser;
	const streamOpts = {
		highWaterMark: job.highWater,
		autoClose: true,
		emitClose: true

	};

	/**
	 * Create a stream pipeline with optional gzip decompression
	 * @param {string} file - file path
	 * @returns {stream.Readable | stream.Transform} - processed stream
	 */
	const createStreamWithGzipSupport = (file) => {
		let fileStream = fs.createReadStream(file, streamOpts);

		// Add gzip decompression if needed
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: COMPRESSION_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: COMPRESSION_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: COMPRESSION_CONFIG.GZIP_MEM_LEVEL
			});
			fileStream = fileStream.pipe(gunzip);
		}

		return fileStream;
	};

	//parsing folders
	if (Array.isArray(filePath)) {

		if (type === "jsonl") {
			stream = new MultiStream(filePath.map((file) => createStreamWithGzipSupport(file).pipe(createEnsureNewlineTransform())), streamOpts);
			// @ts-ignore
			parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: job.parseErrorHandler, ...streamOpts })).map(token => token.value);
			return parsedStream;

		}
		if (type === "strict_json") {
			stream = filePath.map((file) => createStreamWithGzipSupport(file));
			// @ts-ignore
			parsedStream = MultiStream.obj(stream.map(s => s.pipe(parser(streamOpts)).map(token => token.value)));
			return parsedStream;
		}
	}

	//parsing files
	else {
		stream = createStreamWithGzipSupport(filePath);
		// @ts-ignore
		parsedStream = stream.pipe(parser({ includeUndecided: false, errorIndicator: job.parseErrorHandler, ...streamOpts })).map(token => token.value);
	}

	return parsedStream;

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
function parquetStreamArray(filePaths, job, isGzipped = false) {
	// Check for gzipped parquet files in the array
	if (isGzipped) {
		throw new Error(`Gzipped parquet files are not yet supported for local files. Please decompress the files first or use cloud storage which supports .parquet.gz files.`);
	}

	// @ts-ignore
	const lazyStreamGen = createParquetFactory(filePaths, job);
	// @ts-ignore
	return MultiStream.obj(lazyStreamGen);
}

/**
 * Streams rows from a Parquet file using hyparquet for pure JavaScript processing.
 * Replaces DuckDB dependency with native JS solution.
 *
 * @param {string}   filename – path to the Parquet file
 * @param {object}   [job]    - may include parseErrorHandler/fileErrorHandler
 * @returns {Promise<Readable>} – object-mode Readable of sanitized rows
 */
async function parquetStream(filename, job = {}, isGzipped = false) {
	const filePath = path.resolve(filename);

	// Check if gzipped parquet files are supported
	if (isGzipped) {
		throw new Error(`Gzipped parquet files (${filePath}) are not yet supported for local files. Please decompress the file first or use cloud storage which supports .parquet.gz files.`);
	}

	// Handlers
	const fileErrorHandler = job.fileErrorHandler || (err => {
		console.error(`Error reading ${filePath}:`, err.message || err);
		throw new Error(`Error reading ${filePath}: ${err.message || err}`);
	});
	const parseErrorHandler = job.parseErrorHandler || ((err, row) => {
		console.error(`Error parsing row`, row, `from ${filePath}:`, err);
		return {};
	});

	try {
		// Check if file exists
		if (!fs.existsSync(filePath)) {
			throw new Error(`File not found: ${filePath}`);
		}

		// Read the parquet file into buffer
		const buffer = fs.readFileSync(filePath);

		// Create async buffer interface for hyparquet (convert Buffer to ArrayBuffer)
		const asyncBuffer = {
			byteLength: buffer.length,
			slice: (start, end) => {
				const subBuffer = buffer.subarray(start, end);
				// Convert Buffer to ArrayBuffer
				const arrayBuffer = subBuffer.buffer.slice(
					subBuffer.byteOffset,
					subBuffer.byteOffset + subBuffer.byteLength
				);
				return Promise.resolve(arrayBuffer);
			}
		};

		// Use hyparquet's streaming interface (dynamically loaded)
		const parquetReadFn = await getParquetRead();

		// Read parquet data first, then create stream
		const parquetData = await new Promise((resolve, reject) => {
			parquetReadFn({
				file: asyncBuffer,
				rowFormat: 'object',
				onComplete: (data) => {
					try {
						// Sanitize each row in-place before resolving (same logic as DuckDB version)
						if (data && data.length > 0) {
							const sanitizedData = [];
							for (const row of data) {
								try {
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
									sanitizedData.push(row);
								} catch (rowError) {
									// Use parseErrorHandler for row-level errors, push the result
									const errorResult = parseErrorHandler(rowError, row);
									if (errorResult !== null && errorResult !== undefined) {
										sanitizedData.push(errorResult);
									}
								}
							}
							resolve(sanitizedData);
						} else {
							resolve([]);
						}
					} catch (sanitizeError) {
						reject(new Error(`Error sanitizing parquet data: ${sanitizeError.message}`));
					}
				}
			}).catch(reject);
		});

		// Create readable stream from the parsed data
		return stream.Readable.from(parquetData, {
			objectMode: true,
			highWaterMark: job.highWater
		});

	} catch (error) {
		return fileErrorHandler(error);
	}
}



/**
 * wraps csvStream with MultiStream to turn a folder of csv files into a single stream
 * @param  {string[]} filePaths
 * @param  {JobConfig} jobConfig
 */
function csvStreamArray(filePaths, jobConfig, isGzipped = false) {
	const streams = filePaths.map((filePath) => {
		return csvStreamer(filePath, jobConfig, isGzipped);
	});
	return MultiStream.obj(streams);
}

/**
 * streamer for csv files
 * @param  {string} filePath
 * @param {JobConfig} jobConfig
 * @param {boolean} isGzipped
 * @returns {stream.Readable}
 */
function csvStreamer(filePath, jobConfig, isGzipped = false) {
	let fileStream = fs.createReadStream(path.resolve(filePath));

	// Add gzip decompression if needed
	if (isGzipped) {
		const gunzip = zlib.createGunzip({
			chunkSize: COMPRESSION_CONFIG.GZIP_CHUNK_SIZE,
			windowBits: COMPRESSION_CONFIG.GZIP_WINDOW_BITS,
			level: zlib.constants.Z_DEFAULT_COMPRESSION,
			memLevel: COMPRESSION_CONFIG.GZIP_MEM_LEVEL
		});
		fileStream = fileStream.pipe(gunzip);
	}

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
		objectMode: true, highWaterMark: jobConfig.highWater, transform: (chunk, _encoding, callback) => {
			// const { distinct_id = "", $insert_id = "", time = 0, event, ...props } = chunk;
			// const mixpanelEvent = {
			// 	event,
			// 	properties: {
			// 		distinct_id,
			// 		$insert_id,
			// 		time: dayjs.utc(time).valueOf(),
			// 		...props
			// 	}
			// };
			// if (!distinct_id) delete mixpanelEvent.properties.distinct_id;
			// if (!$insert_id) delete mixpanelEvent.properties.$insert_id;
			// if (!time) delete mixpanelEvent.properties.time;
			// callback(null, mixpanelEvent);
			callback(null, chunk);
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
 * @param  {import('../index').Job} jobConfig
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
				// Use cached size if available from pipeline optimization
				const cachedSize = jobConfig.bytesCache && jobConfig.bytesCache.get(item);
				const itemSize = cachedSize || Buffer.byteLength(JSON.stringify(item), 'utf-8');

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
						// Use cached size if available from pipeline optimization
						const cachedSize = jobConfig.bytesCache && jobConfig.bytesCache.get(item);
						const itemSize = cachedSize || Buffer.byteLength(JSON.stringify(item), 'utf-8');

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
 * a general purpose function that can fetch data 
 * @param {string} filePath  Path to either:
 *   • a JSON file containing an array of {id, distinct_id} objects, OR
 *   • an NDJSON file (one JSON object per line)
 * @param {string} keyOne    The key for the id (e.g., "person_id")
 * @param {string} keyTwo    The key for the distinct_id (e.g., "distinct_id")
 * @param {object} [job={}]  Job object containing GCS credentials and project info
 * @returns {Promise<Map<string,string>>}  Maps item.id → item.distinct_id
 */
async function buildMapFromPath(filePath, keyOne, keyTwo, job = {}) {
	if (!keyOne || !keyTwo || !filePath) throw new Error("keyOne and keyTwo are required");

	// Local file validation only
	if (!filePath.startsWith('gs://') && !filePath.startsWith('s3://')) {
		if (!fs.existsSync(path.resolve(filePath))) {
			throw new Error(`buildMapFromPath: File not found: ${filePath}`);
		}
	}

	//check if file has a valid extension
	const validExtensions = ['.json', '.jsonl', '.ndjson']; //todo: add csv + parquet
	const fileExtension = path.extname(filePath);
	if (!validExtensions.includes(fileExtension)) {
		throw new Error(`buildMapFromPath: Invalid file extension: ${fileExtension}`);
	}

	let fileContents;

	//a gcp bucket
	if (filePath?.startsWith('gs://')) fileContents = await fetchFromGCS(filePath, job.gcpProjectId || 'mixpanel-gtm-training', job.gcsCredentials);
	//an s3 bucket
	else if (filePath?.startsWith('s3://')) fileContents = await fetchFromS3(filePath);
	//a local file
	else {
		fileContents = fs.readFileSync(path.resolve(filePath), "utf-8");
	}

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

	// Build the Map<keyOne, keyTwo>
	const idMap = records.reduce((map, item) => {
		if (item[keyOne] == null || item[keyTwo] == null) {
			// you can choose to warn or skip silently
			return map;
		}
		map.set(item[keyOne], item[keyTwo]);
		return map;
	}, new Map());

	// clear file contents so we don't keep it in memory
	fileContents = null;
	return idMap;
}

/**
 * Fetch a file from Google Cloud Storage
 * @param {string} gcsPath Path in format gs://bucket-name/path/to/file.json
 * @returns {Promise<string>} File contents as a string
 */
async function fetchFromGCS(gcsPath, projectId = 'mixpanel-gtm-training', gcsCredentials = '') {


	// Create a storage client using either custom credentials or application default credentials
	const storageConfig = {
		projectId
	};

	// Use custom credentials if provided, otherwise fall back to ADC
	if (gcsCredentials) {
		storageConfig.keyFilename = gcsCredentials;
	}

	const storage = new Storage(storageConfig);

	// Extract bucket and file path from the GCS path
	// gs://bucket-name/path/to/file.json -> bucket="bucket-name", filePath="path/to/file.json"
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];

	try {
		// Download the file
		const [contents] = await storage.bucket(bucketName).file(filePath).download();

		// Convert Buffer to string
		return contents.toString('utf-8');
	} catch (error) {
		throw new Error(`Error fetching from GCS: ${error.message}`);
	}
}

/**
 * Create a high-performance streaming reader for Google Cloud Storage files
 * Supports both compressed (gzip) and uncompressed NDJSON files
 * @param {string} gcsPath Path in format gs://bucket-name/path/to/file.json
 * @param {JobConfig} job Job configuration for optimization
 * @returns {Promise<Readable>} Object-mode readable stream
 */
/**
 * Detect file format from GCS path
 * @param {string} gcsPath 
 * @returns {string} Format: 'json', 'csv', 'parquet'
 */
function detectGCSFormat(gcsPath) {
	const fileName = gcsPath.split('/').pop() || '';

	if (fileName.endsWith('.parquet.gz') || fileName.endsWith('.parquet')) return 'parquet';
	if (fileName.endsWith('.csv.gz') || fileName.endsWith('.csv')) return 'csv';
	if (fileName.endsWith('.json.gz') || fileName.endsWith('.json') || fileName.endsWith('.jsonl') || fileName.endsWith('.jsonl.gz')) return 'json';

	// Default to JSON for unknown formats
	return 'json';
}

/**
 * Main GCS stream factory - detects format and routes to appropriate parser
 * @param {string} gcsPath 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createGCSStream(gcsPath, job) {
	const format = detectGCSFormat(gcsPath);

	switch (format) {
		case 'csv':
			return createGCSCSVStream(gcsPath, job);
		case 'parquet':
			return createGCSParquetStream(gcsPath, job);
		case 'json':
		default:
			return createGCSJSONStream(gcsPath, job);
	}
}

/**
 * Create JSON/JSONL stream from GCS (original implementation)
 * @param {string} gcsPath 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createGCSJSONStream(gcsPath, job) {
	// Create a storage client using either custom credentials or application default credentials
	const storageConfig = {
		projectId: job.gcpProjectId
	};

	// Use custom credentials if provided, otherwise fall back to ADC
	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}

	const storage = new Storage(storageConfig);

	// Extract bucket and file path from the GCS path
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];
	const isGzipped = COMPRESSION_CONFIG.GZIP_EXTENSIONS.some(ext => filePath.endsWith(ext));

	try {
		// Check if file exists
		const gcsFile = storage.bucket(bucketName).file(filePath);
		const [exists] = await gcsFile.exists();
		if (!exists) {
			throw new Error(`File not found: ${gcsPath}`);
		}

		// Create read stream with tunable settings for high throughput
		const gcsReadStream = gcsFile.createReadStream({
			// Use configurable compression and validation settings
			decompress: GCS_STREAMING_CONFIG.DECOMPRESS,
			validation: !GCS_STREAMING_CONFIG.DISABLE_VALIDATION
		});

		// Create transform pipeline based on compression
		let pipeline = gcsReadStream;

		// Handle gzip compression with tunable parameters
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: GCS_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: GCS_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: GCS_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});
			pipeline = pipeline.pipe(gunzip);
		}

		// Convert to NDJSON object stream with tunable performance
		return pipeline.pipe(new JsonlObjectStream(job));

	} catch (error) {
		throw new Error(`Error creating GCS JSON stream: ${error.message}`);
	}
}

/**
 * Create CSV stream from GCS
 * @param {string} gcsPath 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createGCSCSVStream(gcsPath, job) {
	// Create a storage client using either custom credentials or application default credentials
	const storageConfig = {
		projectId: job.gcpProjectId
	};

	// Use custom credentials if provided, otherwise fall back to ADC
	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}

	const storage = new Storage(storageConfig);

	// Extract bucket and file path from the GCS path
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];
	const isGzipped = filePath.endsWith('.csv.gz');

	try {
		// Check if file exists
		const gcsFile = storage.bucket(bucketName).file(filePath);
		const [exists] = await gcsFile.exists();
		if (!exists) {
			throw new Error(`File not found: ${gcsPath}`);
		}

		// Create read stream
		let gcsReadStream = gcsFile.createReadStream({
			decompress: GCS_STREAMING_CONFIG.DECOMPRESS,
			validation: !GCS_STREAMING_CONFIG.DISABLE_VALIDATION
		});

		// Handle gzip compression
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: GCS_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: GCS_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: GCS_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});
			gcsReadStream = gcsReadStream.pipe(gunzip);
		}

		// Parse CSV using Papa Parse
		const mappings = Object.entries(job.aliases);
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

		// Transform to Mixpanel event format (same as local csvStreamer)
		const transformer = new stream.Transform({
			objectMode: true,
			highWaterMark: job.highWater,
			transform: (chunk, _encoding, callback) => {
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

		// Pipe: GCS Stream -> CSV Parser -> Transform -> Output
		return gcsReadStream.pipe(csvParser).pipe(transformer);

	} catch (error) {
		throw new Error(`Error creating GCS CSV stream: ${error.message}`);
	}
}

/**
 * Create Parquet stream from GCS using hyparquet with native GCS streaming
 * @param {string} gcsPath 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createGCSParquetStream(gcsPath, job) {
	// Create a storage client using either custom credentials or application default credentials
	const storageConfig = {
		projectId: job.gcpProjectId
	};

	// Use custom credentials if provided, otherwise fall back to ADC
	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}

	const storage = new Storage(storageConfig);

	// Extract bucket and file path from the GCS path
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];
	const isGzipped = filePath.endsWith('.parquet.gz');

	try {
		// Check if file exists
		const gcsFile = storage.bucket(bucketName).file(filePath);
		const [exists] = await gcsFile.exists();
		if (!exists) {
			throw new Error(`File not found: ${gcsPath}`);
		}

		// Create GCS read stream
		let gcsReadStream = gcsFile.createReadStream({
			decompress: GCS_STREAMING_CONFIG.DECOMPRESS,
			validation: !GCS_STREAMING_CONFIG.DISABLE_VALIDATION
		});

		// Handle gzip decompression for .parquet.gz files
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: GCS_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: GCS_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: GCS_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});
			gcsReadStream = gcsReadStream.pipe(gunzip);
		}

		// Collect stream data into buffer for hyparquet
		const chunks = [];
		for await (const chunk of gcsReadStream) {
			chunks.push(chunk);
		}
		const buffer = Buffer.concat(chunks);

		// Create async buffer interface for hyparquet (convert Buffer to ArrayBuffer)
		const asyncBuffer = {
			byteLength: buffer.length,
			slice: (start, end) => {
				const subBuffer = buffer.subarray(start, end);
				// Convert Buffer to ArrayBuffer
				const arrayBuffer = subBuffer.buffer.slice(
					subBuffer.byteOffset,
					subBuffer.byteOffset + subBuffer.byteLength
				);
				return Promise.resolve(arrayBuffer);
			}
		};

		// Use hyparquet's streaming interface (dynamically loaded)
		const parquetReadFn = await getParquetRead();

		// Read parquet data first, then create stream
		const parquetData = await new Promise((resolve, reject) => {
			parquetReadFn({
				file: asyncBuffer,
				rowFormat: 'object',
				onComplete: (data) => {
					// Sanitize each row in-place before resolving
					if (data && data.length > 0) {
						for (const row of data) {
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
						}
					}
					resolve(data || []);
				}
			}).catch(reject);
		});

		// Create readable stream from the parsed data
		return stream.Readable.from(parquetData, {
			objectMode: true,
			highWaterMark: job.highWater
		});

	} catch (error) {
		throw new Error(`Error creating GCS Parquet stream: ${error.message}`);
	}
}

/**
 * High-performance JSONL to Object transform stream
 * Optimized for processing large GCS files quickly
 */
class JsonlObjectStream extends Transform {
	constructor(job, options = {}) {
		super({
			objectMode: true,
			highWaterMark: job.highWater * GCS_STREAMING_CONFIG.OBJECT_STREAM_MULTIPLIER,
			...options
		});
		this.buffer = '';
		this.lineCount = 0;
		this.parseErrors = 0;
		this.maxParseErrors = JSON_CONFIG.PARSE_ERROR_LIMIT;
	}

	_transform(chunk, _encoding, callback) {
		// Add chunk to buffer
		this.buffer += chunk.toString();

		// Process complete lines
		const lines = this.buffer.split('\n');

		// Keep the last incomplete line in buffer
		this.buffer = lines.pop() || '';

		// Process each complete line
		for (const line of lines) {
			if (line.trim()) {
				try {
					const obj = JSON.parse(line.trim());
					this.push(obj);
					this.lineCount++;
				} catch (error) {
					this.parseErrors++;
					// Emit error but don't stop processing for better resilience
					if (this.parseErrors < this.maxParseErrors) {
						this.emit('warning', `JSON parse error on line ${this.lineCount + 1}: ${error.message}`);
					}
				}
			}
		}

		callback();
	}

	_flush(callback) {
		// Process any remaining data in buffer
		if (this.buffer.trim()) {
			try {
				const obj = JSON.parse(this.buffer.trim());
				this.push(obj);
				this.lineCount++;
			} catch (error) {
				this.emit('warning', `JSON parse error on final line: ${error.message}`);
			}
		}

		// Emit statistics for monitoring
		this.emit('stats', {
			linesProcessed: this.lineCount,
			parseErrors: this.parseErrors
		});

		callback();
	}
}

/**
 * Create a stream that handles multiple GCS files
 * Gracefully skips files that don't exist
 * Validates that all files have the same format
 * @param {string[]} gcsPaths Array of GCS paths (gs://bucket/file)
 * @param {JobConfig} job Job configuration
 * @returns {Promise<PassThrough>} Combined object stream from all files
 */
async function createMultiGCSStream(gcsPaths, job) {
	const { PassThrough } = require('stream');

	// Validate that all files have the same format
	const formats = gcsPaths.map(detectGCSFormat);
	const uniqueFormats = [...new Set(formats)];

	if (uniqueFormats.length > 1) {
		throw new Error(`Mixed file formats not supported. Found formats: ${uniqueFormats.join(', ')}. All files must be the same format.`);
	}

	const format = uniqueFormats[0];
	console.log(`Processing ${gcsPaths.length} ${format} files from GCS...`);

	// Create a passthrough stream that will be our final output
	const output = new PassThrough({ objectMode: true });

	// Process files sequentially to avoid overwhelming GCS
	let processedCount = 0;
	let skippedCount = 0;

	const processNextFile = async () => {
		if (processedCount + skippedCount >= gcsPaths.length) {
			// All files processed, end the stream
			output.end();
			return;
		}

		const gcsPath = gcsPaths[processedCount + skippedCount];

		try {
			// Check if file exists first
			// Create a storage client using either custom credentials or application default credentials
			const storageConfig = {
				projectId: job.gcpProjectId
			};

			// Use custom credentials if provided, otherwise fall back to ADC
			if (job.gcsCredentials) {
				storageConfig.keyFilename = job.gcsCredentials;
			}

			const storage = new Storage(storageConfig);

			const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
			if (!matches) {
				console.warn(`Skipping invalid GCS path: ${gcsPath}`);
				skippedCount++;
				setImmediate(processNextFile);
				return;
			}

			const bucketName = matches[1];
			const filePath = matches[2];
			const file = storage.bucket(bucketName).file(filePath);

			// Check if file exists
			const [exists] = await file.exists();
			if (!exists) {
				console.warn(`Skipping non-existent file: ${gcsPath}`);
				skippedCount++;
				setImmediate(processNextFile);
				return;
			}

			// Create stream for this file
			const fileStream = await createGCSStream(gcsPath, job);

			// Pipe this file's data to our output stream
			fileStream.on('data', (data) => {
				output.write(data);
			});

			fileStream.on('end', () => {
				processedCount++;
				setImmediate(processNextFile);
			});

			fileStream.on('error', (error) => {
				console.warn(`Error reading file ${gcsPath}: ${error.message}`);
				skippedCount++;
				setImmediate(processNextFile);
			});

		} catch (error) {
			console.warn(`Error processing file ${gcsPath}: ${error.message}`);
			skippedCount++;
			setImmediate(processNextFile);
		}
	};

	// Start processing the first file
	setImmediate(processNextFile);

	return output;
}

/**
 * Detect file format from S3 path
 * @param {string} s3Path 
 * @returns {string} Format: 'json', 'csv', 'parquet'
 */
function detectS3Format(s3Path) {
	const fileName = s3Path.split('/').pop() || '';

	if (fileName.endsWith('.parquet.gz') || fileName.endsWith('.parquet')) return 'parquet';
	if (fileName.endsWith('.csv.gz') || fileName.endsWith('.csv')) return 'csv';
	if (fileName.endsWith('.json.gz') || fileName.endsWith('.json') || fileName.endsWith('.jsonl') || fileName.endsWith('.jsonl.gz')) return 'json';

	// Default to JSON for unknown formats
	return 'json';
}

/**
 * Main S3 stream factory - detects format and routes to appropriate parser
 * @param {string} s3Path 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createS3Stream(s3Path, job) {
	const format = detectS3Format(s3Path);

	switch (format) {
		case 'csv':
			return createS3CSVStream(s3Path, job);
		case 'parquet':
			return createS3ParquetStream(s3Path, job);
		case 'json':
		default:
			return createS3JSONStream(s3Path, job);
	}
}

/**
 * Create JSON/JSONL stream from S3
 * @param {string} s3Path 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createS3JSONStream(s3Path, job) {
	// Extract bucket and key from the S3 path
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];
	const isGzipped = COMPRESSION_CONFIG.GZIP_EXTENSIONS.some(ext => key.endsWith(ext));

	// Configure S3 client with credentials from job config
	const s3ClientConfig = {
		region: job.s3Region || S3_STREAMING_CONFIG.DEFAULT_REGION,
		requestTimeout: S3_STREAMING_CONFIG.REQUEST_TIMEOUT,
		maxAttempts: S3_STREAMING_CONFIG.MAX_RETRY_ATTEMPTS
	};

	// Add credentials if provided
	if (job.s3Key && job.s3Secret) {
		s3ClientConfig.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}

	// Throw error if no region specified
	if (!job.s3Region) {
		throw new Error('S3 region is required. Please specify s3Region in job config or use environment variable S3_REGION');
	}

	const s3Client = new S3Client(s3ClientConfig);

	try {
		// Create read stream
		const command = new GetObjectCommand({
			Bucket: bucketName,
			Key: key
		});

		const response = await s3Client.send(command);

		// Convert AWS SDK stream to Node.js stream
		// @ts-ignore - AWS SDK stream conversion
		const s3Stream = stream.Readable.from(response.Body);

		// Handle gzip compression with tunable parameters
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: S3_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: S3_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: S3_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});
			return s3Stream.pipe(gunzip).pipe(new JsonlObjectStream(job));
		}

		// Convert to NDJSON object stream with tunable performance
		return s3Stream.pipe(new JsonlObjectStream(job));

	} catch (error) {
		throw new Error(`Error creating S3 JSON stream: ${error.message}`);
	}
}

/**
 * Create CSV stream from S3
 * @param {string} s3Path 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createS3CSVStream(s3Path, job) {
	// Extract bucket and key from the S3 path
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];
	const isGzipped = key.endsWith('.csv.gz');

	// Configure S3 client with credentials from job config
	const s3ClientConfig = {
		region: job.s3Region || S3_STREAMING_CONFIG.DEFAULT_REGION,
		requestTimeout: S3_STREAMING_CONFIG.REQUEST_TIMEOUT,
		maxAttempts: S3_STREAMING_CONFIG.MAX_RETRY_ATTEMPTS
	};

	// Add credentials if provided
	if (job.s3Key && job.s3Secret) {
		s3ClientConfig.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}

	// Throw error if no region specified
	if (!job.s3Region) {
		throw new Error('S3 region is required. Please specify s3Region in job config or use environment variable S3_REGION');
	}

	const s3Client = new S3Client(s3ClientConfig);

	try {
		// Create read stream
		const command = new GetObjectCommand({
			Bucket: bucketName,
			Key: key
		});

		const response = await s3Client.send(command);

		// Convert AWS SDK stream to Node.js stream
		// @ts-ignore - AWS SDK stream conversion
		const s3Stream = stream.Readable.from(response.Body);

		// Handle gzip compression
		let processedStream = s3Stream;
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: S3_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: S3_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: S3_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});
			processedStream = s3Stream.pipe(gunzip);
		}

		// Parse CSV using Papa Parse
		const mappings = Object.entries(job.aliases);
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

		// Transform to Mixpanel event format (same as local csvStreamer)
		const transformer = new stream.Transform({
			objectMode: true,
			highWaterMark: job.highWater,
			transform: (chunk, _encoding, callback) => {
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

		// Pipe: S3 Stream -> CSV Parser -> Transform -> Output
		return processedStream.pipe(csvParser).pipe(transformer);

	} catch (error) {
		throw new Error(`Error creating S3 CSV stream: ${error.message}`);
	}
}

/**
 * Create Parquet stream from S3 using hyparquet with native S3 streaming
 * @param {string} s3Path 
 * @param {JobConfig} job 
 * @returns {Promise<stream.Readable>}
 */
async function createS3ParquetStream(s3Path, job) {
	// Extract bucket and key from the S3 path
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];
	const isGzipped = key.endsWith('.parquet.gz');

	// Configure S3 client with credentials from job config
	const s3ClientConfig = {
		region: job.s3Region || S3_STREAMING_CONFIG.DEFAULT_REGION,
		requestTimeout: S3_STREAMING_CONFIG.REQUEST_TIMEOUT,
		maxAttempts: S3_STREAMING_CONFIG.MAX_RETRY_ATTEMPTS
	};

	// Add credentials if provided
	if (job.s3Key && job.s3Secret) {
		s3ClientConfig.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}

	// Throw error if no region specified
	if (!job.s3Region) {
		throw new Error('S3 region is required. Please specify s3Region in job config or use environment variable S3_REGION');
	}

	const s3Client = new S3Client(s3ClientConfig);

	try {
		// Create S3 read stream
		const command = new GetObjectCommand({
			Bucket: bucketName,
			Key: key
		});

		const response = await s3Client.send(command);

		// Convert AWS SDK stream to Node.js stream and collect chunks
		const chunks = [];

		// Handle gzip decompression for .parquet.gz files
		if (isGzipped) {
			const gunzip = zlib.createGunzip({
				chunkSize: S3_STREAMING_CONFIG.GZIP_CHUNK_SIZE,
				windowBits: S3_STREAMING_CONFIG.GZIP_WINDOW_BITS,
				level: zlib.constants.Z_DEFAULT_COMPRESSION,
				memLevel: S3_STREAMING_CONFIG.GZIP_MEM_LEVEL
			});

			// Convert AWS SDK stream to Node.js stream and pipe through gunzip
			// @ts-ignore - AWS SDK stream conversion
			const s3Stream = stream.Readable.from(response.Body);
			const decompressedStream = s3Stream.pipe(gunzip);

			for await (const chunk of decompressedStream) {
				chunks.push(chunk);
			}
		} else {
			// Direct processing without compression
			// @ts-ignore - AWS SDK stream iteration
			for await (const chunk of response.Body) {
				chunks.push(chunk);
			}
		}
		const buffer = Buffer.concat(chunks);

		// Create async buffer interface for hyparquet (convert Buffer to ArrayBuffer)
		const asyncBuffer = {
			byteLength: buffer.length,
			slice: (start, end) => {
				const subBuffer = buffer.subarray(start, end);
				// Convert Buffer to ArrayBuffer
				const arrayBuffer = subBuffer.buffer.slice(
					subBuffer.byteOffset,
					subBuffer.byteOffset + subBuffer.byteLength
				);
				return Promise.resolve(arrayBuffer);
			}
		};

		// Use hyparquet's streaming interface (dynamically loaded)
		const parquetReadFn = await getParquetRead();

		// Read parquet data first, then create stream
		const parquetData = await new Promise((resolve, reject) => {
			parquetReadFn({
				file: asyncBuffer,
				rowFormat: 'object',
				onComplete: (data) => {
					// Sanitize each row in-place before resolving
					if (data && data.length > 0) {
						for (const row of data) {
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
						}
					}
					resolve(data || []);
				}
			}).catch(reject);
		});

		// Create readable stream from the parsed data
		return stream.Readable.from(parquetData, {
			objectMode: true,
			highWaterMark: job.highWater
		});

	} catch (error) {
		throw new Error(`Error creating S3 Parquet stream: ${error.message}`);
	}
}

/**
 * Create a stream that handles multiple S3 files
 * Gracefully skips files that don't exist
 * Validates that all files have the same format
 * @param {string[]} s3Paths Array of S3 paths (s3://bucket/file)
 * @param {JobConfig} job Job configuration
 * @returns {Promise<PassThrough>} Combined object stream from all files
 */
async function createMultiS3Stream(s3Paths, job) {
	const { PassThrough } = require('stream');

	// Validate that all files have the same format
	const formats = s3Paths.map(detectS3Format);
	const uniqueFormats = [...new Set(formats)];

	if (uniqueFormats.length > 1) {
		throw new Error(`Mixed file formats not supported. Found formats: ${uniqueFormats.join(', ')}. All files must be the same format.`);
	}

	const format = uniqueFormats[0];
	console.log(`Processing ${s3Paths.length} ${format} files from S3...`);

	// Create a passthrough stream that will be our final output
	const output = new PassThrough({ objectMode: true });

	// Configure S3 client with credentials from job config
	const s3ClientConfig = {
		region: job.s3Region || S3_STREAMING_CONFIG.DEFAULT_REGION,
		requestTimeout: S3_STREAMING_CONFIG.REQUEST_TIMEOUT,
		maxAttempts: S3_STREAMING_CONFIG.MAX_RETRY_ATTEMPTS
	};

	// Add credentials if provided
	if (job.s3Key && job.s3Secret) {
		s3ClientConfig.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}

	// Throw error if no region specified
	if (!job.s3Region) {
		throw new Error('S3 region is required. Please specify s3Region in job config or use environment variable S3_REGION');
	}

	const s3Client = new S3Client(s3ClientConfig);

	// Process files sequentially to avoid overwhelming S3
	let processedCount = 0;
	let skippedCount = 0;

	const processNextFile = async () => {
		if (processedCount + skippedCount >= s3Paths.length) {
			// All files processed, end the stream
			output.end();
			return;
		}

		const s3Path = s3Paths[processedCount + skippedCount];

		try {
			// Check if file exists first
			const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
			if (!matches) {
				console.warn(`Skipping invalid S3 path: ${s3Path}`);
				skippedCount++;
				setImmediate(processNextFile);
				return;
			}

			const bucketName = matches[1];
			const key = matches[2];

			// Check if file exists with a head request
			const headCommand = new GetObjectCommand({
				Bucket: bucketName,
				Key: key
			});

			try {
				await s3Client.send(headCommand);
			} catch (error) {
				console.warn(`Skipping non-existent file: ${s3Path}`);
				skippedCount++;
				setImmediate(processNextFile);
				return;
			}

			// Create stream for this file
			const fileStream = await createS3Stream(s3Path, job);

			// Pipe this file's data to our output stream
			fileStream.on('data', (data) => {
				output.write(data);
			});

			fileStream.on('end', () => {
				processedCount++;
				setImmediate(processNextFile);
			});

			fileStream.on('error', (error) => {
				console.warn(`Error reading file ${s3Path}: ${error.message}`);
				skippedCount++;
				setImmediate(processNextFile);
			});

		} catch (error) {
			console.warn(`Error processing file ${s3Path}: ${error.message}`);
			skippedCount++;
			setImmediate(processNextFile);
		}
	};

	// Start processing the first file
	setImmediate(processNextFile);

	return output;
}

/**
 * Test if we can write to a GCS path by creating and deleting a small test file
 * @param {string} gcsPath Path in format gs://bucket-name/path/to/file.json
 * @param {string} projectId GCP Project ID
 * @param {string} gcsCredentials Path to GCS credentials file
 * @returns {Promise<boolean>} True if writable, throws error if not
 */
async function testGCSWriteAccess(gcsPath, projectId = 'mixpanel-gtm-training', gcsCredentials = '') {
	// Create a storage client using either custom credentials or application default credentials
	const storageConfig = {
		projectId
	};

	// Use custom credentials if provided, otherwise fall back to ADC
	if (gcsCredentials) {
		storageConfig.keyFilename = gcsCredentials;
	}

	const storage = new Storage(storageConfig);

	// Extract bucket and file path from the GCS path
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const filePath = matches[2];
	
	// Create test file path in same directory
	const testFileName = `${path.dirname(filePath)}/.write-test-${Date.now()}-${Math.random().toString(36).substring(2)}`;

	try {
		const bucket = storage.bucket(bucketName);
		const testFile = bucket.file(testFileName);
		
		// Try to create a small test file
		await testFile.save('test-write-access', {
			metadata: {
				contentType: 'text/plain'
			}
		});
		
		// Clean up test file
		await testFile.delete();
		
		return true;
	} catch (error) {
		throw new Error(`Cannot write to GCS path ${gcsPath}: ${error.message}`);
	}
}

/**
 * Test if we can write to an S3 path by creating and deleting a small test file
 * @param {string} s3Path Path in format s3://bucket-name/path/to/file.json
 * @param {object} s3Config S3 configuration with region, credentials
 * @returns {Promise<boolean>} True if writable, throws error if not
 */
async function testS3WriteAccess(s3Path, s3Config = {}) {
	// Extract bucket and key from the S3 path
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];
	
	// Create test key in same directory
	const testKey = `${path.dirname(key)}/.write-test-${Date.now()}-${Math.random().toString(36).substring(2)}`;

	// Configure S3 client
	const s3ClientConfig = {
		region: s3Config.s3Region || S3_STREAMING_CONFIG.DEFAULT_REGION,
		...s3Config
	};

	const s3Client = new S3Client(s3ClientConfig);

	try {
		// Try to create a small test file
		await s3Client.send(new PutObjectCommand({
			Bucket: bucketName,
			Key: testKey,
			Body: 'test-write-access',
			ContentType: 'text/plain'
		}));
		
		// Clean up test file
		await s3Client.send(new DeleteObjectCommand({
			Bucket: bucketName,
			Key: testKey
		}));
		
		return true;
	} catch (error) {
		throw new Error(`Cannot write to S3 path ${s3Path}: ${error.message}`);
	}
}

/**
 * Validate that a cloud path is writable before starting export
 * @param {string} cloudPath GCS or S3 path
 * @param {object} config Configuration with credentials
 * @returns {Promise<boolean>} True if writable, throws error if not
 */
async function validateCloudWriteAccess(cloudPath, config = {}) {
	if (cloudPath.startsWith('gs://')) {
		return testGCSWriteAccess(cloudPath, config.gcpProjectId, config.gcsCredentials);
	} else if (cloudPath.startsWith('s3://')) {
		return testS3WriteAccess(cloudPath, {
			s3Region: config.s3Region,
			credentials: config.s3Key && config.s3Secret ? {
				accessKeyId: config.s3Key,
				secretAccessKey: config.s3Secret
			} : undefined
		});
	} else {
		throw new Error(`Unsupported cloud path format: ${cloudPath}. Use gs:// for Google Cloud Storage or s3:// for Amazon S3`);
	}
}

/**
 * Fetch a file from Amazon S3
 * @param {string} s3Path Path in format s3://bucket-name/path/to/file.json
 * @returns {Promise<string>} File contents as a string
 */
async function fetchFromS3(s3Path) {


	// Create an S3 client using application default credentials
	const s3Client = new S3Client({});

	// Extract bucket and key from the S3 path
	// s3://bucket-name/path/to/file.json -> bucket="bucket-name", key="path/to/file.json"
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];

	try {
		// Configure the GetObject operation
		const command = new GetObjectCommand({
			Bucket: bucketName,
			Key: key
		});

		// Download the file
		const response = await s3Client.send(command);

		// AWS SDK v3 returns a readable byte stream
		// Convert it directly to a string
		let content = '';
		// @ts-ignore
		for await (const chunk of response.Body) {
			content += chunk.toString('utf-8');
		}
		return content;
	} catch (error) {
		throw new Error(`Error fetching from S3: ${error.message}`);
	}
}

module.exports = {
	buildMapFromPath,
	JsonlParser,
	determineDataType,
	existingStreamInterface,
	StreamArray,
	itemStream,
	getEnvVars,
	chunkForSize,
	analyzeFileFormat,
	validateCloudWriteAccess
};
