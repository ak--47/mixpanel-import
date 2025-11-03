/**
 * Destination Writer for outputting transformed data to files or cloud storage
 * Supports local files, Google Cloud Storage (gs://), and Amazon S3 (s3://)
 */

const { Transform, Writable } = require('stream');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

/**
 * Creates a writable stream for the specified destination
 * @param {string} destination - Path to write (local file, gs://, or s3://)
 * @param {object} job - Job configuration with cloud credentials
 * @returns {Promise<stream.Writable>} Writable stream for the destination
 */
async function createDestinationStream(destination, job) {
	if (!destination) {
		throw new Error('Destination path is required');
	}

	// Handle auto-generated filenames for directories
	let finalDestination = destination;

	// Check if destination is a local directory (not a cloud path)
	if (!destination.startsWith('gs://') && !destination.startsWith('s3://')) {
		// Check if it's a directory
		if (fs.existsSync(destination) && fs.lstatSync(destination).isDirectory()) {
			// Generate filename: {recordType}-{dateTime}.ndjson
			const dayjs = require('dayjs');
			const utc = require('dayjs/plugin/utc');
			dayjs.extend(utc);

			const timestamp = dayjs.utc().format('YYYY-MM-DDTHH-mm-ss-SSS[Z]');
			const filename = `${job.recordType}-${timestamp}.ndjson`;
			finalDestination = path.join(destination, filename);

			if (job.verbose) {
				console.log(`📝 Auto-generated filename: ${filename}`);
			}
		}
	}

	// Google Cloud Storage
	if (finalDestination.startsWith('gs://')) {
		return createGCSDestinationStream(finalDestination, job);
	}

	// Amazon S3
	if (finalDestination.startsWith('s3://')) {
		return createS3DestinationStream(finalDestination, job);
	}

	// Local file
	return createLocalDestinationStream(finalDestination, job);
}

/**
 * Creates a local file writable stream
 * @param {string} filePath - Local file path
 * @param {object} job - Job configuration
 * @returns {stream.Writable} Writable stream for local file
 */
function createLocalDestinationStream(filePath, job) {
	// Ensure directory exists
	const dir = path.dirname(filePath);
	if (!fs.existsSync(dir)) {
		fs.mkdirSync(dir, { recursive: true });
	}

	// Create write stream
	let writeStream = fs.createWriteStream(filePath, {
		flags: 'w',
		encoding: 'utf8',
		highWaterMark: 64 * 1024 // 64KB buffer
	});

	// Add gzip compression if file ends with .gz
	if (filePath.endsWith('.gz')) {
		const gzip = zlib.createGzip({
			level: 6, // Balanced compression
		});
		gzip.pipe(writeStream);
		writeStream = gzip;
	}

	// Create a transform stream that converts objects to NDJSON
	const jsonLineWriter = new Transform({
		objectMode: true,
		highWaterMark: job.highWater || 16,
		transform(chunk, encoding, callback) {
			try {
				// Convert object to JSON line
				const line = JSON.stringify(chunk) + '\n';
				callback(null, line);
			} catch (error) {
				callback(error);
			}
		}
	});

	// Pipe through JSON line writer to file stream
	jsonLineWriter.pipe(writeStream);

	// Log when destination is ready
	if (job.verbose) {
		console.log(`📝 Destination stream created: ${filePath}`);
	}

	// Return the transform stream that accepts objects
	return jsonLineWriter;
}

/**
 * Creates a Google Cloud Storage writable stream
 * @param {string} gcsPath - GCS path (gs://bucket/path/to/file)
 * @param {object} job - Job configuration with GCS credentials
 * @returns {Promise<stream.Writable>} Writable stream for GCS
 */
async function createGCSDestinationStream(gcsPath, job) {
	const { Storage } = require('@google-cloud/storage');

	// Parse GCS path
	const matches = gcsPath.match(/^gs:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid GCS path: ${gcsPath}`);
	}

	const bucketName = matches[1];
	const fileName = matches[2];

	// Create storage client
	const storageConfig = {
		projectId: job.gcpProjectId
	};
	if (job.gcsCredentials) {
		storageConfig.keyFilename = job.gcsCredentials;
	}
	const storage = new Storage(storageConfig);

	// Create write stream
	const file = storage.bucket(bucketName).file(fileName);
	let gcsStream = file.createWriteStream({
		resumable: false, // Simpler for streaming
		metadata: {
			contentType: fileName.endsWith('.gz') ? 'application/gzip' : 'application/x-ndjson'
		}
	});

	// Add gzip compression if needed
	if (fileName.endsWith('.gz')) {
		const gzip = zlib.createGzip({
			level: 6, // Balanced compression
		});
		gzip.pipe(gcsStream);
		gcsStream = gzip;
	}

	// Create a transform stream that converts objects to NDJSON
	const jsonLineWriter = new Transform({
		objectMode: true,
		highWaterMark: job.highWater || 16,
		transform(chunk, encoding, callback) {
			try {
				// Convert object to JSON line
				const line = JSON.stringify(chunk) + '\n';
				callback(null, line);
			} catch (error) {
				callback(error);
			}
		}
	});

	// Pipe through JSON line writer to GCS stream
	jsonLineWriter.pipe(gcsStream);

	// Handle GCS stream events
	gcsStream.on('error', (error) => {
		console.error(`❌ GCS write error: ${error.message}`);
		jsonLineWriter.destroy(error);
	});

	gcsStream.on('finish', () => {
		if (job.verbose) {
			console.log(`✅ Successfully wrote to GCS: ${gcsPath}`);
		}
	});

	// Log when destination is ready
	if (job.verbose) {
		console.log(`☁️ GCS destination stream created: ${gcsPath}`);
	}

	// Return the transform stream that accepts objects
	return jsonLineWriter;
}

/**
 * Creates an Amazon S3 writable stream
 * @param {string} s3Path - S3 path (s3://bucket/path/to/file)
 * @param {object} job - Job configuration with S3 credentials
 * @returns {Promise<stream.Writable>} Writable stream for S3
 */
async function createS3DestinationStream(s3Path, job) {
	const { S3Client } = require('@aws-sdk/client-s3');
	const { Upload } = require('@aws-sdk/lib-storage');
	const { PassThrough } = require('stream');

	// Parse S3 path
	const matches = s3Path.match(/^s3:\/\/([^\/]+)\/(.+)$/);
	if (!matches) {
		throw new Error(`Invalid S3 path: ${s3Path}`);
	}

	const bucketName = matches[1];
	const key = matches[2];

	// Configure S3 client
	const s3Config = {
		region: job.s3Region || 'us-east-1'
	};
	if (job.s3Key && job.s3Secret) {
		s3Config.credentials = {
			accessKeyId: job.s3Key,
			secretAccessKey: job.s3Secret
		};
	}
	const s3Client = new S3Client(s3Config);

	// Create a PassThrough stream for S3 upload
	let uploadStream = new PassThrough();

	// Add gzip compression if needed
	if (key.endsWith('.gz')) {
		const gzip = zlib.createGzip({
			level: 6, // Balanced compression
		});
		gzip.pipe(uploadStream);
		uploadStream = gzip;
	}

	// Create S3 upload
	const upload = new Upload({
		client: s3Client,
		params: {
			Bucket: bucketName,
			Key: key,
			Body: uploadStream,
			ContentType: key.endsWith('.gz') ? 'application/gzip' : 'application/x-ndjson'
		},
		queueSize: 4, // Concurrent parts
		partSize: 5 * 1024 * 1024, // 5MB parts
	});

	// Start the upload asynchronously
	upload.done().then(() => {
		if (job.verbose) {
			console.log(`✅ Successfully wrote to S3: ${s3Path}`);
		}
	}).catch(error => {
		console.error(`❌ S3 write error: ${error.message}`);
	});

	// Create a transform stream that converts objects to NDJSON
	const jsonLineWriter = new Transform({
		objectMode: true,
		highWaterMark: job.highWater || 16,
		transform(chunk, encoding, callback) {
			try {
				// Convert object to JSON line
				const line = JSON.stringify(chunk) + '\n';
				callback(null, line);
			} catch (error) {
				callback(error);
			}
		}
	});

	// Pipe through JSON line writer to upload stream
	jsonLineWriter.pipe(uploadStream);

	// Log when destination is ready
	if (job.verbose) {
		console.log(`☁️ S3 destination stream created: ${s3Path}`);
	}

	// Return the transform stream that accepts objects
	return jsonLineWriter;
}

/**
 * Creates a tee stream that duplicates data to both Mixpanel and a destination
 * @param {stream.Writable} destinationStream - Stream to write to destination
 * @returns {stream.Transform} Transform stream that passes data through and writes to destination
 */
function createTeeStream(destinationStream) {
	return new Transform({
		objectMode: true,
		highWaterMark: 16,
		transform(batch, encoding, callback) {
			// Handle both batches (arrays) and individual records
			const records = Array.isArray(batch) ? batch : [batch];

			// Write each record to destination (don't wait for it)
			for (const record of records) {
				destinationStream.write(record, (err) => {
					if (err && err.code !== 'ERR_STREAM_DESTROYED') {
						console.error('Destination write error:', err);
					}
				});
			}

			// Pass the batch through to next stage (Mixpanel)
			callback(null, batch);
		},
		flush(callback) {
			// End the destination stream when we're done
			if (destinationStream && typeof destinationStream.end === 'function') {
				destinationStream.end();
			}
			callback();
		}
	});
}

module.exports = {
	createDestinationStream,
	createTeeStream
};