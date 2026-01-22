const express = require("express");
const path = require("path");
const os = require("os");
const multer = require("multer");
const WebSocket = require("ws");
const { createServer } = require("http");
const mixpanelImport = require("../index.js");
const pino = require("pino");
const { createGcpLoggingPinoConfig } = require("@google-cloud/pino-logging-gcp-config");
const cookieParser = require('cookie-parser');
const { validateCloudWriteAccess } = require('../components/parsers.js');
const { GoogleAuth } = require('google-auth-library');

let { NODE_ENV = "" } = process.env;
if (!NODE_ENV) NODE_ENV = "local";
if (!NODE_ENV) throw new Error("NODE_ENV not set");

// Configure Pino logger
const logLevel = NODE_ENV === "production" ? "info" : NODE_ENV === "test" ? "warn" : "debug";

let logger;
if (NODE_ENV === "production") {
	// Use GCP structured logging in production
	logger = pino(
		createGcpLoggingPinoConfig(
			{
				serviceContext: {
					service: "mixpanel-import-ui",
					version: require("../package.json").version || "1.0.0"
				}
			},
			{
				level: logLevel
			}
		)
	);
} else {
	// Use pino-pretty in development (if available)
	try {
		require.resolve("pino-pretty");
		logger = pino({
			level: logLevel,
			transport: {
				target: "pino-pretty",
				options: {
					colorize: true,
					translateTime: "SYS:standard",
					ignore: "pid,hostname"
				}
			}
		});
	} catch (err) {
		// pino-pretty not available, use basic logger
		logger = pino({
			level: logLevel
		});
	}
}

const app = express();
const server = createServer(app);
const port = parseInt(process.env.PORT) || 3000;

// WebSocket server
const wss = new WebSocket.Server({ server });

// Job tracking (in-memory - not serverless-compatible)
// TODO: Replace with Redis/DynamoDB
const activeJobs = new Map(); // jobId -> { ws, startTime, lastUpdate }

// Job status tracking (in-memory)
const jobStatuses = new Map(); // jobId -> { status, progress, result, startTime, lastUpdate }

// Memory management
const MAX_HEAP_PERCENT = 85;
const JOB_CLEANUP_INTERVAL = 5 * 60 * 1000; // 5 minutes
const JOB_MAX_AGE = 60 * 60 * 1000; // 1 hour

// Snowcat service configuration
const SNOWCAT_URL = 'https://snowcat-queuer-lmozz6xkha-uc.a.run.app';
const auth = new GoogleAuth();
let cachedSnowcatClient = null;

/**
 * Gets an authenticated client for making requests to Snowcat service
 * Uses Application Default Credentials (works in Cloud Run)
 * @return {Promise<import('google-auth-library').IdTokenClient>}
 */
async function getSnowcatClient() {
	// Return cached client if available
	if (cachedSnowcatClient) {
		return cachedSnowcatClient;
	}

	// Create new authenticated client
	const maxRetries = 3;
	for (let i = 0; i < maxRetries; i++) {
		try {
			cachedSnowcatClient = await auth.getIdTokenClient(SNOWCAT_URL);
			logger.info("snowcat client created");
			return cachedSnowcatClient;
		} catch (error) {
			if (i === maxRetries - 1) {
				logger.error({ err: error }, "failed to create snowcat client");
				throw error;
			}
			// Wait 1 second before retry
			await new Promise(resolve => setTimeout(resolve, 1000));
		}
	}
	throw new Error("Could not get Snowcat client");
}

// Execute job over WebSocket
async function executeJobOverWebSocket(ws, jobId, credentials, options, cloudPaths, transformCode, jobLogger) {

	let data;
	let filesToCleanup = [];

	try {
		// Parse credentials and options
		const creds = JSON.parse(credentials);
		const opts = JSON.parse(options);

		// Force abridged mode in production
		if (NODE_ENV === "production" && opts.abridged === undefined) {
			opts.abridged = true;
			jobLogger.info("abridged mode forced (production)");
		}

		// Force manual GC in production
		if (NODE_ENV === "production" && opts.manualGc === undefined) {
			opts.manualGc = true;
			jobLogger.info("manual GC enabled (production)");
		}

		// Limit dry run results in production
		if (NODE_ENV === "production" && opts.dryRun && !opts.maxRecords) {
			opts.maxRecords = 100;
			jobLogger.info("dryRun maxRecords capped (production)");
		}

		// Log abridged mode status for debugging
		jobLogger.info({ abridged: opts.abridged, workers: opts.workers, manualGc: opts.manualGc }, "job options configured");

		// Add progress callback for WebSocket updates
		opts.progressCallback = createProgressCallback(jobId);

		// Handle transform function if provided
		if (transformCode && transformCode.trim()) {
			try {
				// Create transform function from code (eval to support arrow functions)
				opts.transformFunc = eval(`(${transformCode})`);
			} catch (transformError) {
				throw new Error(`Transform function error: ${transformError.message}`);
			}
		}

		// Determine data source: cloud paths OR local files from /job/prepare
		if (cloudPaths) {
			// Cloud storage mode - parse paths (no file upload needed!)
			data = JSON.parse(cloudPaths);
			jobLogger.debug({ cloudPaths: data }, "job w/cloud mode");
		} else {
			// Local file mode - get files from prepared job
			const jobStatus = jobStatuses.get(jobId);
			if (!jobStatus || !jobStatus.filePaths) {
				throw new Error("No files found for job - did you call /job/prepare first?");
			}

			data = jobStatus.filePaths;
			filesToCleanup = jobStatus.files || [];
			jobLogger.debug({ fileCount: Array.isArray(data) ? data.length : 1 }, "job w/local mode");
		}

		// Update status to running
		updateJobStatus(jobId, "running");

		// Run the import (this keeps the WebSocket connection active!)
		const result = await mixpanelImport(creds, data, opts);
		const { total, success, failed, empty } = result;
		jobLogger.info({ total, success, failed, empty }, "job complete");

		// Filter result for client
		const filteredResult = filterResultForClient(result);

		// Update job status
		updateJobStatus(jobId, "completed", null, filteredResult);

		// Send completion via WebSocket
		ws.send(JSON.stringify({
			type: "job-complete",
			jobId: jobId,
			result: filteredResult,
			timestamp: Date.now()
		}));

	} catch (jobError) {
		jobLogger.error({ err: jobError }, "job execution failed");

		// Update job status
		updateJobStatus(jobId, "failed", null, { error: jobError.message });

		// Send error via WebSocket
		ws.send(JSON.stringify({
			type: "job-error",
			jobId: jobId,
			error: jobError.message,
			timestamp: Date.now()
		}));

	} finally {
		jobLogger.debug("job execution finalized");
		// Clean up temporary files if any
		if (filesToCleanup.length > 0) {
			for (const file of filesToCleanup) {
				try {
					if (file.fieldname !== "gcsCredentials") {
						fs.unlinkSync(file.path);
						jobLogger.debug({ filePath: file.path }, "temp file cleaned");
					}
				} catch (cleanupError) {
					jobLogger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}

			// Clean up GCS credentials after delay
			setTimeout(() => {
				for (const file of filesToCleanup) {
					if (file.fieldname === "gcsCredentials") {
						try {
							fs.unlinkSync(file.path);
							jobLogger.debug({ filePath: file.path }, "gcs creds cleaned");
						} catch (cleanupError) {
							jobLogger.warn({ err: cleanupError, filePath: file.path }, "gcs creds cleanup failed");
						}
					}
				}
			}, 5000);
		}

		// Clean up job status
		activeJobs.delete(jobId);

		// Trigger GC if memory usage is high
		if (global.gc) {
			const memUsage = process.memoryUsage();
			const heapPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
			if (heapPercent > MAX_HEAP_PERCENT) {
				global.gc();
				jobLogger.debug({ heapPercent: heapPercent.toFixed(1) }, "gc triggered (high memory)");
			}
		}
	}
}

// WebSocket connection handler
wss.on("connection", ws => {
	// WebSocket connections are ephemeral - no logging needed

	ws.on("message", async message => {
		try {
			// @ts-ignore
			const data = JSON.parse(message);

			if (data.type === "register-job") {
				const jobId = data.jobId;
				activeJobs.set(jobId, {
					ws: ws,
					startTime: Date.now(),
					lastUpdate: Date.now()
				});
				logger.debug({ jobId }, "job created via websocket");

				// Send confirmation
				ws.send(
					JSON.stringify({
						type: "job-registered",
						jobId: jobId
					})
				);
			} else if (data.type === "start_job") {
				// Handle job execution over WebSocket (keeps connection alive!)
				const { jobId, credentials, options, cloudPaths, transformCode } = data;

				// Create child logger for correlation
				const jobLogger = logger.child({ jobId });
				jobLogger.info("job started via websocket");

				// Register this WebSocket for progress updates
				activeJobs.set(jobId, {
					ws: ws,
					startTime: Date.now(),
					lastUpdate: Date.now()
				});

				// Send acknowledgment
				ws.send(JSON.stringify({
					type: "job-started",
					jobId: jobId
				}));

				// Run the job asynchronously
				try {
					await executeJobOverWebSocket(ws, jobId, credentials, options, cloudPaths, transformCode, jobLogger);
					logger.debug({ jobId }, "job finished websocket");
				} catch (error) {
					jobLogger.error({ err: error }, "websocket job failed");
					ws.send(JSON.stringify({
						type: "job-error",
						jobId: jobId,
						error: error.message
					}));
				}
			}
		} catch (error) {
			logger.error({ err: error }, "websocket message error");
		}
	});

	ws.on("close", () => {
		// WebSocket disconnections are ephemeral - no logging needed
		// Clean up any jobs associated with this WebSocket
		for (const [jobId, jobData] of activeJobs.entries()) {
			if (jobData.ws === ws) {
				activeJobs.delete(jobId);
				logger.debug({ jobId }, "websocket cleaned up");
			}
		}
	});

	ws.on("error", error => {
		logger.error({ err: error }, "websocket error");
	});
});

// Function to update job status and optionally broadcast via WebSocket
function updateJobStatus(jobId, status, progressData = null, result = null) {
	const timestamp = Date.now();

	// Update persistent job status (survives WebSocket disconnections)
	// IMPORTANT: Don't store progressData in jobStatuses - it causes memory leaks!
	// Progress is ephemeral and only needs to be sent via WebSocket
	const currentStatus = jobStatuses.get(jobId) || {};
	const updatedStatus = {
		...currentStatus,
		status: status,
		lastUpdate: timestamp,
		startTime: currentStatus.startTime || timestamp
	};

	// Only store result if provided (completion/error)
	if (result) {
		updatedStatus.result = result;
	}

	jobStatuses.set(jobId, updatedStatus);

	// Broadcast progress/status via WebSocket if connection exists
	const jobData = activeJobs.get(jobId);
	if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
		try {
			const message = {
				type: status === "running" ? "progress" : "status-update",
				jobId: jobId,
				status: status,
				data: progressData,
				result: result,
				timestamp: timestamp
			};
			jobData.ws.send(JSON.stringify(message));
			jobData.lastUpdate = timestamp;
		} catch (error) {
			logger.error({ err: error, jobId }, "updateJobStatus websocket send error");
			// Clean up dead connection
			activeJobs.delete(jobId);
		}
	}
}

// Broadcast progress updates to WebSocket clients
function broadcastProgress(jobId, progressData) {
	updateJobStatus(jobId, "running", progressData);
}

// Function to filter result to only essential fields for client
function filterResultForClient(result) {
	const allowedFields = [
		'recordType', 'total', 'success', 'failed', 'empty', 'outOfBounds',
		'duplicates', 'startTime', 'endTime', 'durationHuman', 'bytesHuman',
		'requests', 'retries', 'rateLimit', 'wasStream', 'eps', 'rps', 'mbps',
		'badRecords', 'vendor', 'vendorOpts', 'errors', 'responses', 'files', 'downloadUrl'
	];

	const filtered = {};
	for (const key of allowedFields) {
		if (key in result) {
			filtered[key] = result[key];
		}
	}

	return filtered;
}

// Function to signal job completion
function signalJobComplete(jobId, result) {
	// Filter result to only essential fields
	const filteredResult = filterResultForClient(result);

	// Update job status to completed (persists beyond WebSocket)
	updateJobStatus(jobId, "completed", null, filteredResult);

	// Also send via WebSocket if available (optional enhancement)
	const jobData = activeJobs.get(jobId);
	if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
		try {
			jobData.ws.send(
				JSON.stringify({
					type: "job-complete",
					jobId: jobId,
					result: filteredResult,
					timestamp: Date.now()
				})
			);
		} catch (error) {
			logger.error({ err: error, jobId }, "signalJobComplete websocket send error");
		}
	}

	// Clean up WebSocket tracking (but keep job status for REST API)
	activeJobs.delete(jobId);
	logger.info({ jobId }, "job completed and cleaned up");
}

// Function to create a progress callback for a specific job
function createProgressCallback(jobId) {
	return (recordType, processed, requests, eps, bytesProcessed, downloadMessage) => {
		const progressData = {
			recordType: recordType || "",
			processed: processed || 0,
			requests: requests || 0,
			eps: eps || "",
			bytesProcessed: bytesProcessed || 0,
			memory: process.memoryUsage().heapUsed
		};

		// Add download message if present (for export progress)
		if (downloadMessage) {
			progressData.downloadMessage = downloadMessage;
		}

		broadcastProgress(jobId, progressData);
	};
}

// Configure multer for file uploads (stream to disk for large files)
// @ts-ignore - Using disk storage provides path property on files
const fs = require("fs");

// Cloud Run compatible temporary directory configuration
let tmpDir;
if (NODE_ENV === "production") {
	// Use system temp directory in production (Cloud Run compatible)
	tmpDir = path.join(os.tmpdir(), "mixpanel-import");
} else {
	// Use local ./tmp directory in development
	tmpDir = path.join(__dirname, "..", "tmp");
}

// Ensure tmp directory exists and clean it on startup

if (!fs.existsSync(tmpDir)) {
	fs.mkdirSync(tmpDir, { recursive: true });
	logger.info({ tmpDir }, "created tmp dir");
} else {
	// Clean up any existing temp files on startup
	try {
		const files = fs.readdirSync(tmpDir);
		let cleanedCount = 0;
		for (const file of files) {
			const filePath = path.join(tmpDir, file);
			const stats = fs.statSync(filePath);
			if (stats.isFile() && !file.startsWith(".")) {
				// not a dotfile, safe to delete
				fs.unlinkSync(filePath);
				cleanedCount++;
			}
		}
		if (cleanedCount > 0) {
			logger.info({ cleanedCount }, "cleaned tmp dir (startup	)");
		}
	} catch (cleanupError) {
		logger.warn({ err: cleanupError }, "failed to clean tmp dir (startup)");
	}
}
logger.info({ tmpDir, environment: NODE_ENV }, "temp dir alive");

// File size limits based on environment
// Production (Cloud Run with IAP): 30MB due to HTTP/1.1 32MB limit
// Local/Development: 8GB (no IAP restrictions)
const FILE_SIZE_LIMIT = NODE_ENV === "production"
	? 30 * 1024 * 1024      // 30MB for Cloud Run
	: 8 * 1024 * 1024 * 1024; // 8GB for local

const upload = multer({
	storage: multer.diskStorage({
		destination: (req, file, cb) => {
			cb(null, tmpDir);
		},
		filename: (req, file, cb) => {
			// Generate unique filename with timestamp
			const uniqueName = `mixpanel-import-${Date.now()}-${Math.random().toString(36).substring(2)}-${file.originalname}`;
			cb(null, uniqueName);
		}
	}),
	limits: {
		fileSize: FILE_SIZE_LIMIT,
		files: 10 // Max 10 files per upload
	}
});

logger.info({
	fileSizeLimit: `${(FILE_SIZE_LIMIT / 1024 / 1024).toFixed(0)} MB`,
	environment: NODE_ENV
}, "file upload limit configured");

/** @typedef {import('../index.d.ts').Options} Options */
/** @typedef {import('../index.d.ts').Creds} Creds */
/** @typedef {import('../index.d.ts').Data} Data */

// Extended multer file type for disk storage
/** @typedef {Object} MulterDiskFile
 * @property {string} fieldname
 * @property {string} originalname
 * @property {string} encoding
 * @property {string} mimetype
 * @property {number} size
 * @property {string} destination
 * @property {string} filename
 * @property {string} path
 * @property {Buffer} buffer
 */

// Middleware
app.use(express.json({ limit: "2000mb" }));
app.use(express.urlencoded({ extended: true, limit: "2000mb" }));

// Helper function to safely serve files
function serveFile(res, filename) {
	const filePath = path.join(__dirname, "public", filename);

	try {
		if (fs.existsSync(filePath)) {
			const content = fs.readFileSync(filePath, 'utf8');
			res.setHeader('Content-Type', 'text/html');
			res.send(content);
		} else {
			res.status(404).send(`File not found: ${filename} at ${filePath}`);
		}
	} catch (error) {
		res.status(500).send(`Error serving file: ${error.message}`);
	}
}

// Static files middleware - this serves index.html at / automatically
app.use(express.static(path.join(__dirname, "public")));
// @ts-ignore
app.use(cookieParser());
app.use((req, res, next) => {
	//for idmgmt: https://cloud.google.com/iap/docs/identity-howto
	const rawUser = req.headers['x-goog-authenticated-user-email'];
	if (rawUser) {
		let user;
		try {
			// Extract email from accounts.google.com:user@domain.com format
			// @ts-ignore
			const decodedUser = decodeURIComponent(rawUser);
			user = decodedUser.includes(':') ? decodedUser.split(':').pop() : decodedUser;
			//logger.info({ user }, "authed user");
		} catch (error) {
			user = 'anonymous';
		}
		res.cookie('user', user, {
			maxAge: 900000,
			httpOnly: false
			//sameSite: 'none'
		});
	}
	next();
});




// Routes for import and export
app.get("/import", (req, res) => {
	serveFile(res, "import.html");
});

app.get("/export", (req, res) => {
	serveFile(res, "export.html");
});

// Multer error handling middleware
// eslint-disable-next-line no-unused-vars
function handleMulterError(err, req, res, next) {
	if (err instanceof multer.MulterError) {
		if (err.code === 'LIMIT_FILE_SIZE') {
			logger.warn({ errorCode: err.code, fileSize: err.field }, "file too large");

			const limitMB = (FILE_SIZE_LIMIT / 1024 / 1024).toFixed(0);
			let errorMessage = `File too large. Maximum size is ${limitMB}MB.`;

			// In production (Cloud Run), suggest alternatives
			if (NODE_ENV === "production") {
				errorMessage += `\n\nFor larger files, you can:\n` +
					`1. Use Google Cloud Storage (gs://) or Amazon S3 (s3://) paths\n` +
					`2. Run the app locally: npx mixpanel-import --ui`;
			} else {
				errorMessage += ` For files larger than ${limitMB}MB, please use Google Cloud Storage (gs://) or Amazon S3 (s3://) paths.`;
			}

			return res.status(413).json({
				success: false,
				error: errorMessage
			});
		}
		if (err.code === 'LIMIT_FILE_COUNT') {
			return res.status(400).json({
				success: false,
				error: `Too many files. Maximum is 10 files per upload.`
			});
		}
		logger.error({ err }, "multer error");
		return res.status(400).json({
			success: false,
			error: `Upload error: ${err.message}`
		});
	}
	next(err);
}

// Handle file upload preparation (returns jobId for WebSocket execution)
// @ts-ignore
app.post("/job/prepare", upload.array("files"), handleMulterError, async (req, res) => {
	try {
		const { options } = req.body;

		// Parse options to get file source info
		const opts = JSON.parse(options || "{}");

		// Generate unique job ID
		const jobId = `job-${Date.now()}-${Math.random().toString(36).substring(2)}`;

		// For local files, store file paths temporarily
		let filePaths = null;
		if (req.files && req.files.length > 0) {
			const dataFiles = req.files.filter(file => file.fieldname === "files");
			if (dataFiles.length === 1) {
				filePaths = dataFiles[0].path;
			} else if (dataFiles.length > 0) {
				filePaths = dataFiles.map(file => file.path);
			}

			// Store file info for this job (will be used when WebSocket starts job)
			jobStatuses.set(jobId, {
				status: "prepared",
				filePaths: filePaths,
				files: req.files,
				startTime: Date.now(),
				lastUpdate: Date.now()
			});

			logger.debug({ jobId, fileCount: dataFiles.length }, "files uploaded and prepared");
		}

		// Return jobId so client can connect WebSocket
		res.json({
			success: true,
			jobId: jobId,
			message: "Files uploaded - connect WebSocket to start job"
		});
	} catch (error) {
		logger.error({ err: error }, "file upload error");
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Legacy endpoint (backward compatibility)
// @ts-ignore
app.post("/job", upload.array("files"), handleMulterError, async (req, res) => {
	// Extract parameters for logging
	const logParams = {
		credentials: req.body.credentials ? "provided" : "missing",
		options: req.body.options ? JSON.parse(req.body.options || "{}") : {},
		transformCode: req.body.transformCode ? "provided" : "none",
		cloudPaths: req.body.cloudPaths ? JSON.parse(req.body.cloudPaths || "[]") : null,
		fileCount: req.files ? req.files.length : 0,
		fileNames: req.files ? req.files.map(f => ({ fieldname: f.fieldname, originalname: f.originalname, size: f.size })) : []
	};

	logger.info(logParams, "job request");
	try {
		const { credentials, options, transformCode } = req.body;

		// Parse JSON strings
		/** @type {Creds} */
		const creds = JSON.parse(credentials || "{}");
		/** @type {Options} */
		const opts = JSON.parse(options || "{}");

		// Handle GCS credentials file if provided
		const gcsCredentialsFile = req.files?.find(file => file.fieldname === "gcsCredentials");
		if (gcsCredentialsFile) {
			try {
				// Validate it's a JSON file
				const credentialsContent = fs.readFileSync(gcsCredentialsFile.path, "utf8");
				const credentialsJson = JSON.parse(credentialsContent);

				// Validate it looks like a service account key
				if (!credentialsJson.type || credentialsJson.type !== "service_account") {
					throw new Error("Invalid service account credentials file");
				}

				// Pass the credentials file path to the import options
				opts.gcsCredentials = gcsCredentialsFile.path;
				logger.info("");
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: `GCS credentials file error: ${err.message}`
				});
			}
		}

		// Add transform function if provided
		if (transformCode && transformCode.trim()) {
			try {
				// Create function from code string
				// opts.transformFunc = new Function('data', 'heavy', transformCode);
				opts.transformFunc = eval(`(${transformCode})`);
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: `Transform function error: ${err.message}`
				});
			}
		}

		opts.showProgress = true;
		if (NODE_ENV === "production") opts.verbose = false;
		if (NODE_ENV === "production") opts.logs = false;

		// Process files or cloud paths
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				logger.info({ cloudPaths }, "cloud paths");
				data = cloudPaths; // Pass cloud paths directly to mixpanel-import
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: "Invalid cloud paths format"
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Filter out non-data files (like GCS credentials)
			const dataFiles = req.files.filter(file => file.fieldname === "files");

			if (dataFiles.length === 0) {
				return res.status(400).json({
					success: false,
					error: "No data files provided"
				});
			}

			// Handle local files - pass file paths to mixpanel-import
			if (dataFiles.length === 1) {
				// Single file - pass file path
				data = dataFiles[0].path;
				logger.debug({ filePath: data }, "single file");
			} else {
				// Multiple files - pass array of file paths
				data = dataFiles.map(file => file.path);
				logger.debug({ fileCount: dataFiles.length }, "multiple files");
			}
		} else {
			return res.status(400).json({
				success: false,
				error: "No files or cloud paths provided"
			});
		}

		// Generate unique job ID for tracking
		const jobId = `job-${Date.now()}-${Math.random().toString(36).substring(2)}`;

		// Create child logger with jobId for correlation
		const jobLogger = logger.child({ jobId });
		jobLogger.info({ fileCount: Array.isArray(data) ? data.length : 1 }, "import started");

		// Initialize job status (serverless-friendly tracking)
		updateJobStatus(jobId, "starting");

		// Add progress callback for WebSocket updates (client may already be connected)
		opts.progressCallback = createProgressCallback(jobId);

		// Send jobId immediately so client can connect WebSocket for progress updates
		res.json({
			success: true,
			jobId: jobId,
			message: "Import started - connect WebSocket for progress",
			statusUrl: `/job/${jobId}/status`
		});

		// Run the import asynchronously
		// WebSocket progress updates will keep the Cloud Run container alive
		try {
			// Update status to running
			updateJobStatus(jobId, "running");

			const result = await mixpanelImport(creds, data, opts);
			const { total, success, failed, empty } = result;
			jobLogger.info({ total, success, failed, empty, jobId }, "job complete");

			// Signal job completion via WebSocket (for real-time UI updates)
			signalJobComplete(jobId, result);
		} catch (jobError) {
			jobLogger.error({ err: jobError }, "job failed");

			// Update job status to failed (persists beyond WebSocket)
			updateJobStatus(jobId, "failed", null, { error: jobError.message });

			// Signal failure via WebSocket if available
			const jobData = activeJobs.get(jobId);
			if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
				try {
					jobData.ws.send(
						JSON.stringify({
							type: "job-error",
							jobId: jobId,
							error: jobError.message,
							timestamp: Date.now()
						})
					);
				} catch (wsError) {
					jobLogger.error({ err: wsError }, "ws error send failed");
				}
			}
			activeJobs.delete(jobId);
		} finally {
			// Clean up temporary files after job completion/failure
			if (req.files && req.files.length > 0) {
				for (const file of req.files) {
					try {
						// Don't clean up GCS credentials file immediately - it might still be needed
						if (file.fieldname !== "gcsCredentials") {
							fs.unlinkSync(file.path);
							jobLogger.debug({ filePath: file.path }, "temp file cleaned");
						}
					} catch (cleanupError) {
						jobLogger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
					}
				}

				// // Clean up GCS credentials file after a delay
				// setTimeout(() => {
				// 	for (const file of req.files) {
				// 		if (file.fieldname === "gcsCredentials") {
				// 			try {
				// 				fs.unlinkSync(file.path);
				// 				jobLogger.debug({ filePath: file.path }, "gcs creds cleaned");
				// 			} catch (cleanupError) {
				// 				jobLogger.warn({ err: cleanupError, filePath: file.path }, "gcs creds cleanup failed");
				// 			}
				// 		}
				// 	}
				// }, 5000); // 5 second delay
			}
		}
	} catch (error) {
		logger.error({ err: error }, "job error");

		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned (error)");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle data preview/sample
// @ts-ignore
app.post("/sample", upload.array("files"), handleMulterError, async (req, res) => {
	try {
		const { credentials, options } = req.body;

		// Parse JSON strings - handle both array (from FormData) and string formats
		const credentialsStr = Array.isArray(credentials) ? credentials[0] : credentials;
		const optionsStr = Array.isArray(options) ? options[0] : options;

		const creds = JSON.parse(credentialsStr || "{}");
		const opts = JSON.parse(optionsStr || "{}");
		if (NODE_ENV === "production") opts.verbose = false;
		if (NODE_ENV === "production") opts.logs = false;

		// Force sample settings - no transforms, maxRecords=500, dryRun=true
		// Preview: no credentials, no transforms
		opts.dryRun = true;
		opts.maxRecords = 500;
		opts.transformFunc = function id(a) {
			return a;
		}; // Identity function
		opts.fixData = false; // CRITICAL: Keep raw CSV structure
		opts.removeNulls = false; // Keep raw data as-is
		opts.flattenData = false; // No flattening
		opts.vendor = ""; // No vendor transforms
		opts.fixTime = false; // No time fixing
		opts.addToken = false; // No token addition
		opts.compress = false; // No compression
		opts.strict = false; // No validation
		opts.dedupe = false; // No deduplication
		opts.recordType = ""; // CRITICAL: Remove recordType to prevent CSV->event transformation

		// Process files or cloud paths (same as main endpoint)
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				logger.debug({ cloudPaths }, "sample from cloud");
				data = cloudPaths;
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: "Invalid cloud paths format"
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Handle local files - pass file paths to mixpanel-import
			if (req.files.length === 1) {
				// Single file - pass file path
				data = req.files[0].path;
				logger.debug({ filePath: data }, "sample from file");
			} else {
				// Multiple files - pass array of file paths
				data = req.files.map(file => file.path);
				logger.debug({ fileCount: req.files.length }, "sample from files");
			}
		} else {
			return res.status(400).json({
				success: false,
				error: "No files or cloud paths provided"
			});
		}

		logger.debug({ maxRecords: opts.maxRecords }, "sampling");

		// For dryRun preview, only need cloud storage credentials (not Mixpanel credentials)
		// Keep: gcpProjectId, s3Key, s3Secret, s3Region
		// Remove: token, secret, project, acct, pass, groupKey, lookupTableId, dataGroupId, secondToken
		const previewCreds = {
			gcpProjectId: creds.gcpProjectId,
			s3Key: creds.s3Key,
			s3Secret: creds.s3Secret,
			s3Region: creds.s3Region
		};

		// Run the sample
		const result = await mixpanelImport(previewCreds, data, opts);

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.json({
			success: true,
			sampleData: result.dryRun || []
		});
	} catch (error) {
		logger.error({ err: error }, "sample error");

		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned (error)");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle column detection for mapper
// @ts-ignore
app.post("/columns", upload.array("files"), handleMulterError, async (req, res) => {
	try {
		const { credentials, options } = req.body;

		// Parse JSON strings - handle both array (from FormData) and string formats
		const credentialsStr = Array.isArray(credentials) ? credentials[0] : credentials;
		const optionsStr = Array.isArray(options) ? options[0] : options;

		const creds = JSON.parse(credentialsStr || "{}");
		const opts = JSON.parse(optionsStr || "{}");
		if (NODE_ENV === "production") opts.verbose = false;
		if (NODE_ENV === "production") opts.logs = false;

		// Force sample settings - let mixpanel-import handle all parsing
		// Column detection: no credentials, no transforms
		opts.dryRun = true;
		opts.maxRecords = 500; // Sample up to 500 records
		opts.transformFunc = function id(a) {
			return a;
		}; // Identity function - no transforms
		opts.fixData = false; // CRITICAL: Keep raw CSV structure - no event/properties shape
		opts.removeNulls = false; // Keep all columns
		opts.flattenData = false; // No flattening
		opts.vendor = ""; // No vendor transforms
		opts.fixTime = false; // No time fixing
		opts.addToken = false; // No token addition
		opts.compress = false; // No compression
		opts.strict = false; // No validation
		opts.dedupe = false; // No deduplication
		opts.recordType = ""; // CRITICAL: Remove recordType to prevent CSV->event transformation

		// Let mixpanel-import handle file parsing - pass raw data directly
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				logger.debug({ cloudPaths }, "columns from cloud");
				// Only use the first file for column detection
				data = Array.isArray(cloudPaths) ? cloudPaths[0] : cloudPaths;
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: "Invalid cloud paths format"
				});
			}
		} else if (req.files && req.files.length > 0) {
			// Use uploaded file from disk storage
			const uploadedFile = req.files[0];
			logger.debug({ fileName: uploadedFile.originalname, filePath: uploadedFile.path }, "columns from file");

			data = uploadedFile.path; // Pass file path directly
		} else {
			return res.status(400).json({
				success: false,
				error: "No files or cloud paths provided"
			});
		}

		logger.debug({ maxRecords: opts.maxRecords }, "detecting columns");

		// Let mixpanel-import handle all parsing and get parsed results from dryRun
		const result = await mixpanelImport(creds, data, opts);

		const sampleData = result.dryRun || [];
		logger.debug({ recordCount: sampleData.length }, "parsed records");

		// Extract unique column names from the parsed dryRun results
		const columnSet = new Set();
		sampleData.forEach((record, index) => {
			if (record && typeof record === "object") {
				Object.keys(record).forEach(key => columnSet.add(key));
			} else {
				logger.debug({ index, recordType: typeof record }, "non-object record");
			}
		});

		const columns = Array.from(columnSet).sort();

		logger.debug({ columnCount: columns.length }, "columns detected");

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.json({
			success: true,
			columns: columns,
			sampleCount: sampleData.length
		});
	} catch (error) {
		logger.error({ err: error }, "columns error");

		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned (error)");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle dry run
// @ts-ignore
app.post("/dry-run", upload.array("files"), handleMulterError, async (req, res) => {
	try {
		const { credentials, options, transformCode } = req.body;

		// Parse JSON strings
		const creds = JSON.parse(credentials || "{}");

		const opts = JSON.parse(options || "{}");
		if (NODE_ENV === "production") opts.verbose = false;
		if (NODE_ENV === "production") opts.logs = false;

		// Force dry run with maxRecords limit
		opts.dryRun = true;
		opts.maxRecords = 100; // Limit dry run to 100 records for testing
		// Ensure fixData is explicitly controlled by user options, not forced

		// Add transform function if provided
		if (transformCode && transformCode.trim()) {
			try {
				// opts.transformFunc = new Function('data', 'heavy', transformCode);
				opts.transformFunc = eval(`(${transformCode})`);
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: `Transform function error: ${err.message}`
				});
			}
		}

		// Process files or cloud paths (same as main endpoint)
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				logger.debug({ cloudPaths }, "dry run from cloud");
				data = cloudPaths; // Pass cloud paths directly to mixpanel-import
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: "Invalid cloud paths format"
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Handle local files - pass file paths to mixpanel-import
			if (req.files.length === 1) {
				// Single file - pass file path
				data = req.files[0].path;
				logger.debug({ filePath: data }, "dry run from file");
			} else {
				// Multiple files - pass array of file paths
				data = req.files.map(file => file.path);
				logger.debug({ fileCount: req.files.length }, "dry run from files");
			}
		} else {
			return res.status(400).json({
				success: false,
				error: "No files or cloud paths provided"
			});
		}

		logger.debug({ fileCount: Array.isArray(data) ? data.length : 1 }, "dry run started");

		// Run raw data fetch first (no transforms) for comparison
		const rawOpts = { ...opts };
		rawOpts.transformFunc = null;
		rawOpts.fixData = false; // CRITICAL: Keep raw CSV structure
		rawOpts.removeNulls = false;
		rawOpts.flattenData = false;
		rawOpts.vendor = "";
		rawOpts.maxRecords = 100; // Match dry run limit

		const rawResult = await mixpanelImport(creds, data, rawOpts);

		// Run the transformed dry run
		const transformedResult = await mixpanelImport(creds, data, opts);

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.json({
			success: true,
			result: transformedResult,
			previewData: transformedResult.dryRun || [],
			rawData: rawResult.dryRun || []
		});
	} catch (error) {
		logger.error({ err: error }, "dry run error");

		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					logger.debug({ filePath: file.path }, "temp file cleaned (error)");
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, filePath: file.path }, "temp cleanup failed");
				}
			}
		}

		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle Snowcat job requests (import and export)
app.post("/snowcat/request", async (req, res) => {
	try {
		const jobConfig = req.body;
		const jobType = jobConfig.jobType || 'import';  // default to import for backward compat

		// Remove jobType from config (not part of Snowcat API)
		delete jobConfig.jobType;

		logger.info({ jobConfig, jobType }, "snowcat request");

		// Validate required fields
		if (!jobConfig.cloud_path) {
			return res.status(400).json({
				success: false,
				error: "cloud_path is required for Snowcat jobs"
			});
		}

		if (!jobConfig.mp_token && !jobConfig.mp_secret) {
			return res.status(400).json({
				success: false,
				error: "Either mp_token or mp_secret is required for Snowcat jobs"
			});
		}

		// Export-specific validation
		if (jobType === 'export') {
			if (!jobConfig.options?.start_date || !jobConfig.options?.end_date) {
				return res.status(400).json({
					success: false,
					error: "options.start_date and options.end_date are required for export jobs"
				});
			}
		}

		// Build endpoint URL based on job type
		const endpoint = `${SNOWCAT_URL}/${jobType}`;

		// Make authenticated request to Snowcat service using ADC
		const client = await getSnowcatClient();

		// Make authenticated request to Snowcat service
		const response = await client.request({
			url: endpoint,
			method: 'POST',
			data: jobConfig,
			headers: {
				'Content-Type': 'application/json'
			}
		});

		logger.info({ snowcatResponse: response.data, jobType }, "snowcat job created");

		// Return Snowcat response to client
		res.json({
			success: true,
			snowcatResponse: response.data,
			message: `Snowcat ${jobType} job requested successfully`
		});

	} catch (error) {
		logger.error({ err: error }, "snowcat request error");

		// Parse error details if available
		let errorMessage = error.message;
		if (error.response?.data) {
			errorMessage = error.response.data.error || error.response.data.message || errorMessage;
		}

		res.status(500).json({
			success: false,
			error: `Snowcat request failed: ${errorMessage}`
		});
	}
});

// Handle export operations
// @ts-ignore
app.post("/export", async (req, res) => {
	try {
		const exportData = req.body;

		// Parse credentials and options for export
		/** @type {Creds} */
		const creds = {
			acct: exportData.acct,
			pass: exportData.pass,
			secret: exportData.secret,
			project: exportData.project,
			token: exportData.token,
			groupKey: exportData.groupKey,
			dataGroupId: exportData.dataGroupId,
			secondToken: exportData.secondToken
		};

		// Generate unique job ID for this export
		const jobId = `export-${Date.now()}-${Math.random().toString(36).substring(2)}`;

		// Create job-specific temp directory
		const jobTmpDir = path.join(tmpDir, jobId);
		if (!fs.existsSync(jobTmpDir)) {
			fs.mkdirSync(jobTmpDir, { recursive: true });
		}

		/** @type {Options} */
		const opts = {
			recordType: exportData.recordType,
			region: exportData.region || "US",
			workers: exportData.workers || 10,
			start: exportData.start,
			end: exportData.end,
			epochStart: exportData.epochStart,
			epochEnd: exportData.epochEnd,
			whereClause: exportData.whereClause,
			limit: exportData.limit,
			logs: exportData.logs || false,
			verbose: exportData.verbose || false,
			showProgress: exportData.showProgress || false,
			writeToFile: exportData.writeToFile !== false, // Default to true for exports that create files
			where: jobTmpDir, // Save files to job-specific temp directory (local default)
			outputFilePath: exportData.outputFilePath,
			abridged: exportData.abridged !== undefined ? exportData.abridged : true,  // Default to true for performance
			compress: exportData.compress !== false, // Default true for cloud exports
			compressionLevel: exportData.compressionLevel || 6,

			// Cloud destination options
			gcpProjectId: exportData.gcpProjectId,
			gcsCredentials: exportData.gcsCredentials,
			s3Region: exportData.s3Region,
			s3Key: exportData.s3Key,
			s3Secret: exportData.s3Secret
		};

		// Handle cloud destinations
		const destinationType = exportData.destinationType || 'local';
		if (destinationType === 'gcs' && exportData.gcsPath) {
			// Validate GCS write access before starting export
			try {
				await validateCloudWriteAccess(exportData.gcsPath, {
					gcpProjectId: opts.gcpProjectId,
					gcsCredentials: opts.gcsCredentials
				});

				// Set GCS path as export destination
				opts.where = exportData.gcsPath;
				logger.debug({ jobId, gcsPath: exportData.gcsPath }, "gcs write validated");
			} catch (error) {
				logger.error({ err: error, jobId, gcsPath: exportData.gcsPath }, "gcs validation failed");
				return res.status(400).json({
					success: false,
					error: `GCS write validation failed: ${error.message}`
				});
			}
		} else if (destinationType === 's3' && exportData.s3Path) {
			// Validate S3 write access before starting export
			try {
				await validateCloudWriteAccess(exportData.s3Path, {
					s3Region: opts.s3Region,
					s3Key: opts.s3Key,
					s3Secret: opts.s3Secret
				});

				// Set S3 path as export destination
				opts.where = exportData.s3Path;
				logger.debug({ jobId, s3Path: exportData.s3Path }, "s3 write validated");
			} catch (error) {
				logger.error({ err: error, jobId, s3Path: exportData.s3Path }, "s3 validation failed");
				return res.status(400).json({
					success: false,
					error: `S3 write validation failed: ${error.message}`
				});
			}
		}

		// Check if this is a file-producing export type
		const fileProducingTypes = ["export", "profile-export", "profile-delete", "group-export", "group-delete", "annotations", "get-annotations"];
		const isFileProducing = fileProducingTypes.includes(exportData.recordType);

		if (isFileProducing) {
			// For file-producing exports, send jobId then run asynchronously

			// Add progress callback for WebSocket updates
			opts.progressCallback = createProgressCallback(jobId);

			// Create child logger with jobId for correlation
			const exportLogger = logger.child({ jobId });
			exportLogger.info({ recordType: opts.recordType }, "export started");

			// Send jobId immediately so client can connect WebSocket
			res.json({
				success: true,
				jobId: jobId,
				message: "Export started - connect WebSocket for progress"
			});

			// Run the export asynchronously (WebSocket keeps container alive)
			try {
				const result = await mixpanelImport(creds, null, opts);
				exportLogger.info({ recordsProcessed: result.total }, "export complete");

				// Check if this was a cloud storage export
				const destinationType = exportData.destinationType || 'local';
				const isCloudExport = (destinationType === 'gcs' && exportData.gcsPath) ||
					(destinationType === 's3' && exportData.s3Path);

				let exportResult;
				if (isCloudExport) {
					// For cloud storage exports, include the destination path
					exportResult = {
						...result,
						cloudDestination: destinationType === 'gcs' ? exportData.gcsPath : exportData.s3Path,
						destinationType: destinationType,
						message: `Successfully exported ${result.total} records to ${destinationType === 'gcs' ? exportData.gcsPath : exportData.s3Path}`
					};
				} else {
					// For local exports, find the created file(s) and prepare for download
					const exportedFiles = [];
					if (fs.existsSync(jobTmpDir)) {
						const files = fs.readdirSync(jobTmpDir);
						for (const file of files) {
							const filePath = path.join(jobTmpDir, file);
							const stats = fs.statSync(filePath);
							if (stats.isFile()) {
								exportedFiles.push({
									name: file,
									path: filePath,
									size: stats.size
								});
							}
						}
					}

					exportResult = {
						...result,
						files: exportedFiles,
						downloadUrl: exportedFiles.length === 1 ? `/download/${jobId}/${exportedFiles[0].name}` : `/download/${jobId}`
					};
				}

				// Signal job completion via WebSocket with file info
				signalJobComplete(jobId, exportResult);
			} catch (exportError) {
				exportLogger.error({ err: exportError }, "export failed");

				// Clean up temp directory on error
				if (fs.existsSync(jobTmpDir)) {
					fs.rmSync(jobTmpDir, { recursive: true, force: true });
				}

				// Update job status to failed (persists beyond WebSocket)
				updateJobStatus(jobId, "failed", null, { error: exportError.message });

				// Signal failure via WebSocket if available
				const jobData = activeJobs.get(jobId);
				if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
					try {
						jobData.ws.send(
							JSON.stringify({
								type: "job-error",
								jobId: jobId,
								error: exportError.message,
								timestamp: Date.now()
							})
						);
					} catch (wsError) {
						exportLogger.error({ err: wsError }, "ws error send failed");
					}
				}
				activeJobs.delete(jobId);
			}
		} else {
			// Stream-to-stream operations run synchronously
			logger.info({ recordType: opts.recordType }, "export-import started");

			const result = await mixpanelImport(creds, null, opts);
			logger.info({ recordsProcessed: result.total }, "export-import complete");

			res.json({
				success: true,
				result: result,
				message: "Export-import operation completed"
			});
		}
	} catch (error) {
		logger.error({ err: error }, "export error");
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle export dry run
// @ts-ignore
app.post("/export-dry-run", async (req, res) => {
	try {
		const exportData = req.body;

		// Parse credentials and options for dry run export
		/** @type {Creds} */
		const creds = {
			acct: exportData.acct,
			pass: exportData.pass,
			secret: exportData.secret,
			project: exportData.project,
			token: exportData.token,
			groupKey: exportData.groupKey,
			dataGroupId: exportData.dataGroupId,
			secondToken: exportData.secondToken
		};

		/** @type {Options} */
		const opts = {
			recordType: exportData.recordType,
			region: exportData.region || "US",
			workers: exportData.workers || 10,
			start: exportData.start,
			end: exportData.end,
			epochStart: exportData.epochStart,
			epochEnd: exportData.epochEnd,
			whereClause: exportData.whereClause,
			limit: Math.min(exportData.limit || 100, 100), // Limit dry runs to 100 records max
			dryRun: true, // Force dry run mode
			verbose: true,
			showProgress: exportData.showProgress || true,
			writeToFile: false, // Never write files in dry run
			logs: false // No logs for dry run
		};

		logger.debug({ recordType: opts.recordType }, "export dry run started");

		// Run the dry run export
		const result = await mixpanelImport(creds, null, opts);

		logger.debug({ recordCount: result.total }, "export dry run complete");

		res.json({
			success: true,
			result,
			previewData: result.dryRun || []
		});
	} catch (error) {
		logger.error({ err: error }, "export dry run error");
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// File download endpoint for exported files - with filename
app.get("/download/:jobId/:filename", (req, res) => {
	try {
		const jobId = req.params.jobId;
		const filename = req.params.filename;

		// Job temp directory
		const jobTmpDir = path.join(tmpDir, jobId);

		if (!fs.existsSync(jobTmpDir)) {
			return res.status(404).json({
				success: false,
				error: `Export files not found for job ${jobId}. The files may have been cleaned up or the export may not have completed successfully.`
			});
		}

		// Get list of files in the job directory
		const files = fs.readdirSync(jobTmpDir).filter(file => {
			const filePath = path.join(jobTmpDir, file);
			return fs.statSync(filePath).isFile();
		});

		if (files.length === 0) {
			return res.status(404).json({
				success: false,
				error: `No export files found for job ${jobId}.`
			});
		}

		let targetFile;
		if (filename) {
			// Download specific file
			if (!files.includes(filename)) {
				return res.status(404).json({
					success: false,
					error: `File ${filename} not found for job ${jobId}.`
				});
			}
			targetFile = filename;
		} else {
			// Download first/only file if no filename specified
			targetFile = files[0];
		}

		const filePath = path.join(jobTmpDir, targetFile);
		const stats = fs.statSync(filePath);

		// Set appropriate headers for file download
		res.setHeader("Content-Disposition", `attachment; filename="${targetFile}"`);
		res.setHeader("Content-Type", "application/octet-stream");
		res.setHeader("Content-Length", stats.size);

		// Stream the file to the client
		const fileStream = fs.createReadStream(filePath);

		fileStream.pipe(res);

		// Handle stream errors
		fileStream.on("error", error => {
			logger.error({ err: error, filePath }, "file stream error");
			if (!res.headersSent) {
				res.status(500).json({
					success: false,
					error: "Failed to stream export file"
				});
			}
		});

		// Clean up temp directory after successful download
		fileStream.on("end", () => {
			logger.info({ fileName: targetFile, fileSize: stats.size }, "download complete");

			// Clean up the temp directory after download
			setTimeout(() => {
				try {
					if (fs.existsSync(jobTmpDir)) {
						fs.rmSync(jobTmpDir, { recursive: true, force: true });
						logger.debug({ jobId }, "temp dir cleaned");
					}
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, jobId }, "temp dir cleanup failed");
				}
			}, 1000); // Small delay to ensure download completes
		});
	} catch (error) {
		logger.error({ err: error }, "download error");
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// File download endpoint for exported files - without filename
app.get("/download/:jobId", (req, res) => {
	try {
		const jobId = req.params.jobId;
		const filename = undefined; // No filename specified

		// Job temp directory
		const jobTmpDir = path.join(tmpDir, jobId);

		if (!fs.existsSync(jobTmpDir)) {
			return res.status(404).json({
				success: false,
				error: `Export files not found for job ${jobId}. The files may have been cleaned up or the export may not have completed successfully.`
			});
		}

		// Get list of files in the job directory
		const files = fs.readdirSync(jobTmpDir).filter(file => {
			const filePath = path.join(jobTmpDir, file);
			return fs.statSync(filePath).isFile();
		});

		if (files.length === 0) {
			return res.status(404).json({
				success: false,
				error: `No export files found for job ${jobId}.`
			});
		}

		let targetFile;
		if (filename) {
			// Download specific file
			if (!files.includes(filename)) {
				return res.status(404).json({
					success: false,
					error: `File ${filename} not found for job ${jobId}.`
				});
			}
			targetFile = filename;
		} else {
			// Download first/only file if no filename specified
			targetFile = files[0];
		}

		const filePath = path.join(jobTmpDir, targetFile);
		const stats = fs.statSync(filePath);

		// Set appropriate headers for file download
		res.setHeader("Content-Disposition", `attachment; filename="${targetFile}"`);
		res.setHeader("Content-Type", "application/octet-stream");
		res.setHeader("Content-Length", stats.size);

		// Stream the file to the client
		const fileStream = fs.createReadStream(filePath);

		fileStream.pipe(res);

		// Handle stream errors
		fileStream.on("error", error => {
			logger.error({ err: error, filePath }, "file stream error");
			if (!res.headersSent) {
				res.status(500).json({
					success: false,
					error: "Failed to stream export file"
				});
			}
		});

		// Clean up temp directory after successful download
		fileStream.on("end", () => {
			logger.info({ fileName: targetFile, fileSize: stats.size }, "download complete");

			// Clean up the temp directory after download
			setTimeout(() => {
				try {
					if (fs.existsSync(jobTmpDir)) {
						fs.rmSync(jobTmpDir, { recursive: true, force: true });
						logger.debug({ jobId }, "temp dir cleaned");
					}
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, jobId }, "temp dir cleanup failed");
				}
			}, 1000); // Small delay to ensure download completes
		});
	} catch (error) {
		logger.error({ err: error }, "download error");
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Job status endpoint - provides REST API alternative to WebSocket
app.get("/job/:jobId/status", (req, res) => {
	const jobId = req.params.jobId;
	const jobStatus = jobStatuses.get(jobId);

	if (!jobStatus) {
		return res.status(404).json({
			success: false,
			error: "Job not found",
			jobId: jobId
		});
	}

	res.json({
		success: true,
		jobId: jobId,
		status: jobStatus.status,
		progress: jobStatus.progress || {},
		result: jobStatus.result,
		startTime: jobStatus.startTime,
		lastUpdate: jobStatus.lastUpdate
	});
});

// Job cleanup endpoint - helps with serverless memory management
app.delete("/job/:jobId", (req, res) => {
	const jobId = req.params.jobId;
	const existed = jobStatuses.has(jobId);

	// Clean up job status
	jobStatuses.delete(jobId);
	activeJobs.delete(jobId);

	// Clean up any temporary files for this job
	const jobTmpDir = path.join(tmpDir, jobId);
	if (fs.existsSync(jobTmpDir)) {
		try {
			fs.rmSync(jobTmpDir, { recursive: true, force: true });
			logger.debug({ jobId }, "job temp dir cleaned");
		} catch (cleanupError) {
			logger.warn({ err: cleanupError, jobId }, "job temp dir cleanup failed");
		}
	}

	logger.debug({ jobId, existed }, "job cleaned up");

	res.json({
		success: true,
		jobId: jobId,
		cleaned: existed
	});
});

// Automatic cleanup function for old jobs and temp files
function cleanupOldJobs() {
	const now = Date.now();
	const maxAge = JOB_MAX_AGE; // Use configurable max age
	let cleanedJobs = 0;
	let cleanedFiles = 0;

	// Clean up old job statuses
	for (const [jobId, jobStatus] of jobStatuses.entries()) {
		if (now - jobStatus.lastUpdate > maxAge) {
			jobStatuses.delete(jobId);
			activeJobs.delete(jobId);
			cleanedJobs++;

			// Clean up associated temp files
			const jobTmpDir = path.join(tmpDir, jobId);
			if (fs.existsSync(jobTmpDir)) {
				try {
					fs.rmSync(jobTmpDir, { recursive: true, force: true });
					cleanedFiles++;
				} catch (cleanupError) {
					logger.warn({ err: cleanupError, jobId }, "expired job cleanup failed");
				}
			}
		}
	}

	// CRITICAL: Limit map sizes to prevent unbounded growth
	if (activeJobs.size > 100) {
		logger.warn({ activeJobsCount: activeJobs.size }, "activeJobs map too large");
		// Remove oldest jobs
		const sortedJobs = Array.from(activeJobs.entries())
			.sort((a, b) => a[1].lastUpdate - b[1].lastUpdate);
		const toRemove = sortedJobs.slice(0, sortedJobs.length - 50);
		toRemove.forEach(([jobId]) => activeJobs.delete(jobId));
		logger.info({ removed: toRemove.length }, "pruned old activeJobs");
	}

	if (jobStatuses.size > 100) {
		logger.warn({ jobStatusesCount: jobStatuses.size }, "jobStatuses map too large");
		// Remove oldest jobs
		const sortedStatuses = Array.from(jobStatuses.entries())
			.sort((a, b) => a[1].lastUpdate - b[1].lastUpdate);
		const toRemove = sortedStatuses.slice(0, sortedStatuses.length - 50);
		toRemove.forEach(([jobId]) => jobStatuses.delete(jobId));
		logger.info({ removed: toRemove.length }, "pruned old jobStatuses");
	}

	// Clean up orphaned temp directories (no corresponding job status)
	try {
		if (fs.existsSync(tmpDir)) {
			const entries = fs.readdirSync(tmpDir, { withFileTypes: true });
			for (const entry of entries) {
				if (entry.isDirectory()) {
					const dirName = entry.name;
					// Check if this looks like a job directory and has no corresponding job status
					if ((dirName.startsWith("job-") || dirName.startsWith("export-")) && !jobStatuses.has(dirName)) {
						const dirPath = path.join(tmpDir, dirName);
						const stats = fs.statSync(dirPath);
						if (now - stats.mtime.getTime() > maxAge) {
							try {
								fs.rmSync(dirPath, { recursive: true, force: true });
								cleanedFiles++;
								logger.debug({ dirPath }, "orphaned dir cleaned");
							} catch (cleanupError) {
								logger.warn({ err: cleanupError, dirPath }, "orphaned cleanup failed");
							}
						}
					}
				}
			}
		}
	} catch (cleanupError) {
		logger.warn({ err: cleanupError, tmpDir }, "cleanup scan failed");
	}

	if (cleanedJobs > 0 || cleanedFiles > 0) {
		logger.info({ cleanedJobs, cleanedFiles }, "auto cleanup complete");
	}

	// Trigger GC after cleanup if available
	if (global.gc) {
		const memUsage = process.memoryUsage();
		const heapPercent = (memUsage.heapUsed / memUsage.heapTotal) * 100;
		if (heapPercent > 70 || cleanedJobs > 0) {
			global.gc();
			logger.debug({ heapPercent: heapPercent.toFixed(1), cleanedJobs }, "gc triggered (cleanup)");
		}
	}
}

// Run cleanup every 5 minutes (more aggressive for Cloud Run)
setInterval(cleanupOldJobs, JOB_CLEANUP_INTERVAL);

// Run initial cleanup after 1 minute (allow server to stabilize first)
setTimeout(cleanupOldJobs, 60 * 1000);

// Health check with memory monitoring
app.get("/health", (req, res) => {
	const memUsage = process.memoryUsage();
	const heapPercent = ((memUsage.heapUsed / memUsage.heapTotal) * 100).toFixed(1);

	res.json({
		status: "ok",
		timestamp: new Date().toISOString(),
		memory: {
			heapUsed: `${(memUsage.heapUsed / 1024 / 1024).toFixed(1)} MB`,
			heapTotal: `${(memUsage.heapTotal / 1024 / 1024).toFixed(1)} MB`,
			heapPercent: `${heapPercent}%`,
			rss: `${(memUsage.rss / 1024 / 1024).toFixed(1)} MB`
		},
		jobs: {
			active: activeJobs.size,
			tracked: jobStatuses.size
		},
		gc: global.gc ? "enabled" : "disabled"
	});
});

// Memory stats endpoint for debugging
app.get("/memory", (req, res) => {
	const memUsage = process.memoryUsage();
	const heapStats = require('v8').getHeapStatistics();

	// Trigger GC if requested and available
	if (req.query.gc === 'true' && global.gc) {
		global.gc();
		logger.info("manual gc triggered (api)");
	}

	res.json({
		process: {
			heapUsed: memUsage.heapUsed,
			heapTotal: memUsage.heapTotal,
			heapPercent: ((memUsage.heapUsed / memUsage.heapTotal) * 100).toFixed(1) + '%',
			rss: memUsage.rss,
			external: memUsage.external,
			arrayBuffers: memUsage.arrayBuffers
		},
		v8: {
			heapSizeLimit: heapStats.heap_size_limit,
			totalAvailableSize: heapStats.total_available_size,
			usedHeapSize: heapStats.used_heap_size,
			mallocedMemory: heapStats.malloced_memory,
			peakMallocedMemory: heapStats.peak_malloced_memory
		},
		jobs: {
			activeJobs: activeJobs.size,
			jobStatuses: jobStatuses.size
		},
		gc: {
			available: !!global.gc,
			threshold: `${MAX_HEAP_PERCENT}%`
		}
	});
});

// Start server
function startUI(options = {}) {
	const serverPort = options.port || port;
	const { version } = require("../package.json");

	// ASCII Art Logo (same as CLI)
	const hero = String.raw`
_  _ _ _  _ ___  ____ _  _ ____ _       _ _  _ ___  ____ ____ ___ 
|\/| |  \/  |__] |__| |\ | |___ |       | |\/| |__] |  | |__/  |  
|  | | _/\_ |    |  | | \| |___ |___    | |  | |    |__| |  \  |                                                                    
`;

	const banner = `... streamer of data... to mixpanel! (v${version || 2})
\tby AK (ak@mixpanel.com)`;

	return new Promise((resolve, reject) => {
		server.listen(serverPort, err => {
			if (err) {
				reject(err);
			} else {
				// Show CLI logo first
				if (NODE_ENV !== "production") {
					console.log(hero);
					console.log(banner);
					console.log(`\n🚀 UI running at http://localhost:${serverPort}\n\n`);
					// console.log(`\t-webSocket server alive\n\n`);
				}
				logger.info({ port: serverPort }, "server alive");
				resolve(server);
			}
		});
	});
}

// Export for CLI usage
module.exports = { app, startUI };

// If run directly
if (require.main === module) {
	startUI();
}
