const express = require('express');
const path = require('path');
const multer = require('multer');
const WebSocket = require('ws');
const { createServer } = require('http');
const mixpanelImport = require('../index.js');

const app = express();
const server = createServer(app);
const port = process.env.PORT || 3000;

// WebSocket server for real-time progress updates
const wss = new WebSocket.Server({ server });

// Job tracking for WebSocket connections
const activeJobs = new Map(); // jobId -> { ws, startTime, lastUpdate }

// WebSocket connection handler
wss.on('connection', (ws) => {
	console.log('WebSocket client connected');
	
	ws.on('message', (message) => {
		try {
			const data = JSON.parse(message);
			
			if (data.type === 'register-job') {
				const jobId = data.jobId;
				activeJobs.set(jobId, {
					ws: ws,
					startTime: Date.now(),
					lastUpdate: Date.now()
				});
				console.log(`Registered job ${jobId} for WebSocket updates`);
				
				// Send confirmation
				ws.send(JSON.stringify({
					type: 'job-registered',
					jobId: jobId
				}));
			}
		} catch (error) {
			console.error('WebSocket message error:', error);
		}
	});
	
	ws.on('close', () => {
		console.log('WebSocket client disconnected');
		// Clean up any jobs associated with this WebSocket
		for (const [jobId, jobData] of activeJobs.entries()) {
			if (jobData.ws === ws) {
				activeJobs.delete(jobId);
				console.log(`Cleaned up job ${jobId} on WebSocket disconnect`);
			}
		}
	});
	
	ws.on('error', (error) => {
		console.error('WebSocket error:', error);
	});
});

// Function to broadcast progress updates to WebSocket clients
function broadcastProgress(jobId, progressData) {
	const jobData = activeJobs.get(jobId);
	if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
		try {
			const message = {
				type: 'progress',
				jobId: jobId,
				data: progressData,
				timestamp: Date.now()
			};
			jobData.ws.send(JSON.stringify(message));
			jobData.lastUpdate = Date.now();
		} catch (error) {
			console.error(`Failed to send progress to job ${jobId}:`, error);
			// Clean up dead connection
			activeJobs.delete(jobId);
		}
	}
}

// Function to signal job completion
function signalJobComplete(jobId, result) {
	const jobData = activeJobs.get(jobId);
	if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
		try {
			jobData.ws.send(JSON.stringify({
				type: 'job-complete',
				jobId: jobId,
				result: result,
				timestamp: Date.now()
			}));
		} catch (error) {
			console.error(`Failed to send completion to job ${jobId}:`, error);
		}
	}
	// Clean up job tracking
	activeJobs.delete(jobId);
	console.log(`Job ${jobId} completed and cleaned up`);
}

// Function to create a progress callback for a specific job
function createProgressCallback(jobId) {
	return (recordType, processed, requests, eps, bytesProcessed) => {
		broadcastProgress(jobId, {
			recordType: recordType || '',
			processed: processed || 0,
			requests: requests || 0,
			eps: eps || '',
			bytesProcessed: bytesProcessed || 0,
			memory: process.memoryUsage().heapUsed
		});
	};
}

// Configure multer for file uploads (stream to disk for large files)
// @ts-ignore - Using disk storage provides path property on files
const fs = require('fs');

// Ensure tmp directory exists and clean it on startup
const tmpDir = path.join(__dirname, '..', 'tmp');
if (!fs.existsSync(tmpDir)) {
	fs.mkdirSync(tmpDir, { recursive: true });
} else {
	// Clean up any existing temp files on startup
	try {
		const files = fs.readdirSync(tmpDir);
		let cleanedCount = 0;
		for (const file of files) {
			const filePath = path.join(tmpDir, file);
			const stats = fs.statSync(filePath);
			if (stats.isFile() && !file.startsWith('.')) {
				// not a dotfile, safe to delete
				fs.unlinkSync(filePath);
				cleanedCount++;
			}
		}
		if (cleanedCount > 0) {
			console.log(`Cleaned up ${cleanedCount} temporary files from ./tmp/`);
		}
	} catch (cleanupError) {
		console.warn('Failed to clean tmp directory on startup:', cleanupError.message);
	}
}

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
		fileSize: 1000 * 1024 * 1024 // 1GB limit
	}
});

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
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ extended: true, limit: '100mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Serve the main landing page (will be created next)
app.get('/', (req, res) => {
	res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Serve the import UI (E.T.L tool)
app.get('/import', (req, res) => {
	res.sendFile(path.join(__dirname, 'public', 'import.html'));
});

// Serve the export UI (L.T.E tool) 
app.get('/export', (req, res) => {
	res.sendFile(path.join(__dirname, 'public', 'export.html'));
});

// Handle job submission
// @ts-ignore
app.post('/job', upload.array('files'), async (req, res) => {
	try {
		const { credentials, options, transformCode } = req.body;

		// Parse JSON strings
		/** @type {Creds} */
		const creds = JSON.parse(credentials || '{}');
		/** @type {Options} */
		const opts = JSON.parse(options || '{}');

		// Handle GCS credentials file if provided
		const gcsCredentialsFile = req.files?.find(file => file.fieldname === 'gcsCredentials');
		if (gcsCredentialsFile) {
			try {
				// Validate it's a JSON file
				const credentialsContent = fs.readFileSync(gcsCredentialsFile.path, 'utf8');
				const credentialsJson = JSON.parse(credentialsContent);
				
				// Validate it looks like a service account key
				if (!credentialsJson.type || credentialsJson.type !== 'service_account') {
					throw new Error('Invalid service account credentials file');
				}
				
				// Pass the credentials file path to the import options
				opts.gcsCredentials = gcsCredentialsFile.path;
				console.log('Using custom GCS credentials file for authentication');
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

		// Process files or cloud paths
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				console.log(`Using cloud storage paths:`, cloudPaths);
				data = cloudPaths; // Pass cloud paths directly to mixpanel-import
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: 'Invalid cloud paths format'
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Filter out non-data files (like GCS credentials)
			const dataFiles = req.files.filter(file => file.fieldname === 'files');
			
			if (dataFiles.length === 0) {
				return res.status(400).json({
					success: false,
					error: 'No data files provided'
				});
			}
			
			// Handle local files - pass file paths to mixpanel-import
			if (dataFiles.length === 1) {
				// Single file - pass file path
				data = dataFiles[0].path;
				console.log(`Using single local file: ${data}`);
			} else {
				// Multiple files - pass array of file paths
				data = dataFiles.map(file => file.path);
				console.log(`Using multiple local files: ${data.join(', ')}`);
			}
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		// Generate unique job ID for WebSocket tracking
		const jobId = `job-${Date.now()}-${Math.random().toString(36).substring(2)}`;
		
		// Add progress callback for WebSocket updates
		opts.progressCallback = createProgressCallback(jobId);
		
		console.log(`Starting import job ${jobId} with ${Array.isArray(data) ? data.length : 'unknown'} files`);

		// Return job ID immediately so client can connect WebSocket
		res.json({
			success: true,
			jobId: jobId,
			message: 'connecting to websocket; initiating streaming'
		});

		// Run the import asynchronously
		try {
			const result = await mixpanelImport(creds, data, opts);
			const { total, success, failed, empty } = result;
			console.log(`Import job ${jobId} completed: ${total} records | ${success} success | ${failed} failed | ${empty} skipped`);
			
			// Signal job completion via WebSocket
			signalJobComplete(jobId, result);
		} catch (jobError) {
			console.error(`Import job ${jobId} failed:`, jobError);
			
			// Signal job failure via WebSocket
			const jobData = activeJobs.get(jobId);
			if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
				try {
					jobData.ws.send(JSON.stringify({
						type: 'job-error',
						jobId: jobId,
						error: jobError.message,
						timestamp: Date.now()
					}));
				} catch (wsError) {
					console.error(`Failed to send error to job ${jobId}:`, wsError);
				}
			}
			activeJobs.delete(jobId);
		} finally {
			// Clean up temporary files after job completion/failure
			if (req.files && req.files.length > 0) {
				for (const file of req.files) {
					try {
						// Don't clean up GCS credentials file immediately - it might still be needed
						if (file.fieldname !== 'gcsCredentials') {
							fs.unlinkSync(file.path);
							console.log(`Cleaned up temp file: ${file.path}`);
						}
					} catch (cleanupError) {
						console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
					}
				}
				
				// Clean up GCS credentials file after a delay
				setTimeout(() => {
					for (const file of req.files) {
						if (file.fieldname === 'gcsCredentials') {
							try {
								fs.unlinkSync(file.path);
								console.log(`Cleaned up GCS credentials file: ${file.path}`);
							} catch (cleanupError) {
								console.warn(`Failed to clean up GCS credentials file ${file.path}:`, cleanupError.message);
							}
						}
					}
				}, 5000); // 5 second delay
			}
		}

	} catch (error) {
		console.error('Job error:', error);
		
		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file after error: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
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
app.post('/sample', upload.array('files'), async (req, res) => {
	try {
		const { credentials, options } = req.body;

		// Parse JSON strings
		const creds = JSON.parse(credentials[0] || '{}');
		const opts = JSON.parse(options[0] || '{}');
		
		// Override any fixData setting from client - must be false for raw preview
		opts.fixData = false;

		// Force sample settings - no transforms, maxRecords=500, dryRun=true
		opts.dryRun = true;
		opts.maxRecords = 500;
		opts.transformFunc = function id(a) { return a; }; // Identity function
		opts.fixData = false; // CRITICAL: Keep raw CSV structure
		opts.removeNulls = false; // Keep raw data as-is
		opts.flattenData = false; // No flattening
		opts.vendor = ''; // No vendor transforms
		opts.fixTime = false; // No time fixing
		opts.addToken = false; // No token addition
		opts.compress = false; // No compression
		opts.strict = false; // No validation
		opts.dedupe = false; // No deduplication
		opts.recordType = ''; // CRITICAL: Remove recordType to prevent CSV->event transformation

		// Process files or cloud paths (same as main endpoint)
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				console.log(`Sample data from cloud storage paths:`, cloudPaths);
				data = cloudPaths;
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: 'Invalid cloud paths format'
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Handle local files - pass file paths to mixpanel-import
			if (req.files.length === 1) {
				// Single file - pass file path
				data = req.files[0].path;
				console.log(`Sampling from single local file: ${data}`);
			} else {
				// Multiple files - pass array of file paths
				data = req.files.map(file => file.path);
				console.log(`Sampling from multiple local files: ${data.join(', ')}`);
			}
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		console.log(`Sampling raw data: up to ${opts.maxRecords} records`);

		// Run the sample
		const result = await mixpanelImport(creds, data, opts);

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
				}
			}
		}

		res.json({
			success: true,
			sampleData: result.dryRun || []
		});

	} catch (error) {
		console.error('Sample error:', error);
		
		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file after error: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
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
app.post('/columns', upload.array('files'), async (req, res) => {
	try {
		const { credentials, options } = req.body;

		// Parse JSON strings
		const creds = JSON.parse(credentials[0] || '{}');
		const opts = JSON.parse(options[0] || '{}');
		
		// Override any fixData setting from client - must be false for column detection
		opts.fixData = false;

		// Force sample settings - let mixpanel-import handle all parsing
		opts.dryRun = true;
		opts.maxRecords = 500; // Sample up to 500 records
		opts.transformFunc = function id(a) { return a; }; // Identity function - no transforms
		opts.fixData = false; // CRITICAL: Keep raw CSV structure - no event/properties shape
		opts.removeNulls = false; // Keep all columns
		opts.flattenData = false; // No flattening
		opts.vendor = ''; // No vendor transforms
		opts.fixTime = false; // No time fixing
		opts.addToken = false; // No token addition
		opts.compress = false; // No compression
		opts.strict = false; // No validation
		opts.dedupe = false; // No deduplication
		opts.recordType = ''; // CRITICAL: Remove recordType to prevent CSV->event transformation

		// Let mixpanel-import handle file parsing - pass raw data directly
		let data;

		// Check if cloud paths were provided
		if (req.body.cloudPaths) {
			try {
				const cloudPaths = JSON.parse(req.body.cloudPaths);
				console.log(`Detecting columns from cloud storage paths:`, cloudPaths);
				// Only use the first file for column detection
				data = Array.isArray(cloudPaths) ? cloudPaths[0] : cloudPaths;
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: 'Invalid cloud paths format'
				});
			}
		} else if (req.files && req.files.length > 0) {
			// Use uploaded file from disk storage
			const uploadedFile = req.files[0];
			console.log(`Detecting columns from uploaded file: ${uploadedFile.originalname}`);
			console.log(`Using file path: ${uploadedFile.path}`);
			
			data = uploadedFile.path; // Pass file path directly
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		console.log(`Running column detection with up to ${opts.maxRecords} records`);

		// Let mixpanel-import handle all parsing and get parsed results from dryRun
		const result = await mixpanelImport(creds, data, opts);
		
		const sampleData = result.dryRun || [];
		console.log(`Got ${sampleData.length} parsed records from mixpanel-import`);

		// Extract unique column names from the parsed dryRun results
		const columnSet = new Set();
		sampleData.forEach((record, index) => {
			if (record && typeof record === 'object') {
				Object.keys(record).forEach(key => columnSet.add(key));
			} else {
				console.log(`Non-object record at index ${index}:`, typeof record, record);
			}
		});

		const columns = Array.from(columnSet).sort();

		console.log(`Detected ${columns.length} unique columns:`, columns);

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
				}
			}
		}

		res.json({
			success: true,
			columns: columns,
			sampleCount: sampleData.length
		});

	} catch (error) {
		console.error('Column detection error:', error);
		
		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file after error: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
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
app.post('/dry-run', upload.array('files'), async (req, res) => {
	try {
		const { credentials, options, transformCode } = req.body;

		// Parse JSON strings
		const creds = JSON.parse(credentials || '{}');

		const opts = JSON.parse(options || '{}');

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
				console.log(`Dry run with cloud storage paths:`, cloudPaths);
				data = cloudPaths; // Pass cloud paths directly to mixpanel-import
			} catch (err) {
				return res.status(400).json({
					success: false,
					error: 'Invalid cloud paths format'
				});
			}
			// @ts-ignore
		} else if (req.files && req.files.length > 0) {
			// Handle local files - pass file paths to mixpanel-import
			if (req.files.length === 1) {
				// Single file - pass file path
				data = req.files[0].path;
				console.log(`Dry run with single local file: ${data}`);
			} else {
				// Multiple files - pass array of file paths
				data = req.files.map(file => file.path);
				console.log(`Dry run with multiple local files: ${data.join(', ')}`);
			}
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		console.log(`Starting dry run with ${Array.isArray(data) ? data.length : 'unknown'} files`);

		// Run raw data fetch first (no transforms) for comparison
		const rawOpts = { ...opts };
		rawOpts.transformFunc = null;
		rawOpts.fixData = false; // CRITICAL: Keep raw CSV structure
		rawOpts.removeNulls = false;
		rawOpts.flattenData = false;
		rawOpts.vendor = '';
		rawOpts.maxRecords = 100; // Match dry run limit

		const rawResult = await mixpanelImport(creds, data, rawOpts);

		// Run the transformed dry run
		const transformedResult = await mixpanelImport(creds, data, opts);

		// Clean up temporary files
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
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
		console.error('Dry run error:', error);
		
		// Clean up temporary files even on error
		if (req.files && req.files.length > 0) {
			for (const file of req.files) {
				try {
					fs.unlinkSync(file.path);
					console.log(`Cleaned up temp file after error: ${file.path}`);
				} catch (cleanupError) {
					console.warn(`Failed to clean up temp file ${file.path}:`, cleanupError.message);
				}
			}
		}
		
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle export operations
// @ts-ignore
app.post('/export', async (req, res) => {
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

		/** @type {Options} */
		const opts = {
			recordType: exportData.recordType,
			region: exportData.region || 'US',
			workers: exportData.workers || 10,
			start: exportData.start,
			end: exportData.end,
			epochStart: exportData.epochStart,
			epochEnd: exportData.epochEnd,
			whereClause: exportData.whereClause,
			limit: exportData.limit,
			logs: exportData.logs || false,
			verbose: exportData.verbose || false,
			showProgress: exportData.showProgress || true,
			writeToFile: exportData.writeToFile || false,
			where: exportData.where,
			outputFilePath: exportData.outputFilePath
		};

		// Generate unique job ID for WebSocket tracking
		const jobId = `export-${Date.now()}-${Math.random().toString(36).substring(2)}`;
		
		// Add progress callback for WebSocket updates
		opts.progressCallback = createProgressCallback(jobId);
		
		console.log(`Starting export operation ${jobId}: ${opts.recordType}`);

		// Return job ID immediately so client can connect WebSocket
		res.json({
			success: true,
			jobId: jobId,
			message: 'Export started - connect WebSocket for progress updates'
		});

		// Run the export asynchronously
		try {
			const result = await mixpanelImport(creds, null, opts);
			console.log(`Export ${jobId} completed: ${result.total} records processed`);
			
			// Signal job completion via WebSocket
			signalJobComplete(jobId, result);
		} catch (exportError) {
			console.error(`Export ${jobId} failed:`, exportError);
			
			// Signal job failure via WebSocket
			const jobData = activeJobs.get(jobId);
			if (jobData && jobData.ws.readyState === WebSocket.OPEN) {
				try {
					jobData.ws.send(JSON.stringify({
						type: 'job-error',
						jobId: jobId,
						error: exportError.message,
						timestamp: Date.now()
					}));
				} catch (wsError) {
					console.error(`Failed to send error to export ${jobId}:`, wsError);
				}
			}
			activeJobs.delete(jobId);
		}

	} catch (error) {
		console.error('Export error:', error);
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Handle export dry run
// @ts-ignore
app.post('/export-dry-run', async (req, res) => {
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
			region: exportData.region || 'US',
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

		console.log(`Starting export dry run: ${opts.recordType}`);

		// Run the dry run export
		const result = await mixpanelImport(creds, null, opts);

		console.log(`Export dry run completed: ${result.total} records would be exported`);

		res.json({
			success: true,
			result,
			previewData: result.dryRun || []
		});

	} catch (error) {
		console.error('Export dry run error:', error);
		res.status(500).json({
			success: false,
			error: error.message
		});
	}
});

// Health check
app.get('/health', (req, res) => {
	res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
function startUI(options = {}) {
	const serverPort = options.port || port;
	const { version } = require('../package.json');

	// ASCII Art Logo (same as CLI)
	const hero = String.raw`
_  _ _ _  _ ___  ____ _  _ ____ _       _ _  _ ___  ____ ____ ___ 
|\/| |  \/  |__] |__| |\ | |___ |       | |\/| |__] |  | |__/  |  
|  | | _/\_ |    |  | | \| |___ |___    | |  | |    |__| |  \  |                                                                    
`;

	const banner = `... streamer of data... to mixpanel! (v${version || 2})
\tby AK (ak@mixpanel.com)`;

	return new Promise((resolve, reject) => {
		server.listen(serverPort, (err) => {
			if (err) {
				reject(err);
			} else {
				// Show CLI logo first
				console.log(hero);
				console.log(banner);
				console.log(`\n🚀 UI running at http://localhost:${serverPort}`);
				console.log(`📡 WebSocket server alive`);
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