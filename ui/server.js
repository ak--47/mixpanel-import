const express = require('express');
const path = require('path');
const multer = require('multer');
const mixpanelImport = require('../index.js');

const app = express();
const port = process.env.PORT || 3000;

// Configure multer for file uploads (store in memory)
const upload = multer({
	storage: multer.memoryStorage(),
	limits: {
		fileSize: 1000 * 1024 * 1024 // 1GB limit
	}
});

/** @typedef {import('../index.d.ts').Options} Options */
/** @typedef {import('../index.d.ts').Creds} Creds */
/** @typedef {import('../index.d.ts').Data} Data */


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
			// Handle local files
			if (req.files.length === 1) {
				// Single file - convert buffer to JSON
				const fileContent = req.files[0].buffer.toString('utf8');
				try {
					data = JSON.parse(fileContent);
				} catch (err) {
					// Try parsing as JSONL
					data = fileContent.trim().split('\n').map(line => JSON.parse(line));
				}
			} else {
				// Multiple files - combine into array
				data = [];
				// @ts-ignore
				for (const file of req.files) {
					const fileContent = file.buffer.toString('utf8');
					try {
						const fileData = JSON.parse(fileContent);
						data = data.concat(Array.isArray(fileData) ? fileData : [fileData]);
					} catch (err) {
						// Try parsing as JSONL
						const jsonlData = fileContent.trim().split('\n').map(line => JSON.parse(line));
						data = data.concat(jsonlData);
					}
				}
			}
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		console.log(`Starting import job with ${Array.isArray(data) ? data.length : 'unknown'} records`);

		// Run the import
		const result = await mixpanelImport(creds, data, opts);
		const { total, success, failed, empty } = result;
		console.log(`Import job completed: ${total} records | ${success} success | ${failed} failed | ${empty} skipped`);

		res.json({
			success: true,
			result
		});

	} catch (error) {
		console.error('Job error:', error);
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

		// Force sample settings - no transforms, maxRecords=500, dryRun=true
		opts.dryRun = true;
		opts.maxRecords = 500;
		opts.transformFunc = function id(a) { return a; }; // Identity function
		opts.fixData = false; // No data shaping
		opts.removeNulls = false; // Keep raw data as-is
		opts.flattenData = false; // No flattening
		opts.vendor = ''; // No vendor transforms
		opts.fixTime = false; // No time fixing
		opts.addToken = false; // No token addition
		opts.compress = false; // No compression
		opts.strict = false; // No validation
		opts.dedupe = false; // No deduplication

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
			if (req.files.length === 1) {
				const fileContent = req.files[0].buffer.toString('utf8');
				try {
					data = JSON.parse(fileContent);
				} catch (err) {
					// Try parsing as JSONL
					data = fileContent.trim().split('\n').map(line => JSON.parse(line));
				}
			} else {
				// Multiple files - combine into array
				data = [];
				// @ts-ignore
				for (const file of req.files) {
					const fileContent = file.buffer.toString('utf8');
					try {
						const fileData = JSON.parse(fileContent);
						data = data.concat(Array.isArray(fileData) ? fileData : [fileData]);
					} catch (err) {
						// Try parsing as JSONL
						const jsonlData = fileContent.trim().split('\n').map(line => JSON.parse(line));
						data = data.concat(jsonlData);
					}
				}
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

		res.json({
			success: true,
			sampleData: result.dryRun || []
		});

	} catch (error) {
		console.error('Sample error:', error);
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
			if (req.files.length === 1) {
				const fileContent = req.files[0].buffer.toString('utf8');
				try {
					data = JSON.parse(fileContent);
				} catch (err) {
					// Try parsing as JSONL
					data = fileContent.trim().split('\n').map(line => JSON.parse(line));
				}
			} else {
				// Multiple files - combine into array
				data = [];
				// @ts-ignore
				for (const file of req.files) {
					const fileContent = file.buffer.toString('utf8');
					try {
						const fileData = JSON.parse(fileContent);
						data = data.concat(Array.isArray(fileData) ? fileData : [fileData]);
					} catch (err) {
						// Try parsing as JSONL
						const jsonlData = fileContent.trim().split('\n').map(line => JSON.parse(line));
						data = data.concat(jsonlData);
					}
				}
			}
		} else {
			return res.status(400).json({
				success: false,
				error: 'No files or cloud paths provided'
			});
		}

		console.log(`Starting dry run with ${Array.isArray(data) ? data.length : 'unknown'} records`);

		// Run raw data fetch first (no transforms) for comparison
		const rawOpts = { ...opts };
		rawOpts.transformFunc = null;
		rawOpts.fixData = false;
		rawOpts.removeNulls = false;
		rawOpts.flattenData = false;
		rawOpts.vendor = '';
		rawOpts.maxRecords = 100; // Match dry run limit

		const rawResult = await mixpanelImport(creds, data, rawOpts);

		// Run the transformed dry run
		const transformedResult = await mixpanelImport(creds, data, opts);

		res.json({
			success: true,
			result: transformedResult,
			previewData: transformedResult.dryRun || [],
			rawData: rawResult.dryRun || []
		});

	} catch (error) {
		console.error('Dry run error:', error);
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

		console.log(`Starting export operation: ${opts.recordType}`);

		// Run the export - note that for exports, data parameter can be null/empty
		const result = await mixpanelImport(creds, null, opts);

		console.log(`Export completed: ${result.total} records processed`);

		// Check if we should stream the file back or just return results
		if (opts.writeToFile && opts.outputFilePath) {
			// Try to read and stream the file back to client
			try {
				const fs = require('fs');
				const path = require('path');

				if (fs.existsSync(opts.outputFilePath)) {
					// Set appropriate headers for file download
					const filename = path.basename(opts.outputFilePath);
					res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
					res.setHeader('Content-Type', 'application/json');

					// Stream the file
					const fileStream = fs.createReadStream(opts.outputFilePath);
					fileStream.pipe(res);

					// Clean up file after streaming (optional - comment out if you want to keep files)
					fileStream.on('end', () => {
						setTimeout(() => {
							try {
								fs.unlinkSync(opts.outputFilePath);
								console.log(`Cleaned up temporary file: ${opts.outputFilePath}`);
							} catch (cleanupError) {
								console.warn(`Failed to clean up file ${opts.outputFilePath}:`, cleanupError.message);
							}
						}, 1000);
					});

					return; // Don't send JSON response
				}
			} catch (fileError) {
				console.warn('File handling error:', fileError.message);
				// Fall through to JSON response
			}
		}

		// Return JSON result if no file streaming
		res.json({
			success: true,
			result
		});

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
		const server = app.listen(serverPort, (err) => {
			if (err) {
				reject(err);
			} else {
				// Show CLI logo first
				console.log(hero);
				console.log(banner);
				console.log(`\n🚀 UI running at http://localhost:${serverPort}`);
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