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

// Middleware
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ extended: true, limit: '100mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// Serve the main UI
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Handle job submission
// @ts-ignore
app.post('/job', upload.array('files'), async (req, res) => {
  try {
    const { credentials, options, transformCode } = req.body;
    
    // Parse JSON strings
    const creds = JSON.parse(credentials || '{}');
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

// Handle dry run
// @ts-ignore
app.post('/dry-run', upload.array('files'), async (req, res) => {
  try {
    const { credentials, options, transformCode } = req.body;
    
    // Parse JSON strings
    const creds = JSON.parse(credentials || '{}');
    const opts = JSON.parse(options || '{}');
    
    // Force dry run
    opts.dryRun = true;
    opts.recordsPerBatch = Math.min(opts.recordsPerBatch || 2000, 10); // Limit preview to 10 records
    
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
          // If array, take only first 10 for preview
          if (Array.isArray(data)) {
            data = data.slice(0, 10);
          }
        } catch (err) {
          const jsonlData = fileContent.trim().split('\n').map(line => JSON.parse(line));
          data = jsonlData.slice(0, 10);
        }
      } else {
        data = [];
        let recordCount = 0;
        // @ts-ignore
        for (const file of req.files) {
          if (recordCount >= 10) break;
          const fileContent = file.buffer.toString('utf8');
          try {
            const fileData = JSON.parse(fileContent);
            const fileArray = Array.isArray(fileData) ? fileData : [fileData];
            data = data.concat(fileArray.slice(0, 10 - recordCount));
            recordCount = data.length;
          } catch (err) {
            const jsonlData = fileContent.trim().split('\n').map(line => JSON.parse(line));
            data = data.concat(jsonlData.slice(0, 10 - recordCount));
            recordCount = data.length;
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
    
    // Run the dry run
    const result = await mixpanelImport(creds, data, opts);
    
    res.json({
      success: true,
      result,
      previewData: result.dryRun || []
    });
    
  } catch (error) {
    console.error('Dry run error:', error);
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
  
  return new Promise((resolve, reject) => {
    const server = app.listen(serverPort, (err) => {
      if (err) {
        reject(err);
      } else {
        console.log(`🚀 Mixpanel Import UI running at http://localhost:${serverPort}`);
        console.log('📁 Drop files, configure options, and import data!');
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