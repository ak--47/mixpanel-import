// @ts-nocheck
/* eslint-env browser */
/* global Dropzone, monaco */


// Mixpanel Import UI Application
class MixpanelImportUI {
	constructor() {
		this.files = [];
		this.editor = null;
		this.sampleData = []; // Store up to 500 sample records for preview
		this.detectedColumns = []; // Store columns detected from data
		this.columnMappings = {}; // Store user-defined column mappings
		this.websocket = null; // WebSocket connection for real-time progress
		this.currentJobId = null; // Track current job ID
		this.lastResults = null; // Store last results for download
		this.initializeUI();
		this.setupEventListeners();
		this.updateColumnMapperButtons(); // Set initial button state
		this.initializeMonacoEditor();
		this.initializeETLCycling();
	}

	// WebSocket connection methods
	connectWebSocket(jobId) {
		try {
			const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
			const wsUrl = `${protocol}//${window.location.host}`;
			
			this.websocket = new WebSocket(wsUrl);
			this.currentJobId = jobId;
			
			this.websocket.onopen = () => {
				// Register this connection with the job
				this.websocket.send(JSON.stringify({
					type: 'register-job',
					jobId: jobId
				}));
			};
			
			this.websocket.onmessage = (event) => {
				try {
					const data = JSON.parse(event.data);
					this.handleWebSocketMessage(data);
				} catch (error) {
					console.error('Failed to parse WebSocket message:', error);
				}
			};
			
			this.websocket.onerror = (error) => {
				console.error('WebSocket error:', error);
			};
			
			this.websocket.onclose = () => {
				this.websocket = null;
				this.currentJobId = null;
			};
			
		} catch (error) {
			console.error('Failed to connect WebSocket:', error);
		}
	}
	
	disconnectWebSocket() {
		if (this.websocket) {
			this.websocket.close();
			this.websocket = null;
			this.currentJobId = null;
		}
	}
	
	handleWebSocketMessage(data) {
		if (data.jobId !== this.currentJobId) {
			return; // Ignore messages for other jobs
		}
		
		switch (data.type) {
			case 'job-registered':
				break;
				
			case 'progress':
				this.updateProgressDisplay(data.data);
				break;
				
			case 'job-complete':
				this.hideLoading();
				// Clear any previous results first
				this.clearResults();
				this.showResults(data.result, false);
				this.disconnectWebSocket();
				break;
				
			case 'job-error':
				console.error('Job failed:', data.error);
				this.hideLoading();
				this.showError(`Import failed: ${data.error}`);
				this.disconnectWebSocket();
				break;
				
			default:
				// Unknown message type - ignore silently
		}
	}
	
	updateProgressDisplay(progressData) {
		// Update the loading message with real-time progress
		const loadingMessage = document.querySelector('.loading-details');
		if (loadingMessage) {
			const { recordType, processed, requests, eps, memory, bytesProcessed } = progressData;
			
			const formatNumber = (num) => {
				if (typeof num === 'number') {
					return num.toLocaleString();
				}
				return num || '0';
			};
			
			const formatBytes = (bytes) => {
				if (!bytes || bytes === 0) return '0 B';
				const k = 1024;
				const sizes = ['B', 'KB', 'MB', 'GB'];
				const i = Math.floor(Math.log(bytes) / Math.log(k));
				return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
			};
			
			loadingMessage.innerHTML = `
				<div class="progress-stats">
					<div class="stat-item">
						<span class="stat-label">${recordType || 'Records'}:</span>
						<span class="stat-value">${formatNumber(processed)}</span>
					</div>
					${requests ? `
					<div class="stat-item">
						<span class="stat-label">Requests:</span>
						<span class="stat-value">${formatNumber(requests)}</span>
					</div>` : ''}
					${eps ? `
					<div class="stat-item">
						<span class="stat-label">Events/sec:</span>
						<span class="stat-value">${eps}</span>
					</div>` : ''}
					${memory ? `
					<div class="stat-item">
						<span class="stat-label">Memory:</span>
						<span class="stat-value">${formatBytes(memory)}</span>
					</div>` : ''}
					${bytesProcessed ? `
					<div class="stat-item">
						<span class="stat-label">Processed:</span>
						<span class="stat-value">${formatBytes(bytesProcessed)}</span>
					</div>` : ''}
				</div>
			`;
		}
	}

	// Helper method for safe element access
	getElement(id) {
		return document.getElementById(id);
	}

	// Helper method to get element value safely
	getElementValue(id, defaultValue = '') {
		const el = this.getElement(id);
		return el ? el.value : defaultValue;
	}

	// Helper method to get element checked state safely
	getElementChecked(id) {
		const el = this.getElement(id);
		return el ? el.checked : false;
	}

	// Fill form with development values for quick testing
	fillDevValues() {
		try {
			// Set record type to event first (to show credentials section)
			const recordTypeSelect = document.getElementById('recordType');
			if (recordTypeSelect) {
				recordTypeSelect.value = 'event';
				recordTypeSelect.dispatchEvent(new Event('change'));
			}

			// Set file source to GCS
			const gcsRadio = document.querySelector('input[name="fileSource"][value="gcs"]');
			if (gcsRadio) {
				gcsRadio.checked = true;
				gcsRadio.dispatchEvent(new Event('change'));
			}

			// Fill GCS path
			const gcsPathsInput = document.getElementById('gcsPaths');
			if (gcsPathsInput) {
				// gcsPathsInput.value = 'gs://mixpanel-import-public-data/example-dnd-events.json';
				gcsPathsInput.value = 'gs://mixpanel-import-public-data/twofifty-k-events.json.gz';
				gcsPathsInput.dispatchEvent(new Event('input'));
			}

			// Fill project token
			const tokenInput = document.getElementById('token');
			if (tokenInput) {
				tokenInput.value = '921270447fc5f98015b04a1b95d23572';
			}

			// Enable show progress
			const showProgressCheckbox = document.getElementById('showProgress');
			if (showProgressCheckbox) {
				showProgressCheckbox.checked = true;
			}

			console.log('Dev values filled successfully');
		} catch (error) {
			console.error('Failed to fill dev values:', error);
		}
	}

	resetForm() {
		try {
			// Reset the entire form
			const form = document.getElementById('importForm');
			if (form) {
				form.reset();
			}

			// Reset file source to local
			const localRadio = document.querySelector('input[name="fileSource"][value="local"]');
			if (localRadio) {
				localRadio.checked = true;
				localRadio.dispatchEvent(new Event('change'));
			}

			// Clear dropzone files
			if (this.dropzone) {
				this.dropzone.removeAllFiles();
			}

			// Clear sample data
			this.sampleData = [];
			this.currentSampleIndex = 0;

			// Reset record type to event (default)
			const recordTypeSelect = document.getElementById('recordType');
			if (recordTypeSelect) {
				recordTypeSelect.value = 'event';
				recordTypeSelect.dispatchEvent(new Event('change'));
			}

			// Clear Monaco editor if it exists
			if (this.editor) {
				this.editor.setValue(this.getDefaultTransformFunction());
			}

			// Hide preview and results sections
			const dataPreview = document.getElementById('data-preview');
			if (dataPreview) dataPreview.style.display = 'none';

			const results = document.getElementById('results');
			if (results) results.style.display = 'none';

			const loading = document.getElementById('loading');
			if (loading) loading.style.display = 'none';

			// Clear column mapper
			this.detectedColumns = [];
			this.columnMappings = {};
			this.updateColumnMapperButtons();

			// Update CLI command
			this.updateCLICommand();

			console.log('Form reset successfully');
		} catch (error) {
			console.error('Failed to reset form:', error);
		}
	}

	initializeUI() {
		// Initialize Dropzone (with safety check)
		if (typeof Dropzone !== 'undefined') {
			Dropzone.autoDiscover = false;
			this.dropzone = new Dropzone('#file-dropzone', {
				url: '/upload', // This won't be used, we handle manually
				autoProcessQueue: false,
				clickable: '#file-dropzone', // Make entire dropzone clickable
				dictDefaultMessage: `
					<div class="dropzone-message">
						<span class="dropzone-icon">⬆️</span>
						<p>Drop files here or click to browse</p>
						<small>JSON, JSONL, CSV, Parquet supported</small>
					</div>
				`,
				previewsContainer: false,
				createImageThumbnails: false,
				init: function () {
					this.on('addedfile', (file) => {
						window.app.addFile(file);
					});
				}
			});
		} else {
			console.warn('Dropzone not loaded, falling back to basic file input');
			this.setupBasicFileInput();
		}
	}

	initializeETLCycling() {
		// Separate word banks for E, T, and L
		const eWords = [
			'Extract', 'Enrich', 'Evaluate', 'Export', 'Execute',
			'Expand', 'Elevate', 'Edge', 'Enterprise', 'Endpoint',
			'Entity', 'Engine', 'Event', 'Elasticsearch', 'Encode',
			'Encrypt', 'Enhance', 'Establish', 'Evolve', 'Examine',
			'Explore', 'Express', 'Extend', 'Embed', 'Enable',
			'Enforce', 'Engage', 'Ensure', 'Enumerate', 'Equalize',
			'Estimate', 'Evoke', 'Exceed', 'Exchange', 'Exclude',
			'Exemplify', 'Exhaust', 'Exhibit', 'Expedite', 'Experiment',
			'Exploit', 'Expose', 'Externalize', 'Extrapolate', 'Extrude',
			'Elaborate', 'Elect', 'Eliminate', 'Elucidate', 'Emanate',
			'Embrace', 'Emerge', 'Emit', 'Emphasize', 'Employ',
			'Empower', 'Emulate', 'Encapsulate', 'Encompass', 'Encounter',
			'Energize', 'Engineer', 'Engrave', 'Enjoy', 'Enlarge',
			'Enlighten', 'Enlist', 'Enqueue', 'Entangle', 'Enter',
			'Entertain', 'Entice', 'Entrench', 'Entrust', 'Envelop',
			'Envision', 'Epitomize', 'Equip', 'Eradicate', 'Erect',
			'Escalate', 'Escape', 'Escort', 'Etch', 'Evaporate',
			'Evict', 'Evidence', 'Exacerbate', 'Exalt', 'Excavate'
		];

		const tWords = [
			'Transform', 'Transfer', 'Transpose', 'Transact', 'Translate',
			'Traverse', 'Track', 'Trace', 'Train', 'Transmit',
			'Transport', 'Transpile', 'Transcribe', 'Transcend', 'Transition',
			'Transmute', 'Transplant', 'Trap', 'Treat', 'Trend',
			'Triage', 'Trigger', 'Trim', 'Triple', 'Troubleshoot',
			'Truncate', 'Trust', 'Tune', 'Tunnel', 'Turn',
			'Twist', 'Type', 'Typeset', 'Tackle', 'Tag',
			'Tail', 'Tailor', 'Take', 'Talk', 'Tally',
			'Tame', 'Tap', 'Target', 'Task', 'Taste',
			'Teach', 'Team', 'Tear', 'Tease', 'Teleport',
			'Tell', 'Temper', 'Template', 'Tempt', 'Tend',
			'Terminate', 'Terraform', 'Test', 'Tether', 'Text',
			'Thank', 'Thaw', 'Theme', 'Theorize', 'Thin',
			'Think', 'Thread', 'Threaten', 'Thrive', 'Throttle',
			'Throw', 'Thrust', 'Thwart', 'Tick', 'Tickle',
			'Tide', 'Tidy', 'Tie', 'Tighten', 'Tilt',
			'Time', 'Tint', 'Tip', 'Title', 'Toast',
			'Toggle', 'Tokenize', 'Tolerate', 'Tool', 'Top',
			'Topple', 'Torch', 'Torque', 'Toss', 'Total',
			'Touch', 'Tour', 'Tow', 'Toy', 'Traceability'
		];

		const lWords = [
			'Load', 'Launch', 'Leverage', 'Lift', 'Logic',
			'Library', 'Layer', 'Lake', 'Logstash', 'Link',
			'List', 'Listen', 'Locate', 'Lock', 'Log',
			'Loop', 'Latch', 'Learn', 'Lease', 'Leave',
			'Lecture', 'Ledger', 'Legitimize', 'Lend', 'Lengthen',
			'Lesson', 'Let', 'Level', 'Levy', 'Liberate',
			'License', 'Lick', 'Lie', 'Lighten', 'Like',
			'Limit', 'Line', 'Linger', 'Liquidate', 'Liquefy',
			'List', 'Literalize', 'Litigate', 'Litter', 'Live',
			'Livestream', 'Lobby', 'Localize', 'Lodge', 'Loft',
			'Loiter', 'Look', 'Loom', 'Loosen', 'Loot',
			'Lose', 'Lounge', 'Love', 'Lower', 'Lubricate',
			'Lucid', 'Lug', 'Lull', 'Lumber', 'Lump',
			'Lunge', 'Lure', 'Lurk', 'Lust', 'Luxuriate',
			'Label', 'Labor', 'Lace', 'Lack', 'Ladder',
			'Ladle', 'Lag', 'Lament', 'Laminate', 'Land',
			'Landscape', 'Language', 'Languish', 'Lap', 'Lapse',
			'Lard', 'Large', 'Lash', 'Last', 'Lather',
			'Laud', 'Laugh', 'Launder', 'Lavish', 'Law',
			'Lay', 'Lazy', 'Lead', 'Leaf', 'Leak',
			'Lean', 'Leap', 'Lease', 'Leash', 'Leather'
		];

		// Store previously used combinations to avoid immediate repeats
		const recentCombos = [];
		const maxRecent = 20; // Remember last 20 combinations

		const descriptionElement = document.getElementById('cute-description');

		if (!descriptionElement) return;

		// Function to generate a random combination
		const generateRandomETL = () => {
			let combo;
			let attempts = 0;
			const maxAttempts = 50;

			do {
				const e = eWords[Math.floor(Math.random() * eWords.length)];
				const t = tWords[Math.floor(Math.random() * tWords.length)];
				const l = lWords[Math.floor(Math.random() * lWords.length)];
				combo = `${e} ${t} ${l}`;
				attempts++;
			} while (recentCombos.includes(combo) && attempts < maxAttempts);

			// Add to recent combos and maintain size limit
			recentCombos.push(combo);
			if (recentCombos.length > maxRecent) {
				recentCombos.shift();
			}

			return combo;
		};

		// Function to cycle descriptions
		const cycleDescription = () => {
			// Add fading class
			descriptionElement.classList.add('fading');

			// After fade out, change text and fade back in
			setTimeout(() => {
				const newCombo = generateRandomETL();
				descriptionElement.textContent = newCombo;
				descriptionElement.classList.remove('fading');

			}, 250); // Half of the transition time
		};

		// Set initial random combination
		descriptionElement.textContent = generateRandomETL();

		// Start cycling every 10 seconds
		setInterval(cycleDescription, 10000);

		// Optional: Add click handler for manual cycling
		descriptionElement.style.cursor = 'pointer';
		descriptionElement.title = 'Click for new combination';
		descriptionElement.addEventListener('click', cycleDescription);
	}

	setupBasicFileInput() {
		// Fallback: create a basic file input if Dropzone fails
		const dropzoneElement = document.getElementById('file-dropzone');

		// Create file input
		const fileInput = document.createElement('input');
		fileInput.type = 'file';
		fileInput.multiple = true;
		fileInput.accept = '.json,.jsonl,.csv,.parquet,.gz';
		fileInput.style.display = 'none';

		// Handle file selection
		fileInput.addEventListener('change', (e) => {
			Array.from(e.target.files).forEach(file => {
				this.addFile(file);
			});
		});

		// Make dropzone clickable
		dropzoneElement.addEventListener('click', () => {
			fileInput.click();
		});

		// Handle drag and drop manually
		dropzoneElement.addEventListener('dragover', (e) => {
			e.preventDefault();
			dropzoneElement.classList.add('dragover');
		});

		dropzoneElement.addEventListener('dragleave', (e) => {
			e.preventDefault();
			dropzoneElement.classList.remove('dragover');
		});

		dropzoneElement.addEventListener('drop', (e) => {
			e.preventDefault();
			dropzoneElement.classList.remove('dragover');

			Array.from(e.dataTransfer.files).forEach(file => {
				this.addFile(file);
			});
		});

		// Append hidden input to body
		document.body.appendChild(fileInput);
	}

	addFile(file) {
		this.files.push(file);
		this.updateFileList();
	}

	removeFile(index) {
		this.files.splice(index, 1);
		this.updateFileList();
	}

	updateFileList() {
		const fileList = document.getElementById('file-list');
		const columnMapperSection = document.getElementById('column-mapper-section');

		if (this.files.length === 0) {
			fileList.innerHTML = '';
			if (columnMapperSection) columnMapperSection.style.display = 'none';
			return;
		}

		fileList.innerHTML = this.files.map((file, index) => `
            <div class="file-item">
                <span class="file-name">📄 ${file.name}</span>
                <span class="file-size">${this.formatFileSize(file.size)}</span>
                <button type="button" class="file-remove" onclick="window.app.removeFile(${index})">✕</button>
            </div>
        `).join('');

		// Show column mapper section when files are added
		if (columnMapperSection) columnMapperSection.style.display = 'block';
	}

	formatFileSize(bytes) {
		if (bytes === 0) return '0 Bytes';
		const k = 1024;
		const sizes = ['Bytes', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
	}

	setupEventListeners() {
		// File source toggle
		const fileSourceRadios = document.querySelectorAll('input[name="fileSource"]');
		fileSourceRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleFileSource.bind(this));
		});

		// Cloud paths input for GCS
		const gcsPathsInput = document.getElementById('gcsPaths');
		if (gcsPathsInput) {
			gcsPathsInput.addEventListener('input', this.updateCloudFilePreview.bind(this));
		}

		// Cloud paths input for S3
		const s3PathsInput = document.getElementById('s3Paths');
		if (s3PathsInput) {
			s3PathsInput.addEventListener('input', this.updateCloudFilePreview.bind(this));
		}

		// Advanced options toggle (only if element exists)
		const advancedToggle = document.getElementById('showAdvanced');
		if (advancedToggle) {
			advancedToggle.addEventListener('change', (e) => {
				const advancedSection = document.getElementById('advanced-options');
				if (advancedSection) {
					advancedSection.style.display = e.target.checked ? 'block' : 'none';
				}
			});
		}

		// Form submission
		const form = document.getElementById('importForm');
		form.addEventListener('submit', (e) => {
			e.preventDefault();
			this.submitJob();
		});

		// Dry run button
		const dryRunBtn = document.getElementById('dry-run-btn');
		dryRunBtn.addEventListener('click', () => {
			this.submitJob(true);
		});

		// Preview Data button - shows raw data before transforms
		const previewDataBtn = document.getElementById('preview-data-btn');
		if (previewDataBtn) {
			previewDataBtn.addEventListener('click', () => {
				this.previewRawData();
			});
		}

		// See More button - shows different random 5 records from sample
		const seeMoreBtn = document.getElementById('see-more-btn');
		if (seeMoreBtn) {
			seeMoreBtn.addEventListener('click', () => {
				this.showMorePreviewRecords();
			});
		}

		// Column mapper button - detect columns from data
		const detectColumnsBtn = document.getElementById('detect-columns-btn');
		if (detectColumnsBtn) {
			detectColumnsBtn.addEventListener('click', () => {
				this.detectColumns();
			});
		}

		// Column mapper - preview data first button
		const previewDataFromMapperBtn = document.getElementById('preview-data-from-mapper-btn');
		if (previewDataFromMapperBtn) {
			previewDataFromMapperBtn.addEventListener('click', () => {
				// Trigger the main preview button
				const mainPreviewBtn = document.getElementById('preview-data-btn');
				if (mainPreviewBtn) {
					mainPreviewBtn.click();
				}
			});
		}

		// Clear transform button - reset to default with helpful template
		const clearBtn = document.getElementById('clear-transform');
		clearBtn.addEventListener('click', () => {
			if (this.editor) {
				this.editor.setValue(this.getDefaultTransformFunction());
			}
		});

		// Record type change - show/hide relevant fields
		const recordTypeSelect = document.getElementById('recordType');
		recordTypeSelect.addEventListener('change', this.updateFieldVisibility.bind(this));
		recordTypeSelect.addEventListener('change', this.refreshColumnMapper.bind(this));
		this.updateFieldVisibility(); // Initial call

		// CLI command copy button
		const copyCliBtn = document.getElementById('copy-cli');
		copyCliBtn.addEventListener('click', this.copyCLICommand.bind(this));

		// Download results button
		const downloadResultsBtn = document.getElementById('download-results-btn');
		if (downloadResultsBtn) {
			downloadResultsBtn.addEventListener('click', this.downloadResults.bind(this));
		}

		// Update CLI command when form changes
		form.addEventListener('input', this.updateCLICommand.bind(this));
		form.addEventListener('change', this.updateCLICommand.bind(this));

		// Clean up WebSocket connection when page is unloaded
		window.addEventListener('beforeunload', () => {
			this.disconnectWebSocket();
		});

		// Dev key button for quick form filling
		const devKeyBtn = document.getElementById('dev-key-btn');
		if (devKeyBtn) {
			devKeyBtn.addEventListener('click', this.fillDevValues.bind(this));
		}

		// Reset button to clear form
		const resetBtn = document.getElementById('reset-btn');
		if (resetBtn) {
			resetBtn.addEventListener('click', this.resetForm.bind(this));
		}
	}


	toggleFileSource() {
		const fileSource = document.querySelector('input[name="fileSource"]:checked').value;
		const localUpload = document.getElementById('local-upload');
		const gcsUpload = document.getElementById('gcs-upload');
		const s3Upload = document.getElementById('s3-upload');
		const s3Credentials = document.getElementById('s3-credentials');
		const gcsCredentials = document.getElementById('gcs-credentials');

		// Clear context when switching import sources
		this.clearImportContext();

		// Hide all upload sections first
		localUpload.style.display = 'none';
		gcsUpload.style.display = 'none';
		s3Upload.style.display = 'none';
		s3Credentials.style.display = 'none';
		gcsCredentials.style.display = 'none';

		// Show the selected upload section
		if (fileSource === 'local') {
			localUpload.style.display = 'block';
		} else if (fileSource === 'gcs') {
			gcsUpload.style.display = 'block';
			gcsCredentials.style.display = 'block';
		} else if (fileSource === 's3') {
			s3Upload.style.display = 'block';
			s3Credentials.style.display = 'block';
		}

		// Update CLI command when file source changes
		this.updateCLICommand();
	}

	updateCloudFilePreview() {
		const fileSource = document.querySelector('input[name="fileSource"]:checked')?.value;
		let cloudPathsEl, preview, expectedPrefix;

		if (fileSource === 'gcs') {
			cloudPathsEl = document.getElementById('gcsPaths');
			preview = document.getElementById('gcs-file-list');
			expectedPrefix = 'gs://';
		} else if (fileSource === 's3') {
			cloudPathsEl = document.getElementById('s3Paths');
			preview = document.getElementById('s3-file-list');
			expectedPrefix = 's3://';
		} else {
			return; // Not a cloud file source
		}

		if (!cloudPathsEl || !preview) return;

		const cloudPaths = cloudPathsEl.value;
		const columnMapperSection = document.getElementById('column-mapper-section');
		
		if (!cloudPaths.trim()) {
			preview.innerHTML = '';
			if (columnMapperSection) columnMapperSection.style.display = 'none';
			return;
		}

		const paths = cloudPaths.split(/[,\n]/).map(p => p.trim()).filter(p => p);
		const previewHTML = paths.map(path => {
			const isValid = path.startsWith(expectedPrefix);
			return `<span class="cloud-path${isValid ? '' : ' invalid'}">${path}</span>`;
		}).join('');

		preview.innerHTML = previewHTML;

		// Show column mapper section when cloud paths are entered
		if (columnMapperSection && paths.length > 0) {
			columnMapperSection.style.display = 'block';
		}

		// Update CLI command when cloud paths change
		this.updateCLICommand();
	}

	// Clear all import context when switching sources
	clearImportContext() {
		// Clear files
		this.files = [];
		this.updateFileList();
		
		// Clear cloud paths
		const gcsPathsInput = document.getElementById('gcsPaths');
		const s3PathsInput = document.getElementById('s3Paths');
		if (gcsPathsInput) gcsPathsInput.value = '';
		if (s3PathsInput) s3PathsInput.value = '';
		
		// Clear previews
		const gcsPreview = document.getElementById('gcs-file-list');
		const s3Preview = document.getElementById('s3-file-list');
		if (gcsPreview) gcsPreview.innerHTML = '';
		if (s3Preview) s3Preview.innerHTML = '';
		
		// Clear sample data and hide preview
		this.sampleData = [];
		const dataPreview = document.getElementById('data-preview');
		if (dataPreview) dataPreview.style.display = 'none';
		
		// Clear column mappings
		this.detectedColumns = [];
		this.columnMappings = {};
		const columnMapperSection = document.getElementById('column-mapper-section');
		if (columnMapperSection) columnMapperSection.style.display = 'none';
	}

	updateFieldVisibility() {
		const recordType = document.getElementById('recordType').value;

		// Hide all groups initially
		const allGroups = [
			'project-group', 'lookupTableId-group', 'token-group', 'groupKey-group',
			'dataGroupId-group', 'secondToken-group', 'auth-toggle', 'service-auth', 'secret-auth'
		];
		allGroups.forEach(groupId => {
			const element = document.getElementById(groupId);
			if (element) element.style.display = 'none';
		});

		// Define authentication requirements based on RecordType
		switch (recordType) {
			case 'event':
			case 'user':
				// Only project token is required
				document.getElementById('token-group').style.display = 'block';
				break;

			case 'group':
				// Project token + groupKey is required
				document.getElementById('token-group').style.display = 'block';
				document.getElementById('groupKey-group').style.display = 'block';
				break;
		}

		// Update CLI command after field visibility changes
		this.updateCLICommand();
	}

	validateRequiredFields(recordType) {
		switch (recordType) {
			case 'event':
			case 'user': {
				// Only project token is required
				const token = document.getElementById('token').value;
				if (!token) {
					return { isValid: false, message: 'Project token is required for events and user profiles.' };
				}
				break;
			}

			case 'group': {
				// Project token + groupKey is required
				const groupToken = document.getElementById('token').value;
				const groupKey = document.getElementById('groupKey').value;
				if (!groupToken) {
					return { isValid: false, message: 'Project token is required for group profiles.' };
				}
				if (!groupKey) {
					return { isValid: false, message: 'Group key is required for group profiles.' };
				}
				break;
			}





			default:
				return { isValid: false, message: 'Please select a valid import type.' };
		}

		return { isValid: true };
	}

	updateCLICommand() {
		const cliElement = document.getElementById('cli-command');

		try {
			const recordType = document.getElementById('recordType').value;
			if (!recordType) {
				cliElement.textContent = 'Select an import type to generate CLI command...';
				cliElement.classList.add('empty');
				return;
			}

			let command = 'npx mixpanel-import';

			// File source
			const fileSource = document.querySelector('input[name="fileSource"]:checked').value;
			if (fileSource === 'local') {
				command += ' ./your-data-file.json';
			} else if (fileSource === 'gcs') {
				const gcsPathsEl = document.getElementById('gcsPaths');
				const gcsPaths = gcsPathsEl ? gcsPathsEl.value : '';
				if (gcsPaths.trim()) {
					const firstPath = gcsPaths.split(/[,\n]/)[0].trim();
					if (firstPath) command += ` "${firstPath}"`;
				} else {
					command += ' gs://your-bucket/your-file.json';
				}
			} else if (fileSource === 's3') {
				const s3PathsEl = document.getElementById('s3Paths');
				const s3Paths = s3PathsEl ? s3PathsEl.value : '';
				if (s3Paths.trim()) {
					const firstPath = s3Paths.split(/[,\n]/)[0].trim();
					if (firstPath) command += ` "${firstPath}"`;
				} else {
					command += ' s3://your-bucket/your-file.json';
				}
			}

			// Core options
			command += ` --type ${recordType}`;

			// Credentials - only add if fields are visible and have values
			const projectElement = document.getElementById('project');
			if (projectElement && projectElement.closest('.form-group').style.display !== 'none' && projectElement.value) {
				command += ` --project ${projectElement.value}`;
			}

			const tokenElement = document.getElementById('token');
			if (tokenElement && tokenElement.closest('.form-group').style.display !== 'none' && tokenElement.value) {
				command += ` --token ${tokenElement.value}`;
			}

			const groupKeyElement = document.getElementById('groupKey');
			if (groupKeyElement && groupKeyElement.closest('.form-group').style.display !== 'none' && groupKeyElement.value) {
				command += ` --group ${groupKeyElement.value}`;
			}


			// Service account credentials
			const acctElement = document.getElementById('acct');
			const passElement = document.getElementById('pass');
			if (acctElement && acctElement.value) command += ` --acct ${acctElement.value}`;
			if (passElement && passElement.value) command += ` --pass [password]`;

			// S3 credentials if S3 is selected
			if (fileSource === 's3') {
				const s3KeyElement = document.getElementById('s3Key');
				const s3SecretElement = document.getElementById('s3Secret');
				const s3RegionElement = document.getElementById('s3Region');
				
				if (s3KeyElement && s3KeyElement.value) command += ` --s3Key ${s3KeyElement.value}`;
				if (s3SecretElement && s3SecretElement.value) command += ` --s3Secret [s3-secret]`;
				if (s3RegionElement && s3RegionElement.value) command += ` --s3Region ${s3RegionElement.value}`;
			}

			// Common options
			const workersEl = document.getElementById('workers');
			const workers = workersEl ? workersEl.value : '';
			if (workers && workers !== '10') command += ` --workers ${workers}`;

			const recordsPerBatchEl = document.getElementById('recordsPerBatch');
			const recordsPerBatch = recordsPerBatchEl ? recordsPerBatchEl.value : '';
			if (recordsPerBatch && recordsPerBatch !== '2000') command += ` --batch ${recordsPerBatch}`;

			const regionEl = document.getElementById('region');
			const region = regionEl ? regionEl.value : '';
			if (region && region !== 'US') command += ` --region ${region}`;

			const vendorEl = document.getElementById('vendor');
			const vendor = vendorEl ? vendorEl.value : '';
			if (vendor) command += ` --vendor ${vendor}`;

			// Boolean flags
			const booleanFlags = [
				['compress', 'compress'],
				['fixData', 'fix'],
				['fixTime', 'fixTime'],
				['strict', 'strict'],
				['removeNulls', 'clean'],
				['flattenData', 'flatten'],
				['fixJson', 'fix-json'],
				['dedupe', 'dedupe'],
				['forceStream', 'stream'],
				['verbose', 'verbose'],
				['logs', 'logs']
			];

			booleanFlags.forEach(([elementId, cliFlag]) => {
				const element = document.getElementById(elementId);
				if (element && element.checked) {
					command += ` --${cliFlag}`;
				}
			});

			// Time filters
			const epochStartEl = document.getElementById('epochStart');
			const epochStart = epochStartEl ? epochStartEl.value : '';
			if (epochStart) command += ` --epoch-start ${epochStart}`;

			const epochEndEl = document.getElementById('epochEnd');
			const epochEnd = epochEndEl ? epochEndEl.value : '';
			if (epochEnd) command += ` --epoch-end ${epochEnd}`;

			const timeOffsetEl = document.getElementById('timeOffset');
			const timeOffset = timeOffsetEl ? timeOffsetEl.value : '';
			if (timeOffset && timeOffset !== '0') command += ` --offset ${timeOffset}`;

			// Aliases from column mapper and text input
			const aliases = this.getCurrentAliases();
			if (Object.keys(aliases).length > 0) {
				command += ` --aliases '${JSON.stringify(aliases)}'`;
			}

			// Other text options
			const textOptions = [
				['scrubProps', 'scrub'],
				['eventWhitelist', 'event-whitelist'],
				['eventBlacklist', 'event-blacklist'],
				['propKeyWhitelist', 'prop-whitelist'],
				['propKeyBlacklist', 'prop-blacklist'],
				['propValWhitelist', 'val-whitelist'],
				['propValBlacklist', 'val-blacklist'],
				['insertIdTuple', 'insert-tuple'],
				['tags', 'tags'],
				['vendorOpts', 'vendor-opts']
			];

			textOptions.forEach(([elementId, cliFlag]) => {
				const element = document.getElementById(elementId);
				if (element && element.value.trim()) {
					if (cliFlag === 'tags' || cliFlag === 'vendor-opts') {
						command += ` --${cliFlag} '${element.value}'`;
					} else {
						command += ` --${cliFlag} ${element.value}`;
					}
				}
			});

			// Additional numeric options
			const numericOptions = [
				['bytesPerBatch', 'bytes'],
				['maxRetries', 'retries'],
				['compressionLevel', 'compression-level']
			];

			numericOptions.forEach(([elementId, cliFlag]) => {
				const element = document.getElementById(elementId);
				if (element && element.value && element.value !== element.defaultValue) {
					command += ` --${cliFlag} ${element.value}`;
				}
			});

			// Stream format option
			const streamFormatEl = document.getElementById('streamFormat');
			const streamFormat = streamFormatEl ? streamFormatEl.value : '';
			if (streamFormat) command += ` --stream-format ${streamFormat}`;

			// Transport option
			const transportEl = document.getElementById('transport');
			const transport = transportEl ? transportEl.value : '';
			if (transport && transport !== 'got') command += ` --transport ${transport}`;

			// Legacy and additional authentication
			const secretElement = document.getElementById('secret');
			if (secretElement && secretElement.value) command += ` --secret ${secretElement.value}`;

			const bearerElement = document.getElementById('bearer');
			if (bearerElement && bearerElement.value) command += ` --bearer ${bearerElement.value}`;

			const tableElement = document.getElementById('lookupTableId');
			if (tableElement && tableElement.closest('.form-group').style.display !== 'none' && tableElement.value) {
				command += ` --table ${tableElement.value}`;
			}

			const secondTokenElement = document.getElementById('secondToken');
			if (secondTokenElement && secondTokenElement.closest('.form-group').style.display !== 'none' && secondTokenElement.value) {
				command += ` --second-token ${secondTokenElement.value}`;
			}

			// Export-specific options
			const startElement = document.getElementById('start');
			if (startElement && startElement.value) command += ` --start ${startElement.value}`;

			const endElement = document.getElementById('end');
			if (endElement && endElement.value) command += ` --end ${endElement.value}`;

			const whereElement = document.getElementById('where');
			if (whereElement && whereElement.value) command += ` --where "${whereElement.value}"`;

			const whereClauseElement = document.getElementById('whereClause');
			if (whereClauseElement && whereClauseElement.value) command += ` --where-clause "${whereClauseElement.value}"`;

			// Stream and processing options
			const waterElement = document.getElementById('water');
			if (waterElement && waterElement.value && waterElement.value !== '27') {
				command += ` --water ${waterElement.value}`;
			}

			// SCD options
			const scdTypeElement = document.getElementById('scdType');
			if (scdTypeElement && scdTypeElement.value && scdTypeElement.value !== 'string') {
				command += ` --scd-type ${scdTypeElement.value}`;
			}

			const scdKeyElement = document.getElementById('scdKey');
			if (scdKeyElement && scdKeyElement.value) command += ` --scd-key ${scdKeyElement.value}`;

			const scdLabelElement = document.getElementById('scdLabel');
			if (scdLabelElement && scdLabelElement.value) command += ` --scd-label ${scdLabelElement.value}`;

			// Cohort and data group options
			const cohortIdElement = document.getElementById('cohortId');
			if (cohortIdElement && cohortIdElement.value) command += ` --cohort-id ${cohortIdElement.value}`;

			const dataGroupIdElement = document.getElementById('dataGroupId');
			if (dataGroupIdElement && dataGroupIdElement.closest('.form-group').style.display !== 'none' && dataGroupIdElement.value) {
				command += ` --data-group-id ${dataGroupIdElement.value}`;
			}

			// Additional boolean flags
			const additionalBooleanFlags = [
				['writeToFile', 'write-to-file'],
				['createProfiles', 'create-profiles'],
				['addToken', 'add-token'],
				['abridged', 'abridged'],
				['http2', 'http2'],
				['keepBadRecords', 'keep-bad-records']
			];

			additionalBooleanFlags.forEach(([elementId, cliFlag]) => {
				const element = document.getElementById(elementId);
				if (element && element.checked) {
					command += ` --${cliFlag}`;
				}
			});

			cliElement.textContent = command;
			cliElement.classList.remove('empty');

		} catch (error) {
			console.warn('Error generating CLI command:', error);
			cliElement.textContent = 'Error generating CLI command...';
			cliElement.classList.add('empty');
		}
	}

	copyCLICommand() {
		const cliElement = document.getElementById('cli-command');
		const command = cliElement.textContent;

		if (command.includes('Select') || command.includes('Error')) {
			this.showError('Please configure your import settings first.');
			return;
		}

		// Copy to clipboard
		navigator.clipboard.writeText(command).then(() => {
			// Show success feedback
			const copyBtn = document.getElementById('copy-cli');
			const originalText = copyBtn.innerHTML;
			copyBtn.innerHTML = '<span class="btn-icon">✓</span> Copied!';
			copyBtn.style.background = 'var(--success)';

			setTimeout(() => {
				copyBtn.innerHTML = originalText;
				copyBtn.style.background = '';
			}, 2000);
		}).catch(_err => {
			this.showError('Could not copy to clipboard. Please select and copy manually.');
		});
	}

	downloadResults() {
		if (!this.lastResults) {
			this.showError('No results available to download.');
			return;
		}

		// Create a blob with the JSON data
		const jsonString = JSON.stringify(this.lastResults, null, 2);
		const blob = new Blob([jsonString], { type: 'application/json' });

		// Create a temporary download link
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `mixpanel-import-results-${Date.now()}.json`;

		// Trigger download
		document.body.appendChild(a);
		a.click();

		// Cleanup
		document.body.removeChild(a);
		URL.revokeObjectURL(url);

		// Show success feedback
		const downloadBtn = document.getElementById('download-results-btn');
		const originalText = downloadBtn.innerHTML;
		downloadBtn.innerHTML = '<span class="btn-icon">✓</span> Downloaded!';

		setTimeout(() => {
			downloadBtn.innerHTML = originalText;
		}, 2000);
	}

	async initializeMonacoEditor() {
		return new Promise((resolve, _reject) => {
			// Load Monaco Editor dynamically
			if (typeof monaco !== 'undefined') {
				// Monaco already loaded
				this.createMonacoEditor();
				resolve();
				return;
			}

			// Create script element for Monaco loader
			const script = document.createElement('script');
			script.src = 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs/loader.js';
			script.onload = () => {
				// Configure and load Monaco
				require.config({
					paths: {
						vs: 'https://cdnjs.cloudflare.com/ajax/libs/monaco-editor/0.44.0/min/vs'
					}
				});

				require(['vs/editor/editor.main'], () => {
					this.createMonacoEditor();
					resolve();
				}, (error) => {
					console.warn('Monaco Editor failed to load:', error);
					this.createFallbackEditor();
					resolve();
				});
			};

			script.onerror = () => {
				console.warn('Monaco Editor script failed to load, using fallback');
				this.createFallbackEditor();
				resolve();
			};

			document.head.appendChild(script);
		});
	}

	getDefaultTransformFunction() {
		return `
function transform(row) {
	// Transform each record: add props, rename fields, filter, or split
	
	// Examples:
	// row.custom_source = 'my-import'; // Add property
	// row.time = new Date(row.timestamp).getTime(); // Fix time
	// row.distinct_id = row.user_id; delete row.user_id; // Rename
	// if (!row.event) return {}; // Skip record
	// return [row1, row2]; // Split into multiple
	
	return row; // Return modified record
}`;
	}

	createMonacoEditor() {
		this.editor = monaco.editor.create(document.getElementById('monaco-editor'), {
			value: this.getDefaultTransformFunction(),
			language: 'javascript',
			theme: 'vs-dark',
			fontSize: 13,
			minimap: { enabled: false },
			scrollBeyondLastLine: false,
			automaticLayout: true,
			wordWrap: 'on'
		});
	}

	createFallbackEditor() {
		// Create a simple textarea fallback
		const editorContainer = document.getElementById('monaco-editor');
		const textarea = document.createElement('textarea');
		textarea.className = 'fallback-editor';
		textarea.style.cssText = `
            width: 100%;
            height: 300px;
            background: var(--bg-tertiary);
            color: var(--text-primary);
            border: 1px solid var(--border-primary);
            border-radius: 8px;
            padding: 16px;
            font-family: Monaco, Menlo, 'Ubuntu Mono', monospace;
            font-size: 13px;
            resize: vertical;
            tab-size: 2;
        `;
		textarea.value = this.getDefaultTransformFunction();

		editorContainer.innerHTML = '';
		editorContainer.appendChild(textarea);

		// Create simple editor interface
		this.editor = {
			getValue: () => textarea.value,
			setValue: (value) => { textarea.value = value; }
		};
	}

	collectFormData() {
		const formData = new FormData();

		// Add files
		this.files.forEach(file => {
			formData.append('files', file);
		});

		// Check for cloud mode files
		const fileSource = document.querySelector('input[name="fileSource"]:checked')?.value;
		if (fileSource === 'gcs') {
			const gcsPathsInput = document.getElementById('gcsPaths').value;
			if (gcsPathsInput) {
				const cloudPaths = gcsPathsInput.split(/[,\n]/).map(p => p.trim()).filter(p => p);
				formData.append('cloudPaths', JSON.stringify(cloudPaths));
			}
		} else if (fileSource === 's3') {
			const s3PathsInput = document.getElementById('s3Paths').value;
			if (s3PathsInput) {
				const cloudPaths = s3PathsInput.split(/[,\n]/).map(p => p.trim()).filter(p => p);
				formData.append('cloudPaths', JSON.stringify(cloudPaths));
			}
		}

		// Collect credentials
		const credentials = {};

		// Add project ID if visible
		const projectElement = document.getElementById('project');
		if (projectElement && projectElement.closest('.form-group').style.display !== 'none' && projectElement.value) {
			credentials.project = projectElement.value;
		}


		// Add service account authentication
		const acctElement = document.getElementById('acct');
		const passElement = document.getElementById('pass');
		if (acctElement && acctElement.value) credentials.acct = acctElement.value;
		if (passElement && passElement.value) credentials.pass = passElement.value;

		// Add optional fields if visible and have values
		const tokenElement = document.getElementById('token');
		if (tokenElement && tokenElement.closest('.form-group').style.display !== 'none' && tokenElement.value) {
			credentials.token = tokenElement.value;
		}

		const groupKeyElement = document.getElementById('groupKey');
		if (groupKeyElement && groupKeyElement.closest('.form-group').style.display !== 'none' && groupKeyElement.value) {
			credentials.groupKey = groupKeyElement.value;
		}

		// Add S3 credentials if S3 is selected
		if (fileSource === 's3') {
			const s3KeyElement = document.getElementById('s3Key');
			const s3SecretElement = document.getElementById('s3Secret');
			const s3RegionElement = document.getElementById('s3Region');
			
			if (s3KeyElement && s3KeyElement.value) credentials.s3Key = s3KeyElement.value;
			if (s3SecretElement && s3SecretElement.value) credentials.s3Secret = s3SecretElement.value;
			if (s3RegionElement && s3RegionElement.value) credentials.s3Region = s3RegionElement.value;
		}

		// Add GCS credentials if GCS is selected
		if (fileSource === 'gcs') {
			const gcpProjectIdElement = document.getElementById('gcpProjectId');
			const gcsCredentialsElement = document.getElementById('gcsCredentials');
			
			if (gcpProjectIdElement && gcpProjectIdElement.value) {
				credentials.gcpProjectId = gcpProjectIdElement.value;
			}
			
			// Add GCS credentials file if uploaded
			if (gcsCredentialsElement?.files?.length > 0) {
				formData.append('gcsCredentials', gcsCredentialsElement.files[0]);
			}
		}

		const dataGroupIdElement = document.getElementById('dataGroupId');
		if (dataGroupIdElement && dataGroupIdElement.closest('.form-group').style.display !== 'none' && dataGroupIdElement.value) {
			credentials.dataGroupId = dataGroupIdElement.value;
		}

		const secondTokenElement = document.getElementById('secondToken');
		if (secondTokenElement && secondTokenElement.closest('.form-group').style.display !== 'none' && secondTokenElement.value) {
			credentials.secondToken = secondTokenElement.value;
		}

		formData.append('credentials', JSON.stringify(credentials));

		// Collect options
		const recordTypeEl = document.getElementById('recordType');
		const workersEl = document.getElementById('workers');
		const recordsPerBatchEl = document.getElementById('recordsPerBatch');
		const regionEl = document.getElementById('region');
		const compressEl = document.getElementById('compress');
		const fixDataEl = document.getElementById('fixData');
		const strictEl = document.getElementById('strict');
		const verboseEl = document.getElementById('verbose');

		const options = {
			recordType: recordTypeEl ? recordTypeEl.value : '',
			workers: workersEl ? parseInt(workersEl.value) || 10 : 10,
			recordsPerBatch: recordsPerBatchEl ? parseInt(recordsPerBatchEl.value) || 2000 : 2000,
			region: regionEl ? regionEl.value || 'US' : 'US',
			compress: compressEl ? compressEl.checked : false,
			fixData: fixDataEl ? fixDataEl.checked : false,
			strict: strictEl ? strictEl.checked : false,
			verbose: verboseEl ? verboseEl.checked : false
		};

		const vendor = document.getElementById('vendor').value;
		if (vendor) options.vendor = vendor;

		// Add aliases from column mapper (merged with text input, text takes precedence)
		const currentAliases = this.getCurrentAliases();
		if (Object.keys(currentAliases).length > 0) {
			options.aliases = currentAliases;
		}

		formData.append('options', JSON.stringify(options));

		// Add transform code if present
		const transformCode = this.editor ? this.editor.getValue().trim() : '';
		if (transformCode && !transformCode.startsWith('// Transform function')) {
			formData.append('transformCode', transformCode);
		}

		return formData;
	}

	// Preview raw data functionality
	async previewRawData() {
		try {
			// Validation - same as submitJob but more lenient
			const fileSource = document.querySelector('input[name="fileSource"]:checked').value;

			if (fileSource === 'local' && this.files.length === 0) {
				this.showError('Please select at least one file to preview.');
				return;
			}

			if (fileSource === 'gcs') {
				const gcsPaths = document.getElementById('gcsPaths').value;
				if (!gcsPaths.trim()) {
					this.showError('Please enter at least one GCS path to preview.');
					return;
				}
			} else if (fileSource === 's3') {
				const s3Paths = document.getElementById('s3Paths').value;
				if (!s3Paths.trim()) {
					this.showError('Please enter at least one S3 path to preview.');
					return;
				}
			}

			// Show loading state
			const previewBtn = document.getElementById('preview-data-btn');
			const originalText = previewBtn.innerHTML;
			previewBtn.innerHTML = '<span class="btn-icon">⏳</span> Loading...';
			previewBtn.disabled = true;

			// Collect minimal form data for preview
			const formData = this.collectFormData();

			// Add minimal options for raw preview (no transforms)
			const options = {
				recordType: document.getElementById('recordType').value || 'event',
				region: document.getElementById('region')?.value || 'US'
			};

			formData.append('options', JSON.stringify(options));
			formData.append('credentials', JSON.stringify({})); // Empty creds for preview

			// Call sample endpoint
			const response = await fetch('/sample', {
				method: 'POST',
				body: formData
			});

			const result = await response.json();

			if (!result.success) {
				throw new Error(result.error || 'Preview failed');
			}

			// Store sample data and show preview
			this.sampleData = result.sampleData || [];
			this.displayPreviewRecords();

			// Toggle column mapper buttons visibility
			this.updateColumnMapperButtons();

			// Reset button
			previewBtn.innerHTML = originalText;
			previewBtn.disabled = false;

		} catch (error) {
			console.error('Preview error:', error);
			this.showError('Preview failed: ' + error.message);

			// Reset button
			const previewBtn = document.getElementById('preview-data-btn');
			previewBtn.innerHTML = '<span class="btn-icon">👁️</span> Preview Data (Raw)';
			previewBtn.disabled = false;
		}
	}

	displayPreviewRecords() {
		const previewSection = document.getElementById('data-preview');
		const previewContent = document.getElementById('preview-content');

		if (this.sampleData.length === 0) {
			previewSection.style.display = 'block';
			previewContent.innerHTML = '<p class="muted-text">No data found in the source.</p>';
			return;
		}

		// Show random 5 records
		const randomRecords = this.getRandomRecords(5);

		previewSection.style.display = 'block';
		previewContent.innerHTML = `<pre><code class="json">${this.highlightJSON(JSON.stringify(randomRecords, null, 2))}</code></pre>`;

		// Show record count info
		const recordInfo = document.createElement('p');
		recordInfo.className = 'section-description';
		recordInfo.textContent = `Showing 5 of ${this.sampleData.length} sample records (max 500)`;
		previewContent.prepend(recordInfo);
	}

	showMorePreviewRecords() {
		if (this.sampleData.length === 0) return;

		// Get different random 5 records and update display
		const randomRecords = this.getRandomRecords(5);
		const previewContent = document.getElementById('preview-content');

		// Keep the info text, replace the JSON
		const pre = previewContent.querySelector('pre');
		if (pre) {
			pre.innerHTML = `<code class="json">${this.highlightJSON(JSON.stringify(randomRecords, null, 2))}</code>`;
		}
	}

	getRandomRecords(count) {
		if (this.sampleData.length <= count) {
			return this.sampleData;
		}

		// Get random indices without replacement
		const indices = [];
		while (indices.length < count) {
			const randomIndex = Math.floor(Math.random() * this.sampleData.length);
			if (!indices.includes(randomIndex)) {
				indices.push(randomIndex);
			}
		}

		return indices.map(i => this.sampleData[i]);
	}

	async submitJob(isDryRun = false) {
		try {
			// Validation
			const fileSource = document.querySelector('input[name="fileSource"]:checked').value;

			if (fileSource === 'local' && this.files.length === 0) {
				this.showError('Please select at least one file to import.');
				return;
			}

			if (fileSource === 'gcs') {
				const gcsPaths = document.getElementById('gcsPaths').value;
				if (!gcsPaths.trim()) {
					this.showError('Please enter at least one GCS path.');
					return;
				}

				const paths = gcsPaths.split(/[,\n]/).map(p => p.trim()).filter(p => p);
				const invalidPaths = paths.filter(p => !p.startsWith('gs://'));
				if (invalidPaths.length > 0) {
					this.showError('All GCS paths must start with gs://. Invalid paths: ' + invalidPaths.join(', '));
					return;
				}
			} else if (fileSource === 's3') {
				const s3Paths = document.getElementById('s3Paths').value;
				if (!s3Paths.trim()) {
					this.showError('Please enter at least one S3 path.');
					return;
				}

				const paths = s3Paths.split(/[,\n]/).map(p => p.trim()).filter(p => p);
				const invalidPaths = paths.filter(p => !p.startsWith('s3://'));
				if (invalidPaths.length > 0) {
					this.showError('All S3 paths must start with s3://. Invalid paths: ' + invalidPaths.join(', '));
					return;
				}
			}

			const recordType = document.getElementById('recordType').value;
			if (!recordType) {
				this.showError('Please select an import type.');
				return;
			}

			// Validate required fields based on record type
			const validationResult = this.validateRequiredFields(recordType);
			if (!validationResult.isValid) {
				this.showError(validationResult.message);
				return;
			}

			// Clear any previous results
			this.clearResults();

			// Show loading
			this.showLoading(isDryRun ? 'Running Test...' : 'Importing Data...',
				isDryRun ? 'Processing sample data to preview results' : 'Importing your data to Mixpanel');

			// Collect form data
			const formData = this.collectFormData();

			// Submit to appropriate endpoint
			const endpoint = isDryRun ? '/dry-run' : '/job';
			const response = await fetch(endpoint, {
				method: 'POST',
				body: formData
			});

			const result = await response.json();

			// Handle different responses for dry runs vs real jobs
			if (isDryRun) {
				this.hideLoading();
				if (result.success) {
					this.showResults(result, isDryRun);
				} else {
					this.showError(`Test failed: ${result.error}`);
				}
			} else {
				// For real jobs, the server returns a jobId and runs asynchronously
				if (result.success && result.jobId) {
					// Connect WebSocket for real-time progress updates
					this.connectWebSocket(result.jobId);
					// Update loading message to show that job has started
					const loadingMessage = document.querySelector('.loading-details');
					if (loadingMessage) {
						loadingMessage.innerHTML = 'connecting to websocket; initiating streaming';
					}
				} else {
					this.hideLoading();
					this.showError(`Import failed: ${result.error}`);
				}
			}

		} catch (error) {
			this.hideLoading();
			this.showError(`Network error: ${error.message}`);
		}
	}


	showLoading(title, message) {
		document.getElementById('loading-title').textContent = title;
		document.getElementById('loading-message').textContent = message;
		document.getElementById('loading').style.display = 'flex';
	}

	hideLoading() {
		document.getElementById('loading').style.display = 'none';
	}

	clearResults() {
		const resultsSection = document.getElementById('results');
		const resultsData = document.getElementById('results-data');
		
		if (resultsSection) {
			resultsSection.style.display = 'none';
		}
		if (resultsData) {
			resultsData.innerHTML = '';
		}
		
		// Clear any stored comparison data
		this.comparisonData = null;
	}

	showResults(result, isDryRun) {
		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = isDryRun ? 'Preview Results' : 'Import Complete!';

		// Store results for download
		this.lastResults = result;

		// For dry runs with both raw and transformed data, show side-by-side comparison
		if (isDryRun && result.rawData && result.previewData) {
			this.showSideBySideComparison(result.rawData, result.previewData);
		} else {
			// Regular single display for non-dry runs or legacy dry runs
			// Handle both WebSocket results (direct result object) and HTTP results (wrapped in .result)
			let displayData = result.result || result;

			if (isDryRun && result.previewData && result.previewData.length > 0) {
				displayData = [...result.previewData.slice(0, 100)];
			}

			resultsData.innerHTML = `<pre><code class="json">${this.highlightJSON(JSON.stringify(displayData, null, 2))}</code></pre>`;
		}

		resultsSection.style.display = 'block';
		resultsSection.scrollIntoView({ behavior: 'smooth' });
	}

	showSideBySideComparison(rawData, transformedData) {
		const resultsData = document.getElementById('results-data');
		
		// Store data for pagination
		this.comparisonData = {
			raw: rawData,
			transformed: transformedData,
			currentPage: 0,
			recordsPerPage: 10
		};
		
		// Initialize scroll sync state (default: locked/synchronized)
		this.scrollSyncEnabled = true;

		// Create side-by-side container
		resultsData.innerHTML = `
			<div class="side-by-side-container">
				<div class="comparison-header">
					<div class="comparison-title">
						<h3>Raw Data</h3>
						<span class="record-count">${rawData.length} records</span>
					</div>
					<div class="comparison-title">
						<h3>Transformed Data</h3>
						<span class="record-count">${transformedData.length} records</span>
					</div>
				</div>
				<div class="comparison-controls">
					<button type="button" id="prev-records-btn" class="btn btn-secondary" disabled>
						← Previous 10
					</button>
					<span id="record-range">Records 1-10</span>
					<button type="button" id="next-records-btn" class="btn btn-secondary">
						Next 10 →
					</button>
					<div class="scroll-sync-control">
						<button type="button" id="scroll-sync-toggle" class="btn btn-ghost btn-sm scroll-sync-locked" title="Toggle synchronized scrolling">
							<span class="btn-icon">🔒</span>
							<span class="sync-label">Sync Scroll</span>
						</button>
					</div>
				</div>
				<div class="comparison-panels">
					<div class="comparison-panel" id="raw-panel">
						<div class="panel-content" id="raw-content"></div>
					</div>
					<div class="comparison-panel" id="transformed-panel">
						<div class="panel-content" id="transformed-content"></div>
					</div>
				</div>
			</div>
		`;

		this.renderComparisonPage();
		this.setupComparisonControls();
		this.setupScrollSyncToggle();
	}

	renderComparisonPage() {
		const { raw, transformed, currentPage, recordsPerPage } = this.comparisonData;
		const startIdx = currentPage * recordsPerPage;
		const endIdx = Math.min(startIdx + recordsPerPage, Math.max(raw.length, transformed.length));
		
		const rawSlice = raw.slice(startIdx, endIdx);
		const transformedSlice = transformed.slice(startIdx, endIdx);
		
		// Render raw data
		const rawContent = document.getElementById('raw-content');
		rawContent.innerHTML = this.renderRecordList(rawSlice, 'raw');
		
		// Render transformed data
		const transformedContent = document.getElementById('transformed-content');
		transformedContent.innerHTML = this.renderRecordList(transformedSlice, 'transformed');
		
		// Update controls
		this.updateComparisonControls(startIdx + 1, endIdx, Math.max(raw.length, transformed.length));
		this.setupSynchronizedScrolling();
	}

	renderRecordList(records, type) {
		return records.map((record, index) => `
			<div class="record-item" data-index="${index}">
				<div class="record-header">Record ${this.comparisonData.currentPage * this.comparisonData.recordsPerPage + index + 1}</div>
				<pre><code class="json">${this.highlightJSON(JSON.stringify(record, null, 2))}</code></pre>
			</div>
		`).join('');
	}

	updateComparisonControls(start, end, total) {
		const prevBtn = document.getElementById('prev-records-btn');
		const nextBtn = document.getElementById('next-records-btn');
		const rangeSpan = document.getElementById('record-range');
		
		prevBtn.disabled = this.comparisonData.currentPage === 0;
		nextBtn.disabled = end >= total;
		rangeSpan.textContent = `Records ${start}-${end} of ${total}`;
	}

	setupComparisonControls() {
		const prevBtn = document.getElementById('prev-records-btn');
		const nextBtn = document.getElementById('next-records-btn');
		
		prevBtn.addEventListener('click', () => {
			if (this.comparisonData.currentPage > 0) {
				this.comparisonData.currentPage--;
				this.renderComparisonPage();
			}
		});
		
		nextBtn.addEventListener('click', () => {
			const { raw, transformed, currentPage, recordsPerPage } = this.comparisonData;
			const maxRecords = Math.max(raw.length, transformed.length);
			if ((currentPage + 1) * recordsPerPage < maxRecords) {
				this.comparisonData.currentPage++;
				this.renderComparisonPage();
			}
		});
	}

	setupSynchronizedScrolling() {
		const rawContent = document.getElementById('raw-content');
		const transformedContent = document.getElementById('transformed-content');
		
		// Remove existing listeners to prevent duplicates
		if (rawContent) rawContent.replaceWith(rawContent.cloneNode(true));
		if (transformedContent) transformedContent.replaceWith(transformedContent.cloneNode(true));
		
		// Get fresh references after cloning
		const rawPanel = document.getElementById('raw-content');
		const transformedPanel = document.getElementById('transformed-content');
		
		if (!rawPanel || !transformedPanel) return;
		
		let isScrolling = false;
		
		const syncScroll = (source, target) => {
			// Only sync if scroll sync is enabled
			if (!this.scrollSyncEnabled || isScrolling) return;
			isScrolling = true;
			target.scrollTop = source.scrollTop;
			setTimeout(() => { isScrolling = false; }, 10);
		};
		
		rawPanel.addEventListener('scroll', () => syncScroll(rawPanel, transformedPanel));
		transformedPanel.addEventListener('scroll', () => syncScroll(transformedPanel, rawPanel));
	}

	setupScrollSyncToggle() {
		const toggleBtn = document.getElementById('scroll-sync-toggle');
		if (!toggleBtn) return;

		toggleBtn.addEventListener('click', () => {
			this.scrollSyncEnabled = !this.scrollSyncEnabled;
			this.updateScrollSyncButton();
		});
	}

	updateScrollSyncButton() {
		const toggleBtn = document.getElementById('scroll-sync-toggle');
		const icon = toggleBtn.querySelector('.btn-icon');
		
		if (this.scrollSyncEnabled) {
			// Locked state
			toggleBtn.classList.add('scroll-sync-locked');
			toggleBtn.classList.remove('scroll-sync-unlocked');
			icon.textContent = '🔒';
			toggleBtn.title = 'Click to unlock independent scrolling';
		} else {
			// Unlocked state  
			toggleBtn.classList.add('scroll-sync-unlocked');
			toggleBtn.classList.remove('scroll-sync-locked');
			icon.textContent = '🔓';
			toggleBtn.title = 'Click to lock synchronized scrolling';
		}
	}

	showError(message) {
		// Remove existing error messages
		const existingErrors = document.querySelectorAll('.error');
		existingErrors.forEach(error => error.remove());

		// Create new error message
		const errorDiv = document.createElement('div');
		errorDiv.className = 'error';
		errorDiv.textContent = message;

		// Insert after header
		const header = document.querySelector('.header');
		header.parentNode.insertBefore(errorDiv, header.nextSibling);

		// Scroll to error
		errorDiv.scrollIntoView({ behavior: 'smooth' });

		// Auto-remove after 5 seconds
		setTimeout(() => {
			if (errorDiv.parentNode) {
				errorDiv.remove();
			}
		}, 5000);
	}

	showSuccess(message) {
		const successDiv = document.createElement('div');
		successDiv.className = 'success';
		successDiv.textContent = message;

		const header = document.querySelector('.header');
		header.parentNode.insertBefore(successDiv, header.nextSibling);

		setTimeout(() => {
			if (successDiv.parentNode) {
				successDiv.remove();
			}
		}, 3000);
	}

	highlightJSON(jsonString) {
		if (!jsonString || typeof jsonString !== 'string') {
			return '';
		}
		return jsonString
			.replace(/("([^"\\]|\\.)*")\s*:/g, '<span class="json-key">$1</span>:')
			.replace(/:\s*("([^"\\]|\\.)*")/g, ': <span class="json-string">$1</span>')
			.replace(/:\s*(true|false)/g, ': <span class="json-boolean">$1</span>')
			.replace(/:\s*(null)/g, ': <span class="json-null">$1</span>')
			.replace(/:\s*(-?\d+\.?\d*)/g, ': <span class="json-number">$1</span>')
			.replace(/([{}[\]])/g, '<span class="json-punctuation">$1</span>');
	}

	// Column Mapper Methods
	async detectColumns() {
		try {
			// Validation - same as preview data
			const fileSource = document.querySelector('input[name="fileSource"]:checked').value;

			if (fileSource === 'local' && this.files.length === 0) {
				this.showError('Please select at least one file to detect columns.');
				return;
			}

			if (fileSource !== 'local') {
				const cloudPathsEl = fileSource === 'gcs' ? 
					document.getElementById('gcsPaths') : 
					document.getElementById('s3Paths');
				
				if (!cloudPathsEl || !cloudPathsEl.value.trim()) {
					this.showError('Please enter at least one cloud storage path to detect columns.');
					return;
				}
			}

			// Show loading state
			const detectBtn = document.getElementById('detect-columns-btn');
			const originalText = detectBtn.innerHTML;
			detectBtn.innerHTML = '<span class="btn-icon">⏳</span> Detecting...';
			detectBtn.disabled = true;

			// Collect form data for column detection
			const formData = this.collectFormData();

			// Add minimal options for column detection
			const options = {
				recordType: document.getElementById('recordType').value || 'event',
				region: document.getElementById('region')?.value || 'US'
			};

			formData.append('options', JSON.stringify(options));
			formData.append('credentials', JSON.stringify({})); // Empty creds for detection

			// Call columns endpoint
			const response = await fetch('/columns', {
				method: 'POST',
				body: formData
			});

			const responseText = await response.text();

			let result;
			try {
				result = JSON.parse(responseText);
			} catch (parseError) {
				console.error('JSON parse error:', parseError);
				throw new Error(`Server returned invalid JSON: ${responseText.substring(0, 100)}...`);
			}

			if (!result.success) {
				throw new Error(result.error || 'Column detection failed');
			}

			// Store detected columns and render mapper
			this.detectedColumns = result.columns || [];
			this.renderColumnMapper();

			// Reset button
			detectBtn.innerHTML = originalText;
			detectBtn.disabled = false;

		} catch (error) {
			console.error('Column detection error:', error);
			this.showError('Column detection failed: ' + error.message);

			// Reset button
			const detectBtn = document.getElementById('detect-columns-btn');
			detectBtn.innerHTML = '<span class="btn-icon">🔍</span> Detect Columns from Data';
			detectBtn.disabled = false;
		}
	}

	renderColumnMapper() {
		if (this.detectedColumns.length === 0) {
			this.showError('No columns detected in your data.');
			return;
		}

		const mapperContent = document.getElementById('column-mapper-content');
		const mapperGrid = mapperContent.querySelector('.column-mapper-grid');

		// Get current record type to show appropriate fields
		const recordType = document.getElementById('recordType').value;
		
		// Define Mixpanel target fields based on record type
		let mixpanelFields = [];
		
		if (recordType === 'event') {
			mixpanelFields = [
				{ key: 'event', icon: '🎯', label: 'Event Name', description: 'The name of the event' },
				{ key: 'time', icon: '⏰', label: 'Timestamp', description: 'Event timestamp (Unix milliseconds)' },
				{ key: 'distinct_id', icon: '👤', label: 'Distinct ID (orig)', description: 'Unique identifier for the user' },
				{ key: 'insert_id', icon: '🔑', label: 'Insert ID', description: 'Unique identifier for deduplication' },
				{ key: 'user_id', icon: '🆔', label: 'User ID (simp)', description: '$user_id for simplified id merge' },
				{ key: 'device_id', icon: '📱', label: 'Device ID (simp)', description: '$device_id for simplified id merge' }
			];
		} else if (recordType === 'user') {
			mixpanelFields = [
				{ key: 'distinct_id', icon: '👤', label: 'Distinct ID', description: 'Unique identifier for the user profile' },
				{ key: 'name', icon: '📝', label: 'Full Name', description: 'User\'s full name' },
				{ key: 'first_name', icon: '👤', label: 'First Name', description: 'User\'s first name' },
				{ key: 'last_name', icon: '👤', label: 'Last Name', description: 'User\'s last name' },
				{ key: 'email', icon: '📧', label: 'Email', description: 'User\'s email address' },
				{ key: 'phone', icon: '📞', label: 'Phone', description: 'User\'s phone number' },
				{ key: 'created', icon: '📅', label: 'Created Date', description: 'When the user was created' },
				{ key: 'avatar', icon: '🖼️', label: 'Avatar URL', description: 'URL to user\'s profile picture' }
			];
		} else if (recordType === 'group') {
			mixpanelFields = [
				{ key: 'group_id', icon: '👥', label: 'Group ID', description: 'Unique identifier for the group' },
				{ key: 'name', icon: '📝', label: 'Group Name', description: 'Name of the group/organization' },
				{ key: 'created', icon: '📅', label: 'Created Date', description: 'When the group was created' },
				{ key: 'plan', icon: '💼', label: 'Plan/Tier', description: 'Subscription plan or tier' },
				{ key: 'industry', icon: '🏢', label: 'Industry', description: 'Industry category' },
				{ key: 'employees', icon: '👨‍💼', label: 'Employee Count', description: 'Number of employees' }
			];
		} else {
			// Fallback for unknown record types
			mixpanelFields = [
				{ key: 'distinct_id', icon: '👤', label: 'Distinct ID', description: 'Unique identifier' },
				{ key: 'name', icon: '📝', label: 'Name', description: 'Display name' }
			];
		}

		// Render mapping rows
		mapperGrid.innerHTML = mixpanelFields.map(field => `
			<div class="column-mapping-row mixpanel-field-${field.key}">
				<div class="mixpanel-field-label">
					<span class="mixpanel-field-icon">${field.icon}</span>
					${field.label}
				</div>
				<select class="source-column-select" data-target="${field.key}">
					<option value="">-- Select source column --</option>
					${this.detectedColumns.map(col => 
						`<option value="${col}" ${this.columnMappings[field.key] === col ? 'selected' : ''}>${col}</option>`
					).join('')}
				</select>
				<button type="button" class="clear-mapping-btn" data-target="${field.key}" title="Clear mapping">
					✕
				</button>
			</div>
		`).join('');

		// Add event listeners for the dropdowns and clear buttons
		mapperGrid.querySelectorAll('.source-column-select').forEach(select => {
			select.addEventListener('change', (e) => {
				const target = e.target.dataset.target;
				const value = e.target.value;
				
				if (value) {
					this.columnMappings[target] = value;
				} else {
					delete this.columnMappings[target];
				}
				
				this.updateMapperStatus();
			});
		});

		mapperGrid.querySelectorAll('.clear-mapping-btn').forEach(btn => {
			btn.addEventListener('click', (e) => {
				const target = e.target.dataset.target;
				const select = mapperGrid.querySelector(`select[data-target="${target}"]`);
				
				select.value = '';
				delete this.columnMappings[target];
				this.updateMapperStatus();
			});
		});

		// Show the mapper content and update status
		mapperContent.style.display = 'block';
		this.updateMapperStatus();
	}

	refreshColumnMapper() {
		// Re-render the column mapper when record type changes
		if (this.detectedColumns.length > 0) {
			this.renderColumnMapper();
		}
	}

	updateMapperStatus() {
		const statusEl = document.getElementById('mapper-status');
		const statusContainer = statusEl.parentElement;
		const mappingCount = Object.keys(this.columnMappings).length;

		if (mappingCount === 0) {
			statusEl.textContent = `Detected ${this.detectedColumns.length} columns from your data. Select mappings above to create aliases.`;
			statusContainer.classList.remove('has-mappings');
		} else {
			const mappings = Object.entries(this.columnMappings)
				.map(([target, source]) => `${source} → ${target}`)
				.join(', ');

			statusEl.innerHTML = `<span class="mapping-count">${mappingCount} mapping${mappingCount === 1 ? '' : 's'}</span> created: ${mappings}`;
			statusContainer.classList.add('has-mappings');
		}
	}

	updateColumnMapperButtons() {
		const detectBtn = document.getElementById('detect-columns-btn');
		const previewFirstBtn = document.getElementById('preview-data-from-mapper-btn');

		// Show "Detect Columns" if we have preview data, otherwise show "Preview Data First"
		if (this.sampleData && this.sampleData.length > 0) {
			if (detectBtn) detectBtn.style.display = 'inline-block';
			if (previewFirstBtn) previewFirstBtn.style.display = 'none';
		} else {
			if (detectBtn) detectBtn.style.display = 'none';
			if (previewFirstBtn) previewFirstBtn.style.display = 'inline-block';
		}
	}

	// Method to get the current aliases (combining text input + mapper)
	getCurrentAliases() {
		// Get aliases from text input (if it exists)
		const aliasesInput = document.getElementById('aliases');
		let textAliases = {};
		
		if (aliasesInput && aliasesInput.value.trim()) {
			try {
				textAliases = JSON.parse(aliasesInput.value.trim());
			} catch (e) {
				console.warn('Invalid JSON in aliases field:', e);
			}
		}

		// Merge with column mapper aliases (text input takes precedence)
		return { ...this.columnMappings, ...textAliases };
	}
}

// Global function for collapsible sections
// eslint-disable-next-line no-unused-vars
function toggleSection(sectionId) {
	const section = document.getElementById(sectionId);
	const header = section?.previousElementSibling || section?.parentElement?.querySelector('.section-header');

	if (!section) return;

	const isVisible = section.style.display !== 'none';
	section.style.display = isVisible ? 'none' : 'block';

	if (header) {
		header.setAttribute('aria-expanded', !isVisible);
	}
}

// Global function for toggling all sections
// eslint-disable-next-line no-unused-vars
function toggleAllSections() {
	const toggleBtn = document.getElementById('toggle-all-btn');
	const toggleText = document.getElementById('toggle-all-text');
	const collapsibleSections = document.querySelectorAll('.collapsible-content');
	
	const isExpanding = toggleText.textContent === 'Expand All';
	
	collapsibleSections.forEach(section => {
		section.style.display = isExpanding ? 'block' : 'none';
		const header = section.previousElementSibling || section.parentElement?.querySelector('.section-header');
		if (header) {
			header.setAttribute('aria-expanded', isExpanding);
		}
	});
	
	toggleText.textContent = isExpanding ? 'Collapse All' : 'Expand All';
	toggleBtn.querySelector('.btn-icon').textContent = isExpanding ? '📁' : '📂';
}

// Initialize the application
let app;
document.addEventListener('DOMContentLoaded', () => {
	app = new MixpanelImportUI();
	// Make app globally available after initialization
	window.app = app;

	// Initialize default section states
	// All subsections start collapsed by default

	// Initialize CLI command
	if (window.app && window.app.updateCLICommand) {
		window.app.updateCLICommand();
	}
});