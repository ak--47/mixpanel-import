// @ts-nocheck
/* eslint-env browser */
/* global Dropzone, monaco */


// Mixpanel Import UI Application
class MixpanelImportUI {
	constructor() {
		this.files = [];
		this.editor = null;
		this.sampleData = []; // Store up to 500 sample records for preview
		this.initializeUI();
		this.setupEventListeners();
		this.initializeMonacoEditor();
		this.initializeETLCycling();
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

	initializeUI() {
		// Initialize Dropzone (with safety check)
		if (typeof Dropzone !== 'undefined') {
			Dropzone.autoDiscover = false;
			this.dropzone = new Dropzone('#file-dropzone', {
				url: '/upload', // This won't be used, we handle manually
				autoProcessQueue: false,
				clickable: true,
				dictDefaultMessage: '',
				previewsContainer: false,
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

				// Optional: Log the combination for debugging/fun
				console.log(`New ETL combo: ${newCombo}`);
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

		if (this.files.length === 0) {
			fileList.innerHTML = '';
			return;
		}

		fileList.innerHTML = this.files.map((file, index) => `
            <div class="file-item">
                <span class="file-name">📄 ${file.name}</span>
                <span class="file-size">${this.formatFileSize(file.size)}</span>
                <button type="button" class="file-remove" onclick="window.app.removeFile(${index})">✕</button>
            </div>
        `).join('');
	}

	formatFileSize(bytes) {
		if (bytes === 0) return '0 Bytes';
		const k = 1024;
		const sizes = ['Bytes', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
	}

	setupEventListeners() {
		// Auth method toggle
		const authRadios = document.querySelectorAll('input[name="authMethod"]');
		authRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleAuthMethod);
		});

		// File source toggle
		const fileSourceRadios = document.querySelectorAll('input[name="fileSource"]');
		fileSourceRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleFileSource.bind(this));
		});

		// Cloud paths input
		const cloudPathsInput = document.getElementById('cloudPaths');
		if (cloudPathsInput) {
			cloudPathsInput.addEventListener('input', this.updateCloudFilePreview.bind(this));
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
		this.updateFieldVisibility(); // Initial call

		// CLI command copy button
		const copyCliBtn = document.getElementById('copy-cli');
		copyCliBtn.addEventListener('click', this.copyCLICommand.bind(this));

		// Update CLI command when form changes
		form.addEventListener('input', this.updateCLICommand.bind(this));
		form.addEventListener('change', this.updateCLICommand.bind(this));
	}

	toggleAuthMethod() {
		const serviceAuth = document.getElementById('service-auth');
		const secretAuth = document.getElementById('secret-auth');
		const selectedMethod = document.querySelector('input[name="authMethod"]:checked').value;

		if (selectedMethod === 'service') {
			serviceAuth.style.display = 'block';
			secretAuth.style.display = 'none';
		} else {
			serviceAuth.style.display = 'none';
			secretAuth.style.display = 'block';
		}
	}

	toggleFileSource() {
		const fileSource = document.querySelector('input[name="fileSource"]:checked').value;
		const localUpload = document.getElementById('local-upload');
		const cloudUpload = document.getElementById('cloud-upload');

		if (fileSource === 'local') {
			localUpload.style.display = 'block';
			cloudUpload.style.display = 'none';
		} else {
			localUpload.style.display = 'none';
			cloudUpload.style.display = 'block';
		}

		// Update CLI command when file source changes
		this.updateCLICommand();
	}

	updateCloudFilePreview() {
		const cloudPathsEl = document.getElementById('cloudPaths');
		const preview = document.getElementById('cloud-file-list');

		if (!cloudPathsEl || !preview) return;

		const cloudPaths = cloudPathsEl.value;
		if (!cloudPaths.trim()) {
			preview.innerHTML = '';
			return;
		}

		const paths = cloudPaths.split(/[,\n]/).map(p => p.trim()).filter(p => p);
		const previewHTML = paths.map(path => {
			const isValid = path.startsWith('gs://');
			return `<span class="cloud-path${isValid ? '' : ' invalid'}">${path}</span>`;
		}).join('');

		preview.innerHTML = previewHTML;

		// Update CLI command when cloud paths change
		this.updateCLICommand();
	}

	updateFieldVisibility() {
		const recordType = document.getElementById('recordType').value;
		const credentialsSection = document.getElementById('credentials-section');
		const credentialsDescription = document.getElementById('credentials-description');

		// Hide all groups initially
		const allGroups = [
			'project-group', 'lookupTableId-group', 'token-group', 'groupKey-group',
			'dataGroupId-group', 'secondToken-group', 'auth-toggle', 'service-auth', 'secret-auth'
		];
		allGroups.forEach(groupId => {
			const element = document.getElementById(groupId);
			if (element) element.style.display = 'none';
		});

		// Show credentials section if a record type is selected
		if (!recordType) {
			credentialsSection.style.display = 'none';
			this.updateCLICommand(); // Update CLI when record type changes
			return;
		}

		credentialsSection.style.display = 'block';

		// Define authentication requirements based on RecordType
		switch (recordType) {
			case 'event':
			case 'user':
				// Only project token is required
				credentialsDescription.textContent = 'Events and User profiles only require a project token.';
				document.getElementById('token-group').style.display = 'block';
				break;

			case 'group':
				// Project token + groupKey is required
				credentialsDescription.textContent = 'Group profiles require a project token and group key.';
				document.getElementById('token-group').style.display = 'block';
				document.getElementById('groupKey-group').style.display = 'block';
				break;

			case 'table':
				// lookupTableId, project id, service account user + pass required
				credentialsDescription.textContent = 'Lookup tables require table ID, project ID, and service account credentials.';
				document.getElementById('lookupTableId-group').style.display = 'block';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				// Force service auth for tables
				document.querySelector('input[name=\"authMethod\"][value=\"service\"]').checked = true;
				break;




			default:
				credentialsDescription.textContent = 'Select an import type to see required authentication settings.';
		}

		// Update auth method visibility and trigger toggle
		const authToggle = document.getElementById('auth-toggle');
		if (authToggle.style.display === 'block') {
			this.toggleAuthMethod();
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

			case 'table': {
				// lookupTableId, project id, service account user + pass required
				const lookupTableId = document.getElementById('lookupTableId').value;
				const project = document.getElementById('project').value;
				const acct = document.getElementById('acct').value;
				const pass = document.getElementById('pass').value;

				if (!lookupTableId) {
					return { isValid: false, message: 'Lookup Table ID is required for table imports.' };
				}
				if (!project) {
					return { isValid: false, message: 'Project ID is required for table imports.' };
				}
				if (!acct || !pass) {
					return { isValid: false, message: 'Service account username and password are required for table imports.' };
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
			} else {
				const cloudPathsEl = document.getElementById('cloudPaths');
				const cloudPaths = cloudPathsEl ? cloudPathsEl.value : '';
				if (cloudPaths.trim()) {
					const firstPath = cloudPaths.split(/[,\n]/)[0].trim();
					if (firstPath) command += ` "${firstPath}"`;
				} else {
					command += ' gs://your-bucket/your-file.json';
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

			const lookupTableIdElement = document.getElementById('lookupTableId');
			if (lookupTableIdElement && lookupTableIdElement.closest('.form-group').style.display !== 'none' && lookupTableIdElement.value) {
				command += ` --table ${lookupTableIdElement.value}`;
			}

			const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
			if (authMethod === 'service') {
				const acctElement = document.getElementById('acct');
				const passElement = document.getElementById('pass');
				if (acctElement && acctElement.value) command += ` --acct ${acctElement.value}`;
				if (passElement && passElement.value) command += ` --pass [password]`;
			} else {
				const secretElement = document.getElementById('secret');
				if (secretElement && secretElement.value) command += ` --secret [api-secret]`;
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
	// This function is called for each record in your data.
	// you can use it to 
	//   - add new props
	//   - modify existing props
	//   - filter unwanted data (return {}) to skip)
	//   - split records (return array to create multiple records)

	// Examples:
    
    // Add a custom property
    // row.custom_source = 'my-import';
    
    // Fix timestamp format (convert to Unix milliseconds)
    // if (row.timestamp) {
    //     row.time = new Date(row.timestamp).getTime();
    // }
    
    // Rename properties
    // if (row.user_id) {
    //     row.distinct_id = row.user_id;
    //     delete row.user_id;
    // }
    
    // Skip invalid records
    // if (!row.event || !row.distinct_id) {
    //     return {}; // Skip this record
    // }
    
    // Split one record into multiple
    // if (row.events && Array.isArray(row.events)) {
    //     return row.events.map(event => ({
    //         event: event.name,
    //         properties: { ...row.properties, ...event.properties }
    //     }));
    // }
	
	// always return the (possibly modified) record
	// you can return {} which will exclude the record (like a filter)
    return row; 
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

		//check for cloud mode files
		const cloudPathsInput = document.getElementById('cloudPaths').value;
		if (cloudPathsInput) {
			const cloudPaths = cloudPathsInput.split(/[,\n]/).map(p => p.trim()).filter(p => p);
			formData.append('cloudPaths', JSON.stringify(cloudPaths));
		}

		// Collect credentials
		const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value || 'service';
		const credentials = {};

		// Add project ID if visible
		const projectElement = document.getElementById('project');
		if (projectElement && projectElement.closest('.form-group').style.display !== 'none' && projectElement.value) {
			credentials.project = projectElement.value;
		}

		// Add lookup table ID if visible
		const lookupTableIdElement = document.getElementById('lookupTableId');
		if (lookupTableIdElement && lookupTableIdElement.closest('.form-group').style.display !== 'none' && lookupTableIdElement.value) {
			credentials.lookupTableId = lookupTableIdElement.value;
		}

		// Add authentication based on method
		if (authMethod === 'service') {
			const acctElement = document.getElementById('acct');
			const passElement = document.getElementById('pass');
			if (acctElement && acctElement.value) credentials.acct = acctElement.value;
			if (passElement && passElement.value) credentials.pass = passElement.value;
		} else {
			const secretElement = document.getElementById('secret');
			if (secretElement && secretElement.value) credentials.secret = secretElement.value;
		}

		// Add optional fields if visible and have values
		const tokenElement = document.getElementById('token');
		if (tokenElement && tokenElement.closest('.form-group').style.display !== 'none' && tokenElement.value) {
			credentials.token = tokenElement.value;
		}

		const groupKeyElement = document.getElementById('groupKey');
		if (groupKeyElement && groupKeyElement.closest('.form-group').style.display !== 'none' && groupKeyElement.value) {
			credentials.groupKey = groupKeyElement.value;
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

			if (fileSource === 'cloud') {
				const cloudPaths = document.getElementById('cloudPaths').value;
				if (!cloudPaths.trim()) {
					this.showError('Please enter at least one cloud storage path to preview.');
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

			if (fileSource === 'cloud') {
				const cloudPaths = document.getElementById('cloudPaths').value;
				if (!cloudPaths.trim()) {
					this.showError('Please enter at least one cloud storage path.');
					return;
				}

				const paths = cloudPaths.split(/[,\n]/).map(p => p.trim()).filter(p => p);
				const invalidPaths = paths.filter(p => !p.startsWith('gs://'));
				if (invalidPaths.length > 0) {
					this.showError('All cloud paths must start with gs://. Invalid paths: ' + invalidPaths.join(', '));
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

			this.hideLoading();

			if (result.success) {
				this.showResults(result, isDryRun);
			} else {
				this.showError(`${isDryRun ? 'Test' : 'Import'} failed: ${result.error}`);
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

	showResults(result, isDryRun) {
		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = isDryRun ? 'Preview Results' : 'Import Complete!';

		// For dry runs with both raw and transformed data, show side-by-side comparison
		if (isDryRun && result.rawData && result.previewData) {
			this.showSideBySideComparison(result.rawData, result.previewData);
		} else {
			// Regular single display for non-dry runs or legacy dry runs
			let displayData = result.result;
			
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
		const rawPanel = document.getElementById('raw-panel');
		const transformedPanel = document.getElementById('transformed-panel');
		
		let isScrolling = false;
		
		const syncScroll = (source, target) => {
			if (!isScrolling) {
				isScrolling = true;
				target.scrollTop = source.scrollTop;
				setTimeout(() => { isScrolling = false; }, 10);
			}
		};
		
		rawPanel.addEventListener('scroll', () => syncScroll(rawPanel, transformedPanel));
		transformedPanel.addEventListener('scroll', () => syncScroll(transformedPanel, rawPanel));
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
		return jsonString
			.replace(/("([^"\\]|\\.)*")\s*:/g, '<span class="json-key">$1</span>:')
			.replace(/:\s*("([^"\\]|\\.)*")/g, ': <span class="json-string">$1</span>')
			.replace(/:\s*(true|false)/g, ': <span class="json-boolean">$1</span>')
			.replace(/:\s*(null)/g, ': <span class="json-null">$1</span>')
			.replace(/:\s*(-?\d+\.?\d*)/g, ': <span class="json-number">$1</span>')
			.replace(/([{}[\]])/g, '<span class="json-punctuation">$1</span>');
	}
}

// Global function for collapsible sections
// Used by HTML onclick handlers
// eslint-disable-next-line no-unused-vars
function toggleSection(sectionId) {
	const content = document.getElementById(sectionId);
	const header = content.previousElementSibling;
	const isVisible = content.style.display !== 'none';

	if (isVisible) {
		content.style.display = 'none';
		header.classList.remove('expanded');
	} else {
		content.style.display = 'block';
		header.classList.add('expanded');
	}
}

// Global function to toggle all collapsible sections
// eslint-disable-next-line no-unused-vars
function toggleAllSections() {
	const sections = document.querySelectorAll('.collapsible-content');
	const toggleBtn = document.getElementById('toggle-all-btn');
	const toggleText = document.getElementById('toggle-all-text');
	const btnIcon = toggleBtn.querySelector('.btn-icon');

	// Check if any sections are currently expanded
	const anyExpanded = Array.from(sections).some(section => section.style.display === 'block');

	sections.forEach(section => {
		const header = section.previousElementSibling;

		if (anyExpanded) {
			// Collapse all
			section.style.display = 'none';
			header.classList.remove('expanded');
		} else {
			// Expand all
			section.style.display = 'block';
			header.classList.add('expanded');
		}
	});

	// Update button text and icon
	if (anyExpanded) {
		toggleText.textContent = 'Collapse All';
		btnIcon.textContent = '📁';
	} else {
		toggleText.textContent = 'Expand All';
		btnIcon.textContent = '📂';
	}
}

// Initialize the application
let app;
document.addEventListener('DOMContentLoaded', () => {
	app = new MixpanelImportUI();
	// Make app globally available after initialization
	window.app = app;

	// Initialize default section states
	// Keep performance section open by default
	const performanceSection = document.getElementById('performance-section');
	const performanceHeader = performanceSection?.previousElementSibling;
	if (performanceSection) {
		performanceSection.style.display = 'block';
		performanceHeader?.classList.add('expanded');
	}

	// Initialize CLI command
	if (window.app && window.app.updateCLICommand) {
		window.app.updateCLICommand();
	}
});