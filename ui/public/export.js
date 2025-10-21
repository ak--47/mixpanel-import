// @ts-nocheck
/* eslint-env browser */

// Mixpanel Export UI Application (L.T.E Tool)
class MixpanelExportUI {
	constructor() {
		this.websocket = null; // WebSocket connection for real-time progress
		this.currentJobId = null; // Track current job ID
		this.initializeLTECycling();
		this.setupEventListeners();
	}

	// WebSocket connection methods
	connectWebSocket(jobId) {
		try {
			const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
			const wsUrl = `${protocol}//${window.location.host}`;
			
			this.websocket = new WebSocket(wsUrl);
			this.currentJobId = jobId;
			
			this.websocket.onopen = () => {
				console.log('WebSocket connected for export job:', jobId);
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
				console.log('WebSocket disconnected');
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
				console.log('Export job registration confirmed:', data.jobId);
				break;
				
			case 'progress':
				this.updateProgressDisplay(data.data);
				break;
				
			case 'job-complete':
				console.log('Export job completed:', data.result);
				this.hideLoading();
				this.showResults(data.result);
				
				// If the export produced files, automatically download them
				if (data.result.downloadUrl) {
					this.downloadExportFile(data.result.downloadUrl, data.result.files);
				}
				
				this.disconnectWebSocket();
				break;
				
			case 'job-error':
				console.error('Export job failed:', data.error);
				this.hideLoading();
				this.showError(`Export failed: ${data.error}`);
				this.disconnectWebSocket();
				break;
				
			default:
				console.log('Unknown WebSocket message type:', data.type);
		}
	}
	
	updateProgressDisplay(progressData) {
		// Update the loading message with real-time progress
		const loadingMessage = document.querySelector('.loading-details');
		if (loadingMessage) {
			const { recordType, processed, requests, eps, memory, bytesProcessed, downloadMessage } = progressData;

			const formatNumber = (num) => {
				const parsed = typeof num === 'number' ? num : parseFloat(num);
				if (!isNaN(parsed)) {
					return Math.round(parsed).toLocaleString();
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
					${downloadMessage ? `
					<div class="stat-item download-progress">
						<span class="stat-label">📥 Download:</span>
						<span class="stat-value">${downloadMessage.replace('downloaded: ', '')}</span>
					</div>` : ''}
					${(processed !== null && processed !== undefined && processed > 0) ? `
					<div class="stat-item">
						<span class="stat-label">${recordType || 'Records'}:</span>
						<span class="stat-value">${formatNumber(processed)}</span>
					</div>` : ''}
					${requests ? `
					<div class="stat-item">
						<span class="stat-label">Requests:</span>
						<span class="stat-value">${formatNumber(requests)}</span>
					</div>` : ''}
					${eps ? `
					<div class="stat-item">
						<span class="stat-label">Events/sec:</span>
						<span class="stat-value">${formatNumber(eps)}</span>
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
			// Set record type to export events
			const recordTypeSelect = document.getElementById('recordType');
			if (recordTypeSelect) {
				recordTypeSelect.value = 'export';
				recordTypeSelect.dispatchEvent(new Event('change'));
			}

			// Set auth method to service account
			const serviceAccountRadio = document.querySelector('input[name="authMethod"][value="serviceAccount"]');
			if (serviceAccountRadio) {
				serviceAccountRadio.checked = true;
				serviceAccountRadio.dispatchEvent(new Event('change'));
			}

			// Fill credentials after a brief delay to ensure visibility
			setTimeout(() => {
				// Project ID
				const projectInput = document.getElementById('project');
				if (projectInput) {
					projectInput.value = '3730336';
				}

				// Service account
				const acctInput = document.getElementById('acct');
				if (acctInput) {
					acctInput.value = 'quicktest.0a627b.mp-service-account';
				}

				// Service account password
				const passInput = document.getElementById('pass');
				if (passInput) {
					passInput.value = 'QwZbu3l4dGXIuiSMHE0cHhIlXK4aGzhq';
				}
			}, 100);

			// Set date range to 2025-10-01 to 2025-10-01
			const startInput = document.getElementById('start');
			const endInput = document.getElementById('end');
			if (startInput && endInput) {
				startInput.value = '2025-10-01';
				endInput.value = '2025-10-01';
			}

			// Enable show progress
			const showProgressCheckbox = document.getElementById('showProgress');
			if (showProgressCheckbox) {
				showProgressCheckbox.checked = true;
			}

			console.log('Export dev values filled successfully');
		} catch (error) {
			console.error('Failed to fill export dev values:', error);
		}
	}

	// Reset form to initial state
	resetForm() {
		try {
			// Clear session storage
			sessionStorage.removeItem('export-form-state');

			// Reset the entire form
			const form = document.getElementById('exportForm');
			if (form) {
				form.reset();
			}

			// Reset record type to empty (to hide credential fields)
			const recordTypeSelect = document.getElementById('recordType');
			if (recordTypeSelect) {
				recordTypeSelect.value = '';
				recordTypeSelect.dispatchEvent(new Event('change'));
			}

			// Reset auth method to service account
			const serviceAccountRadio = document.querySelector('input[name="authMethod"][value="serviceAccount"]');
			if (serviceAccountRadio) {
				serviceAccountRadio.checked = true;
				serviceAccountRadio.dispatchEvent(new Event('change'));
			}

			// Hide results section
			const results = document.getElementById('results');
			if (results) results.style.display = 'none';

			const loading = document.getElementById('loading');
			if (loading) loading.style.display = 'none';

			console.log('Export form reset successfully');
		} catch (error) {
			console.error('Failed to reset export form:', error);
		}
	}

	// Save form state to session storage
	saveFormState() {
		try {
			const formState = {};

			// Save all text inputs, selects, and textareas
			const inputs = document.querySelectorAll('#exportForm input[type="text"], #exportForm input[type="password"], #exportForm input[type="date"], #exportForm input[type="number"], #exportForm select, #exportForm textarea');
			inputs.forEach(input => {
				if (input.id && input.value) {
					formState[input.id] = input.value;
				}
			});

			// Save radio buttons
			const radios = document.querySelectorAll('#exportForm input[type="radio"]:checked');
			radios.forEach(radio => {
				if (radio.name) {
					formState[`radio-${radio.name}`] = radio.value;
				}
			});

			// Save checkboxes
			const checkboxes = document.querySelectorAll('#exportForm input[type="checkbox"]');
			checkboxes.forEach(checkbox => {
				if (checkbox.id) {
					formState[`checkbox-${checkbox.id}`] = checkbox.checked;
				}
			});

			sessionStorage.setItem('export-form-state', JSON.stringify(formState));
		} catch (error) {
			// Silently fail - don't break the user experience
			console.debug('Could not save form state:', error);
		}
	}

	// Load form state from session storage
	loadFormState() {
		try {
			const savedState = sessionStorage.getItem('export-form-state');
			if (!savedState) return;

			const formState = JSON.parse(savedState);

			// Restore text inputs, selects, and textareas
			Object.keys(formState).forEach(key => {
				if (key.startsWith('radio-')) {
					// Restore radio button
					const radioName = key.replace('radio-', '');
					const radio = document.querySelector(`input[name="${radioName}"][value="${formState[key]}"]`);
					if (radio) {
						radio.checked = true;
						radio.dispatchEvent(new Event('change'));
					}
				} else if (key.startsWith('checkbox-')) {
					// Restore checkbox
					const checkboxId = key.replace('checkbox-', '');
					const checkbox = document.getElementById(checkboxId);
					if (checkbox) {
						checkbox.checked = formState[key];
					}
				} else {
					// Restore regular input
					const input = document.getElementById(key);
					if (input) {
						input.value = formState[key];
					}
				}
			});

			console.log('Export form state restored from session storage');
		} catch (error) {
			// Silently fail - just load initial state
			console.debug('Could not load form state:', error);
		}
	}

	initializeLTECycling() {
		// Separate word banks for L, T, and E
		const lWords = [
			'Load', 'Launch', 'Leverage', 'Lift', 'Logic',
			'Library', 'Layer', 'Lake', 'Link', 'List',
			'Listen', 'Locate', 'Lock', 'Log', 'Loop',
			'Latch', 'Learn', 'Lease', 'Leave', 'Lecture',
			'Ledger', 'Legitimize', 'Lend', 'Lengthen', 'Lesson',
			'Let', 'Level', 'Levy', 'Liberate', 'License',
			'Lick', 'Lie', 'Lighten', 'Like', 'Limit',
			'Line', 'Linger', 'Liquidate', 'Liquefy', 'Literalize',
			'Litigate', 'Litter', 'Live', 'Livestream', 'Lobby',
			'Localize', 'Lodge', 'Loft', 'Loiter', 'Look',
			'Loom', 'Loosen', 'Loot', 'Lose', 'Lounge',
			'Love', 'Lower', 'Lubricate', 'Lucid', 'Lug',
			'Lull', 'Lumber', 'Lump', 'Lunge', 'Lure',
			'Lurk', 'Lust', 'Luxuriate', 'Label', 'Labor',
			'Lace', 'Lack', 'Ladder', 'Ladle', 'Lag',
			'Lament', 'Laminate', 'Land', 'Landscape', 'Language',
			'Languish', 'Lap', 'Lapse', 'Lard', 'Large',
			'Lash', 'Last', 'Lather', 'Laud', 'Laugh',
			'Launder', 'Lavish', 'Law', 'Lay', 'Lazy',
			'Lead', 'Leaf', 'Leak', 'Lean', 'Leap',
			'Lease', 'Leash', 'Leather', 'Lecture', 'Ledge',
			'Leech', 'Leer', 'Left', 'Legal', 'Legend'
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
			'Touch', 'Tour', 'Tow', 'Toy', 'Traceability',
			'Trade', 'Trail', 'Trample', 'Transfix', 'Transgress',
			'Transit', 'Trap', 'Trash', 'Travel', 'Trawl',
			'Tread', 'Treasure', 'Trek', 'Tremble', 'Trick'
		];

		const eWords = [
			'Export', 'Execute', 'Extract', 'Expand', 'Elevate',
			'Edge', 'Enterprise', 'Endpoint', 'Entity', 'Engine',
			'Event', 'Elasticsearch', 'Encode', 'Encrypt', 'Enhance',
			'Establish', 'Evolve', 'Examine', 'Explore', 'Express',
			'Extend', 'Embed', 'Enable', 'Enforce', 'Engage',
			'Ensure', 'Enumerate', 'Equalize', 'Estimate', 'Evoke',
			'Exceed', 'Exchange', 'Exclude', 'Exemplify', 'Exhaust',
			'Exhibit', 'Expedite', 'Experiment', 'Exploit', 'Expose',
			'Externalize', 'Extrapolate', 'Extrude', 'Elaborate', 'Elect',
			'Eliminate', 'Elucidate', 'Emanate', 'Embrace', 'Emerge',
			'Emit', 'Emphasize', 'Employ', 'Empower', 'Emulate',
			'Encapsulate', 'Encompass', 'Encounter', 'Energize', 'Engineer',
			'Engrave', 'Enjoy', 'Enlarge', 'Enlighten', 'Enlist',
			'Enqueue', 'Enrich', 'Enroll', 'Ensemble', 'Entangle',
			'Enter', 'Entertain', 'Entice', 'Entrench', 'Entrust',
			'Envelop', 'Envision', 'Epitomize', 'Equip', 'Eradicate',
			'Erect', 'Escalate', 'Escape', 'Escort', 'Etch',
			'Evaporate', 'Evict', 'Evidence', 'Exacerbate', 'Exalt',
			'Excavate', 'Excel', 'Excise', 'Excite', 'Exclaim',
			'Exclude', 'Excuse', 'Exemplar', 'Exert', 'Exfoliate',
			'Exhale', 'Exhilarate', 'Exhort', 'Exile', 'Exist',
			'Exit', 'Exonerate', 'Expand', 'Expatriate', 'Expect',
			'Expel', 'Experience', 'Expire', 'Explain', 'Explode'
		];

		// Store previously used combinations to avoid immediate repeats
		const recentCombos = [];
		const maxRecent = 20; // Remember last 20 combinations

		const descriptionElement = document.getElementById('lte-description');

		if (!descriptionElement) return;

		// Function to generate a random combination
		const generateRandomLTE = () => {
			let combo;
			let attempts = 0;
			const maxAttempts = 50;

			do {
				const l = lWords[Math.floor(Math.random() * lWords.length)];
				const t = tWords[Math.floor(Math.random() * tWords.length)];
				const e = eWords[Math.floor(Math.random() * eWords.length)];
				combo = `${l} ${t} ${e}`;
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
				const newCombo = generateRandomLTE();
				descriptionElement.textContent = newCombo;
				descriptionElement.classList.remove('fading');

				// Optional: Log the combination for debugging/fun
				// console.log(`New LTE combo: ${newCombo}`);
			}, 250); // Half of the transition time
		};

		// Set initial random combination
		descriptionElement.textContent = generateRandomLTE();

		// Start cycling every 10 seconds
		setInterval(cycleDescription, 10000);

		// Optional: Add click handler for manual cycling
		descriptionElement.style.cursor = 'pointer';
		descriptionElement.title = 'Click for new combination';
		descriptionElement.addEventListener('click', cycleDescription);
	}

	setupEventListeners() {
		// Auth method toggle
		const authRadios = document.querySelectorAll('input[name="authMethod"]');
		authRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleAuthMethod);
		});

		// Form submission
		const form = document.getElementById('exportForm');
		form.addEventListener('submit', (e) => {
			e.preventDefault();
			this.submitExport();
		});

		// Dry run button
		const dryRunBtn = document.getElementById('dry-run-btn');
		dryRunBtn.addEventListener('click', () => {
			this.submitExport(true);
		});

		// Test run button (1000 records)
		const testRunBtn = document.getElementById('test-run-btn');
		testRunBtn.addEventListener('click', () => {
			this.submitExport(false, 1000);
		});

		// Destination type toggle
		const destinationRadios = document.querySelectorAll('input[name="destinationType"]');
		destinationRadios.forEach(radio => {
			radio.addEventListener('change', this.toggleDestinationType);
		});
		this.toggleDestinationType(); // Initial call

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

		// Session storage persistence - save form state on input/change
		form.addEventListener('input', this.saveFormState.bind(this));
		form.addEventListener('change', this.saveFormState.bind(this));

		// Load saved form state on page load
		this.loadFormState();
	}

	toggleAuthMethod() {
		const serviceAuth = document.getElementById('service-auth');
		const secretAuth = document.getElementById('secret-auth');
		const projectGroup = document.getElementById('project-group');
		const selectedMethod = document.querySelector('input[name="authMethod"]:checked').value;

		if (selectedMethod === 'service') {
			serviceAuth.style.display = 'block';
			secretAuth.style.display = 'none';
			// Show project ID for service account
			if (projectGroup) projectGroup.style.display = 'block';
		} else {
			serviceAuth.style.display = 'none';
			secretAuth.style.display = 'block';
			// Hide project ID for API secret (not needed)
			if (projectGroup) projectGroup.style.display = 'none';
		}
	}

	toggleDestinationType() {
		const localDestination = document.getElementById('local-destination');
		const gcsDestination = document.getElementById('gcs-destination');
		const s3Destination = document.getElementById('s3-destination');
		const selectedDestination = document.querySelector('input[name="destinationType"]:checked')?.value || 'local';

		// Hide all destination sections
		localDestination.style.display = 'none';
		gcsDestination.style.display = 'none';
		s3Destination.style.display = 'none';

		// Show selected destination section
		switch (selectedDestination) {
			case 'local':
				localDestination.style.display = 'block';
				break;
			case 'gcs':
				gcsDestination.style.display = 'block';
				break;
			case 's3':
				s3Destination.style.display = 'block';
				break;
		}
	}

	updateFieldVisibility() {
		const recordType = document.getElementById('recordType').value;
		const credentialsSection = document.getElementById('credentials-section');
		const credentialsDescription = document.getElementById('credentials-description');

		// Hide all groups initially
		const allGroups = [
			'project-group', 'lookupTableId-group', 'token-group', 'groupKey-group',
			'dataGroupId-group', 'secondToken-group', 'secondRegion-group', 'auth-toggle', 
			'service-auth', 'secret-auth', 'destination-title', 'destination-description'
		];
		allGroups.forEach(groupId => {
			const element = document.getElementById(groupId);
			if (element) element.style.display = 'none';
		});

		// Show credentials section if a record type is selected
		if (!recordType) {
			credentialsSection.style.display = 'none';
			this.updateCLICommand();
			return;
		}

		credentialsSection.style.display = 'block';

		// Define authentication requirements based on export type
		switch (recordType) {
			case 'export':
				// Events: API secret OR service account (with project ID) required, start/end dates required
				credentialsDescription.textContent = 'Event exports require API secret OR service account (with project ID). Start and end dates are required.';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				break;

			case 'profile-export':
			case 'profile-delete':
				// User profiles: API secret OR service account (with project ID) required
				credentialsDescription.textContent = 'User profile operations require API secret OR service account (with project ID).';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				break;

			case 'group-export':
			case 'group-delete':
				// Group profiles: API secret OR service account (with project ID) + groupKey + dataGroupId required
				credentialsDescription.textContent = 'Group profile operations require API secret OR service account (with project ID), plus groupKey and dataGroupId.';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('groupKey-group').style.display = 'block';
				document.getElementById('dataGroupId-group').style.display = 'block';
				break;

			case 'scd':
			case 'annotations':
			case 'get-annotations':
			case 'delete-annotations':
				// Other operations: service account required
				credentialsDescription.textContent = 'This operation requires service account credentials and project ID.';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				// Force service auth
				document.querySelector('input[name="authMethod"][value="service"]').checked = true;
				break;

			case 'export-import-events':
				// Export-import events: project ID + token/secret OR service account + start/end dates + optional destination
				credentialsDescription.textContent = 'Export-import events require source project credentials (API secret OR service account with project ID) and source data residency. Optionally specify destination token and residency (leave blank to reimport to same project).';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('token-group').style.display = 'block';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('destination-title').style.display = 'block';
				document.getElementById('destination-description').style.display = 'block';
				document.getElementById('secondToken-group').style.display = 'block';
				document.getElementById('secondRegion-group').style.display = 'block';
				break;

			case 'export-import-profiles':
				// Export-import user profiles: project ID + token/secret OR service account + optional destination
				credentialsDescription.textContent = 'Export-import profiles require source project credentials (API secret OR service account with project ID) and source data residency. Optionally specify destination token and residency (leave blank to reimport to same project).';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('token-group').style.display = 'block';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('destination-title').style.display = 'block';
				document.getElementById('destination-description').style.display = 'block';
				document.getElementById('secondToken-group').style.display = 'block';
				document.getElementById('secondRegion-group').style.display = 'block';
				break;

			case 'export-import-groups':
				// Export-import group profiles: project ID + token/secret OR service account + groupKey + dataGroupId + optional destination
				credentialsDescription.textContent = 'Export-import groups require source project credentials (API secret OR service account with project ID), source data residency, groupKey, and dataGroupId. Optionally specify destination token and residency (leave blank to reimport to same project).';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('token-group').style.display = 'block';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('groupKey-group').style.display = 'block';
				document.getElementById('dataGroupId-group').style.display = 'block';
				document.getElementById('destination-title').style.display = 'block';
				document.getElementById('destination-description').style.display = 'block';
				document.getElementById('secondToken-group').style.display = 'block';
				document.getElementById('secondRegion-group').style.display = 'block';
				break;

			default:
				credentialsDescription.textContent = 'Select an export type to see required authentication settings.';
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
			case 'export':
			case 'profile-export':
			case 'profile-delete': {
				// API secret OR service user/pass (with project_id) required
				const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
				if (authMethod === 'service') {
					const exportProject = document.getElementById('project').value;
					if (!exportProject) {
						return { isValid: false, message: 'Project ID is required when using service account authentication.' };
					}
					const exportAcct = document.getElementById('acct').value;
					const exportPass = document.getElementById('pass').value;
					if (!exportAcct || !exportPass) {
						return { isValid: false, message: 'Service account credentials are required for exports.' };
					}
				} else {
					const exportSecret = document.getElementById('secret').value;
					if (!exportSecret) {
						return { isValid: false, message: 'API secret is required for exports.' };
					}
				}
				break;
			}

			case 'scd':
			case 'annotations':
			case 'get-annotations':
			case 'delete-annotations': {
				// Service user/pass and project_id required
				const scdProject = document.getElementById('project').value;
				const scdAcct = document.getElementById('acct').value;
				const scdPass = document.getElementById('pass').value;

				if (!scdProject) {
					return { isValid: false, message: 'Project ID is required for this operation.' };
				}
				if (!scdAcct || !scdPass) {
					return { isValid: false, message: 'Service account credentials are required for this operation.' };
				}
				break;
			}

			case 'group-export':
			case 'group-delete': {
				// API secret OR service user/pass (with project_id) + groupKey + dataGroupId required
				const groupKey = document.getElementById('groupKey').value;
				const dataGroupId = document.getElementById('dataGroupId').value;
				if (!groupKey) {
					return { isValid: false, message: 'Group key is required for group operations.' };
				}
				if (!dataGroupId) {
					return { isValid: false, message: 'Data group ID is required for group operations.' };
				}

				const groupAuthMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
				if (groupAuthMethod === 'service') {
					const groupProject = document.getElementById('project').value;
					if (!groupProject) {
						return { isValid: false, message: 'Project ID is required when using service account authentication.' };
					}
					const groupAcct = document.getElementById('acct').value;
					const groupPass = document.getElementById('pass').value;
					if (!groupAcct || !groupPass) {
						return { isValid: false, message: 'Service account credentials are required for group operations.' };
					}
				} else {
					const groupSecret = document.getElementById('secret').value;
					if (!groupSecret) {
						return { isValid: false, message: 'API secret is required for group operations.' };
					}
				}
				break;
			}

			case 'export-import-events':
			case 'export-import-profiles': {
				// API secret OR service user/pass and project_id required
				const eiProject = document.getElementById('project').value;
				if (!eiProject) {
					return { isValid: false, message: 'Project ID is required for export-import operations.' };
				}

				const eiAuthMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
				if (eiAuthMethod === 'service') {
					const eiAcct = document.getElementById('acct').value;
					const eiPass = document.getElementById('pass').value;
					if (!eiAcct || !eiPass) {
						return { isValid: false, message: 'Service account credentials are required for export-import operations.' };
					}
				} else {
					const eiSecret = document.getElementById('secret').value;
					if (!eiSecret) {
						return { isValid: false, message: 'API secret is required for export-import operations.' };
					}
				}
				break;
			}

			case 'export-import-groups': {
				// API secret OR service user/pass and project_id + groupKey + dataGroupId required
				const eigProject = document.getElementById('project').value;
				if (!eigProject) {
					return { isValid: false, message: 'Project ID is required for export-import group operations.' };
				}

				const eigGroupKey = document.getElementById('groupKey').value;
				const eigDataGroupId = document.getElementById('dataGroupId').value;
				if (!eigGroupKey) {
					return { isValid: false, message: 'Group key is required for export-import group operations.' };
				}
				if (!eigDataGroupId) {
					return { isValid: false, message: 'Data group ID is required for export-import group operations.' };
				}

				const eigAuthMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
				if (eigAuthMethod === 'service') {
					const eigAcct = document.getElementById('acct').value;
					const eigPass = document.getElementById('pass').value;
					if (!eigAcct || !eigPass) {
						return { isValid: false, message: 'Service account credentials are required for export-import group operations.' };
					}
				} else {
					const eigSecret = document.getElementById('secret').value;
					if (!eigSecret) {
						return { isValid: false, message: 'API secret is required for export-import group operations.' };
					}
				}
				break;
			}

			default:
				return { isValid: false, message: 'Please select a valid export type.' };
		}

		// Validate cloud destination settings
		const destinationType = document.querySelector('input[name="destinationType"]:checked')?.value || 'local';
		if (destinationType === 'gcs') {
			const gcsPath = document.getElementById('gcsPath').value;
			if (!gcsPath) {
				return { isValid: false, message: 'GCS path is required when using Google Cloud Storage destination.' };
			}
			if (!gcsPath.startsWith('gs://')) {
				return { isValid: false, message: 'GCS path must start with gs:// (e.g., gs://bucket/path/file.jsonl).' };
			}
		} else if (destinationType === 's3') {
			const s3Path = document.getElementById('s3Path').value;
			const s3Region = document.getElementById('s3Region').value;
			if (!s3Path) {
				return { isValid: false, message: 'S3 path is required when using Amazon S3 destination.' };
			}
			if (!s3Path.startsWith('s3://')) {
				return { isValid: false, message: 'S3 path must start with s3:// (e.g., s3://bucket/path/file.jsonl).' };
			}
			if (!s3Region) {
				return { isValid: false, message: 'S3 region is required when using Amazon S3 destination.' };
			}
		}

		return { isValid: true };
	}

	updateCLICommand() {
		const cliElement = document.getElementById('cli-command');

		try {
			const recordType = document.getElementById('recordType').value;
			if (!recordType) {
				cliElement.textContent = 'Select an export type to generate CLI command...';
				cliElement.classList.add('empty');
				return;
			}

			let command = 'npx mixpanel-import';

			// Core options
			command += ` --type ${recordType}`;

			// Credentials - only add if fields are visible and have values
			const projectElement = document.getElementById('project');
			if (projectElement && projectElement.closest('.form-group').style.display !== 'none' && projectElement.value) {
				command += ` --project ${projectElement.value}`;
			}

			const tokenElement = document.getElementById('token');
			if (tokenElement && tokenElement.closest('.form-group').style.display !== 'none' && tokenElement.value) {
				command += ` --token [project-token]`;
			}

			// Auth credentials (redact secrets for security)
			const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
			if (authMethod === 'service') {
				const acct = this.getElementValue('acct');
				const pass = this.getElementValue('pass');
				if (acct) command += ` --acct ${acct}`;
				if (pass) command += ` --pass [password]`;
			} else {
				const secret = this.getElementValue('secret');
				if (secret) {
					command += ` --secret [api-secret]`;
				}
			}

			// Additional options with values
			const optionsMap = {
				'groupKey': '--groupKey',
				'dataGroupId': '--dataGroupId',
				'region': '--region',
				'workers': '--workers',
				'start': '--start',
				'end': '--end',
				'epochStart': '--epochStart',
				'epochEnd': '--epochEnd',
				'whereClause': '--whereClause',
				'limit': '--limit',
				'where': '--where',
				'outputFilePath': '--outputFilePath'
			};

			Object.entries(optionsMap).forEach(([fieldId, flag]) => {
				const value = this.getElementValue(fieldId);
				if (value) {
					command += ` ${flag} "${value}"`;
				}
			});

			// Handle secondToken separately (redact for security)
			const secondToken = this.getElementValue('secondToken');
			if (secondToken) {
				command += ` --secondToken [destination-token]`;
			}

			// Boolean flags
			const booleanFlags = {
				'logs': '--logs',
				'verbose': '--verbose',
				'showProgress': '--showProgress',
				'writeToFile': '--writeToFile'
			};

			Object.entries(booleanFlags).forEach(([fieldId, flag]) => {
				if (this.getElementChecked(fieldId)) {
					command += ` ${flag}`;
				}
			});

			cliElement.textContent = command;
			cliElement.classList.remove('empty');

		} catch (error) {
			console.error('Error updating CLI command:', error);
			cliElement.textContent = 'Error generating CLI command';
			cliElement.classList.add('empty');
		}
	}

	copyCLICommand() {
		const cliElement = document.getElementById('cli-command');
		const command = cliElement.textContent;

		if (command && !command.includes('Select') && !command.includes('Error')) {
			navigator.clipboard.writeText(command).then(() => {
				// Visual feedback
				const copyBtn = document.getElementById('copy-cli');
				const originalText = copyBtn.innerHTML;
				copyBtn.innerHTML = '<span class="btn-icon">✅</span>Copied!';
				setTimeout(() => {
					copyBtn.innerHTML = originalText;
				}, 2000);
			}).catch(err => {
				console.error('Failed to copy command:', err);
				alert('Failed to copy command. Please select and copy manually.');
			});
		}
	}

	async submitExport(isDryRun = false, maxRecords = null) {
		const recordType = document.getElementById('recordType').value;

		// Skip credential validation for dry runs (dry runs don't require credentials)
		if (!isDryRun) {
			// Validate required fields (including cloud destinations)
			const validation = this.validateRequiredFields(recordType);
			if (!validation.isValid) {
				alert(validation.message);
				return;
			}
		}

		// Set loading message based on operation type
		let loadingTitle, loadingMessage;
		if (isDryRun) {
			loadingTitle = 'Testing Export...';
			loadingMessage = 'Running export test';
		} else if (maxRecords) {
			loadingTitle = 'Test Run...';
			loadingMessage = `Testing export with up to ${maxRecords.toLocaleString()} records`;
		} else {
			loadingTitle = 'Exporting Data...';
			loadingMessage = 'Exporting your data from Mixpanel';
		}

		this.showLoading(loadingTitle, loadingMessage);

		try {
			// Collect form data
			const formData = this.collectFormData();
			formData.isDryRun = isDryRun;
			
			// Apply maxRecords limit for Test Run
			if (maxRecords) {
				formData.limit = Math.min(formData.limit || maxRecords, maxRecords);
			}

			// Submit to appropriate endpoint
			const endpoint = isDryRun ? '/export-dry-run' : '/export';
			const response = await fetch(endpoint, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json'
				},
				body: JSON.stringify(formData)
			});

			const result = await response.json();

			// Handle different responses for dry runs vs real exports
			if (isDryRun) {
				this.hideLoading();
				if (result.success) {
					this.showResults(result, isDryRun);
				} else {
					throw new Error(result.error || 'Export test failed');
				}
			} else {
				// For real exports, the server returns a jobId and runs asynchronously
				if (result.success && result.jobId) {
					// Connect WebSocket for real-time progress updates
					this.connectWebSocket(result.jobId);
					// Update loading message to show that export has started
					const loadingMessage = document.querySelector('.loading-details');
					if (loadingMessage) {
						loadingMessage.innerHTML = 'Export started - connecting for real-time updates...';
					}
				} else {
					this.hideLoading();
					throw new Error(result.error || 'Export failed');
				}
			}

		} catch (error) {
			console.error('Export error:', error);
			this.hideLoading();
			alert(`Export failed: ${error.message}`);
		}
	}

	collectFormData() {
		// Collect all form fields into structured data
		const data = {
			recordType: this.getElementValue('recordType'),

			// Credentials
			project: this.getElementValue('project'),
			token: this.getElementValue('token'),
			secret: this.getElementValue('secret'),
			acct: this.getElementValue('acct'),
			pass: this.getElementValue('pass'),
			groupKey: this.getElementValue('groupKey'),
			dataGroupId: this.getElementValue('dataGroupId'),
			secondToken: this.getElementValue('secondToken'),

			// Configuration
			region: this.getElementValue('region', 'US'),
			workers: parseInt(this.getElementValue('workers', '10')),

			// Time filters
			start: this.getElementValue('start'),
			end: this.getElementValue('end'),
			epochStart: this.getElementValue('epochStart'),
			epochEnd: this.getElementValue('epochEnd'),

			// Advanced filters
			whereClause: this.getElementValue('whereClause'),
			limit: this.getElementValue('limit'),

			// Output options
			logs: this.getElementChecked('logs'),
			verbose: this.getElementChecked('verbose'),
			showProgress: this.getElementChecked('showProgress'),
			writeToFile: this.getElementChecked('writeToFile'),
			where: this.getElementValue('where'),
			outputFilePath: this.getElementValue('outputFilePath'),

			// Cloud destination options
			destinationType: this.getElementValue('destinationType', 'local'),
			gcsPath: this.getElementValue('gcsPath'),
			gcpProjectId: this.getElementValue('gcpProjectId'),
			gcsCredentials: this.getElementValue('gcsCredentials'),
			s3Path: this.getElementValue('s3Path'),
			s3Region: this.getElementValue('s3Region'),
			s3Key: this.getElementValue('s3Key'),
			s3Secret: this.getElementValue('s3Secret')
		};

		// Remove empty values
		Object.keys(data).forEach(key => {
			if (data[key] === '' || data[key] === null || data[key] === undefined) {
				delete data[key];
			}
		});

		return data;
	}

	showLoading(title, message) {
		document.getElementById('loading-title').textContent = title;
		document.getElementById('loading-message').textContent = message;
		document.getElementById('loading').style.display = 'flex';
		document.getElementById('results').style.display = 'none';
	}

	hideLoading() {
		document.getElementById('loading').style.display = 'none';
	}
	
	showError(errorMessage) {
		// Hide loading and show error in results section
		this.hideLoading();
		
		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = 'Export Failed';
		resultsData.innerHTML = `
			<div class="error-message">
				<div class="error-icon">❌</div>
				<div class="error-content">
					<h4>Export Error</h4>
					<p>${errorMessage}</p>
					<details style="margin-top: 10px;">
						<summary>What can I try?</summary>
						<ul style="margin-left: 20px; margin-top: 10px;">
							<li>Check your credentials and project settings</li>
							<li>Verify your date range and filters</li>
							<li>Try reducing the export limit or date range</li>
							<li>Check the console for more detailed error information</li>
						</ul>
					</details>
				</div>
			</div>
		`;
		
		resultsSection.style.display = 'block';
		resultsSection.scrollIntoView({ behavior: 'smooth' });
	}

	showResults(result, isDryRun) {
		this.hideLoading();

		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = isDryRun ? 'Export Test Complete!' : 'Export Complete!';
		
		// Show download info for file exports
		let resultHtml = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
		if (result.files && result.files.length > 0 && !isDryRun) {
			resultHtml = `
				<div class="export-files-info">
					<h4>📁 Exported Files:</h4>
					<ul>
						${result.files.map(file => `
							<li>
								<strong>${file.name}</strong> (${this.formatFileSize(file.size)})
								${result.downloadUrl ? '<span class="download-status">📥 Downloaded</span>' : ''}
							</li>
						`).join('')}
					</ul>
					${result.downloadUrl ? '<p class="download-note">💡 Files have been automatically downloaded to your Downloads folder.</p>' : ''}
				</div>
				<details style="margin-top: 20px;">
					<summary>📊 Full Export Results</summary>
					<pre>${JSON.stringify(result, null, 2)}</pre>
				</details>
			`;
		}
		
		resultsData.innerHTML = resultHtml;
		resultsSection.style.display = 'block';

		// Scroll to results
		resultsSection.scrollIntoView({ behavior: 'smooth' });
	}
	
	formatFileSize(bytes) {
		if (bytes === 0) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	}
	
	downloadExportFile(downloadUrl, files) {
		try {
			// Create a hidden link and trigger download
			const link = document.createElement('a');
			link.href = downloadUrl;
			link.style.display = 'none';
			
			// Set download attribute with filename if we have file info
			if (files && files.length === 1) {
				link.download = files[0].name;
			}
			
			document.body.appendChild(link);
			link.click();
			document.body.removeChild(link);
			
			console.log('Export file download initiated:', downloadUrl);
			
			// Show success message
			this.showDownloadSuccess(files);
			
		} catch (error) {
			console.error('Failed to download export file:', error);
			this.showDownloadError(downloadUrl);
		}
	}
	
	showDownloadSuccess(files) {
		// Create a temporary success message
		const successMsg = document.createElement('div');
		successMsg.className = 'download-success-toast';
		successMsg.innerHTML = `
			<div class="toast-content">
				<span class="toast-icon">✅</span>
				<span class="toast-message">Export file downloaded successfully!</span>
			</div>
		`;
		successMsg.style.cssText = `
			position: fixed;
			top: 20px;
			right: 20px;
			background: #4CAF50;
			color: white;
			padding: 12px 20px;
			border-radius: 8px;
			box-shadow: 0 4px 12px rgba(0,0,0,0.2);
			z-index: 10000;
			opacity: 0;
			transition: opacity 0.3s ease;
		`;
		
		document.body.appendChild(successMsg);
		
		// Fade in
		setTimeout(() => {
			successMsg.style.opacity = '1';
		}, 100);
		
		// Remove after 5 seconds
		setTimeout(() => {
			successMsg.style.opacity = '0';
			setTimeout(() => {
				if (successMsg.parentNode) {
					document.body.removeChild(successMsg);
				}
			}, 300);
		}, 5000);
	}
	
	showDownloadError(downloadUrl) {
		// Show error message with manual download link
		const errorMsg = document.createElement('div');
		errorMsg.className = 'download-error-toast';
		errorMsg.innerHTML = `
			<div class="toast-content">
				<span class="toast-icon">⚠️</span>
				<span class="toast-message">
					Automatic download failed. 
					<a href="${downloadUrl}" target="_blank" style="color: #fff; text-decoration: underline;">Click here to download manually</a>
				</span>
			</div>
		`;
		errorMsg.style.cssText = `
			position: fixed;
			top: 20px;
			right: 20px;
			background: #f44336;
			color: white;
			padding: 12px 20px;
			border-radius: 8px;
			box-shadow: 0 4px 12px rgba(0,0,0,0.2);
			z-index: 10000;
			opacity: 0;
			transition: opacity 0.3s ease;
		`;
		
		document.body.appendChild(errorMsg);
		
		// Fade in
		setTimeout(() => {
			errorMsg.style.opacity = '1';
		}, 100);
		
		// Remove after 10 seconds (longer for error messages)
		setTimeout(() => {
			errorMsg.style.opacity = '0';
			setTimeout(() => {
				if (errorMsg.parentNode) {
					document.body.removeChild(errorMsg);
				}
			}, 300);
		}, 10000);
	}
}

// Utility functions for collapsible sections
function toggleSection(sectionId) {
	const section = document.getElementById(sectionId);
	const header = section.previousElementSibling;
	const icon = header.querySelector('.toggle-icon');

	if (section.style.display === 'none') {
		section.style.display = 'block';
		icon.style.transform = 'rotate(180deg)';
		header.setAttribute('aria-expanded', 'true');
	} else {
		section.style.display = 'none';
		icon.style.transform = 'rotate(0deg)';
		header.setAttribute('aria-expanded', 'false');
	}
}

function toggleAllSections() {
	const sections = document.querySelectorAll('.collapsible-content');
	const toggleBtn = document.getElementById('toggle-all-btn');
	const toggleText = document.getElementById('toggle-all-text');
	const btnIcon = toggleBtn.querySelector('.btn-icon');

	// Check if any sections are currently expanded
	const anyExpanded = Array.from(sections).some(section => section.style.display === 'block');

	sections.forEach(section => {
		const header = section.previousElementSibling;
		const icon = header.querySelector('.toggle-icon');

		if (anyExpanded) {
			// Collapse all
			section.style.display = 'none';
			icon.style.transform = 'rotate(0deg)';
			header.setAttribute('aria-expanded', 'false');
		} else {
			// Expand all
			section.style.display = 'block';
			icon.style.transform = 'rotate(180deg)';
			header.setAttribute('aria-expanded', 'true');
		}
	});

	// Update button text and icon
	if (anyExpanded) {
		toggleText.textContent = 'Expand All';
		btnIcon.textContent = '📂';
	} else {
		toggleText.textContent = 'Collapse All';
		btnIcon.textContent = '📁';
	}
}

// Initialize the application
const app = new MixpanelExportUI();
window.app = app;