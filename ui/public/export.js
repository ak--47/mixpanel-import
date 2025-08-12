// @ts-nocheck
/* eslint-env browser */

// Mixpanel Export UI Application (L.T.E Tool)
class MixpanelExportUI {
	constructor() {
		this.initializeLTECycling();
		this.setupEventListeners();
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

	initializeLTECycling() {
		// L.T.E word bank - export and data processing terms
		const lteCombos = [
			'Load Transform Export',
			'Launch Transform Execute',
			'Locate Transform Extract', 
			'Library Transform Export',
			'Leverage Transform Export',
			'Logic Transform Execute',
			'Layer Transform Export',
			'Load Transfer Export',
			'Lift Transform Export',
			'Link Transform Export',
			'List Transform Export',
			'Loop Transform Export',
			'Load Transpose Export',
			'Launch Transfer Export',
			'Locate Transfer Export',
			'Library Transfer Export',
			'Leverage Transfer Export',
			'Logic Transfer Export',
			'Layer Transfer Export',
			'Lift Transfer Export'
		];

		let currentIndex = 0;
		const descriptionElement = document.getElementById('lte-description');
		
		if (!descriptionElement) return;

		// Function to cycle descriptions
		const cycleDescription = () => {
			// Add fading class
			descriptionElement.classList.add('fading');
			
			// After fade out, change text and fade back in
			setTimeout(() => {
				currentIndex = (currentIndex + 1) % lteCombos.length;
				descriptionElement.textContent = lteCombos[currentIndex];
				descriptionElement.classList.remove('fading');
			}, 250); // Half of the transition time
		};

		// Start cycling every 10 seconds
		setInterval(cycleDescription, 10000);
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

		// Define authentication requirements based on export type
		switch (recordType) {
			case 'export':
			case 'profile-export':
			case 'profile-delete':
				// API secret OR service user/pass and project_id required, dataGroupId optional
				credentialsDescription.textContent = 'Exports require either API secret or service account credentials with project ID. Data Group ID is optional.';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('dataGroupId-group').style.display = 'block';
				break;

			case 'scd':
			case 'annotations':
			case 'get-annotations':
			case 'delete-annotations':
				// Service user/pass and project_id required
				credentialsDescription.textContent = 'This operation requires service account credentials and project ID.';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				// Force service auth
				document.querySelector('input[name="authMethod"][value="service"]').checked = true;
				break;

			case 'export-import-events':
			case 'export-import-profiles':
				// API secret OR service user/pass and project_id required, secondToken optional
				credentialsDescription.textContent = 'Export-import operations require either API secret or service account credentials. Second token, group key, and data group ID are optional.';
				document.getElementById('project-group').style.display = 'block';
				document.getElementById('auth-toggle').style.display = 'block';
				document.getElementById('service-auth').style.display = 'block';
				document.getElementById('secondToken-group').style.display = 'block';
				if (recordType === 'export-import-profiles') {
					document.getElementById('groupKey-group').style.display = 'block';
					document.getElementById('dataGroupId-group').style.display = 'block';
				}
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
				// API secret OR service user/pass and project_id required
				const exportProject = document.getElementById('project').value;
				if (!exportProject) {
					return { isValid: false, message: 'Project ID is required for exports.' };
				}

				const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
				if (authMethod === 'service') {
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

			default:
				return { isValid: false, message: 'Please select a valid export type.' };
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
				command += ` --token ${tokenElement.value}`;
			}

			// Auth credentials
			const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value;
			if (authMethod === 'service') {
				const acct = this.getElementValue('acct');
				const pass = this.getElementValue('pass');
				if (acct && pass) {
					command += ` --acct ${acct} --pass ${pass}`;
				}
			} else {
				const secret = this.getElementValue('secret');
				if (secret) {
					command += ` --secret ${secret}`;
				}
			}

			// Additional options with values
			const optionsMap = {
				'groupKey': '--groupKey',
				'dataGroupId': '--dataGroupId',
				'secondToken': '--secondToken',
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

	async submitExport(isDryRun = false) {
		const recordType = document.getElementById('recordType').value;

		// Validate required fields
		const validation = this.validateRequiredFields(recordType);
		if (!validation.isValid) {
			alert(validation.message);
			return;
		}

		this.showLoading(isDryRun ? 'Testing Export...' : 'Exporting Data...', 
						isDryRun ? 'Running export test' : 'Exporting your data from Mixpanel');

		try {
			// Collect form data
			const formData = this.collectFormData();
			formData.isDryRun = isDryRun;

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

			if (result.success) {
				this.showResults(result, isDryRun);
			} else {
				throw new Error(result.error || 'Export failed');
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
			outputFilePath: this.getElementValue('outputFilePath')
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

	showResults(result, isDryRun) {
		this.hideLoading();
		
		const resultsSection = document.getElementById('results');
		const resultsTitle = document.getElementById('results-title');
		const resultsData = document.getElementById('results-data');

		resultsTitle.textContent = isDryRun ? 'Export Test Complete!' : 'Export Complete!';
		resultsData.innerHTML = `<pre>${JSON.stringify(result, null, 2)}</pre>`;
		resultsSection.style.display = 'block';

		// Scroll to results
		resultsSection.scrollIntoView({ behavior: 'smooth' });
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