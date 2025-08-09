// Mixpanel Import UI Application
class MixpanelImportUI {
    constructor() {
        this.files = [];
        this.editor = null;
        this.initializeUI();
        this.setupEventListeners();
        this.initializeMonacoEditor();
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
                init: function() {
                    this.on('addedfile', (file) => {
                        app.addFile(file);
                    });
                }
            });
        } else {
            console.warn('Dropzone not loaded, falling back to basic file input');
            this.setupBasicFileInput();
        }
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
                <button type="button" class="file-remove" onclick="app.removeFile(${index})">✕</button>
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

        // Preview button
        const previewBtn = document.getElementById('preview-btn');
        previewBtn.addEventListener('click', () => {
            this.previewTransform();
        });

        // Clear transform button
        const clearBtn = document.getElementById('clear-transform');
        clearBtn.addEventListener('click', () => {
            if (this.editor) {
                this.editor.setValue('');
            }
        });

        // Record type change - show/hide relevant fields
        const recordTypeSelect = document.getElementById('recordType');
        recordTypeSelect.addEventListener('change', this.updateFieldVisibility.bind(this));
        this.updateFieldVisibility(); // Initial call
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
                document.querySelector('input[name=\"authMethod\"][value=\"service\"]').checked = true;
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
                credentialsDescription.textContent = 'Select an import type to see required authentication settings.';
        }

        // Update auth method visibility and trigger toggle
        const authToggle = document.getElementById('auth-toggle');
        if (authToggle.style.display === 'block') {
            this.toggleAuthMethod();
        }
    }

    validateRequiredFields(recordType) {
        switch (recordType) {
            case 'event':
            case 'user':
                // Only project token is required
                const token = document.getElementById('token').value;
                if (!token) {
                    return { isValid: false, message: 'Project token is required for events and user profiles.' };
                }
                break;

            case 'group':
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

            case 'table':
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

            case 'export':
            case 'profile-export':
            case 'profile-delete':
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

            case 'scd':
            case 'annotations':
            case 'get-annotations':
            case 'delete-annotations':
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

            case 'export-import-events':
            case 'export-import-profiles':
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

            default:
                return { isValid: false, message: 'Please select a valid import type.' };
        }

        return { isValid: true };
    }

    async initializeMonacoEditor() {
        return new Promise((resolve, reject) => {
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

    createMonacoEditor() {
        this.editor = monaco.editor.create(document.getElementById('monaco-editor'), {
            value: `// Transform function - modify the data object and return it
// Available: data (current record), heavy (job utilities)

return {
    ...data,
    // Add your transformations here
    // custom_property: 'value',
    // transformed_at: new Date().toISOString()
};`,
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
        textarea.value = `// Transform function - modify the data object and return it
// Available: data (current record), heavy (job utilities)

return {
    ...data,
    // Add your transformations here
    // custom_property: 'value',
    // transformed_at: new Date().toISOString()
};`;
        
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

        // Collect credentials
        const authMethod = document.querySelector('input[name="authMethod"]:checked')?.value || 'service';
        const credentials = {};

        // Add project ID if visible
        const projectElement = document.getElementById('project');
        if (projectElement.closest('.form-group').style.display !== 'none' && projectElement.value) {
            credentials.project = projectElement.value;
        }

        // Add lookup table ID if visible
        const lookupTableIdElement = document.getElementById('lookupTableId');
        if (lookupTableIdElement.closest('.form-group').style.display !== 'none' && lookupTableIdElement.value) {
            credentials.lookupTableId = lookupTableIdElement.value;
        }

        // Add authentication based on method
        if (authMethod === 'service') {
            const acctElement = document.getElementById('acct');
            const passElement = document.getElementById('pass');
            if (acctElement.value) credentials.acct = acctElement.value;
            if (passElement.value) credentials.pass = passElement.value;
        } else {
            const secretElement = document.getElementById('secret');
            if (secretElement.value) credentials.secret = secretElement.value;
        }

        // Add optional fields if visible and have values
        const tokenElement = document.getElementById('token');
        if (tokenElement.closest('.form-group').style.display !== 'none' && tokenElement.value) {
            credentials.token = tokenElement.value;
        }

        const groupKeyElement = document.getElementById('groupKey');
        if (groupKeyElement.closest('.form-group').style.display !== 'none' && groupKeyElement.value) {
            credentials.groupKey = groupKeyElement.value;
        }

        const dataGroupIdElement = document.getElementById('dataGroupId');
        if (dataGroupIdElement.closest('.form-group').style.display !== 'none' && dataGroupIdElement.value) {
            credentials.dataGroupId = dataGroupIdElement.value;
        }

        const secondTokenElement = document.getElementById('secondToken');
        if (secondTokenElement.closest('.form-group').style.display !== 'none' && secondTokenElement.value) {
            credentials.secondToken = secondTokenElement.value;
        }

        formData.append('credentials', JSON.stringify(credentials));

        // Collect options
        const options = {
            recordType: document.getElementById('recordType').value,
            workers: parseInt(document.getElementById('workers').value) || 10,
            recordsPerBatch: parseInt(document.getElementById('recordsPerBatch').value) || 2000,
            region: document.getElementById('region').value || 'US',
            compress: document.getElementById('compress').checked,
            fixData: document.getElementById('fixData').checked,
            strict: document.getElementById('strict').checked,
            verbose: document.getElementById('verbose').checked
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

    async submitJob(isDryRun = false) {
        try {
            // Validation
            if (this.files.length === 0) {
                this.showError('Please select at least one file to import.');
                return;
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

    async previewTransform() {
        if (this.files.length === 0) {
            this.showError('Please select a file first to preview the transform.');
            return;
        }

        const transformCode = this.editor ? this.editor.getValue().trim() : '';
        if (!transformCode || transformCode.startsWith('// Transform function')) {
            this.showError('Please write a transform function to preview.');
            return;
        }

        // Run a dry run to get preview data
        await this.submitJob(true);
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

        let displayData = result.result;
        
        // For dry runs, also show preview data
        if (isDryRun && result.previewData && result.previewData.length > 0) {
            displayData = {
                ...result.result,
                preview: result.previewData.slice(0, 5) // Show first 5 transformed records
            };

            // Also show in preview section
            const previewSection = document.getElementById('preview-results');
            const previewContent = document.getElementById('preview-content');
            
            previewContent.innerHTML = `<pre>${JSON.stringify(result.previewData.slice(0, 3), null, 2)}</pre>`;
            previewSection.style.display = 'block';
        }

        resultsData.innerHTML = `<pre>${JSON.stringify(displayData, null, 2)}</pre>`;
        resultsSection.style.display = 'block';

        // Scroll to results
        resultsSection.scrollIntoView({ behavior: 'smooth' });
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
}

// Global function for collapsible sections
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
});