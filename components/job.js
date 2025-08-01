const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;
// @ts-ignore
const u = require('ak-tools');
const transforms = require('./transforms.js');
const { ampEventsToMp, ampUserToMp, ampGroupToMp } = require('../vendor/amplitude.js');
const { heapEventsToMp, heapUserToMp, heapGroupToMp, heapParseErrorHandler } = require('../vendor/heap.js');
const { gaEventsToMp, gaUserToMp, gaGroupsToMp } = require('../vendor/ga4.js');
const { mParticleEventsToMixpanel, mParticleUserToMixpanel, mParticleGroupToMixpanel } = require('../vendor/mparticle.js');
const { postHogEventsToMp, postHogPersonToMpProfile } = require('../vendor/posthog.js');
const { buildMapFromPath } = require('./parsers.js');


/** @typedef {import('../index.js').Creds} Creds */
/** @typedef {import('../index.js').Options} Options */

/**
 * a singleton to hold state about the imported data
 * @example
 * const importJob = new Job(creds, opts)
 * @class 
 * @param {Creds} creds - mixpanel project credentials
 * @param {Options} opts - options for import
 * @method summary summarize state of import
*/
class Job {
	/**
	 * @param  {Creds} creds
	 * @param  {Options} [opts]
	 */
	constructor(creds, opts = {}) {
		// ? credentials
		if (!creds) throw new Error('no credentials provided!');
		this.acct = creds.acct || ``; //service acct username
		this.pass = creds.pass || ``; //service acct secret
		this.project = creds.project || ``; //project id
		this.workspace = creds.workspace || ``; //workspace id
		this.org = creds.org || ``; //org id
		this.secret = creds.secret || ``; //api secret (deprecated auth)
		this.bearer = creds.bearer || ``;
		this.token = creds.token || ``; //project token 
		this.secondToken = creds.secondToken || ``; //second token for export / import
		this.lookupTableId = creds.lookupTableId || ``; //lookup table id
		this.groupKey = creds.groupKey || opts.groupKey || ``; //group key id
		this.auth = this.resolveProjInfo();
		this.startTime = new Date().toISOString();
		this.endTime = null;
		this.hashTable = new Set(); //used if de-dupe is on
		this.memorySamples = []; //used to calculate memory usage
		this.wasStream = null; //was the data loaded into memory or streamed?
		this.dryRunResults = []; //results of dry run	
		this.badRecords = {};
		this.insertIdTuple = opts.insertIdTuple || []; //tuple of keys for insert_id	
		this.scdLabel = opts.scdLabel || ''; //scd label
		this.scdKey = opts.scdKey || ''; //scd key
		this.scdType = opts.scdType || 'string'; //scd type
		this.scdId = opts.scdId || ''; //scd id
		this.scdPropId = opts.scdPropId || ''; //scd prop id
		this.transport = opts.transport || 'got'; // transport mechanism to use for sending data (default: got)

		this.dimensionMaps = opts.dimensionMaps || []; //dimension map for scd
		this.heavyObjects = {}; //used to store heavy objects
		this.insertHeavyObjects = async function (arrayOfKeysAndFilesPaths = this.dimensionMaps) {
			for (const keyFilePath of arrayOfKeysAndFilesPaths) {
				const { filePath, keyOne, keyTwo, label = u.makeName(3, '-') } = keyFilePath;
				const result = await buildMapFromPath(filePath, keyOne, keyTwo);
				this.heavyObjects[label] = result;
			}
		};

		// ? export stuff
		if (opts.limit) {
			this.limit = opts.limit;
		}
		if (opts.whereClause) {
			this.whereClause = opts.whereClause;
		}

		//? dates
		if (opts.start) {
			this.start = dayjs(opts.start).format(dateFormat);
		}
		else {
			this.start = dayjs().subtract(30, 'd').format(dateFormat);

		}
		if (opts.end) {
			this.end = dayjs(opts.end).format(dateFormat);
		}

		else {
			this.end = dayjs().format(dateFormat);
		}

		if (opts.cohortId) {
			try {
				if (typeof opts.cohortId === 'string') this.cohort_id = parseInt(opts.cohortId);
				else this.cohortId = opts.cohortId;
			}
			catch (e) {
				console.error('cohort_id must be an integer');
				throw e;
			}
		}

		if (opts.dataGroupId) {
			// this is a hack to PREVENT negative dataGroupId from scd endpoint... which is weird
			if (opts.recordType === "scd") {
				if (opts.dataGroupId?.startsWith('-')) this.dataGroupId = opts.dataGroupId.split("-")[1];
				else this.dataGroupId = opts.dataGroupId;
			}
			else {
				this.dataGroupId = opts.dataGroupId;
			}

		}

		// ? string options
		this.recordType = opts.recordType || `event`; // event, user, group or table		
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.region = opts.region || `US`; // US or EU or IN
		this.secondRegion = opts.secondRegion || ''; // US or EU or IN; used for exports => import
		this.vendor = opts.vendor || ''; // heap or amplitude

		// ? number options
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)		
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 9 * 1024 * 1024; // max bytes in each req ... api max: 10485760
		this.maxRetries = opts.maxRetries || 10; // number of times to retry a batch
		this.timeOffset = opts.timeOffset || 0; // utc hours offset
		this.compressionLevel = opts.compressionLevel || 6; // gzip compression level
		this.workers = opts.workers || 10; // number of workers to use
		this.highWater = (this.workers * this.recordsPerBatch) || 2000;
		this.epochStart = opts.epochStart || 0; // start date for epoch
		this.epochEnd = opts.epochEnd || 9991427224; // end date for epoch; i will die many years before this is a problem

		if (opts.concurrency) this.workers = opts.concurrency; // alias for workers

		// ? don't allow batches bigger than API limits
		if (this.recordType === 'event' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.recordType === 'user' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.recordType === 'group' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;

		// ? boolean options
		this.compress = u.isNil(opts.compress) ? true : opts.compress; //gzip data (events only)
		this.strict = u.isNil(opts.strict) ? true : opts.strict; // use strict mode?
		this.logs = u.isNil(opts.logs) ? false : opts.logs; //create log file
		this.where = u.isNil(opts.logs) ? '' : opts.where; // where to put logs
		this.verbose = u.isNil(opts.verbose) ? false : opts.verbose;  // print to stdout?
		this.showProgress = u.isNil(opts.showProgress) ? false : opts.showProgress; // show progress bar
		this.fixData = u.isNil(opts.fixData) ? false : opts.fixData; //apply transforms on the data
		this.fixTime = u.isNil(opts.fixTime) ? false : opts.fixTime; //fix time to utc
		this.fixJson = u.isNil(opts.fixJson) ? false : opts.fixJson; //fix json
		this.removeNulls = u.isNil(opts.removeNulls) ? false : opts.removeNulls; //remove null fields
		this.flattenData = u.isNil(opts.flattenData) ? false : opts.flattenData; //flatten nested properties
		this.abridged = u.isNil(opts.abridged) ? false : opts.abridged; //don't include success responses
		this.forceStream = u.isNil(opts.forceStream) ? true : opts.forceStream; //don't ever buffer files into memory
		this.dedupe = u.isNil(opts.dedupe) ? false : opts.dedupe; //remove duplicate records
		this.createProfiles = u.isNil(opts.createProfiles) ? false : opts.createProfiles; //remove duplicate records
		this.dryRun = u.isNil(opts.dryRun) ? false : opts.dryRun; //don't actually send data
		this.http2 = u.isNil(opts.http2) ? false : opts.http2; //use http2
		this.addToken = u.isNil(opts.addToken) ? false : opts.addToken; //add token to each record
		this.shouldWhiteBlackList = false;
		this.shouldEpochFilter = false;
		this.shouldAddTags = false;
		this.shouldApplyAliases = false;
		this.shouldCreateInsertId = false;
		this.writeToFile = u.isNil(opts.writeToFile) ? false : opts.writeToFile; //write to file instead of sending
		this.outputFilePath = opts.outputFilePath || './mixpanel-transform.json'; //where to write the file
		this.skipWriteToDisk = u.isNil(opts.skipWriteToDisk) ? false : opts.skipWriteToDisk; //don't write to disk
		this.keepBadRecords = u.isNil(opts.keepBadRecords) ? true : opts.keepBadRecords; //keep bad records

		// ? tagging options
		this.tags = parse(opts.tags) || {}; //tags for the import		
		this.aliases = parse(opts.aliases) || {}; //aliases for the import
		this.vendorOpts = parse(opts.vendorOpts) || {}; //options for vendor transforms

		// ? whitelist/blacklist options
		this.eventWhitelist = parse(opts.eventWhitelist) || [];
		this.eventBlacklist = parse(opts.eventBlacklist) || [];
		this.propKeyWhitelist = parse(opts.propKeyWhitelist) || [];
		this.propKeyBlacklist = parse(opts.propKeyBlacklist) || [];
		this.propValWhitelist = parse(opts.propValWhitelist) || [];
		this.propValBlacklist = parse(opts.propValBlacklist) || [];
		this.comboWhiteList = parse(opts.comboWhiteList) || {};
		this.comboBlackList = parse(opts.comboBlackList) || {};
		this.scrubProps = parse(opts.scrubProps) || [];

		// @ts-ignore backwards compatibility
		if (opts?.scrubProperties) this.scrubProps = parse(opts.scrubProperties) || [];

		// ? transform options
		this.transformFunc = opts.transformFunc || null;
		this.vendorTransform = null;
		this.ezTransform = noop;
		this.nullRemover = noop;
		this.UTCoffset = noop;
		this.addTags = noop;
		this.applyAliases = noop;
		this.deduper = noop;
		this.whiteAndBlackLister = noop;

		this.epochFilter = noop;
		this.flattener = noop;
		this.insertIdAdder = noop;
		this.jsonFixer = noop;
		this.propertyScrubber = noop;
		this.parseErrorHandler = opts.parseErrorHandler || returnEmpty(this);
		this.tokenAdder = noop;
		this.scdTransform = noop;
		this.timeTransform = noop;

		// ? transform conditions
		if (this.fixData || this.recordType?.includes('export-import'))this.ezTransform = transforms.ezTransforms(this);
		if (this.fixJson) this.jsonFixer = transforms.fixJson();
		if (this.removeNulls) this.nullRemover = transforms.removeNulls();
		if (this.timeOffset) this.UTCoffset = transforms.UTCoffset(this.timeOffset);
		if (this.dedupe) this.deduper = transforms.dedupeRecords(this);
		if (this.flattenData) this.flattener = transforms.flattenProperties(".");
		if (this.addToken) this.tokenAdder = transforms.addToken(this);
		if (this.recordType === 'scd') this.scdTransform = transforms.scdTransform(this);
		if (this.recordType === 'event' && this.fixTime) this.timeTransform = transforms.fixTime(this);

		if (this.insertIdTuple.length > 0 && this.recordType === 'event') {
			this.shouldCreateInsertId = true;
			this.insertIdAdder = transforms.addInsert(this.insertIdTuple);
		}
		if (Object.keys(this.tags).length > 0) {
			this.shouldAddTags = true;
			this.addTags = transforms.addTags(this);
		}
		if (Object.keys(this.aliases).length > 0) {
			this.shouldApplyAliases = true;
			this.applyAliases = transforms.applyAliases(this);
		}
		const whiteOrBlacklist = {
			eventWhitelist: this.eventWhitelist,
			eventBlacklist: this.eventBlacklist,
			propKeyWhitelist: this.propKeyWhitelist,
			propKeyBlacklist: this.propKeyBlacklist,
			propValWhitelist: this.propValWhitelist,
			propValBlacklist: this.propValBlacklist,
			comboWhiteList: this.comboWhiteList,
			comboBlackList: this.comboBlackList
		};
		if (Object.values(whiteOrBlacklist).some(array => array.length >= 1)) {
			this.whiteAndBlackLister = transforms.whiteAndBlackLister(this, whiteOrBlacklist);
			this.shouldWhiteBlackList = true;
		}
		if (Object.keys(this.comboWhiteList).length > 0 || Object.keys(this.comboBlackList).length > 0) {
			this.whiteAndBlackLister = transforms.whiteAndBlackLister(this, whiteOrBlacklist);
			this.shouldWhiteBlackList = true;
		}

		if (opts.epochStart || opts.epochEnd) {
			this.shouldEpochFilter = true;
			this.epochFilter = transforms.epochFilter(this);
		}

		if (this.scrubProps.length > 0) {
			this.propertyScrubber = transforms.scrubProperties(this.scrubProps);
		}

		this.vendor = opts.vendor || '';


		// ? counters
		this.recordsProcessed = 0;
		this.success = 0;
		this.failed = 0;
		this.retries = 0;
		this.batches = 0;
		this.requests = 0;
		this.empty = 0;
		this.rateLimited = 0;
		this.serverErrors = 0;
		this.clientErrors = 0;
		this.bytesProcessed = 0;
		this.outOfBounds = 0;
		this.duplicates = 0;
		this.whiteListSkipped = 0;
		this.blackListSkipped = 0;
		this.batchLengths = [];
		this.lastBatchLength = 0;
		this.unparsable = 0;
		this.timer = u.time('etl');
		
		// Memory management for large jobs
		this.maxBatchLengths = 1000; // Limit batch length tracking
		this.maxMemorySamples = 100; // Limit memory samples
		this.maxBadRecordsPerMessage = 100; // Limit bad records per error message
		this.maxBadRecordMessages = 50; // Limit number of distinct error messages

		// ? requests
		/** @type {'POST' | 'GET' | 'PUT' | 'PATCH'} */
		this.reqMethod = "POST";
		this.contentType = "application/json";
		this.encoding = "";
		this.responses = [];
		this.errors = [];

		// if we're in abridged mode errors is a hash
		if (this.abridged) this.errors = {};

		// SCD cannot be strict mode -_-
		if (this.recordType === "scd") this.strict = false;


		// ? allow plurals
		// @ts-ignore
		if (this.recordType === 'events') this.recordType === 'event';
		// @ts-ignore
		if (this.recordType === 'users') this.recordType === 'user';
		// @ts-ignore
		if (this.recordType === 'groups') this.recordType === 'group';
		// @ts-ignore
		if (this.recordType === 'tables') this.recordType === 'table';

		// ? headers for lookup tables
		if (this.recordType === "table") {
			this.reqMethod = 'PUT';
			this.contentType = 'text/csv';
			this.fixData = false;
		}

		// ? headers for exports
		if (this.recordType === "export") {
			this.reqMethod = 'GET';
		}
		this.file = "";
		this.folder = "";


	}

	// ? props
	version = this.getVersion();
	lineByLineFileExt = ['.txt', '.jsonl', '.ndjson'];
	objectModeFileExt = ['.json'];
	tableFileExt = ['.csv', '.tsv'];
	otherFormats = ['.parquet'];
	supportedFileExt = [...this.lineByLineFileExt, ...this.objectModeFileExt, ...this.tableFileExt, ...this.otherFormats];
	endpoints = {
		us: {
			event: `https://api.mixpanel.com/import`,
			scd: `https://api.mixpanel.com/import`,
			user: `https://api.mixpanel.com/engage`,
			group: `https://api.mixpanel.com/groups`,
			table: `https://api.mixpanel.com/lookup-tables/`,
			export: `https://data.mixpanel.com/api/2.0/export`,
			"profile-export": `https://mixpanel.com/api/2.0/engage`,
			"export-import-events": `https://data.mixpanel.com/api/2.0/export`,
			"export-import-profiles": `https://mixpanel.com/api/2.0/engage`
		},
		eu: {
			event: `https://api-eu.mixpanel.com/import`,
			scd: `https://api-eu.mixpanel.com/import`,
			user: `https://api-eu.mixpanel.com/engage`,
			group: `https://api-eu.mixpanel.com/groups`,
			table: `https://api-eu.mixpanel.com/lookup-tables/`,
			export: `https://data-eu.mixpanel.com/api/2.0/export`,
			"profile-export": `https://eu.mixpanel.com/api/2.0/engage`,
			"export-import-events": `https://data-eu.mixpanel.com/api/2.0/export`,
			"export-import-profiles": `https://eu.mixpanel.com/api/2.0/engage`
		},
		in: {
			event: `https://api-in.mixpanel.com/import`,
			scd: `https://api-eu.mixpanel.com/import`,
			user: `https://api-in.mixpanel.com/engage`,
			group: `https://api-in.mixpanel.com/groups`,
			table: `https://api-in.mixpanel.com/lookup-tables/`,
			export: `https://data-in.mixpanel.com/api/2.0/export`,
			"profile-export": `https://in.mixpanel.com/api/2.0/engage`,
			"export-import-events": `https://data-in.mixpanel.com/api/2.0/export`,
			"export-import-profiles": `https://in.mixpanel.com/api/2.0/engage`
		}

	};

	// ? get/set	
	get type() {
		return this.recordType;
	}
	get url() {
		let url = this.endpoints[this.region.toLowerCase()][this.recordType.toLowerCase()];
		if (this.recordType === "table") url += this.lookupTableId;
		return url;
	}
	get opts() {
		const { recordType, compress, streamSize, workers, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc } = this;
		return { recordType, compress, streamSize, workers, region, recordsPerBatch, bytesPerBatch, strict, logs, fixData, streamFormat, transformFunc };
	}
	get creds() {
		const { acct, pass, project, secret, token, lookupTableId, groupKey, auth, bearer, workspace } = this;
		return { acct, pass, project, secret, token, lookupTableId, groupKey, auth, bearer, workspace };
	}
	// @ts-ignore
	set batchSize(chunkSize) {
		this.recordsPerBatch = chunkSize;
	}
	// @ts-ignore
	set transform(fn) {
		this.transformFunc = fn;
	}

	// ? methods

	async init() {
		// await job.insertHeavyObjects(job.dimensionMaps)
		await this.insertHeavyObjects();

		//setup the vendor transforms
		if (this.vendor) {
			let vendorTransformFunc = noop;
			const chosenVendor = this.vendor.toLowerCase();
			const recordType = this.recordType?.toLowerCase();
			switch (chosenVendor) {
				case 'amplitude':
					switch (recordType) {
						case 'event':
							vendorTransformFunc = ampEventsToMp(this.vendorOpts);
							break;
						case 'user':
							//ALWAYS dedupe user profiles for amplitude
							this.dedupe = true;
							this.deduper = transforms.dedupeRecords(this);
							vendorTransformFunc = ampUserToMp(this.vendorOpts);
							break;
						case 'group':
							vendorTransformFunc = ampGroupToMp(this.vendorOpts);
							break;
						default:
							vendorTransformFunc = ampEventsToMp(this.vendorOpts);
							break;
					}
					break;

				case 'heap':
					this.parseErrorHandler = heapParseErrorHandler;
					switch (recordType) {
						case 'event':
							vendorTransformFunc = heapEventsToMp(this.vendorOpts);
							break;
						case 'user':
							vendorTransformFunc = heapUserToMp(this.vendorOpts);
							break;
						case 'group':
							vendorTransformFunc = heapGroupToMp(this.vendorOpts);
							break;
						default:
							vendorTransformFunc = heapEventsToMp(this.vendorOpts);
							break;
					}
					break;

				case 'ga4':
					switch (recordType) {
						case 'event':
							vendorTransformFunc = gaEventsToMp(this.vendorOpts);
							break;
						case 'user':
							//ALWAYS dedupe user profiles for ga4
							this.dedupe = true;
							this.deduper = transforms.dedupeRecords(this);
							vendorTransformFunc = gaUserToMp(this.vendorOpts);
							break;
						case 'group':
							vendorTransformFunc = gaGroupsToMp(this.vendorOpts);
							break;
						default:
							vendorTransformFunc = gaEventsToMp(this.vendorOpts);
							break;
					}
					break;
				case 'mparticle':
					switch (recordType) {
						case 'event':
							vendorTransformFunc = mParticleEventsToMixpanel(this.vendorOpts);
							break;
						case 'user':
							this.dedupe = true;
							this.deduper = transforms.dedupeRecords(this);
							vendorTransformFunc = mParticleUserToMixpanel(this.vendorOpts);
							break;
						case 'group':
							vendorTransformFunc = mParticleGroupToMixpanel(this.vendorOpts);
							break;
						default:
							vendorTransformFunc = mParticleEventsToMixpanel(this.vendorOpts);
							break;
					}
					break;
				case 'posthog':
					switch (recordType) {
						case 'event':
							vendorTransformFunc = postHogEventsToMp(this.vendorOpts, this.heavyObjects);
							break;
						case 'user':
							this.dedupe = true;
							this.deduper = transforms.dedupeRecords(this);
							vendorTransformFunc = postHogPersonToMpProfile(this.vendorOpts);
							break;
						case 'group':
							throw new Error('posthog does not support groups');
						default:
							vendorTransformFunc = postHogEventsToMp(this.vendorOpts, this.heavyObjects);
							break;
					}
					break;
				default:
					vendorTransformFunc = noop;
					break;
			}
			this.vendorTransform = vendorTransformFunc;

		}
	}

	report() {
		return Object.assign({}, this);
	}
	store(response, success = true) {
		const isVerbose = !this.abridged;
		if (isVerbose) {
			if (success) this.responses.push(response);
			if (!success) {
				if (!this.abridged) {
					this.errors.push(response);

				}

			}
		}

		if (!isVerbose) {
			// summarize the error + count			
			if (!success && response?.failed_records) {
				if (Array.isArray(response.failed_records)) {
					response.failed_records.forEach(failure => {
						const { message = "unknown error" } = failure;
						if (!this.errors[message]) this.errors[message] = 1;
						this.errors[message]++;
					});

				}
			}

		}

		return;



	}

	getVersion() {
		const { version } = require('../package.json');
		if (version) return version;
		if (process.env.npm_package_version) return process.env.npm_package_version;
		return 'unknown';
	}
	resolveProjInfo() {
		//preferred method: service acct
		if (this.acct && this.pass && this.project) {
			return `Basic ${Buffer.from(this.acct + ':' + this.pass, 'binary').toString('base64')}`;
		}

		//fallback method: secret auth
		else if (this.secret) {
			return `Basic ${Buffer.from(this.secret + ':', 'binary').toString('base64')}`;
		}

		else if (this.token) {
			return `Basic ${Buffer.from(this.token + ':', 'binary').toString('base64')}`;
		}

		else if (this.bearer) {
			return `Bearer ${this.bearer}`;
		}

		else if (this.type === 'user' || this.type === 'group') {
			return ``;
		}

		else {
			console.error('no secret or service account + project provided!', { config: this.report() });
			throw new Error('no secret or service account provided!');
			// process.exit(0);
		}

	}

	// Capture a memory sample with bounded collection
	memSamp() {
		const memoryUsage = process.memoryUsage();
		
		// Implement circular buffer to prevent unbounded growth
		if (this.memorySamples.length >= this.maxMemorySamples) {
			this.memorySamples.shift(); // Remove oldest sample
		}
		this.memorySamples.push(memoryUsage);
		return memoryUsage;
	}
	
	// Add bounded batch length tracking
	addBatchLength(length) {
		// Implement circular buffer to prevent unbounded growth
		if (this.batchLengths.length >= this.maxBatchLengths) {
			this.batchLengths.shift(); // Remove oldest batch length
		}
		this.batchLengths.push(length);
		this.lastBatchLength = length;
	}

	// Add bad record with memory bounds
	addBadRecord(message, record) {
		if (!this.keepBadRecords) return; // Skip if disabled
		
		// Limit number of distinct error messages
		const messageKeys = Object.keys(this.badRecords);
		if (!this.badRecords[message] && messageKeys.length >= this.maxBadRecordMessages) {
			// Remove oldest error message if at limit
			const oldestMessage = messageKeys[0];
			delete this.badRecords[oldestMessage];
		}
		
		// Initialize array for new message
		if (!this.badRecords[message]) {
			this.badRecords[message] = [];
		}
		
		// Limit records per message
		if (this.badRecords[message].length >= this.maxBadRecordsPerMessage) {
			this.badRecords[message].shift(); // Remove oldest record
		}
		
		this.badRecords[message].push(record);
	}

	// Compute the average of the collected memorySamples
	memAvg() {
		if (this.memorySamples.length === 0) {
			return process.memoryUsage();

		}

		const sum = this.memorySamples.reduce((acc, curr) => {
			acc.rss += curr.rss;
			acc.heapTotal += curr.heapTotal;
			acc.heapUsed += curr.heapUsed;
			acc.external += curr.external;
			acc.arrayBuffers += curr.arrayBuffers;
			return acc;
		}, {
			rss: 0,
			heapTotal: 0,
			heapUsed: 0,
			external: 0,
			arrayBuffers: 0
		});

		// Calculate the average for each metric
		const averageMemoryUsage = {
			rss: sum.rss / this.memorySamples.length,
			heapTotal: sum.heapTotal / this.memorySamples.length,
			heapUsed: sum.heapUsed / this.memorySamples.length,
			external: sum.external / this.memorySamples.length,
			arrayBuffers: sum.arrayBuffers / this.memorySamples.length
		};

		return averageMemoryUsage;
	}

	// human readable memory usage
	memPretty() {
		const memoryUsage = this.memAvg();
		if (!memoryUsage) {
			return {};
		}
		return u.objMap(memoryUsage, (v) => u.bytesHuman(v));
	}

	// Clear the samples
	memRest() {
		this.samples = [];
	}
	getEps() {
		const duration = (Date.now() - dayjs(this.startTime).valueOf()) / 1000;
		const eps = this.recordsProcessed / duration;
		return eps.toFixed(2);
	}
	/**
	 * summary of the results of an import
	 * @param {boolean} includeResponses - should `errors` and `responses` be included in summary
	 * @returns {import('../index.js').ImportResults} `{success, failed, total, requests, duration}`
	 */
	summary(includeResponses = true) {
		this.timer.stop(false)
		const { delta, human } = this.timer.report(false);
		const memoryHuman = this.memPretty();
		const memory = this.memAvg();
		/** @type {import('../index.js').ImportResults} */
		const summary = {
			recordType: this.recordType,

			total: this.recordsProcessed || 0,
			success: this.success || 0,
			failed: this.failed || 0,
			empty: this.empty || 0,
			outOfBounds: this.outOfBounds || 0,
			duplicates: this.duplicates || 0,
			whiteListSkipped: this.whiteListSkipped || 0,
			blackListSkipped: this.blackListSkipped || 0,
			unparsable: this.unparsable || 0,

			startTime: this.startTime,
			endTime: new Date().toISOString(),
			duration: delta || 0,
			durationHuman: human,
			bytes: this.bytesProcessed,
			bytesHuman: u.bytesHuman(this.bytesProcessed),

			requests: this.requests,
			batches: this.batches,
			retries: this.retries,
			rateLimit: this.rateLimited,
			serverErrors: this.serverErrors,
			clientErrors: this.clientErrors,

			version: this.version,
			workers: this.workers,
			memory,
			memoryHuman,
			wasStream: this.wasStream,

			avgBatchLength: u.avg(...this.batchLengths),
			eps: 0,
			rps: 0,
			mbps: 0,
			percentQuota: 0,
			errors: [],
			responses: [],
			transport: this.transport,
			// @ts-ignore
			badRecords: this.badRecords,
			dryRun: this.dryRunResults,
			vendor: this.vendor || "",
			vendorOpts: this.vendorOpts
		};

		// stats
		if (summary.total && summary.duration && summary.requests && summary.bytes) {
			summary.eps = Math.floor(summary.total / summary.duration * 1000);
			summary.rps = u.round(summary.requests / summary.duration * 1000, 3);
			summary.mbps = u.round((summary.bytes / 1e+6) / summary.duration * 1000, 3);

			// OLD QUOTA
			// // 2GB uncompressed per min (rolling)
			// // ? https://developer.mixpanel.com/reference/import-events#rate-limits
			// const quota = 2e9; //2GB in bytes
			// const gbPerMin = (summary.bytes / quota) / (summary.duration / 60000);
			// summary.percentQuota = u.round(gbPerMin, 5) * 100;

			// NEW QUOTA
			const quota = 1.8e6; // 1.8M events per min 
			const eventsPerMin = summary.total / (summary.duration / 60000);
			summary.percentQuota = u.round(eventsPerMin / quota, 5) * 100;

		}

		summary.errors = this.errors;


		if (includeResponses && this?.responses?.length) {
			summary.responses = this.responses;
		}

		if (this.file) {
			summary.file = this.file;
		}

		if (this.folder) {
			summary.folder = this.folder;
		}

		if (this.abridged) {
			const includeOnly = [
				"bytes",
				"bytesHuman",
				"duration",
				"durationHuman",
				"dryRun",
				"eps",
				"rateLimit",
				"recordType",
				"requests",
				"success",
				"total",
				"failed",
				"errors"
			];
			for (const key in summary) {
				if (!includeOnly.includes(key)) delete summary[key];
			}
			if (!summary?.dryRun?.length) delete summary.dryRun;
		}

		return summary;
	}
}


/** 
 * helper to parse values passed in from cli
 * @param {string | string[] | import('../index').genericObj | void | any} val - value to parse
 * @param {any} [defaultVal] value if it can't be parsed
 * @return {Object<length,
 *  number>}
 */
function parse(val, defaultVal = []) {
	if (typeof val === 'string') {
		try {
			val = JSON.parse(val);
		}
		catch (firstError) {
			try {
				if (typeof val === 'string') val = JSON.parse(val?.replace(/'/g, '"'));
			}
			catch (secondError) {
				if (this.verbose) console.log(`error parsing tags: ${val}\ntags must be valid JSON`);
				val = defaultVal; //bad json
			}
		}
	}
	return val;
}


// a noop function
function noop(a) { return a; }

// for catching parse errors
function returnEmpty(jobConfig) {
	// eslint-disable-next-line no-unused-vars
	return function (_err, _record, _reviver) {
		jobConfig.unparsable++;
		return {};
	};
}

module.exports = Job;