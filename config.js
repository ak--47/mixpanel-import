const dayjs = require('dayjs');
const dateFormat = `YYYY-MM-DD`;
const u = require('ak-tools');
const transforms = require('./transforms.js');
// eslint-disable-next-line no-unused-vars
const types = require("./types/types.js");

/**
 * a singleton to hold state about the imported data
 * @example
 * const config = new importJob(creds, opts)
 * @class 
 * @param {types.Creds} creds - mixpanel project credentials
 * @param {types.Options} opts - options for import
 * @method summary summarize state of import
*/
class importJob {
	constructor(creds, opts) {
		// ? credentials
		this.acct = creds.acct || ``; //service acct username
		this.pass = creds.pass || ``; //service acct secret
		this.project = creds.project || ``; //project id
		this.secret = creds.secret || ``; //api secret (deprecated auth)
		this.bearer = creds.bearer || ``;
		this.token = creds.token || ``; //project token 
		this.lookupTableId = creds.lookupTableId || ``; //lookup table id
		this.groupKey = creds.groupKey || ``; //group key id
		this.auth = this.resolveProjInfo();


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

		// ? string options
		this.recordType = opts.recordType || `event`; // event, user, group or table		
		this.streamFormat = opts.streamFormat || ''; // json or jsonl ... only relevant for streams
		this.region = opts.region || `US`; // US or EU

		// ? number options
		this.streamSize = opts.streamSize || 27; // power of 2 for highWaterMark in stream  (default 134 MB)		
		this.recordsPerBatch = opts.recordsPerBatch || 2000; // records in each req; max 2000 (200 for groups)
		this.bytesPerBatch = opts.bytesPerBatch || 2 * 1024 * 1024; // max bytes in each req
		this.maxRetries = opts.maxRetries || 10; // number of times to retry a batch
		this.timeOffset = opts.timeOffset || 0; // utc hours offset

		// ? don't allow batches bigger than API limits
		if (this.type === 'event' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'user' && this.recordsPerBatch > 2000) this.recordsPerBatch = 2000;
		if (this.type === 'group' && this.recordsPerBatch > 200) this.recordsPerBatch = 200;

		// ? boolean options
		this.compress = u.isNil(opts.compress) ? false : opts.compress; //gzip data (events only)
		this.strict = u.isNil(opts.strict) ? true : opts.strict; // use strict mode?
		this.logs = u.isNil(opts.logs) ? false : opts.logs; //create log file
		this.where = u.isNil(opts.logs) ? '' : opts.where; // where to put logs
		this.verbose = u.isNil(opts.verbose) ? true : opts.verbose;  // print to stdout?
		this.fixData = u.isNil(opts.fixData) ? false : opts.fixData; //apply transforms on the data
		this.removeNulls = u.isNil(opts.removeNulls) ? false : opts.removeNulls; //remove null fields
		this.abridged = u.isNil(opts.abridged) ? false : opts.abridged; //don't include success responses
		this.forceStream = u.isNil(opts.forceStream) ? true : opts.forceStream; //don't ever buffer files into memory

		// ? transform options
		this.transformFunc = opts.transformFunc || function noop(a) { return a; }; //will be called on every record
		this.ezTransform = function noop(a) { return a; }; //placeholder for ez transforms
		this.nullRemover = function noop(a) { return a; }; //placeholder for null remove
		this.UTCoffset = function noop(a) { return a; }; //placeholder for null remove
		if (this.fixData) this.ezTransform = transforms.ezTransforms(this);
		if (this.removeNulls) this.nullRemover = transforms.removeNulls();
		if (this.timeOffset) this.UTCoffset = transforms.UTCoffset(this.timeOffset);

		// ? counters
		this.recordsProcessed = 0;
		this.success = 0;
		this.failed = 0;
		this.retries = 0;
		this.batches = 0;
		this.requests = 0;
		this.empty = 0;
		this.timer = u.time('etl');

		// ? requests
		this.reqMethod = "POST";
		this.contentType = "application/json";
		this.encoding = "";
		this.responses = [];
		this.errors = [];
		this.workers = Number.isInteger(opts.workers) ? opts.workers : 10;
		this.highWater = (this.workers * this.recordsPerBatch) || 2000;

		// ? allow plurals
		if (this.recordType === 'events') this.recordType === 'event';
		if (this.recordType === 'users') this.recordType === 'user';
		if (this.recordType === 'groups') this.recordType === 'group';
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
	supportedTypes = ['event', 'user', 'group', 'table'];
	lineByLineFileExt = ['.txt', '.jsonl', '.ndjson'];
	objectModeFileExt = ['.json'];
	supportedFileExt = [...this.lineByLineFileExt, ...this.objectModeFileExt, '.csv'];
	endpoints = {
		us: {
			event: `https://api.mixpanel.com/import`,
			user: `https://api.mixpanel.com/engage`,
			group: `https://api.mixpanel.com/groups`,
			table: `https://api.mixpanel.com/lookup-tables/`,
			export: `https://data.mixpanel.com/api/2.0/export`,
			peopleexport: `https://mixpanel.com/api/2.0/engage`
		},
		eu: {
			event: `https://api-eu.mixpanel.com/import`,
			user: `https://api-eu.mixpanel.com/engage`,
			group: `https://api-eu.mixpanel.com/groups`,
			table: `https://api-eu.mixpanel.com/lookup-tables/`,
			export: `https://data-eu.mixpanel.com/api/2.0/export`,
			peopleexport: `https://eu.mixpanel.com/api/2.0/engage`
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
		const { acct, pass, project, secret, token, lookupTableId, groupKey, auth } = this;
		return { acct, pass, project, secret, token, lookupTableId, groupKey, auth };
	}
	set batchSize(chunkSize) {
		this.recordsPerBatch = chunkSize;
	}
	set transform(fn) {
		this.transformFunc = fn;
	}

	// ? methods
	report() {
		return Object.assign(this);
	}
	store(response, success = true) {
		if (!this.abridged) {
			if (success) this.responses.push(response);
		}

		if (!success) this.errors.push(response);
	}
	getVersion() {
		const { version } = require('./package.json');
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

		else if (this.bearer) {
			return `Bearer ${this.bearer}`;
		}

		else if (this.type === 'user' || this.type === 'group') {
			return ``;
		}

		else {
			console.error('no secret or service account provided! quitting...');
			process.exit(0);
		}

	}
	/**
	 * summary of the results of an import
	 * @param {boolean} includeResponses - should `errors` and `responses` be included in summary
	 * @returns {types.ImportResults} `{success, failed, total, requests, duration}`
	 */
	summary(includeResponses = true) {
		const summary = {
			success: this.success,
			failed: this.failed,
			total: this.recordsProcessed,
			requests: this.requests,
			batches: this.batches,
			recordType: this.recordType,
			duration: this.timer.report(false).delta,
			human: this.timer.report(false).human,
			retries: this.retries,
			version: this.version,
			workers: this.workers,
			empty: this.empty
		};

		summary.eps = Math.floor(this.recordsProcessed / this.timer.report(false).delta * 1000);
		summary.rps = u.round(summary.requests / this.timer.report(false).delta * 1000, 3);

		if (includeResponses) {
			summary.responses = this.responses;
			summary.errors = this.errors;
		}

		if (this.file) {
			summary.file = this.file;
		}

		if (this.folder) {
			summary.folder = this.folder;
		}

		return summary;
	}
}

module.exports = importJob;