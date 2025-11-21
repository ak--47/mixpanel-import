declare namespace main {
  /**
   * Mixpanel Importer Stream
   * stream `events`, `users`, `groups`, and `tables` to mixpanel!
   * @example
   * // pipe a stream to mixpanel
   * const { createMpStream } = require('mixpanel-import')
   * const mpStream = createMpStream(creds, opts, callback);
   * const observer = new PassThrough({objectMode: true})
   * observer.on('data', (response)=> { })
   * // create a pipeline
   * myStream.pipe(mpStream).pipe(observer);
   * @param {import('../index.d.ts').Creds} creds - mixpanel project credentials
   * @param {import('../index.d.ts').Options} opts - import options
   * @param {function(): importJob | void} finish - end of pipelines
   * @returns a transform stream
   */
  function createMpStream(
    creds: Creds,
    opts: Options,
    finish?: Function
  ): import("stream").Transform;
  async function validateToken(token: string): Promise<{
    token: string;
    valid: boolean;
    type: "idmgmt_v2" | "idmgmt_v3" | "unknown";
  }>;
  /**
   * valid records types which can be imported
   */
  type RecordType =
    | "event"
    | "user"
    | "group"
    | "table"
    | "export"
    | "profile-export"
    | "profile-delete"
    | "group-export"
    | "group-delete"   
    | "scd"
    | "annotations"
    | "get-annotations"
	| "delete-annotations"
	| "export-import-event"
	| "export-import-profile"
	| "export-import-group";
	
  /**
   * - a path to a file/folder, objects in memory, or a readable object/file stream that contains data you wish to import
   */
  type Data =
    | string
    | Array<mpEvent | mpUser | mpGroup>
    | import("fs").ReadStream
    | Any;
  /**
   * mixpanel project credentials for the import job
   */
  type Creds = {
    /**
     * - service account username
     */
    acct?: string;
    /**
     * - service account password
     */
    pass?: string;
    /**
     * - project id (numeric ID, passed as string internally)
     */
    project?: number | string;
    /**
     * - project token (for importing user profiles)
     */
    token?: string;
    /**
     * - lookup table ID (for importing lookup tables)
     */
    lookupTableId?: string;
    /**
     * - group identifier (for importing group profiles)
     */
    groupKey?: string;
    /**
     * - mixpanel project secret
     */
    secret?: string;
    /**
     * - a bearer token (https://mixpanel.com/oauth/access_token) which be used for exports
     */
    bearer?: string;
    /**
     * - workspace id (numeric ID for data views, passed as string internally)
     */
    workspace?: number | string;
    /**
     * - organization id (numeric ID for data views, passed as string internally)
     */
    org?: number | string;
	/**
	 * - for export/import (the destination project)
	 */
	secondToken?: string;
	/**
	 * - Google Cloud project ID for GCS operations (defaults to 'mixpanel-gtm-training')
	 */
	gcpProjectId?: string;
	/**
	 * - AWS S3 access key ID for S3 operations
	 */
	s3Key?: string;
	/**
	 * - AWS S3 secret access key for S3 operations
	 */
	s3Secret?: string;
	/**
	 * - AWS S3 region for S3 operations (required for S3 access)
	 */
	s3Region?: string;
	/**
	 * - Path to GCS service account credentials JSON file (optional, defaults to ADC)
	 */
	gcsCredentials?: string;
	/**
	 * - a data_group_id to use for exporting group profiles
	 */
	dataGroupId?: string;
  };

  /**
   * built in transform functions for various vendors
   */
  type Vendors =
    | "amplitude"
    | "heap"
    | "mixpanel"
    | "ga4"
    | "june"
    | "adobe"
    | "pendo"
    | "mparticle"
    | ""
	| "posthog";

  type WhiteAndBlackListParams = {
    eventWhitelist: string[];
    eventBlacklist: string[];
    propKeyWhitelist: string[];
    propKeyBlacklist: string[];
    propValWhitelist: string[];
    propValBlacklist: string[];
    comboWhiteList: { [key: string]: string[] };
    comboBlackList: { [key: string]: string[] };
  };

  type Regions = "US" | "EU" | "IN";
  type SupportedFormats = "strict_json" | "jsonl" | "csv" | "parquet";
  type transports = 'got' | 'undici';

  type dependentTables = {
	filePath: string;
	keyOne: string;
	keyTwo: string;
	label?: string;
  }

  /**
   * Job class interface - extends Options with runtime properties
   */
  interface Job extends Options {
    bytesCache?: WeakMap<any, number>;
    lineByLineFileExt: string[];
    objectModeFileExt: string[];
    tableFileExt: string[];
    gzippedLineByLineFileExt: string[];
    gzippedObjectModeFileExt: string[];
    gzippedTableFileExt: string[];
    streamFormat: SupportedFormats;
  }

  /**
   * Configuration options for the import/export job
   * @interface Options
   */
  type Options = {
    // ═══════════════════════════════════════════════════════════════
    // CORE CONFIGURATION
    // ═══════════════════════════════════════════════════════════════

    /**
     * Type of record to import/export
     * @default "event"
     * @example
     * { recordType: "event" }    // Import events
     * { recordType: "user" }     // Import user profiles
     * { recordType: "group" }    // Import group profiles
     * { recordType: "table" }    // Import lookup tables
     * { recordType: "export" }   // Export events
     */
    recordType?: RecordType;

    /**
     * Data residency region for Mixpanel API
     * @default "US"
     * @example
     * { region: "US" }  // United States data center
     * { region: "EU" }  // European data center
     * { region: "IN" }  // India data center
     */
    region?: Regions;

    /**
     * Format of the data stream
     * @default "jsonl"
     * @example
     * { streamFormat: "jsonl" }     // Newline-delimited JSON
     * { streamFormat: "strict_json" } // Standard JSON array
     * { streamFormat: "csv" }       // CSV with headers
     * { streamFormat: "parquet" }   // Apache Parquet format
     */
    streamFormat?: SupportedFormats;

    // ═══════════════════════════════════════════════════════════════
    // PERFORMANCE & CONCURRENCY
    // ═══════════════════════════════════════════════════════════════

    /**
     * Number of concurrent HTTP workers for parallel requests
     * Controls the speed of data import (more workers = faster)
     * @range 1-50 (practical), 1-100 (theoretical)
     * @default 10
     * @memory Each worker holds ~2-3 batches in memory
     * @example
     * { workers: 5 }   // Conservative (low memory)
     * { workers: 10 }  // Balanced (default)
     * { workers: 30 }  // Aggressive (high speed, high memory)
     */
    workers?: number;

    /**
     * Alias for workers (either works)
     * @deprecated Use `workers` instead
     */
    concurrency?: number;

    /**
     * Stream buffer size between pipeline stages
     * Controls memory vs throughput trade-off
     * @range 16-500
     * @default min(workers * 10, 100)
     * @memory Direct impact - each object stored in buffer
     * @example
     * // Small events (<1KB): maximize throughput
     * { workers: 30, highWater: 200 }
     *
     * // Large events (>10KB): minimize memory
     * { workers: 3, highWater: 20 }
     *
     * // Balanced approach
     * { workers: 10, highWater: 100 }
     */
    highWater?: number;

    /**
     * Maximum records per API batch
     * @range 1-2000 (events/users), 1-200 (groups)
     * @default 2000 (events/users), 200 (groups)
     * @example
     * { recordsPerBatch: 2000 }  // Maximum efficiency
     * { recordsPerBatch: 1000 }  // Reduced memory per batch
     * { recordsPerBatch: 500 }   // For very large events
     */
    recordsPerBatch?: number;

    /**
     * Maximum bytes per API batch
     * @range 1-10485760 (10MB max API limit)
     * @default 10276045 (9.8MB - safely under 10MB limit)
     * @example
     * { bytesPerBatch: 10276045 }  // Default - 9.8MB
     * { bytesPerBatch: 5242880 }   // Conservative 5MB batches
     * { bytesPerBatch: 1048576 }   // Small 1MB batches
     */
    bytesPerBatch?: number;

    /**
     * Maximum retry attempts for failed requests
     * @range 0-100
     * @default 10
     * @example
     * { maxRetries: 10 }  // Default with exponential backoff
     * { maxRetries: 3 }   // Quick fail for testing
     * { maxRetries: 50 }  // Persistent retry for critical data
     */
    maxRetries?: number;

    // ═══════════════════════════════════════════════════════════════
    // MEMORY MANAGEMENT
    // ═══════════════════════════════════════════════════════════════

    /**
     * Enable aggressive garbage collection for memory-constrained environments
     * - Runs periodic GC every 30 seconds
     * - Triggers emergency GC when heap usage exceeds 90%
     * - Requires Node.js to be started with --expose-gc flag
     * @default false
     * @example
     * // Run with: node --expose-gc index.js
     * { aggressiveGC: true }
     */
    aggressiveGC?: boolean;

    /**
     * Enable memory monitoring without verbose output
     * Tracks and logs memory usage statistics
     * @default false
     * @example
     * { memoryMonitor: true, verbose: false }  // Silent memory tracking
     */
    memoryMonitor?: boolean;

    /**
     * Enable memory throttling for cloud storage streams (GCS/S3)
     * Prevents OOM errors by pausing downloads when memory is high
     * @default false
     * @example
     * // Essential for large cloud files
     * { throttleMemory: true }
     */
    throttleMemory?: boolean;

    /**
     * Alias for throttleMemory (either works)
     * @deprecated Use `throttleMemory` instead
     */
    throttleGCS?: boolean;

    /**
     * Memory threshold (MB) to pause cloud downloads
     * @range 500-8000
     * @default 1500 (1.5GB)
     * @example
     * { throttleMemory: true, throttlePauseMB: 1500 }  // Pause at 1.5GB
     * { throttleMemory: true, throttlePauseMB: 3000 }  // Pause at 3GB (high memory system)
     */
    throttlePauseMB?: number;

    /**
     * Memory threshold (MB) to resume cloud downloads
     * @range 500-8000
     * @default 1000 (1GB)
     * @note Must be lower than throttlePauseMB
     * @example
     * { throttleMemory: true, throttleResumeMB: 1000 }  // Resume at 1GB
     */
    throttleResumeMB?: number;

    /**
     * Maximum buffer size (MB) for BufferQueue
     * @range 1000-10000
     * @default 2000 (2GB)
     * @example
     * { throttleMaxBufferMB: 2000 }  // 2GB max buffer
     */
    throttleMaxBufferMB?: number;
    // ═══════════════════════════════════════════════════════════════
    // DATA COMPRESSION & STREAMING
    // ═══════════════════════════════════════════════════════════════

    /**
     * Enable gzip compression for API requests (events only)
     * Reduces bandwidth by 60-80%
     * @default true
     * @example
     * { compress: true }   // Enable compression (recommended)
     * { compress: false }  // Disable for debugging
     */
    compress?: boolean;

    /**
     * Gzip compression level (1=fastest, 9=smallest)
     * @range 0-9 (0=no compression, 9=maximum)
     * @default 6
     * @example
     * { compressionLevel: 1 }  // Fast compression
     * { compressionLevel: 6 }  // Balanced (default)
     * { compressionLevel: 9 }  // Maximum compression
     */
    compressionLevel?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;

    /**
     * Force treat input files as gzipped (overrides extension detection)
     * @default false
     * @example
     * // For files without .gz extension that are gzipped
     * { isGzip: true }
     */
    isGzip?: boolean;

    /**
     * Force streaming mode even for small files
     * @default false
     * @example
     * { forceStream: true }  // Always stream, never buffer in memory
     */
    forceStream?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // DATA TRANSFORMATION
    // ═══════════════════════════════════════════════════════════════

    /**
     * Custom transformation function applied to each record
     * - Return {} to skip the record
     * - Return [{}, {}, {}] to split into multiple records
     * @default null
     * @example
     * {
     *   transformFunc: (record) => {
     *     // Skip test events
     *     if (record.properties.test) return {};
     *     // Add custom property
     *     record.properties.processed = true;
     *     return record;
     *   }
     * }
     */
    transformFunc?: transFunc;

    /**
     * Apply built-in data fixes and validations
     * @default true
     * @example
     * { fixData: true }  // Apply all automatic fixes
     */
    fixData?: boolean;

    /**
     * Fix and validate timestamp formats to UNIX epoch
     * @default false
     * @example
     * { fixTime: true }  // Convert various time formats to UNIX timestamp
     */
    fixTime?: boolean;

    /**
     * Remove null, empty, and undefined values from records
     * @default false
     * @example
     * { removeNulls: true }  // Clean up sparse data
     */
    removeNulls?: boolean;

    /**
     * UTC offset in hours for time adjustments
     * @range -12 to 12
     * @default 0
     * @example
     * { timeOffset: -8 }  // PST timezone adjustment
     * { timeOffset: 1 }   // CET timezone adjustment
     */
    timeOffset?: number;

    /**
     * Tags to add to all records
     * @default {}
     * @example
     * { tags: { source: "mobile_app", version: "2.0" } }
     */
    tags?: genericObj;

    /**
     * Property key aliases for renaming fields
     * Required for CSV imports to map columns
     * @default {}
     * @example
     * // For CSV imports
     * { aliases: {
     *   "user_id": "distinct_id",
     *   "timestamp": "time",
     *   "action": "event"
     * }}
     */
    aliases?: genericObj;

    // ═══════════════════════════════════════════════════════════════
    // VENDOR TRANSFORMS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Built-in transform for vendor data formats
     * @default null
     * @example
     * { vendor: "amplitude" }  // Convert Amplitude data
     * { vendor: "posthog" }    // Convert PostHog data
     * { vendor: "ga4" }        // Convert Google Analytics 4
     * { vendor: "heap" }       // Convert Heap Analytics
     */
    vendor?: Vendors;

    /**
     * Options for vendor-specific transforms
     * @default {}
     * @example
     * // PostHog options
     * { vendorOpts: { v2_compat: true, ignore_events: ["$pageview"] } }
     * // GA4 options
     * { vendorOpts: { time_conversion: "ms", set_insert_id: true } }
     */
    vendorOpts?: amplitudeOpts | heapOpts | ga4Opts | juneOpts | postHogOpts | {};

    // ═══════════════════════════════════════════════════════════════
    // FILTERING & VALIDATION
    // ═══════════════════════════════════════════════════════════════

    /**
     * Remove duplicate records based on content hash
     * @default false
     * @example
     * { dedupe: true }  // Skip duplicate records
     */
    dedupe?: boolean;

    /**
     * Skip records before this UNIX timestamp (seconds)
     * @example
     * { epochStart: 1609459200 }  // Skip before Jan 1, 2021
     */
    epochStart?: number;

    /**
     * Skip records after this UNIX timestamp (seconds)
     * @example
     * { epochEnd: 1640995200 }  // Skip after Jan 1, 2022
     */
    epochEnd?: number;

    /**
     * Only import events with these names
     * @example
     * { eventWhitelist: ["Sign Up", "Purchase", "Login"] }
     */
    eventWhitelist?: string[];

    /**
     * Skip events with these names
     * @example
     * { eventBlacklist: ["Test Event", "$pageview"] }
     */
    eventBlacklist?: string[];

    /**
     * Only import events containing these property keys
     * @example
     * { propKeyWhitelist: ["user_id", "session_id"] }
     */
    propKeyWhitelist?: string[];

    /**
     * Skip events containing these property keys
     * @example
     * { propKeyBlacklist: ["internal_id", "debug_info"] }
     */
    propKeyBlacklist?: string[];

    /**
     * Only import events with these property values
     * @example
     * { propValWhitelist: ["production", "paid"] }
     */
    propValWhitelist?: string[];

    /**
     * Skip events with these property values
     * @example
     * { propValBlacklist: ["test", "debug"] }
     */
    propValBlacklist?: string[];

    /**
     * Only import events with specific key-value combinations
     * @example
     * { comboWhiteList: { environment: ["production"], plan: ["enterprise"] } }
     */
    comboWhiteList?: { [key: string]: string[] };

    /**
     * Skip events with specific key-value combinations
     * @example
     * { comboBlackList: { status: ["deleted"], test: ["true"] } }
     */
    comboBlackList?: { [key: string]: string[] };
    /**
     * Validate data strictly (events only)
     * @default true
     * @example
     * { strict: true }   // Enforce strict validation
     * { strict: false }  // Allow malformed data
     */
    strict?: boolean;

    /**
     * Scrub specific properties from all records (PII removal)
     * @example
     * { scrubProps: ["ssn", "credit_card", "email"] }
     */
    scrubProps?: string[];

    /**
     * Drop specific columns from CSV/TSV data
     * @example
     * { dropColumns: ["internal_id", "debug_column"] }
     */
    dropColumns?: string[];

    /**
     * Generate insert_id from specified columns (uses MurmurHash3)
     * @example
     * { insertIdTuple: ["user_id", "timestamp", "event"] }
     */
    insertIdTuple?: string[];

    /**
     * Fix malformed JSON in string values
     * @default false
     * @example
     * { fixJson: true }  // Attempt to parse and fix broken JSON
     */
    fixJson?: boolean;

    /**
     * Flatten nested object structures
     * @default false
     * @example
     * { flattenData: true }  // Convert nested.property to nested_property
     */
    flattenData?: boolean;

    /**
     * Add token to all records
     * @default false
     * @example
     * { addToken: true }  // Adds $token or token field
     */
    addToken?: boolean;

    /**
     * Skip all transformations (fast mode for pre-processed data)
     * @default false
     * @example
     * { fastMode: true }  // Skip all data transformations
     */
    fastMode?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // OUTPUT & LOGGING
    // ═══════════════════════════════════════════════════════════════

    /**
     * Display verbose output with detailed progress
     * @default true
     * @example
     * { verbose: false }  // Silent mode
     */
    verbose?: boolean;

    /**
     * Show progress bar (only when verbose is false)
     * @default false
     * @example
     * { verbose: false, showProgress: true }  // Progress bar only
     */
    showProgress?: boolean;

    /**
     * Save results to ./logs/ directory
     * @default false
     * @example
     * { logs: true }  // Create detailed log files
     */
    logs?: boolean;

    /**
     * Only include error responses, not successes
     * @default false
     * @example
     * { abridged: true }  // Minimize response storage
     */
    abridged?: boolean;

    /**
     * Base directory for logs and exports
     * @default "./"
     * @example
     * { where: "/tmp/mixpanel/" }  // Custom output directory
     */
    where?: string;

    /**
     * Callback for progress updates (used by UI WebSocket)
     * @internal
     */
    progressCallback?: (recordType: string, processed: number, requests: number, eps: string, bytesProcessed: number) => void;

    // ═══════════════════════════════════════════════════════════════
    // TESTING & DRY RUN
    // ═══════════════════════════════════════════════════════════════

    /**
     * Test mode - transform but don't send to Mixpanel
     * @default false
     * @example
     * { dryRun: true }  // Test transformations without API calls
     */
    dryRun?: boolean;

    /**
     * Maximum records to process (useful for testing)
     * @example
     * { maxRecords: 1000 }  // Process only first 1000 records
     */
    maxRecords?: number;

    /**
     * Keep bad/failed records in results
     * @default false
     * @example
     * { keepBadRecords: true }  // Include failed records for debugging
     */
    keepBadRecords?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // EXPORT OPTIONS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Start date for exports (YYYY-MM-DD)
     * @example
     * { start: "2024-01-01" }
     */
    start?: string;

    /**
     * End date for exports (YYYY-MM-DD)
     * @example
     * { end: "2024-12-31" }
     */
    end?: string;

    /**
     * Limit the number of records returned (exports only)
     * @example
     * { limit: 1000 }  // Return max 1000 records
     */
    limit?: number;

    /**
     * WHERE clause for exports (Mixpanel segmentation expression syntax)
     * @see https://developer.mixpanel.com/reference/segmentation-expressions
     * @example
     * { whereClause: "properties['$os'] == 'iOS'" }
     */
    whereClause?: string;

    /**
     * Additional query parameters for export endpoints
     * @example
     * { params: { event: ['Sign Up'], limit: 1000 } }
     */
    params?: Record<string, any>;

    /**
     * Cohort ID for profile exports
     * @example
     * { cohortId: 12345 }
     */
    cohortId?: string | number;

    /**
     * Data group ID for group profile exports
     * @example
     * { dataGroupId: "company" }
     */
    dataGroupId?: string;

    // ═══════════════════════════════════════════════════════════════
    // FILE OUTPUT OPTIONS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Write transformed data to file instead of Mixpanel
     * @default false
     * @example
     * { writeToFile: true, outputFilePath: "./output.jsonl" }
     */
    writeToFile?: boolean;

    /**
     * Output file path for writeToFile mode
     * @example
     * { outputFilePath: "./transformed_data.jsonl" }
     */
    outputFilePath?: string;

    /**
     * Destination path for exports or dual writing
     * @example
     * { destination: "./exports/events.jsonl" }
     * { destination: "gs://bucket/path/events.jsonl" }
     * { destination: "s3://bucket/path/events.jsonl" }
     */
    destination?: string;

    /**
     * Skip Mixpanel and only write to destination
     * @default false
     * @example
     * { destinationOnly: true, destination: "./output.jsonl" }
     */
    destinationOnly?: boolean;

    /**
     * Skip writing export data to disk (hold in memory)
     * @default false
     * @internal Used by UI for streaming exports
     */
    skipWriteToDisk?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // ADVANCED OPTIONS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Transport mechanism for HTTP requests
     * @default "undici" (faster than got)
     * @example
     * { transport: "undici" }  // Recommended
     * { transport: "got" }     // Legacy option
     */
    transport?: transports;

    /**
     * Use HTTP/2 (experimental, usually slower)
     * @default false
     */
    http2?: boolean;

    /**
     * Error handler for parsing failures
     * @example
     * { parseErrorHandler: (err, record) => ({ ...record, error: err.message }) }
     */
    parseErrorHandler?: transFunc;

    /**
     * Response handler for API responses (debugging)
     * @internal
     */
    responseHandler?: (response: any, record: any) => void;

    /**
     * Dimension maps for lookups in transforms
     * @example
     * { dimensionMaps: [{ filePath: "./users.csv", keyOne: "id", keyTwo: "name" }] }
     */
    dimensionMaps?: dependentTables[];

    /**
     * Heavy objects cache for transforms
     * @internal
     */
    heavyObjects?: Object;

    /**
     * Second region for export-import operations
     * @example
     * { secondRegion: "EU" }  // Export from US, import to EU
     */
    secondRegion?: "US" | "EU" | "IN" | "";

    /**
     * Bytes cache for performance optimization
     * @internal
     */
    bytesCache?: WeakMap<any, number>;

    /**
     * Path to GCS service account credentials
     * @example
     * { gcsCredentials: "./service-account.json" }
     */
    gcsCredentials?: string;

    /**
     * Google Cloud project ID for GCS operations
     * @default "mixpanel-gtm-training"
     * @example
     * { gcpProjectId: "my-project-123" }
     */
    gcpProjectId?: string;

    /**
     * AWS S3 access key ID
     * @example
     * { s3Key: "FOOBARBAZ7EXAMPLE" }
     */
    s3Key?: string;

    /**
     * AWS S3 secret access key
     * @example
     * { s3Secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" }
     */
    s3Secret?: string;

    /**
     * AWS S3 region
     * @example
     * { s3Region: "us-west-2" }
     */
    s3Region?: string;

    /**
     * Enable v2 compatibility mode for ID management
     * @default false
     * @example
     * { v2_compat: true }  // Auto-set distinct_id from $user_id or $device_id
     */
    v2_compat?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // SCD (SLOWLY CHANGING DIMENSIONS) OPTIONS
    // ═══════════════════════════════════════════════════════════════

    /**
     * SCD label for data definitions
     * @internal
     */
    scdLabel?: string;

    /**
     * SCD key in the data
     * @example
     * { scdKey: "plan_type" }
     */
    scdKey?: string;

    /**
     * SCD data type
     * @example
     * { scdType: "string" }
     */
    scdType?: "string" | "number" | "boolean";

    /**
     * SCD ID in data definitions
     * @internal
     */
    scdId?: string;

    /**
     * SCD property ID
     * @internal
     */
    scdPropId?: string;

    /**
     * Group key for group profiles
     * @example
     * { groupKey: "company_id" }
     */
    groupKey?: string | number;

    /**
     * Create profiles after SCD import
     * @default false
     */
    createProfiles?: boolean;

    // ═══════════════════════════════════════════════════════════════
    // PROFILE UPDATE OPTIONS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Profile update operation directive (user and group profiles only)
     * Controls how properties are updated in user/group profiles
     * @default "$set"
     * @example
     * { directive: "$set" }       // Overwrite existing values (default)
     * { directive: "$set_once" }  // Only set if property doesn't exist
     * { directive: "$add" }       // Increment numeric properties
     * { directive: "$union" }     // Add unique items to list properties
     * { directive: "$append" }    // Add items to list properties
     * { directive: "$remove" }    // Remove specific items from list properties
     * { directive: "$unset" }     // Delete properties from profiles
     */
    directive?: ProfileOperation;
  };

  /**
   * - a transform function to `map()` over the data
   * - if it returns `{}` the record will be skipped
   * - if it returns `[{},{},{}]` the record will be split into multiple records
   */
  type transFunc = (
    data: any,
	heavyObjects?: any
  ) => mpEvent | mpUser | mpGroup | Object[] | Object;
  /**
   * - a transform function to handle parsing errors
   * - whatever is returned will be forwarded down the pipeline
   * - the signature of this function is `(err, record, reviver) => {}`
   * - default is  `(a) => { return {} }}`
   */
  type ErrorHandler = (err: Error, record: Object, reviver: any) => any;
  /**
   * a summary of the import
   */
  type ImportResults = {
    /**
     * - type of record imported
     */
    recordType: RecordType;
    /**
     * - num records successfully imported
     */
    success: number;
    /**
     * - num of records failed to import
     */
    failed: number;
    /**
     * - num of request retries
     */
    retries: number;
    /**
     * - num of total records processed
     */
    total: number;
    /**
     * - num of empty records found
     */
    empty: number;
    /**
     * - total num of batches
     */
    batches: number;
    /**
     * - total num of requests
     */
    requests: number;
    /**
     * - estimate of "events per second" throughput
     */
    eps: number;
    /**
     * - estimate of "requests per second" throughput
     */
    rps: number;
    /**
     * - successful import records (200s)
     */
    responses: any[];
    /**
     * - failed import records (400s)
     */
    errors: any;
    /**
     * - the elapsed time in ms
     */
    duration: number;
    /**
     * - human readable timestamp
     */
    durationHuman: string;
    /**
     * - the number of times a 429 response was received (and the request was retried)
     */
    rateLimit: number;
    /**
     * - the number of times a 500x responses was received (and the request was retried)
     */
    serverErrors: number;
    /**
     * - number of times a client side error occurred (timeout/socket hangup... these requests are retried)
     */
    clientErrors: number;
    /**
     * - the number of bytes sent to mixpanel (uncompressed)
     */
    bytes: number;
    /**
     * - MB sent to mixpanel (uncompressed)
     */
    bytesHuman: string;
    /**
     * - throughput in MB/s
     */
    mbps: number;
    /**
     * - summary of memory usage (averaged over the duration of the job)
     */
    memory: Object;
    /**
     * - summary of memory usage in human readable format
     */
    memoryHuman: Object;
    /**
     * - if `false` the data was loaded into memory
     */
    wasStream: Boolean | void;
    /**
     * - average # of records per batch
     */
    avgBatchLength: number;
    /**
     * - the start timestamp of the job (ISO 8601)
     */
    startTime: string;
    /**
     * - the end timestamp of the job (ISO 8601)
     */
    endTime: string;
    /**
     * - data points skipped due to epochStart/epochEnd
     */
    outOfBounds: number;

    /**
     * data points skipped due to dedupe
     * only available if dedupe is true
     */
    duplicates: number;
    /**
     * data points skipped due to whitelist
     */
    whiteListSkipped: number;
    /**
     * data points skipped due to blacklist
     */
    blackListSkipped: number;
    /**
     * data points skipped due to parsing errors
     */
    unparsable?: number;
    /**
     * event exports only: path to exported file
     */
    file?: string;
    /**
     * profile exports only: path to exported folders
     */
    folder?: string;
    /**
     * for dry runs, what is the transformed data
     */
    dryRun?: ArrayOfObjects;
    /**
     * the # of concurrent requests
     */
    workers: number;
    /**
     * app version!
     */
    version: string;
    /**
     * the vendor transform function used
     */
    vendor?: string;
    vendorOpts?: object;
	badRecords?: ArrayOfObjects; // records that failed to import
  };

  type genericObj = {
    [x: string]: string | number | boolean;
  };
  // generic for `{}`
  type openObject = {
    [key: string]: any;
  };

  // generic for `[{},{},{}]`
  type ArrayOfObjects = openObject[];

  // ! MIXPANEL TYPES

  /**
   * valid mixpanel property values; {@link https://help.mixpanel.com/hc/en-us/articles/115004547063-Properties-Supported-Data-Types more info}
   */
  type PropValues =
    | string
    | string[]
    | number
    | number[]
    | boolean
    | boolean[]
    | Date
    | Object
    | Object[]
    | null
    | undefined;
  /**
   * mixpanel's required event properties
   */
  type mpEvStandardProps = {
    /**
     * - uuid of the end user
     */
    distinct_id?: string;
    /**
     * - anon id of the end user (simplified id mgmt)
     */
    $device_id?: string;
    /**
     * - known id of the end user (simplified id mgmt)
     */
    $user_id?: string;
    /**
     * - the UTC time of the event (unix epoch)
     */
    time: number;
    /**
     * - unique row id; used for deduplication
     */
    $insert_id?: string;
  };
  /**
   * event properties payload
   */
  type mpEvProperties = {
    [x: string]: PropValues;
  } & mpEvStandardProps;
  /**
   * - a mixpanel event
   */
  type mpEvent = {
    /**
     * - the event name
     */
    event: string;
    /**
     * - the event's properties
     */
    properties: mpEvProperties;
  };
  /**
   * valid profile update types; {@link https://developer.mixpanel.com/reference/profile-set more info}
   */
  type ProfileOperation =
    | "$set"
    | "$set_once"
    | "$add"
    | "$union"
    | "$append"
    | "$remove"
    | "$unset";
  /**
   * object of k:v pairs to update the profile
   */
  type ProfileData = Partial<
    Record<
      ProfileOperation,
      {
        [x: string]: PropValues;
      }
    >
  >;
  type mpUserStandardProps = {
    /**
     * - the `distinct_id` of the profile to update
     */
    $distinct_id: string;
    /**
     * - the mixpanel project identifier
     */
    $token: string;
    /**
     * - the IP of the end user (used for geo resolution) or `0` to turn off
     */
    $ip?: string | number;
    /**
     * - whether or not to update `$last_seen`; default `true`
     */
    $ignore_time?: boolean;
  };
  /**
   * - a mixpanel user profile
   */
  type mpGroupStandardProps = {
    /**
     * - the group (analytics) key for the entity
     */
    $group_key: string;
    /**
     * - the uuid of the group; like `$distinct_id` for user profiles
     */
    $group_id: string;
    /**
     * - the mixpanel project identifier
     */
    $token: string;
  };
  /**
   * a group profile update payload
   */
  type mpGroup = mpGroupStandardProps & ProfileData;
  /**
   * a user profile update payload
   */
  type mpUser = mpUserStandardProps & ProfileData;

  /**
   * posthog transform opts
   */
  type postHogOpts = {
	device_id_map?: Map<string, string>;
	device_id_file?: string;
	v2_compat?: boolean; // use v2 api
	ignore_events?: string[]; // ignore these events
	ignore_props?: string[]; // strip these properties
	identify_events?: string[];
	directive?: ProfileOperation;
  }

  /**
   * amplitude transform opts
   */
  type amplitudeOpts = {
    user_id?: string;
    group_keys?: string[];
    v2_compat?: boolean; // use v2 api
  };

  /**
   * mparticle transform opts
   */
  type mparticleOpts = {
    user_id?: string[];
    device_id?: string[];
    insert_id?: string;
    user_attributes?: boolean;
    context?: boolean;
    identities?: boolean;
    application_info?: boolean;
    device_info?: boolean;
    source_info?: boolean;
  };

  /**
   * GA4 transform opts
   */
  type ga4Opts = {
    user_id?: string;
    device_id?: string;
    group_keys?: string[];
    insert_id_col?: string;
    set_insert_id?: boolean;
    insert_id_tup?: string[];
    time_conversion?: "ms" | "s" | "milliseconds" | "seconds";
  };

  /**
   * heap transform opts
   */
  type heapOpts = {
    user_id?: string;
    group_keys?: string[];
    device_id_map?: Map<string, string>;
    device_id_file?: string;
  };

  /**
   * June.so transform opts
   */
  type juneOpts = {
    user_id?: string;
    anonymous_id?: string;
    group_key?: string;
    v2compat?: boolean;
  };
}

/**
 * Mixpanel Importer
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * const mp = require('mixpanel-import')
 * const imported = await mp(creds, data, options)
 * @param {import('./index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('./index.d.ts').Data} data - data to import
 * @param {import('./index.d.ts').Options} opts - import options
 * @param {boolean} isCLI - `true` when run as CLI
 * @returns {Promise<import('./index.d.ts').ImportResults>} API receipts of imported data
 */
declare function main(
  creds: main.Creds | void,
  data: main.Data,
  opts?: main.Options,
  isCLI?: boolean
): Promise<main.ImportResults>;

// Additional type definitions for external libraries and custom extensions

declare module 'got' {
  interface GotResponse {
    body: any;
    statusCode: number;
    ip?: string;
    requestUrl?: string;
    headers: any;
    json(): any;
  }
  
  interface GotPromise extends Promise<GotResponse> {
    json(): Promise<any>;
  }
  
  interface Got {
    (options: any): GotPromise;
    (url: string, options?: any): GotPromise;
    stream(options: any): import('stream').Readable;
    stream(url: string, options?: any): import('stream').Readable;
  }
  const got: Got;
  export = got;
}

// Extend Node.js stream types with custom properties
declare module 'stream' {
  interface Transform {
    _buf?: string;
  }
  
  interface Readable {
    _page?: number;
    _session_id?: string | null;
    _buffer?: any[];
  }
}

declare module 'fs' {
  interface ReadStream {
    path?: string | Buffer;
    pending?: boolean;
  }
}

// Extend Node.js zlib types
declare module 'zlib' {
  interface Gunzip extends import('stream').Transform {
    path?: string | Buffer;
    pending?: boolean;
  }
}

// Extend Express Request type for multer file uploads
declare global {
  namespace Express {
    interface Request {
      files?: Array<{
        buffer: Buffer;
        fieldname: string;
        originalname: string;
        encoding: string;
        mimetype: string;
        size: number;
        path?: string;
      }>;
    }
  }
  
  namespace Express.Multer {
    interface File {
      buffer: Buffer;
      fieldname: string;
      originalname: string;
      encoding: string;
      mimetype: string;
      size: number;
      path?: string;
    }
  }
  
  // Extend Error type with got-specific properties
  interface Error {
    statusCode?: number;
    ip?: string;
    requestUrl?: string;
    headers?: any;
  }
}

export = main;
