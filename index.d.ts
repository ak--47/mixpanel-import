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
    | "events"
    | "users"
    | "groups"
    | "tables";
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
     * - project id
     */
    project?: string | number;
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
  };

  /**
   * built in transform functions for various vendors
   */
  type Vendors =
    | "amplitude"
    | "heap"
    | "mixpanel"
    | "ga4"
    | "adobe"
    | "pendo"
    | "mparticle"
    | ""
    | void;

  type WhiteAndBlackListParams = {
    eventWhitelist: string[];
    eventBlacklist: string[];
    propKeyWhitelist: string[];
    propKeyBlacklist: string[];
    propValWhitelist: string[];
    propValBlacklist: string[];
  };

  type Regions = "US" | "EU";
  type SupportedFormats = "json" | "jsonl" | "csv";

  /**
   * options for the import job
   */
  type Options = {
    /**
     * - type of record to import (`event`, `user`, `group`, or `table`)
     * - default `event`
     */
    recordType?: RecordType;

    /**
     * - US or EU (data residency)
     * - default `US`
     */
    region?: Regions;

    /**
     * - format of underlying data stream; json or jsonl
     * - default `jsonl`
     */
    streamFormat?: SupportedFormats;
    /**
     * - use gzip compression (events only)
     * - default `true`
     */
    compress?: boolean;
    /**
     * - compression level (events only)
     * - default `6`
     */
    compressionLevel?: 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9;
    /**
     * - validate data on send (events only) ...
     * - default `true`
     */
    strict?: boolean;
    /**
     * - log results to `./logs/`
     * - default `false`
     */
    logs?: boolean;
    /**
     * - display verbose output messages
     * - default `true`
     */
    verbose?: boolean;
    /**
     * - apply various transformations to ensure data is properly ingested
     * - default `true`
     */
    fixData?: boolean;
    /**
     * - remove the following (keys and values) from each record with values = `null`, `''`, `undefined`, `{}`, or `[]`
     * - default `false`
     */
    removeNulls?: boolean;
    /**
     * - included only error responses; not successes
     * - default `false`
     */
    abridged?: boolean;
    /**
     * - don't buffer files into memory (even if they can fit)
     * - default `false`
     */
    forceStream?: boolean;
    /**
     * - 2^N; highWaterMark value for stream [DEPRECATED] ... use workers instead
     */
    streamSize?: number;
    /**
     * - UTC offset which will add/subtract hours to an event's `time` value; can be a positive or negative number; default `0`
     * - default `0`
     */
    timeOffset?: number;
    /**
     * - max # of records in each payload (max 2000; max 200 for group profiles)
     * - default `2000` (events + users), `200` (groups)
     */
    recordsPerBatch?: number;
    /**
     * - max # of bytes in each payload (max 2MB)
     * - default `2000000`
     */
    bytesPerBatch?: number;
    /**
     * - maximum # of times to retry
     * - default `10`
     */
    maxRetries?: number;
    /**
     * - # of concurrent workers sending requests (same as concurrency)
     * - default `10`
     */
    workers?: number;
    /**
     * - # of concurrent requests (same as workers)
     * - default `10`
     */
    concurrency?: number;
    /**
     * - where to put files (logs, exports)
     * - default `./`
     */
    where?: string;
    /**
     * - a transform function to `map()` over the data
     * - if it returns `{}` the record will be skipped
     * - if it returns `[{},{},{}]` the record will be split into multiple records
     * - default `undefined`
     */
    transformFunc?: transFunc;
    /**
     * - a transform function to handle parsing errors
     * - whatever is returned will be forwarded down the pipeline
     * - the signature of this function is `(err, record, reviver) => {}`
     * - default is  `(a) => { return {} }}`
     */
    parseErrorHandler?: transFunc;
    /**
     * - a set of tags which will be added to all records
     * - default `{}`
     */
    tags?: genericObj;
    /**
     * - a set of aliases used to rename property keys in the source data
     * - note this is required for importing CSVs; we expect a value like `{uuid: "distinct_id", row_id: "$insert_id"}`, etc..
     * - default `{}`
     */

    aliases?: genericObj;

    /**
     * data points with a UNIX time BEFORE this value will be skipped
     */
    epochStart?: number;

    /**
     * data points with a UNIX time AFTER this value will be skipped
     */
    epochEnd?: number;

    /**
     * if true, will remove duplicate records based on a hash of the records
     */
    dedupe?: boolean;
    /**
     * only import events on the whitelist
     */
    eventWhitelist?: string[];
    /**
     * don't import events on the blacklist
     */
    eventBlacklist?: string[];
    /**
     * only import events with property keys on the whitelist
     */
    propKeyWhitelist?: string[];
    /**
     * don't import events with property keys on the blacklist
     */
    propKeyBlacklist?: string[];
    /**
     * only import events with property values on the whitelist
     */
    propValWhitelist?: string[];
    /**
     * don't import events with property values on the blacklist
     */
    propValBlacklist?: string[];
    /**
     * the start date of the export (events only)
     */
    start?: string;
    /**
     * the end date of the export (events only)
     */
    end?: string;
    /**
     * don't actually send the data to mixpanel, just transform it
     */
    dryRun?: boolean;
    /**
     * built in transform functions for various vendors
     */
    vendor?: Vendors;
    /**
     * options for built in transform functions
     */
    vendorOpts?: amplitudeOpts | heapOpts | ga4Opts | {};
    /**
     * whether or not to use http2; default `false` and http2 seems slower...
     */
    http2?: boolean;
    /**
     * whether or not to flatten the data; default `false`
     */
    flattenData?: boolean;
    /**
     * a tuple of column names to use as the insert_id; only set this if you want mixpanel-import to generate the insert_id for you
     * it will use mumurhash3
     */
    insertIdTuple?: string[];
    /**
     * attempt to parse values that poorly encoded json into valid json
     */
    fixJson?: boolean;
    /**
     * a cohort_id to use for people profile exports
     */
    cohortId?: string | number;
    /**
     * a data_group_id to use for exporting group profiles
     */
    dataGroupId?: string;
    /**
     * a list of properties to scrub from the data; this is useful for removing PII or other sensitive data
     * the properties will be deleted from the data before it is sent to mixpanel
     */
    scrubProps?: string[];

    /**
     * whether or not to write the transformed data to a file instead of sending it to mixpanel
     */
    writeToFile?: boolean;
    /**
     * the path to write the transformed data to
     */
    outputFilePath?: string;
  };

  /**
   * - a transform function to `map()` over the data
   * - if it returns `{}` the record will be skipped
   * - if it returns `[{},{},{}]` the record will be split into multiple records
   */
  type transFunc = (
    data: any
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
    errors: any[];
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
     * - estimation of consumption of mixpanel's event quota
     */
    percentQuota: number;
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
    dryRun: ArrayOfObjects;
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
   * amplitude transform opts
   */
  type amplitudeOpts = {
    user_id?: string;
    group_keys?: string[];
  };

  /**
   * amplitude transform opts
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
    source_info: ?boolean;
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
export = main;
