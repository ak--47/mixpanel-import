
/**
 * Mixpanel Importer
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * const mp = require('mixpanel-import')
 * const imported = await mp(creds, data, options)
 * @param {import('./index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('./index.d.ts').Data} data - data to import
 * @param {import('./index.d.ts').Options} [opts] - import options
 * @param {boolean} [isCLI] - `true` when run as CLI
 * @returns {Promise<import('./index.d.ts').ImportResults>} API receipts of imported data
 */
export function main(data: Data, creds: Creds, options?: Options, isCLI? : Boolean): Promise<ImportResults>;


/**
 * Mixpanel Importer Stream
 * stream `events`, `users`, `groups`, and `tables` to mixpanel!
 * @example
 * // pipe a stream to mixpanel
 * const { mpStream } = require('mixpanel-import')
 * const mpStream = createMpStream(creds, opts, callback);
 * const observer = new PassThrough({objectMode: true})
 * observer.on('data', (response)=> { })
 * // create a pipeline
 * myStream.pipe(mpStream).pipe(observer);
 * @param {import('./index.d.ts').Creds} creds - mixpanel project credentials
 * @param {import('./index.d.ts').Options} opts - import options
 * @param {function()} [finish] - callback @ end of pipeline
 * @returns a transform stream
 */
export function createMpStream(creds: Creds, opts: Options, finish?: Function): import("stream").Transform;

/**
 * valid records types which can be imported
 */
type RecordType = "event" | "user" | "group" | "table" | "export" | "peopleExport";

/**
 * - a path to a file/folder, objects in memory, or a readable object/file stream that contains data you wish to import
 */
export type Data = string | Array<mpEvent | mpUser | mpGroup> | import("fs").ReadStream;
/**
 * mixpanel project credentials for the import job
 */
export type Creds = {
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
};
/**
 * options for the import job
 */
export type Options = {
    /**
     * - type of record to import (`event`, `user`, `group`, or `table`)
     */
    recordType?: RecordType;
    /**
     * - US or EU (data residency)
     */
    region?: "US" | "EU";
    /**
     * - format of underlying data stream; json or jsonl
     */
    streamFormat?: "json" | "jsonl";
    /**
     * - use gzip compression (events only)
     */
    compress?: boolean;
    /**
     * - validate data on send (events only)
     */
    strict?: boolean;
    /**
     * - log results to `./logs/`
     */
    logs?: boolean;
    /**
     * - display verbose output messages
     */
    verbose?: boolean;
    /**
     * - apply various transformations to ensure data is properly ingested
     */
    fixData?: boolean;
    /**
     * - remove the following (keys and values) from each record with values = `null`, `''`, `undefined`, `{}`, or `[]`
     */
    removeNulls?: boolean;
    /**
     * - included only error responses; not successes
     */
    abridged?: boolean;
    /**
     * - don't buffer files into memory (even if they can fit)
     */
    forceStream?: boolean;
    /**
     * - 2^N; highWaterMark value for stream [DEPRECATED] ... use workers instead
     */
    streamSize?: number;
    /**
     * - UTC offset which will add/subtract hours to an event's `time` value; can be a positive or negative number; default `0`
     */
    timeOffset?: number;
    /**
     * - max # of records in each payload (max 2000; max 200 for group profiles)
     */
    recordsPerBatch?: number;
    /**
     * - max # of bytes in each payload (max 2MB)
     */
    bytesPerBatch?: number;
    /**
     * - maximum # of times to retry
     */
    maxRetries?: number;
    /**
     * - # of concurrent workers sending requests
     */
    workers?: number;
    /**
     * - where to put files
     */
    where?: string;
    /**
     * - a function to apply to every record before sending
     */
    transformFunc?: transFunc;
};
/**
 * a transform function to `map()` over the data
 */
type transFunc = (data: any) => mpEvent | mpUser | mpGroup;
/**
 * a summary of the import
 */
export type ImportResults = {
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
     * - num of batches
     */
    batches: number;
    /**
     * - num of requests
     */
    requests: number;
    /**
     * - estimate of "events per second" throughput
     */
    eps: number;
    /**
     * - estimate of "requests per second"
     */
    rps: number;
    /**
     * - successful import records (200s)
     */
    responses?: any[];
    /**
     * - failed import records (400s)
     */
    errors: any[];
};
/**
 * valid mixpanel property values; {@link https://help.mixpanel.com/hc/en-us/articles/115004547063-Properties-Supported-Data-Types more info}
 */
type PropValues = string | string[] | number | number[] | boolean | boolean[] | Date;
/**
 * mixpanel's required event properties
 */
type mpEvStandardProps = {
    /**
     * - uuid of the end user
     */
    distinct_id: string;
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
type ProfileOperation = "$set" | "$set_once" | "$add" | "$union" | "$append" | "$remove" | "$unset";
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
