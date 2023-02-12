/**
 * @namespace types
 */

/*
------------
MODULE STUFF
------------
*/

/**
 * valid records types which can be imported
 * @typedef {('event' | 'user' | 'group' | 'table' | 'export' | 'peopleExport')} RecordType
 */

/**
 * @typedef {import('fs').ReadStream} ReadableStream
 */

/**
 * valid data that can be passed into the module
 * @typedef {string | Array<mpEvent | mpUser | mpGroup> | ReadableStream } Data - a path to a file/folder, objects in memory, or a readable object/file stream that contains data you wish to import
 */


/**
 * mixpanel project credentials for the import job
 * @typedef {Object} Creds 
 * @property {string} acct - service account username
 * @property {string} pass - service account password
 * @property {(string | number)} project - project id
 * @property {string} [token] - project token (for importing user profiles)
 * @property {string} [lookupTableId] - lookup table ID (for importing lookup tables)
 * @property {string} [groupKey] - group identifier (for importing group profiles)
 * @property {string} secret - mixpanel project secret
 */

/**
 * options for the import job
 * @typedef {Object} Options
 * @property {RecordType} [recordType=event] - type of record to import (`event`, `user`, `group`, or `table`)
 * @property {('US' | 'EU')} [region=US] - US or EU (data residency)
 * @property {('json' | 'jsonl')} [streamFormat] - format of underlying data stream; json or jsonl
 * @property {boolean} [compress=false] - use gzip compression (events only)
 * @property {boolean} [strict=true] - validate data on send (events only)
 * @property {boolean} [logs=true] - log data to console
 * @property {boolean} [verbose=true] - display verbose output messages
 * @property {boolean} [fixData=false] - apply transformations to ensure data is properly ingested
 * @property {number} [streamSize=27] - 2^N; highWaterMark value for stream [DEPRECATED] ... use workers instead
 * @property {number} [recordsPerBatch=2000] - max # of records in each payload (max 2000; max 200 for group profiles) 
 * @property {number} [bytesPerBatch=2*1024*1024] - max # of bytes in each payload (max 2MB)
 * @property {number} [workers=10] - # of concurrent workers sending requests
 * @property {string} [where] - where to put files
 * @property {transFunc} [transformFunc=()=>{}] - a function to apply to every record before sending
 */

/**
 * a transform function to `map()` over the data
 * @callback transFunc
 * @param {Object} data - data to transform (`map()` style)
 * @returns {(mpEvent | mpUser | mpGroup)}
 */

/**
 * a summary of the import
 * @typedef {Object} ImportResults
 * @property {number} recordsProcessed - num records seen in pipeline
 * @property {number} success - num records successfully imported
 * @property {number} failed - num of records failed to import
 * @property {number} retries - num of request retries
 * @property {number} batches - num of batches
 * @property {number} requests - num of requests
 * @property {number} eps - estimate of "events per second" throughput
 * @property {number} rps - estimate of "requests per second"
 * @property {Array} responses - successful import records (200s)
 * @property {Array} errors - failed import records (400s)
 */



/*
---------------
MIXPANEL STUFF
---------------
*/

// PROFILES

/**
 * valid mixpanel property values; {@link https://help.mixpanel.com/hc/en-us/articles/115004547063-Properties-Supported-Data-Types more info}
 * @typedef { string | string[] | number | number[] | boolean | boolean[] | Date} PropValues
 */

/**
 * mixpanel's required event properties
 * @typedef {Object} mpEvStandardProps
 * @property {string} distinct_id - uuid of the end user
 * @property {number} time - the UTC time of the event (unix epoch)
 * @property {string} [$insert_id] - unique row id; used for deduplication
 */

/**
 * event properties payload
 * @typedef {Object<string, PropValues> & mpEvStandardProps} mpEvProperties
 */

/**
 * @typedef {Object} mpEvent - a mixpanel event
 * @property {string} event - the event name
 * @property {mpEvProperties} properties - the event's properties
 */

// PROFILES

/**
 * valid profile update types; {@link https://developer.mixpanel.com/reference/profile-set more info}
 * @typedef {'$set' | '$set_once' | '$add' | '$union' | '$append' | '$remove' | '$unset' } ProfileOperation
 * 
 */

/**
 * object of k:v pairs to update the profile
 * @typedef {Partial<Record<ProfileOperation, Object<string, PropValues>>>} ProfileData
 * 
 */

/**
 * @typedef {Object} mpUserStandardProps
 * @property {string} $distinct_id - the `distinct_id` of the profile to update
 * @property {string} $token - the mixpanel project identifier
 * @property {string | number} [$ip] - the IP of the end user (used for geo resolution) or `0` to turn off
 * @property {boolean} [$ignore_time] - whether or not to update `$last_seen`; default `true`
 */

/**
 * @typedef {Object} mpGroupStandardProps - a mixpanel user profile
 * @property {string} $group_key - the group (analytics) key for the entity
 * @property {string} $group_id - the uuid of the group; like `$distinct_id` for user profiles
 * @property {string} $token - the mixpanel project identifier
 */

/**
 * a group profile update payload
 * @typedef {mpGroupStandardProps & ProfileData} mpGroup
 */

/**
 * a user profile update payload
 * @typedef {mpUserStandardProps & ProfileData} mpUser
 */



exports.unused = {};