/**
 * @namespace types
 */

/**
 * @typedef {Object} Creds - mixpanel project credentials
 * @property {string} acct - service account username
 * @property {string} pass - service account password
 * @property {(string | number)} project - project id
 * @property {string} [token] - project token (for importing user profiles)
 * @property {string} [lookupTableId] - lookup table ID (for importing lookup tables)
 * @property {string} [groupKey] - group identifier (for importing group profiles)
 */

/**
 * @typedef {Object} Options - import options
 * @property {('event' | 'user' | 'group' | 'table' | 'export' | 'peopleExport')} [recordType=event] - type of record to import (`event`, `user`, `group`, or `table`)
 * @property {('US' | 'EU')} [region=US] - US or EU (data residency)
 * @property {('json' | 'jsonl')} [streamFormat] - format of underlying data stream; json or jsonl
 * @property {boolean} [compress=false] - use gzip compression (events only)
 * @property {boolean} [strict=true] - validate data on send (events only)
 * @property {boolean} [logs=true] - log data to console
 * @property {boolean} [fixData=false] - apply transformations to ensure data is properly ingested
 * @property {number} [streamSize=27] - 2^N; highWaterMark value for stream [DEPRECATED] ... use workers instead
 * @property {number} [recordsPerBatch=2000] - max # of records in each payload (max 2000; max 200 for group profiles) 
 * @property {number} [bytesPerBatch=2*1024*1024] - max # of bytes in each payload (max 2MB)
 * @property {number} [workers=10] - # of concurrent workers sending requests
 * @property {string} [where] - where to put files
 * @property {transFunc} [transformFunc=()=>{}] - a function to apply to every record before sending
 */

/**
 * @callback transFunc
 * @param {Object} data - data to transform (`map()` style)
 * @returns {(mpEvent | mpUser | mpGroup)}
 */

/**
 * @typedef {string | Array<mpEvent | mpUser | mpGroup> | ReadableStream } Data - a path to a file/folder, objects in memory, or a readable object/file stream that contains data you wish to import
 */

/**
 * @typedef {Object} mpEvent - a mixpanel event
 * @property {string} event - the event name
 * @property {mpProperties} properties - the event's properties
 */

/**
 * @typedef {Object} mpProperties - mixpanel event properties
 * @property {string} distinct_id - uuid of the end user
 * @property {string} time - the UTC time of the event
 * @property {string} $insert_id - 
 */

/**
 * @typedef {Object} mpUser - a mixpanel user profile
 * @property {string} $token - the project token
 * @property {string} $distinct_id - the uuid of the user
 * @property {profileDirective} - a `$set` style operation
 */

/**
 * @typedef {Object} mpGroup - a mixpanel user profile
 * @property {string} $token - the project token
 * @property {string} $group_key - the group (analytics) key for the entity
 * @property {string} $group_id - the uuid of the group
 * @property {profileDirective} - a `$set` style operation
 */

/**
 * @typedef {Object} profileDirective
 * @property {Object} [$set]
 * @property {Object} [$set_once]
 * @property {Object} [$add]
 * @property {Object} [$union]
 * @property {Object} [$append]
 * @property {Object} [$remove]
 * @property {Object} [$unset]
 */


/**
 * @typedef {import('fs').ReadStream} ReadableStream
 */

/**
 * @typedef {Object} ImportResults
 * @property {number} recordsProcessed
 * @property {number} success
 * @property {number} failed
 * @property {number} retries
 * @property {number} batches
 * @property {number} requests
 * @property {Array} responses
 * @property {Array} errors
 */

exports.unused = {};