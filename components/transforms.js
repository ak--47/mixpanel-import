const murmurhash = require("murmurhash");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const u = require("ak-tools");
const stringify = require("json-stable-stringify");
const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];
// ? https://docs.mixpanel.com/docs/data-structure/user-profiles#reserved-profile-properties
const specialProps = ["name", "first_name", "last_name", "email", "phone", "avatar", "created", "insert_id", "city", "region", "lib_version", "os", "os_version", "browser", "browser_version", "app_build_number", "app_version_string", "device", "screen_height", "screen_width", "screen_dpi", "current_url", "initial_referrer", "initial_referring_domain", "referrer", "referring_domain", "search_engine", "manufacturer", "brand", "model", "watch_model", "carrier", "radio", "wifi", "bluetooth_enabled", "bluetooth_version", "has_nfc", "has_telephone", "google_play_services", "duration", "country", "country_code"];
const outsideProps = ["distinct_id", "group_id", "token", "group_key", "ip"]; //these are the props that are outside of the $set
const badUserIds = ["-1", "0", "00000000-0000-0000-0000-000000000000", "<nil>", "[]", "anon", "anonymous", "false", "lmy47d", "n/a", "na", "nil", "none", "null", "true", "undefined", "unknown", "{}", null, undefined]
const MAX_STR_LEN = 255;

/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */
/** @typedef {import('../index').Options} Options */
/** @typedef {import('../index').Creds} Creds */
/** @typedef {import('../index').ImportResults} ImportResults */


// a noop function
function noop(a) { return a; }

/**
 * Truncate a string to MAX_STR_LEN characters.
 * @param {string} s
 * @returns {string}
 */
function truncate(s) {
	return s.length > MAX_STR_LEN ? s.slice(0, MAX_STR_LEN) : s;
}

/**
 * @param  {JobConfig} job
 */
function ezTransforms(job) {
	// EVENT RECORDS
	if (job.recordType?.startsWith("event") || job.recordType === "export-import-event") {
		return function transformEvent(record) {
			// 1. Fix “wrong shape”: ensure record.properties exists
			if (!record.properties) {
				record.properties = { ...record };
				for (const key of Object.keys(record)) {
					if (key !== "properties" && key !== "event") {
						delete record[key];
					}
				}
			}

			// 2. Normalize time to UNIX epoch (ms)
			if (
				record.properties.time &&
				Number.isNaN(Number(record.properties.time))
			) {
				record.properties.time = dayjs.utc(record.properties.time).valueOf();
			}

			// 3. Add $insert_id if missing
			if (!record.properties.$insert_id) {
				try {
					const tuple = [
						record.event,
						record.properties.distinct_id || "",
						record.properties.time,
					].join("-");
					record.properties.$insert_id = murmurhash.v3(tuple).toString();
				} catch {
					record.properties.$insert_id = String(record.properties.distinct_id);
				}
			}

			// 4. Rename well-known keys to Mixpanel’s $-prefixed versions
			["user_id", "device_id", "source"].forEach((orig) => {
				if (record.properties[orig]) {
					record.properties[`$${orig}`] = record.properties[orig];
					delete record.properties[orig];
				}
			});

			// 5. Promote “special” props
			for (const key of Object.keys(record.properties)) {
				if (specialProps.includes(key)) {
					if (key === "country") {
						record.properties.mp_country_code = record.properties[key];
					} else {
						record.properties[`$${key}`] = record.properties[key];
					}
					delete record.properties[key];
				}
			}

			// 6. Ensure distinct_id, $user_id, $device_id are strings
			["distinct_id", "$user_id", "$device_id"].forEach((k) => {
				if (record.properties[k] != null) {
					record.properties[k] = String(record.properties[k]);
				}
			});

			// 6a. Remove bad distinct_ids
			["distinct_id", "$user_id", "$device_id"].forEach((k) => {
				if (badUserIds.includes(record.properties[k])) {
					delete record.properties[k];
				}
			});

			// 7. Truncate all string property values
			for (const [k, v] of Object.entries(record.properties)) {
				if (typeof v === "string") {
					record.properties[k] = truncate(v);
				}
			}

			return record;
		};
	}

	// USER PROFILE RECORDS
	if (job.recordType?.startsWith("user") || (job.recordType === "export-import-profile" && !job.groupKey)) {
		return function transformUser(user) {
			// Determine the directive to use
			const directive = (job.directive && validOperations.includes(job.directive)) ? job.directive : null;

			// 1. Fix "wrong shape" into {$directive: {...}} or default to {$set: {...}}
			// Skip if record already has correct vendor-transform structure
			// (has a valid operation AND has $distinct_id at root - means vendor already processed it)
			const hasVendorTransformStructure = validOperations.some((op) => op in user) &&
				('$distinct_id' in user || 'distinct_id' in user);

			// Only apply fix if: record doesn't have vendor structure AND (directive specified OR no valid operation)
			if (!hasVendorTransformStructure && (directive || !validOperations.some((op) => op in user))) {
				const uuidKey = user.$distinct_id
					? "$distinct_id"
					: user.distinct_id
						? "distinct_id"
						: null;
				if (!uuidKey) return {}; // skip if no distinct_id
				//!important store uuid value
				const uuidValue = String(user[uuidKey]);

				// Collect all properties from the user object (including any existing operations)
				const base = { ...user };
				// Remove all existing operation buckets if directive is specified
				if (directive) {
					for (const op of validOperations) {
						if (base[op]) {
							// Merge properties from existing operations into base
							Object.assign(base, base[op]);
							delete base[op];
						}
					}
				}

				// Use the specified directive if provided, otherwise default to $set
				const finalDirective = directive || '$set';

				// For $unset, we need an array of property names, not an object
				if (finalDirective === '$unset') {
					const propsToUnset = [];
					for (const key of Object.keys(base)) {
						if (key !== uuidKey && key !== '$token' && !key.startsWith('$')) {
							propsToUnset.push(key);
						}
					}
					user = { [finalDirective]: propsToUnset };
				} else {
					user = { [finalDirective]: base };
					delete user[finalDirective][uuidKey];
					delete user[finalDirective].$token;
				}
				//!important: set $distinct_id
				user.$distinct_id = uuidValue;

				// Handle Mixpanel-export shape (only for non-$unset operations):
				if (finalDirective !== '$unset' && typeof user[finalDirective].$properties === "object") {
					user[finalDirective] = { ...user[finalDirective].$properties };
					delete user[finalDirective].$properties;
				}
			}

			// 2. Ensure $token is present
			if (!user.$token && job.token) {
				user.$token = job.token;
			}

			// 3. Rename specialProps inside each operation bucket
			for (const op of validOperations) {
				if (typeof user[op] === "object") {
					for (const prop of Object.keys(user[op])) {
						if (specialProps.includes(prop)) {
							if (prop === "country" || prop === "country_code") {
								user[op].$country_code = user[op][prop].toUpperCase();
							} else {
								user[op][`$${prop}`] = user[op][prop];
							}
							delete user[op][prop];
						}
					}
				}
			}

			// 4. First extract outsideProps from every operation bucket up to the root
			for (const op of validOperations) {
				if (typeof user[op] === 'object') {
					for (const prop of outsideProps) {
						if (prop in user[op]) {
							user[`$${prop}`] = user[op][prop];
							delete user[op][prop];
						}
					}
				}
			}

			// 5. Now pull any remaining outsideProps at the root, and truncate strings
			for (const [key, val] of Object.entries(user)) {
				if (outsideProps.includes(key)) {
					user[`$${key}`] = val;
					delete user[key];
				} else if (typeof val === "string") {
					user[key] = truncate(val);
				}
			}

			return user;
		};
	}

	// GROUP PROFILE RECORDS
	// @ts-ignore
	if (job.recordType?.startsWith("group") || (job.recordType === "export-import-profile" && job.groupKey)) {
		return function transformGroup(group) {
			// Determine the directive to use
			const directive = (job.directive && validOperations.includes(job.directive)) ? job.directive : null;

			// 1. Fix "wrong shape" into {$directive: {...}} or default to {$set: {...}}
			// If directive is specified, always use it (even if other operations exist)
			if (directive || !validOperations.some((op) => op in group)) {
				// fallback chain for uuidKey
				const uuidKey =
					(group?.[job?.groupKey] && job.groupKey) ||
					(group?.$group_id && '$group_id') ||
					(group?.group_id && 'group_id') ||
					(group?.$distinct_id && '$distinct_id') ||
					(group?.distinct_id && 'distinct_id') ||
					null;
				if (!uuidKey) return {}; // skip if no group_id

				const uuidValue = String(group[uuidKey]);

				// Collect all properties from the group object (including any existing operations)
				const base = { ...group };
				// Remove all existing operation buckets if directive is specified
				if (directive) {
					for (const op of validOperations) {
						if (base[op]) {
							// Merge properties from existing operations into base
							Object.assign(base, base[op]);
							delete base[op];
						}
					}
				}

				// Use the specified directive if provided, otherwise default to $set
				const finalDirective = directive || '$set';

				// For $unset, we need an array of property names, not an object
				if (finalDirective === '$unset') {
					const propsToUnset = [];
					for (const key of Object.keys(base)) {
						if (key !== uuidKey && key !== '$group_id' && key !== '$token' && key !== '$group_key' && !key.startsWith('$')) {
							propsToUnset.push(key);
						}
					}
					group = { [finalDirective]: propsToUnset };
				} else {
					group = { [finalDirective]: base };
					delete group[finalDirective][uuidKey];
					delete group[finalDirective].$group_id;
					delete group[finalDirective].$token;
				}

				group.$group_id = uuidValue;
			}

			// 2. Ensure $token and $group_key are present
			if (!group.$token && job.token) group.$token = job.token;
			if (!group.$group_key && job.groupKey) group.$group_key = job.groupKey;

			// 3. Rename specialProps inside each operation bucket
			for (const op of validOperations) {
				if (typeof group[op] === "object") {
					for (const prop of Object.keys(group[op])) {
						if (specialProps.includes(prop)) {
							group[op][`$${prop}`] = group[op][prop];
							delete group[op][prop];
						}
					}
				}
			}

			// 4. First extract outsideProps from every operation bucket up to the root
			for (const op of validOperations) {
				if (typeof group[op] === 'object') {
					for (const prop of outsideProps) {
						if (prop in group[op]) {
							group[`$${prop}`] = group[op][prop];
							delete group[op][prop];
						}
					}
				}
			}

			// 5. Now pull any remaining outsideProps up to the root, and truncate strings
			for (const [key, val] of Object.entries(group)) {
				if (outsideProps.includes(key)) {
					group[`$${key}`] = val;
					delete group[key];
				} else if (typeof val === 'string') {
					group[key] = truncate(val);
				}
			}

			return group;
		};
	}

	// NO-OP for all other record types
	return noop;
}


//flattener
function flattenProperties(sep = ".") {
	function flatPropertiesRecurse(properties, roots = []) {
		return Object.keys(properties)
			.reduce((memo, prop) => {
				// Check if the property is an object but not an array
				const isObjectNotArray = properties[prop] !== null
					&& typeof properties[prop] === 'object'
					&& !Array.isArray(properties[prop]);

				return Object.assign({}, memo,
					isObjectNotArray
						? flatPropertiesRecurse(properties[prop], roots.concat([prop]))
						: { [roots.concat([prop]).join(sep)]: properties[prop] }
				);
			}, {});
	}

	return function (record) {
		if (record.properties && typeof record.properties === 'object') {
			record.properties = flatPropertiesRecurse(record.properties);
			return record;
		}

		if (record.$set && typeof record.$set === 'object') {
			record.$set = flatPropertiesRecurse(record.$set);
			return record;

		}

		return {};


	};
}


// side-effects; for efficiency
// removes: null, '', undefined, {}, []
function removeNulls(valuesToRemove = [null, "", undefined]) {
	return function (record) {
		const keysToEnum = ["properties", ...validOperations];
		for (const recordKey of keysToEnum) {
			for (const badVal of valuesToRemove) {
				if (record?.[recordKey]) {
					for (const p in record[recordKey]) {
						if (record[recordKey][p] === badVal) {
							delete record[recordKey][p];
						}
						//test for {}
						try {
							if (typeof record[recordKey][p] === "object") {
								if (Object.keys(record[recordKey][p]).length === 0) {
									delete record[recordKey][p];
								}
							}
						} catch (e) {
							//noop
						}
					}
				}
			}
		}
		return record;
	};
}

//add tags to every record
/**
 * @param  {JobConfig} jobConfig
 */
function addTags(jobConfig) {
	const type = jobConfig.recordType;
	const tags = jobConfig.tags || {};
	return function (record) {
		if (!Object.keys(tags).length) return record;
		if (type === "event") {
			if (record.properties) record.properties = { ...record.properties, ...tags };
			return record;
		}

		const operation = Object.keys(record).find(predicate => predicate.startsWith("$") && validOperations.includes(predicate));

		if (type === "user" || type === "group") {
			if (operation) record[operation] = { ...record[operation], ...tags };
			return record;
		}

		return record;
	};
}


/**
 * rename property keys
 * @param  {JobConfig} jobConfig
 */
function applyAliases(jobConfig) {
	const type = jobConfig.recordType;
	const aliases = jobConfig.aliases || {};
	return function (record) {
		if (!Object.keys(aliases).length) return record;
		if (type === "event") {
			if (record.properties) {
				record.properties = u.rnKeys(record.properties, aliases);
			} else {
				record = u.rnKeys(record, aliases);
			}
			return record;
		}
		const operation = Object.keys(record).find(predicate => predicate.startsWith("$") && validOperations.includes(predicate));

		if (type === "user" || type === "group") {
			if (operation) {
				record[operation] = u.rnKeys(record[operation], aliases);
				return record;
			} else {
				record = u.rnKeys(record, aliases);
				return record;
			}
		}

		return record;
	};
}

/**
 * adds mixpanel token
 * @param  {JobConfig} jobConfig
 */
function addToken(jobConfig) {
	const type = jobConfig.recordType;
	const token = jobConfig.token;
	return function (record) {
		if (type === "event") {
			if (record.properties) record.properties.token = token;
			else {
				record.properties = { token };
			}
			return record;
		}
		if (type === "user" || type === "group") {
			if (record) record.$token = token;
			return record;
		}
		return record;
	};
}

/**
 * set distinct_id from $user_id or $device_id (for v2 compatibility)
 * only applies to events, only sets if distinct_id doesn't exist
 * prefers $user_id, falls back to $device_id
 * @returns {function}
 */
function setDistinctIdFromV2Props() {
	return function (record) {
		// Only for events with properties
		if (record?.properties) {
			// Only set if distinct_id doesn't exist
			if (!record.properties.distinct_id) {
				// Prefer $user_id, fallback to $device_id
				if (record.properties.$user_id) {
					record.properties.distinct_id = record.properties.$user_id;
				} else if (record.properties.$device_id) {
					record.properties.distinct_id = record.properties.$device_id;
				}
			}
		}
		return record;
	};
}

/**
 * offset the time of events by an integer number of hours
 * @param  {number} timeOffset=0
 */
function UTCoffset(timeOffset = 0) {
	return function (record) {
		if (record?.properties?.time) {
			const oldTime = dayjs.unix(record.properties.time);
			const newTime = oldTime.add(timeOffset, "h").valueOf();
			record.properties.time = newTime;
		}
		return record;
	};
}

/**
 * this will dedupe records based on their (murmur v3) hash
 * records with the same hash will be filtered out
 * @param  {JobConfig} jobConfig
 */
function dedupeRecords(jobConfig) {
	const hashTable = jobConfig.hashTable;
	return function (record) {
		//JSON stable stringification
		const hash = murmurhash.v3(stringify(record));
		if (hashTable.has(hash)) {
			jobConfig.duplicates++;
			return {};
		} else {
			hashTable.add(hash);
			return record;
		}
	};
}

/**
 * this function is used to whitelist or blacklist events, prop keys, or prop values
 * @param  {JobConfig} jobConfig
 * @param  {import('../index').WhiteAndBlackListParams} params
 */
function whiteAndBlackLister(jobConfig, params) {
	const {
		eventWhitelist = [],
		eventBlacklist = [],
		propKeyWhitelist = [],
		propKeyBlacklist = [],
		propValBlacklist = [],
		propValWhitelist = [],
		comboWhiteList = {},
		comboBlackList = {}
	} = params;

	return function whiteAndBlackList(record) {
		let pass;
		//check for event whitelist
		if (eventWhitelist.length) {
			if (!eventWhitelist.includes(record?.event)) {
				jobConfig.whiteListSkipped++;
				return {};
			}
		}

		//check for event blacklist
		if (eventBlacklist.length) {
			if (eventBlacklist.includes(record?.event)) {
				jobConfig.blackListSkipped++;
				return {};
			}
		}

		//check for prop key whitelist
		if (propKeyWhitelist.length) {
			pass = false;
			for (const key in record?.properties) {
				if (propKeyWhitelist.includes(key)) {
					pass = true;
				}
			}
			if (!pass) {
				jobConfig.whiteListSkipped++;
				return {};
			}
		}

		//check for prop key blacklist
		if (propKeyBlacklist.length) {
			pass = true;
			for (const key in record?.properties) {
				if (propKeyBlacklist.includes(key)) {
					jobConfig.blackListSkipped++;
					pass = false;
				}
			}
			if (!pass) return {};
		}

		//check for prop val whitelist
		if (propValWhitelist.length) {
			pass = false;
			for (const key in record?.properties) {
				if (propValWhitelist.includes(record.properties[key])) {

					pass = true;
				}
			}
			if (!pass) {
				jobConfig.whiteListSkipped++;
				return {};
			}
		}

		//check for prop val blacklist
		if (propValBlacklist.length) {
			pass = true;
			for (const key in record?.properties) {
				if (propValBlacklist.includes(record.properties[key])) {
					jobConfig.blackListSkipped++;
					pass = false;
				}
			}
			if (!pass) return {};
		}

		//check for combo whitelist		
		if (Object.keys(comboWhiteList).length) {
			let pass = false; // Assume the record does not pass initially
			for (const key in comboWhiteList) {
				let propVals = Array.isArray(comboWhiteList[key]) ? comboWhiteList[key] : [comboWhiteList[key]];
				if (propVals.map(v => v?.toString()).includes(record?.properties?.[key]?.toString())) {
					pass = true; // If any key-value matches, set pass to true
					break; // Break early since at least one condition is satisfied
				}
			}
			if (!pass) { // If after checking all, no match was found
				jobConfig.whiteListSkipped++;
				return {}; // Skip the record
			}
		}

		//check for combo blacklist
		if (Object.keys(comboBlackList).length) {
			pass = true;
			for (const key in comboBlackList) {
				let propVals = comboBlackList[key];
				if (!Array.isArray(propVals)) propVals = [propVals];
				let foundMatch = false;
				for (const val of propVals) {
					if (record?.properties?.[key]?.toString() === val?.toString()) foundMatch = true;
				}
				if (foundMatch) {
					pass = false;
					break;
				}
			}
			if (!pass) {
				jobConfig.blackListSkipped++;
				return {};
			}
		}

		return record;
	};
}


/**
 * this function is used to whitelist or blacklist events, prop keys, or prop values
 * @param  {JobConfig} jobConfig
 */
function epochFilter(jobConfig) {
	const epochStart = dayjs.unix(jobConfig.epochStart).utc();
	const epochEnd = dayjs.unix(jobConfig.epochEnd).utc();
	return function filterEventsOnTime(record) {
		if (jobConfig.recordType === 'event') {
			if (record?.properties?.time) {
				let eventTime = record.properties.time;
				if (eventTime.toString().length === 10) eventTime = eventTime * 1000;
				eventTime = dayjs.utc(eventTime);
				if (eventTime.isBefore(epochStart)) {
					jobConfig.outOfBounds++;
					return null;
				}
				else if (eventTime.isAfter(epochEnd)) {
					jobConfig.outOfBounds++;
					return null;
				}
			}
		}
		return record;
	};
}

/**
 * this function is used to see if a record is empty (basically `{}`, `[]`, or `null` or `undefined`)
 * @param  {any} data
 * @returns {boolean}
 */
function isNotEmpty(data) {
	if (!data) return false;
	if (typeof data !== "object") return false;
	if (Array.isArray(data)) {
		if (data.length === 0) return false;
	}
	if (Object.keys(data).length === 0) return false;
	return true;
}

function fixTime() {
	return function (record) {
		if (record?.properties) {

			if (record?.properties?.time) {
				if (record.properties.time && Number.isNaN(Number(record.properties.time))) {
					record.properties.time = dayjs.utc(record.properties.time).valueOf();
				}
			}

			else {
				record.properties.time = dayjs.utc().valueOf();
			}

		}
		else {
			throw new Error("Record has no properties object, cannot fix time");
		}

		return record;
	};
}

/**
 * this function is used to add an insert_id to every record based on a tuple of keys OR murmurhash the whole record
 * @param  {string[]} insert_tuple
 */
function addInsert(insert_tuple = []) {
	return function (record) {
		//empty record
		if (!Object.keys(record)) return {};
		if (insert_tuple.length === 0) return record;
		// don't overwrite existing insert_id
		if (record.properties.$insert_id) return record;
		const actual_tuple = [];
		for (const key of insert_tuple) {
			if (record[key]) actual_tuple.push(record[key]);
			if (record?.properties?.[key]) actual_tuple.push(record.properties[key]);
		}
		if (actual_tuple.length === insert_tuple.length) {
			const insert_id = murmurhash.v3(actual_tuple.join("-")).toString();
			record.properties.$insert_id = insert_id;
		}
		// if the tuple can't be found, just hash the whole record
		else {
			const hash = murmurhash.v3(stringify(record)).toString();
			record.properties.$insert_id = hash;
		}

		return record;
	};
}

function fixJson() {
	return function (record) {
		try {
			if (record.properties) {
				for (const key in record.properties) {
					if (mightBeJson(record.properties[key])) {

						//CASE: JSON, just stringified
						try {
							const attempt = JSON.parse(record.properties[key]);
							if (typeof attempt === "string") throw "failed";
							record.properties[key] = attempt;

						} catch (e) {
							//CASE 2: JSON escaped
							try {
								const attempt = JSON.parse(record.properties[key].replace(/\\\\/g, '\\'));
								if (typeof attempt === "string") throw "failed";
								record.properties[key] = attempt;
							}
							catch (e) {
								//CASE 3: Double Stringified JSON
								const attempt = JSON.parse(JSON.parse(record.properties[key]));
								if (typeof attempt === "string") throw "failed";
								record.properties[key] = attempt;

								// ok... we couldn't figure it out...early return
								return record;
							}
						}
					}
				}
			}
			return record;
		}
		catch (e) {
			return record;
		}
	};
}

/**
 * Resolves the first non-empty value for the provided keys from the data object; recursively searches through objects and arrays.
 * @param {object} data - The data object to search
 * @param {string[]} keys - The keys to search in the data object
 * @returns {string | null} - The first non-empty value found, or null if none found
 */
function resolveFallback(data, keys) {
	if (!data || keys.length === 0) return null;

	for (const key of keys) {
		if (data?.hasOwnProperty(key)) {
			const value = data[key];

			// Check if the value is not undefined, not null, and not an empty string
			if (value !== undefined && value !== null && value !== '') {
				if (Array.isArray(value) && value.length === 0) return null;
				if (typeof value === 'object' && Object.keys(value).length === 0) return null;
				return value.toString();
			}
		}

		// If the current key doesn't lead to a valid value, check if data is an object
		// and recursively call the function for nested objects.
		if (typeof data === 'object') {
			for (const nestedKey in data) {
				if (typeof data[nestedKey] === 'object') {
					const result = resolveFallback(data[nestedKey], [key]);
					if (result !== null) {
						return result;
					}
				}
			}
		}
	}

	return null;
}

/**
 * delete properties from an object; useful for removing PII or redacting sensitive or duplicate data
 * @param  {string[]} keysToScrub
 */
function scrubProperties(keysToScrub = []) {
	return function recursiveScrub(data) {
		if (!data || keysToScrub.length === 0) return data;
		scrubber(data, keysToScrub);
		return data;
	};
}

/**
 * remove specific columns/properties from records in the pipeline
 * @param  {string[]} columnsToDrop - array of property keys to remove
 */
function dropColumns(columnsToDrop = []) {
	return function dropColumnTransform(record) {
		if (!record || columnsToDrop.length === 0) return record;
		
		// Handle event records - remove from properties
		if (record.properties && typeof record.properties === 'object') {
			for (const key of columnsToDrop) {
				delete record.properties[key];
			}
		}
		
		// Handle profile records - remove from operation buckets ($set, $add, etc.)
		for (const op of validOperations) {
			if (record[op] && typeof record[op] === 'object') {
				for (const key of columnsToDrop) {
					delete record[op][key];
				}
			}
		}
		
		// Also remove from root level (for malformed records or direct properties)
		for (const key of columnsToDrop) {
			if (key !== 'event' && key !== 'properties' && !key.startsWith('$')) {
				delete record[key];
			}
		}
		
		return record;
	};
}


// performance optimized recursive function to scrub properties from an object
// https://chat.openai.com/share/98f40372-2a3a-413c-a42c-c8cf28f6d074
// MUTATES THE OBJECT
function scrubber(obj, keysToScrub) {
	try {
		if (Array.isArray(obj)) {
			obj.forEach(element => scrubber(element, keysToScrub));
		} else if (obj !== null && typeof obj === 'object') {
			for (const key of keysToScrub) {
				try {
					if (obj.hasOwnProperty(key)) {
						delete obj[key];
					}
				}
				catch (e) {
					// noop
				}
			}
			for (const key in obj) {
				scrubber(obj[key], keysToScrub);
			}
		}
	}
	catch (e) {
		// noop
	}
}


/**
 * Quickly determine if a string might be JSON.
 * @param {string} input - The string to check.
 * @returns {boolean} - True if the string might be JSON, otherwise false.
 */
function mightBeJson(input) {
	try {
		if (typeof input !== 'string') return false;
		const isItJson =
			(input.startsWith(`{`) && input.endsWith(`}`)) ||
			(input.startsWith(`"{`) && input.endsWith(`}"`)) ||
			(input.startsWith(`'{`) && input.endsWith(`}'`)) ||
			(input.startsWith(`\\"{`) && input.endsWith(`}'\\`)) ||
			(input.startsWith(`\\'{`) && input.endsWith(`}'\\`)) ||
			(input.startsWith(`[`) && input.endsWith(`]`)) ||
			(input.startsWith(`"[`) && input.endsWith(`]"`)) ||
			(input.startsWith(`'[`) && input.endsWith(`]'`)) ||
			(input.startsWith(`\\"[`) && input.endsWith(`]"\\`)) ||
			(input.startsWith(`\\'[`) && input.endsWith(`]'\\`));

		return isItJson;
	}
	catch (e) {
		return false;
	}
}



/**
 * rename property keys
 * @param  {JobConfig} job
 */
function scdTransform(job) {
	const { groupKey, dataGroupId, scdLabel, scdKey } = job;
	return function (record) {
		const mpSCDEvent = {
			"event": scdLabel,
			"properties": {
				"$mp_updated_at": record?.insertTime || record?.insert_time || new Date().toISOString(),
				"time": record?.startTime || record?.start_time || record?.timestamp || new Date().toISOString(),
			}
		};

		const value = record[scdKey] || record[scdLabel];
		if (!value) return {};

		mpSCDEvent.properties[scdKey] = value;

		if (dataGroupId || groupKey) {
			mpSCDEvent.properties[groupKey] = record?.[groupKey] || record?.["distinct_id"] || record?.["$distinct_id"];
		}

		if (!dataGroupId || !groupKey) {
			mpSCDEvent.properties["distinct_id"] = record?.distinct_id || record?.user_id || record?.device_id;
		}

		if (typeof mpSCDEvent.properties.time !== "number") {
			mpSCDEvent.properties.time = dayjs.utc(mpSCDEvent.properties.time).valueOf();
		}

		return mpSCDEvent;

	};
}


module.exports = {
	ezTransforms,
	removeNulls,
	UTCoffset,
	addTags,
	applyAliases,
	dedupeRecords,
	whiteAndBlackLister,
	epochFilter,
	isNotEmpty,
	flattenProperties,
	addInsert,
	fixJson,
	resolveFallback,
	scrubProperties,
	dropColumns,
	addToken,
	setDistinctIdFromV2Props,
	scdTransform,
	fixTime
};


/**
 * @param  {JobConfig} job
 */
// eslint-disable-next-line no-unused-vars
function ezTransformsOLD(job) {
	if (job.recordType === `event` || job.recordType === 'export-import-event') {
		return function FixShapeAndAddInsertIfAbsentAndFixTime(record) {
			//wrong shape
			if (!record.properties) {
				record.properties = { ...record };
				//delete properties outside properties
				for (const key in record) {
					if (key !== "properties" && key !== "event") delete record[key];
				}
				delete record.properties.event;
			}

			//fixing time
			if (record.properties.time && Number.isNaN(Number(record.properties.time))) {
				record.properties.time = dayjs.utc(record.properties.time).valueOf();
			}
			//adding insert_id
			if (!record?.properties?.$insert_id) {
				try {
					const deDupeTuple = [record.event, record.properties.distinct_id || "", record.properties.time];
					const hash = murmurhash.v3(deDupeTuple.join("-")).toString();
					record.properties.$insert_id = hash;
				} catch (e) {
					record.properties.$insert_id = record.properties.distinct_id;
				}
			}

			//renaming "user_id" to "$user_id"
			if (record.properties.user_id) {
				record.properties.$user_id = record.properties.user_id;
				delete record.properties.user_id;
			}

			//renaming "device_id" to "$device_id"
			if (record.properties.device_id) {
				record.properties.$device_id = record.properties.device_id;
				delete record.properties.device_id;
			}

			//renaming "source" to "$source"
			if (record.properties.source) {
				record.properties.$source = record.properties.source;
				delete record.properties.source;
			}

			for (const key in record.properties) {
				if (specialProps.includes(key)) {
					if (key === "country") {
						record.properties[`mp_country_code`] = record.properties[key];
						delete record.properties[key];
					}
					else {
						record.properties[`$${key}`] = record.properties[key];
						delete record.properties[key];
					}
				}



			}

			//make sure id is a string
			if (record.properties.distinct_id) {
				record.properties.distinct_id = record.properties.distinct_id.toString();
			}
			if (record.properties.$user_id) {
				record.properties.$user_id = record.properties.$user_id.toString();
			}
			if (record.properties.$device_id) {
				record.properties.$device_id = record.properties.$device_id.toString();
			}

			return record;
		};
	}

	//for user imports, make sure every record has a $token and the right shape
	if (job.recordType === `user` || job.recordType === 'export-import-profile') {
		return function addUserTokenIfAbsent(user) {

			//wrong shape; fix it
			if (!validOperations.some(op => Object.keys(user).includes(op))) {
				let uuidKey;
				if (user.$distinct_id) uuidKey = "$distinct_id";
				else if (user.distinct_id) uuidKey = "distinct_id";
				else {
					if (job.verbose) console.log(`user record has no uuid:\n${JSON.stringify(user)}\n skipping record`);
					return {};
				}
				user = { $set: { ...user } };
				user.$distinct_id = user.$set[uuidKey];
				delete user.$set[uuidKey];
				delete user.$set.$token;

				//deal with mp export shape
				//? https://developer.mixpanel.com/reference/engage-query
				if (typeof user.$set?.$properties === "object") {
					user.$set = { ...user.$set.$properties };
					delete user.$set.$properties;
				}
			}

			//catch missing token
			if (!user.$token && job.token) user.$token = job.token;

			//rename special props
			for (const key in user) {
				if (typeof user[key] === "object") {
					for (const prop in user[key]) {
						if (specialProps.includes(prop)) {
							if (prop === "country" || prop === "country_code") {
								user[key][`$country_code`] = user[key][prop].toUpperCase();
								delete user[key][prop];
							}
							else {
								user[key][`$${prop}`] = user[key][prop];
								delete user[key][prop];
							}
						}

						if (outsideProps.includes(prop)) {
							user[`$${prop}`] = user[key][prop];
							delete user[key][prop];
						}
					}
				}
				else {
					if (outsideProps.includes(key)) {
						user[`$${key}`] = user[key];
						delete user[key];
					}
				}
			}

			return user;
		};
	}

	//for group imports, make sure every record has a $token and the right shape
	// @ts-ignore
	if (job.recordType === `group` || job.recordType === 'export-import-profile') {
		return function addGroupKeysIfAbsent(group) {
			//wrong shape; fix it
			if (!(group.$set || group.$set_once || group.$add || group.$union || group.$append || group.$remove || group.$unset)) {
				let uuidKey;
				if (group.$distinct_id) uuidKey = "$distinct_id";
				else if (group.distinct_id) uuidKey = "distinct_id";
				else if (group.$group_id) uuidKey = "$group_id";
				else if (group.group_id) uuidKey = "group_id";
				else {
					if (job.verbose) console.log(`group record has no uuid:\n${JSON.stringify(group)}\n skipping record`);
					return {};
				}
				group = { $set: { ...group } };
				group.$group_id = group.$set[uuidKey];
				delete group.$set[uuidKey];
				delete group.$set.$group_id;
				delete group.$set.$token;
			}

			//catch missing token
			if (!group.$token && job.token) group.$token = job.token;

			//catch group key
			if (!group.$group_key && job.groupKey) group.$group_key = job.groupKey;

			//rename special props
			for (const key in group) {
				if (typeof group[key] === "object") {
					for (const prop in group[key]) {
						if (specialProps.includes(prop)) {
							group[key][`$${prop}`] = group[key][prop];
							delete group[key][prop];
						}

						if (outsideProps.includes(prop)) {
							group[`$${prop}`] = group[key][prop];
							delete group[key][prop];
						}
					}
				}
				else {
					if (outsideProps.includes(key)) {
						group[`$${key}`] = group[key];
						delete group[key];
					}
				}
			}

			return group;
		};
	}

	return noop;
}
