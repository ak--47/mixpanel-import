const murmurhash = require("murmurhash");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const u = require("ak-tools");
const stringify = require("json-stable-stringify");
const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];
// ? https://docs.mixpanel.com/docs/data-structure/user-profiles#reserved-profile-properties
const specialProps = ["name", "first_name", "last_name", "email", "phone", "avatar", "created", "insert_id"];
const outsideProps = ["distinct_id", "group_id", "token", "group_key", "ip"]; //these are the props that are outside of the $set

/** @typedef {import('./job')} JobConfig */
/** @typedef {import('../index').Data} Data */
/** @typedef {import('../index').Options} Options */
/** @typedef {import('../index').Creds} Creds */
/** @typedef {import('../index').ImportResults} ImportResults */


// a noop function
function noop(a) { return a; }

/**
 * @param  {JobConfig} jobConfig
 */
function ezTransforms(jobConfig) {
	if (jobConfig.recordType === `event`) {
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

			return record;
		};
	}

	//for user imports, make sure every record has a $token and the right shape
	if (jobConfig.recordType === `user`) {
		return function addUserTokenIfAbsent(user) {

			//wrong shape; fix it
			if (!validOperations.some(op => Object.keys(user).includes(op))) {
				let uuidKey;
				if (user.$distinct_id) uuidKey = "$distinct_id";
				else if (user.distinct_id) uuidKey = "distinct_id";
				else {
					if (jobConfig.verbose) console.log(`user record has no uuid:\n${JSON.stringify(user)}\n skipping record`);
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
			if (!user.$token && jobConfig.token) user.$token = jobConfig.token;

			//rename special props
			for (const key in user) {
				if (typeof user[key] === "object") {
					for (const prop in user[key]) {
						if (specialProps.includes(prop)) {
							user[key][`$${prop}`] = user[key][prop];
							delete user[key][prop];
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
	if (jobConfig.recordType === `group`) {
		return function addGroupKeysIfAbsent(group) {
			//wrong shape; fix it
			if (!(group.$set || group.$set_once || group.$add || group.$union || group.$append || group.$remove || group.$unset)) {
				let uuidKey;
				if (group.$distinct_id) uuidKey = "$distinct_id";
				else if (group.distinct_id) uuidKey = "distinct_id";
				else if (group.$group_id) uuidKey = "$group_id";
				else if (group.group_id) uuidKey = "group_id";
				else {
					if (jobConfig.verbose) console.log(`group record has no uuid:\n${JSON.stringify(group)}\n skipping record`);
					return {};
				}
				group = { $set: { ...group } };
				group.$group_id = group.$set[uuidKey];
				delete group.$set[uuidKey];
				delete group.$set.$group_id;
				delete group.$set.$token;
			}

			//catch missing token
			if (!group.$token && jobConfig.token) group.$token = jobConfig.token;

			//catch group key
			if (!group.$group_key && jobConfig.groupKey) group.$group_key = jobConfig.groupKey;

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
		propValWhitelist = []
	} = params;

	return function whiteAndBlackList(record) {
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
			let pass = false;
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
			let pass = true;
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
			let pass = false;
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
			let pass = true;
			for (const key in record?.properties) {
				if (propValBlacklist.includes(record.properties[key])) {
					jobConfig.blackListSkipped++;
					pass = false;
				}
			}
			if (!pass) return {};
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
	addToken
};
