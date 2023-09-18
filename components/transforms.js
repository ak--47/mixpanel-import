const murmurhash = require("murmurhash");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const u = require("ak-tools");
const stringify = require("json-stable-stringify");
const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];

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

			return group;
		};
	}

	return noop;
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

// rename property keys
/**
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

module.exports = {
	ezTransforms,
	removeNulls,
	UTCoffset,
	addTags,
	applyAliases,
	dedupeRecords,
	whiteAndBlackLister,
	epochFilter,
	isNotEmpty
};
