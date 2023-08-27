// const md5 = require('md5');
const murmurhash = require("murmurhash");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc");
dayjs.extend(utc);
const u = require("ak-tools");
const stringify = require("json-stable-stringify");

const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];

function ezTransforms(config) {
	if (config.recordType === `event`) {
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
	if (config.recordType === `user`) {
		return function addUserTokenIfAbsent(user) {
			//todo make it possible to take existing profiles and send them...

			//wrong shape; fix it
			if (!validOperations.some(op => Object.keys(user).includes(op))) {
				user = { $set: { ...user } };
				user.$distinct_id = user.$set.$distinct_id;
				delete user.$set.$distinct_id;
				delete user.$set.$token;

				//deal with mp export shape
				//? https://developer.mixpanel.com/reference/engage-query
				if (typeof user.$set?.$properties === "object") {
					user.$set = { ...user.$set.$properties };
					delete user.$set.$properties;
				}
			}

			//catch missing token
			if (!user.$token && config.token) user.$token = config.token;

			return user;
		};
	}

	//for group imports, make sure every record has a $token and the right shape
	if (config.recordType === `group`) {
		return function addGroupKeysIfAbsent(group) {
			//wrong shape; fix it
			if (!(group.$set || group.$set_once || group.$add || group.$union || group.$append || group.$remove || group.$unset)) {
				group = { $set: { ...group } };
				if (group.$set?.$group_key) group.$group_key = group.$set.$group_key;
				if (group.$set?.$distinct_id) group.$group_id = group.$set.$distinct_id;
				if (group.$set?.$group_id) group.$group_id = group.$set.$group_id;
				delete group.$set.$distinct_id;
				delete group.$set.$group_id;
				delete group.$set.$token;
			}

			//catch missing token
			if (!group.$token && config.token) group.$token = config.token;

			//catch group key
			if (!group.$group_key && config.groupKey) group.$group_key = config.groupKey;

			return group;
		};
	}
}

// side-effects; for efficiency
// removes: null, '', undefined, {}, []
function removeNulls(valuesToRemove = [null, "", undefined]) {
	return function (record) {
		const keysToEnum = ["properties", "$set", "$set_once"];
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
function addTags(config) {
	const type = config.recordType;
	const tags = config.tags || {};
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
function applyAliases(config) {
	const type = config.recordType;
	const aliases = config.aliases || {};
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

//offset the time of events by an integer number of hours
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
 */
function dedupeRecords(config) {
	const hashTable = config.hashTable;
	return function (record) {
		//JSON stable stringification
		const hash = murmurhash.v3(stringify(record));
		if (hashTable.has(hash)) {
			config.duplicates++;
			return {};
		} else {
			hashTable.add(hash);
			return record;
		}
	};
}

/**
 * this function is used to whitelist or blacklist events, prop keys, or prop values
 * @param  {any} config
 * @param  {import('../index.js').WhiteAndBlackListParams} params
 */
function whiteAndBlackLister(config, params) {
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
				config.whiteListSkipped++;
				return {};
			}
		}

		//check for event blacklist
		if (eventBlacklist.length) {
			if (eventBlacklist.includes(record?.event)) {
				config.blackListSkipped++;
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
				config.whiteListSkipped++;
				return {};
			}
		}

		//check for prop key blacklist
		if (propKeyBlacklist.length) {
			let pass = true;
			for (const key in record?.properties) {
				if (propKeyBlacklist.includes(key)) {
					config.blackListSkipped++;
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
				config.whiteListSkipped++;
				return {};
			}
		}

		//check for prop val blacklist
		if (propValBlacklist.length) {
			let pass = true;
			for (const key in record?.properties) {
				if (propValBlacklist.includes(record.properties[key])) {
					config.blackListSkipped++;
					pass = false;
				}
			}
			if (!pass) return {};
		}

		return record;
	};
}

module.exports = {
	ezTransforms,
	removeNulls,
	UTCoffset,
	addTags,
	applyAliases,
	dedupeRecords,
	whiteAndBlackLister
};
