const md5 = require('md5');
const dayjs = require('dayjs');

function ezTransforms(config) {
	//for strict event imports, make every record has an $insert_id
	if (config.recordType === `event`) {
		return function addInsertIfAbsent(event) {
			if (!event?.properties?.$insert_id) {
				try {
					let deDupeTuple = [event.name, event.properties.distinct_id || "", event.properties.time];
					let hash = md5(deDupeTuple);
					event.properties.$insert_id = hash;
				}
				catch (e) {
					event.properties.$insert_id = event.properties.distinct_id;
				}
				return event;
			}
			else {
				return event;
			}
		};

	}

	//for user imports, make sure every record has a $token and the right shape
	if (config.recordType === `user`) {
		return function addUserTokenIfAbsent(user) {
			//todo make it possible to take existing profiles and send them...

			//wrong shape; fix it
			if (!(user.$set || user.$set_once || user.$add || user.$union || user.$append || user.$remove || user.$unset)) {
				user = { $set: { ...user } };
				user.$distinct_id = user.$set.$distinct_id;
				delete user.$set.$distinct_id;
				delete user.$set.$token;
				
				//deal with mp export shape
				//? https://developer.mixpanel.com/reference/engage-query
				if (typeof user.$set?.$properties === 'object') {
					user.$set = { ...user.$set.$properties };
					delete user.$set.$properties;
				}
			}

			//catch missing token
			if ((!user.$token) && config.token) user.$token = config.token;



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
			if ((!group.$token) && config.token) group.$token = config.token;

			//catch group key
			if ((!group.$group_key) && config.groupKey) group.$group_key = config.groupKey;

			return group;
		};
	}
}

// side-effects; for efficiency 
// removes: null, '', undefined, {}, [] 
function removeNulls(valuesToRemove = [null, '', undefined]) {
	return function (record) {
		const keysToEnum = ['properties', '$set', '$set_once'];
		for (const recordKey of keysToEnum) {
			for (const badVal of valuesToRemove) {
				if (record?.[recordKey]) {
					for (const p in record[recordKey]) {
						if (record[recordKey][p] === badVal) {
							delete record[recordKey][p];
						}
						//test for {}
						try {
							if (typeof record[recordKey][p] === 'object') {
								if (Object.keys(record[recordKey][p]).length === 0) {
									delete record[recordKey][p];
								}
							}
						}
						catch (e) {
							//noop
						}
					}
				}
			}
		}

		return record;
	};

}

function UTCoffset(timeOffset = 0) {
	return function (record) {
		if (record?.properties?.time) {
			const oldTime = dayjs.unix((record.properties.time));
			const newTime = oldTime.add(timeOffset, 'h').valueOf();
			record.properties.time = newTime;
		}
		return record;
	};
}

module.exports = {
	ezTransforms,
	removeNulls,
	UTCoffset
};