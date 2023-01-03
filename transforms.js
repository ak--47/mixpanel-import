const md5 = require('md5');

function ezTransforms(config) {	
	//for strict event imports, make every record has an $insert_id
	if (config.recordType === `event` && config.transformFunc('A') === 'A') {
		config.transform = function addInsertIfAbsent(event) {
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
	if (config.recordType === `user` && config.transformFunc('A') === 'A') {
		config.transform = function addUserTokenIfAbsent(user) {
			//wrong shape; fix it
			if (!(user.$set || user.$set_once || user.$add || user.$union || user.$append || user.$remove || user.$unset)) {
				user = { $set: { ...user } };
				user.$distinct_id = user.$set.$distinct_id;
				delete user.$set.$distinct_id;
				delete user.$set.$token;
			}

			//catch missing token
			if ((!user.$token) && config.token) user.$token = config.token;

			return user;
		};
	}


	//for group imports, make sure every record has a $token and the right shape
	if (config.recordType === `group` && config.transformFunc('A') === 'A') {
		config.transform = function addGroupKeysIfAbsent(group) {
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

module.exports = ezTransforms