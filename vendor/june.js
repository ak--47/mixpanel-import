const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
const murmurhash = require("murmurhash");
const { NODE_ENV = "dev" } = process.env;

/*
----
TRANSFORMS
----
*/

/**
 * returns a function that transforms a June.so event into a mixpanel event
 * @param  {import('../index').juneOpts} options
 */
function juneEventsToMp(options = {}) {
	const { user_id = "user_id", anonymous_id = "anonymous_id", v2compat = true } = options;

	return function transform(juneEvent) {
		// CSV data gets wrapped in properties object by the pipeline
		let { anonymous_id = "",
			context = "{}",
			name = "",
			properties = "{}",
			timestamp = "",
			type = "",
			user_id = "" } = juneEvent;

		try {
			properties = JSON.parse(properties);
		}
		catch (e) {
			//noop
		}

		try {
			context = JSON.parse(context);
		} catch (e) {
			// noop
		}

		const mixpanelProperties = {
			time: dayjs.utc(timestamp).valueOf(),
			$source: 'june-to-mixpanel',
			type,
			...properties,
			...context
		};

		//insert_id = delivery_id
		if (properties.delivery_id) {
			mixpanelProperties.$insert_id = murmurhash.v3(properties.delivery_id).toString();
		}
		else {
			// Generate a synthetic insert_id to prevent duplicates
			const tuple = [anonymous_id, user_id, name, timestamp].join("-");
			mixpanelProperties.$insert_id = murmurhash.v3(tuple).toString();
			// if (NODE_ENV === "dev") debugger;
		}



		// $device_id = anonymous_id
		// $user_id = user_id
		if (anonymous_id) {
			mixpanelProperties.$device_id = anonymous_id;
		}
		if (user_id) {
			mixpanelProperties.$user_id = user_id;
		}

		if (v2compat) {
			// In v2 compatibility mode, set distinct_id for Mixpanel events
			if (user_id) {
				mixpanelProperties.distinct_id = user_id;
			} else if (anonymous_id) {
				mixpanelProperties.distinct_id = anonymous_id;
			}
		}


		// Extract context properties
		if (context.page) {
			const page = context.page;
			if (page.url) mixpanelProperties.$current_url = page.url;
			if (page.path) mixpanelProperties.$pathname = page.path;
			if (page.referrer) mixpanelProperties.$referrer = page.referrer;
			if (page.search) mixpanelProperties.$search = page.search;
			if (page.title) mixpanelProperties.$title = page.title;
		}

		if (context.userAgent) {
			mixpanelProperties.$browser = context.userAgent;
		}

		if (context.ip) {
			mixpanelProperties.ip = context.ip;
		}

		if (context.locale) {
			mixpanelProperties.$locale = context.locale;
		}

		if (context.library) {
			mixpanelProperties.$lib = context.library.name;
			mixpanelProperties.$lib_version = context.library.version;
		}

		if (context.integration) {
			mixpanelProperties.integration_name = context.integration.name;
			mixpanelProperties.integration_version = context.integration.version;
		}
		const finalEvent = {
			event: name || "unnamed june event",
			properties: mixpanelProperties
		}

		return finalEvent;
	};
}

/**
 * returns a function that transforms a June.so user identify call into a mixpanel user profile
 * @param  {import('../index').juneOpts} options
 */
function juneUserToMp(options = {}) {
	const { user_id = "user_id" } = options;

	return function transform(juneUser) {
		// CSV data gets wrapped in properties object by the pipeline
		const userData = juneUser.properties || juneUser;

		// Parse JSON fields from CSV
		let traits = {};
		let context = {};

		try {
			if (typeof userData.traits === 'string') {
				traits = JSON.parse(userData.traits);
			} else if (typeof userData.traits === 'object') {
				traits = userData.traits || {};
			}
		} catch (e) {
			traits = {};
		}

		try {
			if (typeof userData.context === 'string') {
				context = JSON.parse(userData.context);
			} else if (typeof userData.context === 'object') {
				context = userData.context || {};
			}
		} catch (e) {
			context = {};
		}

		const mixpanelUser = {
			$distinct_id: userData.user_id || userData.anonymous_id,
			$set: {
				$source: 'june-to-mixpanel'
			}
		};

		// Skip if no user identifier
		if (!mixpanelUser.$distinct_id || mixpanelUser.$distinct_id === '') {
			return null;
		}

		// Add user traits as profile properties
		Object.keys(traits).forEach(key => {
			let value = traits[key];

			// Map common fields to Mixpanel standard properties
			switch (key) {
				case 'email':
					mixpanelUser.$set.$email = value;
					break;
				case 'firstName':
					mixpanelUser.$set.$first_name = value;
					break;
				case 'lastName':
					mixpanelUser.$set.$last_name = value;
					break;
				case 'phoneNumber':
					mixpanelUser.$set.$phone = value;
					break;
				case 'creationDate':
					mixpanelUser.$set.$created = dayjs.utc(value).toISOString();
					break;
				default:
					mixpanelUser.$set[key] = value;
			}
		});

		// Add context data with june_context_ prefix
		Object.keys(context).forEach(key => {
			if (key === 'ip') {
				mixpanelUser.$ip = context[key];
			} else {
				mixpanelUser.$set[`june_context_${key}`] = context[key];
			}
		});

		// Add anonymous_id if present
		if (userData.anonymous_id) {
			mixpanelUser.$set.june_anonymous_id = userData.anonymous_id;
		}

		return mixpanelUser;
	};
}

/**
 * returns a function that transforms a June.so group call into a mixpanel group profile
 * @param  {import('../index').juneOpts} options
 */
function juneGroupToMp(options = {}) {
	const { group_key = "group_id" } = options;

	return function transform(juneGroup) {
		// CSV data gets wrapped in properties object by the pipeline
		const groupData = juneGroup.properties || juneGroup;

		// Parse JSON fields from CSV
		let traits = {};
		let context = {};

		try {
			if (typeof groupData.traits === 'string') {
				traits = JSON.parse(groupData.traits);
			} else if (typeof groupData.traits === 'object') {
				traits = groupData.traits || {};
			}
		} catch (e) {
			traits = {};
		}

		try {
			if (typeof groupData.context === 'string') {
				context = JSON.parse(groupData.context);
			} else if (typeof groupData.context === 'object') {
				context = groupData.context || {};
			}
		} catch (e) {
			context = {};
		}

		const mixpanelGroup = {
			$group_key: group_key,
			$group_id: groupData.group_id,
			$set: {
				$source: 'june-to-mixpanel'
			}
		};

		// Skip if no group identifier
		if (!mixpanelGroup.$group_id || mixpanelGroup.$group_id === '') {
			return null;
		}

		// Add group traits as group profile properties
		Object.keys(traits).forEach(key => {
			let value = traits[key];

			// Map common fields
			switch (key) {
				case 'name':
					mixpanelGroup.$set.name = value;
					break;
				case 'creationDate':
					mixpanelGroup.$set.$created = dayjs.utc(value).toISOString();
					break;
				default:
					mixpanelGroup.$set[key] = value;
			}
		});

		// Add context data with june_context_ prefix
		Object.keys(context).forEach(key => {
			mixpanelGroup.$set[`june_context_${key}`] = context[key];
		});

		// Add user association if present
		if (groupData.user_id) {
			mixpanelGroup.$set.associated_user_id = groupData.user_id;
		}

		if (groupData.anonymous_id) {
			mixpanelGroup.$set.june_anonymous_id = groupData.anonymous_id;
		}

		return mixpanelGroup;
	};
}

// Common June.so to Mixpanel field mappings
const juneMixPairs = [
	["user_id", "$user_id"],
	["anonymous_id", "anonymous_id"],
	["timestamp", "time"]
];

module.exports = {
	juneEventsToMp,
	juneUserToMp,
	juneGroupToMp,
	juneMixPairs
};