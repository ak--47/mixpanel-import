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
		};

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
		let { user_id = "", anonymous_id = "", traits = {}, context = {} } = juneUser;

		if (!user_id) return {};

		try {
			traits = JSON.parse(traits);
		}
		catch (e) {
			//noop
		}

		try {
			context = JSON.parse(context);
		} catch (e) {
			// noop
		}

		const props = {
			...context,
			...traits
		};

		const juneMixMap = Object.fromEntries(juneMixPairs);

		for (const key in props) {
			const mixpanelKey = juneMixMap[key];
			if (mixpanelKey) {
				props[mixpanelKey] = props[key];
				delete props[key];
			}
		}

		const mixpanelUser = {
			$distinct_id: user_id,
			$set: {
				$source: 'june-to-mixpanel',
				...props
			}
		};

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
		let { group_id = "", traits = {}, context = {} } = juneGroup;
		if (!group_id) return {};
		try {
			traits = JSON.parse(traits);
		}
		catch (e) {
			// noop
		}

		try {
			context = JSON.parse(context);
		}
		catch (e) {
			// noop
		}

		const props = {
			...traits,
			...context
		};

		const juneMixMap = Object.fromEntries(juneMixPairs);

		for (const key in props) {
			const mixpanelKey = juneMixMap[key];
			if (mixpanelKey) {
				props[mixpanelKey] = props[key];
				delete props[key];
			}
		}


		const mixpanelGroup = {
			$group_id: group_id,
			$group_key: group_key,
			$set: {
				$source: 'june-to-mixpanel',
				...props
			}
		};

		return mixpanelGroup;
	};
}

// Common June.so to Mixpanel field mappings
const juneMixPairs = [
	["user_id", "$user_id"],
	["anonymous_id", "anonymous_id"],
	["timestamp", "time"],
	["firstName", "$first_name"],
	["lastName", "$last_name"],
	["email", "$email"],
	["phoneNumber", "$phone"],
	["creationDate", "$created"],
	["ip", "$ip"],
	["name", "$name"]
];

module.exports = {
	juneEventsToMp,
	juneUserToMp,
	juneGroupToMp,
	juneMixPairs
};