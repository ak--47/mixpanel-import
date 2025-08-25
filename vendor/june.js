const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
const murmurhash = require("murmurhash");

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
	const { user_id = "user_id", anonymous_id = "anonymous_id" } = options;

	return function transform(juneEvent) {
		// CSV data gets wrapped in properties object by the pipeline
		const eventData = juneEvent.properties || juneEvent;
		
		// Parse JSON fields from CSV
		let properties = {};
		let context = {};
		
		try {
			if (typeof eventData.properties === 'string') {
				properties = JSON.parse(eventData.properties);
			} else if (typeof eventData.properties === 'object') {
				properties = eventData.properties || {};
			}
		} catch (e) {
			properties = {};
		}

		try {
			if (typeof eventData.context === 'string') {
				context = JSON.parse(eventData.context);
			} else if (typeof eventData.context === 'object') {
				context = eventData.context || {};
			}
		} catch (e) {
			context = {};
		}

		const mixpanelEvent = {
			event: eventData.name || eventData.event || "Unknown Event",
			properties: {
				time: dayjs.utc(eventData.timestamp).valueOf(),
				$source: 'june-to-mixpanel'
			}
		};

		// Set user identifiers
		if (eventData.user_id && eventData.user_id !== '') {
			mixpanelEvent.properties.distinct_id = eventData.user_id;
			mixpanelEvent.properties.$user_id = eventData.user_id;
		} else if (eventData.anonymous_id && eventData.anonymous_id !== '') {
			mixpanelEvent.properties.distinct_id = eventData.anonymous_id;
			mixpanelEvent.properties.anonymous_id = eventData.anonymous_id;
		}

		// Generate $insert_id for deduplication
		const insertIdComponents = [
			eventData.user_id || eventData.anonymous_id || '',
			eventData.timestamp || '',
			eventData.name || eventData.event || ''
		].join("-");
		mixpanelEvent.properties.$insert_id = murmurhash.v3(insertIdComponents).toString();

		// Extract context properties
		if (context.page) {
			const page = context.page;
			if (page.url) mixpanelEvent.properties.$current_url = page.url;
			if (page.path) mixpanelEvent.properties.$pathname = page.path;
			if (page.referrer) mixpanelEvent.properties.$referrer = page.referrer;
			if (page.search) mixpanelEvent.properties.$search = page.search;
			if (page.title) mixpanelEvent.properties.$title = page.title;
		}

		if (context.userAgent) {
			mixpanelEvent.properties.$browser = context.userAgent;
		}

		if (context.ip) {
			mixpanelEvent.properties.ip = context.ip;
		}

		if (context.locale) {
			mixpanelEvent.properties.$locale = context.locale;
		}

		if (context.library) {
			mixpanelEvent.properties.$lib = context.library.name;
			mixpanelEvent.properties.$lib_version = context.library.version;
		}

		if (context.integration) {
			mixpanelEvent.properties.integration_name = context.integration.name;
			mixpanelEvent.properties.integration_version = context.integration.version;
		}

		// Add all custom properties from June event
		mixpanelEvent.properties = {
			...properties,
			...mixpanelEvent.properties
		};

		// Add any remaining context properties that weren't specifically mapped
		const contextCopy = { ...context };
		if ('page' in contextCopy) delete contextCopy.page;
		if ('userAgent' in contextCopy) delete contextCopy.userAgent;
		if ('ip' in contextCopy) delete contextCopy.ip;
		if ('locale' in contextCopy) delete contextCopy.locale;
		if ('library' in contextCopy) delete contextCopy.library;
		if ('integration' in contextCopy) delete contextCopy.integration;

		// Flatten remaining context with june_context_ prefix
		Object.keys(contextCopy).forEach(key => {
			mixpanelEvent.properties[`june_context_${key}`] = contextCopy[key];
		});

		return mixpanelEvent;
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