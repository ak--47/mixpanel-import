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
 * returns a function that transforms a mixpanel event in cloud storage into a mixpanel event over API 
 * @param  {import('../index').amplitudeOpts} options
 */
function mixpanelEventsToMixpanel(options) {
	const { v2_compat = true } = options;

	return function transform(mpEvent) {
		// Direct property access is faster than destructuring
		const finalProperties = { ...mpEvent.properties };

		// Only add properties that exist (avoid extra checks)
		if (mpEvent.device_id) finalProperties.$device_id = mpEvent.device_id;
		if (mpEvent.distinct_id) finalProperties.distinct_id = mpEvent.distinct_id;
		if (mpEvent.insert_id) finalProperties.$insert_id = mpEvent.insert_id;
		if (mpEvent.time) finalProperties.time = mpEvent.time;
		if (mpEvent.user_id) finalProperties.$user_id = mpEvent.user_id;

		return {
			event: mpEvent.event_name || "unnamed",
			properties: finalProperties
		};
	};
}

/**
 * returns a function that transforms an amplitude user into a mixpanel user
 * @param  {import('../index').amplitudeOpts} options
 */
function ampUserToMp(options) {
	const { user_id = "user_id" } = options;

	return function transform(ampEvent) {
		const userProps = ampEvent.user_properties;

		//skip empty props
		if (Object.keys(userProps).length === 0) return {};

		let distinct_id;
		//canonical id resolution
		if (ampEvent?.user_properties?.[user_id]) distinct_id = ampEvent.user_properties[user_id];
		if (ampEvent[user_id]) distinct_id = ampEvent[user_id];

		//skip no user_id
		if (!distinct_id) return {};

		const mixpanelProfile = {
			$distinct_id: distinct_id,
			$ip: ampEvent.ip_address,
			$set: userProps
		};

		//include defaults, if they exist
		for (let ampMixPair of ampMixPairs) {
			if (ampEvent[ampMixPair[0]]) {
				mixpanelProfile.$set[ampMixPair[1]] = ampEvent[ampMixPair[0]];
			}
		}

		return mixpanelProfile;
	};
}

/**
 * returns a function that transforms an amplitude group into a mixpanel group
 * @param  {import('../index').amplitudeOpts} options
 */
function ampGroupToMp(options) {
	// const { user_id, group_keys } = options;

	return function transform(ampEvent) {
		const groupProps = ampEvent.group_properties;

		//skip empty + no user_id
		if (Object.keys(groupProps).length === 0) return {};
		if (!ampEvent.user_id) return {};

		const mixpanelGroup = {
			$group_key: null,
			$group_id: null,
			$set: groupProps
		};

		return mixpanelGroup;
	};
}







module.exports = {
	mixpanelEventsToMixpanel,
	// ampUserToMp,
	// ampGroupToMp,
	// ampMixPairs
};