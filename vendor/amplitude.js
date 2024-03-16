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
 * returns a function that transforms an amplitude event into a mixpanel event
 * @param  {import('../index').amplitudeOpts} options
 */
function ampEventsToMp(options) {
	const { user_id = "user_id" } = options;

	return function transform(ampEvent) {
		const mixpanelEvent = {
			event: ampEvent.event_type,
			properties: {
				$device_id: ampEvent.device_id || "",
				time: dayjs.utc(ampEvent.event_time).valueOf(),				
				ip: ampEvent.ip_address,
				$city: ampEvent.city,
				$region: ampEvent.region,
				mp_country_code: ampEvent.country,
				$source: `amplitude-to-mixpanel`
			}
		};
		
		//insert_id resolution
		const $insert_id = ampEvent.$insert_id;
		if ($insert_id) mixpanelEvent.properties.$insert_id = $insert_id;
		if (!$insert_id) mixpanelEvent.properties.$insert_id = murmurhash.v3([ampEvent.device_id, ampEvent.event_time, ampEvent.event_type].join("-")).toString();

		//canonical id resolution
		if (ampEvent?.user_properties?.[user_id]) mixpanelEvent.properties.$user_id = ampEvent.user_properties[user_id];
		if (ampEvent[user_id]) mixpanelEvent.properties.$user_id = ampEvent[user_id];

		//get all custom props + group props + user props
		mixpanelEvent.properties = {
			...ampEvent.event_properties,
			...ampEvent.groups,
			...ampEvent.user_properties,
			...mixpanelEvent.properties
		};

		//remove what we don't need
		delete ampEvent[user_id];
		delete ampEvent.device_id;
		delete ampEvent.event_time;
		delete ampEvent.$insert_id;
		delete ampEvent.user_properties;
		delete ampEvent.group_properties;
		delete ampEvent.global_user_properties;
		delete ampEvent.event_properties;
		delete ampEvent.groups;
		delete ampEvent.data;

		//fill in defaults & delete from amp data (if found)
		for (let ampMixPair of ampMixPairs) {
			if (ampEvent[ampMixPair[0]]) {
				mixpanelEvent.properties[ampMixPair[1]] = ampEvent[ampMixPair[0]];
				delete ampEvent[ampMixPair[0]];
			}
		}

		//gather everything else
		mixpanelEvent.properties = {
			...ampEvent,
			...mixpanelEvent.properties
		};

		return mixpanelEvent;
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
			$group_key: null, //todo
			$group_id: null, //todo
			$set: groupProps
		};

		return mixpanelGroup;
	};
}




/*
----
RANDOM
----
*/

//amp to mp default props
// ? https://developers.amplitude.com/docs/identify-api
// ? https://help.mixpanel.com/hc/en-us/articles/115004613766-Default-Properties-Collected-by-Mixpanel
const ampMixPairs = [
	["app_version", "$app_version_string"],
	["os_name", "$os"],
	["os_name", "$browser"],
	["os_version", "$os_version"],
	["device_brand", "$brand"],
	["device_manufacturer", "$manufacturer"],
	["device_model", "$model"],
	["region", "$region"],
	["city", "$city"]
];




module.exports = {
	ampEventsToMp,
	ampUserToMp,
	ampGroupToMp,
	ampMixPairs
};