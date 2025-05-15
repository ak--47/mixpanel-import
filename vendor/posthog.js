const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
// const murmurhash = require("murmurhash");
const { buildDeviceIdMap } = require("../components/parsers.js");




/*
----
TRANSFORMS
----
*/

/**
 * returns a function that transforms an posthog event into a mixpanel event
 * v2_compat sets distinct_id, but will not implicitly join user_id/device_id
 * ^ in order to do this we would need to return [{ $identify },{ ogEvent }] and pass it down the stream
 * @param  {import('../index').postHogOpts} options
 */
function postHogEventsToMp(options) {
	const {
		device_id_file = "",
		v2_compat = false,
		ignore = ["$feature", "$set", "$webvitals", "$pageleave"],
	} = options;

	if (!device_id_file) throw new Error("device_id_file is required for posthog transform");

	const personMap = buildDeviceIdMap(device_id_file);


	return function transform(postHogEvent) {
		const {
			event: mpEventName,
			distinct_id: postHogDistinctId,
			ip: mpIp,
			timestamp: mpTimestamp,
			uuid: mpInsertId,
			properties: postHogProperties,
			...postHogTopFields

		} = postHogEvent;

		if (ignore.some(prefix => mpEventName.startsWith(prefix))) return {};



		//json resolution
		let parsedPostHogProperties;
		if (typeof postHogProperties === "string") {
			parsedPostHogProperties = JSON.parse(postHogProperties);
		} else if (typeof postHogProperties === "object") {
			parsedPostHogProperties = postHogProperties;
		} else {
			throw new Error("posthog properties must be a string or object");
		}

		const {
			$geoip_city_name: mpCity,
			$geoip_country_code: mpCountryCode,
			$geoip_latitude: mpLatitude,
			$geoip_longitude: mpLongitude,
			$user_id: postHogUserId,
			$device_id: postHogDeviceId,
			...remainingPostHogProperties
		} = parsedPostHogProperties;

		const props = {
			time: dayjs.utc(mpTimestamp).valueOf(),
			$source: `posthog-to-mixpanel`,

		};

		//defaults
		addIfDefined(props, 'ip', mpIp);
		addIfDefined(props, '$city', mpCity);
		addIfDefined(props, '$region', mpCountryCode);
		addIfDefined(props, 'mp_country_code', postHogEvent.country);
		addIfDefined(props, '$insert_id', mpInsertId);
		addIfDefined(props, '$latitude', mpLatitude);
		addIfDefined(props, '$longitude', mpLongitude);


		//identities
		let user_id;
		let device_id;
		let foundUserIdInMap = false;
		if (postHogDistinctId) device_id = postHogDistinctId;
		if (postHogDeviceId) device_id = postHogDeviceId;
		if (postHogUserId) user_id = postHogUserId;

		if (personMap.has(postHogDistinctId)) {
			user_id = personMap.get(postHogDistinctId);
			foundUserIdInMap = true;
		}

		if (user_id) props.$user_id = user_id;
		if (device_id) props.$device_id = device_id;

		const deleteKeyPrefixes = [
			"$feature/",
			"$feature_flag_",
			"$replay_",
			"$sdk_debug",
			"$session_recording",
			"$set",
			"$set_once",
			"token"
		];
		for (const key in remainingPostHogProperties) {
			if (deleteKeyPrefixes.some(prefix => key.startsWith(prefix))) {
				delete remainingPostHogProperties[key];
			}
		}

		const mixpanelEvent = { event: mpEventName, properties: { ...props, ...remainingPostHogProperties } };
		if (mixpanelEvent.properties.token) delete mixpanelEvent.properties.token;

		if (v2_compat) {
			if (device_id) mixpanelEvent.properties.distinct_id = device_id;
			if (user_id) mixpanelEvent.properties.distinct_id = user_id;
		}

		if (!v2_compat) {
			// no identify events in simplified
			if (mixpanelEvent.event === "$identify") {
				return {};
			}
		}


		// if (!v2_compat) delete mixpanelEvent.properties.distinct_id;

		//v2 compat requires distinct_id; prefer user_id if available
		if (v2_compat) {
			// if (device_id) mixpanelEvent.properties.distinct_id = device_id;
			// if (user_id) mixpanelEvent.properties.distinct_id = user_id;

			//todo: v2 also requires identify events...
			if (mpEventName?.startsWith("$identify")) {
				if (foundUserIdInMap) {
					//todo
					debugger;
				}

				if (!foundUserIdInMap) {
					//todo
					debugger;
				}
			}

		}

		return mixpanelEvent;
	};
}

/**
 * returns a function that transforms an amplitude user into a mixpanel user
 * @param  {import('../index').postHogOpts} options
 */
function postHogPersonToMpProfile(options) {
	return function transform(postHogPerson) {
		return postHogPerson;
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
const postHogMixPairs = [
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


function addIfDefined(target, key, value) {
	if (value != null) target[key] = value;
}

module.exports = {
	postHogEventsToMp,
	postHogPersonToMpProfile,
	postHogMixPairs
};


