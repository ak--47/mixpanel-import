const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
// const murmurhash = require("murmurhash");
// const { buildMapFromPath } = require("../components/parsers.js");




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
 * 
 */
function postHogEventsToMp(options, heavyObjects) {
	const {
		v2_compat = false,
		ignore_events = ["$feature", "$set", "$webvitals", "$pageleave"],
		ignore_props = ["$feature/", "$feature_flag_", "$replay_", "$sdk_debug", "$session_recording", "$set", "$set_once"]
	} = options;

	let personMap;

	if (heavyObjects.people) {
		personMap = heavyObjects.people;
	}
	else {
		console.warn("heavyObjects.people is empty, id mgmt is not possible");
		personMap = new Map();
	}

	return function transform(postHogEvent) {
		const {
			event: mpEventName,
			distinct_id: postHogDistinctId,
			ip: mpIp,
			timestamp: mpTimestamp,
			uuid: mpInsertId,
			properties: postHogProperties,
			...postHotTopLevelFields

		} = postHogEvent;

		if (ignore_events.some(prefix => mpEventName.startsWith(prefix))) return {};



		//json resolution
		let parsedPostHogProperties;
		if (typeof postHogProperties === "string") {
			parsedPostHogProperties = JSON.parse(postHogProperties);
		} else if (typeof postHogProperties === "object") {
			parsedPostHogProperties = postHogProperties;
		} else {
			throw new Error("posthog properties must be a string or object");
		}

		//extraction
		const {
			$geoip_city_name: mpCity,
			$geoip_country_code: mpCountryCode,
			$geoip_latitude: mpLatitude,
			$geoip_longitude: mpLongitude,
			$user_id: postHogUserId,
			$device_id: postHogDeviceId,
			...remainingPostHogProperties
		} = parsedPostHogProperties;

		const mp_props = {
			time: dayjs.utc(mpTimestamp).valueOf(),
			$source: `posthog-to-mixpanel`,

		};

		//defaults props
		addIfDefined(mp_props, 'ip', mpIp);
		addIfDefined(mp_props, '$city', mpCity);
		addIfDefined(mp_props, '$region', mpCountryCode);
		addIfDefined(mp_props, 'mp_country_code', postHogEvent.country);
		addIfDefined(mp_props, '$insert_id', mpInsertId);
		addIfDefined(mp_props, '$latitude', mpLatitude);
		addIfDefined(mp_props, '$longitude', mpLongitude);


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

		if (user_id) mp_props.$user_id = user_id;
		if (device_id) mp_props.$device_id = device_id;


		// cleaning
		const deleteKeyPrefixes = [
			"token",
			...ignore_props
		];
		for (const key in remainingPostHogProperties) {
			if (deleteKeyPrefixes.some(prefix => key.startsWith(prefix))) {
				delete remainingPostHogProperties[key];
			}
		}

		//assemble
		const mixpanelEvent = { event: mpEventName, properties: { ...mp_props, ...remainingPostHogProperties } };
		if (mixpanelEvent.properties.token) delete mixpanelEvent.properties.token;

		//idmerge v2
		if (v2_compat) {
			if (device_id) mixpanelEvent.properties.distinct_id = device_id;
			if (user_id) mixpanelEvent.properties.distinct_id = user_id;

			// shape of identify events for v2
			if (mpEventName?.startsWith("$identify")) {
				const identified_id = user_id;
				const anon_id = device_id;
				const identify_props = { $identified_id: identified_id, $anon_id: anon_id };
				const allProps = { ...mixpanelEvent.properties, ...identify_props };
				mixpanelEvent.properties = allProps;

			}




		}

		//idmerge v3
		if (!v2_compat) {
			// don't send identify events in simplified mode
			if (mixpanelEvent.event === "$identify") {
				return {};
			}
		}


		return mixpanelEvent;
	};
}

/**
 * returns a function that transforms an amplitude user into a mixpanel user
 * @param  {import('../index').postHogOpts} options
 */
function postHogPersonToMpProfile(options, heavyObjects = {}) {
	// let personMap;

	// if (heavyObjects.people) {
	// 	personMap = heavyObjects.people;
	// }
	// else {
	// 	console.warn("heavyObjects.people is empty, id mgmt is not possible");
	// 	personMap = new Map();
	// }


	return function transform(postHogPerson) {
		const {
			person_id: distinct_id,
			created_at,
			team_id,
		} = postHogPerson;

		if (!distinct_id) return {};
		const postHogProperties = JSON.parse(postHogPerson.properties) || {};

		const mpProps = {};

		loopProps: for (const key in postHogProperties) {
			if (key.startsWith("$")) continue loopProps;
			if (!postHogProperties[key]) continue loopProps;
			mpProps[`posthog_${key}`] = postHogProperties[key];
		}

		const mpProfile = {
			$distinct_id: distinct_id,
			created_at: dayjs.unix(created_at).toISOString(),
			team_id,
			...mpProps
		};

		return mpProfile;
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


