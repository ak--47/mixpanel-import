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
		ignore_events = ["$feature",
			"$set",
			"$webvitals",
			"$pageleave",
			"$groupidentify",
			"$pageview",
			"$autocapture",
			"$rageclick",
			"$screen",
			"$capture_pageview",
			"$merge_dangerously"
		],
		identify_events = ["$identify"],
		ignore_props = [
			"$feature/",
			"$feature_flag_",
			"$replay_",
			"$sdk_debug",
			"$session_recording",
			"$set",
			"$set_once"
		]
	} = options;

	let personMap;

	if (heavyObjects.people) {
		personMap = heavyObjects.people;
	}
	else {
		console.warn("heavyObjects.people is empty, id mgmt is not possible");
		personMap = new Map();
	}

	// PERFORMANCE: Compile regex patterns ONCE outside the transform
	const deleteKeyPrefixes = [
		"token",
		...ignore_props
	];

	// Build regex pattern for property deletion - compiled ONCE
	const deletePropPattern = new RegExp(
		`^(${deleteKeyPrefixes.map(p => p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`
	);

	// Build regex pattern for event filtering - compiled ONCE
	const ignoreEventPattern = new RegExp(
		`^(${ignore_events.map(e => e.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`
	);

	return function transform(postHogEvent) {
		const {
			event: mpEventName,
			distinct_id: postHogDistinctId,
			ip: mpIp,
			timestamp: mpTimestamp,
			uuid: mpInsertId,
			person_id: postHogPersonId,	//we want to ignore this as it's a "posthog only" value		
			properties: postHogProperties,
			//...postHotTopLevelFields

		} = postHogEvent;

		// PERFORMANCE: Use pre-compiled regex instead of Array.some()
		// MEMORY FIX: Return null instead of {} to filter early in pipeline
		if (ignoreEventPattern.test(mpEventName)) return null;



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
		let distinct_id;
		let foundUserIdInMap = false;
		if (postHogDistinctId) distinct_id = postHogDistinctId;
		if (postHogDeviceId) device_id = postHogDeviceId;
		if (postHogUserId) user_id = postHogUserId;

		// PERFORMANCE: Single Map lookup instead of has() + get()
		const mappedUserId = personMap.get(postHogDistinctId);
		if (mappedUserId) {
			user_id = mappedUserId;
			foundUserIdInMap = true;
		}

		if (user_id) mp_props.$user_id = user_id;
		if (device_id) mp_props.$device_id = device_id;
		if (distinct_id) mp_props.distinct_id = distinct_id;

		// PERFORMANCE: Use pre-compiled regex pattern instead of recompiling
		for (const key in remainingPostHogProperties) {
			if (deletePropPattern.test(key)) {
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
			if (identify_events.some(evt => mpEventName === evt)) {

				const { distinct_id } = postHogEvent;
				const {
					$anon_distinct_id,
					$device_id,
					$session_id

				} = postHogEvent.properties;
				const mpIdentifyProps =
				{
					$user_id: distinct_id,
					$device_id: $device_id || $anon_distinct_id,
					$insert_id: mpInsertId,
					...mp_props
				};

				addIfDefined(mpIdentifyProps, '$session_id', $session_id);

				return {
					event: 'identity association',
					properties: mpIdentifyProps
				};
			}
			// return {};
		}

		// //quality check... $user_id should not have a "-" char
		// //device ids always should have "-"
		// if (mixpanelEvent.properties.$user_id && mixpanelEvent.properties.$user_id.includes("-")) {
		// 	debugger;
		// }
		// if (mixpanelEvent.properties.$device_id && !mixpanelEvent.properties.$device_id.includes("-")) {
		// 	debugger;
		// }

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


