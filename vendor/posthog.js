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
			// "$pageview",
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
		// console.warn("heavyObjects.people is empty, id mgmt is not possible");
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

	const { directive = "$set", ignore_props = ["$creator_event_uuid"] } = options;
	const deleteKeyPrefixes = [
		"token",
		...ignore_props
	];

	// Build regex pattern for property deletion - compiled ONCE
	const deletePropPattern = new RegExp(
		`^(${deleteKeyPrefixes.map(p => p.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|')})`
	);
	// if (heavyObjects.people) {
	// 	personMap = heavyObjects.people;
	// }
	// else {
	// 	console.warn("heavyObjects.people is empty, id mgmt is not possible");
	// 	personMap = new Map();
	// }


	return function transform(postHogPerson) {
		const {
			distinct_id,
			created_at,
			properties: postHogUserProperties,
			...otherProps
		} = postHogPerson;

		if (!distinct_id) return {};
		// if (distinct_id.includes("-")) debugger;
		const mpProps = {};


		// first loop through defaults
		for (const [postHogKey, mpKey] of postHogMixProfilePairs) {
			if (!postHogKey) continue;
			const value = postHogUserProperties[postHogKey];
			if (value == null) continue;
			mpProps[mpKey] = value;
		}

		// then loop through remaining props
		for (const key in postHogUserProperties) {
			// skip already-mapped keys
			if (postHogMixProfilePairs.some(([phKey, _]) => phKey === key)) continue;

			// delete ignored props w/regex
			if (deletePropPattern.test(key)) {
				continue;
			}

			// add remaining props as-is
			const value = postHogUserProperties[key];
			if (value == null) continue;
			mpProps[key] = value;
		}

		const mpProfile = {
			$distinct_id: distinct_id,
			[directive]: {
				$created: dayjs.unix(created_at).toISOString(),
				...mpProps
			}
		};

		// $latitude and $longitude are top level if they exist
		addIfDefined(mpProfile, "$latitude", postHogUserProperties["$initial_geoip_latitude"]);
		addIfDefined(mpProfile, "$longitude", postHogUserProperties["$initial_geoip_longitude"]);
		addIfDefined(mpProfile, "$latitude", postHogUserProperties["$geoip_latitude"]);
		addIfDefined(mpProfile, "$longitude", postHogUserProperties["$geoip_longitude"]);
		

		// don't send empty profiles
		if (Object.keys(mpProfile[directive]).length <= 1) {
			return null;
		}

		return mpProfile;
	};
}


/*
----
RANDOM
----
*/


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

// mapping of posthog person fields to mixpanel profile fields
// posthog: https://posthog.com/docs/product-analytics/person-properties
// mixpanel: https://docs.mixpanel.com/docs/data-structure/property-reference/default-properties
const postHogMixProfilePairs = [
	// --- Standard User Data (Manually set in PostHog, but standard keys) ---
	// ["created_at", "$created"],    // PostHog usually uses "created_at" in the DB, Mixpanel uses "$created"
	["name", "$name"],             // Convention: Not auto-captured
	["first_name", "$first_name"], // Convention: Not auto-captured
	["last_name", "$last_name"],   // Convention: Not auto-captured
	["email", "$email"],           // Convention: Not auto-captured
	["phone", "$phone"],           // Convention: Not auto-captured
	["avatar", "$avatar"],         // Convention: Not auto-captured

	// --- GeoIP / Location Data (Auto-captured by PostHog) ---
	["$geoip_city_name", "$city"],
	["$geoip_subdivision_1_name", "$region"], // Maps State/Province to Region
	["$geoip_country_code", "$country_code"],
	[null, "$locale"],             // Not auto-captured on PostHog Person Profile
	[null, "$geo_source"],         // Not a standard PostHog concept
	["$geoip_time_zone", "$timezone"],

	// --- Tech / System Data ---
	["$os", "$os"],
	["$browser", "$browser"],
	["$browser_version", "$browser_version"],

	// --- Attribution / First Touch (Auto-captured by PostHog) ---
	["$initial_referrer", "$initial_referrer"],
	["$initial_referring_domain", "$initial_referring_domain"],
	["$initial_utm_source", "initial_utm_source"],
	["$initial_utm_medium", "initial_utm_medium"],
	["$initial_utm_campaign", "initial_utm_campaign"],
	["$initial_utm_content", "initial_utm_content"],
	["$initial_utm_term", "initial_utm_term"], // Note: Not in your doc dump, but is standard PostHog capture

	// --- Mobile Hardware Data ---
	// PostHog captures these on EVENTS, not usually on the PERSON profile by default.
	// Use specific event property mapping if needed, otherwise leave null for Profile sync.
	[null, "$android_manufacturer"],
	[null, "$android_brand"],
	[null, "$android_model"],
	[null, "$ios_device_model"]
];

function addIfDefined(target, key, value) {
	if (value != null) target[key] = value;
}

module.exports = {
	postHogEventsToMp,
	postHogPersonToMpProfile,
	postHogMixPairs
};


