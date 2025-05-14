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
		v2_compat = true,
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

		if (!mpEventName) debugger;


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
			...remainingPostHogProperties
		} = parsedPostHogProperties;

		const props = {
			time: dayjs.utc(mpTimestamp).valueOf(),
			$source: `posthog-to-mixpanel`,
			$device_id: postHogDistinctId,
		};

		addIfDefined(props, 'ip', mpIp);
		addIfDefined(props, '$city', mpCity);
		addIfDefined(props, '$region', mpCountryCode);
		addIfDefined(props, 'mp_country_code', postHogEvent.country);
		addIfDefined(props, '$insert_id', mpInsertId);
		addIfDefined(props, '$latitude', mpLatitude);
		addIfDefined(props, '$longitude', mpLongitude);



		//v2 compat requires distinct_id; 
		if (v2_compat) {
			props.distinct_id = postHogDistinctId;
		}

		if (personMap.has(postHogDistinctId)) {
			const person_id = personMap.get(postHogDistinctId);
			props.$user_id = person_id;
		}


		const mixpanelEvent = { event: mpEventName, properties: props };
		// //fill in defaults & delete from amp data (if found)
		// for (let ampMixPair of postHogMixPairs) {
		// 	if (postHogEvent[ampMixPair[0]]) {
		// 		mixpanelEvent.properties[ampMixPair[1]] = postHogEvent[ampMixPair[0]];
		// 		delete postHogEvent[ampMixPair[0]];
		// 	}
		// }

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


