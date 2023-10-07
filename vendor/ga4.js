const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
const murmurhash = require("murmurhash");
const stringify = require("json-stable-stringify");
dayjs.extend(utc);


/*
----
TRANSFORMS
----
*/

/**
 * returns a function that transforms an ga4 event into a mixpanel event
 * @param  {import('../index').ga4Opts} options
 */
function gaEventsToMp(options) {
	const { user_id = "user_id", device_id = "user_pseudo_id", insert_id_col = "" } = options;

	return function transform(gaEvent) {
		const mixpanelEvent = {
			event: gaEvent.event_name,
			properties: {
				$device_id: gaEvent[device_id] || "",
			}
		};

		// micro => mili for time
		const milliseconds = BigInt(gaEvent.event_timestamp) / 1000n;
		const mp_time = Number(milliseconds);
		mixpanelEvent.properties.time = mp_time;

		//insert id creation
		if (insert_id_col && gaEvent[insert_id_col]) {
			mixpanelEvent.properties.$insert_id = gaEvent[insert_id_col];
		}
		else {
			//create insert id from event
			const insert_id = murmurhash.v3(stringify(mixpanelEvent)).toString();
			mixpanelEvent.properties.$insert_id = insert_id;
		}

		//label
		mixpanelEvent.properties.$source = `ga4-to-mixpanel`;

		//canonical id resolution
		if (gaEvent[user_id]) mixpanelEvent.properties.$user_id = gaEvent[user_id];

		const gaCustomParams = flattenGAParams([...gaEvent.event_params]);
		const gaDefaults = GAtoMixpanelDefaults(gaEvent);
		delete gaEvent.event_params;
		delete gaEvent.user_properties;

		//flatten event_params


		//grab all of it!
		mixpanelEvent.properties = {
			...gaEvent,
			...gaDefaults,
			...gaCustomParams,
			...mixpanelEvent.properties
		};

		return mixpanelEvent;
	};
}

/**
 * returns a function that transforms an amplitude user into a mixpanel user
 * @param  {import('../index').ga4Opts} options
 */
function gaUserToMp(options) {
	const { user_id = "user_id" } = options;

	return function transform(gaEvent) {
		const userProps = flattenGAParams(gaEvent.user_properties);

		//skip empty props
		if (Object.keys(userProps).length === 0) return {};

		let distinct_id;
		//canonical id resolution
		if (gaEvent?.user_properties?.[user_id]) distinct_id = gaEvent.user_properties[user_id];
		if (gaEvent[user_id]) distinct_id = gaEvent[user_id];

		//skip no user_id
		if (!distinct_id) return {};

		const defaults = GAtoMixpanelDefaults(gaEvent);


		const mixpanelProfile = {
			$distinct_id: distinct_id,
			$ip: 0,
			$set: {
				...defaults,
				...userProps
			}
		};

		return mixpanelProfile;
	};
}

/**
 * returns a function that transforms an amplitude group into a mixpanel group
 * @param  {import('../index').ga4Opts} options
 */
function gaGroupsToMp(options) {
	// const { user_id, group_keys } = options;

	return function transform(ampEvent) {
		const groupProps = ampEvent.group_properties;

		//skip empty + no user_id
		if (JSON.stringify(groupProps) === "{}") return {};
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


function GAtoMixpanelDefaults(gaEvent) {
	const result = {};

	if (gaEvent?.geo?.city) result.$city = gaEvent.geo.city;
	if (gaEvent?.geo?.country) result.mp_country_code = gaEvent.geo.country;
	if (gaEvent?.geo?.region) result.$region = gaEvent.geo.region;
	if (gaEvent?.device?.operating_system) result.$os = gaEvent.device.operating_system;
	if (gaEvent?.device?.operating_system_version) result.$os_version = gaEvent.device.operating_system_version;
	if (gaEvent?.device?.mobile_marketing_name) result.$brand = gaEvent.device.mobile_marketing_name;
	if (gaEvent?.device?.mobile_model_name) result.$model = gaEvent.device.mobile_model_name;
	if (gaEvent?.device?.mobile_brand_name) result.$manufacturer = gaEvent.device.mobile_brand_name;
	if (gaEvent?.web_info?.browser) result.$browser = gaEvent.web_info.browser;
	if (gaEvent?.web_info?.browser_version) result.$browser_version = gaEvent.web_info.browser_version;
	if (gaEvent?.app_info?.version) result.$app_version_string = gaEvent.app_info.version;

	return result;

}

/**
 * @param  {{key: string, value: object}[]} gaParams
 */
function flattenGAParams(gaParams) {
	const result = {};
	for (let param of gaParams) {
		if (param.key && param.value) {
			const actualValueKey = Object.keys(param.value).filter(a => a.includes('value')).pop();
			if (actualValueKey) result[param.key] = param.value[actualValueKey];
		}
	}

	return result;
}


module.exports = {
	gaEventsToMp,
	gaUserToMp,
	gaGroupsToMp
};