const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
const murmurhash = require("murmurhash");
// const stringify = require("json-stable-stringify");
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
	const {
		user_id = "user_id",
		device_id = "user_pseudo_id",
		insert_id_col = "",
		set_insert_id = true,
		insert_id_tup = ["event_name", "user_pseudo_id", "event_bundle_sequence_id"],
		time_conversion = "seconds"
	} = options;


	if (!Array.isArray(insert_id_tup)) throw new Error("insert_id_tup must be an array");


	return function transform(gaEvent) {
		const mixpanelEvent = {
			event: gaEvent.event_name,
			properties: {
				$device_id: gaEvent[device_id] || "",
			}
		};

		if (time_conversion === "seconds" || time_conversion === "s") {
			// micro => seconds 
			const milliseconds = BigInt(gaEvent.event_timestamp) / 1000000n;
			const mp_time = Number(milliseconds);
			mixpanelEvent.properties.time = mp_time;
		}

		if (time_conversion === "milliseconds" || time_conversion === "ms") {
			// micro => milliseconds
			const milliseconds = BigInt(gaEvent.event_timestamp) / 1000n;
			const mp_time = Number(milliseconds);
			mixpanelEvent.properties.time = mp_time;
		}

		//insert id creation
		// see: https://stackoverflow.com/a/75894260/4808195
		if (set_insert_id) {
			if (insert_id_col && gaEvent[insert_id_col]) {
				mixpanelEvent.properties.$insert_id = gaEvent[insert_id_col];
			}
			else {
				const event_id_tuple = [];
				for (const identifier of insert_id_tup) {
					if (gaEvent[identifier]) event_id_tuple.push(gaEvent[identifier]);
				}
				const event_id = event_id_tuple.join("-");
				if (event_id) {
					const insert_id = murmurhash.v3(event_id).toString();
					mixpanelEvent.properties.$insert_id = insert_id;
					mixpanelEvent.properties.event_id = event_id;
				}
			}
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

	return function transform(gaEvent) {
		const groupProps = gaEvent.group_properties;

		//skip empty + no user_id
		if (JSON.stringify(groupProps) === "{}") return {};
		if (!gaEvent.user_id) return {};

		const mixpanelGroup = {
			$group_key: null, 
			$group_id: null, 
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

/**
 * map mixpanel defaults to GA4 export schema
 * ? https://docs.mixpanel.com/docs/data-structure/property-reference/default-properties
 * ? https://support.google.com/analytics/answer/7029846?hl=en
 * @param  {Object} gaEvent
 */
function GAtoMixpanelDefaults(gaEvent) {
	const result = {};

	// Geographic properties (Mixpanel default properties)
	if (gaEvent?.geo?.city) result.$city = gaEvent.geo.city;
	if (gaEvent?.geo?.country) result.mp_country_code = gaEvent.geo.country;
	if (gaEvent?.geo?.region) result.$region = gaEvent.geo.region;

	// Operating System properties (Mixpanel default properties)
	if (gaEvent?.device?.operating_system) result.$os = gaEvent.device.operating_system;
	if (gaEvent?.device?.operating_system_version) result.$os_version = gaEvent.device.operating_system_version;

	// Browser properties (Mixpanel default properties - web only)
	if (gaEvent?.device?.web_info?.browser) result.$browser = gaEvent.device.web_info.browser;
	if (gaEvent?.device?.web_info?.browser_version) result.$browser_version = gaEvent.device.web_info.browser_version;

	// Mobile device properties (Mixpanel default properties)
	if (gaEvent?.device?.mobile_brand_name) result.$manufacturer = gaEvent.device.mobile_brand_name;
	if (gaEvent?.device?.mobile_marketing_name) result.$brand = gaEvent.device.mobile_marketing_name;
	if (gaEvent?.device?.mobile_model_name) result.$model = gaEvent.device.mobile_model_name;

	// App properties (Mixpanel default properties)
	if (gaEvent?.app_info?.version) result.$app_version_string = gaEvent.app_info.version;

	// Device category (Mixpanel default property - web and unity)
	if (gaEvent?.device?.category) result.$device = gaEvent.device.category;

	// Current URL (Mixpanel default property - web only)
	// Extract page_location from event_params
	const pageLocationParam = gaEvent?.event_params?.find(param => param.key === 'page_location');
	if (pageLocationParam?.value?.string_value) {
		result.$current_url = pageLocationParam.value.string_value;
	} else if (gaEvent?.device?.web_info?.hostname) {
		result.$current_url = gaEvent.device.web_info.hostname;
	}

	// Platform detection for mp_lib (Mixpanel default property)
	if (gaEvent?.platform) {
		switch (gaEvent.platform.toLowerCase()) {
			case 'web':
				result.mp_lib = 'web';
				break;
			case 'android':
				result.mp_lib = 'android';
				break;
			case 'ios':
				result.mp_lib = 'iphone';
				break;
			case 'unity':
				result.mp_lib = 'unity';
				break;
		}
	}

	// Library version (Mixpanel default property)
	result.$lib_version = 'ga4-export';

	// UTM Parameters (Mixpanel default properties - web only)
	if (gaEvent?.collected_traffic_source?.manual_source) {
		result.utm_source = gaEvent.collected_traffic_source.manual_source;
	}
	if (gaEvent?.collected_traffic_source?.manual_medium) {
		result.utm_medium = gaEvent.collected_traffic_source.manual_medium;
	}
	if (gaEvent?.collected_traffic_source?.manual_campaign_name) {
		result.utm_campaign = gaEvent.collected_traffic_source.manual_campaign_name;
	}
	if (gaEvent?.collected_traffic_source?.manual_term) {
		result.utm_term = gaEvent.collected_traffic_source.manual_term;
	}
	if (gaEvent?.collected_traffic_source?.manual_content) {
		result.utm_content = gaEvent.collected_traffic_source.manual_content;
	}

	return result;
}




function GAtoMixpanelDefaultsOld(gaEvent) {
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
	if (gaEvent?.page_location) result.$current_url = gaEvent.page_location;

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