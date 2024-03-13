const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
const { flattenProperties } = require('../components/transforms');


/*
----
TRANSFORMS
----
*/

/**
 * returns a function that transforms an mparticle event into a mixpanel event
 * ? https://docs.mparticle.com/developers/server/json-reference/
 * @param  {import('../index').mparticleOpts} options
 */
function mparticleEventsToMixpanel(options) {
	const { user_id = "customer_id", device_id = "mp_deviceid", insert_id = "event_id" } = options;

	return function transform(mParticleEvents) {
		const { events } = mParticleEvents;
		const knownId = (mParticleEvents?.user_identities?.filter(id => id.identity_type === user_id)[0]?.identity || "").toString();
		const anonId = (mParticleEvents?.user_identities?.filter(id => id.identity_type === device_id)[0]?.identity || mParticleEvents[device_id] || "").toString();
		const inheritedProps = {
			...mParticleEvents.application_info,
			...mParticleEvents.device_info,
			...mParticleEvents.source_info,
			...mParticleEvents.user_attributes,
			identities: mParticleEvents.user_identities,
			batch_id: mParticleEvents.batch_id,
			message_id: mParticleEvents.message_id,
			message_type: mParticleEvents.message_type,
			unique_id: mParticleEvents.unique_id,
			source_request_id: mParticleEvents.source_request_id,
			schema_version: mParticleEvents.schema_version,
			context: flattenProperties()({ properties: mParticleEvents.context }).properties,
		};
		const mixpanelEvents = [];
		// iterate over events
		for (const mParticleEvent of events) {
			//transform each event
			const mixpanelEvent = {
				event: mParticleEvent.event_type,
				properties: {
					$device_id: anonId,
					time: Number(mParticleEvent?.data?.timestamp_unixtime_ms),
					$insert_id: mParticleEvent?.data?.[insert_id],
					$source: `mparticle-to-mixpanel`
				}
			};

			// handle custom event names
			if (mParticleEvent.event_type === "custom_event") mixpanelEvent.event = mParticleEvent?.data?.event_name;

			//canonical id resolution
			if (knownId) mixpanelEvent.properties.$user_id = knownId;
			const customProps = flattenProperties()({ properties: mParticleEvent?.data?.custom_attributes }).properties;
			delete mParticleEvent?.data?.custom_attributes;
			const standardProps = flattenProperties()({ properties: mParticleEvent?.data }).properties;

			//gather all nested things
			mixpanelEvent.properties = {
				...inheritedProps,
				...standardProps,
				...customProps,
				...mixpanelEvent.properties
			};

			mixpanelEvents.push(mixpanelEvent);
		}

		return mixpanelEvents;
	};
}

/**
 * returns a function that transforms an amplitude user into a mixpanel user
 * @param  {import('../index').amplitudeOpts} options
 */
function mParticleUserToMixpanel(options) {
	const { user_id = "customer_id" } = options;

	return function transform(mParticleEvents) {
		const knownId = (mParticleEvents?.user_identities?.filter(id => id.identity_type === user_id)[0]?.identity || "").toString();
		if (!knownId) return {};

		const userProps = mParticleEvents.user_attributes;

		const inheritedProps = {
			...mParticleEvents.application_info,
			...mParticleEvents.device_info,
			identities: mParticleEvents.user_identities,
			mpid: mParticleEvents.mpid,
		};

		//skip empty props
		if (Object.keys(userProps).length === 0) return {};

		const mixpanelProfile = {
			$distinct_id: knownId,
			$set: { ...inheritedProps, ...userProps }
		};

		if (mParticleEvents.ip) mixpanelProfile.$ip = mParticleEvents.ip;


		return mixpanelProfile;
	};
}

/**
 * returns a function that transforms an amplitude group into a mixpanel group
 * @param  {import('../index').mparticleOpts} options
 */
function mParticleGroupToMixpanel(options) {
	const { user_id, device_id } = options;

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
const mParticleMixpanelPairs = [
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
	mparticleEventsToMixpanel,
	mParticleUserToMixpanel,
	mParticleGroupToMixpanel,
	mParticleMixpanelPairs
};