const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
const { flattenProperties } = require('../components/transforms');
const murmurhash = require("murmurhash");

const bad_user_ids = [
	"-1",
	"0",
	"00000000-0000-0000-0000-000000000000",
	"<nil>",
	"]",
	"anon",
	"anonymous",
	"false",
	"lmy47d",
	"n/a",
	"na",
	"nil",
	"none",
	"null",
	"true",
	"undefined",
	"unknown",
	"{}"
];


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
function mParticleEventsToMixpanel(options) {
	const { user_id = ["customer_id"],
		device_id = ["mp_deviceid", "mpid", "session_uuid"],
		insert_id = "event_id",
		user_attributes = false,
		context = false,
		identities = false,
		application_info = true,
		device_info = true,
		source_info = true
	} = options;

	return function transform(mParticleEvents) {
		const { events = [] } = mParticleEvents;
		// Resolve user_id and device_id using fallback pattern
		let knownId = '';
		let anonId = '';

		for (const id of user_id) {
			const foundIdentity = mParticleEvents?.user_identities?.find(identity => identity.identity_type === id);
			if (foundIdentity && foundIdentity.identity) {
				if (bad_user_ids.includes(foundIdentity.identity?.toString())) {
					break;
				}
				if (foundIdentity.identity?.toString()) {
					knownId = foundIdentity.identity?.toString();
					break;
				}

			}
		}

		for (const id of device_id) {
			//look through user_identities
			const foundIdentity = mParticleEvents?.user_identities?.find(identity => identity.identity_type === id);
			if (foundIdentity && foundIdentity.identity) {
				if (foundIdentity.identity?.toString()) {
					anonId = foundIdentity.identity?.toString();
					break;
				}
			}

			// look at top level
			if (mParticleEvents[id]) {
				if (mParticleEvents[id]?.toString()) {
					anonId = mParticleEvents[id]?.toString();
					break;
				}
			}

			// look at event properties
			if (mParticleEvents?.events) {
				const foundIdentity = mParticleEvents?.events?.find(event => event?.data?.[id]);
				if (foundIdentity && foundIdentity?.data?.[id]?.toString()) {
					anonId = foundIdentity?.data?.[id]?.toString();
					break;
				}
			}
		}

		// ! EVENT DEBUGGER
		// if (mParticleEvents?.events?.find(event => event?.data?.event_id === `-3316258325130631688`))  debugger;


		// Initialize with default props
		let inheritedProps = {
			batch_id: mParticleEvents.batch_id,
			message_id: mParticleEvents.message_id,
			message_type: mParticleEvents.message_type,
			unique_id: mParticleEvents.unique_id,
			source_request_id: mParticleEvents.source_request_id,
			schema_version: mParticleEvents.schema_version,
		};

		// Prepare additional properties
		const additionalProps = {
			...(user_attributes ? mParticleEvents.user_attributes : {}),
			...(context ? flattenProperties()({ properties: mParticleEvents.context }).properties : {}),
			...(identities ? { identities: mParticleEvents.user_identities } : {}),
			...(application_info ? mParticleEvents.application_info : {}),
			...(device_info ? mParticleEvents.device_info : {}),
			...(source_info ? mParticleEvents.source_info : {})
		};

		// Combine default and additional properties
		inheritedProps = { ...additionalProps, ...inheritedProps };

		const mixpanelEvents = [];
		// iterate over events
		for (const mParticleEvent of events) {
			const $insert_id = mParticleEvent?.data?.[insert_id];

			const timestamp = mParticleEvent?.data?.timestamp_unixtime_ms;
			//transform each event			
			const mixpanelEvent = {
				event: mParticleEvent.event_type,
				properties: {
					$device_id: anonId, //all events must have device_id
					time: Number(timestamp),
					$source: `mparticle-to-mixpanel`
				}
			};

			// handle insert_id
			if ($insert_id) mixpanelEvent.properties.$insert_id = $insert_id;
			if (!$insert_id) mixpanelEvent.properties.$insert_id = murmurhash.v3([anonId, timestamp, mParticleEvent.event_type].join("-")).toString();

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
 * @param  {import('../index').mparticleOpts} options
 */
function mParticleUserToMixpanel(options) {
	const { user_id = ["customer_id"] } = options;

	return function transform(mParticleEvents) {
		// Resolve user_id using fallback pattern
		let knownId = '';
		for (const id of user_id) {
			const foundIdentity = mParticleEvents?.user_identities?.find(identity => identity.identity_type === id);
			if (foundIdentity && foundIdentity.identity) {
				if (bad_user_ids.includes(foundIdentity.identity?.toString())) break;

				if (foundIdentity.identity?.toString()) {
					knownId = foundIdentity.identity?.toString();
					break;
				}
			}
		}
		if (!knownId) return {};

		const userProps = mParticleEvents.user_attributes;

		const inheritedProps = {
			...mParticleEvents.application_info,
			...mParticleEvents.device_info,
			identities: mParticleEvents.user_identities,
			mpid: mParticleEvents.mpid,
		};

		//skip empty profiles
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
 * 
 * returns a function that transforms an mparticle group into a mixpanel group
 * @param  {import('../index').mparticleOpts} options
 */
function mParticleGroupToMixpanel(options) {
	// const { user_id, device_id } = options;

	return function transform(mParticleEvents) {
		const groupProps = mParticleEvents.group_properties;

		//skip empty + no user_id
		if (Object.keys(groupProps).length === 0) return {};
		if (!mParticleEvents.user_id) return {};

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
	mParticleEventsToMixpanel,
	mParticleUserToMixpanel,
	mParticleGroupToMixpanel,
	mParticleMixpanelPairs
};