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
 * returns a function that transforms a mixpanel event in cloud storage into a mixpanel event over API 
 * @param  {import('../index').amplitudeOpts} options
 */
function mixpanelEventsToMixpanel(options) {
	const { v2_compat = true } = options;

	return function transform(mpEvent) {
		// Direct property access is faster than destructuring
		const finalProperties = { ...mpEvent.properties };

		// Only add properties that exist (avoid extra checks)
		if (mpEvent.device_id) finalProperties.$device_id = mpEvent.device_id;
		if (mpEvent.distinct_id) finalProperties.distinct_id = mpEvent.distinct_id;
		if (mpEvent.insert_id) finalProperties.$insert_id = mpEvent.insert_id;
		if (mpEvent.time) finalProperties.time = mpEvent.time;
		if (mpEvent.user_id) finalProperties.$user_id = mpEvent.user_id;

		return {
			event: mpEvent.event_name || "unnamed",
			properties: finalProperties
		};
	};
}










module.exports = {
	mixpanelEventsToMixpanel,

};