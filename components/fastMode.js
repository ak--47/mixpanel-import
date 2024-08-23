const akFetch = require('ak-fetch');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc.js');
dayjs.extend(utc);
dayjs.extend(utc);
const NODE_ENV = process.env.NODE_ENV || "none";


async function upload(creds = {}, stream, params = {}, custErrorHandler, monitor) {
	const { token } = creds;
	const { type = "event", groupKey = "", region = "US", concurrency = 5, verbose = false } = params;
	let url = `https://api`;
	switch (region) {
		case 'EU':
			url += `-eu`;
			break;
		case 'IN':
			url += `-in`;
			break;
		case 'US':
			break;
		default:
			break;
	}

	url += `.mixpanel.com`;
	switch (type) {
		case 'event':
			url += `/import`;
			break;
		case 'user':
			url += `/engage`;
			break;
		case 'group':
			url += `/groups`;
			break;
	}

	let project_token;
	if (token) project_token = token;
	else project_token = process.env.MIXPANEL_TOKEN;
	if (!project_token) throw new Error("Missing project token");

	const auth = `Basic ${Buffer.from(project_token + ':', 'binary').toString('base64')}`;
	const headers = {
		'Authorization': auth,
		'Content-Type': 'application/json',
		'Accept': 'application/json',
	};

	try {
		/** @type {import('ak-fetch').BatchRequestConfig} */
		const importOptions = {
			data: stream,
			transform: transformToMixpanel({ type, groupKey, project_token }),
			verbose: verbose,
			url,
			headers,
			searchParams: { verbose: 1, strict: 1, ip: 0 },
			method: 'POST',
			keepalive: true,
			batchSize: 2000,
			maxTasks: concurrency * 2,
			concurrency: concurrency,
			debug: false,
			storeResponses: true,
			retries: 10,
			errorHandler: custErrorHandler,
			responseHandler: monitor,
		};
		if (type === "event") importOptions.searchParams.strict = 0;
		const result = await akFetch(importOptions);

		let rows_imported = 0;
		if (result.responses) {
			const { responses } = result;
			for (const res of responses) {
				if (res?.num_records_imported) rows_imported += res.num_records_imported;
				if (res?.num_good_events) rows_imported += res.num_good_events;
			}
		}

		const { responses, ...rest } = result;
		rest.rows_imported = rows_imported;
		const summary = { meta: rest };
		return summary;
	}
	catch (e) {
		if (NODE_ENV === 'dev') debugger;
	}
}

// ? https://docs.mixpanel.com/docs/data-structure/user-profiles#reserved-profile-properties
const validOperations = ["$set", "$set_once", "$add", "$union", "$append", "$remove", "$unset"];
const specialProps = ["name", "first_name", "last_name", "email", "phone", "avatar", "created", "insert_id", "city", "region", "lib_version", "os", "os_version", "browser", "browser_version", "app_build_number", "app_version_string", "device", "screen_height", "screen_width", "screen_dpi", "current_url", "initial_referrer", "initial_referring_domain", "referrer", "referring_domain", "search_engine", "manufacturer", "brand", "model", "watch_model", "carrier", "radio", "wifi", "bluetooth_enabled", "bluetooth_version", "has_nfc", "has_telephone", "google_play_services", "duration", "country", "country_code"];
const outsideProps = ["distinct_id", "group_id", "token", "group_key", "ip"]; //these are the props that are outside of the $set

/**
 * @param  {{type: 'event' | 'user' | 'group', groupKey : string, project_token: string}} config
 */
function transformToMixpanel(config) {
	let { project_token = "" } = config;
	if (!project_token) project_token = process.env.MIXPANEL_TOKEN || "";
	if (!project_token) throw new Error("Missing project token");
	if (config.type === `event`) {
		return function FixShapeAndAddInsertIfAbsentAndFixTime(event) {
			//wrong shape
			if (!event.properties) {
				event.properties = { ...event };
				//delete properties outside properties
				for (const key in event) {
					if (key !== "properties" && key !== "event") delete event[key];
				}
				delete event.properties.event;
			}

			//fixing time
			if (event.properties.time && Number.isNaN(Number(event.properties.time))) {
				event.properties.time = dayjs.utc(event.properties.time).valueOf();
			}

			//renaming "user_id" to "$user_id"
			if (event.properties.user_id) {
				event.properties.$user_id = event.properties.user_id;
				delete event.properties.user_id;
			}

			//renaming "device_id" to "$device_id"
			if (event.properties.device_id) {
				event.properties.$device_id = event.properties.device_id;
				delete event.properties.device_id;
			}

			//renaming "source" to "$source"
			if (event.properties.source) {
				event.properties.$source = event.properties.source;
				delete event.properties.source;
			}

			for (const key in event.properties) {
				if (specialProps.includes(key)) {
					if (key === "country") {
						event.properties[`mp_country_code`] = event.properties[key];
						delete event.properties[key];
					}
					else {
						event.properties[`$${key}`] = event.properties[key];
						delete event.properties[key];
					}
				}



			}

			return event;
		};
	}

	//for user imports, make sure every record has a $token and the right shape
	if (config.type === `user`) {
		return function addUserTokenIfAbsent(user) {

			//wrong shape; fix it
			if (!validOperations.some(op => Object.keys(user).includes(op))) {
				let uuidKey;
				if (user.$distinct_id) uuidKey = "$distinct_id";
				else if (user.distinct_id) uuidKey = "distinct_id";
				else {

					return {};
				}
				user = { $set: { ...user } };
				user.$distinct_id = user.$set[uuidKey];
				delete user.$set[uuidKey];
				delete user.$set.$token;

				//deal with mp export shape
				//? https://developer.mixpanel.com/reference/engage-query
				if (typeof user.$set?.$properties === "object") {
					user.$set = { ...user.$set.$properties };
					delete user.$set.$properties;
				}
			}

			//catch missing token
			if (!user.$token) user.$token = project_token;

			//rename special props
			for (const key in user) {
				if (typeof user[key] === "object") {
					for (const prop in user[key]) {
						if (specialProps.includes(prop)) {
							if (prop === "country" || prop === "country_code") {
								user[key][`$country_code`] = user[key][prop].toUpperCase();
								delete user[key][prop];
							}
							else {
								user[key][`$${prop}`] = user[key][prop];
								delete user[key][prop];
							}
						}

						if (outsideProps.includes(prop)) {
							user[`$${prop}`] = user[key][prop];
							delete user[key][prop];
						}
					}
				}
				else {
					if (outsideProps.includes(key)) {
						user[`$${key}`] = user[key];
						delete user[key];
					}
				}
			}

			return user;
		};
	}

	//for group imports, make sure every record has a $token and the right shape
	if (config.type === `group`) {
		return function addGroupKeysIfAbsent(group) {
			//wrong shape; fix it
			if (!(group.$set || group.$set_once || group.$add || group.$union || group.$append || group.$remove || group.$unset)) {
				let uuidKey;
				if (group.$distinct_id) uuidKey = "$distinct_id";
				else if (group.distinct_id) uuidKey = "distinct_id";
				else if (group.$group_id) uuidKey = "$group_id";
				else if (group.group_id) uuidKey = "group_id";
				else {

					return {};
				}
				group = { $set: { ...group } };
				group.$group_id = group.$set[uuidKey];
				delete group.$set[uuidKey];
				delete group.$set.$group_id;
				delete group.$set.$token;
			}

			//catch missing token
			if (!group.$token) group.$token = project_token;

			//catch group key
			if (!group.$group_key && config.groupKey) group.$group_key = config.groupKey;

			//rename special props
			for (const key in group) {
				if (typeof group[key] === "object") {
					for (const prop in group[key]) {
						if (specialProps.includes(prop)) {
							group[key][`$${prop}`] = group[key][prop];
							delete group[key][prop];
						}

						if (outsideProps.includes(prop)) {
							group[`$${prop}`] = group[key][prop];
							delete group[key][prop];
						}
					}
				}
				else {
					if (outsideProps.includes(key)) {
						group[`$${key}`] = group[key];
						delete group[key];
					}
				}
			}

			return group;
		};
	}

	return noop;
}


const noop = a => a;

module.exports = upload;
