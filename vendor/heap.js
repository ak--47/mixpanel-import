
const u = require("ak-tools");
const dayjs = require("dayjs");
const utc = require("dayjs/plugin/utc.js");
dayjs.extend(utc);
const md5 = require("md5");
const fs = require("fs");
const path = require("path");
const { exportProfiles } = require('../components/exporters.js');


/**
 * @typedef {Object<string, *>} StringKeyedObject
 * An object with string keys and values of any type.
 */

/**
 * @typedef {Array<StringKeyedObject>} arrObj
 * An array of objects with string keys.
 */

// @ts-ignore
async function getDeviceIdMap(secret) {

	// download all the heap user profiles, from mixpanel
	/** @type {import('mixpanel-import').Creds} */
	const creds = {
		secret: secret || "your-secret",
	};

	if (creds.secret === "your-secret") throw new Error('you need to set your secret in this file');

	/** @type {import('mixpanel-import').Options} */
	const opts = {
		recordType: 'peopleExport',
		verbose: false,
	};
	const jobConfig = require('../components/job.js');
	console.log(`\nDownloading User Profiles from Mixpanel\n`);
	await exportProfiles('./mixpanel-exports', new jobConfig(creds, opts));


	const directoryPath = './mixpanel-exports'; // Adjust this to your directory path

	// List all files in the directory
	const files = fs.readdirSync(directoryPath);

	// Filter JSON files and read them into an array
	const jsonArray = files
		.filter(file => path.extname(file) === '.json')
		.map(file => {
			const filePath = path.join(directoryPath, file);
			const fileContent = fs.readFileSync(filePath, 'utf8');
			return JSON.parse(fileContent);
		});

	const allUsers = jsonArray.flat().map(user => {
		return {
			distinct_id: user.$distinct_id,
			id: user.$properties.id?.split(',')?.[1]?.replace(')', ''),
		};
	});

	const outputPath = path.resolve(path.join('./', 'user-device-mappings.json'));
	console.log(`\nWriting ${allUsers.length} user mappings to ${outputPath}\n`);
	fs.writeFileSync(outputPath, JSON.stringify(allUsers));
	console.log(`\nDeleting temporary files\n`);
	await u.rm(directoryPath);
	console.log('\nDone!\nyou can now use this file to map heap events to user profiles by passing it as the device_id_map_file \n');
	return outputPath;

}





/*
----
TRANSFORMS
----
*/

const heapMpPairs = [
	//heap default to mp default props
	// ? https://help.mixpanel.com/hc/en-us/articles/115004613766-Default-Properties-Collected-by-Mixpanel
	["joindate", "$created"],
	["initial_utm_term", "$initial_utm_term"],
	["initial_utm_source", "$initial_utm_source"],
	["initial_utm_medium", "$initial_utm_medium"],
	["initial_utm_content", "$initial_utm_content"],
	["initial_utm_campaign", "$initial_utm_campaign"],
	["initial_search_keyword", "$initial_search_keyword"],
	["initial_region", "$region"],
	["initial_referrer", "$initial_referrer"],

	["initial_platform", "$os"],
	["initial_browser", "$browser"],

	["app_version", "$app_version_string"],

	["device_brand", "$brand"],
	["device_manufacturer", "$manufacturer"],
	["device_model", "$model"],
	["region", "$region"],
	["initial_city", "$city"],
	["initial_country", "$country_code"],
	["email", "$email"],
	["_email", "$email"],
	["firstName", "$first_name"],
	["lastName", "$last_name"],
	["last_modified", "$last_seen"],
	["Name", "$name"],
	["city", "$city"],
	["country", "$country_code"],
	["ip", "$ip"]
];

/**
 * returns a function that transforms a heap event into a mixpanel event
 * @param  {import('../index').heapOpts} options
 */
function heapEventsToMp(options) {
	const { user_id = "", device_id_file = "" } = options;
	let device_id_map;
	if (device_id_file) {
		device_id_map = buildDeviceIdMap(device_id_file);
	}

	else {
		device_id_map = new Map();
	}
	return function transform(heapEvent) {
		let insert_id;
		if (heapEvent.event_id) insert_id = heapEvent.event_id.toString();
		else insert_id = md5(JSON.stringify(heapEvent));


		//some heap events have user_id, some have a weird tuple under id
		const anon_id = heapEvent?.id?.split(",")?.[1]?.replace(")", ""); //ex: { "id": "(2008543124,4810060720600030)"} ... first # is project_id
		let device_id;
		if (heapEvent.user_id) device_id = heapEvent.user_id.toString();
		else device_id = anon_id.toString();

		if (!device_id) return {};

		// event name
		const eventName = heapEvent.type || heapEvent.object || `unknown action`;

		// time
		const time = dayjs.utc(heapEvent.time).valueOf();
		delete heapEvent.time;

		// props
		const customProps = { ...heapEvent.properties };
		delete heapEvent.properties;

		//template
		const mixpanelEvent = {
			event: eventName,
			properties: {
				$device_id: device_id,
				time,
				$insert_id: insert_id,
				$source: `heap-to-mixpanel`
			}
		};

		//get all custom props + group props + user props
		mixpanelEvent.properties = { ...heapEvent, ...customProps, ...mixpanelEvent.properties };

		//relabel for default pairing
		for (let heapMpPair of heapMpPairs) {
			if (mixpanelEvent.properties[heapMpPair[0]]) {
				mixpanelEvent.properties[heapMpPair[1]] = mixpanelEvent.properties[heapMpPair[0]];
				delete mixpanelEvent.properties[heapMpPair[0]];
			}
		}
		if (!user_id) {
			// if the event has an identity prop, it's a heap $identify event, so set $user_id too
			if (heapEvent.identity) {
				mixpanelEvent.event = "identity association";
				const knownId = heapEvent.identity.toString();
				mixpanelEvent.properties.$device_id = device_id;
				mixpanelEvent.properties.$user_id = knownId;
			}

			// if we have a device_id map, look up the device_id in the map and use the mapped value for $user_id
			else if (device_id_map.size) {
				const knownId = device_id_map.get(device_id) || null;
				if (knownId) {
					mixpanelEvent.properties.$user_id = knownId;
				}
			}
		}

		// use the custom user id if it exists on the event
		if (user_id && heapEvent[user_id]) {
			if (typeof heapEvent[user_id] === "string" || typeof heapEvent[user_id] === "number") {
				mixpanelEvent.properties.$user_id = heapEvent[user_id].toString();
			}
		}

		return mixpanelEvent;
	};
}

/**
 * returns a function that transforms a heap user into a mixpanel profile
 * @param  {import('../index').heapOpts} options
 */
function heapUserToMp(options) {
	const { user_id = "" } = options;
	return function (heapUser) {
		//todo... users might have multiple anon identities... for now we can't support that
		let customId = null;
		// use the custom user id if it exists on the event
		if (user_id && heapUser[user_id]) {
			if (typeof heapUser[user_id] === "string" || typeof heapUser[user_id] === "number") {
				customId = heapUser[user_id].toString();
			}
		}
		const anonId = heapUser.id.split(",")[1].replace(")", "");
		const userId = heapUser.identity;
		if (!userId && !customId) {
			return {}; //no identifiable info; skip profile
		}

		// heapUser.anonymous_heap_uuid = anonId;

		// timestamps
		if (heapUser.last_modified) heapUser.last_modified = dayjs.utc(heapUser.last_modified).toISOString();
		if (heapUser.joindate) heapUser.joindate = dayjs.utc(heapUser.joindate).toISOString();
		if (heapUser.identity_time) heapUser.identity_time = dayjs.utc(heapUser.identity_time).toISOString();

		// props
		const customProps = { ...heapUser.properties };
		delete heapUser.properties;
		const defaultProps = { ...heapUser };

		//template
		const mixpanelProfile = {
			$distinct_id: customId || userId || anonId,
			$ip: heapUser.initial_ip,
			$set: { ...defaultProps, ...customProps }
		};

		//relabel
		for (let heapMpPair of heapMpPairs) {
			if (mixpanelProfile.$set[heapMpPair[0]]) {
				mixpanelProfile.$set[heapMpPair[1]] = mixpanelProfile.$set[heapMpPair[0]];
				delete mixpanelProfile.$set[heapMpPair[0]];
			}
		}

		return mixpanelProfile;
	};
}


/**
 * returns a function that transforms a heap group into a mixpanel group
 * @param  {import('../index').heapOpts} options
 */
function heapGroupToMp(options) {
	const { group_keys = [] } = options;
	return function (heapGroup) { };

}

function buildDeviceIdMap(file) {
	if (file) {
		const fileContents = fs.readFileSync(file, "utf-8");
		const data = JSON.parse(fileContents);
		const hashmap = data.reduce((map, item) => {
			map.set(item.id, item.distinct_id);
			return map;
		}, new Map());
		return hashmap;
	}
	else {
		throw new Error("No file provided for device_id_map");
	}
}



// note: heap exports which contain nested objects are DOUBLE escaped;
// we therefore need to fix the string so it's parseable
// @ts-ignore
function heapParseErrorHandler(err, record) {
	let attemptedParse;
	try {
		attemptedParse = JSON.parse(record.replace(/\\\\/g, '\\'));
	}
	catch (e) {
		attemptedParse = {};
	}
	return attemptedParse;
}



module.exports = {
	heapEventsToMp,
	heapUserToMp,
	heapGroupToMp,
	getDeviceIdMap,
	buildDeviceIdMap,
	heapMpPairs,
	heapParseErrorHandler
};