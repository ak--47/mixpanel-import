const u = require('ak-tools');

/** @typedef {import('./job')} JobConfig */

/**
 * @param  {JobConfig} job
 */
function logger(job) {
	return (message, isTable = false) => {		
		if (job.verbose && isTable) console.table(message);
		else if (job.verbose) console.log(message);
		return;
	};
}

/**
 * @param  {Object} data
 */
async function writeLogs(data) {
	try {
		const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
		const fileDir = u.mkdir('./logs');
		const fileName = `${data.recordType}-import-log-${dateTime}.json`;
		const filePath = `${fileDir}/${fileName}`;
		// Convert object to JSON string for file writing
		const file = await u.touch(filePath, JSON.stringify(data, null, 2), true);
		// @ts-ignore
		console.log(`📝 Log saved: ${filePath}`);
		return filePath;
	} catch (error) {
		console.error(`⚠️  Failed to write log file: ${error.message}`);
		return null;
	}
}


module.exports = { logger, writeLogs}