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
	const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
	const fileDir = u.mkdir('./logs');
	const fileName = `${data.recordType}-import-log-${dateTime}.json`;
	const filePath = `${fileDir}/${fileName}`;
	const file = await u.touch(filePath, data, true);
	// @ts-ignore
	l(`\nfull log written to:\n${file}`);
}


module.exports = { logger, writeLogs}