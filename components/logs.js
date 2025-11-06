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
	const fs = require('fs').promises;
	const path = require('path');

	try {
		const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
		const fileName = `${data.recordType}-import-log-${dateTime}.json`;

		const logsDir = path.resolve('./logs');
		await fs.mkdir(logsDir, { recursive: true });

		const filePath = path.join(logsDir, fileName);

		const jsonContent = JSON.stringify(data, null, 2);
		await fs.writeFile(filePath, jsonContent, 'utf8');

		return filePath;
	} catch (error) {
		console.error(`⚠️  Failed to write log file: ${error.message}`);
		return null;
	}
}


module.exports = { logger, writeLogs}