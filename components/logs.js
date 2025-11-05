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
		// Generate timestamped filename
		const dateTime = new Date().toISOString().split('.')[0].replace('T', '--').replace(/:/g, ".");
		const fileName = `${data.recordType}-import-log-${dateTime}.json`;

		// Ensure logs directory exists (recursive: true creates parent dirs if needed)
		const logsDir = path.resolve('./logs');
		await fs.mkdir(logsDir, { recursive: true });

		// Build full file path
		const filePath = path.join(logsDir, fileName);

		// Write pretty-printed JSON with 2 space indentation
		const jsonContent = JSON.stringify(data, null, 2);
		await fs.writeFile(filePath, jsonContent, 'utf8');

		return filePath;
	} catch (error) {
		console.error(`⚠️  Failed to write log file: ${error.message}`);
		return null;
	}
}


module.exports = { logger, writeLogs}