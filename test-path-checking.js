/**
 * Quick test to verify improved path checking functionality
 */

const path = require('path');
const fs = require('fs');

// Replicate the checkPath function from parsers.js
function checkPath(filePath) {
	try {
		const resolvedPath = path.resolve(filePath);
		if (!fs.existsSync(resolvedPath)) {
			return { exists: false, isFile: false, isDirectory: false, path: resolvedPath };
		}
		
		const stats = fs.lstatSync(resolvedPath);
		return {
			exists: true,
			isFile: stats.isFile(),
			isDirectory: stats.isDirectory(),
			path: resolvedPath
		};
	} catch (error) {
		// Log error but return safe defaults
		console.warn(`Error checking path ${filePath}:`, error.message);
		return { exists: false, isFile: false, isDirectory: false, path: filePath };
	}
}

// Test cases
const testCases = [
	'./components',           // Existing directory
	'./components/parsers.js', // Existing file
	'./nonexistent',          // Non-existent path
	'./testData',             // Another existing directory
	'/invalid/path/nowhere',  // Absolutely non-existent path
	''                        // Empty string
];

console.log('Testing improved path checking:\n');

testCases.forEach(testPath => {
	const result = checkPath(testPath);
	console.log(`Path: "${testPath}"`);
	console.log(`  Resolved: ${result.path}`);
	console.log(`  Exists: ${result.exists}`);
	console.log(`  Is File: ${result.isFile}`);
	console.log(`  Is Directory: ${result.isDirectory}`);
	console.log('');
});

console.log('✅ Path checking test completed successfully!');
