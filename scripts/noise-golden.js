#! /usr/bin/env node
// Regenerates the filterMixpanelNoise golden fixture from tests/fixtures/noise/input.ndjson.
// Writes expected.ndjson (destination lines, in output order) and expected-counts.json.
// The Go worker copies all three files; compare records as parsed JSON, not as strings.
// Run: node scripts/noise-golden.js
const fs = require('fs');
const os = require('os');
const path = require('path');
const mp = require('../index.js');

const dir = path.join(__dirname, '..', 'tests', 'fixtures', 'noise');

async function main() {
	const dest = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'noise-golden-')), 'out.ndjson');
	const summary = await mp({ token: 'dummy' }, path.join(dir, 'input.ndjson'), {
		recordType: 'event',
		streamFormat: 'jsonl',
		destination: dest,
		destinationOnly: true,
		fixData: false,
		removeNulls: false
	});
	const lines = fs.readFileSync(dest, 'utf8').split('\n').filter(Boolean);
	fs.writeFileSync(path.join(dir, 'expected.ndjson'), lines.join('\n') + '\n');
	const counts = { records: lines.length, noise_removed: summary.noise_removed, noise_seen: summary.noise_seen };
	fs.writeFileSync(path.join(dir, 'expected-counts.json'), JSON.stringify(counts, null, '\t') + '\n');
	console.log(JSON.stringify(counts, null, 2));
}

main().catch(err => {
	console.error(err);
	process.exit(1);
});
