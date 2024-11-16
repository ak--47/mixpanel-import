// import parquetjs from '@dsnp/parquetjs';
var parquet = require('@dsnp/parquetjs');
var path = require('path');
const { Readable } = require('stream');
const u = require('ak-tools');

// async function parseParquet(filename) {

// 	// create new ParquetReader that reads from 'fruits.parquet`
// 	let reader = await parquet.ParquetReader.openFile(path.resolve(filename));

// 	// create a new cursor
// 	let cursor = reader.getCursor();
// 	const records = [];

// 	// read all records from the file and print them
// 	let record = null;
// 	while ((record = await cursor.next())) {
// 		// console.log(record);
// 		records.push(record);
// 	}

// 	// close the reader
// 	await reader.close();
// 	return records;

// }



/**
 * Creates an object-mode stream that reads Parquet files row by row.
 * @param {string} filename - Path to the Parquet file.
 * @returns {Readable} - A readable stream emitting Parquet rows.
 */
async function parquetStream(filename) {
	const filePath = path.resolve(filename);
	let reader = null;
	let cursor = null;
	let isReading = false; // To prevent concurrent reads
	reader = await parquet.ParquetReader.openFile(filePath);
	cursor = reader.getCursor();

	const stream = new Readable({
		objectMode: true,
		read() {
			if (isReading) return; // Prevent concurrent reads
			isReading = true;

			(async () => {
				try {
					const record = await cursor.next();
					if (record) {
						this.push(record);
					} else {
						// End of file reached
						await reader.close();
						this.push(null);
					}
				} catch (err) {
					this.destroy(err);
				} finally {
					isReading = false;
				}
			})();
		},
		async destroy(err, callback) {
			try {
				if (reader) {
					await reader.close();
				}
				callback(err);
			} catch (closeErr) {
				callback(closeErr || err);
			}
		},
	});

	return stream;
}

async function main() {
	let cnt = 0;
	const stream = await parquetStream('./testData/parquet/data_0.parquet');
	stream.on('data', (row) => {
		cnt++;
		u.progress([["counter", cnt]]);

	});

	stream.on('end', () => {
		console.log('All rows have been read');
	});

	stream.on('error', (err) => {
		console.error('Error reading Parquet file:', err);
	});
}


// Check if the file is being run directly
if (require.main === module) {
	main().then();

	// parseParquet('./testData/parquet/data_0.parquet').then((res) => {
	// 	debugger;
	// }).catch((e) => {
	// 	debugger;
	// });
}

module.exports = parquetStream;