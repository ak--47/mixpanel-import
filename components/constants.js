/**
 * Shared constants for mixpanel-import
 *
 * This file exists to avoid circular dependencies between modules.
 * Keep this file free of require() calls to other internal modules.
 */

// Compression Configuration
const COMPRESSION_CONFIG = {
	GZIP_LEVEL: 6,                   // Default compression level (1-9, 6 is balanced)
	GZIP_WINDOW_BITS: 15,            // Standard gzip window
	GZIP_MEM_LEVEL: 8,               // Memory usage level (1-9)
	GZIP_CHUNK_SIZE: 16 * 1024,      // 16KB chunks for processing
	GZIP_EXTENSIONS: ['.gz', '.gzip'] // File extensions that indicate gzip compression
};

module.exports = {
	COMPRESSION_CONFIG
};
