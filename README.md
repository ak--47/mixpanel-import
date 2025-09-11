# mixpanel-import

## 🤨 tldr;
stream data to mixpanel... quickly. support for events, user/group profiles, lookup tables, annotations, scd. all of it.

use the UI, the CLI, or include it as a module in your pipeline. we have built-in recipes for different vendor formats, performant transform utilities, retries + backoff, monitoring, and more.

don't write your own ETL. use this:

![E.T.L Interface - Extract Transform Load data into Mixpanel](https://aktunes.neocities.org/mp-import.gif)
**note:** this tool is designed for batch data imports and migrations. for real-time tracking in web applications, you want [the official Mixpanel JavaScript SDK](https://github.com/mixpanel/mixpanel-js).

## 🎯 Quick Start (Recommended)

**The fastest way to get started is with the web interface:**

```bash
npx mixpanel-import --ui
```

This opens a browser-based interface where you can:
- 📁 **Drag & drop files** or connect to **Google Cloud Storage** and **Amazon S3**
- 👁️ **Preview your data** before importing 
- 🔧 **Write custom transforms** with a code editor and live preview
- ⚡ **Test everything** with dry runs before importing
- 💻 **Generate CLI commands** for automation
- 📊 **Track progress** with real-time feedback

**Supported file formats:** JSON, JSONL, CSV, Parquet (including `.gz` compressed versions with automatic detection)

### UI Ships Two Powerful Tools:

#### 🔄 **E.T.L** - Import data into Mixpanel
Perfect for bringing data from files, other analytics platforms, or databases into Mixpanel. Includes advanced data processing, filtering, and transformation capabilities.

#### ⬇️ **L.T.E** - Export data from Mixpanel  
Extract events, profiles, and more from Mixpanel projects. Great for data migrations, backups, or moving data between projects.

---

## 🛠️ Other Ways to Use mixpanel-import

### 💻 Command Line Interface (CLI)

```bash
# Import a file
npx mixpanel-import ./mydata.json --token your-project-token

# Import from cloud storage
npx mixpanel-import gs://bucket/file.json --token your-project-token
npx mixpanel-import s3://bucket/file.json --token your-project-token --s3Region us-east-1

# Import with custom transform
npx mixpanel-import ./data.csv --token your-token --vendor amplitude

# Get help and see all options
npx mixpanel-import --help
```

### 🔌 Programmatic Usage (Node.js)

```bash
npm install mixpanel-import
```

```javascript
const mp = require('mixpanel-import');

// Basic import
const results = await mp(
  { token: 'your-project-token' },    // credentials
  './data.json',                      // data source
  { recordType: 'event' }             // options
);

console.log(`Imported ${results.success} events!`);
```

---

## 🎯 What Can You Import?

| Type | Description | Use Cases |
|------|-------------|-----------|
| **Events** | User actions and behaviors | Page views, clicks, purchases, custom events |
| **User Profiles** | Individual user attributes | Names, emails, subscription status, preferences |
| **Group Profiles** | Company/organization data | Account info, team settings, organization properties |
| **Lookup Tables** | Reference data for reports | Product catalogs, campaign mapping, metadata |

---

## 📁 Supported Data Sources

### 📂 **Local Files**
- Drag & drop in the web UI
- CLI: `npx mixpanel-import ./myfile.json`
- Programmatic: `await mp(creds, './myfile.json')`

### ☁️ **Google Cloud Storage**
- Web UI: Paste `gs://` URLs directly
- CLI: `npx mixpanel-import gs://bucket/file.json`
- Supports all formats including compressed files

### 🪣 **Amazon S3**
- Web UI: Enter `s3://` URLs with credentials
- CLI: `npx mixpanel-import s3://bucket/file.json --s3Region us-east-1`
- Requires S3 credentials (`access key`, `secret`, `region`)

### 💼 **Common Vendor Formats**
Built-in transforms for importing from:
- **Amplitude** - Events and user properties
- **Heap** - Events and user profiles  
- **Google Analytics 4** - Events and custom dimensions
- **PostHog** - Events and person profiles
- **Adobe Analytics** - Events and visitor data
- **Pendo** - Feature usage and account data
- **mParticle** - Events and user attributes

### 📊 **Mixpanel-to-Mixpanel**
- Export data from one project and import to another
- Migrate between regions (US ↔ EU ↔ India)
- Copy data for testing environments

---

## 📦 Gzip File Format Support

mixpanel-import provides comprehensive support for gzipped files with **automatic detection** and **manual override** options:

### 🔍 **Automatic Detection**
Files ending with `.gz` are automatically detected and decompressed:
- `events.json.gz` → processed as gzipped JSON
- `data.jsonl.gz` → processed as gzipped JSONL  
- `export.csv.gz` → processed as gzipped CSV
- `dataset.parquet.gz` → processed as gzipped Parquet (cloud storage only)

### ⚙️ **Manual Override**
Use the `isGzip` option to force gzip processing regardless of file extension:

```bash
# Force gzip processing on file without .gz extension
npx mixpanel-import compressed-data.json --token your-token --isGzip

# JavaScript API
const results = await mp(
  { token: 'your-token' },
  './compressed-data.json',
  { isGzip: true }
);
```

### 📁 **Supported Combinations**
All standard formats work with gzip compression:

| Format | Local Files | Cloud Storage (GCS/S3) |
|--------|-------------|------------------------|
| `.json.gz` | ✅ Automatic | ✅ Automatic |
| `.jsonl.gz` | ✅ Automatic | ✅ Automatic |
| `.csv.gz` | ✅ Automatic | ✅ Automatic |
| `.parquet.gz` | ❌ Not supported | ✅ Automatic |

### 💡 **Usage Examples**

```bash
# Automatic detection from file extension
npx mixpanel-import events.json.gz --token your-token

# Manual override for custom extensions
npx mixpanel-import compressed.data --token your-token --isGzip

# Cloud storage with gzip support
npx mixpanel-import gs://bucket/data.csv.gz --token your-token
npx mixpanel-import s3://bucket/events.parquet.gz --token your-token --s3Region us-east-1
```

**Note:** Gzipped files are always streamed for memory efficiency and cannot be loaded into memory even for small files.

---

## 🔧 Data Processing Features

### 🛠️ **Automatic Data Fixes**
- **Smart Event Structure**: Converts flat objects into proper Mixpanel event format
- **Timestamp Conversion**: Handles ISO dates, Unix timestamps, and various formats
- **ID Generation**: Creates `$insert_id` for deduplication
- **Type Conversion**: Ensures distinct_ids are strings, fixes data types

### 🧹 **Data Cleaning**
- **Remove Empty Values**: Strip null, empty string, empty arrays/objects
- **JSON Parsing**: Automatically parse stringified JSON in properties
- **Flatten Nested Data**: Convert `{user: {plan: "pro"}}` to `{"user.plan": "pro"}`
- **Property Scrubbing**: Remove sensitive data (PII, passwords, etc.)
- **Deduplication**: Skip identical records using content hashing

### 🎯 **Filtering & Selection**
- **Event Filtering**: Whitelist/blacklist by event names
- **Property Filtering**: Include/exclude by property keys or values
- **Time Range Filtering**: Import only data within specific date ranges
- **Combo Filtering**: Complex rules like "only events with `plan=premium`"
- **Record Limits**: Process only first N records (great for testing)

### 🔄 **Data Transformation**
- **Custom JavaScript**: Write transform functions with full access to each record
- **Vendor Transforms**: One-click conversion from other analytics platforms
- **Property Aliases**: Rename fields (e.g., `user_id` → `distinct_id`)
- **Global Tags**: Add properties to all records (e.g., `source: "import"`)
- **Record Splitting**: Turn one record into many (e.g., cart → individual events)

---

## ⚡ Performance & Scale

### 🚀 **High-Throughput Processing**
- **Concurrent Requests**: Process multiple batches simultaneously (default: 10 workers)
- **Optimized Batching**: Pack 2000 records or 2MB per request (configurable)
- **Streaming Architecture**: Process files larger than memory without disk storage
- **Gzip Compression**: Reduce bandwidth usage for faster imports (both input file decompression and output compression)

### 📊 **Real-Time Monitoring**
- **Progress Tracking**: Visual progress bars and EPS (events per second) metrics
- **Memory Monitoring**: Track memory usage during large imports
- **Error Handling**: Automatic retries with exponential backoff
- **Results Logging**: Detailed logs of successes, failures, and performance

### 🏗️ **Enterprise Features**
- **Cloud Streaming**: Direct streaming from GCS/S3 without local download
- **Multi-File Support**: Process entire directories or file lists
- **Region Support**: US, EU, and India data residency
- **Service Account Auth**: Secure authentication for production environments

---

## 🎨 Example Use Cases

### 📊 **Migrating from Another Analytics Platform**

```bash
# Amplitude → Mixpanel with web UI
npx mixpanel-import --ui
# Then select "Amplitude" vendor transform and upload your export

# Or via CLI
npx mixpanel-import amplitude_export.json --vendor amplitude --token your-token
```

### 🗃️ **Importing Historical Data**

```bash
# Large CSV file with custom field mapping
npx mixpanel-import events.csv \
  --token your-token \
  --aliases '{"user_id":"distinct_id","event_name":"event","ts":"time"}' \
  --fixData \
  --fixTime
```

### ☁️ **Processing Cloud Storage Data**

```bash
# Stream from Google Cloud Storage
npx mixpanel-import gs://analytics-exports/events.jsonl.gz --token your-token

# Multiple S3 files with credentials
npx mixpanel-import s3://data-lake/2024/01/*.parquet \
  --s3Key AKIA... \
  --s3Secret xxxx \
  --s3Region us-west-2 \
  --token your-token
```

### 🔄 **Data Quality & Testing**

```bash
# Test with first 1000 records
npx mixpanel-import large_file.json --token your-token --maxRecords 1000 --dryRun

# Clean and dedupe data
npx mixpanel-import messy_data.json \
  --token your-token \
  --removeNulls \
  --dedupe \
  --scrubProps "email,phone,ssn"
```

---

## 📚 Complete Options Reference

### 🔐 **Authentication Options**

| Option | Type | Description |
|--------|------|-------------|
| `token` | `string` | Project token (required for events, users, groups) |
| `secret` | `string` | API secret (legacy authentication) |
| `acct` | `string` | Service account username (recommended) |
| `pass` | `string` | Service account password (recommended) |
| `project` | `string/number` | Project ID (required for service accounts) |
| `groupKey` | `string` | Group key for group profile imports |
| `lookupTableId` | `string` | Lookup table ID for table imports |

### ⚙️ **Core Import Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `recordType` | `string` | `"event"` | Type of data: `event`, `user`, `group`, `table` |
| `region` | `string` | `"US"` | Data residency: `US`, `EU`, `IN` |
| `workers` | `number` | `10` | Number of concurrent HTTP requests |
| `recordsPerBatch` | `number` | `2000` | Records per API request (max 2000 for events) |
| `bytesPerBatch` | `number` | `2000000` | Max bytes per request (2MB) |
| `maxRetries` | `number` | `10` | Retry attempts for failed requests |
| `compress` | `boolean` | `false` | Enable gzip compression (events only) |
| `compressionLevel` | `number` | `6` | Gzip compression level (0-9) |
| `isGzip` | `boolean` | `false` | Force gzip decompression (overrides extension detection) |

### 🛠️ **Data Processing Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fixData` | `boolean` | `true` | Auto-fix data shape and common issues |
| `fixTime` | `boolean` | `false` | Convert timestamps to Unix milliseconds |
| `removeNulls` | `boolean` | `false` | Remove null/empty values from properties |
| `flattenData` | `boolean` | `false` | Flatten nested objects with dot notation |
| `fixJson` | `boolean` | `false` | Parse stringified JSON in properties |
| `dedupe` | `boolean` | `false` | Remove duplicate records using content hash |
| `strict` | `boolean` | `true` | Validate data and fail fast on errors |
| `scrubProps` | `string[]` | `[]` | Property names to remove from all records |

### 🎯 **Filtering Options**

| Option | Type | Description |
|--------|------|-------------|
| `eventWhitelist` | `string[]` | Only import these event names |
| `eventBlacklist` | `string[]` | Skip these event names |
| `propKeyWhitelist` | `string[]` | Only import records with these property keys |
| `propKeyBlacklist` | `string[]` | Skip records with these property keys |
| `propValWhitelist` | `string[]` | Only import records with these property values |
| `propValBlacklist` | `string[]` | Skip records with these property values |
| `epochStart` | `number` | Skip records before this Unix timestamp |
| `epochEnd` | `number` | Skip records after this Unix timestamp |
| `maxRecords` | `number` | Stop processing after N records |

### 🔄 **Transform Options**

| Option | Type | Description |
|--------|------|-------------|
| `transformFunc` | `function` | Custom JavaScript transform function |
| `vendor` | `string` | Built-in transform: `amplitude`, `heap`, `ga4`, `adobe`, `pendo`, `mparticle`, `posthog` |
| `vendorOpts` | `object` | Options for vendor transforms |
| `aliases` | `object` | Rename properties: `{"old_name": "new_name"}` |
| `tags` | `object` | Add properties to all records: `{"source": "import"}` |
| `timeOffset` | `number` | Add/subtract hours from timestamps |
| `insertIdTuple` | `string[]` | Generate `$insert_id` from these columns |

### ☁️ **Cloud Storage Options**

| Option | Type | Description |
|--------|------|-------------|
| `gcpProjectId` | `string` | Google Cloud project ID |
| `s3Key` | `string` | AWS S3 access key ID |
| `s3Secret` | `string` | AWS S3 secret access key |
| `s3Region` | `string` | AWS S3 region (required for S3 access) |

### 📊 **Output & Logging Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `verbose` | `boolean` | `true` | Show detailed progress information |
| `showProgress` | `boolean` | `false` | Show progress bar (when verbose is false) |
| `logs` | `boolean` | `false` | Save detailed logs to `./logs/` directory |
| `where` | `string` | `"./"` | Directory for logs and exported files |
| `writeToFile` | `boolean` | `false` | Write transformed data to file instead of Mixpanel |
| `outputFilePath` | `string` | - | Path for transformed data output |
| `dryRun` | boolean | `false` | Transform data without sending to Mixpanel |

### 🚀 **Performance Options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `forceStream` | `boolean` | `false` | Always stream files (don't load into memory) |
| `streamFormat` | `string` | `"jsonl"` | Override format detection: `json`, `jsonl`, `csv`, `parquet` |
| `transport` | `string` | `"got"` | HTTP client: `got` or `undici` |
| `http2` | `boolean` | `false` | Use HTTP/2 (experimental) |
| `abridged` | `boolean` | `false` | Shorter response logs (errors only) |

### 📈 **Export Options** (for L.T.E tool)

| Option | Type | Description |
|--------|------|-------------|
| `start` | `string` | Start date for exports (YYYY-MM-DD) |
| `end` | `string` | End date for exports (YYYY-MM-DD) |
| `whereClause` | `string` | Mixpanel where clause for filtering |
| `limit` | `number` | Maximum records to export |
| `cohortId` | `string/number` | Cohort ID for profile exports |
| `dataGroupId` | `string` | Data group ID for group profile exports |

### 🔬 **Advanced Options**

| Option | Type | Description |
|--------|------|-------------|
| `parseErrorHandler` | `function` | Custom function to handle parsing errors |
| `responseHandler` | `function` | Custom function to handle API responses |
| `keepBadRecords` | `boolean` | Include failed records in results |
| `dimensionMaps` | `array` | External lookup tables for transforms |
| `scdLabel` | `string` | Label for SCD (Slowly Changing Dimension) imports |
| `scdKey` | `string` | Property name for SCD values |
| `scdType` | `string` | Data type for SCD: `string`, `number`, `boolean` |

---

## 🎓 Transform Function Examples

The `transformFunc` option lets you write custom JavaScript to modify each record:

### ✅ **Basic Transform**
```javascript
function transform(record) {
  // Add a custom property
  record.source = 'my-import';
  
  // Convert timestamp
  if (record.timestamp) {
    record.time = new Date(record.timestamp).getTime();
  }
  
  // Rename property
  if (record.user_id) {
    record.distinct_id = record.user_id;
    delete record.user_id;
  }
  
  return record; // Always return the record
}
```

### 🚫 **Filtering Records**
```javascript
function transform(record) {
  // Skip records without required fields
  if (!record.event || !record.distinct_id) {
    return {}; // Empty object = skip this record
  }
  
  // Only import premium users
  if (record.plan !== 'premium') {
    return {};
  }
  
  return record;
}
```

### 🔄 **Splitting Records**
```javascript
function transform(record) {
  // Turn shopping cart into individual events
  if (record.cart_items && Array.isArray(record.cart_items)) {
    return record.cart_items.map(item => ({
      event: 'Product Added',
      properties: {
        distinct_id: record.user_id,
        product_name: item.name,
        price: item.price,
        quantity: item.quantity
      }
    }));
  }
  
  return record;
}
```

---

## 🔧 Authentication Examples

### 🎯 **Service Account (Recommended)**
```javascript
const creds = {
  acct: 'service-account@yourorg.com',
  pass: 'your-service-account-password', 
  project: 'your-project-id'
};
```

### 🔑 **Project Token (Simple)**
```javascript
const creds = {
  token: 'your-project-token'
};
```

### 🏢 **Group Profiles**
```javascript
const creds = {
  token: 'your-project-token',
  groupKey: 'company_id' // Your group analytics key
};
```

### 📋 **Lookup Tables**
```javascript
const creds = {
  acct: 'service-account@yourorg.com',
  pass: 'your-service-account-password',
  project: 'your-project-id',
  lookupTableId: 'your-lookup-table-id'
};
```

---

## 🌍 Environment Variables

Set credentials and options via environment variables:

```bash
# Service Account Authentication
export MP_ACCT="service-account@yourorg.com"
export MP_PASS="your-service-account-password"
export MP_PROJECT="your-project-id"

# Or Token Authentication  
export MP_TOKEN="your-project-token"

# Optional Settings
export MP_TYPE="event"
export MP_GROUP_KEY="company_id"
export MP_TABLE_ID="your-lookup-table-id"

# Cloud Storage
export S3_KEY="your-s3-access-key"
export S3_SECRET="your-s3-secret-key"
export S3_REGION="us-east-1"

# Then run without credentials
npx mixpanel-import ./data.json
```

---

## 🔄 Advanced Workflows

### 📊 **Export → Transform → Import**
```bash
# 1. Export events from source project
npx mixpanel-import --ui
# Use L.T.E tool to export events

# 2. Transform and import to destination
npx mixpanel-import exported_events.json \
  --token dest-project-token \
  --transformFunc './my-transform.js' \
  --dryRun  # Test first!
```

### 🔄 **Multi-Project Data Migration**
```javascript
const mpImport = require('mixpanel-import');

// Export from source
const sourceData = await mpImport(
  { token: 'source-project-token' },
  null, // No data source for exports
  { recordType: 'export', start: '2024-01-01', end: '2024-12-31' }
);

// Import to destination with transforms
const results = await mpImport(
  { token: 'dest-project-token' },
  sourceData.file,
  { 
    transformFunc: (record) => {
      // Add migration tags
      record.properties.migrated_from = 'old-project';
      record.properties.migration_date = new Date().toISOString();
      return record;
    }
  }
);
```

### 🧪 **Testing Large Datasets**
```bash
# Test with small sample
npx mixpanel-import huge_file.json \
  --token your-token \
  --maxRecords 100 \
  --dryRun \
  --verbose

# Run full import after testing
npx mixpanel-import huge_file.json \
  --token your-token \
  --workers 20 \
  --compress \
  --logs
```

---

## 🔍 Troubleshooting

### ❌ **Common Issues**

**"Rate limited" errors**
- Reduce `workers` (try 5 instead of 10)
- Reduce `recordsPerBatch` (try 1000 instead of 2000)

**"Memory" errors**  
- Add `--forceStream` flag
- Reduce `workers` count
- Process files in smaller chunks

**"Authentication" errors**
- Verify project token in Mixpanel project settings
- For service accounts, check username/password/project ID
- Ensure account has import permissions

**"Data format" errors**
- Use `--fixData` flag for automatic corrections
- Check your transform function syntax
- Use `--dryRun` to test without importing

### 📋 **Getting Help**

```bash
# See all CLI options
npx mixpanel-import --help

# Test authentication
npx mixpanel-import --validate-token your-token

# Enable verbose logging
npx mixpanel-import ./data.json --token your-token --verbose --logs
```

---

## Need More Help?

- 🐛 **Bug Reports**: [GitHub Issues](https://github.com/ak--47/mixpanel-import/issues)
- 📖 **API Documentation**: [Mixpanel Developer Docs](https://developer.mixpanel.com/)
- 💬 **Community**: [Mixpanel Community](https://mixpanel.com/community/)

happy streaming