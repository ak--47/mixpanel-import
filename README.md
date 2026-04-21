# mixpanel-import

## 🤨 tldr;
stream data to mixpanel... quickly. support for events, user/group profiles, lookup tables, annotations, scd. all of it.

use the UI, the CLI, or include it as a module in your pipeline. we have built-in recipes for different vendor formats, performant transform utilities, retries + backoff, monitoring, and more.

don't write your own ETL. use this:

<a href="https://www.loom.com/share/a0be7c53779f467f921961ba910a3ce9?sid=57cd29a8-adaa-486b-a9ff-1ce717b51ee5">![E.T.L Interface - Extract Transform Load data into Mixpanel](https://aktunes.neocities.org/mp-import-ui.png)</a>
**note:** this tool is designed for batch data imports and migrations. for real-time tracking in web applications, you want [the official Mixpanel JavaScript SDK](https://github.com/mixpanel/mixpanel-js).

## 🎯 Quick Start (Recommended)

**The fastest way to get started is with the web interface:**

if you work at mixpanel, just go here:
<a href="https://etl.mixpanel.org">https://etl.mixpanel.org</a>

if you do not:
```bash
npx mixpanel-import --ui
```
then open `http://localhost:3000` in your browser.

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
- **Import**: `npx mixpanel-import gs://bucket/file.json`
- **Export**: `npx mixpanel-import --type export --where gs://bucket/exports/ ...`
- Supports all formats including compressed files (`.json.gz`, `.csv.gz`, etc.)

### 🪣 **Amazon S3**
- **Import**: `npx mixpanel-import s3://bucket/file.json --s3Region us-east-1`
- **Export**: `npx mixpanel-import --type export --where s3://bucket/exports/ --s3Region us-east-1 ...`
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
- **V2 Compatibility**: Automatically sets `distinct_id` from `user_id` or `device_id` (prefixed or unprefixed); falls back to `""` (enable with `v2_compat: true`)

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

### 🎯 **Adaptive Scaling (NEW in v3.1.1)**
- **Automatic Configuration**: Samples first 100 events to optimize settings
- **Event Density Detection**: Categorizes events (tiny/small/medium/large/dense)
- **OOM Prevention**: Automatically reduces workers for memory-intensive data
- **Zero Configuration**: Enable with `--adaptive` flag for hands-off operation
- **Performance Hints**: Use `--avg-event-size` when event size is known

### 💾 **Handling Large/Dense Files (NEW in v3.1.2)**

When importing very large files (>1GB) or dense data from cloud storage:

#### **Recommended Settings for Large Files:**
```bash
# For files > 1GB from GCS/S3
npx mixpanel-import gs://bucket/large-file.json \
  --throttleGCS \
  --throttlePauseMB 1500 \
  --throttleResumeMB 1000 \
  --throttleMaxBufferMB 2000 \
  --token your-token

# For extremely dense events (PostHog, Segment, etc.)
npx mixpanel-import s3://bucket/dense-data.json \
  --throttleMemory \
  --adaptive \
  --workers 5 \
  --token your-token
```

#### **How BufferQueue Works:**
- **Smart Buffering**: Decouples fast cloud downloads (100MB/s) from slower processing (10MB/s)
- **Memory Protection**: Pauses cloud downloads when buffer exceeds threshold, allowing pipeline to drain
- **Continuous Processing**: Pipeline continues sending to Mixpanel while cloud download is paused
- **Auto-Resume**: Downloads resume automatically when buffer drains below threshold
- **No Data Loss**: All data is processed in order without dropping records

#### **Throttle Configuration Options:**
| Option | Default | Description |
|--------|---------|-------------|
| `--throttleGCS` | `false` | Enable memory-aware throttling for cloud storage |
| `--throttlePauseMB` | `1500` | Pause downloads when buffer reaches this size (MB) |
| `--throttleResumeMB` | `1000` | Resume downloads when buffer drops to this size (MB) |
| `--throttleMaxBufferMB` | `2000` | Maximum buffer size before forcing pause (MB) |

**Pro Tips:**
- Use `--throttleGCS` for any GCS/S3 file over 1GB
- Combine with `--adaptive` for automatic worker optimization
- Monitor memory with `--verbose` to see buffer status
- For local files, throttling is not needed (disk I/O is naturally slower)

### 🏗️ **Enterprise Features**
- **Cloud Streaming**: Direct streaming from/to GCS/S3 without local download
- **Cloud Export**: Export events directly to GCS/S3 with gzip compression
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

### 🚀 **Handling Dense Event Data (Adaptive Scaling)**

```bash
# Automatic configuration for dense events (PostHog, Segment, etc.)
npx mixpanel-import dense_events.json --token your-token --adaptive

# Or provide event size hint for immediate optimization (11KB avg)
npx mixpanel-import posthog_export.jsonl --token your-token --avgEventSize 11000 --vendor posthog
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
| `adaptive` | `boolean` | `false` | Enable adaptive scaling to prevent OOM errors |
| `avgEventSize` | `number` | | Average event size hint in bytes (for adaptive mode) |
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
| `v2_compat` | `boolean` | `false` | (Events only) Auto-set `distinct_id` from `$user_id`/`user_id` or `$device_id`/`device_id`; falls back to `""` |
| `directive` | `string` | `"$set"` | (Profiles only) Operation for profile updates: `$set`, `$set_once`, `$add`, `$union`, `$append`, `$remove`, `$unset` |

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
| `where` | `string` | Export destination: local path or cloud (`gs://` or `s3://`) |
| `compress` | `boolean` | Gzip compression for exports (default: `true` for cloud) |
| `whereClause` | `string` | Mixpanel where clause for filtering |
| `limit` | `number` | Maximum records to export |
| `cohortId` | `string/number` | Cohort ID for profile exports |
| `dataGroupId` | `string` | Data group ID for group profile exports |

#### ☁️ **Exporting to Cloud Storage**

Export directly to GCS or S3 with optional compression:

```bash
# Export to Google Cloud Storage (compressed by default)
npx mixpanel-import --type export --start 2024-01-01 --end 2024-01-31 \
  --where gs://bucket/exports/ --acct user --pass pass --project 12345

# Export to S3 without compression
npx mixpanel-import --type export --start 2024-01-01 --end 2024-01-31 \
  --where s3://bucket/exports/ --compress false \
  --s3Key AKIA... --s3Secret xxx --s3Region us-east-1
```

**File Extension Convention:**
- Compressed (`--compress` or default): `.json.gz`
- Uncompressed (`--compress false`): `.ndjson`

**Auto-generated filenames:** When `--where` is a directory path (ending with `/`), filenames are auto-generated as `events-{start}--{end}.json.gz` or `events-{start}--{end}.ndjson`.

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

### 🆔 **V2 Compatibility Mode**

The `v2_compat` option automatically sets `distinct_id` from Mixpanel's ID Management v2 properties. Use it when you don't know in advance whether the destination project is on original or simplified ID merge — original merge expects `distinct_id` on every event, so this guarantees the field is present.

```javascript
// Enable v2_compat in your import
const result = await mpImport(
  { token: 'your-token' },
  './data.json',
  {
    recordType: 'event',
    v2_compat: true  // Auto-set distinct_id from user_id or device_id
  }
);
```

**How it works:**
- Picks a source value in this order: `$user_id`, `user_id`, `$device_id`, `device_id`
- If a source value is found, sets `distinct_id` to that value
- If none of those keys are present, sets `distinct_id` to `""` (empty string) so original-merge identity logic still has a field to attach to
- Never overwrites an existing `distinct_id` value
- Only applies to events (not user/group profiles)
- Original `$user_id` / `user_id` / `$device_id` / `device_id` are preserved

**Example:**
```javascript
// Input event
{
  event: 'Page View',
  properties: {
    $user_id: 'user123',
    $device_id: 'device456',
    page: '/home'
  }
}

// After v2_compat transform
{
  event: 'Page View',
  properties: {
    distinct_id: 'user123',    // ← Added automatically
    $user_id: 'user123',       // ← Preserved
    $device_id: 'device456',   // ← Preserved
    page: '/home'
  }
}
```

---

## 👤 Profile Operations

### **Profile Update Directives**

When importing user or group profiles, use the `directive` parameter to control how properties are updated:

```bash
# Default: $set - Overwrites existing values
npx mixpanel-import profiles.json --recordType user --token your-token

# $set_once - Only set if property doesn't exist
npx mixpanel-import profiles.json --recordType user --token your-token --directive '$set_once'

# $add - Increment numeric properties
npx mixpanel-import profiles.json --recordType user --token your-token --directive '$add'

# $union - Append unique values to lists
npx mixpanel-import profiles.json --recordType user --token your-token --directive '$union'

# $unset - Remove properties
npx mixpanel-import profiles.json --recordType user --token your-token --directive '$unset'
```

**Available directives:**
- `$set` (default) - Overwrite existing property values
- `$set_once` - Only set if property doesn't exist
- `$add` - Add to numeric properties (increment/decrement)
- `$union` - Append unique values to list properties
- `$append` - Append all values to list properties (allows duplicates)
- `$remove` - Remove specific values from list properties
- `$unset` - Remove properties entirely

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

## 🏗️ Recommended Settings for Large/Dense Files

### **Handling Multi-GB Files from Cloud Storage**

When importing large files (>1GB) from Google Cloud Storage or S3, especially with dense events (>5KB each), use these settings to prevent OOM errors:

```bash
# For large files with dense events (e.g., PostHog exports)
npx mixpanel-import gs://bucket/large-file.json \
  --token your-token \
  --throttleGCS \           # Pause cloud downloads when memory is high
  --throttlePauseMB 1500 \  # Pause at 1.5GB heap (default)
  --throttleResumeMB 1000 \ # Resume at 1GB heap (default)
  --workers 10 \            # Lower workers for dense data
  --highWater 50 \          # Lower buffer size
  --verbose                 # Monitor pause/resume cycles

# Alternative: Use adaptive scaling
npx mixpanel-import gs://bucket/large-file.json \
  --token your-token \
  --adaptive \              # Auto-configure based on event density
  --throttleGCS \           # Still use throttling for safety
  --verbose
```

### **Key Options for Memory Management**

| Option | Description | When to Use |
|--------|-------------|-------------|
| `--throttleGCS` | Pauses cloud downloads when memory exceeds threshold | Large cloud files (>500MB) |
| `--throttleMemory` | Alias for throttleGCS | Same as above |
| `--throttlePauseMB` | Memory threshold to pause (MB) | Default 1500, lower if still OOMing |
| `--throttleResumeMB` | Memory threshold to resume (MB) | Default 1000, must be < pauseMB |
| `--adaptive` | Auto-adjusts workers and buffers | Dense events (>2KB each) |
| `--highWater` | Stream buffer size | Lower (16-50) for dense events |
| `--workers` | Concurrent HTTP requests | Lower (5-10) for dense data |

### **How Throttling Works**

1. **BufferQueue decouples cloud storage from processing** - Creates a buffer between fast GCS/S3 downloads and slower Mixpanel uploads
2. **Automatic pause/resume** - When heap exceeds `throttlePauseMB`, cloud downloads pause but pipeline continues draining
3. **Backpressure management** - Prevents unbounded memory growth while maintaining throughput
4. **Verbose monitoring** - Shows pause/resume cycles and memory usage in real-time

### **Example Output with Throttling**
```
🛑 BUFFER QUEUE: Pausing GCS input
    ├─ Queue size: 1523MB > 1500MB threshold
    ├─ Queue depth: 152,304 objects
    └─ Pipeline continues draining buffered data...

    📤 Pipeline draining while paused: Batch #134 sent (heap: 1495MB)
    ⏸️  GCS PAUSED - 2m 15s | QUEUE DRAINING
    ├─ Queue: 982MB (98,234 objects)
    └─ Progress: 54,070 / 152,304 objects sent

▶️  BUFFER QUEUE: Resuming GCS input
    ├─ Queue size: 982MB < 1000MB threshold
    ├─ Duration: Paused for 2m 15s
    └─ Objects: 54,070 processed while paused
```

---

## 🔍 Troubleshooting

### ❌ **Common Issues**

**"Rate limited" errors**
- Reduce `workers` (try 5 instead of 10)
- Reduce `recordsPerBatch` (try 1000 instead of 2000)

**"Memory" errors**
- Use `--adaptive` flag for automatic configuration (recommended)
- Or provide `--avg-event-size` if known (e.g., `--avg-event-size 5000` for 5KB events)
- Manual fixes: Add `--forceStream` flag, reduce `workers` count
- Process files in smaller chunks if adaptive scaling doesn't help

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