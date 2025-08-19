# Mixpanel Import Benchmarks

This directory contains comprehensive performance benchmarking tools for the mixpanel-import module. The benchmarks help identify optimal configurations for maximum throughput and efficiency.

## Directory Structure

```
benchmarks/
├── README.md           # This documentation
├── index.mjs           # 🎯 MASTER ORCHESTRATOR - Run all benchmark suites
├── .env               # Environment variables for credentials
├── new/               # Modern comprehensive benchmark suite
│   ├── index.mjs                 # Main orchestrator with CLI interface
│   ├── workerOptimization.mjs    # Tests optimal worker/concurrency levels
│   ├── parameterMatrix.mjs       # Tests performance parameter combinations
│   ├── formatComparison.mjs      # Tests data format performance
│   ├── transformImpact.mjs       # Tests transform and validation overhead
│   ├── transportComparison.mjs   # Tests HTTP client performance
│   └── memoryVsStream.mjs        # Tests memory vs streaming processing
├── old/               # Legacy benchmark tools (updated for compatibility)
│   ├── httpOneOrTwo.mjs          # HTTP/1.1 vs HTTP/2 comparison
│   ├── main.mjs                  # Original benchmark runner
│   ├── profiler.mjs              # Memory and CPU profiling
│   ├── streamTypes.mjs           # Stream type performance testing
│   ├── streamsVsMemory.mjs       # Memory vs streaming comparison
│   ├── transport.mjs             # Transport layer benchmarking
│   └── workers.mjs               # Worker optimization testing
├── formats/           # Format-specific performance benchmarks
│   ├── index.mjs                 # Format benchmark orchestrator
│   ├── formatPerformance.mjs     # Cross-format performance comparison
│   ├── compressionImpact.mjs     # Compression vs uncompressed analysis
│   └── scalingAnalysis.mjs       # Format scaling characteristics
├── results/           # Benchmark output directory
└── testData/          # Test datasets
    ├── dnd250.ndjson           # 250k records (~83MB)
    ├── dnd250.json             # JSON version
    ├── dnd.csv                 # CSV version
    ├── one-two-million.ndjson  # 1-2M records (~618MB)
    └── formats/                # Comprehensive format test data
        ├── json/               # JSONL files (250k & 1M records)
        ├── json-gz/            # Compressed JSONL files
        ├── csv/                # CSV files (250k & 1M records)
        ├── csv-gz/             # Compressed CSV files
        ├── parquet/            # Parquet files (250k & 1M records)
        └── parquet-gz/         # Compressed Parquet files
```

## Quick Start

### Master Benchmark Suite (All Suites)

Run all benchmark suites with unified reporting:

```bash
cd benchmarks

# Quick test across all suites (~10 min)
node index.mjs --suite quick

# Standard benchmarks from all suites (~30 min)
node index.mjs --suite standard

# Comprehensive testing across all suites (~60 min)
node index.mjs --suite comprehensive

# Run only specific suite types
node index.mjs --suite new-only        # Only modern benchmarks
node index.mjs --suite formats-only    # Only format benchmarks  
node index.mjs --suite old-only        # Only legacy benchmarks

# Test with large datasets and live API
node index.mjs --size large --live
```

### Individual Benchmark Suites

#### Modern Benchmark Suite (Recommended)

```bash
cd benchmarks/new

# Quick performance test (worker optimization + format comparison)
node index.mjs --suite quick

# Standard test suite (most common benchmarks)
node index.mjs --suite standard

# Comprehensive testing (all benchmarks)
node index.mjs --suite comprehensive

# Test with large dataset
node index.mjs --size large

# Live API testing (not dry run)
node index.mjs --live

# Custom output directory
node index.mjs --output ./my-results
```

### Format-Specific Benchmarks

```bash
cd benchmarks/formats

# Quick format performance test
node index.mjs --suite quick

# Standard format suite (performance + compression)
node index.mjs --suite standard

# Comprehensive format testing (all format benchmarks)
node index.mjs --suite comprehensive

# Test with large datasets (1M records)
node index.mjs --size large

# Live API testing
node index.mjs --live
```

### Available Benchmark Suites

#### Master Suites (All Benchmarks)
| Suite | Coverage | Duration | Use Case |
|-------|----------|----------|----------|
| `quick` | Best of all suites | ~10 min | Fast comprehensive check |
| `standard` | Core benchmarks from all suites | ~30 min | Regular optimization |
| `comprehensive` | All benchmarks from all suites | ~60 min | Complete analysis |
| `new-only` | Only modern performance benchmarks | ~30 min | Latest optimization features |
| `formats-only` | Only data format analysis | ~15 min | Format selection optimization |
| `old-only` | Only legacy compatibility benchmarks | ~20 min | Compatibility verification |

#### Individual Suite Details

**Main Benchmarks (`/new/`)**
| Suite | Benchmarks | Duration | Use Case |
|-------|------------|----------|----------|
| `quick` | Worker optimization, Format comparison | ~5 min | Quick performance check |
| `standard` | Worker, Parameters, Format, Transport | ~15 min | Regular optimization |
| `comprehensive` | All 6 benchmarks | ~30 min | Complete analysis |

**Format Benchmarks (`/formats/`)**
| Suite | Benchmarks | Duration | Use Case |
|-------|------------|----------|----------|
| `quick` | Format performance | ~3 min | Basic format comparison |
| `standard` | Performance, Compression impact | ~8 min | Format optimization |
| `comprehensive` | Performance, Compression, Scaling | ~15 min | Complete format analysis |

### Individual Benchmarks

Run specific benchmarks independently:

```bash
cd benchmarks/new

# Test worker optimization only
node workerOptimization.mjs

# Test format performance only  
node formatComparison.mjs

# Test transport clients only
node transportComparison.mjs
```

## Environment Setup

All benchmarks require Mixpanel API credentials when running live tests (not dry run). Set these environment variables or add them to `/benchmarks/.env`:

```bash
# Required for live API testing
export MP_PROJECT=your_project_id
export MP_SECRET=your_api_secret  
export MP_TOKEN=your_project_token
```

Or create `/benchmarks/.env`:
```
MP_PROJECT=your_project_id
MP_SECRET=your_api_secret
MP_TOKEN=your_project_token
```

**Note**: Dry run mode (default) doesn't require credentials and tests parsing/processing performance without making API calls.

## Master Benchmark Results

The master suite generates unified reports across all benchmark types:

- **Master JSON Report**: `results/master-benchmark-TIMESTAMP.json` - Complete results from all suites
- **Executive Summary**: `results/master-summary-TIMESTAMP.txt` - High-level findings and recommendations
- **Individual Reports**: Detailed reports in `results/new/`, `results/formats/`, `results/old/` subdirectories

## Benchmark Details

### Main Benchmark Suite (`/new/`)

#### 1. **Worker Optimization** (`workerOptimization.mjs`)
- **Purpose**: Find optimal concurrency level (1-100 workers)
- **Key Metrics**: EPS (Events Per Second), Memory usage, Improvement over baseline
- **Output**: Optimal worker count, diminishing returns threshold

#### 2. **Parameter Matrix** (`parameterMatrix.mjs`)
- **Purpose**: Test combinations of performance parameters
- **Parameters**: Batch size, compression level, data processing options
- **Output**: Best configuration for speed vs quality trade-offs

#### 3. **Format Comparison** (`formatComparison.mjs`)
- **Purpose**: Compare data format performance (JSON, JSONL, CSV)
- **Modes**: Memory vs streaming processing
- **Output**: Fastest format and processing mode recommendations

#### 4. **Transform Impact** (`transformImpact.mjs`)
- **Purpose**: Measure overhead of data transforms and validation
- **Tests**: fixData, strict validation, custom transforms, null removal
- **Output**: Performance impact analysis and recommendations

#### 5. **Transport Comparison** (`transportComparison.mjs`)
- **Purpose**: Compare HTTP client performance
- **Clients**: GOT, UNDICI, node-fetch, native fetch
- **Output**: Fastest transport client and retry configuration

#### 6. **Memory vs Stream** (`memoryVsStream.mjs`)
- **Purpose**: Compare memory-based vs streaming processing
- **Tests**: Different data sizes, buffer configurations, memory usage
- **Output**: Optimal processing mode for different scenarios

### Format Benchmark Suite (`/formats/`)

#### 1. **Format Performance** (`formatPerformance.mjs`)
- **Purpose**: Compare parsing performance across all supported formats
- **Formats**: JSON(L), JSON-GZ, CSV, CSV-GZ, Parquet, Parquet-GZ
- **Key Metrics**: EPS, Memory usage, File size efficiency, Processing overhead
- **Output**: Fastest format and efficiency rankings

#### 2. **Compression Impact** (`compressionImpact.mjs`)  
- **Purpose**: Analyze performance trade-offs of compressed vs uncompressed formats
- **Comparisons**: JSON vs JSON-GZ, CSV vs CSV-GZ, Parquet vs Parquet-GZ
- **Key Metrics**: Speed impact, Size reduction, Memory usage, Network efficiency
- **Output**: Compression recommendations and impact analysis

#### 3. **Scaling Analysis** (`scalingAnalysis.mjs`)
- **Purpose**: Test how formats scale from 250k to 1M records
- **Analysis**: Linear vs non-linear scaling, Memory growth patterns, Processing efficiency curves
- **Key Metrics**: EPS scaling factor, Memory scaling, Format-specific bottlenecks
- **Output**: Best scaling formats and large dataset recommendations

### Legacy Benchmark Tools

The `old/` directory contains updated versions of the original benchmarks, now compatible with the modern API:

- **`httpOneOrTwo.mjs`**: HTTP/1.1 vs HTTP/2 performance comparison
- **`profiler.mjs`**: Detailed memory and CPU profiling during imports
- **`streamTypes.mjs`**: Highland.js stream vs native stream performance
- **`transport.mjs`**: Transport layer benchmarking with detailed metrics
- **`workers.mjs`**: Worker optimization with memory profiling

## Understanding Results

### Key Performance Metrics

- **EPS**: Events Per Second - primary throughput metric
- **RPS**: Requests Per Second - HTTP request efficiency  
- **MBPS**: Megabytes Per Second - data transfer rate
- **Memory**: Peak memory usage during processing
- **Efficiency**: Combined score considering speed and resource usage

### Benchmark Reports

Each benchmark run generates:

1. **JSON Report** (`benchmark-TIMESTAMP.json`)
   - Detailed results for each test
   - Raw performance data
   - Configuration details

2. **Summary Report** (`benchmark-summary-TIMESTAMP.txt`)
   - Key findings and recommendations
   - Performance highlights
   - Optimal configurations

3. **Console Output**
   - Real-time progress and results
   - Quick performance comparisons
   - Error reporting

### Sample Output

```
🚀 MIXPANEL IMPORT PERFORMANCE BENCHMARK SUITE
============================================================
📊 Suite: STANDARD
📁 Data Size: small (../testData/dnd250.ndjson)
🔬 Dry Run: YES (no actual API calls)

[1/4] Running workerOptimization...
🔧 WORKER OPTIMIZATION BENCHMARK
  Testing 1 workers...    EPS: 20,645, RPS: 103, Memory: 45MB
  Testing 10 workers...   EPS: 26,134, RPS: 131, Memory: 52MB
  Testing 20 workers...   EPS: 28,891, RPS: 144, Memory: 58MB

📊 Worker Optimization Analysis:
   🏆 Optimal Workers: 20 (28,891 EPS)
   📈 Peak Improvement: +39.9%
   ⚠️  Diminishing Returns At: 25 workers

RECOMMENDATIONS:
• Optimal worker count: 20 (best EPS performance)
• Use JSONL format with streaming processing
• Enable compression with level 6
```

## Performance Optimization Guide

### Based on Benchmark Results

1. **Worker Count**: Typically optimal around 15-25 workers
2. **Data Format**: JSONL generally fastest, CSV for compatibility
3. **Processing Mode**: Streaming for large datasets, memory for small
4. **Transport**: UNDICI often faster than GOT
5. **Compression**: Usually beneficial with level 6
6. **Transforms**: Minimal transforms for maximum speed

### Best Practices

- Run benchmarks on production-similar hardware
- Test with representative datasets
- Use `--live` flag sparingly (makes real API calls)
- Monitor system resources during benchmarking
- Compare results across different data sizes

## Troubleshooting

### Common Issues

1. **Import Path Errors**: Ensure you're running from correct directory
2. **Missing Test Data**: Verify test files exist in `testData/`
3. **Memory Issues**: Use smaller datasets or streaming mode
4. **Timeout Errors**: Increase timeout for large datasets

### Debug Mode

```bash
# Enable verbose logging
node index.mjs --suite quick --verbose

# Monitor system resources
htop  # or Activity Monitor on macOS

# Check file sizes
ls -lah testData/
```

## Contributing

When adding new benchmarks:

1. Follow the established pattern in `new/` directory
2. Include comprehensive analysis functions
3. Add proper error handling and logging
4. Update this README with new benchmark details
5. Test with both small and large datasets

## Data Sources

Test datasets are generated using the `make-mp-data` package:

```bash
# Generate new test data
npm run generate

# View existing data
head -n 5 testData/dnd250.ndjson
```

---

For questions or issues, please refer to the main project documentation or open an issue on GitHub.