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

### 🎯 Master Benchmark Suite (Recommended)

The master orchestrator runs all benchmark suites with unified reporting and live API calls:

```bash
cd benchmarks

# Quick test across all suites (250k records, ~10 min)
node index.mjs --suite quick

# Standard benchmarks from all suites (1M records, ~30 min)
node index.mjs --suite standard

# Comprehensive testing across all suites (1M records, ~60 min)
node index.mjs --suite comprehensive

# Run only specific suite types
node index.mjs --suite new-only        # Only modern benchmarks (1M records)
node index.mjs --suite formats-only    # Only format benchmarks (1M records)  
node index.mjs --suite old-only        # Only legacy benchmarks (1M records)

# Override data size (use 250k records for any suite)
node index.mjs --size small

# Custom output directory
node index.mjs --output ./my-results
```

### Individual Benchmark Suites

You can also run individual benchmark suites directly:

#### Modern Benchmark Suite

```bash
cd benchmarks/new

# Quick performance test (250k records, worker + format optimization)
node index.mjs --suite quick

# Standard test suite (1M records, core benchmarks)
node index.mjs --suite standard

# Comprehensive testing (1M records, all 6 benchmarks)
node index.mjs --suite comprehensive

# Override data size
node index.mjs --size small    # Use 250k records
node index.mjs --size large    # Use 1M records
```

#### Format-Specific Benchmarks

```bash
cd benchmarks/formats

# Quick format performance test (250k records)
node index.mjs --suite quick

# Standard format suite (1M records - performance + compression)
node index.mjs --suite standard

# Comprehensive format testing (1M records - all format benchmarks)
node index.mjs --suite comprehensive

# Override data sizes
node index.mjs --size small    # Force 250k records
node index.mjs --size large    # Force 1M records
```

## Environment Setup

**⚠️ All benchmarks require Mixpanel API credentials** since they make live API calls for realistic performance measurements.

Set these environment variables or add them to `/benchmarks/.env`:

```bash
# Required for all benchmark executions
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

## Available Benchmark Suites

### Master Suites (All Benchmarks)
| Suite | Coverage | Data Size | Duration | Use Case |
|-------|----------|-----------|----------|----------|
| `quick` | Best of all suites | **250k records** | ~10 min | Fast comprehensive check |
| `standard` | Core benchmarks from all suites | **1M records** | ~30 min | Regular optimization |
| `comprehensive` | All benchmarks from all suites | **1M records** | ~60 min | Complete analysis |
| `new-only` | Only modern performance benchmarks | **1M records** | ~30 min | Latest optimization features |
| `formats-only` | Only data format analysis | **1M records** | ~15 min | Format selection optimization |
| `old-only` | Only legacy compatibility benchmarks | **1M records** | ~20 min | Compatibility verification |

### Individual Suite Details

**Main Benchmarks (`/new/`)**
| Suite | Benchmarks | Default Size | Duration | Use Case |
|-------|------------|--------------|----------|----------|
| `quick` | Worker optimization, Format comparison | 250k | ~5 min | Quick performance check |
| `standard` | Worker, Parameters, Format, Transport | 1M | ~15 min | Regular optimization |
| `comprehensive` | All 6 benchmarks | 1M | ~30 min | Complete analysis |

**Format Benchmarks (`/formats/`)**
| Suite | Benchmarks | Default Size | Duration | Use Case |
|-------|------------|--------------|----------|----------|
| `quick` | Format performance | 250k | ~3 min | Basic format comparison |
| `standard` | Performance, Compression impact | 1M | ~8 min | Format optimization |
| `comprehensive` | Performance, Compression, Scaling | 1M | ~15 min | Complete format analysis |

## Benchmark Results

### Master Suite Reports

The master orchestrator generates unified reports across all benchmark types:

- **Master JSON Report**: `results/master-benchmark-TIMESTAMP.json` - Complete results from all suites
- **Executive Summary**: `results/master-summary-TIMESTAMP.txt` - High-level findings and recommendations
- **Individual Reports**: Detailed reports in `results/new/`, `results/formats/`, `results/old/` subdirectories

### Sample Output

```
🚀 MIXPANEL IMPORT MASTER BENCHMARK SUITE
======================================================================
📊 Suite: QUICK
📁 Data Size: small (250k records)
🎯 Live API: YES (real performance testing)
📂 Output: ./results
======================================================================

[1/3] 🆕 RUNNING NEW BENCHMARK SUITE
──────────────────────────────────────────────────
🔧 WORKER OPTIMIZATION BENCHMARK
  Testing 1 workers...    EPS: 20,645, RPS: 103, Memory: 45MB
  Testing 10 workers...   EPS: 26,134, RPS: 131, Memory: 52MB
  Testing 20 workers...   EPS: 28,891, RPS: 144, Memory: 58MB

📊 Worker Optimization Analysis:
   🏆 Optimal Workers: 20 (28,891 EPS)
   📈 Peak Improvement: +39.9%
```

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

### Legacy Benchmark Tools (`/old/`)

The `old/` directory contains updated versions of the original benchmarks, now compatible with the modern API:

- **`httpOneOrTwo.mjs`**: HTTP/1.1 vs HTTP/2 performance comparison
- **`main.mjs`**: Basic performance verification and system testing
- **`profiler.mjs`**: Detailed memory and CPU profiling during imports
- **`streamTypes.mjs`**: Highland.js stream vs native stream performance
- **`streamsVsMemory.mjs`**: Memory vs streaming processing comparison
- **`transport.mjs`**: Transport layer benchmarking with detailed metrics
- **`workers.mjs`**: Worker optimization with memory profiling

## Understanding Results

### Key Performance Metrics

- **EPS**: Events Per Second - primary throughput metric
- **RPS**: Requests Per Second - HTTP request efficiency  
- **MBPS**: Megabytes Per Second - data transfer rate
- **Memory**: Peak memory usage during processing
- **Efficiency**: Combined score considering speed and resource usage

### Performance Optimization Guide

Based on benchmark results:

1. **Worker Count**: Typically optimal around 15-25 workers
2. **Data Format**: JSONL generally fastest, CSV for compatibility, Parquet for large datasets
3. **Processing Mode**: Streaming for large datasets, memory for small
4. **Transport**: UNDICI often faster than GOT
5. **Compression**: Usually beneficial for network transfer (JSON-GZ, CSV-GZ)
6. **Transforms**: Minimal transforms for maximum speed

### Best Practices

- **Start with Quick Suite**: `node index.mjs --suite quick` for fast insights
- **Use Standard for Regular Testing**: `node index.mjs --suite standard` for thorough analysis
- **Run Comprehensive for Optimization**: `node index.mjs --suite comprehensive` for complete analysis
- **Monitor System Resources**: Ensure adequate CPU/memory during benchmarking
- **Compare Results**: Run benchmarks after configuration changes to measure impact

## Troubleshooting

### Common Issues

1. **Missing Credentials**: Ensure `MP_PROJECT`, `MP_SECRET`, and `MP_TOKEN` are set
2. **Missing Test Data**: Verify test files exist in `testData/` and `testData/formats/`
3. **Memory Issues**: Use smaller datasets (`--size small`) or increase system memory
4. **API Rate Limits**: Add delays between benchmarks if hitting Mixpanel rate limits

### Debug Commands

```bash
# Check credentials
echo $MP_PROJECT $MP_SECRET $MP_TOKEN

# Verify test data
ls -la testData/formats/json/

# Test with smallest dataset
node index.mjs --suite quick --size small

# Individual benchmark testing
cd new && node workerOptimization.mjs
```

## Contributing

When adding new benchmarks:

1. Follow the established pattern in the appropriate directory (`new/`, `formats/`, `old/`)
2. Include comprehensive analysis functions
3. Add proper error handling and credential checking
4. Update this README with new benchmark details
5. Test with both small and large datasets

---

For questions or issues, please refer to the main project documentation or open an issue on GitHub.