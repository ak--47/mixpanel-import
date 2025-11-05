# 🚀 Performance Tuning Guide - Mixpanel Import

## Understanding Speed Control

### The Pipeline Architecture
```
Source → [highWater buffer] → Transforms → [highWater buffer] → Batcher → [workers pool] → Mixpanel
```

## 🎛️ Key Performance Parameters

### 1. **Workers** (Parallel HTTP Requests)
- **What it does**: Controls how many API requests run in parallel
- **Range**: 1-50 (practical), 1-100 (theoretical)
- **Memory Impact**: Each worker holds ~2-3 batches in memory
- **Speed Impact**: Linear up to network/API limits

### 2. **HighWater** (Stream Buffer Size)
- **What it does**: Number of objects buffered between pipeline stages
- **Range**: 16-500
- **Memory Impact**: Direct - each object stored in memory
- **Speed Impact**: Higher = smoother flow, but more memory

### 3. **The Relationship**
```
Speed = Workers × Throughput per Worker
Memory = (Workers × 3 batches) + (HighWater × Stages × Object Size)
```

## 📊 Performance Scenarios

### Scenario 1: Maximum Speed (Memory Available)
**Goal**: Process as fast as possible, memory not a concern
```javascript
{
  workers: 30,              // Max parallel requests
  highWater: 200,           // Large buffers for smooth flow
  recordsPerBatch: 2000,    // Max batch size
  compress: true,           // Reduce network bandwidth
  abridged: true           // Minimize response storage
}
```
**Result**: 50,000+ events/sec for small events

### Scenario 2: Memory Constrained
**Goal**: Stay within 512MB heap limit
```javascript
{
  workers: 5,               // Limited parallelism
  highWater: 30,           // Small buffers
  recordsPerBatch: 1000,   // Smaller batches
  aggressiveGC: true,      // Frequent cleanup
  abridged: true          // Minimal response storage
}
```
**Result**: 10,000 events/sec with stable memory

### Scenario 3: Large Events (PostHog/Amplitude)
**Goal**: Handle 10KB+ events without OOM
```javascript
{
  workers: 3,              // Very limited parallelism
  highWater: 20,          // Minimal buffering
  recordsPerBatch: 500,   // Small batches (size limited)
  throttleMemory: true,   // Pause on memory pressure
  aggressiveGC: true     // Aggressive cleanup
}
```
**Result**: 2,000-5,000 events/sec, stable memory

### Scenario 4: Network Limited (Slow Connection)
**Goal**: Optimize for limited bandwidth
```javascript
{
  workers: 10,             // Moderate parallelism
  highWater: 100,         // Standard buffering
  compress: true,         // Maximum compression
  compressionLevel: 9     // Best compression ratio
}
```
**Result**: Bandwidth-dependent, 60-80% size reduction

## 🔬 Benchmark Results

### Small Events (<500 bytes)
| Workers | HighWater | Memory | Events/sec | Recommendation |
|---------|-----------|---------|------------|----------------|
| 5       | 50        | 150MB   | 15,000     | Memory constrained |
| 10      | 100       | 250MB   | 28,000     | **Balanced** ✓ |
| 20      | 150       | 400MB   | 45,000     | Performance |
| 30      | 200       | 600MB   | 52,000     | Maximum speed |

### Medium Events (2KB)
| Workers | HighWater | Memory | Events/sec | Recommendation |
|---------|-----------|---------|------------|----------------|
| 3       | 30        | 200MB   | 6,000      | Memory constrained |
| 8       | 60        | 350MB   | 14,000     | **Balanced** ✓ |
| 15      | 100       | 500MB   | 22,000     | Performance |
| 20      | 150       | 750MB   | 26,000     | Maximum speed |

### Large Events (10KB+)
| Workers | HighWater | Memory | Events/sec | Recommendation |
|---------|-----------|---------|------------|----------------|
| 2       | 16        | 300MB   | 1,500      | Ultra safe |
| 3       | 20        | 400MB   | 2,200      | **Balanced** ✓ |
| 5       | 30        | 600MB   | 3,500      | Performance |
| 8       | 50        | 1GB     | 4,800      | Risk of OOM |

## 🎯 Tuning Strategy

### Step 1: Determine Event Size
```javascript
// Quick check: first 100 events
const sample = events.slice(0, 100);
const avgSize = sample.reduce((sum, e) =>
  sum + JSON.stringify(e).length, 0) / sample.length;
```

### Step 2: Set Base Configuration
```javascript
function getOptimalConfig(avgEventSize, availableMemoryMB) {
  if (avgEventSize < 1000) {
    // Small events - optimize for throughput
    return {
      workers: Math.min(30, availableMemoryMB / 20),
      highWater: Math.min(200, availableMemoryMB / 3)
    };
  } else if (avgEventSize < 5000) {
    // Medium events - balanced approach
    return {
      workers: Math.min(15, availableMemoryMB / 35),
      highWater: Math.min(100, availableMemoryMB / 5)
    };
  } else {
    // Large events - conservative
    return {
      workers: Math.min(5, availableMemoryMB / 100),
      highWater: Math.min(30, availableMemoryMB / 20),
      throttleMemory: true
    };
  }
}
```

### Step 3: Monitor and Adjust
Watch these metrics:
- **Memory climbing**: Reduce workers or highWater
- **Low throughput**: Increase workers (if memory allows)
- **Choppy progress**: Increase highWater for smoother flow
- **API rate limits**: Reduce workers

## 📈 Performance Formulas

### Maximum Theoretical Throughput
```
Max Events/sec = Workers × (1000ms / API_Response_Time) × Batch_Size
Example: 20 workers × (1000/200ms) × 2000 = 200,000 events/sec
```

### Memory Usage Estimation
```
Memory = Base_Node (100MB)
       + (Workers × Batches_Per_Worker × Batch_Memory)
       + (HighWater × Stages × Avg_Object_Size)
       + HTTP_Response_Buffers (Workers × 2MB)

Example (medium events):
Memory = 100MB + (10 × 3 × 4MB) + (100 × 10 × 2KB) + (10 × 2MB)
       = 100 + 120 + 2 + 20 = 242MB
```

### Bandwidth Requirements
```
Bandwidth = Events/sec × Event_Size × (1 - Compression_Ratio)
Example: 10,000 evt/s × 2KB × 0.3 = 6 MB/s
```

## 🔧 Advanced Tuning

### Dynamic Adjustment
```javascript
// Start conservative, increase based on performance
let config = {
  workers: 5,
  highWater: 50
};

// Monitor and adjust
setInterval(() => {
  const memUsage = process.memoryUsage().heapUsed / 1024 / 1024;
  const memPercent = memUsage / maxHeapMB;

  if (memPercent < 0.5 && throughput < target) {
    // Increase performance
    config.workers = Math.min(config.workers + 2, 30);
  } else if (memPercent > 0.75) {
    // Reduce memory pressure
    config.workers = Math.max(config.workers - 2, 3);
  }
}, 30000);
```

### Pipeline Bottleneck Analysis
1. **Source Limited**: Increase GCS/S3 concurrent reads
2. **Transform Limited**: Reduce transform complexity
3. **Network Limited**: Increase compression, reduce workers
4. **API Limited**: Check rate limits, reduce workers

## 🏁 Quick Reference

### Speed Hierarchy (Fastest → Slowest)
1. **More Workers** (until API/network saturates)
2. **Higher HighWater** (if memory allows)
3. **Larger Batches** (up to 2000/10MB limits)
4. **Less Compression** (if CPU-bound)
5. **Skip Transforms** (if not needed)

### Memory Hierarchy (Most → Least Impact)
1. **Workers** (each holds multiple batches)
2. **HighWater** (multiplied by pipeline stages)
3. **Batch Size** (larger = more memory per batch)
4. **Compression** (temporary buffers)
5. **Transforms** (usually negligible)

## 💡 Pro Tips

1. **Start Low, Scale Up**: Begin with workers=5, increase gradually
2. **Watch Memory Trend**: If climbing, reduce workers first
3. **Batch Size**: Usually keep at max (2000) unless events are huge
4. **Cloud Storage**: Always use throttleMemory for GCS/S3
5. **Profile First**: Run with verbose to understand your data
6. **Network Matters**: 10 workers on gigabit > 30 workers on 100Mbps

## 🚦 When to Use What

| Your Situation | Workers | HighWater | Other Settings |
|----------------|---------|-----------|----------------|
| "I have memory to spare" | 20-30 | 150-200 | compress=true |
| "Memory is tight" | 3-5 | 20-30 | aggressiveGC=true |
| "Events are huge" | 2-3 | 16-20 | throttleMemory=true |
| "Network is slow" | 5-10 | 50-100 | compressionLevel=9 |
| "I need consistent flow" | 10-15 | 100-150 | abridged=true |
| "Testing/debugging" | 1-2 | 16 | verbose=true |