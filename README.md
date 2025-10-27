# Python Experiments

Exploring modern Python features, performance optimizations, and emerging technologies through hands-on experiments and benchmarks.

## Experiments

### 🧵 [Free-Threading (No-GIL)](./free-threading/)

Benchmarks demonstrating Python 3.14's experimental free-threaded build that removes the Global Interpreter Lock (GIL). Compares threading vs multiprocessing performance and explores shared memory use cases.

**Key findings:** 5-6x speedup for CPU-bound tasks, competitive with multiprocessing, 10x memory savings for shared state.

### ✈️ [Arrow Flight vs Parquet-over-HTTP](./arrow-flight-vs-http/)

Benchmark comparing Arrow Flight streaming protocol against traditional Parquet file downloads served by nginx. Tests throughput, latency, and memory efficiency for columnar data transfer.

**Comparison:** Arrow Flight enables zero-copy streaming vs compressed file downloads. Parquet-over-HTTP represents the lower bound (fastest non-Arrow approach).

## Requirements

- [uv](https://docs.astral.sh/uv/) - Fast Python package manager

## Quick Start

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Navigate to an experiment
cd free-threading

# Follow the experiment's README for specific setup
```

## About

This repository is a collection of hands-on experiments exploring:
- Modern Python features and performance characteristics
- Emerging technologies in the Python ecosystem
- Real-world performance comparisons and benchmarks
- Practical integration patterns and best practices

Each experiment includes runnable code, detailed explanations, and key findings.

## Contributing

Have an idea for a performance experiment? Open an issue or submit a pull request!

## License

MIT - Feel free to use these benchmarks for your own testing and analysis.