# Arrow Flight vs Parquet-over-HTTP

Benchmark comparing Arrow Flight streaming protocol against Parquet file downloads served by nginx.

## What is Arrow Flight?

Arrow Flight is a high-performance RPC framework for columnar data transport built on gRPC and Arrow.

**Arrow Flight:** Streams Arrow data directly over gRPC (zero-copy, streaming)

**Parquet-over-HTTP:** Serves static Parquet files via nginx (compressed, cacheable)

**Why compare with Parquet-over-HTTP?** It represents the lower bound - the fastest way to serve Parquet without Arrow Flight. Real systems (Iceberg) add overhead from manifest parsing and multiple files.

## Setup

### Prerequisites

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install Docker
# https://docs.docker.com/get-docker/
```

### Running the Benchmark

```bash
cd arrow-flight-vs-http

# Create test Parquet file
uv run python create_test_data.py --rows 1000000 --output data.parquet

# Terminal 1: Start Arrow Flight server (reads data.parquet on-demand)
uv run python flight_server.py --file data.parquet

# Terminal 2: Start nginx in Docker (serves data.parquet)
docker run -d --name arrow-nginx -p 8080:80 -v $(pwd):/usr/share/nginx/html:ro nginx:alpine

# Terminal 3: Run benchmark
uv run python benchmark.py --runs 10

# Cleanup
docker stop arrow-nginx && docker rm arrow-nginx
```

## Benchmark Results

### Ubuntu Server 22.04 (100 runs, 1M rows)

```
Arrow Flight:         38ms   1443 MB/s   (1.55x faster, 55.1 MB uncompressed)
Parquet over HTTP:    59ms    677 MB/s   (baseline, 40.0 MB compressed)
```

Arrow Flight is 1.55x faster despite transferring 38% more data.

### Key Findings

1. Arrow Flight avoids decompression + Parquet deserialization overhead
2. On localhost, CPU overhead (decompression/deserialization) likely dominates network transfer cost
3. Untested: slower networks or column projection impact

## Trade-offs

**Arrow Flight:**
- Zero-copy streaming
- No decompression overhead
- Built-in authentication/authorization support
- Requires gRPC infrastructure
- Not cacheable (dynamic)

**Parquet-over-HTTP:**
- Smaller transfer size (27% compressed)
- HTTP/CDN compatible
- Cacheable (static files)
- Requires decompression + deserialization

## Technical Details

- **Data size**: 1M rows, ~100 MB uncompressed
- **Schema**: Timestamp, IDs, floats (similar to analytics data)
- **Network**: localhost (eliminates network bandwidth as bottleneck)
- **Parquet compression**: Snappy (default)

## Learn More

- [Arrow Flight Documentation](https://arrow.apache.org/docs/format/Flight.html)
- [Apache Parquet](https://parquet.apache.org/)
- [nginx](https://nginx.org/)
