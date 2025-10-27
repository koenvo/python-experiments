"""
Benchmark Arrow Flight vs Parquet-over-HTTP.

Measures throughput and latency.
"""

import argparse
import time
import statistics
import sys
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
import pyarrow.compute as pc
import requests


def benchmark_arrow_flight(host: str, port: int) -> float:
    """Benchmark Arrow Flight data transfer."""
    start = time.perf_counter()

    # Connect and fetch data
    client = flight.connect(f"grpc://{host}:{port}")
    descriptor = flight.FlightDescriptor.for_path("data")
    info = client.get_flight_info(descriptor)
    stream = client.do_get(info.endpoints[0].ticket)

    # Read all data
    table = stream.read_all()

    elapsed = time.perf_counter() - start
    return elapsed


def benchmark_parquet_http(url: str) -> float:
    """Benchmark Parquet file download over HTTP."""
    start = time.perf_counter()

    # Download and read Parquet file
    response = requests.get(url)
    response.raise_for_status()

    # Read from bytes
    import io
    table = pq.read_table(io.BytesIO(response.content))

    elapsed = time.perf_counter() - start
    return elapsed


def run_benchmark(runs: int, flight_host: str, flight_port: int, http_url: str):
    """Run benchmark multiple times and report statistics."""
    print(f"Running {runs} iterations...\n")

    flight_times = []
    http_times = []

    for i in range(runs):
        print(f"Run {i+1}/{runs}...", end=" ", flush=True)

        # Benchmark Arrow Flight
        try:
            flight_time = benchmark_arrow_flight(flight_host, flight_port)
            flight_times.append(flight_time)
        except Exception as e:
            print(f"\nError connecting to Arrow Flight server: {e}")
            print("Make sure the server is running: uv run python flight_server.py --file data.parquet")
            sys.exit(1)

        # Benchmark Parquet HTTP
        try:
            http_time = benchmark_parquet_http(http_url)
            http_times.append(http_time)
        except requests.exceptions.RequestException as e:
            print(f"\nError fetching Parquet file over HTTP: {e}")
            print("Make sure nginx is running: docker run -d --name arrow-nginx -p 8080:80 -v $(pwd):/usr/share/nginx/html:ro nginx:alpine")
            sys.exit(1)

        print(f"Flight: {flight_time:.3f}s, HTTP: {http_time:.3f}s")

    # Create results directory
    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    # Generate output
    output = []
    output.append("=" * 70)
    output.append("Results Summary")
    output.append("=" * 70)

    output.append("\nArrow Flight:")
    output.append(f"  Mean time: {statistics.mean(flight_times):.3f}s")
    output.append(f"  Median time: {statistics.median(flight_times):.3f}s")
    output.append(f"  Min time: {min(flight_times):.3f}s")
    output.append(f"  Max time: {max(flight_times):.3f}s")
    if len(flight_times) > 1:
        output.append(f"  Std dev: {statistics.stdev(flight_times):.3f}s")

    output.append("\nParquet over HTTP:")
    output.append(f"  Mean time: {statistics.mean(http_times):.3f}s")
    output.append(f"  Median time: {statistics.median(http_times):.3f}s")
    output.append(f"  Min time: {min(http_times):.3f}s")
    output.append(f"  Max time: {max(http_times):.3f}s")
    if len(http_times) > 1:
        output.append(f"  Std dev: {statistics.stdev(http_times):.3f}s")

    output.append("\n" + "=" * 70)
    output.append("Comparison")
    output.append("=" * 70)

    flight_avg = statistics.mean(flight_times)
    http_avg = statistics.mean(http_times)

    if flight_avg < http_avg:
        speedup = http_avg / flight_avg
        output.append(f"Arrow Flight is {speedup:.2f}x faster ({(speedup - 1) * 100:.1f}% faster)")
    else:
        speedup = flight_avg / http_avg
        output.append(f"Parquet HTTP is {speedup:.2f}x faster ({(speedup - 1) * 100:.1f}% faster)")

    # Print to console
    print("\n" + "\n".join(output))

    # Write to file
    import platform
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    filename = f"benchmark-{platform.system().lower()}-{timestamp}.txt"
    output_path = results_dir / filename

    with open(output_path, "w") as f:
        f.write(f"Python: {sys.version.split()[0]}\n")
        f.write(f"Platform: {platform.system()} {platform.release()}\n")
        f.write(f"Runs: {runs}\n\n")
        f.write("\n".join(output))

    print(f"\nResults written to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark Arrow Flight vs HTTP")
    parser.add_argument("--runs", type=int, default=10, help="Number of benchmark runs")
    parser.add_argument("--flight-host", type=str, default="localhost", help="Flight server host")
    parser.add_argument("--flight-port", type=int, default=8815, help="Flight server port")
    parser.add_argument("--http-url", type=str, default="http://localhost:8080/data.parquet", help="HTTP URL")
    args = parser.parse_args()

    run_benchmark(args.runs, args.flight_host, args.flight_port, args.http_url)


if __name__ == "__main__":
    main()
