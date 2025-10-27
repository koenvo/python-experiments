"""
Create test Parquet file for benchmarking.

Generates a realistic analytics dataset with timestamps, IDs, and measurements.
"""

import argparse
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


def create_test_data(num_rows: int, output_path: str):
    """Create a Parquet file with test data."""
    print(f"Generating {num_rows:,} rows...")

    # Generate realistic analytics data
    data = {
        "timestamp": np.arange(num_rows, dtype=np.int64),
        "user_id": np.random.randint(1, 10000, num_rows, dtype=np.int32),
        "session_id": np.random.randint(1, 100000, num_rows, dtype=np.int32),
        "event_type": np.random.choice(["click", "view", "purchase", "scroll"], num_rows),
        "value": np.random.uniform(0, 1000, num_rows),
        "duration": np.random.uniform(0, 60, num_rows),
        "x_coord": np.random.uniform(0, 1920, num_rows),
        "y_coord": np.random.uniform(0, 1080, num_rows),
    }

    schema = pa.schema([
        pa.field("timestamp", pa.int64()),
        pa.field("user_id", pa.int32()),
        pa.field("session_id", pa.int32()),
        pa.field("event_type", pa.string()),
        pa.field("value", pa.float64()),
        pa.field("duration", pa.float64()),
        pa.field("x_coord", pa.float64()),
        pa.field("y_coord", pa.float64()),
    ])

    table = pa.table(data, schema=schema)

    print(f"Writing to {output_path}...")
    pq.write_table(table, output_path, compression="snappy")

    # Print stats
    file_size = pa.Table.from_batches([table.to_batches()[0]]).nbytes if table.num_rows > 0 else 0
    import os
    disk_size = os.path.getsize(output_path)

    print(f"✓ Created {output_path}")
    print(f"  Rows: {num_rows:,}")
    print(f"  Columns: {len(schema)}")
    print(f"  Uncompressed size: {file_size / 1024 / 1024:.1f} MB")
    print(f"  File size (snappy): {disk_size / 1024 / 1024:.1f} MB")
    print(f"  Compression ratio: {file_size / disk_size:.2f}x")


def main():
    parser = argparse.ArgumentParser(description="Create test Parquet file")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows")
    parser.add_argument("--output", type=str, default="data.parquet", help="Output file")
    args = parser.parse_args()

    create_test_data(args.rows, args.output)


if __name__ == "__main__":
    main()
