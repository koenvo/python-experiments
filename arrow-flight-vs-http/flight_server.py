"""
Simple Arrow Flight server that reads a Parquet file on-demand and streams it.

No authentication - focus on raw performance comparison.
"""

import argparse
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq


class SimpleFlightServer(flight.FlightServerBase):
    def __init__(self, location: str, parquet_file: str):
        super().__init__(location)
        self.parquet_file = parquet_file
        self.location = location
        print(f"Configured to serve: {parquet_file}")

    def get_flight_info(self, context, descriptor):
        """Return flight info for the dataset."""
        # Open parquet file to get schema
        parquet_file = pq.ParquetFile(self.parquet_file)

        endpoint = flight.FlightEndpoint(b"data", [self.location])
        return flight.FlightInfo(
            parquet_file.schema_arrow,
            descriptor,
            [endpoint],
            total_records=parquet_file.metadata.num_rows,
            total_bytes=-1,  # Unknown without reading
        )

    def do_get(self, context, ticket):
        """Stream the Parquet file as Arrow record batches."""
        # Use RecordBatchReader for streaming
        parquet_file = pq.ParquetFile(self.parquet_file)
        batches = parquet_file.iter_batches()
        reader = pa.RecordBatchReader.from_batches(parquet_file.schema_arrow, batches)
        return flight.RecordBatchStream(reader)


def main():
    parser = argparse.ArgumentParser(description="Arrow Flight server")
    parser.add_argument("--file", type=str, default="data.parquet", help="Parquet file to load")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8815, help="Port to bind")
    args = parser.parse_args()

    import os
    if not os.path.exists(args.file):
        print(f"Error: File '{args.file}' not found")
        print(f"Create it first: uv run python create_test_data.py --output {args.file}")
        import sys
        sys.exit(1)

    location = f"grpc://{args.host}:{args.port}"
    print(f"Starting Arrow Flight server on {location}")

    server = SimpleFlightServer(location, args.file)
    server.serve()


if __name__ == "__main__":
    main()
