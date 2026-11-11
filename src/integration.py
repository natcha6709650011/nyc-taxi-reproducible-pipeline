
"""
Integration Module
Combine processed NYC Taxi datasets into one curated dataset.
"""

import logging
from pathlib import Path

import duckdb


PROJECT_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
CURATED_DIR = PROJECT_DIR / "data" / "curated"

TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]
EXPECTED_FILE_COUNT = 24

OUTPUT_FILE = CURATED_DIR / "nyc_taxi_trips_2024H1.parquet"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def get_processed_files():
    """Find all processed Parquet files."""

    files = []

    for taxi_type in TAXI_TYPES:
        files.extend(
            (PROCESSED_DIR / taxi_type).glob("*.parquet")
        )

    return sorted(files)


def run_integration():
    """Combine all processed taxi datasets."""

    logging.info("Starting NYC Taxi data integration")

    files = get_processed_files()

    logging.info("Found processed files: %d", len(files))

    if len(files) != EXPECTED_FILE_COUNT:
        raise ValueError(
            f"Expected {EXPECTED_FILE_COUNT} files, "
            f"but found {len(files)}"
        )

    CURATED_DIR.mkdir(parents=True, exist_ok=True)

    file_paths = [
        "'"
        + str(file.resolve()).replace("\\", "/").replace("'", "''")
        + "'"
        for file in files
    ]

    file_list = ", ".join(file_paths)

    connection = duckdb.connect()

    try:
        # Combine datasets using column names.
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW unified_trips AS
            SELECT
                *,
                STRFTIME(pickup_datetime, '%Y-%m') AS pickup_month
            FROM read_parquet(
                [{file_list}],
                union_by_name = TRUE
            )
            """
        )

        # Count source records after integration.
        source_count = connection.execute(
            "SELECT COUNT(*) FROM unified_trips"
        ).fetchone()[0]

        # Count records by taxi type.
        summary = connection.execute(
            """
            SELECT
                service_type,
                COUNT(*) AS trip_count
            FROM unified_trips
            GROUP BY service_type
            ORDER BY service_type
            """
        ).fetchall()

        output_path = (
            str(OUTPUT_FILE.resolve())
            .replace("\\", "/")
            .replace("'", "''")
        )

        # Export the unified dataset.
        connection.execute(
            f"""
            COPY (
                SELECT * FROM unified_trips
            )
            TO '{output_path}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                OVERWRITE_OR_IGNORE TRUE
            )
            """
        )

        # Verify the exported curated dataset.
        output_count = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{output_path}')
            """
        ).fetchone()[0]

    finally:
        connection.close()

    if source_count != output_count:
        raise ValueError(
            f"Row count mismatch: "
            f"source={source_count}, curated={output_count}"
        )

    print("\n========== INTEGRATION SUMMARY ==========")

    for taxi_type, trip_count in summary:
        print(f"{taxi_type:8} | {trip_count:,} rows")

    print("-----------------------------------------")
    print(f"Source rows  : {source_count:,}")
    print(f"Curated rows : {output_count:,}")
    print(f"Output file  : {OUTPUT_FILE}")
    print("INTEGRATION RESULT: PASS")

    return {
        "source_rows": source_count,
        "curated_rows": output_count,
        "output_file": str(OUTPUT_FILE),
        "summary": summary,
    }


if __name__ == "__main__":
    run_integration()
