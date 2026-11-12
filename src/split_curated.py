import duckdb
from pathlib import Path
import logging

# Project paths
PROJECT_DIR = Path(__file__).resolve().parents[1]

SOURCE_FILE = (
    PROJECT_DIR
    / "data"
    / "curated"
    / "nyc_taxi_trips_2024H1.parquet"
)

OUTPUT_DIR = PROJECT_DIR / "data" / "curated_split"

TAXI_TYPES = ["FHV", "FHVHV", "GREEN", "YELLOW"]
MONTHS = [f"2024-{month:02d}" for month in range(1, 7)]

# Initial chunk sizes based on export tests
CHUNK_SIZES = {
    "FHV": 4_000_000,
    "FHVHV": 3_000_000,
    "GREEN": 1_000_000,
    "YELLOW": 3_000_000,
}

# Keep a safety margin below GitHub's 100 MB limit
MAX_FILE_SIZE = 90 * 1024 * 1024
MIN_CHUNK_SIZE = 100_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def quote_sql(value):
    """Safely quote a SQL string."""
    return "'" + str(value).replace("'", "''") + "'"


# Prepare source path for DuckDB SQL
SOURCE_PATH = quote_sql(str(SOURCE_FILE).replace("\\", "/"))


def export_chunk(connection, taxi_type, month, offset, chunk_size, part):
    """Export one deterministic chunk and check its actual file size."""

    output_folder = OUTPUT_DIR / taxi_type.lower() / month
    output_folder.mkdir(parents=True, exist_ok=True)

    output_file = output_folder / f"part_{part:04d}.parquet"

    output_path = quote_sql(str(output_file).replace("\\", "/"))

    query = f"""
        COPY (
            SELECT *
            FROM read_parquet({SOURCE_PATH})
            WHERE service_type = {quote_sql(taxi_type)}
              AND pickup_month = {quote_sql(month)}
            ORDER BY pickup_datetime, source_file
            LIMIT {chunk_size}
            OFFSET {offset}
        )
        TO {output_path}
        (
            FORMAT PARQUET,
            COMPRESSION ZSTD
        )
    """

    connection.execute(query)

    actual_size = output_file.stat().st_size

    if actual_size > MAX_FILE_SIZE:
        output_file.unlink()
        return False, 0, actual_size

    row_count = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet({output_path})"
    ).fetchone()[0]

    if row_count == 0:
        output_file.unlink()
        raise RuntimeError(
            f"Empty output file: {taxi_type} {month} part {part}"
        )

    return True, row_count, actual_size


def run_split():
    """Split the curated dataset into smaller Parquet files."""

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Curated source not found: {SOURCE_FILE}"
        )

    # Prevent accidental mixing with files from a previous run
    if OUTPUT_DIR.exists() and any(OUTPUT_DIR.rglob("*.parquet")):
        raise FileExistsError(
            f"Output directory is not empty: {OUTPUT_DIR}\n"
            "Please inspect or clear previous split files before retrying."
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()

    total_exported = 0
    total_files = 0

    try:
        expected_count = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet({SOURCE_PATH})
            """
        ).fetchone()[0]

        logging.info(
            "Source rows: %s",
            f"{expected_count:,}"
        )

        for taxi_type in TAXI_TYPES:
            for month in MONTHS:

                count_query = f"""
                    SELECT COUNT(*)
                    FROM read_parquet({SOURCE_PATH})
                    WHERE service_type = {quote_sql(taxi_type)}
                      AND pickup_month = {quote_sql(month)}
                """

                group_count = connection.execute(
                    count_query
                ).fetchone()[0]

                if group_count == 0:
                    logging.warning(
                        "No records: %s %s",
                        taxi_type, month
                    )
                    continue

                logging.info(
                    "Processing %s %s: %s rows",
                    taxi_type,
                    month,
                    f"{group_count:,}"
                )

                offset = 0
                part = 1
                chunk_size = CHUNK_SIZES[taxi_type]

                while offset < group_count:

                    success, rows, size = export_chunk(
                        connection,
                        taxi_type,
                        month,
                        offset,
                        chunk_size,
                        part
                    )

                    if not success:
                        if chunk_size <= MIN_CHUNK_SIZE:
                            raise RuntimeError(
                                f"Cannot keep file below 90 MB: "
                                f"{taxi_type} {month}"
                            )

                        chunk_size = max(
                            MIN_CHUNK_SIZE,
                            chunk_size // 2
                        )

                        logging.warning(
                            "File too large. Reducing chunk size to %s",
                            f"{chunk_size:,}"
                        )
                        continue

                    logging.info(
                        "Created %s: %s rows, %.2f MB",
                        f"{taxi_type}/{month}/part_{part:04d}.parquet",
                        f"{rows:,}",
                        size / 1024 / 1024
                    )

                    offset += rows
                    total_exported += rows
                    total_files += 1
                    part += 1

        logging.info("Exported rows: %s", f"{total_exported:,}")
        logging.info("Exported files: %s", total_files)

    finally:
        connection.close()

    if total_exported != expected_count:
        raise ValueError(
            f"Row count mismatch: expected {expected_count:,}, "
            f"got {total_exported:,}"
        )

    logging.info("SPLIT RESULT: PASS")


if __name__ == "__main__":
    run_split()
