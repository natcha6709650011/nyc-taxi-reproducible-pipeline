
"""
Transformation Module
Clean and standardize NYC Taxi trip data.
Raw files are never modified.
"""

import logging
import re
from pathlib import Path

import duckdb


PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"

TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

START_DATE = "2024-01-01"
END_DATE = "2024-07-01"

# Negative-value rules from Submission 1.
# NULL and zero values are retained.
# Negative-value rules from Submission 1.
# Negative values are invalid.
# Zero and NULL values are retained.
NEGATIVE_RULES = {
    "yellow": [
        "trip_distance",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ],
    "green": [
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
],
    "fhv": [],
    "fhvhv": [
        "trip_miles",
        "base_passenger_fare",
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
    ],
}

PICKUP_COLUMNS = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhv": "pickup_datetime",
    "fhvhv": "pickup_datetime",
}

DROPOFF_COLUMNS = {
    "yellow": "tpep_dropoff_datetime",
    "green": "lpep_dropoff_datetime",
    "fhv": "dropOff_datetime",
    "fhvhv": "dropoff_datetime",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def quote_identifier(name):
    """Safely quote a SQL column identifier."""
    return '"' + name.replace('"', '""') + '"'


def normalize_column_name(name):
    """Standardize column names to lowercase snake_case."""
    normalized = name.lower()
    normalized = re.sub(r"[\s-]+", "_", normalized)
    return normalized


def get_source_files():
    """Find all expected raw Parquet files."""
    files = []

    for taxi_type in TAXI_TYPES:
        pattern = f"{taxi_type}_tripdata_2024-*.parquet"
        files.extend(RAW_DIR.glob(pattern))

    return sorted(files)


def transform_file(connection, file_path):
    """Clean and standardize one raw Parquet file."""

    filename = file_path.name
    month_match = re.search(r"\d{4}-\d{2}", filename)

    if month_match is None:
        raise ValueError(f"Cannot extract source month from {filename}")

    source_file_month = month_match.group()

    taxi_type = next(
        (
            t for t in TAXI_TYPES
            if filename.startswith(f"{t}_tripdata_")
        ),
        None,
    )

    if taxi_type is None:
        raise ValueError(f"Unknown taxi type: {filename}")

    # Read the actual schema.
    safe_path = str(file_path.resolve()).replace("\\", "/")
    safe_path = safe_path.replace("'", "''")

    schema = connection.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{safe_path}')"
    ).fetchall()

    columns = [row[0] for row in schema]
    column_lookup = {col.lower(): col for col in columns}

    pickup_column = column_lookup.get(
        PICKUP_COLUMNS[taxi_type].lower()
    )
    dropoff_column = column_lookup.get(
        DROPOFF_COLUMNS[taxi_type].lower()
    )

    if pickup_column is None or dropoff_column is None:
        raise ValueError(
            f"Missing pickup/dropoff datetime column in {filename}"
        )

    # Standardize original column names.
    # Pickup and dropoff columns will be added separately
    # using consistent names for all taxi types.
    select_parts = []

    for column in columns:

        if column in (pickup_column, dropoff_column):
            continue

        normalized = normalize_column_name(column)

        if normalized != column:
            select_parts.append(
                f"{quote_identifier(column)} AS "
                f"{quote_identifier(normalized)}"
            )
        else:
            select_parts.append(quote_identifier(column))

    # Add standard metadata and datetime flags.
    pickup = quote_identifier(pickup_column)
    dropoff = quote_identifier(dropoff_column)

    select_parts.extend([
    f"CAST({pickup} AS TIMESTAMP) AS pickup_datetime",
    f"CAST({dropoff} AS TIMESTAMP) AS dropoff_datetime",
    f"'{taxi_type.upper()}' AS service_type",
    f"'{filename}' AS source_file",
    f"'{source_file_month}' AS source_file_month",

    f"""
    CASE
        WHEN {pickup} IS NULL OR {dropoff} IS NULL THEN FALSE
        WHEN YEAR({pickup}) != YEAR({dropoff})
          OR MONTH({pickup}) != MONTH({dropoff})
        THEN TRUE
        ELSE FALSE
    END AS is_cross_month_trip
    """,

        f"""
    CASE
        WHEN {pickup} IS NULL OR {dropoff} IS NULL THEN FALSE
        WHEN {dropoff} < {pickup} THEN TRUE
        ELSE FALSE
    END AS is_invalid_duration
    """,

    f"""
    CASE
        WHEN {pickup} IS NULL OR {dropoff} IS NULL THEN FALSE
        WHEN (
            YEAR({pickup}) != YEAR({dropoff})
            OR MONTH({pickup}) != MONTH({dropoff})
        )
        AND (EPOCH({dropoff}) - EPOCH({pickup})) > 86400
        THEN TRUE
        ELSE FALSE
    END AS is_suspicious_cross_month
    """,
])
    output_columns = [
        part.split(" AS ")[-1].strip().strip('"')
        if " AS " in part
        else part.strip().strip('"')
        for part in select_parts
    ]

    if len(output_columns) != len(set(output_columns)):
        raise ValueError(
            f"Duplicate output column names found in {filename}"
        )
    
    # Apply negative-value rules.
    # A row is retained when every applicable value is >= 0 or NULL.
    conditions = []

    for column in NEGATIVE_RULES[taxi_type]:
        actual_column = column_lookup.get(column.lower())

        if actual_column is None:
            raise ValueError(
                f"Missing negative-rule column '{column}' in {filename}"
            )

        quoted_column = quote_identifier(actual_column)

        conditions.append(
            f"({quoted_column} >= 0 OR {quoted_column} IS NULL)"
        )

    negative_filter = (
        " AND ".join(conditions) if conditions else "TRUE"
    )

    # Keep trips whose pickup datetime is within Jan-Jun 2024.
    # Cross-month trips are retained according to pickup datetime.
    study_period_filter = f"""
        {pickup} >= TIMESTAMP '{START_DATE}'
        AND {pickup} < TIMESTAMP '{END_DATE}'
    """

    output_dir = PROCESSED_DIR / taxi_type
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / filename
    safe_output = str(output_file.resolve()).replace("\\", "/")
    safe_output = safe_output.replace("'", "''")

    before = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{safe_path}')"
    ).fetchone()[0]

    query = f"""
        COPY (
            SELECT
                {", ".join(select_parts)}
            FROM read_parquet('{safe_path}')
            WHERE
                ({negative_filter})
                AND ({study_period_filter})
        )
        TO '{safe_output}'
        (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE)
    """

    connection.execute(query)

    after = connection.execute(
        f"SELECT COUNT(*) FROM read_parquet('{safe_output}')"
    ).fetchone()[0]

    removed = before - after

    return {
        "filename": filename,
        "taxi_type": taxi_type,
        "rows_before": before,
        "rows_after": after,
        "rows_removed": removed,
        "output_file": str(output_file),
    }


def run_transformation():
    """Run transformation for all raw files."""

    logging.info("Starting NYC Taxi transformation")

    files = get_source_files()

    logging.info("Found raw files: %s", len(files))

    if len(files) != 24:
        raise ValueError(
            f"Expected 24 raw files, but found {len(files)}"
        )

    results = []
    connection = duckdb.connect()

    try:
        for index, file_path in enumerate(files, start=1):
            logging.info(
                "Transforming %s/%s: %s",
                index,
                len(files),
                file_path.name,
            )

            result = transform_file(connection, file_path)
            results.append(result)

            logging.info(
                "Rows: %s -> %s (removed: %s)",
                f"{result['rows_before']:,}",
                f"{result['rows_after']:,}",
                f"{result['rows_removed']:,}",
            )

    finally:
        connection.close()

    print("\n========== TRANSFORMATION SUMMARY ==========")

    total_before = sum(r["rows_before"] for r in results)
    total_after = sum(r["rows_after"] for r in results)
    total_removed = sum(r["rows_removed"] for r in results)

    for taxi_type in TAXI_TYPES:
        group = [r for r in results if r["taxi_type"] == taxi_type]

        print(
            f"{taxi_type.upper():6} | "
            f"Files: {len(group)} | "
            f"Rows before: {sum(r['rows_before'] for r in group):,} | "
            f"Rows after: {sum(r['rows_after'] for r in group):,} | "
            f"Removed: {sum(r['rows_removed'] for r in group):,}"
        )

    print("---------------------------------------------")
    print(f"Total rows before : {total_before:,}")
    print(f"Total rows after  : {total_after:,}")
    print(f"Total rows removed: {total_removed:,}")
    print(f"Output directory : {PROCESSED_DIR}")
    print("TRANSFORMATION RESULT: PASS")

    return results


if __name__ == "__main__":
    run_transformation()
