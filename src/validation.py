
"""
Validation Module
Validate NYC Taxi Trip Record Parquet files.
"""
from datetime import datetime
import csv
import glob
import logging
from pathlib import Path


import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_DIR / "data" / "raw"
REPORT_DIR = PROJECT_DIR / "reports"

EXPECTED_FILE_COUNT = 24

TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# Required columns for each taxi type.
# Different taxi types use different column names.
REQUIRED_COLUMNS = {
    "yellow": [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ],
    "green": [
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "PULocationID",
        "DOLocationID",
    ],
    "fhv": [
        "pickup_datetime",
        "dropOff_datetime",
        "PUlocationID",
        "DOlocationID",
    ],
    "fhvhv": [
        "pickup_datetime",
        "dropoff_datetime",
        "trip_miles",
        "trip_time",
        "PULocationID",
        "DOLocationID",
    ],
}

PICKUP_COLUMNS = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhv": "pickup_datetime",
    "fhvhv": "pickup_datetime",
}


# Logging

REPORT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Validation functions

def get_taxi_type(file_path):
    """Get taxi type from filename."""

    filename = Path(file_path).name

    for taxi_type in TAXI_TYPES:
        if filename.startswith(f"{taxi_type}_tripdata_"):
            return taxi_type

    return None


def get_parquet_files():
    """Find all raw Parquet files."""

    return sorted(glob.glob(str(RAW_DIR / "*.parquet")))

def get_expected_month(file_path):
    """Extract expected year and month from filename."""

    filename = Path(file_path).stem
    year_month = filename.split("_")[-1]

    return datetime.strptime(year_month, "%Y-%m")



def validate_file(connection, file_path):
    """Validate one Parquet file."""

    filename = Path(file_path).name
    taxi_type = get_taxi_type(file_path)

    result = {
        "filename": filename,
        "taxi_type": taxi_type or "UNKNOWN",
        "expected_month": "",
        "row_count": 0,
        "column_count": 0,
        "missing_columns": "",
        "null_pickup_count": "",
        "outside_month_count": "",
        "outside_study_period_count": "",
        "pickup_min": "",
        "pickup_max": "",
        "file_validation_status": "FAILED",
        "date_scope_status": "NOT_CHECKED",
        "message": "",
    }

    try:
        if taxi_type is None:
            raise ValueError("Unknown taxi type in filename")

        expected_month = get_expected_month(file_path)
        result["expected_month"] = expected_month.strftime("%Y-%m")

        # Read schema
        safe_path = str(Path(file_path).resolve()).replace("\\", "/")
        safe_path = safe_path.replace("'", "''")

        schema = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{safe_path}')"
        ).fetchall()

        columns = [row[0] for row in schema]
        result["column_count"] = len(columns)

        # Check required columns
        required = REQUIRED_COLUMNS[taxi_type]
        missing = [
            column for column in required
            if column not in columns
        ]

        result["missing_columns"] = ", ".join(missing)

        if missing:
            raise ValueError(
                f"Missing required columns: {missing}"
            )

        # Check row count and pickup datetime quality
        pickup_column = PICKUP_COLUMNS[taxi_type]

        query = f"""
            SELECT
                COUNT(*) AS row_count,
                COUNT(*) FILTER (
                    WHERE "{pickup_column}" IS NULL
                ) AS null_pickup_count,
                MIN("{pickup_column}") AS pickup_min,
                MAX("{pickup_column}") AS pickup_max
            FROM read_parquet('{safe_path}')
        """

        row = connection.execute(query).fetchone()

        row_count, null_count, pickup_min, pickup_max = row

        # Check pickup dates outside the expected file month
        month_start = expected_month.replace(day=1)

        if expected_month.month == 12:
            next_month = expected_month.replace(
                year=expected_month.year + 1,
                month=1
            )
        else:
            next_month = expected_month.replace(
                month=expected_month.month + 1
            )

        date_scope_query = f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE "{pickup_column}" < TIMESTAMP '{month_start:%Y-%m-%d}'
                    OR "{pickup_column}" >= TIMESTAMP '{next_month:%Y-%m-%d}'
                ),
                COUNT(*) FILTER (
                    WHERE "{pickup_column}" < TIMESTAMP '2024-01-01'
                    OR "{pickup_column}" >= TIMESTAMP '2024-07-01'
                )
            FROM read_parquet('{safe_path}')
        """

        date_counts = connection.execute(
            date_scope_query
        ).fetchone()

        result["outside_month_count"] = date_counts[0]
        result["outside_study_period_count"] = date_counts[1]

        result["row_count"] = row_count
        result["null_pickup_count"] = null_count
        result["pickup_min"] = str(pickup_min)
        result["pickup_max"] = str(pickup_max)

        if row_count == 0:
            raise ValueError("File contains zero rows")

        # File validation is separate from date scope
        result["file_validation_status"] = "PASS"

        if result["outside_study_period_count"] > 0:
            result["date_scope_status"] = "HAS_OUT_OF_SCOPE_RECORDS"
        elif null_count > 0:
            result["date_scope_status"] = "HAS_NULL_PICKUP_RECORDS"
        else:
            result["date_scope_status"] = "IN_SCOPE"

        result["message"] = "File is readable and schema is valid"

        if null_count > 0:
            result["message"] += "; NULL pickup datetime records found"

    except Exception as error:
        result["file_validation_status"] = "FAILED"
        result["message"] = str(error)

    return result



# Main validation process

def run_validation():

    logging.info("Starting NYC Taxi data validation")

    files = get_parquet_files()

    logging.info("Expected files: %s", EXPECTED_FILE_COUNT)
    logging.info("Found files: %s", len(files))

    results = []

    connection = duckdb.connect()

    try:
        for index, file_path in enumerate(files, start=1):

            filename = Path(file_path).name

            logging.info(
                "Validating file %s/%s: %s",
                index,
                len(files),
                filename,
            )

            result = validate_file(connection, file_path)
            results.append(result)

            logging.info(
                "%s: %s",
                filename,
                result["file_validation_status"],
            )

    finally:
        connection.close()

    # Save validation report
    report_path = REPORT_DIR / "validation_report.csv"

    fieldnames = [
    "filename",
    "taxi_type",
    "expected_month",
    "row_count",
    "column_count",
    "missing_columns",
    "null_pickup_count",
    "outside_month_count",
    "outside_study_period_count",
    "pickup_min",
    "pickup_max",
    "file_validation_status",
    "date_scope_status",
    "message",
]

    with open(
        report_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as csv_file:

        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(results)

    # Summary
    passed = sum(
        result["file_validation_status"] == "PASS"
        for result in results
    )

    failed = len(results) - passed

    file_count_valid = len(files) == EXPECTED_FILE_COUNT

    print("\n========== VALIDATION SUMMARY ==========")
    print(f"Expected files : {EXPECTED_FILE_COUNT}")
    print(f"Found files    : {len(files)}")
    print(f"Passed         : {passed}")
    print(f"Failed         : {failed}")
    print(f"File count OK  : {file_count_valid}")
    print(f"Report saved   : {report_path}")

    overall_pass = file_count_valid and failed == 0

    print(
        "\nVALIDATION RESULT:",
        "PASS" if overall_pass else "FAIL"
    )

    return overall_pass


if __name__ == "__main__":
    run_validation()
