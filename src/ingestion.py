
"""
NYC Taxi Reproducible Data Pipeline
Module: Ingestion

Sources:
    1. NYC TLC official Trip Record Data
    2. Local backup files (e.g. downloaded from Google Drive)

Period:
    January - June 2024

Taxi types:
    Yellow, Green, FHV, FHVHV
"""

from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from datetime import datetime, timezone
import argparse
import csv
import logging
import sys
import time
import uuid


# 1. PROJECT CONFIGURATION

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DIR = PROJECT_ROOT / "data" / "raw"
REPORT_DIR = PROJECT_ROOT / "reports"

BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)

YEAR = 2024
MONTHS = range(1, 7)

TAXI_TYPES = [
    "yellow",
    "green",
    "fhv",
    "fhvhv"
]

EXPECTED_FILE_COUNT = len(TAXI_TYPES) * len(MONTHS)

MANIFEST_FILE = REPORT_DIR / "ingestion_manifest.csv"
LOG_FILE = REPORT_DIR / "ingestion.log"

MAX_RETRIES = 3
TIMEOUT_SECONDS = 180
CHUNK_SIZE = 1024 * 1024


# 2. LOGGING

def setup_logging():
    """Configure console and file logging."""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(
                LOG_FILE,
                encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout)
        ],
        force=True
    )



# 3. EXPECTED FILES

def get_expected_files():
    """Generate all expected Parquet filenames."""

    files = []

    for taxi_type in TAXI_TYPES:
        for month in MONTHS:

            filename = (
                f"{taxi_type}_tripdata_"
                f"{YEAR}-{month:02d}.parquet"
            )

            files.append(filename)

    return files


def get_file_metadata(filename):
    """Extract taxi type, year and month."""

    parts = filename.replace(".parquet", "").split("_")

    taxi_type = parts[0]
    date_part = parts[-1]

    year, month = date_part.split("-")

    return {
        "taxi_type": taxi_type,
        "year": int(year),
        "month": int(month)
    }


def get_download_url(filename):
    """Build the official TLC download URL."""

    return f"{BASE_URL}/{filename}"


# 4. PARQUET FILE VALIDATION

def is_valid_parquet(file_path):
    """
    Perform basic Parquet signature validation.

    A Parquet file must begin and end with PAR1.
    This does not replace full data-quality validation.
    """

    if not file_path.exists():
        return False

    try:
        if file_path.stat().st_size < 8:
            return False

        with file_path.open("rb") as file:

            header = file.read(4)

            file.seek(-4, 2)

            footer = file.read(4)

        return (
            header == b"PAR1"
            and footer == b"PAR1"
        )

    except OSError:
        return False


# 5. MANIFEST CONFIGURATION

MANIFEST_FIELDS = [
    "run_id",
    "run_timestamp",
    "filename",
    "taxi_type",
    "year",
    "month",
    "source",
    "status",
    "size_bytes",
    "url",
    "message"
]


def prepare_manifest():
    """
    Handle an older manifest schema from the sample test.

    Preserve the old manifest by renaming it before
    creating a new manifest with the updated columns.
    """

    if not MANIFEST_FILE.exists():
        return

    with MANIFEST_FILE.open(
        "r",
        newline="",
        encoding="utf-8"
    ) as file:

        reader = csv.reader(file)
        existing_header = next(reader, [])

    if existing_header == MANIFEST_FIELDS:
        return

    legacy_file = (
        REPORT_DIR / "ingestion_manifest_legacy.csv"
    )

    if legacy_file.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        legacy_file = (
            REPORT_DIR
            / f"ingestion_manifest_legacy_{timestamp}.csv"
        )

    MANIFEST_FILE.replace(legacy_file)

    logging.info(
        "Previous manifest preserved as %s",
        legacy_file.name
    )


def save_manifest(results, run_id):
    """Append execution results to the manifest."""

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    prepare_manifest()

    file_exists = MANIFEST_FILE.exists()

    timestamp = datetime.now(timezone.utc).isoformat()

    with MANIFEST_FILE.open(
        "a",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=MANIFEST_FIELDS
        )

        if not file_exists:
            writer.writeheader()

        for result in results:

            writer.writerow({
                "run_id": run_id,
                "run_timestamp": timestamp,
                **result
            })


# ==================================================
# 6. DOWNLOAD ONE FILE
# ==================================================

def download_file(filename):
    """
    Download one file from TLC.

    Existing valid local files are skipped.
    Invalid existing files are reported without
    silently overwriting them.
    """

    destination = RAW_DIR / filename

    temporary_file = RAW_DIR / f"{filename}.part"

    url = get_download_url(filename)

    metadata = get_file_metadata(filename)

    # Check existing local file

    if destination.exists():

        if is_valid_parquet(destination):

            size = destination.stat().st_size

            logging.info(
                "SKIPPED: %s already exists",
                filename
            )

            return {
                **metadata,
                "filename": filename,
                "source": "LOCAL_EXISTING",
                "status": "SKIPPED",
                "size_bytes": size,
                "url": url,
                "message": (
                    "Existing local file passed "
                    "Parquet signature check"
                )
            }

        logging.error(
            "INVALID: Existing file %s is not valid Parquet",
            filename
        )

        return {
            **metadata,
            "filename": filename,
            "source": "LOCAL_EXISTING",
            "status": "INVALID",
            "size_bytes": destination.stat().st_size,
            "url": url,
            "message": (
                "Existing file failed Parquet signature check. "
                "Inspect or replace it manually."
            )
        }

    # Download with retry

    request = Request(
        url,
        headers={
            "User-Agent": (
                "NYC-Taxi-Reproducible-Pipeline/1.0"
            )
        }
    )

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            logging.info(
                "DOWNLOADING: %s (Attempt %d/%d)",
                filename,
                attempt,
                MAX_RETRIES
            )

            with urlopen(
                request,
                timeout=TIMEOUT_SECONDS
            ) as response:

                with temporary_file.open("wb") as output:

                    while True:

                        chunk = response.read(CHUNK_SIZE)

                        if not chunk:
                            break

                        output.write(chunk)

            # Validate downloaded file

            if not is_valid_parquet(temporary_file):

                raise ValueError(
                    "Downloaded file failed Parquet "
                    "signature validation"
                )

            # Move completed file into raw directory

            temporary_file.replace(destination)

            size = destination.stat().st_size

            logging.info(
                "SUCCESS: %s (%.2f MB)",
                filename,
                size / (1024 * 1024)
            )

            return {
                **metadata,
                "filename": filename,
                "source": "NYC_TLC",
                "status": "SUCCESS",
                "size_bytes": size,
                "url": url,
                "message": "Downloaded and signature validated"
            }

        except Exception as error:

            if temporary_file.exists():
                temporary_file.unlink()

            logging.warning(
                "Attempt %d failed for %s: %s",
                attempt,
                filename,
                str(error)
            )

            if attempt < MAX_RETRIES:

                time.sleep(2 ** attempt)

    logging.error(
        "FAILED: %s after %d attempts",
        filename,
        MAX_RETRIES
    )

    return {
        **metadata,
        "filename": filename,
        "source": "NYC_TLC",
        "status": "FAILED",
        "size_bytes": 0,
        "url": url,
        "message": (
            f"Download failed after {MAX_RETRIES} attempts"
        )
    }


# 7. EXECUTION SUMMARY

def print_summary(results):
    """Print pipeline execution summary."""

    counts = {
        status: sum(
            result["status"] == status
            for result in results
        )
        for status in [
            "SUCCESS",
            "SKIPPED",
            "FAILED",
            "INVALID"
        ]
    }

    logging.info("=" * 65)
    logging.info("INGESTION SUMMARY")
    logging.info("=" * 65)

    logging.info(
        "Expected files : %d",
        EXPECTED_FILE_COUNT
    )

    logging.info(
        "Processed      : %d",
        len(results)
    )

    logging.info(
        "Downloaded     : %d",
        counts["SUCCESS"]
    )

    logging.info(
        "Skipped        : %d",
        counts["SKIPPED"]
    )

    logging.info(
        "Failed         : %d",
        counts["FAILED"]
    )

    logging.info(
        "Invalid        : %d",
        counts["INVALID"]
    )

    logging.info("=" * 65)

    return counts


# 8. MAIN INGESTION PIPELINE

def run_ingestion(limit=None, dry_run=False):
    """Run the complete ingestion process."""

    setup_logging()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())

    expected_files = get_expected_files()

    if limit is not None:
        expected_files = expected_files[:limit]

    logging.info("=" * 65)
    logging.info("NYC TAXI INGESTION PIPELINE")
    logging.info("Run ID: %s", run_id)
    logging.info("Year: %d", YEAR)
    logging.info("Period: January - June")
    logging.info("Taxi types: %s", ", ".join(TAXI_TYPES))
    logging.info("Expected files in full run: %d", EXPECTED_FILE_COUNT)
    logging.info("=" * 65)

    # Dry run: inspect files without downloading

    if dry_run:

        for filename in expected_files:

            destination = RAW_DIR / filename

            if destination.exists():

                if is_valid_parquet(destination):
                    status = "LOCAL FILE READY"
                else:
                    status = "INVALID LOCAL FILE"

            else:
                status = "WILL DOWNLOAD"

            logging.info(
                "%s | %s",
                filename,
                status
            )

        logging.info(
            "DRY RUN COMPLETE: No files were downloaded"
        )

        return True

    # Normal ingestion

    results = []

    for index, filename in enumerate(
        expected_files,
        start=1
    ):

        logging.info(
            "Processing file %d/%d",
            index,
            len(expected_files)
        )

        result = download_file(filename)

        results.append(result)

    save_manifest(results, run_id)

    counts = print_summary(results)

    failed = (
        counts["FAILED"] > 0
        or counts["INVALID"] > 0
    )

    if failed:

        logging.error(
            "INGESTION STATUS: FAIL"
        )

        return False

    logging.info(
        "INGESTION STATUS: PASS"
    )

    return True


# 9. COMMAND LINE INTERFACE

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "NYC Taxi Reproducible Data Ingestion Pipeline"
        )
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check expected files without downloading"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N expected files"
    )

    args = parser.parse_args()

    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be at least 1")

    successful = run_ingestion(
        limit=args.limit,
        dry_run=args.dry_run
    )

    if not successful:
        sys.exit(1)
