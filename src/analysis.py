import logging
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[1]

CURATED_FILE = (
    PROJECT_DIR
    / "data"
    / "curated"
    / "nyc_taxi_trips_2024H1.parquet"
)

def analyze_taxi_usage(connection):
    """
    Analyze total trips and usage percentage by taxi type.
    One record represents one trip.
    """

    query = """
        SELECT
            service_type,
            COUNT(*) AS total_trips,
            ROUND(
                COUNT(*) * 100.0 /
                SUM(COUNT(*)) OVER (),
                2
            ) AS usage_percentage
        FROM read_parquet(?)
        GROUP BY service_type
        ORDER BY total_trips DESC
    """

    results = connection.execute(
        query, [str(CURATED_FILE)]
    ).fetchall()

    return results


def analyze_monthly_usage(connection):
    """
    Analyze monthly trips by taxi type.
    """

    query = """
        SELECT
            pickup_month,
            service_type,
            COUNT(*) AS total_trips
        FROM read_parquet(?)
        GROUP BY pickup_month, service_type
        ORDER BY pickup_month, service_type
    """

    results = connection.execute(
        query, [str(CURATED_FILE)]
    ).fetchall()

    return results


if __name__ == "__main__":
    connection = duckdb.connect()

    try:
        results = analyze_taxi_usage(connection)

        print("\n========== TAXI USAGE SUMMARY ==========")
        print("Taxi Type | Total Trips | Usage (%)")

        for taxi_type, total_trips, percentage in results:
            print(
                f"{taxi_type:8} | "
                f"{total_trips:12,} | "
                f"{percentage:6.2f}%"
            )

    finally:
        connection.close()