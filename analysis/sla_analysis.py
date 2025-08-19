"""Queries gold.fct_sla_trends and prints the formatted finding.

Numbers come from live Snowflake gold tables -- measured, not fabricated.
Answers the project's core question: which NYC agencies are
systematically failing service level targets, and is it getting worse?
"""

import logging
import os

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

QUERY = """
    SELECT
        complaint_type,
        responsible_agency,
        breach_rate_y_minus_4,
        breach_rate_y_minus_3,
        breach_rate_y_minus_2,
        breach_rate_y_minus_1,
        breach_rate_current,
        trend_direction
    FROM NYC_311.gold.fct_sla_trends
    ORDER BY breach_rate_current DESC
"""


def fetch_trends() -> list[tuple]:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    try:
        cursor = conn.cursor()
        cursor.execute(QUERY)
        return cursor.fetchall()
    finally:
        conn.close()


def print_report(rows: list[tuple]) -> None:
    print("=" * 60)
    print("  NYC 311 SLA Breach Analysis")
    print("  Measured from Real Pipeline Data (2021-2025)")
    print("=" * 60)
    print()

    for i, row in enumerate(rows, start=1):
        (complaint_type, agency, y4, y3, y2, y1, current, trend) = row
        print(f"{i}. {complaint_type} ({agency})")
        print(
            f"   2021: {y4:.1%}  2022: {y3:.1%}  2023: {y2:.1%}  "
            f"2024: {y1:.1%}  2025: {current:.1%}"
        )
        print(f"   Trend: {trend.upper()}")
        print()

    print("=" * 60)
    print("Reproduce: python analysis/sla_analysis.py")
    print("=" * 60)


def main() -> None:
    rows = fetch_trends()
    if not rows:
        logging.error("fct_sla_trends returned no rows -- check dbt run succeeded")
        return
    print_report(rows)


if __name__ == "__main__":
    main()
