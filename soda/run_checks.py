"""Run Soda Core quality checks against Snowflake staging/gold tables.

Exit code 1 on any check failure -- triggers Kestra pipeline failure
alerts in Stage 5. Not a bare except: only catches what Soda itself
raises.
"""

import logging
import os
import sys

from dotenv import load_dotenv
from soda.scan import Scan

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def run_soda_checks() -> int:
    scan = Scan()
    scan.set_data_source_name("nyc_311")

    # Soda Core 3.x connects via a configuration YAML string, not a
    # add_snowflake_connection() method (that API doesn't exist in 3.3.0 --
    # discovered via real AttributeError, not assumed from docs).
    # No default schema here -- checks_311.yml fully qualifies each table
    # as schema.table (e.g. "checks for staging.stg_311_requests"). Setting
    # schema here too caused Soda to double it: NYC_311.staging.staging.x
    config_yaml = f"""
data_source nyc_311:
  type: snowflake
  username: {os.getenv('SNOWFLAKE_USER')}
  password: {os.getenv('SNOWFLAKE_PASSWORD')}
  account: {os.getenv('SNOWFLAKE_ACCOUNT')}
  database: {os.getenv('SNOWFLAKE_DATABASE')}
  warehouse: {os.getenv('SNOWFLAKE_WAREHOUSE')}
  role: {os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')}
"""
    scan.add_configuration_yaml_str(config_yaml)

    scan.add_sodacl_yaml_file("soda/checks_311.yml")
    scan.execute()

    # get_checks_pass() doesn't exist in Soda Core 3.3.0 (only
    # get_checks_fail() does) -- discovered via real AttributeError.
    # Soda's own scan.execute() already logs a full pass/fail summary to
    # stdout, so we only need to pull failures for our own error logging.
    for check_result in scan.get_checks_fail():
        logging.error(f"FAILED: {check_result.check.name}")
        logging.error(f"  {check_result.outcome}")

    if scan.has_check_fails():
        logging.error(f"Soda checks FAILED: {scan.get_checks_fail_count()} failures")
        return 1

    # get_checks_pass_count() doesn't exist in Soda Core 3.3.0 either.
    # has_check_fails() is the only pass/fail signal this version exposes
    # reliably; the scan summary above already shows the real counts.
    logging.info("Soda checks PASSED: all checks green")
    return 0


if __name__ == "__main__":
    sys.exit(run_soda_checks())