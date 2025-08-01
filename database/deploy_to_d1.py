# database/deploy_to_d1.py
"""
Handles the deployment of the locally built SQLite DB to Cloudflare D1.

This module orchestrates the entire deployment pipeline, which includes:
1.  Exporting the local SQLite database to a standard SQL text file.
2.  Interacting with the Cloudflare API to CLEAR all tables from the target database.
    IT DOES NOT DELETE THE DATABASE, preserving all bindings.
3.  Executing the multi-step D1 import process to populate the cleared database.
"""

import hashlib
import os
import subprocess
import time
from typing import Any
import re

import requests
import structlog
from cloudflare import Cloudflare

from config import DB_FILE, SQL_DUMP_FILE, get_d1_config_from_env

log = structlog.get_logger(__name__)


def dump_sqlite_to_sql() -> bool:
    """
    Exports the SQLite database to a .sql file and preprocesses it for D1 compatibility.

    This function is a prerequisite for the D1 import. It now includes a critical
    preprocessing step to handle non-standard SQL functions.

    Workflow:
    1.  Checks if the source SQLite database file exists.
    2.  Invokes the 'sqlite3' command-line utility to dump the database schema and data.
    3.  Captures the output and processes it line by line.
    4.  REMOVES transaction control statements (BEGIN, COMMIT, PRAGMA).
    5.  NEW: FINDS and REPLACES all occurrences of the custom `unistr('...')` function
        with standard, D1-compatible Unicode string literals.
    6.  Writes the fully processed and cleaned SQL commands to the output file.

    Returns:
        - bool: True if the SQL dump was created successfully, False otherwise.
    """
    if not os.path.exists(DB_FILE):
        log.error("Local database not found, cannot create SQL dump.", file=DB_FILE)
        return False

    log.info("Dumping local SQLite DB to SQL file...", source=DB_FILE, dest=SQL_DUMP_FILE)

    # Define the regex pattern to find unistr('HEX_STRING') calls.
    # It captures the hexadecimal content inside the single quotes.
    unistr_pattern = re.compile(r"unistr\('([0-9a-fA-F]+)'\)")

    def replace_unistr_match(match: re.Match) -> str:
        """
        A helper function called for each `unistr` match found by the regex.
        It converts the hex representation into a proper, escaped SQL string.
        """
        hex_string = match.group(1)
        # The hex string represents Unicode code points, typically 4 hex chars per character.
        if len(hex_string) % 4 != 0:
            return match.group(0)  # Return original if format is unexpected

        try:
            # Convert each 4-char hex sequence to an integer, then to a character.
            char_codes = [int(hex_string[i:i + 4], 16) for i in range(0, len(hex_string), 4)]
            result_string = "".join(chr(code) for code in char_codes)

            # For SQL, single quotes in the string must be escaped by doubling them.
            escaped_string = result_string.replace("'", "''")

            # Return the final string literal, wrapped in single quotes for SQL.
            return f"'{escaped_string}'"
        except ValueError:
            # If any conversion fails, return the original `unistr(...)` call to avoid breaking the line.
            return match.group(0)

    try:
        with open(SQL_DUMP_FILE, "w", encoding="utf-8") as f_out:
            # Execute `sqlite3 <db_file> .dump`
            result = subprocess.run(
                ["sqlite3", DB_FILE, "-escape","off", ".dump"],
                capture_output=True,
                text=True,
                check=True,
                encoding="utf-8",
            )

            # Process the dump output line by line
            for line in result.stdout.splitlines():
                # First, strip unsupported transaction statements
                if not line.startswith(("PRAGMA", "BEGIN", "COMMIT")):
                    # Then, process the line to replace any `unistr` calls
                    processed_line = unistr_pattern.sub(replace_unistr_match, line)
                    f_out.write(processed_line + "\n")

        log.info("SQL dump created and preprocessed for D1 successfully.")
        return True
    except FileNotFoundError:
        log.error(
            "sqlite3 command not found.",
            error_type="MissingDependency",
            details="Ensure the 'sqlite3' command-line tool is installed and in your system's PATH.",
        )
        return False
    except subprocess.CalledProcessError as e:
        log.error(
            "Failed to dump SQLite database via subprocess.",
            error_type="SubprocessError",
            stderr=e.stderr,
        )
        return False


def _find_database_by_name(
    client: Cloudflare, account_id: str, db_name: str
) -> Any | None:
    """
    A helper function to find a D1 database's details by its name.

    D1 API operations often require a database's UUID, not its human-readable name.
    This function bridges that gap.

    Workflow:
    1.  Calls the Cloudflare API to list all D1 databases for the given account.
    2.  Iterates through the list of database objects.
    3.  Compares the `name` attribute of each database with the `db_name` parameter.
    4.  If a match is found, it returns the entire database object.
    5.  If the entire list is traversed with no match, it returns None.

    Args:
        - client (Cloudflare): An authenticated `cloudflare-python` client instance.
        - account_id (str): The Cloudflare account ID.
        - db_name (str): The human-readable name of the database to find.

    Returns:
        - An object representing the database (Any) if found, otherwise None. The object
          will have attributes like `.uuid` and `.name`.
    """
    log.info("Querying for existing D1 database by name...", name=db_name)
    try:
        databases = client.d1.database.list(account_id=account_id)
        for db in databases:
            if db.name == db_name:
                log.info("Found existing D1 database.", uuid=db.uuid)
                return db
        log.info("No D1 database with the specified name was found.")
        return None
    except Exception as e:
        log.error("Failed to list D1 databases from Cloudflare API.", error=str(e))
        return None


def clear_d1_database(client: Cloudflare, account_id: str, db_id: str) -> bool:
    """
    Clears all tables from the specified D1 database without deleting the database itself.

    This non-destructive operation ensures all existing data is wiped while
    preserving the database's UUID and any external bindings (e.g., from Workers).

    Workflow:
    1.  Executes a query against the D1 database to get a list of all existing tables
        from the 'sqlite_master' schema table.
    2.  It specifically excludes SQLite's internal tables (those starting with 'sqlite_').
    3.  If no user tables are found, the operation is considered complete.
    4.  If tables exist, it constructs a batch of `DROP TABLE IF EXISTS` SQL statements.
    5.  It executes this batch query to delete all tables in a single API call.

    Args:
        - client (Cloudflare): An authenticated `cloudflare-python` client instance.
        - account_id (str): The Cloudflare account ID.
        - db_id (str): The UUID of the database to clear.

    Returns:
        - bool: True if the database was cleared successfully, False otherwise.
    """
    log.info("Attempting to clear all tables from D1 database...", database_id=db_id)
    try:
        # 1. Query for all existing table names
        list_tables_sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_%';"
        log.info("Listing tables to be dropped...", query=list_tables_sql)

        query_response = client.d1.database.query(
            account_id=account_id, database_id=db_id, sql=list_tables_sql
        )

        if not query_response or not query_response.result:
            log.error("Received an invalid or empty response when listing tables.")
            return False

        tables_to_drop = [row["name"] for row in query_response.result[0].results]

        if not tables_to_drop:
            log.info("Database is already empty. No tables to drop.")
            return True

        # 2. Generate and execute DROP TABLE statements
        log.warning("Tables to be dropped:", tables=tables_to_drop)
        drop_statements = [f"DROP TABLE IF EXISTS {name};" for name in tables_to_drop if not name.startswith("_") ]
        batch_sql = " ".join(drop_statements)
        if len(batch_sql) > 0:
            log.info("Executing batch DROP TABLE statement...")
            drop_response = client.d1.database.query(
                account_id=account_id, database_id=db_id, sql=batch_sql
            )

            if drop_response and drop_response.result and drop_response.result[0].success:
                log.info("All existing tables cleared successfully from D1 database.")
                return True
            else:
                log.error(
                    "Failed to execute DROP TABLE statements.", response=drop_response
                )
                return False
        else:
            return  True
    except Exception as e:
        log.exception(
            "An exception occurred while trying to clear the D1 database.", error=str(e)
        )
        return False


def upload_and_import_sql(account_id: str, api_token: str, db_id: str) -> bool:
    """
    Executes the full D1 file import workflow using raw HTTP requests.

    This implements the 4-step process required by the D1 import API, as the
    `cloudflare-python` library does not offer a high-level wrapper for it.

    Workflow:
    1.  **Init Upload**: Calculates the MD5 hash of the SQL file. Sends a request
        to the D1 API to initialize an import. Cloudflare validates the hash and
        returns a temporary, pre-signed R2 upload URL.
    2.  **Upload to R2**: Puts the SQL file content to the pre-signed URL. After
        the upload, it verifies that the ETag (MD5 hash) returned by R2 matches
        the one calculated locally to ensure file integrity during transit.
    3.  **Start Ingestion**: Sends another request to the D1 API to signal that
        the upload is complete and D1 should start ingesting the file from R2.
    4.  **Poll for Status**: Enters a loop, periodically querying the D1 API for
        the import job's status. The loop continues until the status becomes
        'complete' or 'error'.

    Args:
        - account_id (str): The Cloudflare account ID.
        - api_token (str): The Cloudflare API token.
        - db_id (str): The UUID of the target D1 database.

    Returns:
        - bool: True if the import process completes successfully, False otherwise.
    """
    d1_api_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{db_id}/import"
    headers = {"Authorization": f"Bearer {api_token}"}

    try:
        with open(SQL_DUMP_FILE, "rb") as f:
            sql_content = f.read()

        md5_hash = hashlib.md5(sql_content).hexdigest()
        log.info("Calculated MD5 hash for SQL file.", hash=md5_hash)

        # Step 1: Init Upload
        log.info("[1/4] Initializing D1 import...")
        init_res = requests.post(
            d1_api_url, headers=headers, json={"action": "init", "etag": md5_hash}
        )
        init_res.raise_for_status()
        upload_data = init_res.json()["result"]
        upload_url = upload_data.get("upload_url")
        if not upload_url:
            log.info("API 'init' response did not contain an 'upload_url'.",message=[upload_data])
            raise ValueError
        log.info("Import initialized, received R2 upload URL.")

        # Step 2: Upload to R2
        log.info("[2/4] Uploading SQL file to R2 presigned URL...")
        r2_res = requests.put(
            upload_url,
            data=sql_content,
            headers={"Content-Type": "application/octet-stream"},
        )
        r2_res.raise_for_status()
        r2_etag = r2_res.headers.get("ETag", "").strip('"')
        if r2_etag != md5_hash:
            raise ValueError(
                f"ETag mismatch after R2 upload. Expected {md5_hash}, got {r2_etag}"
            )
        log.info("SQL file uploaded successfully.")

        # Step 3: Start Ingestion
        log.info("[3/4] Starting D1 ingestion process...")
        ingest_res = requests.post(
            d1_api_url,
            headers=headers,
            json={
                "action": "ingest",
                "etag": md5_hash,
                "filename": upload_data["filename"],
            },
        )
        ingest_res.raise_for_status()
        ingest_data = ingest_res.json()["result"]
        current_bookmark = ingest_data.get("at_bookmark")
        if not current_bookmark:
            raise ValueError("API 'ingest' response did not include an 'at_bookmark'.")
        log.info("Ingestion started.", bookmark=current_bookmark)

        # Step 4: Polling for completion
        log.info("[4/4] Polling for import completion status...")
        while True:
            time.sleep(2)  # Wait for 2 seconds between polls
            poll_res = requests.post(
                d1_api_url,
                headers=headers,
                json={"action": "poll", "current_bookmark": current_bookmark},
            )
            poll_res.raise_for_status()
            poll_data = poll_res.json()["result"]

            if poll_data.get("status") == "complete":
                log.info("D1 import completed successfully!")
                return True
            if poll_data.get("status") == "error":
                log.error("D1 import failed.", reason=poll_data.get("error"))
                return False

            log.info(
                "Polling... import in progress.", messages=poll_data.get("messages", [poll_data])
            )

    except (requests.RequestException, ValueError) as e:
        log.exception(
            "A fatal error occurred during the D1 import process.", error=str(e)
        )
        return False
    finally:
        # Final cleanup of the temporary SQL dump file.
        if os.path.exists(SQL_DUMP_FILE):
            os.remove(SQL_DUMP_FILE)
            log.info("Cleaned up temporary SQL dump file.", file=SQL_DUMP_FILE)


def run_d1_deployment():
    """
    The main orchestrator for the D1 deployment process.

    This function now uses a NON-DESTRUCTIVE "clear and import" strategy.

    Workflow:
    1.  Loads Cloudflare credentials.
    2.  Dumps the local SQLite database to a `.sql` file.
    3.  Finds the target D1 database by its name to get its UUID.
    4.  Calls `clear_d1_database()` to wipe all tables from the target database.
    5.  Calls `upload_and_import_sql()` to migrate the new data.
    """
    log.info("--- Starting Deployment to Cloudflare D1 (Non-Destructive) ---")

    # Step 1: Load Cloudflare configuration.
    d1_config = get_d1_config_from_env()
    account_id = d1_config["d1_account_id"]
    api_token = d1_config["d1_api_token"]
    db_name = d1_config["d1_database_name"]

    # Step 2: Prepare the data dump.
    if not dump_sqlite_to_sql():
        log.error("Halting deployment due to SQL dump failure.")
        return

    # Step 3: Find the database and clear it.
    client = Cloudflare(api_token=api_token)
    database = _find_database_by_name(client, account_id, db_name)
    if not database:
        log.error(
            "Target D1 database not found. Please create it in the Cloudflare dashboard first.",
            name=db_name,
            error_type="ConfigurationError",
        )
        return

    db_id = database.uuid
    if not clear_d1_database(client, account_id, db_id):
        log.error("Halting deployment due to database clearing failure.")
        return

    # Step 4: Perform the upload and import.
    success = upload_and_import_sql(account_id, api_token, db_id)

    if success:
        log.info("--- Cloudflare D1 Deployment Successful! ---")
    else:
        log.error("--- Cloudflare D1 Deployment Failed. See logs for details. ---")
