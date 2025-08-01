# database/d1_connector.py
"""
Cloudflare D1 database connector implementation via the official Python SDK.

This module provides a concrete implementation of the `DatabaseConnector` for
Cloudflare's D1 serverless database. All interactions happen through the
`cloudflare` python package. This version is enhanced with raw query support
for performance and more detailed error reporting.
"""

from typing import List, Tuple, Any, Dict, Iterable

import structlog

# 严格按照文档，仅引入 `Cloudflare` 主类
from cloudflare import Cloudflare

from database.base_connector import DatabaseConnector
from config import mask_sensitive_data

log = structlog.get_logger(__name__)


class D1Connector(DatabaseConnector):
    """
    Manages connection and operations for Cloudflare D1 using the official SDK.

    This class fulfills the `DatabaseConnector` contract by translating
    standard database methods into authenticated SDK calls. It includes
    optimizations like batching for writes and raw queries for reads.
    """

    # This constant is no longer used by executemany but kept for context.
    MAX_STATEMENTS_PER_BATCH = 10000

    def __init__(self, config: Dict[str, str]):
        """
        Initializes the D1 connector using the Cloudflare SDK.

        Args:
            - config (Dict[str, str]): A dictionary with D1 credentials from `config.py`.
                                     Must contain 'd1_account_id', 'd1_database_id',
                                     and 'd1_api_token'.
        """
        self.config = config
        self.account_id = self.config["d1_account_id"]
        self.database_id = self.config["d1_database_id"]
        # Initialize the Cloudflare client with the API token per documentation.
        self.client = Cloudflare(api_token=self.config["d1_api_token"])
        self.last_results: List[Dict[str, Any] | List[Any]] = []

    def connect(self):
        """
        Confirms the D1 connector is ready. Kept for interface compatibility.

        The Cloudflare client manages its own connection state, so no explicit
        session setup is needed.
        """
        log.info(
            "Cloudflare D1 connector initialized.",
            config=mask_sensitive_data(self.config),
        )

    def close(self):
        """
        Logs closure. Kept for interface compatibility.

        The Cloudflare client does not require explicit closing.
        """
        log.info("Cloudflare D1 connection closed.")

    def execute(self, sql: str, params: Tuple[Any, ...] = ()):
        """
        Executes a single SQL statement against the D1 `/query` endpoint.
        This endpoint returns results as an array of objects (dictionaries).

        Args:
            - sql (str): The SQL query string.
            - params (Tuple): A tuple of parameters to bind to the query.
        """
        try:
            # The 'query' endpoint supports multiple statements ONLY IF params are not used.
            # For single statements, params are allowed.
            if ";" in sql.strip().rstrip(";") and params:
                log.warning(
                    "Executing multiple statements with params, which might be unsupported."
                )

            response = self.client.d1.database.query(
                database_id=self.database_id,
                account_id=self.account_id,
                sql=sql,
                params=list(params),
            )
            # Per documentation, the response contains a list of results.
            # We extract the 'results' from the first item in that list.
            results = response.result
            self.last_results = (
                results[0].results if results and hasattr(results[0], "results") else []
            )
        except Exception as e:
            log.error("D1 API query failed", error=str(e))
            raise ConnectionError(f"D1 API Error: {e}") from e

    def executemany(self, sql: str, data_list: Iterable[Tuple[Any, ...]]):
        """
        Executes the same SQL statement for each item in the data list.

        NOTE: Due to a D1 API limitation where parameterized queries cannot be
        batched into a single semicolon-separated request, this method executes
        each statement as a separate API call.
        """
        data = list(data_list)
        if not data:
            return

        log.info(
            f"Executing {len(data)} statements individually due to API limitations."
        )

        for params in data:
            try:
                # Execute each statement in a separate API call.
                self.client.d1.database.query(
                    database_id=self.database_id,
                    account_id=self.account_id,
                    sql=sql,
                    params=list(params),
                )
            except Exception as e:
                log.error(
                    "D1 API call failed during executemany loop",
                    sql=sql,
                    params=params,
                    error=str(e),
                )
                # Re-raise the exception to stop the entire build process.
                raise ConnectionError(
                    f"D1 API Error during batch execution: {e}"
                ) from e

    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Returns results from the last `execute` call as a list of dictionaries.
        """
        if self.last_results and not isinstance(self.last_results[0], dict):
            log.warning(
                "fetchall() called after a raw query. Results are not dicts. Use fetchall_raw() instead."
            )
        return self.last_results

    def commit(self):
        """No-op for D1, as each API request is auto-committed."""
        pass

    def rollback(self):
        """No-op for D1, as the API does not support manual, multi-request transactions."""
        pass

    # --- NEW AND IMPROVED METHODS ---

    def execute_raw(self, sql: str, params: Tuple[Any, ...] = ()):
        """
        Executes a single SQL statement against the D1 `/raw` endpoint.
        This is a performance-optimized endpoint that returns results as an
        array of arrays, which is faster and uses less bandwidth than objects.
        """
        try:
            response = self.client.d1.database.raw(
                database_id=self.database_id,
                account_id=self.account_id,
                sql=sql,
                params=list(params),
            )
            # The result from a /raw call contains an object with 'columns' and 'rows'.
            results = response.result
            self.last_results = (
                results[0].results if results and hasattr(results[0], "results") else []
            )
        except Exception as e:
            log.error("D1 API raw query failed", error=str(e))
            raise ConnectionError(f"D1 API Error: {e}") from e

    def fetchall_raw(self) -> List[List[Any]]:
        """
        Returns results from the last `execute_raw` call.
        """
        return self.last_results

    def get_database_details(self) -> Dict[str, Any]:
        """
        Fetches metadata about the D1 database using the `get` endpoint.
        Useful for health checks or verifying configuration.

        Returns:
            - A dictionary containing database metadata (name, version, size, etc.).
        """
        log.info("Fetching D1 database details...")
        try:
            d1_object = self.client.d1.database.get(
                database_id=self.database_id,
                account_id=self.account_id,
            )
            # The documentation states the 'get' method returns a D1 object.
            # We convert this object to a dictionary to match the method's return type.
            return {
                "uuid": getattr(d1_object, "uuid", None),
                "name": getattr(d1_object, "name", None),
                "version": getattr(d1_object, "version", None),
                "file_size": getattr(d1_object, "file_size", None),
                "num_tables": getattr(d1_object, "num_tables", None),
                "read_replication": getattr(d1_object, "read_replication", None),
            }
        except Exception as e:
            log.error("Failed to get D1 database details", error=str(e))
            raise ConnectionError(f"D1 API Error: {e}") from e
