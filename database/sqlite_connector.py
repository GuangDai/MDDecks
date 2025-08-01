# database/sqlite_connector.py
"""
SQLite database connector implementation.

This module provides a concrete implementation of the `DatabaseConnector`
for interacting with a local SQLite database file. It wraps the standard
`sqlite3` library.
"""

import sqlite3
from typing import List, Tuple, Any, Iterable, Dict

import structlog

from database.base_connector import DatabaseConnector
from config import DB_FILE

log = structlog.get_logger(__name__)


class SQLiteConnector(DatabaseConnector):
    """
    Manages connection and operations for a local SQLite database.

    This class fulfills the `DatabaseConnector` contract for SQLite.
    """

    def __init__(self, db_path: str = DB_FILE):
        """
        Initializes the SQLite connector.

        Args:
            - db_path (str): The file path for the SQLite database.
                             Defaults to the value in `config.py`.
        """
        self.db_path = db_path
        self.conn: sqlite3.Connection | None = None
        self.cursor: sqlite3.Cursor | None = None

    def connect(self):
        """
        Establishes the connection to the SQLite file and creates a cursor.

        Workflow:
        1.  Calls `sqlite3.connect()` with the database file path.
        2.  Sets the connection's `row_factory` to `sqlite3.Row`. This is a crucial
            step that makes rows behave like dictionaries (e.g., `row['id']`)
            instead of tuples (`row[0]`), which makes code much more readable.
        3.  Creates a cursor object to execute commands.
        4.  Enables foreign key support with a PRAGMA command for data integrity.
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row  # Enable dictionary-like row access.
            self.cursor = self.conn.cursor()
            # This PRAGMA is essential for enforcing foreign key constraints.
            self.execute("PRAGMA foreign_keys = ON;")
            log.info("SQLite connection successful.", path=self.db_path)
        except sqlite3.Error as e:
            log.exception("Failed to connect to SQLite database.", error=str(e))
            # Re-raise the exception to be handled by the main application loop.
            raise

    def close(self):
        """Closes the database connection if it is open."""
        if self.conn:
            self.conn.close()
            log.info("SQLite connection closed.")

    def execute(self, sql: str, params: Tuple[Any, ...] = ()):
        """Executes a single SQL statement using the internal cursor."""
        if not self.cursor:
            raise ConnectionError("Database not connected. Call connect() first.")
        try:
            self.cursor.execute(sql, params)
        except sqlite3.Error as e:
            log.error("SQLite execution error.", sql=sql, error=str(e))
            raise

    def executemany(self, sql: str, data_list: Iterable[Tuple[Any, ...]]):
        """Executes a SQL statement for each item in data_list using the cursor."""
        if not self.cursor:
            raise ConnectionError("Database not connected. Call connect() first.")
        try:
            self.cursor.executemany(sql, data_list)
        except sqlite3.Error as e:
            log.error("SQLite executemany error.", sql=sql, error=str(e))
            raise

    def fetchall(self) -> List[Dict[str, Any]]:
        """
        Fetches all rows from the last query as a list of dictionaries.

        Thanks to `row_factory = sqlite3.Row`, we can easily convert each
        `sqlite3.Row` object into a standard Python dictionary.
        """
        if not self.cursor:
            raise ConnectionError("Database not connected. Call connect() first.")
        return [dict(row) for row in self.cursor.fetchall()]

    def commit(self):
        """Commits the current transaction using the connection object."""
        if self.conn:
            self.conn.commit()

    def rollback(self):
        """Rolls back the current transaction using the connection object."""
        if self.conn:
            self.conn.rollback()
