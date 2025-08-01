# database/base_connector.py
"""
Defines the abstract base class for all database connectors.

This module provides the `DatabaseConnector` Abstract Base Class (ABC).
By defining a standard interface (a "contract"), we ensure that any database
implementation (like SQLite or Cloudflare D1) will have the same public methods.
This allows the main application logic to be completely decoupled from the
database technology, making it easy to swap or add new database backends
in the future without rewriting the core data processing code.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple, Any, Dict, Iterable


class DatabaseConnector(ABC):
    """
    Abstract Base Class that defines the interface for database connectors.

    Any class that inherits from DatabaseConnector MUST implement all methods
    decorated with `@abstractmethod`.
    """

    @abstractmethod
    def connect(self):
        """
        Establishes a connection to the database.

        An implementing class should handle all setup required to start
        communicating with the database, such as opening a file, establishing
        a network session, or authenticating.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Closes the database connection and releases any resources.

        This method should handle cleanup, like closing file handles or
        terminating network sessions.
        """
        pass

    @abstractmethod
    def execute(self, sql: str, params: Tuple[Any, ...] = ()):
        """
        Executes a single SQL statement.

        This method is for running individual queries, typically DDL (like
        CREATE TABLE) or DML (like a single INSERT or a SELECT).

        Args:
            - sql (str): The SQL query to execute.
            - params (Tuple[Any, ...]): A tuple of parameters to be safely
                                        bound to the query to prevent SQL injection.
        """
        pass

    @abstractmethod
    def executemany(self, sql: str, data_list: Iterable[Tuple[Any, ...]]):
        """
        Executes a SQL statement against all parameter sequences in an iterable.

        This method is a critical performance optimization for inserting large
        volumes of data. It is significantly faster than calling `execute` in a loop.

        Args:
            - sql (str): The SQL query to execute (e.g., "INSERT INTO ... VALUES (?, ?)").
            - data_list (Iterable[Tuple[Any, ...]]): An iterable of parameter tuples,
                                                      where each tuple represents one row.
        """
        pass

    @abstractmethod
    def fetchall(self) -> List[Dict[str, Any] | Tuple[Any, ...]]:
        """
        Fetches all rows from the result of the last executed SELECT query.

        Returns:
            - A list of rows. The specific format of each row (e.g., a dictionary
              with column names as keys, or a simple tuple) depends on the
              concrete implementation of the connector.
        """
        pass

    @abstractmethod
    def commit(self):
        """
        Commits the current transaction to the database.

        For transactional databases like SQLite, this makes all changes since
        the last commit permanent. For auto-committing APIs like D1, this
        method may be a no-op.
        """
        pass

    @abstractmethod
    def rollback(self):
        """
        Rolls back the current transaction, discarding any recent changes.

        This is used for error handling. If a step in a multi-part process
        fails, this method can be called to undo the previous steps in the
        transaction. For auto-committing APIs, this may be a no-op.
        """
        pass
