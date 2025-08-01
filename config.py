# config.py
"""
Centralized configuration management, project-wide constants, and logging setup.

This module acts as the single source of truth for configurable parameters,
preventing the use of "magic strings" or numbers throughout the application.
It also initializes the application's logging system to ensure consistent,
structured, and informative logs from all modules.
"""

import logging
import os
import sys
from typing import Dict, Any

import structlog

# --- Project Constants ---

# --- File and Directory Names ---
# Defines the standard names for files and directories used by the application.
# This makes it easy to change a filename in one place if needed.
DB_FILE = os.path.join(
    os.path.dirname(__file__), "data/yugioh_decks.db"
)  # Default filename for the SQLite database.
DECK_DIR = os.path.join(
    os.path.dirname(__file__), "deck_data"
)  # Directory where raw deck JSON files are stored.
UPDATE_INFO_FILE = os.path.join(
    os.path.dirname(__file__), "data/update_info.json"
)  # Cache file to store update timestamps and MD5 hashes.

LOCAL_CARDS_FILE = os.path.join(
    os.path.dirname(__file__), "data/cards.json"
)  # Local copy of the main card data file.
LOCAL_SETCODES_FILE = os.path.join(
    os.path.dirname(__file__), "data/strings.conf"
)  # Local copy of the setcode definitions.
LOCAL_CONSTANTS_FILE = os.path.join(
    os.path.dirname(__file__), "data/constant.lua"
)  # Local copy of game constants (Race, Attribute, etc.).

LOCAL_ALIAS_DB_FILE = os.path.join(
    os.path.dirname(__file__), "data/cards.cdb"
)  # Local copy of the alias/alternate artwork database.

SQL_DUMP_FILE = os.path.join(os.path.dirname(__file__), "data/yugioh_decks_dump.sql")

# --- Update Behavior ---
# Controls how often the application checks for new data.
UPDATE_INTERVAL_SECONDS = (
    10 * 86400
)  # 10 days in seconds. Used to avoid spamming the data source APIs.

# --- Data Source URLs ---
# A dictionary mapping a logical name to its remote URL. Centralizing these
# makes it simple to update a data source if its URL changes.
URLS: Dict[str, str] = {
    # ZIP file containing the primary cards.json data.
    "cards_zip": "https://ygocdb.com/api/v0/cards.zip",
    # A small file containing the MD5 hash of the latest cards.zip. Used for efficient update checking.
    "cards_md5": "https://ygocdb.com/api/v0/cards.zip.md5?callback=gu",
    # Raw text file defining set names and their corresponding hex codes.
    "setcodes": "https://raw.githubusercontent.com/Fluorohydride/ygopro/refs/heads/master/strings.conf",
    # Lua script defining game constants like Race, Attribute, and Type as bitmasks.
    "constants": "https://raw.githubusercontent.com/Fluorohydride/ygopro-scripts/refs/heads/master/constant.lua",
    # A SQLite database containing mappings for alternate card artworks to their original IDs.
    "alias_db": "https://code.moenext.com/mycard/ygopro-database/-/raw/master/locales/zh-CN/cards.cdb",
}


# --- Logging Setup ---


def setup_logging():
    """
    Configures structlog for rich, context-aware, and structured logging.

    Workflow:
    1.  Sets up Python's standard logging module as the base.
    2.  Configures structlog to wrap this base logger.
    3.  Defines a chain of "processors" that enrich and format log records before output.
        - This chain adds context, timestamps, log levels, and exception information.
    4.  The final processor (`ConsoleRenderer`) formats the log record into a
        human-readable, colorized line for development environments.
    """
    # Step 1: Configure the standard library's logging.
    # structlog will pass its final, processed log records to this handler.
    logging.basicConfig(
        level=logging.INFO,  # Set the minimum level of messages to handle.
        format="%(message)s",  # The format is simple as structlog handles the complex parts.
        stream=sys.stdout,  # Log to the console.
    )

    # Step 2: Configure structlog's processor chain.
    # Processors are functions that receive the log record and can modify it.
    # They are executed in the order they are listed.
    structlog.configure(
        processors=[
            # Merges context from `structlog.contextvars` into the event dict.
            # Allows adding context to all subsequent logs within a block.
            structlog.contextvars.merge_contextvars,
            # Adds the logger's name (e.g., 'data_management.updater') to the record.
            structlog.stdlib.add_logger_name,
            # Adds the log level (e.g., 'info', 'error') to the record.
            structlog.stdlib.add_log_level,
            # Adds a timestamp to the record. `fmt="iso"` gives `YYYY-MM-DDTHH:MM:SS`.
            structlog.processors.TimeStamper(fmt="iso"),
            # If the log record contains exception info, this renders it into a string.
            structlog.processors.format_exc_info,
            # The final step: Renders the structured log record into a beautiful,
            # colorized console output, perfect for development.
            # For production, this could be swapped with `structlog.processors.JSONRenderer()`.
            structlog.dev.ConsoleRenderer(colors=True),
        ],
        # `BoundLogger` is the standard wrapper that makes the logging API work.
        wrapper_class=structlog.stdlib.BoundLogger,
        # `LoggerFactory` creates standard `logging.Logger` instances.
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Caching the logger instance improves performance slightly.
        cache_logger_on_first_use=True,
    )


# --- Cloudflare D1 Configuration ---


def get_d1_config_from_env() -> Dict[str, str]:
    """
    Loads Cloudflare D1 configuration from environment variables.

    This is a secure method for CI/CD environments as it avoids hardcoding
    secrets in the source code.

    Workflow:
    1.  Reads the values of 'D1_ACCOUNT_ID', 'D1_DATABASE_NAME', and 'D1_API_TOKEN'
        from the environment.
    2.  Checks if any of these values are missing.
    3.  If any are missing, it terminates the application.
    4.  If all are present, it returns them in a dictionary.

    Expected Input:
    - Environment variables:
        - D1_ACCOUNT_ID: Your Cloudflare account ID.
        - D1_DATABASE_NAME: The target D1 database name (e.g., "yugioh-database").
        - D1_API_TOKEN: A Cloudflare API token with D1 read/write permissions.

    Returns:
        - A dictionary containing the credentials.
          Example: {'d1_account_id': '...', 'd1_database_name': '...', 'd1_api_token': '...'}

    Raises:
        - SystemExit: If any of the required environment variables are not set.
    """
    log = structlog.get_logger("config.d1")
    # CHANGED: Now reads D1_DATABASE_NAME instead of D1_DATABASE_ID for robust deployment.
    config = {
        "d1_account_id": os.getenv("D1_ACCOUNT_ID"),
        "d1_database_name": os.getenv("D1_DATABASE_NAME"),
        "d1_api_token": os.getenv("D1_API_TOKEN"),
    }

    missing_keys = [key for key, value in config.items() if not value]
    if missing_keys:
        log.error(
            "D1 config missing from environment",
            missing_keys=[key.upper() for key in missing_keys],
            error_type="ConfigurationError",
        )
        sys.exit("Error: Required environment variables for D1 are not set. Exiting.")

    log.info(
        "D1 configuration loaded successfully from environment variables",
        account_id=config["d1_account_id"],
        database_name=config["d1_database_name"],
    )
    return config


def mask_sensitive_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Creates a copy of a dictionary and masks sensitive values for safe logging.

    This is a security utility to prevent accidental leakage of secrets like
    API tokens or passwords into log files or console output.

    Workflow:
    1.  Creates a shallow copy of the input dictionary to avoid side effects.
    2.  Defines a list of substrings that indicate a key is sensitive (e.g., 'token').
    3.  Iterates through the copied dictionary.
    4.  If a key's name contains one of the sensitive substrings, its value is
        replaced with '***REDACTED***'.
    5.  Returns the sanitized copy.

    Args:
        - data (Dict[str, Any]): The dictionary to process.

    Returns:
        - A new dictionary (Dict[str, Any]) with sensitive values redacted.
    """
    sensitive_keys = ["token", "password", "key", "id"]
    # Create a shallow copy to avoid modifying the original dictionary in place.
    safe_data = data.copy()
    for key, value in safe_data.items():
        # Check if any of the sensitive substrings are in the key name (case-insensitive).
        if any(sens_key in key.lower() for sens_key in sensitive_keys):
            if isinstance(value, str):
                safe_data[key] = "***REDACTED***"
    return safe_data
