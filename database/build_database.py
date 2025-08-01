# database/build_database.py
"""
Contains the core logic for building the local SQLite database from source files.
"""

import os
import sqlite3
import sys
import time
from typing import Set

import structlog

from config import setup_logging, DB_FILE
from database.sqlite_connector import SQLiteConnector
from database.base_connector import DatabaseConnector
from processing.parser import parse_local_constants, load_alias_map
from processing.builder import (
    create_schema,
    populate_lookup_tables,
    process_cards,
    process_decks,
)

# Initialize logger if it hasn't been already
try:
    log = structlog.get_logger(__name__)
except structlog.exceptions.NotConfigured:
    setup_logging()
    log = structlog.get_logger(__name__)


def run_build_process() -> bool:
    """
    Executes the full local SQLite database build process.

    Returns:
        - bool: True if the build was successful, False otherwise.
    """
    log.info("--- Starting SQLite Database Build Process ---")
    start_time = time.time()

    # For a fresh build, always remove the old database file.
    if os.path.exists(DB_FILE):
        log.info("Removing existing SQLite database file.", file=DB_FILE)
        try:
            os.remove(DB_FILE)
        except OSError as e:
            log.error("Failed to remove existing database file.", error=str(e))
            return False

    db: DatabaseConnector | None = None
    try:
        db = SQLiteConnector()
        db.connect()

        log.info("[1/7] Creating database schema...")
        create_schema(db)
        db.commit()

        log.info("[2/7] Parsing local constants...")
        maps = parse_local_constants()
        log.info("[3/7] Populating lookup tables...")
        setcode_map = populate_lookup_tables(db, maps)
        db.commit()

        log.info("[4/7] Processing card data...")
        process_cards(db, maps, setcode_map)
        db.commit()

        log.info("[5/7] Loading card alias ID map...")
        alias_map = load_alias_map()

        log.info("[6/7] Caching valid card IDs...")
        db.execute("SELECT id FROM Cards")
        rows = db.fetchall()
        valid_card_ids: Set[int] = {row["id"] for row in rows}
        log.info(f"Loaded {len(valid_card_ids)} valid card IDs.")

        log.info("[7/7] Processing local deck files...")
        process_decks(db, valid_card_ids, alias_map)
        db.commit()

        total_time = time.time() - start_time
        log.info(
            f"--- Local Database Build Successful! ---", total_time=f"{total_time:.2f}s"
        )
        return True

    except (ConnectionError, ValueError, sqlite3.Error) as e:
        log.exception(
            "A critical database error occurred during the build process.",
            error_type=type(e).__name__,
        )
        if db:
            db.rollback()
        return False
    except Exception:
        log.exception("An unexpected error occurred during the build process.")
        if db:
            db.rollback()
        return False
    finally:
        if db:
            db.close()
