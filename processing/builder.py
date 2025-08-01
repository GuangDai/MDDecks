# processing/builder.py
"""
Core logic for building the database.

These functions are the heart of the application. They take a database
connector and populate it with data parsed from the local files. By operating
on the `DatabaseConnector` interface, this module remains independent of the
underlying database technology (SQLite, D1, etc.).
"""

import json
import os
import re
from collections import Counter
from typing import Dict, Tuple, Set, List

import structlog

from database.base_connector import DatabaseConnector
from processing.parser import parse_setcodes
from config import LOCAL_CARDS_FILE, DECK_DIR

log = structlog.get_logger(__name__)


def create_schema(db: DatabaseConnector):
    """
    Creates all necessary tables and indexes in the database.

    The schema is designed to be compatible with both SQLite and Cloudflare D1.
    It has been updated to include fields for deck cosmetics (`deckCase`, `deckProtector`)
    and cover cards to support the target API's response structure.

    Note: While the schema defines FOREIGN KEYs, their enforcement depends on the
    database backend (SQLite supports it, D1 currently does not enforce them).

    Args:
        - db (DatabaseConnector): An active database connector instance.
    """
    log.info("Creating database schema (tables and indexes)...")
    schema_statements = [
        # Stores deck metadata. deck_id is the primary key.
        # This table includes cosmetic and cover card data parsed from the YDK.
        """CREATE TABLE IF NOT EXISTS Decks (
            deck_id TEXT PRIMARY KEY,
            deck_name TEXT NOT NULL,
            user_id INTEGER,
            deck_contributor TEXT,
            deck_like INTEGER DEFAULT 0,
            upload_date INTEGER,
            update_date INTEGER,
            is_public INTEGER DEFAULT 1,
            deck_ydk TEXT,
            deckCase INTEGER DEFAULT 0,
            deckProtector INTEGER DEFAULT 0,
            deckCoverCard1 INTEGER DEFAULT 0,
            deckCoverCard2 INTEGER DEFAULT 0,
            deckCoverCard3 INTEGER DEFAULT 0
        )""",
        # Stores primary information for every unique card.
        """CREATE TABLE IF NOT EXISTS Cards (
            id INTEGER PRIMARY KEY, cid INTEGER, cn_name TEXT NOT NULL,
            jp_name TEXT, en_name TEXT, card_text_types TEXT,
            card_text_desc TEXT, atk INTEGER, def INTEGER, level INTEGER
        )""",
        # Lookup tables for static data. These store descriptive names for integer codes.
        "CREATE TABLE IF NOT EXISTS Races (race_code INTEGER PRIMARY KEY, race_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS Attributes (attribute_code INTEGER PRIMARY KEY, attribute_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS CardTypes (type_code INTEGER PRIMARY KEY, type_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS Setcodes (set_code INTEGER PRIMARY KEY, set_name_cn TEXT, set_name_jp TEXT)",
        # Link tables for many-to-many relationships. These connect cards and decks.
        "CREATE TABLE IF NOT EXISTS DeckCards (deck_id TEXT NOT NULL, card_id INTEGER NOT NULL, card_type TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (deck_id, card_id, card_type))",
        "CREATE TABLE IF NOT EXISTS CardToRace (card_id INTEGER NOT NULL, race_code INTEGER NOT NULL, PRIMARY KEY (card_id, race_code))",
        "CREATE TABLE IF NOT EXISTS CardToAttribute (card_id INTEGER NOT NULL, attribute_code INTEGER NOT NULL, PRIMARY KEY (card_id, attribute_code))",
        "CREATE TABLE IF NOT EXISTS CardToType (card_id INTEGER NOT NULL, type_code INTEGER NOT NULL, PRIMARY KEY (card_id, type_code))",
        "CREATE TABLE IF NOT EXISTS CardToSetcode (card_id INTEGER NOT NULL, set_code INTEGER NOT NULL, PRIMARY KEY (card_id, set_code))",
        # Indexes to dramatically speed up common search and sort operations.
        "CREATE INDEX IF NOT EXISTS idx_decks_user_id ON Decks(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_decks_like ON Decks(deck_like)",
        "CREATE INDEX IF NOT EXISTS idx_decks_update_date ON Decks(update_date)",
        "CREATE INDEX IF NOT EXISTS idx_deckcards_card_id ON DeckCards(card_id)",
    ]
    for statement in schema_statements:
        db.execute(statement)
    log.info("Schema creation complete.")


def populate_lookup_tables(
    db: DatabaseConnector, maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]]
) -> Dict[int, str]:
    """
    Populates the static lookup tables (Races, Attributes, Types, Setcodes).

    These tables store static, descriptive data that is referenced by other tables.
    Populating them first allows for foreign key relationships to be established.

    Workflow:
    1.  Receives the pre-parsed maps for Race, Attribute, and Type.
    2.  Uses `executemany` to efficiently bulk-insert this data. `INSERT OR IGNORE`
        is used to prevent errors if the data already exists.
    3.  Calls the `parse_setcodes` function to get setcode data.
    4.  Bulk-inserts the setcode data.
    5.  Returns the `setcode_map` for use in the next processing step.

    Args:
        - db (DatabaseConnector): An active database connector instance.
        - maps (Tuple): A tuple of `(race_map, attribute_map, type_map)`.

    Returns:
        - The `setcode_map` (Dict[int, str]) for use in card processing.
    """
    log.info("Populating lookup tables (Races, Attributes, Types, Setcodes)...")
    race_map, attribute_map, type_map = maps

    db.executemany("INSERT OR IGNORE INTO Races VALUES (?, ?)", race_map.items())
    db.executemany(
        "INSERT OR IGNORE INTO Attributes VALUES (?, ?)", attribute_map.items()
    )
    db.executemany("INSERT OR IGNORE INTO CardTypes VALUES (?, ?)", type_map.items())

    setcode_map, setcodes_to_insert = parse_setcodes()
    if setcodes_to_insert:
        db.executemany(
            "INSERT OR IGNORE INTO Setcodes VALUES (?, ?, ?)", setcodes_to_insert
        )

    log.info("Lookup tables populated.")
    return setcode_map


def process_cards(
    db: DatabaseConnector,
    maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]],
    setcode_map: Dict[int, str],
):
    """
    Processes `cards.json` and populates the `Cards` table and its relation tables.

    This is a major data processing step. It reads the entire card database,
    transforms the data, and prepares it for efficient batch insertion.

    Workflow:
    1.  Loads the entire `cards.json` file into memory. (File size is ~30-40MB,
        which is acceptable for modern systems).
    2.  Initializes several empty lists to act as in-memory buffers for the data
        that will be inserted into each table.
    3.  Iterates through every card in the JSON data. For each card:
        a. Extracts the primary card data and appends it as a tuple to `cards_to_insert`.
        b. For bitmask fields (Race, Attribute, Type), it performs a bitwise AND
           operation (`&`) against every known code from the parsed maps. If the
           result matches the code, it signifies a relationship, which is appended
           to the appropriate relation list (e.g., `card_races`).
        c. For the `setcode`, it performs a simple, exact-match lookup in the `setcode_map`.
    4.  After iterating through all ~15,000+ cards, it performs a series of
        high-performance `executemany` calls to bulk-insert the contents of the
        buffered lists into the database. `INSERT OR REPLACE` is used for the `Cards`
        table to ensure the latest data is present, while `INSERT OR IGNORE` is
        used for relation tables.

    Args:
        - db (DatabaseConnector): An active database connector.
        - maps (Tuple): A tuple of `(race_map, attribute_map, type_map)`.
        - setcode_map (Dict[int, str]): A dictionary mapping setcodes to names.
    """
    log.info("Processing card data from cards.json...", file=LOCAL_CARDS_FILE)
    race_map, attribute_map, type_map = maps

    try:
        with open(LOCAL_CARDS_FILE, "r", encoding="utf-8") as f:
            all_cards_data = json.load(f)
    except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
        log.error(
            "Failed to read or parse cards.json, cannot process cards.", error=str(e)
        )
        return

    # In-memory buffers for batch insertion.
    cards_to_insert, card_races, card_attrs, card_types, card_setcodes = (
        [],
        [],
        [],
        [],
        [],
    )

    for _, data in all_cards_data.items():
        card_id = data.get("id")
        if not card_id:
            log.warning("Skipping card with no ID.", card_data=data)
            continue

        cards_to_insert.append(
            (
                card_id,
                data.get("cid"),
                data.get("cn_name"),
                data.get("jp_name"),
                data.get("en_name"),
                data.get("text", {}).get("types"),
                data.get("text", {}).get("desc"),
                data.get("data", {}).get("atk"),
                data.get("data", {}).get("def"),
                data.get("data", {}).get("level"),
            )
        )

        d = data.get("data", {})
        # Process bitmask fields.
        for code in race_map:
            if (d.get("race", 0) & code) == code:
                card_races.append((card_id, code))
        for code in attribute_map:
            if (d.get("attribute", 0) & code) == code:
                card_attrs.append((card_id, code))
        for code in type_map:
            if (d.get("type", 0) & code) == code:
                card_types.append((card_id, code))

        # Process setcode.
        card_setcode_value = d.get("setcode", 0)
        if card_setcode_value and card_setcode_value in setcode_map:
            card_setcodes.append((card_id, card_setcode_value))

    # Bulk insert all buffered data.
    log.info(f"Inserting {len(cards_to_insert)} cards and their relations...")
    db.executemany(
        "INSERT OR REPLACE INTO Cards VALUES (?,?,?,?,?,?,?,?,?,?)", cards_to_insert
    )
    db.executemany("INSERT OR IGNORE INTO CardToRace VALUES (?,?)", card_races)
    db.executemany("INSERT OR IGNORE INTO CardToAttribute VALUES (?,?)", card_attrs)
    db.executemany("INSERT OR IGNORE INTO CardToType VALUES (?,?)", card_types)
    db.executemany("INSERT OR IGNORE INTO CardToSetcode VALUES (?,?)", card_setcodes)
    log.info("Card data processing complete.")


def process_decks(
    db: DatabaseConnector, valid_card_ids: Set[int], alias_map: Dict[int, int]
):
    """
    Scans the deck directory, validates decks, and populates the database.

    This function reads local deck files, validates that every card within them
    is a known card, parses cosmetic data, and then adds the valid decks to the database.

    Workflow:
    1.  Checks if the `DECK_DIR` exists.
    2.  Iterates through every `.json` file in the directory. For each file:
        a. Reads and parses the deck's JSON data.
        b. Extracts the deck's YDK string (a standard deck format).
        c. Performs a stateful parse of the YDK string, keeping track of the
           current section (`#main`, `#extra`, `!side`) to collect card IDs.
        d. For each card ID found, it resolves its alias and validates its existence
           in `valid_card_ids`. If any card is invalid, the entire deck is discarded.
        e. If the deck is valid, it uses regex to parse cosmetic data (case, protector)
           from special comment lines in the YDK string.
        f. It uses the first 3 cards from the main deck as a reasonable approximation
           for the deck's cover cards.
        g. It adds the full deck metadata (including cosmetics) to the `decks_to_insert` buffer.
        h. It uses `collections.Counter` to efficiently count card occurrences
           for insertion into the `DeckCards` link table.
    3.  After processing all deck files, it performs two large `executemany` calls
        to insert all valid decks and their card lists into the database.

    Args:
        - db (DatabaseConnector): An active database connector.
        - valid_card_ids (Set[int]): A set of all known card IDs for fast validation.
        - alias_map (Dict[int, int]): A map of alias IDs to original IDs.
    """
    if not os.path.isdir(DECK_DIR):
        log.warning(
            "Deck data directory not found, skipping deck processing.",
            directory=DECK_DIR,
        )
        return

    log.info("Scanning and processing deck files...", directory=DECK_DIR)
    decks_to_insert, deck_cards_to_insert = [], []
    total_files, successful_decks = 0, 0

    for filename in os.listdir(DECK_DIR):
        if not filename.endswith(".json"):
            continue

        total_files += 1
        filepath = os.path.join(DECK_DIR, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            log.warning(
                "Skipping invalid or unreadable deck file.", file=filepath, error=str(e)
            )
            continue

        deck_id, deck_name = data.get("deckId"), data.get("deckName", "Unknown Name")
        if not deck_id:
            log.warning("Skipping deck file with no deckId.", file=filepath)
            continue

        is_deck_valid = True
        resolved_cards: Dict[str, List[int]] = {"main": [], "extra": [], "side": []}
        ydk_str = data.get("deckYdk", "")
        current_section: List[int] | None = None
        main_deck_cards_for_cover = []

        # --- YDK Parsing for Card IDs ---
        for line in ydk_str.splitlines():
            line = line.strip()
            if not line:
                continue

            # State machine for parsing YDK sections.
            if line.startswith("#main"):
                current_section = resolved_cards["main"]
                continue
            if line.startswith("#extra"):
                current_section = resolved_cards["extra"]
                continue
            if line.startswith("!side"):
                current_section = resolved_cards["side"]
                continue
            if line.startswith("#") or current_section is None:
                continue

            try:
                card_id = int(line)
                # Validation Step: Resolve alias, then check for existence.
                original_id = alias_map.get(card_id, card_id)
                if original_id in valid_card_ids:
                    current_section.append(original_id)
                    # Also collect main deck cards to determine cover cards later.
                    if current_section is resolved_cards["main"]:
                        main_deck_cards_for_cover.append(original_id)
                else:
                    log.warning(
                        "Deck contains an invalid card ID. Discarding deck.",
                        deck_name=deck_name,
                        deck_id=deck_id,
                        invalid_id=card_id,
                    )
                    is_deck_valid = False
                    break  # Stop processing this deck immediately.
            except ValueError:
                log.warning(
                    "Skipping non-integer line in YDK section.",
                    line=line,
                    deck_id=deck_id,
                )

        if is_deck_valid and len(resolved_cards["main"]) > 5 :
            successful_decks += 1

            # --- Parse cosmetic and cover data from the YDK string ---
            deck_case_match = re.search(r"#case\s*(\d+)", ydk_str)
            deck_protector_match = re.search(r"#protector\s*(\d+)", ydk_str)
            deck_case = int(deck_case_match.group(1)) if deck_case_match else 0
            deck_protector = (
                int(deck_protector_match.group(1)) if deck_protector_match else 0
            )

            # Use the first 3 cards of the main deck as cover cards, a reasonable approximation.
            covers = main_deck_cards_for_cover[:3]
            deck_cover1 = covers[0] if len(covers) > 0 else 0
            deck_cover2 = covers[1] if len(covers) > 1 else 0
            deck_cover3 = covers[2] if len(covers) > 2 else 0

            decks_to_insert.append(
                (
                    deck_id,
                    deck_name,
                    data.get("userId"),
                    data.get("deckContributor"),
                    data.get("deckLike", 0),
                    data.get("deckUploadDate"),
                    data.get("deckUpdateDate"),
                    1 if data.get("isPublic", True) else 0,
                    ydk_str,
                    deck_case,
                    deck_protector,
                    deck_cover1,
                    deck_cover2,
                    deck_cover3,
                )
            )
            # Use Counter to efficiently aggregate card counts (e.g., 3x Ash Blossom).
            for card_type, id_list in resolved_cards.items():
                for card_id, count in Counter(id_list).items():
                    deck_cards_to_insert.append((deck_id, card_id, card_type, count))

    log.info(
        "Deck processing stats",
        total_files=total_files,
        successful_decks=successful_decks,
        skipped_decks=total_files - successful_decks,
    )

    log.info(f"Inserting {len(decks_to_insert)} decks and their card lists...")
    # The INSERT statement now has 14 placeholders for the 14 columns.
    db.executemany(
        "INSERT OR REPLACE INTO Decks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        decks_to_insert,
    )
    db.executemany(
        "INSERT OR REPLACE INTO DeckCards VALUES (?,?,?,?)", deck_cards_to_insert
    )
    log.info("Deck data insertion complete.")
