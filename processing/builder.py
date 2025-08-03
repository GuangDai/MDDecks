# processing/builder.py

"""
Core logic for building the database.

This module contains the entire pipeline for processing raw data files (cards, decks)
and populating a structured database. It operates on a `DatabaseConnector` interface,
making it independent of the underlying database technology (SQLite, D1, etc.).

The build process is structured as a sequential pipeline:
1.  Create Schema: Define all necessary tables and indexes.
2.  Populate Lookups: Insert static data like card types, races, etc.
3.  Process Cards: Parse card data, build a card-level search index with optimizations.
4.  Process Decks: Scan, validate, merge duplicates, and insert deck data.
5.  Build Unified Index: Create the final, comprehensive search index for decks.

Each step is managed by a public function, which in turn calls a series of private
helper functions to perform specific, modular tasks.
"""

import json
import os
import re
from collections import Counter, defaultdict
from typing import Dict, Tuple, Set, List, Any

import structlog

from database.base_connector import DatabaseConnector
from processing.parser import parse_setcodes
from config import LOCAL_CARDS_FILE, DECK_DIR, PREDEFINED_KEYWORDS_FILE

log = structlog.get_logger(__name__)

# ==============================================================================
# == CONFIGURATION CONSTANTS
# ==============================================================================

# Configuration for search index generation
KEYWORD_MIN_CARDS = (
    2  # A keyword must be associated with at least this many cards to be indexed.
)
KEYWORD_MAX_CARDS = 500  # Keywords associated with more cards than this are considered too generic and are ignored.
SUBSET_POINTER_MAX_DIFF = 20  # Max difference in card count for creating a subset pointer in the keyword index.

# Configuration for Unified Index Compression
DECK_TERM_THRESHOLD = 2  # A deck must contain at least this many cards related to a term to be indexed by that term.


# ==============================================================================
# == STAGE 1: SCHEMA CREATION
# ==============================================================================


def create_schema(db: DatabaseConnector):
    """
    Initializes the database by creating all tables and performance-critical indexes.

    Detailed Explanation:
    This function serves as the blueprint for the entire database structure. It is designed
    to be executed once at the very beginning of a fresh build. It defines not only the
    core data tables (`Decks`, `Cards`, etc.) but also the crucial, pre-computed search
    index tables (`SearchKeywords`, `SearchIndexToDecks`) that are essential for achieving
    high query performance without relying on database-native FTS.

    Workflow:
    1.  A comprehensive list of SQL `CREATE TABLE` and `CREATE INDEX` commands is defined.
    2.  The function iterates through this list and executes each command using the
        provided database connector.
    3.  After execution, the database structure is in place and ready for data population.

    Args:
        - db (DatabaseConnector): An active database connector instance.
    """
    log.info("STAGE 1: Creating database schema (tables and indexes)...")
    schema_statements = [
        # Stores deck metadata. deck_id is the primary key.
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
        # Stores primary information for every unique card, including all name variants.
        """CREATE TABLE IF NOT EXISTS Cards (
            id INTEGER PRIMARY KEY, 
            cid INTEGER UNIQUE, 
            cn_name TEXT,
            sc_name TEXT,
            md_name TEXT,
            nwbbs_n TEXT,
            cnocg_n TEXT,
            jp_name TEXT, 
            en_name TEXT, 
            card_text_types TEXT,
            card_text_desc TEXT, 
            card_text_pdesc TEXT, 
            atk INTEGER, 
            def INTEGER, 
            level INTEGER
        )""",
        # Lookup tables for static data.
        "CREATE TABLE IF NOT EXISTS Races (race_code INTEGER PRIMARY KEY, race_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS Attributes (attribute_code INTEGER PRIMARY KEY, attribute_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS CardTypes (type_code INTEGER PRIMARY KEY, type_name TEXT NOT NULL UNIQUE)",
        "CREATE TABLE IF NOT EXISTS Setcodes (set_code INTEGER PRIMARY KEY, set_name_cn TEXT, set_name_jp TEXT)",
        # Link tables for many-to-many relationships.
        "CREATE TABLE IF NOT EXISTS DeckCards (deck_id TEXT NOT NULL, card_id INTEGER NOT NULL, card_type TEXT NOT NULL, count INTEGER NOT NULL, PRIMARY KEY (deck_id, card_id, card_type))",
        "CREATE TABLE IF NOT EXISTS CardToRace (card_id INTEGER NOT NULL, race_code INTEGER NOT NULL, PRIMARY KEY (card_id, race_code))",
        "CREATE TABLE IF NOT EXISTS CardToAttribute (card_id INTEGER NOT NULL, attribute_code INTEGER NOT NULL, PRIMARY KEY (card_id, attribute_code))",
        "CREATE TABLE IF NOT EXISTS CardToType (card_id INTEGER NOT NULL, type_code INTEGER NOT NULL, PRIMARY KEY (card_id, type_code))",
        "CREATE TABLE IF NOT EXISTS CardToSetcode (card_id INTEGER NOT NULL, set_code INTEGER NOT NULL, PRIMARY KEY (card_id, set_code))",
        # --- Advanced Pre-computed Search Index Tables ---
        """CREATE TABLE IF NOT EXISTS SearchKeywords (
            keyword TEXT PRIMARY KEY,
            card_count INTEGER NOT NULL,
            pointer_to TEXT,
            FOREIGN KEY(pointer_to) REFERENCES SearchKeywords(keyword)
        )""",
        """CREATE TABLE IF NOT EXISTS KeywordToCard (
            keyword TEXT NOT NULL,
            card_id INTEGER NOT NULL,
            PRIMARY KEY (keyword, card_id),
            FOREIGN KEY(keyword) REFERENCES SearchKeywords(keyword)
        )""",
        """CREATE TABLE IF NOT EXISTS SearchIndexToDecks (
            term TEXT NOT NULL,
            deck_id TEXT NOT NULL,
            PRIMARY KEY (term, deck_id)
        )""",
        # --- Performance Indexes ---
        "CREATE INDEX IF NOT EXISTS idx_decks_user_id ON Decks(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_decks_like ON Decks(deck_like)",
        "CREATE INDEX IF NOT EXISTS idx_decks_update_date ON Decks(update_date)",
        "CREATE INDEX IF NOT EXISTS idx_decks_deck_name ON Decks(deck_name)",
        "CREATE INDEX IF NOT EXISTS idx_deckcards_card_id ON DeckCards(card_id)",
        "CREATE INDEX IF NOT EXISTS idx_cards_cn_name ON Cards(cn_name)",
        "CREATE INDEX IF NOT EXISTS idx_cards_sc_name ON Cards(sc_name)",
        "CREATE INDEX IF NOT EXISTS idx_cards_md_name ON Cards(md_name)",
        "CREATE INDEX IF NOT EXISTS idx_cards_nwbbs_n ON Cards(nwbbs_n)",
        "CREATE INDEX IF NOT EXISTS idx_cards_cnocg_n ON Cards(cnocg_n)",
        "CREATE INDEX IF NOT EXISTS idx_cards_jp_name ON Cards(jp_name)",
        "CREATE INDEX IF NOT EXISTS idx_cards_en_name ON Cards(en_name)",
        "CREATE INDEX IF NOT EXISTS idx_search_index_to_decks_term ON SearchIndexToDecks(term)",
    ]
    for statement in schema_statements:
        db.execute(statement)
    log.info("STAGE 1: Schema creation complete.")


# ==============================================================================
# == STAGE 2: LOOKUP TABLE POPULATION
# ==============================================================================


def populate_lookup_tables(
    db: DatabaseConnector, maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]]
) -> Dict[int, str]:
    """
    Populates static, non-relational data into lookup tables.

    Detailed Explanation:
    This function handles the insertion of simple, relatively unchanging data like
    card races, attributes, and types. These are essentially key-value pairs
    (e.g., code `1` -> name `"Warrior"`). It also parses and inserts setcode (archetype)
    information. The function uses highly efficient bulk insertion.

    Workflow:
    1.  Receives pre-parsed dictionaries (`maps`) for Races, Attributes, and Types.
    2.  Uses `db.executemany()` to perform high-performance bulk inserts for each map.
    3.  Calls the external utility `parse_setcodes()` to read archetype information.
    4.  Performs a final bulk insert for the setcode data.
    5.  Returns the `setcode_map` for use in later card processing stages.

    Args:
        - db (DatabaseConnector): An active database connector instance.
        - maps (Tuple): A tuple of `(race_map, attribute_map, type_map)`.

    Returns:
        - The `setcode_map` (Dict[int, str]) for use in card processing.
    """
    log.info(
        "STAGE 2: Populating lookup tables (Races, Attributes, Types, Setcodes)..."
    )
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

    log.info("STAGE 2: Lookup tables populated.")
    return setcode_map


# ==============================================================================
# == STAGE 3: CARD DATA PROCESSING & INDEXING
# ==============================================================================

# --- Stage 3 Helper Functions ---


def _load_json_data(filepath: str, expected_type=dict) -> Any:
    """Loads and validates a JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, expected_type):
            log.error(
                f"JSON file content is not the expected type.",
                file=filepath,
                expected=str(expected_type),
            )
            return None
        return data
    except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
        log.error("Failed to read or parse JSON file.", file=filepath, error=str(e))
        return None


def _prepare_base_card_data(all_cards_data: Dict, maps: Tuple, setcode_map: Dict):
    """Parses raw card data and prepares it for batch database insertion."""
    race_map, attribute_map, type_map = maps
    data_for_insertion = {
        "cards": [],
        "races": [],
        "attrs": [],
        "types": [],
        "setcodes": [],
    }

    for _, data in all_cards_data.items():
        card_id = data.get("id")
        if not card_id:
            continue

        card_text = data.get("text", {})
        data_for_insertion["cards"].append(
            (
                card_id,
                data.get("cid"),
                data.get("cn_name"),
                data.get("sc_name"),
                data.get("md_name"),
                data.get("nwbbs_n"),
                data.get("cnocg_n"),
                data.get("jp_name"),
                data.get("en_name"),
                card_text.get("types"),
                card_text.get("desc"),
                card_text.get("pdesc"),
                data.get("data", {}).get("atk"),
                data.get("data", {}).get("def"),
                data.get("data", {}).get("level"),
            )
        )
        d = data.get("data", {})
        for code in race_map:
            if (d.get("race", 0) & code) == code:
                data_for_insertion["races"].append((card_id, code))
        for code in attribute_map:
            if (d.get("attribute", 0) & code) == code:
                data_for_insertion["attrs"].append((card_id, code))
        for code in type_map:
            if (d.get("type", 0) & code) == code:
                data_for_insertion["types"].append((card_id, code))

        card_setcode_value = d.get("setcode", 0)
        if card_setcode_value in setcode_map:
            data_for_insertion["setcodes"].append((card_id, card_setcode_value))

    return data_for_insertion


def _insert_base_card_data(db: DatabaseConnector, data: Dict):
    """Inserts the parsed base card data and relations into the database."""
    log.info(f"Inserting {len(data['cards'])} cards and their base relations...")
    db.executemany(
        "INSERT OR REPLACE INTO Cards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        data["cards"],
    )
    db.executemany("INSERT OR IGNORE INTO CardToRace VALUES (?,?)", data["races"])
    db.executemany("INSERT OR IGNORE INTO CardToAttribute VALUES (?,?)", data["attrs"])
    db.executemany("INSERT OR IGNORE INTO CardToType VALUES (?,?)", data["types"])
    db.executemany("INSERT OR IGNORE INTO CardToSetcode VALUES (?,?)", data["setcodes"])
    log.info("Base card data populated.")


def _build_raw_keyword_map(
    predefined_keywords: List[str], all_cards_data: Dict
) -> Dict[str, Set[int]]:
    """Builds an in-memory inverted index: keyword -> {set of card_ids}."""
    log.info(
        f"Building card sets for {len(predefined_keywords)} predefined keywords..."
    )
    keyword_map = defaultdict(set)
    for keyword in predefined_keywords:
        if not keyword or len(keyword) <= 1:
            continue
        lower_keyword = keyword.lower()
        for _, data in all_cards_data.items():
            card_id = int(data.get("id", 0))
            if not card_id:
                continue
            texts_to_check = [
                data.get("cn_name", ""),
                data.get("sc_name", ""),
                data.get("md_name", ""),
                data.get("nwbbs_n", ""),
                data.get("cnocg_n", ""),
                data.get("jp_name", ""),
                data.get("en_name", ""),
                data.get("text", {}).get("desc", ""),
                data.get("text", {}).get("pdesc", ""),
            ]
            for text in texts_to_check:
                if text and lower_keyword in text.lower():
                    keyword_map[keyword].add(card_id)
                    break
    log.info(f"Finished building sets. Found results for {len(keyword_map)} keywords.")
    return keyword_map


def _optimize_keyword_pointers(
    sorted_keywords: List[Tuple[str, Set[int]]],
) -> Dict[str, Dict]:
    """
    Optimizes the keyword index by creating pointers for subset keywords.
    This reduces data redundancy in the KeywordToCard table.
    """
    log.info("Optimizing keyword pointers...")
    final_keyword_data = {}

    # First pass: Identify direct subsets of larger sets already processed.
    for keyword, ids in sorted_keywords:
        is_subset_of_existing = False
        len_ids = len(ids)
        for existing_kw, existing_data in final_keyword_data.items():
            if existing_data["pointer"] is None:
                len_existing_ids = len(existing_data["ids"])
                if (
                    len_existing_ids - len_ids
                ) <= SUBSET_POINTER_MAX_DIFF and ids.issubset(existing_data["ids"]):
                    final_keyword_data[keyword] = {"ids": ids, "pointer": existing_kw}
                    is_subset_of_existing = True
                    break
        if not is_subset_of_existing:
            final_keyword_data[keyword] = {"ids": ids, "pointer": None}

    # Second pass: Check if any non-pointer keywords are now subsets of the newly added one.
    # This ensures that if A is processed, then B (a superset of A) is processed, A correctly points to B.
    for keyword, data in list(final_keyword_data.items()):
        if data["pointer"] is not None:
            continue
        ids = data["ids"]
        len_ids = len(ids)
        for other_kw, other_data in final_keyword_data.items():
            if keyword == other_kw or other_data["pointer"] is not None:
                continue
            other_ids = other_data["ids"]
            len_other_ids = len(other_ids)
            if (
                len_ids - len_other_ids
            ) <= SUBSET_POINTER_MAX_DIFF and other_ids.issubset(ids):
                other_data["pointer"] = keyword

    log.info(
        f"Pointer optimization complete. Final keyword count: {len(final_keyword_data)}."
    )
    return final_keyword_data


def _insert_keyword_index_data(db: DatabaseConnector, final_keyword_data: Dict):
    """Prepares and inserts the optimized keyword index data into the database."""
    keywords_to_insert = []
    keyword_to_card_to_insert = []
    for keyword, data in final_keyword_data.items():
        keywords_to_insert.append((keyword, len(data["ids"]), data["pointer"]))
        if data["pointer"] is None:
            for card_id in data["ids"]:
                keyword_to_card_to_insert.append((keyword, card_id))

    log.info(f"Inserting {len(keywords_to_insert)} keywords into SearchKeywords...")
    db.executemany(
        "INSERT OR REPLACE INTO SearchKeywords VALUES (?,?,?)", keywords_to_insert
    )
    log.info(
        f"Inserting {len(keyword_to_card_to_insert)} mappings into KeywordToCard..."
    )
    db.executemany(
        "INSERT OR REPLACE INTO KeywordToCard VALUES (?,?)", keyword_to_card_to_insert
    )


# --- Stage 3 Main Function ---


def process_cards(
    db: DatabaseConnector,
    maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]],
    setcode_map: Dict[int, str],
):
    """
    Processes `cards.json`, populates `Cards` and related tables, and builds the
    card-level `SearchKeywords` index.

    Workflow:
    1.  Load `cards.json` and `keywords.json` from files.
    2.  Parse all card data into lists suitable for batch insertion.
    3.  Insert this base card data into the `Cards`, `CardToRace`, `CardToType`, etc. tables.
    4.  Build a raw in-memory map of predefined keywords to the card IDs they match.
    5.  Filter out keywords that are too rare or too common.
    6.  Optimize the keyword index by creating pointers for keywords that are subsets of others.
    7.  Insert the final, optimized keyword index into the `SearchKeywords` and `KeywordToCard` tables.
    """
    log.info("STAGE 3: Processing card data and building card-level search index...")

    # --- Part 1: Load and Insert Base Card Data ---
    all_cards_data = _load_json_data(LOCAL_CARDS_FILE)
    if not all_cards_data:
        return

    prepared_data = _prepare_base_card_data(all_cards_data, maps, setcode_map)
    _insert_base_card_data(db, prepared_data)

    # --- Part 2: Build and Insert Keyword Search Index ---
    predefined_keywords = _load_json_data(PREDEFINED_KEYWORDS_FILE, expected_type=list)
    if not predefined_keywords:
        return

    raw_keywords = _build_raw_keyword_map(predefined_keywords, all_cards_data)

    log.info(
        "Filtering keywords by size...", min=KEYWORD_MIN_CARDS, max=KEYWORD_MAX_CARDS
    )
    filtered_keywords = {
        kw: ids
        for kw, ids in raw_keywords.items()
        if KEYWORD_MIN_CARDS <= len(ids) <= KEYWORD_MAX_CARDS
    }
    log.info(f"Filtered down to {len(filtered_keywords)} keywords.")

    sorted_keywords = sorted(
        filtered_keywords.items(), key=lambda item: len(item[1]), reverse=True
    )

    final_keyword_data = _optimize_keyword_pointers(sorted_keywords)

    _insert_keyword_index_data(db, final_keyword_data)

    log.info("STAGE 3: Card processing and index building complete.")


# ==============================================================================
# == STAGE 4: DECK DATA PROCESSING
# ==============================================================================

# --- Stage 4 Helper Functions ---


def _scan_and_parse_valid_decks(
    deck_dir: str, valid_card_ids: Set[int], alias_map: Dict[int, int]
) -> List[Dict]:
    """Scans deck directory, parses .json files, validates cards, and returns a list of valid decks."""
    log.info("Scanning and parsing all deck files into memory...")
    all_valid_decks_data = []
    total_files = 0

    for filename in os.listdir(deck_dir):
        if not filename.endswith(".json"):
            continue
        total_files += 1
        filepath = os.path.join(deck_dir, filename)

        data = _load_json_data(filepath)
        if not data or not data.get("deckId"):
            continue

        is_deck_valid = True
        resolved_cards: Dict[str, List[int]] = defaultdict(list)
        ydk_str = data.get("deckYdk", "")
        current_section_key = None

        for line in ydk_str.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("#main"):
                current_section_key = "main"
            elif line.startswith("#extra"):
                current_section_key = "extra"
            elif line.startswith("!side"):
                current_section_key = "side"
            elif line.startswith("#") or current_section_key is None:
                continue

            try:
                card_id = int(line)
                original_id = alias_map.get(card_id, card_id)
                if original_id in valid_card_ids:
                    resolved_cards[current_section_key].append(original_id)
                else:
                    is_deck_valid = False
                    break
            except ValueError:
                pass

        if not is_deck_valid:
            continue

        if len(resolved_cards.get("main", [])) > 5:
            all_valid_decks_data.append(
                {"raw_data": data, "resolved_cards": resolved_cards}
            )

    log.info(
        f"Read {total_files} files, found {len(all_valid_decks_data)} valid decks for processing."
    )
    return all_valid_decks_data


def _merge_duplicate_decks(all_valid_decks: List[Dict]) -> List[Dict]:
    """
    Merges decks with identical card content.

    Detailed Explanation:
    This function processes a list of valid decks and merges those that have the exact
    same main, extra, and side deck contents. The merging logic is as follows:
    - The deck with the earliest `deckUploadDate` is kept as the base "champion".
    - The `deckLike` counts from all duplicate decks are summed up and assigned to the champion.
    - The `deckUpdateDate` from the most recently updated duplicate deck is assigned to the champion.
    - All other metadata (name, contributor, etc.) is taken from the base champion deck.

    This ensures that each unique deck composition is represented by a single entry in the
    database, but with aggregated likes and the most recent update timestamp.

    Args:
        all_valid_decks (List[Dict]): A list of valid deck dictionaries, where each dict
                                      contains 'raw_data' and 'resolved_cards'.

    Returns:
        List[Dict]: A list of the final, merged "champion" deck dictionaries.
    """
    log.info("Merging duplicate decks based on content...")
    content_to_champion_deck = {}

    for deck_info in all_valid_decks:
        raw_data = deck_info["raw_data"]
        resolved_cards = deck_info["resolved_cards"]

        content_key = (
            tuple(sorted(resolved_cards["main"])),
            tuple(sorted(resolved_cards["extra"])),
            tuple(sorted(resolved_cards["side"])),
        )

        if content_key not in content_to_champion_deck:
            # First time seeing this deck content. It becomes the initial champion.
            # We make a copy and attach the resolved cards for later processing.
            champion_deck = raw_data.copy()
            champion_deck["resolved_cards"] = resolved_cards
            content_to_champion_deck[content_key] = champion_deck
        else:
            # Duplicate content found. Merge with the existing champion.
            champion_deck = content_to_champion_deck[content_key]
            current_deck_data = raw_data

            # 1. Sum the likes
            champion_deck["deckLike"] = champion_deck.get(
                "deckLike", 0
            ) + current_deck_data.get("deckLike", 0)

            # 2. Keep the latest update date
            current_update = current_deck_data.get("deckUpdateDate") or 0
            champion_update = champion_deck.get("deckUpdateDate") or 0
            if current_update > champion_update:
                champion_deck["deckUpdateDate"] = current_update

            # 3. The deck with the earliest upload date becomes the base champion.
            #    If the current deck is older, it takes over as the champion,
            #    but we carry over the aggregated likes and latest update date.
            current_upload = current_deck_data.get("deckUploadDate") or 0
            champion_upload = champion_deck.get("deckUploadDate") or 0
            if current_upload < champion_upload:
                # Preserve aggregated values
                aggregated_likes = champion_deck["deckLike"]
                latest_update_date = champion_deck["deckUpdateDate"]

                # The current deck becomes the new champion
                new_champion = current_deck_data.copy()
                new_champion["resolved_cards"] = resolved_cards

                # Apply aggregated values to the new champion
                new_champion["deckLike"] = aggregated_likes
                new_champion["deckUpdateDate"] = latest_update_date

                content_to_champion_deck[content_key] = new_champion

    final_decks = list(content_to_champion_deck.values())
    log.info(
        f"Merging complete. Retaining {len(final_decks)} unique deck compositions."
    )
    return final_decks


def _prepare_deck_db_data(final_decks: List[Dict]) -> Tuple[List[Tuple], List[Tuple]]:
    """Prepares the final lists of deck metadata and card links for batch insertion."""
    decks_to_insert = []
    deck_cards_to_insert = []

    for deck_to_keep in final_decks:
        # 'deck_to_keep' is now the merged data dictionary itself.
        data = deck_to_keep
        resolved_cards = deck_to_keep["resolved_cards"]
        ydk_str = data.get("deckYdk", "")

        # Extract extra info from YDK string
        deck_case_match = re.search(r"#case\s*(\d+)", ydk_str)
        deck_protector_match = re.search(r"#protector\s*(\d+)", ydk_str)
        deck_case = int(deck_case_match.group(1)) if deck_case_match else 0
        deck_protector = (
            int(deck_protector_match.group(1)) if deck_protector_match else 0
        )

        # Determine cover cards
        main_deck_cards = resolved_cards.get("main", [])
        covers = main_deck_cards[:3]
        deck_cover1 = covers[0] if len(covers) > 0 else 0
        deck_cover2 = covers[1] if len(covers) > 1 else 0
        deck_cover3 = covers[2] if len(covers) > 2 else 0

        decks_to_insert.append(
            (
                data["deckId"],
                data.get("deckName", "Unknown Name"),
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
        for card_type, id_list in resolved_cards.items():
            for card_id, count in Counter(id_list).items():
                deck_cards_to_insert.append((data["deckId"], card_id, card_type, count))

    return decks_to_insert, deck_cards_to_insert


def _insert_deck_data(db: DatabaseConnector, decks: List, deck_cards: List):
    """Inserts the final, merged deck data into the database."""
    log.info(f"Inserting {len(decks)} merged decks and their card lists...")
    db.executemany(
        "INSERT OR REPLACE INTO Decks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", decks
    )
    db.executemany("INSERT OR REPLACE INTO DeckCards VALUES (?,?,?,?)", deck_cards)
    log.info("Deck data insertion complete.")


# --- Stage 4 Main Function ---


def process_decks(
    db: DatabaseConnector, valid_card_ids: Set[int], alias_map: Dict[int, int]
):
    """
    Scans, validates, merges duplicates, and populates decks into the database.

    Workflow:
    1.  Check if the deck directory exists.
    2.  Scan all `.json` files in the directory, parse their YDK strings, and validate
        every card ID against the master list of known cards.
    3.  Collect all valid decks into an in-memory list.
    4.  Perform a sophisticated merging pass: for decks with identical content,
        likes are summed and the latest update date is kept. The earliest uploaded
        deck is used as the base.
    5.  Prepare the final, merged list of decks for database insertion.
    6.  Perform two large, efficient bulk inserts to populate the `Decks` and `DeckCards` tables.
    """
    log.info("STAGE 4: Processing deck data...")
    if not os.path.isdir(DECK_DIR):
        log.warning(
            "Deck data directory not found, skipping deck processing.",
            directory=DECK_DIR,
        )
        return

    all_valid_decks = _scan_and_parse_valid_decks(DECK_DIR, valid_card_ids, alias_map)
    if not all_valid_decks:
        log.warning("No valid decks found to process.")
        return

    final_decks = _merge_duplicate_decks(all_valid_decks)

    decks_to_insert, deck_cards_to_insert = _prepare_deck_db_data(final_decks)

    _insert_deck_data(db, decks_to_insert, deck_cards_to_insert)

    log.info("STAGE 4: Deck processing complete.")


# ==============================================================================
# == STAGE 5: UNIFIED SEARCH INDEX BUILD
# ==============================================================================

# --- Stage 5 Helper Functions ---


def _get_content_based_mappings(
    db: DatabaseConnector, threshold: int
) -> Set[Tuple[str, str]]:
    """
    Finds (term, deck_id) mappings where a deck contains a minimum number of cards
    related to the term. This logic is executed entirely within the database for performance.
    """
    log.info("Part 1/2: Processing content-based mappings with threshold...")
    mappings = set()

    # SQL queries to find mappings from various term sources
    queries = {
        "keywords": """
            SELECT T1.keyword, T2.deck_id FROM KeywordToCard AS T1 JOIN DeckCards AS T2 ON T1.card_id = T2.card_id
            GROUP BY T1.keyword, T2.deck_id HAVING COUNT(DISTINCT T1.card_id) >= ?
        """,
        "card_names": """
            SELECT T1.{col} AS term, T2.deck_id FROM Cards AS T1 JOIN DeckCards AS T2 ON T1.id = T2.card_id
            WHERE T1.{col} IS NOT NULL GROUP BY T1.{col}, T2.deck_id HAVING COUNT(DISTINCT T1.id) >= ?
        """,
        "setcodes": """
            SELECT T1.{col} AS term, T3.deck_id FROM Setcodes AS T1 JOIN CardToSetcode AS T2 ON T1.set_code = T2.set_code
            JOIN DeckCards AS T3 ON T2.card_id = T3.card_id WHERE T1.{col} IS NOT NULL
            GROUP BY T1.{col}, T3.deck_id HAVING COUNT(DISTINCT T2.card_id) >= ?
        """,
    }

    # Process keywords
    db.execute(queries["keywords"], (threshold,))
    for row in db.fetchall():
        mappings.add((row["keyword"].lower(), row["deck_id"]))
    log.info(f"  - Mappings after keywords: {len(mappings)}")

    # Process card names
    name_columns = [
        "cn_name",
        "sc_name",
        "md_name",
        "nwbbs_n",
        "cnocg_n",
        "jp_name",
        "en_name",
    ]
    for col in name_columns:
        db.execute(queries["card_names"].format(col=col), (threshold,))
        for row in db.fetchall():
            mappings.add((row["term"].lower(), row["deck_id"]))
    log.info(f"  - Mappings after card names: {len(mappings)}")

    # Process setcode names
    setcode_name_columns = ["set_name_cn", "set_name_jp"]
    for col in setcode_name_columns:
        db.execute(queries["setcodes"].format(col=col), (threshold,))
        for row in db.fetchall():
            mappings.add((row["term"].lower(), row["deck_id"]))

    log.info(f"Total mappings after content-based processing: {len(mappings)}")
    return mappings


def _get_title_based_mappings(
    db: DatabaseConnector, existing_mappings: Set
) -> Set[Tuple[str, str]]:
    """
    Finds (term, deck_id) mappings where a deck's name contains a known search term.
    """
    log.info("Part 2/2: Processing title-based mappings...")

    # Gather all potential search terms (raw keywords and setcode names)
    all_terms = set()
    raw_keywords = _load_json_data(PREDEFINED_KEYWORDS_FILE, expected_type=list)
    if raw_keywords:
        for kw in raw_keywords:
            if kw:
                all_terms.add(kw.lower())

    db.execute("SELECT set_name_cn FROM Setcodes WHERE set_name_cn IS NOT NULL")
    for row in db.fetchall():
        all_terms.add(row["set_name_cn"].lower())
    db.execute("SELECT set_name_jp FROM Setcodes WHERE set_name_jp IS NOT NULL")
    for row in db.fetchall():
        all_terms.add(row["set_name_jp"].lower())

    log.info(f"  - Found {len(all_terms)} unique terms to check against deck names.")

    # Fetch all deck names
    db.execute("SELECT deck_id, deck_name FROM Decks")
    all_decks = db.fetchall()

    # Add new mappings based on title match
    new_mappings = set()
    for term in all_terms:
        if not term or len(term) <= 1:
            continue
        for deck in all_decks:
            if term in deck["deck_name"].lower():
                new_mappings.add((term, deck["deck_id"]))

    combined_mappings = existing_mappings.union(new_mappings)
    log.info(
        f"Total unique mappings after title-based processing: {len(combined_mappings)}"
    )
    return combined_mappings


# --- Stage 5 Main Function ---


def build_unified_search_index(db: DatabaseConnector):
    """
    Builds the final, compressed, unified search index (`SearchIndexToDecks`).

    Detailed Explanation:
    This is the final optimization step. It pre-computes all relationships between
    search terms and decks to make queries extremely fast. A term-deck mapping is
    created if EITHER of two conditions is met:
    1.  Content Relevance: The deck contains a significant number of cards related to the
        term (defined by `DECK_TERM_THRESHOLD`).
    2.  Title Relevance: The deck's name contains the term.
    All terms are normalized to lowercase to ensure case-insensitive searching.

    Workflow:
    1.  Delete all old data from `SearchIndexToDecks` for a fresh build.
    2.  Call a helper to generate mappings based on deck content and the threshold.
    3.  Call a helper to generate mappings based on matching terms in deck titles.
    4.  Combine the results from both methods, ensuring uniqueness.
    5.  Insert the final, comprehensive list of mappings into the database with a single
        high-performance `executemany` call.
    """
    log.info(
        f"STAGE 5: Building unified search index with dual logic (Threshold: {DECK_TERM_THRESHOLD}, Name Match)..."
    )

    db.execute("DELETE FROM SearchIndexToDecks")

    content_mappings = _get_content_based_mappings(db, DECK_TERM_THRESHOLD)

    final_mappings = _get_title_based_mappings(db, content_mappings)

    if final_mappings:
        sorted_mappings = sorted(list(final_mappings))
        log.info(
            f"Inserting {len(sorted_mappings)} final unique mappings into SearchIndexToDecks..."
        )
        db.executemany(
            "INSERT OR IGNORE INTO SearchIndexToDecks VALUES (?, ?)", sorted_mappings
        )

    log.info("STAGE 5: Unified search index build complete.")
