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
from collections import Counter, defaultdict
from typing import Dict, Tuple, Set, List

import structlog

from database.base_connector import DatabaseConnector
from processing.parser import parse_setcodes
from config import LOCAL_CARDS_FILE, DECK_DIR, PREDEFINED_KEYWORDS_FILE

log = structlog.get_logger(__name__)

# Configuration for search index generation
KEYWORD_MIN_CARDS = 2
KEYWORD_MAX_CARDS = 500
SUBSET_POINTER_MAX_DIFF = 20

# Configuration for Unified Index Compression
DECK_TERM_THRESHOLD = 5


def create_schema(db: DatabaseConnector):
    """
    Initializes the database by creating all tables and performance-critical indexes.

    Detailed Explanation:
    This function serves as the blueprint for the entire database structure. It is designed
    to be executed once at the very beginning of a fresh build. It defines not only the
    core data tables (`Decks`, `Cards`, etc.) but also the crucial, pre-computed search
    index tables (`SearchKeywords`, `SearchIndexToDecks`) that are essential for achieving
    high query performance without relying on database-native FTS. Every table and index
    is deliberately chosen to support the application's search and data retrieval needs.

    Workflow:
    1.  A comprehensive list of SQL `CREATE TABLE` and `CREATE INDEX` commands is defined
        as Python strings within a single list, `schema_statements`.
    2.  The function iterates through this list of SQL commands.
    3.  For each command, it calls `db.execute()` to run the SQL statement against the
        database, creating a table or an index.
    4.  After all commands have been executed, the fundamental structure of the
        database is in place and ready for data population.

    Args:
        - db (DatabaseConnector): An active database connector instance.
    """
    log.info("Creating database schema (tables and indexes)...")
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
    log.info("Schema creation complete.")


def populate_lookup_tables(
    db: DatabaseConnector, maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]]
) -> Dict[int, str]:
    """
    Populates static, non-relational data into lookup tables.

    Detailed Explanation:
    This function handles the insertion of simple, relatively unchanging data like
    card races, attributes, and types. These are essentially key-value pairs
    (e.g., code `1` -> name `"Warrior"`). Populating these tables early in the
    build process is good practice as other, more complex data (like cards) will
    reference these values. The function uses highly efficient bulk insertion.

    Workflow:
    1.  Receives pre-parsed Python dictionaries (`maps`) containing the data for
        Races, Attributes, and Types.
    2.  Calls `db.executemany()` to perform a high-performance bulk insert of all
        Race data into the `Races` table. The `INSERT OR IGNORE` command prevents
        errors if the data already exists.
    3.  Repeats the efficient bulk insert process for Attribute and Type data into
        their respective tables.
    4.  Calls the external utility `parse_setcodes()` to read and parse setcode
        (archetype) information from its source.
    5.  Performs a final bulk insert for the setcode data into the `Setcodes` table.
    6.  Returns the `setcode_map` dictionary, which will be needed by later
        functions to associate cards with their archetypes.

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


def _build_results_for_predefined_keywords(
    predefined_keywords: List[str], all_cards_data: Dict
) -> Dict[str, Set[int]]:
    """
    Finds all cards that match a given list of user-defined keywords.

    Detailed Explanation:
    This is a helper function that acts as the first step in building the advanced
    search index. Its purpose is to create an "inverted index" in memory, mapping
    each curated keyword from `keywords.json` to a set of all card IDs that contain
    that keyword in their names or text. This is a pure data processing function
    that operates on in-memory Python objects for maximum speed.

    Workflow:
    1.  Initializes an empty `defaultdict(set)`, named `keyword_map`, which will
        store the mapping of `keyword -> {card_id_1, card_id_2, ...}`.
    2.  Begins a loop through each `keyword` in the input `predefined_keywords` list.
        It skips any empty or single-character keywords to reduce noise.
    3.  For each keyword, it enters a nested loop that iterates through every card's
        `data` dictionary in the `all_cards_data` object.
    4.  Inside the inner loop, it compiles a temporary list, `texts_to_check`,
        containing all of the current card's searchable text fields (all name
        variants, effect description, and pendulum description).
    5.  It then loops through `texts_to_check`, performing a case-insensitive
        substring check to see if the `keyword` exists within the card's text.
    6.  Upon finding the first match for a card, it adds the card's ID to the set
        corresponding to the current `keyword` in `keyword_map` and immediately
        `break`s out of the inner-most loop. This is an optimization to avoid
        unnecessarily checking other text fields on the same card.
    7.  After the outer loops complete, the function returns the fully populated
        `keyword_map`, which is ready for further processing and filtering.

    Args:
        - predefined_keywords (List[str]): A list of user-defined string keywords.
        - all_cards_data (Dict): A dictionary containing all card information.

    Returns:
        - Dict[str, Set[int]]: An in-memory inverted index mapping keywords to sets of card IDs.
    """
    log.info(
        f"Building card sets for {len(predefined_keywords)} predefined keywords..."
    )
    keyword_map = defaultdict(set)

    for keyword in predefined_keywords:
        if not keyword or len(keyword) <= 1:
            continue

        lower_keyword = keyword.lower()
        for card_key, data in all_cards_data.items():
            card_id_int = int(data.get("id", 0))
            if not card_id_int:
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
                    # IMPORTANT: The key is the original keyword, not the lowercased version,
                    # to preserve the canonical representation from keywords.json.
                    # Normalization will happen in the final index build step.
                    keyword_map[keyword].add(card_id_int)
                    break

    log.info(f"Finished building sets. Found results for {len(keyword_map)} keywords.")
    return keyword_map


def process_cards(
    db: DatabaseConnector,
    maps: Tuple[Dict[int, str], Dict[int, str], Dict[int, str]],
    setcode_map: Dict[int, str],
):
    """
    Processes the main `cards.json` file, populating the `Cards` table and its
    direct link tables, and builds the card-level `SearchKeywords` index.

    Detailed Explanation:
    This is a major data ingestion function responsible for two main tasks. First, it
    parses the entire `cards.json` file and populates the core `Cards` table and its
    many-to-many link tables (e.g., `CardToRace`). Second, it orchestrates the
    creation of the pointer-optimized keyword index (`SearchKeywords` and `KeywordToCard`).
    This index is a sophisticated optimization that reduces data redundancy for
    related keywords and serves as a foundational data source for the final
    `SearchIndexToDecks` table.

    Workflow:
    1.  **Data Loading & Preparation:** Reads the entire `cards.json` file into memory.
        It then prepares several empty lists (`cards_to_insert`, `card_races`, etc.)
        that will be used to buffer data for batch insertion.
    2.  **Card Iteration & Relation Mapping:** Loops through every card in the loaded
        JSON data. For each card, it extracts all its attributes (name, ATK/DEF, etc.)
        and its relationships (race, type, etc.) and appends formatted tuples to the
        corresponding buffer lists.
    3.  **Base Table Population:** Performs a series of highly efficient `executemany`
        calls to bulk-insert the buffered data into the `Cards`, `CardToRace`,
        `CardToAttribute`, `CardToType`, and `CardToSetcode` tables.
    4.  **Keyword Index Generation:**
        a. Loads the curated list of keywords from the `keywords.json` file.
        b. Calls the `_build_results_for_predefined_keywords` helper function to get
           the initial mapping of keywords to card IDs.
        c. Filters this mapping based on the `KEYWORD_MIN_CARDS` and `KEYWORD_MAX_CARDS`
           configuration values. This removes keywords that are either too rare or too
           common to be useful search terms.
        d. Sorts the keywords by the number of associated cards (descending). This is a
           critical optimization for the next step.
    5.  **Pointer Optimization:** It iterates through the sorted keywords to find
        subset/superset relationships. If keyword A is a subset of a larger keyword B,
        it creates a `pointer_to` link from A to B. This saves significant space in
        the `KeywordToCard` table, as the card list for A doesn't need to be stored.
    6.  **Keyword Index Population:** Prepares the final, pointer-optimized keyword
        data for insertion and uses two `executemany` calls to populate the
        `SearchKeywords` and `KeywordToCard` tables.

    Args:
        - db (DatabaseConnector): An active database connector.
        - maps (Tuple): A tuple of `(race_map, attribute_map, type_map)`.
        - setcode_map (Dict[int, str]): A dictionary mapping setcodes to names.
    """
    log.info("Processing card data and building card-level search index...")
    race_map, attribute_map, type_map = maps

    try:
        with open(LOCAL_CARDS_FILE, "r", encoding="utf-8") as f:
            all_cards_data = json.load(f)
    except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
        log.error("Failed to read or parse cards.json.", error=str(e))
        return

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
            continue

        card_text = data.get("text", {})
        cards_to_insert.append(
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
                card_races.append((card_id, code))
        for code in attribute_map:
            if (d.get("attribute", 0) & code) == code:
                card_attrs.append((card_id, code))
        for code in type_map:
            if (d.get("type", 0) & code) == code:
                card_types.append((card_id, code))
        card_setcode_value = d.get("setcode", 0)
        if card_setcode_value and card_setcode_value in setcode_map:
            card_setcodes.append((card_id, card_setcode_value))

    log.info(f"Inserting {len(cards_to_insert)} cards and their base relations...")
    db.executemany(
        "INSERT OR REPLACE INTO Cards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        cards_to_insert,
    )
    db.executemany("INSERT OR IGNORE INTO CardToRace VALUES (?,?)", card_races)
    db.executemany("INSERT OR IGNORE INTO CardToAttribute VALUES (?,?)", card_attrs)
    db.executemany("INSERT OR IGNORE INTO CardToType VALUES (?,?)", card_types)
    db.executemany("INSERT OR IGNORE INTO CardToSetcode VALUES (?,?)", card_setcodes)
    log.info("Base card data populated.")

    try:
        with open(PREDEFINED_KEYWORDS_FILE, "r", encoding="utf-8") as f:
            predefined_keywords = json.load(f)
        if not isinstance(predefined_keywords, list):
            log.error(
                "keywords.json should contain a JSON array of strings.",
                file=PREDEFINED_KEYWORDS_FILE,
            )
            return
    except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
        log.error(
            "Could not read or parse keywords.json.",
            file=PREDEFINED_KEYWORDS_FILE,
            error=str(e),
        )
        return

    raw_keywords = _build_results_for_predefined_keywords(
        predefined_keywords, all_cards_data
    )

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

    log.info("Optimizing keyword pointers...")
    final_keyword_data = {}
    for keyword, ids in sorted_keywords:
        is_subset_of_existing = False
        len_ids = len(ids)
        for existing_kw, existing_data in final_keyword_data.items():
            len_existing_ids = len(existing_data["ids"])
            if (
                existing_data["pointer"] is None
                and (len_existing_ids - len_ids) <= SUBSET_POINTER_MAX_DIFF
                and ids.issubset(existing_data["ids"])
            ):
                final_keyword_data[keyword] = {"ids": ids, "pointer": existing_kw}
                is_subset_of_existing = True
                break
        if not is_subset_of_existing:
            final_keyword_data[keyword] = {"ids": ids, "pointer": None}
            for existing_kw, existing_data in final_keyword_data.items():
                len_existing_ids = len(existing_data["ids"])
                if (
                    existing_kw != keyword
                    and existing_data["pointer"] is None
                    and (len_ids - len_existing_ids) <= SUBSET_POINTER_MAX_DIFF
                    and existing_data["ids"].issubset(ids)
                ):
                    existing_data["pointer"] = keyword
    log.info(
        f"Pointer optimization complete. Final keyword count: {len(final_keyword_data)}."
    )

    keywords_to_insert = []
    keyword_to_card_to_insert = []
    for keyword, data in final_keyword_data.items():
        keywords_to_insert.append((keyword, len(data["ids"]), data["pointer"]))
        if data["pointer"] is None:
            for card_id in data["ids"]:
                keyword_to_card_to_insert.append((keyword, card_id))

    log.info(
        f"Inserting {len(keywords_to_insert)} keywords into SearchKeywords table..."
    )
    db.executemany(
        "INSERT OR REPLACE INTO SearchKeywords VALUES (?,?,?)", keywords_to_insert
    )
    log.info(
        f"Inserting {len(keyword_to_card_to_insert)} mappings into KeywordToCard table..."
    )
    db.executemany(
        "INSERT OR REPLACE INTO KeywordToCard VALUES (?,?)", keyword_to_card_to_insert
    )
    log.info("Card-level search index built successfully.")


def process_decks(
    db: DatabaseConnector, valid_card_ids: Set[int], alias_map: Dict[int, int]
):
    """
    Scans, validates, deduplicates, and populates decks into the database.

    Detailed Explanation:
    This is an extremely critical data quality and ingestion function. Its primary
    responsibility is to process all local deck files and ensure that only valid and
    unique decks are added to the database. The deduplication logic is particularly
    sophisticated: for any group of decks that have the exact same card lists (in the
    main, extra, and side decks), it will only keep the single best one. The "best" is
    determined first by the number of likes (more is better), and if there's a tie,
    by the upload date (earlier is better). This entire process happens in memory
    before a single deck is inserted into the database, ensuring maximum data integrity.

    Workflow:
    1.  **Initial Scan and Validation:** The function iterates through all `.json` files
        in the configured deck directory. For each file, it performs these checks:
        a. Reads and parses the JSON data.
        b. Validates that every card ID in the YDK string exists in the `valid_card_ids`
           set (after resolving aliases).
        c. If a deck is fully valid, its parsed data and resolved card lists are
           appended to a temporary in-memory buffer, `all_valid_decks_data`.
    2.  **Deduplication - Champion Selection:** This is the core logic.
        a. An empty dictionary, `content_to_champion_deck`, is created. This map will
           store the "champion" deck for each unique deck content.
        b. The function then iterates through the buffered `all_valid_decks_data`.
        c. For each deck, it generates a "content signature" by sorting the card ID lists
           for the main, extra, and side decks and converting them into a hashable tuple.
           This signature uniquely represents the deck's content, regardless of card order.
        d. It uses this signature as a key to look up the `content_to_champion_deck` map.
        e. **If the signature is new,** the current deck is the first of its kind and
           is immediately designated the "champion" for that content.
        f. **If the signature already exists,** a competition begins between the `current_deck`
           and the existing `champion_deck`:
             i. The deck with the higher `deck_like` count wins.
             ii. If like counts are identical, the deck with the lower (earlier)
                `upload_date` wins.
             iii. The winner of this competition becomes the new champion for that content
                  signature in the map.
    3.  **Final Data Preparation:** After checking all decks, the `values()` of the
        `content_to_champion_deck` map represent the final, unique list of decks
        that should be inserted into the database. The function iterates through this
        final list of champion decks.
    4.  **Batch Insertion:** For each champion deck, it prepares the data tuples for
        insertion into the `Decks` table (metadata) and the `DeckCards` table (card
        lists). Finally, it performs two large, highly efficient `executemany` bulk
        inserts to populate the database with the clean, deduplicated data.

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

    log.info("Scanning and processing all deck files into memory...")
    all_valid_decks_data = []
    total_files = 0

    for filename in os.listdir(DECK_DIR):
        if not filename.endswith(".json"):
            continue

        total_files += 1
        filepath = os.path.join(DECK_DIR, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError):
            continue

        deck_id = data.get("deckId")
        if not deck_id:
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

        if is_deck_valid and len(resolved_cards["main"]) > 5:
            all_valid_decks_data.append(
                {"raw_data": data, "resolved_cards": resolved_cards}
            )

    log.info(
        f"Read {total_files} files, found {len(all_valid_decks_data)} valid decks for processing."
    )

    log.info("Deduplicating decks based on content, likes, and upload date...")
    content_to_champion_deck = {}

    for deck_info in all_valid_decks_data:
        current_deck_data = deck_info["raw_data"]
        resolved = deck_info["resolved_cards"]

        main_sorted = tuple(sorted(resolved["main"]))
        extra_sorted = tuple(sorted(resolved["extra"]))
        side_sorted = tuple(sorted(resolved["side"]))
        content_key = (main_sorted, extra_sorted, side_sorted)

        current_deck = {
            "id": current_deck_data["deckId"],
            "like": current_deck_data.get("deckLike", 0),
            "date": current_deck_data.get("deckUploadDate") or 0,
            "data": current_deck_data,
            "cards": resolved,
        }

        if content_key not in content_to_champion_deck:
            content_to_champion_deck[content_key] = current_deck
        else:
            champion_deck = content_to_champion_deck[content_key]
            if current_deck["like"] > champion_deck["like"]:
                content_to_champion_deck[content_key] = current_deck
            elif current_deck["like"] == champion_deck["like"]:
                if current_deck["date"] < champion_deck["date"]:
                    content_to_champion_deck[content_key] = current_deck

    final_decks = list(content_to_champion_deck.values())
    log.info(f"Deduplication complete. Retaining {len(final_decks)} unique decks.")

    decks_to_insert = []
    deck_cards_to_insert = []

    for deck_to_keep in final_decks:
        data = deck_to_keep["data"]
        resolved_cards = deck_to_keep["cards"]
        ydk_str = data.get("deckYdk", "")

        deck_case_match = re.search(r"#case\s*(\d+)", ydk_str)
        deck_protector_match = re.search(r"#protector\s*(\d+)", ydk_str)
        deck_case = int(deck_case_match.group(1)) if deck_case_match else 0
        deck_protector = (
            int(deck_protector_match.group(1)) if deck_protector_match else 0
        )

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

    log.info(
        f"Inserting {len(decks_to_insert)} deduplicated decks and their card lists..."
    )
    db.executemany(
        "INSERT OR REPLACE INTO Decks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        decks_to_insert,
    )
    db.executemany(
        "INSERT OR REPLACE INTO DeckCards VALUES (?,?,?,?)", deck_cards_to_insert
    )
    log.info("Deck data insertion complete.")


def build_unified_search_index(db: DatabaseConnector):
    """
    Builds the final, compressed, unified search index with dual-condition logic
    and case-insensitive normalization.

    Detailed Explanation:
    This is the most critical optimization step. It constructs the `SearchIndexToDecks`
    table, which acts as a pre-computed "materialized view" to accelerate searches.
    To ensure searches are fast, complete, and relevant, it implements a sophisticated
    dual-condition, case-insensitive logic. A term-deck mapping is created if EITHER of
    the following conditions is met:

    1.  **Content-based Relevance:** The deck contains a significant number of cards
        related to the term (a "critical mass"), as defined by `DECK_TERM_THRESHOLD`.
    2.  **Title-based Relevance:** The deck's user-given name `deck_name` contains the
        term as a substring.

    All terms are normalized to lowercase before insertion, ensuring that searches at
    query-time are case-insensitive. The entire aggregation and filtering logic is
    pushed down into the SQLite database engine to ensure a fast and scalable build process.

    Workflow:
    1.  **Cleanup:** Deletes all old data from `SearchIndexToDecks` for a fresh build.
    2.  **Part 1: Content-based Indexing (Threshold Logic):**
        a. A series of powerful SQL queries are executed for each term source (keywords,
           card names, setcode names).
        b. These queries use `JOIN`, `GROUP BY`, and `HAVING COUNT(DISTINCT ...)` to
           efficiently calculate, within the database, all `(term, deck_id)` pairs
           that meet the `DECK_TERM_THRESHOLD`.
        c. The resulting terms are normalized to lowercase and collected into a master `set`.
    3.  **Part 2: Title-based Indexing (Name Matching Logic):**
        a. A comprehensive set of all unique, potential search terms is gathered.
           This includes the **raw, unfiltered list from `keywords.json`** and all
           setcode names to ensure maximum coverage. All terms are normalized to lowercase.
        b. The function then iterates through this complete set of terms. For each term, it
           scans through all deck names (which are also converted to lowercase on the fly)
           to find matches.
        c. These title-based matches are also added to the master `set`.
    4.  **Final Deduplication and Insertion:**
        a. The master `set`, now containing all unique, lowercase `(term, deck_id)` pairs
           from both content and title matching, is converted to a sorted list.
        b. A single, high-performance `executemany` call inserts this final, clean, and
           highly-relevant index into the `SearchIndexToDecks` table.

    Args:
        - db (DatabaseConnector): An active database connector instance.
    """
    log.info(
        f"Building unified search index with dual logic (Threshold: {DECK_TERM_THRESHOLD}, Name Match)..."
    )

    db.execute("DELETE FROM SearchIndexToDecks")

    all_mappings = set()

    # --- Part 1: Content-based indexing with threshold ---
    log.info("Part 1/2: Processing content-based mappings with threshold...")

    sql_keywords = """
        SELECT T1.keyword, T2.deck_id
        FROM KeywordToCard AS T1 JOIN DeckCards AS T2 ON T1.card_id = T2.card_id
        GROUP BY T1.keyword, T2.deck_id
        HAVING COUNT(DISTINCT T1.card_id) >= ?
    """
    db.execute(sql_keywords, (DECK_TERM_THRESHOLD,))
    for row in db.fetchall():
        all_mappings.add((row["keyword"].lower(), row["deck_id"]))
    log.info(f"  - Mappings after keywords: {len(all_mappings)}")

    name_columns = [
        "cn_name",
        "sc_name",
        "md_name",
        "nwbbs_n",
        "cnocg_n",
        "jp_name",
        "en_name",
    ]
    base_sql_card_names = """
        SELECT T1.{col} AS term, T2.deck_id
        FROM Cards AS T1 JOIN DeckCards AS T2 ON T1.id = T2.card_id
        WHERE T1.{col} IS NOT NULL GROUP BY T1.{col}, T2.deck_id
        HAVING COUNT(DISTINCT T1.id) >= ?
    """
    for col in name_columns:
        sql = base_sql_card_names.format(col=col)
        db.execute(sql, (DECK_TERM_THRESHOLD,))
        for row in db.fetchall():
            all_mappings.add((row["term"].lower(), row["deck_id"]))
    log.info(f"  - Mappings after card names: {len(all_mappings)}")

    setcode_name_columns = ["set_name_cn", "set_name_jp"]
    base_sql_setcodes = """
        SELECT T1.{col} AS term, T3.deck_id
        FROM Setcodes AS T1 JOIN CardToSetcode AS T2 ON T1.set_code = T2.set_code
        JOIN DeckCards AS T3 ON T2.card_id = T3.card_id
        WHERE T1.{col} IS NOT NULL GROUP BY T1.{col}, T3.deck_id
        HAVING COUNT(DISTINCT T2.card_id) >= ?
    """
    for col in setcode_name_columns:
        sql = base_sql_setcodes.format(col=col)
        db.execute(sql, (DECK_TERM_THRESHOLD,))
        for row in db.fetchall():
            all_mappings.add((row["term"].lower(), row["deck_id"]))
    log.info(f"Total mappings after content-based processing: {len(all_mappings)}")

    # --- Part 2: Title-based indexing ---
    log.info("Part 2/2: Processing title-based mappings...")

    all_terms = set()
    try:
        with open(PREDEFINED_KEYWORDS_FILE, "r", encoding="utf-8") as f:
            raw_keywords = json.load(f)
            if isinstance(raw_keywords, list):
                for kw in raw_keywords:
                    if kw:
                        all_terms.add(kw.lower())
    except (IOError, FileNotFoundError, json.JSONDecodeError) as e:
        log.warning(
            "Could not read keywords.json for title matching, skipping.", error=str(e)
        )

    db.execute("SELECT set_name_cn FROM Setcodes WHERE set_name_cn IS NOT NULL")
    for row in db.fetchall():
        all_terms.add(row["set_name_cn"].lower())
    db.execute("SELECT set_name_jp FROM Setcodes WHERE set_name_jp IS NOT NULL")
    for row in db.fetchall():
        all_terms.add(row["set_name_jp"].lower())

    log.info(f"  - Found {len(all_terms)} unique terms to check against deck names.")

    db.execute("SELECT deck_id, deck_name FROM Decks")
    all_decks = db.fetchall()

    for term in all_terms:
        if not term or len(term) <= 1:
            continue
        for deck in all_decks:
            if term in deck["deck_name"].lower():
                all_mappings.add((term, deck["deck_id"]))

    log.info(f"Total unique mappings after title-based processing: {len(all_mappings)}")

    # --- Final Insertion ---
    if all_mappings:
        sorted_mappings = sorted(list(all_mappings))
        log.info(
            f"Inserting {len(sorted_mappings)} final unique mappings into SearchIndexToDecks table..."
        )
        db.executemany(
            "INSERT OR IGNORE INTO SearchIndexToDecks VALUES (?, ?)", sorted_mappings
        )

    log.info("Unified search index build complete.")
