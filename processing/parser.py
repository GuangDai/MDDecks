# processing/parser.py
"""
Contains functions for parsing data from local files.

This module is responsible for reading the raw data files (like `constant.lua`
and `strings.conf`) and transforming them into structured Python objects
(dictionaries and lists) that can be used by the database builder. Each function
is tailored to the specific format of the file it processes.
"""

import re
import sqlite3
from typing import Tuple, Dict, List
import os
import structlog

from config import LOCAL_CONSTANTS_FILE, LOCAL_SETCODES_FILE, LOCAL_ALIAS_DB_FILE

log = structlog.get_logger(__name__)


def parse_local_constants() -> Tuple[Dict[int, str], Dict[int, str], Dict[int, str]]:
    """
    Parses card constants (Race, Attribute, Type) from the `constant.lua` file.

    The `.lua` file defines these constants as global variables with their
    hexadecimal bitmask value and a comment containing the human-readable name.
    This function uses a regular expression to capture this information.

    Workflow:
    1.  Reads the entire content of the `constant.lua` file.
    2.  Defines a regex pattern to match lines defining constants.
    3.  Iterates through each line of the file.
    4.  If a line matches the pattern, it extracts the category (RACE, etc.),
        the hex value, and the name from the comment.
    5.  It converts the hex value to an integer and stores the mapping in the
        appropriate dictionary.
    6.  It skips certain helper/aggregate constants.
    7.  Returns the three populated dictionaries.

    Regex Breakdown: `^(TYPE|ATTRIBUTE|RACE)_([A-Z_]+)\s*=\s*(0x[0-9a-fA-F]+)\s*--\s*(.+)$`
    - `^`: Asserts position at the start of the string.
    - `(TYPE|ATTRIBUTE|RACE)`: Group 1. Matches and captures the constant category.
    - `_([A-Z_]+)`: Group 2. Matches the constant's specific name (e.g., 'WARRIOR').
    - `\s*=\s*`: Matches the equals sign, surrounded by optional whitespace.
    - `(0x[0-9a-fA-F]+)`: Group 3. Matches and captures the hexadecimal value.
    - `\s*--\s*`: Matches the Lua comment delimiter '--', surrounded by optional whitespace.
    - `(.+)`: Group 4. Greedily captures the human-readable name from the comment.
    - `$`: Asserts position at the end of the string.

    Returns:
        - A tuple containing three dictionaries: `(race_map, attribute_map, type_map)`.
          Each dictionary maps an integer constant value to its string name.
          Example `race_map`: `{1: 'Warrior', 2: 'Spellcaster', ...}`.
          The dictionaries will contain approximately 25-30 items each.
    """
    log.info("Parsing local constants file...", file=LOCAL_CONSTANTS_FILE)
    maps: Dict[str, Dict[int, str]] = {"RACE": {}, "ATTRIBUTE": {}, "TYPE": {}}
    try:
        with open(LOCAL_CONSTANTS_FILE, "r", encoding="utf-8") as f:
            content = f.read()
    except (IOError, FileNotFoundError) as e:
        log.error("Failed to read constants file.", error=str(e))
        return {}, {}, {}

    pattern = re.compile(
        r"^(TYPE|ATTRIBUTE|RACE)_([A-Z_]+)\s*=\s*(0x[0-9a-fA-F]+)\s*--\s*(.+)"
    )
    for line in content.splitlines():
        match = pattern.match(line)
        if match:
            map_key, _, hex_val, name = match.groups()
            # Skip aggregate/helper constants that are not actual types.
            if "ALL" in line or "TYPES_" in line:
                continue
            maps[map_key][int(hex_val, 16)] = name.strip()

    log.info("Finished parsing constants.", counts={k: len(v) for k, v in maps.items()})
    return maps["RACE"], maps["ATTRIBUTE"], maps["TYPE"]


def load_alias_map() -> Dict[int, int]:
    """
    Loads card ID aliases (for alternate artworks) from the local `cards.cdb`.

    `cards.cdb` is a SQLite database. This function connects to it in read-only
    mode to extract mappings from an alternate art card's ID to its original,
    canonical card ID.

    Workflow:
    1.  Connects to the `.cdb` file using `sqlite3`, specifying read-only mode.
    2.  Executes a simple SELECT query on the `datas` table.
    3.  Fetches all results where a card has a non-zero `alias` value.
    4.  Constructs a dictionary mapping the alias card ID (`id` column) to the
        original card ID (`alias` column).
    5.  Closes the connection and returns the map.

    Returns:
        - A dictionary mapping an alias card ID (int) to its original card ID (int).
          Example: `{12345678: 87654321}`.
          Size: Contains mappings for several hundred alternate art cards.
    """
    log.info("Loading alias ID map...", file=LOCAL_ALIAS_DB_FILE)
    alias_map: Dict[int, int] = {}
    if not os.path.exists(LOCAL_ALIAS_DB_FILE):
        log.warning(
            "Alias database not found, continuing without alias mapping.",
            file=LOCAL_ALIAS_DB_FILE,
        )
        return {}

    try:
        # Connect in read-only mode to prevent accidental modification.
        conn = sqlite3.connect(f"file:{LOCAL_ALIAS_DB_FILE}?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute("SELECT id, alias FROM datas WHERE alias != 0")
        # Creates a dict of {alias_id: original_id}.
        alias_map = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
    except sqlite3.Error as e:
        log.error(
            "Failed to load or query alias database.",
            file=LOCAL_ALIAS_DB_FILE,
            error=str(e),
        )

    log.info(f"Loaded {len(alias_map)} alias ID mappings.")
    return alias_map


def parse_setcodes() -> Tuple[Dict[int, str], List[Tuple]]:
    """
    Parses setcode information from the `strings.conf` file.

    The file contains lines starting with `!setname` that define archetypes/series.
    This function extracts this information into two useful data structures.

    Workflow:
    1.  Reads the `strings.conf` file line by line.
    2.  If a line starts with `!setname`, it splits the line into parts.
    3.  It expects at least 3 parts: `!setname`, hex code, and Chinese name.
        A fourth part (Japanese name) is optional.
    4.  It parses the hex code into an integer.
    5.  It populates two data structures simultaneously:
        a. `setcode_map`: A dictionary for fast lookups (`{code: name}`).
        b. `setcodes_to_insert`: A list of tuples, pre-formatted for database
           insertion with `executemany`.
    6.  Returns both structures.

    Returns:
        - A tuple containing:
          1. `setcode_map` (Dict[int, str]): Maps setcode integer to name.
             Used for in-memory checks during card processing.
             Example: `{0x3011: 'Blue-Eyes'}`.
             Size: Contains several hundred setcodes.
          2. `setcodes_to_insert` (List[Tuple]): Formatted for `executemany`.
             Example: `[(12305, 'Blue-Eyes', '青眼')]`.
    """
    log.info("Parsing setcodes file...", file=LOCAL_SETCODES_FILE)
    setcode_map: Dict[int, str] = {}
    setcodes_to_insert: List[Tuple] = []
    try:
        with open(LOCAL_SETCODES_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("!setname"):
                    parts = line.strip().split(maxsplit=3)
                    if len(parts) >= 3:
                        try:
                            code = int(parts[1], 16)
                            name_cn = parts[2]
                            name_jp = parts[3] if len(parts) > 3 else None
                            setcodes_to_insert.append((code, name_cn, name_jp))
                            setcode_map[code] = name_cn
                        except (ValueError, IndexError):
                            log.warning(
                                "Skipping malformed setname line.", line=line.strip()
                            )
                            continue
    except (IOError, FileNotFoundError) as e:
        log.error("Failed to read setcodes file.", error=str(e))

    log.info(f"Parsed {len(setcode_map)} setcodes.")
    return setcode_map, setcodes_to_insert
