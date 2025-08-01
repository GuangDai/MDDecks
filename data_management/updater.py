# data_management/updater.py
"""
Handles downloading and updating all local data files from web sources.

This module is responsible for all network interactions related to fetching
data. It uses a local cache (`update_info.json`) to avoid re-downloading
files unnecessarily, respecting the update interval defined in the config.
"""

import io
import json
import os
import re
import time
import zipfile
from typing import Dict, Any, Optional

import requests
import structlog

from config import (
    URLS,
    LOCAL_CARDS_FILE,
    LOCAL_CONSTANTS_FILE,
    LOCAL_SETCODES_FILE,
    LOCAL_ALIAS_DB_FILE,
    UPDATE_INFO_FILE,
    UPDATE_INTERVAL_SECONDS,
)

log = structlog.get_logger(__name__)


def _load_update_info() -> Dict[str, Any]:
    """
    Loads the update information cache from a local JSON file.

    The cache file stores metadata about the last download attempt for each file,
    such as the timestamp and MD5 hash.

    Workflow:
    1.  Checks if `update_info.json` exists. If not, returns an empty dict.
    2.  If it exists, it attempts to open, read, and parse the JSON content.
    3.  Handles potential errors like the file being unreadable (`IOError`),
        not found (`FileNotFoundError`), or containing invalid JSON
        (`json.JSONDecodeError`). In these cases, it logs a warning and
        returns an empty dict, forcing a fresh download cycle.

    Expected Input:
    - A JSON file (`update_info.json`) on disk, possibly non-existent.
      Example structure: {"cards": {"last_check": 1672531200, "md5": "..."}}

    Returns:
        - A dictionary containing the cached update info.
          Size: Varies, but typically less than 1KB.
    """
    if not os.path.exists(UPDATE_INFO_FILE):
        return {}
    try:
        with open(UPDATE_INFO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
        log.warning(
            "Could not read or parse update_info.json. Starting fresh.",
            file=UPDATE_INFO_FILE,
            error_type=type(e).__name__,
            error_message=str(e),
        )
        return {}


def _save_update_info(info: Dict[str, Any]):
    """
    Saves the update information cache to a local JSON file.

    Workflow:
    1.  Opens `update_info.json` in write mode.
    2.  Dumps the provided dictionary into the file as formatted JSON.
    3.  Catches `IOError` if the file cannot be written (e.g., due to permissions).

    Args:
        - info (Dict[str, Any]): The dictionary containing the update metadata to save.
    """
    try:
        with open(UPDATE_INFO_FILE, "w", encoding="utf-8") as f:
            json.dump(info, f, indent=4)
    except IOError as e:
        log.error(
            "Failed to save update info file.", file=UPDATE_INFO_FILE, error=str(e)
        )


def _fetch_url(url: str, is_binary: bool = False) -> Optional[bytes | str]:
    """
    Fetches content from a URL with robust error handling.

    Workflow:
    1.  Sends an HTTP GET request to the specified URL with a 60-second timeout.
    2.  Calls `response.raise_for_status()` to automatically raise an exception
        for HTTP error codes (4xx or 5xx).
    3.  If the request is successful, it returns the response content.
    4.  If any `requests.exceptions.RequestException` occurs (e.g., timeout,
        DNS error, HTTP error), it logs the error and returns `None`.

    Args:
        - url (str): The URL to fetch.
        - is_binary (bool): If True, returns content as bytes. Otherwise, returns as a decoded string.

    Returns:
        - The content as bytes or string, or `None` if an error occurred.
          Size: Can range from a few bytes (MD5 file) to several megabytes (zip file).
    """
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()  # Check for HTTP errors
        return response.content if is_binary else response.text
    except requests.exceptions.RequestException as e:
        log.error("Network error while fetching URL.", url=url, error=str(e))
        return None


def update_local_files(force: bool = False) -> bool:
    """
    Checks for and downloads updates for all necessary data files.

    This function is idempotent and respects the update interval to avoid
    unnecessary downloads. It's the main public function of this module.

    Workflow:
    1.  Loads the existing update cache from disk.
    2.  Iterates through a predefined list of "generic" file targets.
        a. For each target, it determines if an update is needed based on the `force`
           flag or if enough time (`UPDATE_INTERVAL_SECONDS`) has passed.
        b. If an update is needed, it fetches the file and writes it to disk.
        c. On success, it updates the timestamp for that file in the cache.
    3.  Performs a special check for the main card data, which uses an MD5 hash.
        a. It first fetches the remote MD5 hash.
        b. If the remote hash is different from the cached local hash (or if forced),
           it proceeds to download the large zip file.
        c. It extracts `cards.json` from the zip file.
        d. On success, it updates both the timestamp and the MD5 hash in the cache.
    4.  Finally, it saves the modified update cache back to disk.

    Args:
        - force (bool): If True, bypasses the update interval and MD5 checks,
                        forcing a download attempt for all files.

    Returns:
        - True if any file was downloaded/updated, False otherwise.
    """
    log.info("--- Starting Data Update Check ---", force_mode=force)
    update_info = _load_update_info()
    files_updated = False

    # --- Part 1: Generic file updates (based on time interval) ---
    update_targets = {
        "constants": {
            "url": URLS["constants"],
            "file": LOCAL_CONSTANTS_FILE,
            "label": "Constants file",
        },
        "setcodes": {
            "url": URLS["setcodes"],
            "file": LOCAL_SETCODES_FILE,
            "label": "Setcodes file",
        },
        "alias_db": {
            "url": URLS["alias_db"],
            "file": LOCAL_ALIAS_DB_FILE,
            "label": "Alias DB",
            "binary": True,
        },
    }

    for key, target in update_targets.items():
        log.info(
            f"Checking {target['label']}...", file=os.path.basename(target["file"])
        )
        info = update_info.get(key, {})
        is_stale = time.time() - info.get("last_check", 0) > UPDATE_INTERVAL_SECONDS

        if force or is_stale:
            content = _fetch_url(target["url"], is_binary=target.get("binary", False))
            if content:
                try:
                    mode = "wb" if target.get("binary", False) else "w"
                    encoding = None if target.get("binary", False) else "utf-8"
                    with open(target["file"], mode, encoding=encoding) as f:
                        f.write(content)
                    log.info(
                        f" -> '{os.path.basename(target['file'])}' has been updated."
                    )
                    update_info[key] = {"last_check": time.time()}
                    files_updated = True
                except IOError as e:
                    log.error(f"Failed to write file {target['file']}", error=str(e))
        else:
            log.info(" -> Update check skipped (within interval).")

    # --- Part 2: Special card data update (based on MD5 check for efficiency) ---
    log.info("Checking card data (cards.json)...")
    card_info = update_info.get("cards", {})
    is_card_stale = (
        time.time() - card_info.get("last_check", 0) > UPDATE_INTERVAL_SECONDS
    )

    if force or is_card_stale:
        md5_text = _fetch_url(URLS["cards_md5"])
        if md5_text:
            # The MD5 is wrapped in a JS callback function, so we extract it with regex.
            match = re.search(r'gu\("([a-f0-9]{32})"\);', md5_text)
            remote_md5 = match.group(1) if match else None

            if not remote_md5:
                log.error(
                    "Could not parse remote MD5 from callback.", response_text=md5_text
                )
            elif force or remote_md5 != card_info.get("md5"):
                log.info(
                    f" -> New version found (MD5: {remote_md5[:12]}...), downloading..."
                )
                zip_content = _fetch_url(URLS["cards_zip"], is_binary=True)
                if zip_content:
                    try:
                        # Process the downloaded zip file in memory.
                        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
                            # Extract only the file we need.
                            zf.extract("cards.json", path=".")
                        log.info(
                            f" -> '{LOCAL_CARDS_FILE}' successfully downloaded and extracted."
                        )
                        update_info["cards"] = {
                            "last_check": time.time(),
                            "md5": remote_md5,
                        }
                        files_updated = True
                    except (zipfile.BadZipFile, KeyError, IOError) as e:
                        log.error("Failed to process cards.zip.", error=str(e))
                else:
                    log.error("Failed to download cards.zip content.")
            else:
                log.info(" -> 'cards.json' is already up to date.")
                # Even if no download, update the check time so we don't check again soon.
                update_info["cards"]["last_check"] = time.time()
    else:
        log.info(" -> Update check skipped (within interval).")

    # Persist the updated cache to disk for the next run.
    _save_update_info(update_info)
    log.info("--- Data Update Check Finished ---", files_were_updated=files_updated)
    return files_updated
