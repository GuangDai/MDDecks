# manage.py
"""
Main command-line interface for managing the Yu-Gi-Oh! database.
Provides commands to build the local DB and deploy it to Cloudflare D1.
"""

import argparse
import sys

import structlog

from config import setup_logging
from data_management.updater import update_local_files

# CHANGED: Updated import paths to reflect the new file locations.
from database.build_database import run_build_process
from database.deploy_to_d1 import run_d1_deployment

# Initialize logging as the very first step
setup_logging()
log = structlog.get_logger(__name__)


def main():
    """Parses command-line arguments and executes the requested action."""
    parser = argparse.ArgumentParser(
        description="Yu-Gi-Oh! Database Management Tool.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Available commands"
    )

    # --- 'build-local' command ---
    parser_build = subparsers.add_parser(
        "build-local", help="Build the local SQLite database from source files."
    )
    parser_build.add_argument(
        "--update", action="store_true", help="Check for data updates before building."
    )
    parser_build.add_argument(
        "--force-update",
        action="store_true",
        help="Force download all data before building.",
    )

    # --- 'deploy-d1' command ---
    parser_deploy = subparsers.add_parser(
        "deploy-d1", help="Build the local DB and deploy it to Cloudflare D1."
    )
    parser_deploy.add_argument(
        "--update",
        action="store_true",
        help="Check for data updates before building and deploying.",
    )
    parser_deploy.add_argument(
        "--force-update",
        action="store_true",
        help="Force download all data before building and deploying.",
    )

    args = parser.parse_args()

    try:
        # Step 1: Handle data updates if requested
        should_run_action = False
        if args.force_update:
            log.info("Force update requested. Re-downloading all data...")
            update_local_files(force=True)
            should_run_action = True
        elif args.update:
            log.info("Update check requested.")
            if update_local_files():
                log.info("Data was updated, proceeding with action.")
                should_run_action = True
            else:
                log.info("Local data is up-to-date.")
                if args.command == "build-local":
                    log.info(
                        "No rebuild needed. Use 'build-local' without flags to force a build."
                    )
                else:
                    should_run_action = True
        else:
            should_run_action = True
            log.info("No update check requested. Proceeding directly with the command.")

        # Step 2: Execute the chosen command
        if not should_run_action:
            sys.exit(0)

        if args.command == "build-local":
            run_build_process()

        elif args.command == "deploy-d1":
            log.info("Starting D1 deployment workflow...")
            log.info("Step 1: Building local SQLite database...")
            build_success = run_build_process()

            if build_success:
                log.info("Local build successful. Proceeding to D1 deployment.")
                log.info("Step 2: Deploying to Cloudflare D1...")
                run_d1_deployment()
            else:
                log.error("Local database build failed. Halting deployment.")
                sys.exit(1)

    except Exception:
        log.exception("A fatal error occurred in the management script.")
        sys.exit(1)


if __name__ == "__main__":
    main()
