#!/usr/bin/env python3
"""
STAGING LAYER: Load CSV data into raw staging tables
Handles CSV ingestion into the olist database.

Usage:
    python main_staging.py                    # Load data (smart mode)
    python main_staging.py --force            # Recreate & reload
    python main_staging.py --no-load          # Create tables only
"""

import sys
import time
import logging
from scripts import DatabaseConfig, DatabaseConnection, SchemaManager, DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def setup_staging_database(
    server: str = "localhost,1433",
    username: str = "sa",
    password: str = "Test123!",
    sql_file: str = "1_staging_tables.sql",
    force_recreate: bool = False,
    skip_data_load: bool = False,
) -> bool:
    """Setup staging database with raw CSV data."""
    logger.info("=" * 70)
    logger.info("STAGING LAYER: Loading CSV Data")
    logger.info("=" * 70)

    # Configuration for staging database
    config = DatabaseConfig(
        server=server, username=username, password=password, database="olist"
    )

    # Initialize connection manager
    db_conn = DatabaseConnection(config)

    # Connect to master database
    if not db_conn.connect_to_master():
        logger.error("Failed to connect to SQL Server")
        return False

    try:
        # Check if database exists
        db_exists = db_conn.database_exists()

        if db_exists:
            if force_recreate:
                logger.info(f"Database 'olist' already exists. Recreating...")
                if not db_conn.drop_database():
                    logger.error("Failed to drop database")
                    return False
                time.sleep(2)
                if not db_conn.create_database():
                    logger.error("Failed to create database")
                    return False
            else:
                logger.info(f"Database 'olist' already exists. Connecting...")
        else:
            if not db_conn.create_database():
                logger.error("Failed to create database")
                return False

        # Close connection to master and connect to staging database
        db_conn.close_connection()
        if not db_conn.connect_to_database():
            logger.error("Failed to connect to staging database")
            return False

        # Create tables
        schema_manager = SchemaManager(db_conn)

        # Check if tables already exist
        existing_tables = schema_manager.list_tables()
        if not existing_tables or force_recreate:
            if not schema_manager.create_tables(sql_file):
                logger.error("Failed to create staging tables")
                return False
        else:
            logger.info(f"Staging tables exist ({len(existing_tables)} tables)")

        # List created tables
        tables = schema_manager.list_tables()
        if tables:
            logger.info(f"Staging tables ready ({len(tables)} tables):")
            for table in tables:
                logger.info(f"  - {table[0]}")

        # Load data if not skipped
        if not skip_data_load:
            data_loader = DataLoader(db_conn)
            if not data_loader.load_all_data(skip_existing=not force_recreate):
                logger.warning("Some CSV data failed to load")

        logger.info("=" * 70)
        logger.info("✓ Staging layer completed successfully!")
        logger.info("=" * 70)
        return True

    finally:
        db_conn.close_connection()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Staging Layer: Load CSV data into raw tables"
    )
    parser.add_argument(
        "--server",
        default="localhost,1433",
        help="SQL Server instance (default: localhost,1433)",
    )
    parser.add_argument("--username", default="sa", help="Username (default: sa)")
    parser.add_argument(
        "--password", default="Test123!", help="Password (default: Test123!)"
    )
    parser.add_argument(
        "--sql-file",
        default="1_staging_tables.sql",
        help="SQL file with staging table definitions",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recreate database and reload all data",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Alias for --force (backwards compatibility)",
    )
    parser.add_argument(
        "--no-load",
        action="store_true",
        help="Skip CSV data loading, only create tables",
    )

    args = parser.parse_args()
    force_recreate = args.force or args.recreate

    success = setup_staging_database(
        server=args.server,
        username=args.username,
        password=args.password,
        sql_file=args.sql_file,
        force_recreate=force_recreate,
        skip_data_load=args.no_load,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
