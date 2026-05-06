"""Schema management for database tables."""

import logging
from typing import Optional
from .database import DatabaseConnection

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages database schema operations."""

    def __init__(self, connection: DatabaseConnection):
        """Initialize with a database connection."""
        self.connection = connection

    def create_tables(self, sql_file: str = "staging_tables.sql") -> bool:
        """Create tables from SQL file."""
        logger.info(f"Creating tables from {sql_file}...")

        try:
            with open(sql_file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # Split by GO batches
            batches = sql_content.split("GO")
            batch_count = 0

            for batch in batches:
                batch = batch.strip()
                if batch:
                    if not self.connection.execute_query(batch):
                        logger.error(f"Failed to execute batch {batch_count + 1}")
                        return False
                    batch_count += 1

            logger.info(
                f"Successfully created tables from {sql_file} ({batch_count} batches)"
            )
            return True

        except FileNotFoundError:
            logger.error(f"SQL file not found: {sql_file}")
            return False
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            return False

    def list_tables(self) -> Optional[list]:
        """List all tables in the database."""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE='BASE TABLE' 
        ORDER BY TABLE_NAME
        """
        results = self.connection.fetch_query(query)
        return results

    def table_count(self, table_name: str) -> Optional[int]:
        """Get row count for a specific table."""
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.connection.fetch_query(query)
            if result and len(result) > 0:
                return result[0][0]
            return 0
        except Exception as e:
            logger.warning(f"Could not get count for {table_name}: {e}")
            return None
