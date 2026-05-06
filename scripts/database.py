"""Database connection management."""

import logging
import time
from dataclasses import dataclass
from typing import Optional
import pymssql

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    """Database configuration."""

    server: str = "localhost,1433"
    username: str = "sa"
    password: str = "Test123!"
    database: str = "olist"

    def get_connection_string(self, master: bool = False) -> dict:
        """Get connection parameters for pymssql."""
        server_parts = self.server.split(",")
        host = server_parts[0]
        port = int(server_parts[1]) if len(server_parts) > 1 else 1433

        return {
            "server": f"{host}:{port}",
            "user": self.username,
            "password": self.password,
            "database": "master" if master else self.database,
            "as_dict": False,
        }


class DatabaseConnection:
    """Manages SQL Server database connections."""

    def __init__(self, config: DatabaseConfig):
        """Initialize with database configuration."""
        self.config = config
        self.connection = None

    def connect_to_master(self) -> bool:
        """Connect to master database."""
        try:
            logger.info("Connecting to master database...")
            conn_params = self.config.get_connection_string(master=True)
            self.connection = pymssql.connect(**conn_params)
            logger.info("Connected to master database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to master database: {e}")
            return False

    def connect_to_database(self) -> bool:
        """Connect to olist database."""
        try:
            logger.info(f"Connecting to {self.config.database} database...")
            conn_params = self.config.get_connection_string(master=False)
            self.connection = pymssql.connect(**conn_params)
            logger.info(f"Connected to {self.config.database} database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to {self.config.database} database: {e}")
            return False

    def close_connection(self):
        """Close the current connection."""
        if self.connection:
            self.connection.close()
            logger.debug("Connection closed")

    def execute_query(self, query: str, autocommit: bool = False) -> bool:
        """Execute a query."""
        if not self.connection:
            logger.error("No active connection")
            return False

        try:
            if autocommit:
                self.connection.autocommit(True)

            cursor = self.connection.cursor()
            cursor.execute(query)
            cursor.close()

            if autocommit:
                self.connection.autocommit(False)
            else:
                self.connection.commit()

            logger.debug("Query executed successfully")
            return True
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def fetch_query(self, query: str) -> Optional[list]:
        """Execute a SELECT query and return results."""
        if not self.connection:
            logger.error("No active connection")
            return None

        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return None

    def database_exists(self) -> bool:
        """Check if database exists."""
        query = f"SELECT database_id FROM sys.databases WHERE name = '{self.config.database}'"
        result = self.fetch_query(query)
        return result is not None and len(result) > 0

    def create_database(self) -> bool:
        """Create the database."""
        logger.info(f"Creating database '{self.config.database}'...")
        query = f"CREATE DATABASE [{self.config.database}]"
        success = self.execute_query(query, autocommit=True)
        if success:
            logger.info(f"Database '{self.config.database}' created successfully")
            time.sleep(2)  # Allow database to fully initialize
        else:
            logger.error(f"Failed to create database '{self.config.database}'")
        return success

    def drop_database(self) -> bool:
        """Drop the database."""
        logger.info(f"Dropping database '{self.config.database}'...")
        self.close_connection()

        # Reconnect to master to drop database
        if not self.connect_to_master():
            logger.error("Failed to reconnect to master database")
            return False

        query = f"ALTER DATABASE [{self.config.database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{self.config.database}]"
        success = self.execute_query(query, autocommit=True)
        if success:
            logger.info(f"Database '{self.config.database}' dropped successfully")
        else:
            logger.error(f"Failed to drop database '{self.config.database}'")
        return success
