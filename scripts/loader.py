"""CSV data loading for Olist dataset."""

import csv
import json
import logging
import os
from typing import Dict, Tuple
from tqdm import tqdm
from .database import DatabaseConnection

logger = logging.getLogger(__name__)


class DataLoader:
    """Manages CSV data loading into database."""

    CSV_TABLE_MAPPING = {
        "data/olist_customers_dataset.csv": "olist_customers",
        "data/olist_geolocation_dataset.csv": "olist_geolocation",
        "data/olist_orders_dataset.csv": "olist_orders",
        "data/olist_order_items_dataset.csv": "olist_order_items",
        "data/olist_order_payments_dataset.csv": "olist_order_payments",
        "data/olist_order_reviews_dataset.csv": "olist_order_reviews",
        "data/olist_products_dataset.csv": "olist_products",
        "data/olist_sellers_dataset.csv": "olist_sellers",
        "data/product_category_name_translation.csv": "product_category_name_translation",
    }

    def __init__(self, connection: DatabaseConnection):
        """Initialize with a database connection."""
        self.connection = connection

    def load_csv_to_table(self, csv_file: str, table_name: str) -> Tuple[bool, int]:
        """Load data from CSV file into a table."""
        logger.info(f"Loading data from {csv_file} into {table_name}...")

        if not os.path.exists(csv_file):
            logger.error(f"CSV file not found: {csv_file}")
            return False, 0

        try:
            row_count = 0
            duplicate_count = 0
            error_count = 0
            cursor = self.connection.connection.cursor()

            with open(csv_file, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)

                if not reader.fieldnames:
                    logger.error(f"CSV file {csv_file} has no headers")
                    return False, 0

                columns = [col.strip('"') for col in reader.fieldnames]
                placeholders = ", ".join(["%s"] * len(columns))
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

                total_rows = self._count_csv_rows(csv_file)
                with tqdm(total=total_rows, desc=table_name, unit="rows", leave=False) as pbar:
                    for row in reader:
                        try:
                            values = []
                            for col in columns:
                                val = row.get(col.strip('"'), "")
                                if val == "" or val == "nan":
                                    values.append(None)
                                else:
                                    values.append(
                                        val.strip('"') if isinstance(val, str) else val
                                    )

                            cursor.execute(insert_query, tuple(values))
                            row_count += 1
                        except Exception as e:
                            error_msg = str(e).lower()
                            if "duplicate key" in error_msg or "2627" in error_msg:
                                duplicate_count += 1
                            else:
                                logger.warning(f"Error inserting row in {table_name}: {e}")
                                error_count += 1
                        finally:
                            pbar.update(1)

                self.connection.connection.commit()

                # Log summary
                total_processed = row_count + duplicate_count + error_count
                logger.info(
                    f"Loaded {row_count} rows into {table_name} "
                    f"({duplicate_count} duplicates skipped, {error_count} errors)"
                )
                return error_count == 0, row_count

        except Exception as e:
            logger.error(f"Failed to load CSV {csv_file}: {e}")
            if self.connection.connection:
                self.connection.connection.rollback()
            return False, 0

    LOAD_STATUS_FILE = "load_status.json"

    def _get_table_row_count(self, table_name: str) -> int:
        """Return current row count for a table, or -1 on error."""
        try:
            result = self.connection.fetch_query(f"SELECT COUNT(*) FROM {table_name}")
            return result[0][0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name}: {e}")
            return -1

    def _count_csv_rows(self, csv_file: str) -> int:
        """Count data rows in CSV file (excluding header), or -1 on error."""
        try:
            with open(csv_file, "r", encoding="utf-8-sig") as f:
                return sum(1 for _ in f) - 1
        except Exception:
            return -1

    def _read_status(self) -> dict[str, int]:
        """Return persisted load status (table -> inserted row count)."""
        try:
            with open(self.LOAD_STATUS_FILE, "r") as f:
                data = json.load(f)
                return {str(k): int(v) for k, v in data.items()}
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write_status(self, table_name: str, row_count: int) -> None:
        """Record a completed load so resume logic can skip it next run."""
        status = self._read_status()
        status[table_name] = row_count
        with open(self.LOAD_STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2)

    def _bootstrap_status(self, exclude: set[str]) -> None:
        """Populate status file from current DB counts for tables not in exclude set.
        Used when the status file is missing but tables already have correct data.
        """
        status = self._read_status()
        for table_name in self.CSV_TABLE_MAPPING.values():
            if table_name in exclude or table_name in status:
                continue
            count = self._get_table_row_count(table_name)
            if count > 0:
                status[table_name] = count
                logger.info(f"Bootstrapped status for {table_name} ({count} rows)")
        with open(self.LOAD_STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2)

    def load_all_data(self, skip_existing: bool = True, reload_tables: set[str] | None = None) -> bool:
        """Load all CSV data into the database."""
        logger.info("=" * 60)
        logger.info("Loading CSV Data")
        logger.info("=" * 60)

        if not self.connection.connection:
            logger.error("Not connected to database")
            return False

        success_count = 0
        skipped_count = 0
        total_count = len(self.CSV_TABLE_MAPPING)
        total_rows = 0

        if reload_tables:
            self._bootstrap_status(exclude=reload_tables)

        status = self._read_status() if skip_existing else {}

        table_items = list(self.CSV_TABLE_MAPPING.items())
        with tqdm(total=len(table_items), desc="Tables", unit="table") as outer:
            for csv_file, table_name in table_items:
                outer.set_postfix(table=table_name)
                force_this = reload_tables is not None and table_name in reload_tables
                if skip_existing and not force_this:
                    actual = self._get_table_row_count(table_name)
                    recorded = status.get(table_name, -1)

                    if recorded >= 0 and actual == recorded:
                        logger.info(f"Skipping {table_name} ({actual} rows, complete)")
                        skipped_count += 1
                        outer.update(1)
                        continue
                    elif actual > 0:
                        logger.warning(
                            f"Partial load in {table_name} ({actual} rows, expected {recorded}), truncating and reloading"
                        )
                        self.connection.execute_query(f"TRUNCATE TABLE {table_name}")
                elif force_this:
                    actual = self._get_table_row_count(table_name)
                    if actual > 0:
                        logger.info(f"Forcing reload of {table_name}, truncating ({actual} rows)")
                        self.connection.execute_query(f"TRUNCATE TABLE {table_name}")

                success, row_count = self.load_csv_to_table(csv_file, table_name)
                if success:
                    success_count += 1
                    total_rows += row_count
                    self._write_status(table_name, row_count)
                else:
                    logger.warning(f"Failed to load {csv_file}")
                outer.update(1)

        logger.info("=" * 60)
        logger.info(
            f"Data loading completed: {success_count} loaded, {skipped_count} skipped, "
            f"{total_count - success_count - skipped_count} failed"
        )
        logger.info(f"Total rows loaded: {total_rows}")
        logger.info("=" * 60)

        return success_count + skipped_count == total_count
