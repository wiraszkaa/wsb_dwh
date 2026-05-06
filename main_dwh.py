#!/usr/bin/env python3
"""
DATA WAREHOUSE LAYER: ETL from Staging to Star Schema
Transforms raw staging data into dimensional model for analytics.

Architecture:
    Staging DB (olist)  →  ETL Logic  →  DWH DB (olist_dwh)
         ↓                                      ↓
    • olist_customers              • dim_customer
    • olist_products               • dim_product
    • olist_sellers                • dim_seller
    • olist_orders          ─→      • dim_date
    • olist_order_items             • fact_order_items
    • olist_reviews                 • fact_orders
    • olist_payments                • fact_reviews
    • olist_geolocation             • fact_payments

Usage:
    python main_dwh.py              # Create DWH schema (default)
    python main_dwh.py --force      # Recreate DWH
    python main_dwh.py --etl        # Run full ETL pipeline
"""

import sys
import time
import logging
from datetime import datetime, timedelta
from scripts import DatabaseConfig, DatabaseConnection, SchemaManager

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DWHETLPipeline:
    """
    Data Warehouse ETL Pipeline

    Handles transformation from staging to star schema in logical steps:
    1. Database setup
    2. Schema creation
    3. Dimension population
    4. Fact table population
    5. Validation & indexing
    """

    def __init__(self, staging_server: str, staging_user: str, staging_pass: str):
        """Initialize ETL pipeline with staging connection details."""
        self.staging_server = staging_server
        self.staging_user = staging_user
        self.staging_pass = staging_pass

        # Connections
        self.staging_conn = None
        self.dwh_conn = None

    def connect_to_staging(self) -> bool:
        """Step 1: Connect to staging database."""
        logger.info("Step 1: Connecting to staging database 'olist'...")
        config = DatabaseConfig(
            server=self.staging_server,
            username=self.staging_user,
            password=self.staging_pass,
            database="olist",
        )
        self.staging_conn = DatabaseConnection(config)
        return self.staging_conn.connect_to_database()

    def connect_to_dwh(self) -> bool:
        """Step 2: Connect to DWH database."""
        logger.info("Step 2: Connecting to DWH database 'olist_dwh'...")
        config = DatabaseConfig(
            server=self.staging_server,
            username=self.staging_user,
            password=self.staging_pass,
            database="olist_dwh",
        )
        self.dwh_conn = DatabaseConnection(config)
        return self.dwh_conn.connect_to_database()

    def setup_dwh_database(self, force_recreate: bool = False) -> bool:
        """Step 0: Setup DWH database."""
        logger.info("Step 0: Setting up DWH database 'olist_dwh'...")

        # Connect to master
        config = DatabaseConfig(
            server=self.staging_server,
            username=self.staging_user,
            password=self.staging_pass,
            database="olist_dwh",
        )
        master_conn = DatabaseConnection(config)

        if not master_conn.connect_to_master():
            logger.error("Failed to connect to master database")
            return False

        try:
            # Check if database exists
            if master_conn.database_exists():
                if force_recreate:
                    logger.info("Dropping existing DWH database...")
                    if not master_conn.drop_database():
                        logger.error("Failed to drop DWH database")
                        return False
                    time.sleep(2)
                else:
                    logger.info("DWH database 'olist_dwh' already exists")
                    master_conn.close_connection()
                    return True

            # Create database
            logger.info("Creating DWH database 'olist_dwh'...")
            if not master_conn.create_database():
                logger.error("Failed to create DWH database")
                return False

            time.sleep(2)
            return True
        finally:
            master_conn.close_connection()

    def _table_has_data(self, table_name: str) -> bool:
        """Return True if the DWH table already contains rows."""
        try:
            result = self.dwh_conn.fetch_query(f"SELECT COUNT(*) FROM {table_name}")
            return result[0][0] > 0 if result else False
        except Exception:
            return False

    def create_dwh_schema(self, skip_if_exists: bool = True) -> bool:
        """Step 3: Create DWH star schema."""
        schema_manager = SchemaManager(self.dwh_conn)
        existing = schema_manager.list_tables()
        if skip_if_exists and existing:
            logger.info(f"Step 3: DWH schema already exists ({len(existing)} tables), skipping")
            return True
        logger.info("Step 3: Creating DWH star schema...")
        return schema_manager.create_tables("2_dwh_schema.sql")

    def populate_dim_date(self, skip_if_loaded: bool = True) -> bool:
        """
        Step 4: Populate Date Dimension
        Creates calendar from 2015-01-01 to 2020-12-31 (covers Olist data)
        """
        if skip_if_loaded and self._table_has_data("dim_date"):
            logger.info("Step 4: dim_date already loaded, skipping")
            return True
        logger.info("Step 4: Populating dim_date...")

        query = """
        -- Generate calendar dimension (covers Olist dataset period)
        DECLARE @start_date DATE = '2015-01-01';
        DECLARE @end_date DATE = '2020-12-31';
        DECLARE @current_date DATE = @start_date;

        WHILE @current_date <= @end_date
        BEGIN
            INSERT INTO dim_date (
                date_key, calendar_date, day_of_week, day_name,
                week_of_year, month_of_year, month_name, quarter, year,
                is_weekend, is_holiday
            )
            VALUES (
                CONVERT(INT, FORMAT(@current_date, 'yyyyMMdd')),
                @current_date,
                DATEPART(WEEKDAY, @current_date),
                DATENAME(WEEKDAY, @current_date),
                DATEPART(WEEK, @current_date),
                DATEPART(MONTH, @current_date),
                DATENAME(MONTH, @current_date),
                DATEPART(QUARTER, @current_date),
                DATEPART(YEAR, @current_date),
                CASE WHEN DATEPART(WEEKDAY, @current_date) IN (1, 7) THEN 1 ELSE 0 END,
                0  -- Can populate with Brazilian holidays later
            );
            
            SET @current_date = DATEADD(DAY, 1, @current_date);
        END;
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_customer(self, skip_if_loaded: bool = True) -> bool:
        """
        Step 5: Populate Customer Dimension
        Load customer master from staging with enrichment
        """
        if skip_if_loaded and self._table_has_data("dim_customer"):
            logger.info("Step 5: dim_customer already loaded, skipping")
            return True
        logger.info("Step 5: Populating dim_customer...")

        query = """
        INSERT INTO dim_customer (
            customer_id, customer_zip_code_prefix, customer_city,
            customer_state, region_name, customer_segment
        )
        SELECT DISTINCT
            c.customer_id,
            c.customer_zip_code_prefix,
            c.customer_city,
            c.customer_state,
            CASE c.customer_state
                WHEN 'PR' THEN 'South'
                WHEN 'SC' THEN 'South'
                WHEN 'RS' THEN 'South'
                WHEN 'SP' THEN 'Southeast'
                WHEN 'RJ' THEN 'Southeast'
                WHEN 'MG' THEN 'Southeast'
                WHEN 'ES' THEN 'Southeast'
                WHEN 'BA' THEN 'Northeast'
                WHEN 'PE' THEN 'Northeast'
                WHEN 'CE' THEN 'Northeast'
                WHEN 'PB' THEN 'Northeast'
                WHEN 'RN' THEN 'Northeast'
                WHEN 'AL' THEN 'Northeast'
                WHEN 'SE' THEN 'Northeast'
                WHEN 'MA' THEN 'Northeast'
                WHEN 'PI' THEN 'Northeast'
                WHEN 'PA' THEN 'North'
                WHEN 'AM' THEN 'North'
                WHEN 'RO' THEN 'North'
                WHEN 'AC' THEN 'North'
                WHEN 'AP' THEN 'North'
                WHEN 'RR' THEN 'North'
                WHEN 'TO' THEN 'North'
                WHEN 'DF' THEN 'Center-West'
                WHEN 'GO' THEN 'Center-West'
                WHEN 'MT' THEN 'Center-West'
                WHEN 'MS' THEN 'Center-West'
                ELSE 'Unknown'
            END as region_name,
            'Regular'  -- Can be enhanced with RFM scoring
        FROM olist.dbo.olist_customers c
        WHERE c.customer_id IS NOT NULL;
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_seller(self, skip_if_loaded: bool = True) -> bool:
        """
        Step 6: Populate Seller Dimension
        Load seller master with regional classification
        """
        if skip_if_loaded and self._table_has_data("dim_seller"):
            logger.info("Step 6: dim_seller already loaded, skipping")
            return True
        logger.info("Step 6: Populating dim_seller...")

        query = """
        INSERT INTO dim_seller (
            seller_id, seller_zip_code_prefix, seller_city,
            seller_state, region_name, seller_segment
        )
        SELECT DISTINCT
            s.seller_id,
            s.seller_zip_code_prefix,
            s.seller_city,
            s.seller_state,
            CASE s.seller_state
                WHEN 'PR' THEN 'South'
                WHEN 'SC' THEN 'South'
                WHEN 'RS' THEN 'South'
                WHEN 'SP' THEN 'Southeast'
                WHEN 'RJ' THEN 'Southeast'
                WHEN 'MG' THEN 'Southeast'
                WHEN 'ES' THEN 'Southeast'
                WHEN 'BA' THEN 'Northeast'
                WHEN 'PE' THEN 'Northeast'
                WHEN 'CE' THEN 'Northeast'
                WHEN 'PB' THEN 'Northeast'
                WHEN 'RN' THEN 'Northeast'
                WHEN 'AL' THEN 'Northeast'
                WHEN 'SE' THEN 'Northeast'
                WHEN 'MA' THEN 'Northeast'
                WHEN 'PI' THEN 'Northeast'
                WHEN 'PA' THEN 'North'
                WHEN 'AM' THEN 'North'
                WHEN 'RO' THEN 'North'
                WHEN 'AC' THEN 'North'
                WHEN 'AP' THEN 'North'
                WHEN 'RR' THEN 'North'
                WHEN 'TO' THEN 'North'
                WHEN 'DF' THEN 'Center-West'
                WHEN 'GO' THEN 'Center-West'
                WHEN 'MT' THEN 'Center-West'
                WHEN 'MS' THEN 'Center-West'
                ELSE 'Unknown'
            END as region_name,
            'Standard'  -- Can be enhanced with performance scoring
        FROM olist.dbo.olist_sellers s
        WHERE s.seller_id IS NOT NULL;
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_product(self, skip_if_loaded: bool = True) -> bool:
        """
        Step 7: Populate Product Dimension
        Load product catalog with category translation and derived fields
        """
        if skip_if_loaded and self._table_has_data("dim_product"):
            logger.info("Step 7: dim_product already loaded, skipping")
            return True
        logger.info("Step 7: Populating dim_product...")

        query = """
        INSERT INTO dim_product (
            product_id, category_id, category_name_en, category_name_pt,
            product_name_length, product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            volume_cm3, is_heavy, size_class
        )
        SELECT
            p.product_id,
            p.product_category_name,
            ISNULL(pct.product_category_name_english, p.product_category_name),
            p.product_category_name,
            p.product_name_lenght,
            p.product_description_lenght,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,
            CAST(ISNULL(p.product_length_cm, 0) * 
                 ISNULL(p.product_height_cm, 0) * 
                 ISNULL(p.product_width_cm, 0) AS DECIMAL(15,2)),
            CASE WHEN ISNULL(p.product_weight_g, 0) > 10000 THEN 1 ELSE 0 END,
            CASE
                WHEN ISNULL(p.product_weight_g, 0) < 500 AND
                     ISNULL(p.product_length_cm, 0) < 20 THEN 'Small'
                WHEN ISNULL(p.product_weight_g, 0) < 5000 AND
                     ISNULL(p.product_length_cm, 0) < 50 THEN 'Medium'
                WHEN ISNULL(p.product_weight_g, 0) < 15000 THEN 'Large'
                ELSE 'Extra Large'
            END
        FROM olist.dbo.olist_products p
        LEFT JOIN olist.dbo.product_category_name_translation pct
            ON p.product_category_name = pct.product_category_name
        WHERE p.product_id IS NOT NULL;
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_payment_type(self, skip_if_loaded: bool = True) -> bool:
        """Step 8: Populate Payment Type Dimension (static)"""
        if skip_if_loaded and self._table_has_data("dim_payment_type"):
            logger.info("Step 8: dim_payment_type already loaded, skipping")
            return True
        logger.info("Step 8: Populating dim_payment_type...")

        query = """
        INSERT INTO dim_payment_type (payment_type_code, payment_type_name, payment_category, risk_level)
        VALUES
            ('credit_card', 'Credit Card', 'Card', 'Medium'),
            ('debit_card', 'Debit Card', 'Card', 'Low'),
            ('boleto', 'Boleto Bancário', 'Bank Transfer', 'Low'),
            ('voucher', 'Voucher', 'Cash', 'Low'),
            ('not_defined', 'Not Defined', 'Unknown', 'High');
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_order_status(self, skip_if_loaded: bool = True) -> bool:
        """Step 9: Populate Order Status Dimension (static)"""
        if skip_if_loaded and self._table_has_data("dim_order_status"):
            logger.info("Step 9: dim_order_status already loaded, skipping")
            return True
        logger.info("Step 9: Populating dim_order_status...")

        query = """
        INSERT INTO dim_order_status (status_code, status_name, is_final_status, is_problem_status)
        VALUES
            ('created',         'Created',         0, 0),
            ('approved',        'Approved',        0, 0),
            ('invoiced',        'Invoiced',        0, 0),
            ('pending_payment', 'Pending Payment', 0, 0),
            ('processing',      'Processing',      0, 0),
            ('shipped',         'Shipped',         0, 0),
            ('delivered',       'Delivered',       1, 0),
            ('canceled',        'Canceled',        1, 1),
            ('unavailable',     'Unavailable',     1, 1);
        """

        return self.dwh_conn.execute_query(query)

    def populate_dim_geolocation(self, skip_if_loaded: bool = True) -> bool:
        """Step 10: Populate Geolocation Dimension — averaged per zip code prefix."""
        if skip_if_loaded and self._table_has_data("dim_geolocation"):
            logger.info("Step 10: dim_geolocation already loaded, skipping")
            return True
        logger.info("Step 10: Populating dim_geolocation...")

        query = """
        INSERT INTO dim_geolocation (
            zip_code_prefix, latitude, longitude, city, state, region_name,
            distance_from_sao_paulo_km
        )
        SELECT
            geolocation_zip_code_prefix,
            AVG(geolocation_lat)            AS latitude,
            AVG(geolocation_lng)            AS longitude,
            MIN(geolocation_city)           AS city,
            MIN(geolocation_state)          AS state,
            CASE MIN(geolocation_state)
                WHEN 'PR' THEN 'South'      WHEN 'SC' THEN 'South'
                WHEN 'RS' THEN 'South'
                WHEN 'SP' THEN 'Southeast'  WHEN 'RJ' THEN 'Southeast'
                WHEN 'MG' THEN 'Southeast'  WHEN 'ES' THEN 'Southeast'
                WHEN 'BA' THEN 'Northeast'  WHEN 'PE' THEN 'Northeast'
                WHEN 'CE' THEN 'Northeast'  WHEN 'PB' THEN 'Northeast'
                WHEN 'RN' THEN 'Northeast'  WHEN 'AL' THEN 'Northeast'
                WHEN 'SE' THEN 'Northeast'
                WHEN 'MA' THEN 'Northeast'  WHEN 'PI' THEN 'Northeast'
                WHEN 'PA' THEN 'North'      WHEN 'AM' THEN 'North'
                WHEN 'RO' THEN 'North'      WHEN 'AC' THEN 'North'
                WHEN 'AP' THEN 'North'      WHEN 'RR' THEN 'North'
                WHEN 'TO' THEN 'North'
                WHEN 'DF' THEN 'Center-West' WHEN 'GO' THEN 'Center-West'
                WHEN 'MT' THEN 'Center-West' WHEN 'MS' THEN 'Center-West'
                ELSE 'Unknown'
            END                             AS region_name,
            NULL                            AS distance_from_sao_paulo_km
        FROM olist.dbo.olist_geolocation
        GROUP BY geolocation_zip_code_prefix;
        """

        return self.dwh_conn.execute_query(query)

    def populate_fact_order_items(self, skip_if_loaded: bool = True) -> bool:
        """Step 11: Populate fact_order_items — grain: one row per order line."""
        if skip_if_loaded and self._table_has_data("fact_order_items"):
            logger.info("Step 11: fact_order_items already loaded, skipping")
            return True
        logger.info("Step 11: Populating fact_order_items...")

        query = """
        INSERT INTO fact_order_items (
            order_id, order_item_id, customer_key, product_key, seller_key,
            order_date_key, quantity, price, freight_value, total_item_value,
            is_out_of_stock
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            dc.customer_key,
            dp.product_key,
            ds.seller_key,
            CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) AS order_date_key,
            1                   AS quantity,
            oi.price,
            oi.freight_value,
            oi.price            AS total_item_value,
            0                   AS is_out_of_stock
        FROM olist.dbo.olist_order_items oi
        JOIN olist.dbo.olist_orders   o  ON oi.order_id   = o.order_id
        JOIN dim_customer             dc ON o.customer_id  = dc.customer_id
        JOIN dim_product              dp ON oi.product_id  = dp.product_id
        JOIN dim_seller               ds ON oi.seller_id   = ds.seller_id
        JOIN dim_date                 dd ON CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) = dd.date_key;
        """

        return self.dwh_conn.execute_query(query)

    def populate_fact_orders(self, skip_if_loaded: bool = True) -> bool:
        """Step 12: Populate fact_orders — grain: one row per order."""
        if skip_if_loaded and self._table_has_data("fact_orders"):
            logger.info("Step 12: fact_orders already loaded, skipping")
            return True
        logger.info("Step 12: Populating fact_orders...")

        query = """
        INSERT INTO fact_orders (
            order_id, customer_key, order_date_key,
            estimated_delivery_date_key, actual_delivery_date_key,
            order_status_key, total_items, total_price, total_freight_value,
            total_order_value, order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date, days_to_delivery, is_delayed
        )
        SELECT
            o.order_id,
            dc.customer_key,
            CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd'))         AS order_date_key,
            CASE WHEN o.order_estimated_delivery_date IS NOT NULL
                 THEN CONVERT(INT, FORMAT(o.order_estimated_delivery_date, 'yyyyMMdd'))
                 ELSE NULL END                                                    AS estimated_delivery_date_key,
            CASE WHEN o.order_delivered_customer_date IS NOT NULL
                 THEN CONVERT(INT, FORMAT(o.order_delivered_customer_date, 'yyyyMMdd'))
                 ELSE NULL END                                                    AS actual_delivery_date_key,
            dos.order_status_key,
            ISNULL(items.total_items,   0)                                        AS total_items,
            ISNULL(items.total_price,   0)                                        AS total_price,
            ISNULL(items.total_freight, 0)                                        AS total_freight_value,
            ISNULL(items.total_price, 0) + ISNULL(items.total_freight, 0)        AS total_order_value,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            CASE WHEN o.order_delivered_customer_date IS NOT NULL
                 THEN DATEDIFF(DAY, o.order_purchase_timestamp,
                                    o.order_delivered_customer_date)
                 ELSE NULL END                                                    AS days_to_delivery,
            CASE WHEN o.order_delivered_customer_date IS NOT NULL
                      AND o.order_estimated_delivery_date IS NOT NULL
                      AND o.order_delivered_customer_date > o.order_estimated_delivery_date
                 THEN 1 ELSE 0 END                                               AS is_delayed
        FROM olist.dbo.olist_orders o
        JOIN dim_customer    dc  ON o.customer_id = dc.customer_id
        JOIN dim_order_status dos ON o.order_status = dos.status_code
        JOIN dim_date         dd  ON CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) = dd.date_key
        LEFT JOIN (
            SELECT order_id,
                   COUNT(*)           AS total_items,
                   SUM(price)         AS total_price,
                   SUM(freight_value) AS total_freight
            FROM olist.dbo.olist_order_items
            GROUP BY order_id
        ) items ON o.order_id = items.order_id;
        """

        return self.dwh_conn.execute_query(query)

    def populate_fact_reviews(self, skip_if_loaded: bool = True) -> bool:
        """Step 13: Populate fact_reviews — grain: one row per review."""
        if skip_if_loaded and self._table_has_data("fact_reviews"):
            logger.info("Step 13: fact_reviews already loaded, skipping")
            return True
        logger.info("Step 13: Populating fact_reviews...")

        query = """
        INSERT INTO fact_reviews (
            order_id, customer_key, product_key, seller_key, review_date_key,
            review_score, review_comment_length, is_positive_review, has_comment,
            review_creation_time, review_answer_timestamp, days_to_answer
        )
        SELECT
            r.order_id,
            dc.customer_key,
            dp.product_key,
            ds.seller_key,
            CONVERT(INT, FORMAT(r.review_creation_date, 'yyyyMMdd'))             AS review_date_key,
            r.review_score,
            ISNULL(LEN(r.review_comment_message), 0)                             AS review_comment_length,
            CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END                      AS is_positive_review,
            CASE WHEN LEN(r.review_comment_message) > 0 THEN 1 ELSE 0 END        AS has_comment,
            r.review_creation_date,
            r.review_answer_timestamp,
            CASE WHEN r.review_answer_timestamp IS NOT NULL
                 THEN DATEDIFF(DAY, r.review_creation_date, r.review_answer_timestamp)
                 ELSE NULL END                                                    AS days_to_answer
        FROM olist.dbo.olist_order_reviews r
        JOIN olist.dbo.olist_orders o    ON r.order_id = o.order_id
        JOIN dim_customer          dc   ON o.customer_id = dc.customer_id
        JOIN dim_date              dd   ON CONVERT(INT, FORMAT(r.review_creation_date, 'yyyyMMdd')) = dd.date_key
        JOIN (
            SELECT order_id, product_id, seller_id,
                   ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_item_id) AS rn
            FROM olist.dbo.olist_order_items
        ) first_item ON r.order_id = first_item.order_id AND first_item.rn = 1
        JOIN dim_product dp ON first_item.product_id = dp.product_id
        JOIN dim_seller  ds ON first_item.seller_id  = ds.seller_id;
        """

        return self.dwh_conn.execute_query(query)

    def populate_fact_payments(self, skip_if_loaded: bool = True) -> bool:
        """Step 14: Populate fact_payments — grain: one row per payment installment."""
        if skip_if_loaded and self._table_has_data("fact_payments"):
            logger.info("Step 14: fact_payments already loaded, skipping")
            return True
        logger.info("Step 14: Populating fact_payments...")

        query = """
        INSERT INTO fact_payments (
            order_id, customer_key, payment_date_key, payment_type_key,
            payment_sequential, payment_installments, payment_value,
            payment_fee, net_payment_value
        )
        SELECT
            p.order_id,
            dc.customer_key,
            CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) AS payment_date_key,
            dpt.payment_type_key,
            p.payment_sequential,
            p.payment_installments,
            p.payment_value,
            NULL             AS payment_fee,
            p.payment_value  AS net_payment_value
        FROM olist.dbo.olist_order_payments p
        JOIN olist.dbo.olist_orders o  ON p.order_id      = o.order_id
        JOIN dim_customer           dc ON o.customer_id   = dc.customer_id
        JOIN dim_payment_type      dpt ON p.payment_type  = dpt.payment_type_code
        JOIN dim_date               dd ON CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) = dd.date_key;
        """

        return self.dwh_conn.execute_query(query) if self.dwh_conn else False

    def validate_etl(self) -> bool:
        """Step 15: Validate ETL results — row counts for all DWH tables."""
        logger.info("Step 15: Validating ETL results...")

        query = """
        SELECT 'dim_date',          COUNT(*) FROM dim_date
        UNION ALL
        SELECT 'dim_customer',      COUNT(*) FROM dim_customer
        UNION ALL
        SELECT 'dim_seller',        COUNT(*) FROM dim_seller
        UNION ALL
        SELECT 'dim_product',       COUNT(*) FROM dim_product
        UNION ALL
        SELECT 'dim_payment_type',  COUNT(*) FROM dim_payment_type
        UNION ALL
        SELECT 'dim_order_status',  COUNT(*) FROM dim_order_status
        UNION ALL
        SELECT 'dim_geolocation',   COUNT(*) FROM dim_geolocation
        UNION ALL
        SELECT 'fact_order_items',  COUNT(*) FROM fact_order_items
        UNION ALL
        SELECT 'fact_orders',       COUNT(*) FROM fact_orders
        UNION ALL
        SELECT 'fact_reviews',      COUNT(*) FROM fact_reviews
        UNION ALL
        SELECT 'fact_payments',     COUNT(*) FROM fact_payments;
        """

        results = self.dwh_conn.fetch_query(query)
        if results:
            for table_name, row_count in results:
                logger.info(f"  {table_name}: {row_count} rows")

        return True

    def run_etl(self, force_recreate: bool = False) -> bool:
        """Execute complete ETL pipeline."""
        try:
            # Phase 0: Setup
            if not self.setup_dwh_database(force_recreate):
                return False

            # Phase 1: Connect
            if not self.connect_to_staging():
                logger.error("Failed to connect to staging database")
                return False

            if not self.connect_to_dwh():
                logger.error("Failed to connect to DWH database")
                return False

            # Phase 2: Schema
            if not self.create_dwh_schema(skip_if_exists=not force_recreate):
                logger.error("Failed to create DWH schema")
                return False

            # Phase 3: Dimensions
            skip = not force_recreate
            self.populate_dim_date(skip_if_loaded=skip)
            self.populate_dim_customer(skip_if_loaded=skip)
            self.populate_dim_seller(skip_if_loaded=skip)
            self.populate_dim_product(skip_if_loaded=skip)
            self.populate_dim_payment_type(skip_if_loaded=skip)
            self.populate_dim_order_status(skip_if_loaded=skip)
            self.populate_dim_geolocation(skip_if_loaded=skip)

            # Phase 4: Facts
            self.populate_fact_order_items(skip_if_loaded=skip)
            self.populate_fact_orders(skip_if_loaded=skip)
            self.populate_fact_reviews(skip_if_loaded=skip)
            self.populate_fact_payments(skip_if_loaded=skip)

            # Phase 5: Validation
            self.validate_etl()

            return True
        finally:
            if self.staging_conn:
                self.staging_conn.close_connection()
            if self.dwh_conn:
                self.dwh_conn.close_connection()


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Data Warehouse Layer: ETL from Staging to Star Schema"
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
        "--force",
        action="store_true",
        help="Force recreate DWH database",
    )
    parser.add_argument(
        "--etl",
        action="store_true",
        help="Run full ETL pipeline (default: schema only)",
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("DATA WAREHOUSE LAYER: ETL Pipeline")
    logger.info("=" * 70)

    pipeline = DWHETLPipeline(
        staging_server=args.server,
        staging_user=args.username,
        staging_pass=args.password,
    )

    if args.etl:
        logger.info("Running full ETL pipeline...")
        success = pipeline.run_etl(force_recreate=args.force)
    else:
        logger.info("Creating DWH schema only...")
        success = pipeline.setup_dwh_database(force_recreate=args.force)
        if success:
            if pipeline.connect_to_dwh():
                success = pipeline.create_dwh_schema()
                pipeline.dwh_conn.close_connection()

    if success:
        logger.info("=" * 70)
        logger.info("✓ DWH layer completed successfully!")
        logger.info("=" * 70)
        sys.exit(0)
    else:
        logger.error("=" * 70)
        logger.error("✗ DWH layer failed!")
        logger.error("=" * 70)
        sys.exit(1)


if __name__ == "__main__":
    main()
