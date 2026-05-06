USE olist_dwh;
GO

/* Walidacja 1: liczba rekordów w tabelach DWH. */
SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_payment_type', COUNT(*) FROM dim_payment_type
UNION ALL SELECT 'dim_order_status', COUNT(*) FROM dim_order_status
UNION ALL SELECT 'dim_geolocation', COUNT(*) FROM dim_geolocation
UNION ALL SELECT 'fact_order_items', COUNT(*) FROM fact_order_items
UNION ALL SELECT 'fact_orders', COUNT(*) FROM fact_orders
UNION ALL SELECT 'fact_reviews', COUNT(*) FROM fact_reviews
UNION ALL SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL SELECT 'agg_monthly_sales', COUNT(*) FROM agg_monthly_sales;
GO

/* Walidacja 2: czy podstawowe tabele faktów nie są puste. */
SELECT
    CASE WHEN (SELECT COUNT(*) FROM fact_orders) > 0 THEN 'OK' ELSE 'ERROR' END AS fact_orders_status,
    CASE WHEN (SELECT COUNT(*) FROM fact_order_items) > 0 THEN 'OK' ELSE 'ERROR' END AS fact_order_items_status,
    CASE WHEN (SELECT COUNT(*) FROM fact_payments) > 0 THEN 'OK' ELSE 'ERROR' END AS fact_payments_status;
GO

/* Walidacja 3: duplikaty kluczy biznesowych w wymiarach. Wynik powinien zwrócić 0 rekordów. */
SELECT 'dim_customer' AS table_name, customer_id AS business_key, COUNT(*) AS duplicated_count
FROM dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_product', product_id, COUNT(*)
FROM dim_product
GROUP BY product_id
HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_seller', seller_id, COUNT(*)
FROM dim_seller
GROUP BY seller_id
HAVING COUNT(*) > 1;
GO

/* Walidacja 4: brakujące klucze wymiarów w fact_order_items. Wynik powinien zwrócić same zera. */
SELECT
    SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS missing_customer_key,
    SUM(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS missing_product_key,
    SUM(CASE WHEN seller_key IS NULL THEN 1 ELSE 0 END) AS missing_seller_key,
    SUM(CASE WHEN order_date_key IS NULL THEN 1 ELSE 0 END) AS missing_order_date_key
FROM fact_order_items;
GO

/* Walidacja 5: spójność przychodu między tabelą faktów i tabelą agregującą. Różnica powinna być bardzo mała albo 0. */
SELECT
    (SELECT CAST(SUM(ISNULL(total_item_value, 0) + ISNULL(freight_value, 0)) AS DECIMAL(15, 2)) FROM fact_order_items) AS revenue_from_fact_items,
    (SELECT CAST(SUM(total_revenue) AS DECIMAL(15, 2)) FROM agg_monthly_sales) AS revenue_from_aggregate,
    (SELECT CAST(SUM(ISNULL(total_item_value, 0) + ISNULL(freight_value, 0)) AS DECIMAL(15, 2)) FROM fact_order_items)
    -
    (SELECT CAST(SUM(total_revenue) AS DECIMAL(15, 2)) FROM agg_monthly_sales) AS difference;
GO
