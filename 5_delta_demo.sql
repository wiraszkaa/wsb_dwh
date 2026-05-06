USE olist_dwh;
GO

/*
Podstawowa obsługa delty danych — przykład demonstracyjny.
Założenie: w bazie staging olist pojawiły się nowe rekordy. Ten skrypt dopisuje tylko te zamówienia i pozycje,
których nie ma jeszcze w tabelach faktów DWH.

Uwaga: do prezentacji wystarczy pokazać ideę: NOT EXISTS / MERGE, czyli brak ponownego ładowania całej tabeli.
Pełna produkcyjna delta obejmowałaby także aktualizacje wymiarów SCD oraz tabelę logowania uruchomień ETL.
*/

/* Delta dla fact_orders — dopisz tylko nowe order_id. */
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
    CONVERT(INT, FORMAT(o.order_purchase_timestamp, 'yyyyMMdd')) AS order_date_key,
    CASE WHEN o.order_estimated_delivery_date IS NOT NULL
         THEN CONVERT(INT, FORMAT(o.order_estimated_delivery_date, 'yyyyMMdd')) ELSE NULL END,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL
         THEN CONVERT(INT, FORMAT(o.order_delivered_customer_date, 'yyyyMMdd')) ELSE NULL END,
    dos.order_status_key,
    ISNULL(items.total_items, 0),
    ISNULL(items.total_price, 0),
    ISNULL(items.total_freight, 0),
    ISNULL(items.total_price, 0) + ISNULL(items.total_freight, 0),
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL
         THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) ELSE NULL END,
    CASE WHEN o.order_delivered_customer_date IS NOT NULL
              AND o.order_estimated_delivery_date IS NOT NULL
              AND o.order_delivered_customer_date > o.order_estimated_delivery_date
         THEN 1 ELSE 0 END
FROM olist.dbo.olist_orders o
JOIN dim_customer dc ON o.customer_id = dc.customer_id
JOIN dim_order_status dos ON o.order_status = dos.status_code
LEFT JOIN (
    SELECT order_id, COUNT(*) AS total_items, SUM(price) AS total_price, SUM(freight_value) AS total_freight
    FROM olist.dbo.olist_order_items
    GROUP BY order_id
) items ON o.order_id = items.order_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_orders fo
    WHERE fo.order_id = o.order_id
);
GO

/* Delta dla fact_order_items — dopisz tylko nowe pary order_id + order_item_id. */
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
    1 AS quantity,
    oi.price,
    oi.freight_value,
    oi.price AS total_item_value,
    0 AS is_out_of_stock
FROM olist.dbo.olist_order_items oi
JOIN olist.dbo.olist_orders o ON oi.order_id = o.order_id
JOIN dim_customer dc ON o.customer_id = dc.customer_id
JOIN dim_product dp ON oi.product_id = dp.product_id
JOIN dim_seller ds ON oi.seller_id = ds.seller_id
WHERE NOT EXISTS (
    SELECT 1
    FROM fact_order_items foi
    WHERE foi.order_id = oi.order_id
      AND foi.order_item_id = oi.order_item_id
);
GO

/* Po delcie odświeżamy tabelę agregującą. */
DELETE FROM agg_monthly_sales;

INSERT INTO agg_monthly_sales (
    year, month, category_name_en, seller_key,
    total_orders, total_items, total_revenue, total_freight,
    avg_order_value, avg_review_score, orders_delayed
)
SELECT
    dd.year,
    dd.month_of_year,
    ISNULL(dp.category_name_en, 'Unknown'),
    foi.seller_key,
    COUNT(DISTINCT foi.order_id),
    SUM(foi.quantity),
    SUM(ISNULL(foi.total_item_value, 0) + ISNULL(foi.freight_value, 0)),
    SUM(ISNULL(foi.freight_value, 0)),
    CAST(SUM(ISNULL(foi.total_item_value, 0) + ISNULL(foi.freight_value, 0)) / NULLIF(COUNT(DISTINCT foi.order_id), 0) AS DECIMAL(10, 2)),
    CAST(AVG(CAST(rv.avg_review_score AS DECIMAL(5,2))) AS DECIMAL(3,2)),
    COUNT(DISTINCT CASE WHEN fo.is_delayed = 1 THEN fo.order_id END)
FROM fact_order_items foi
JOIN fact_orders fo ON foi.order_id = fo.order_id
JOIN dim_date dd ON foi.order_date_key = dd.date_key
JOIN dim_product dp ON foi.product_key = dp.product_key
LEFT JOIN (
    SELECT order_id, AVG(CAST(review_score AS DECIMAL(5,2))) AS avg_review_score
    FROM fact_reviews
    GROUP BY order_id
) rv ON foi.order_id = rv.order_id
GROUP BY dd.year, dd.month_of_year, ISNULL(dp.category_name_en, 'Unknown'), foi.seller_key;
GO
