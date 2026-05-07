USE olist_dwh;
GO

/*
Raporty zaawansowane SQL: CTE, funkcje okna, LAG, RANK, udziały procentowe, segmentacja.
*/

/* Raport A: dynamika miesięczna sprzedaży z porównaniem miesiąc do miesiąca. */
WITH monthly_revenue AS (
    SELECT
        dd.year,
        dd.month_of_year,
        SUM(fo.total_order_value) AS monthly_revenue,
        COUNT(*) AS total_orders
    FROM dbo.fact_orders fo
    JOIN dbo.dim_date dd ON fo.order_date_key = dd.date_key
    GROUP BY dd.year, dd.month_of_year
), with_lag AS (
    SELECT
        year,
        month_of_year,
        total_orders,
        monthly_revenue,
        LAG(monthly_revenue) OVER (ORDER BY year, month_of_year) AS previous_month_revenue
    FROM monthly_revenue
)
SELECT
    year,
    month_of_year,
    total_orders,
    CAST(monthly_revenue AS DECIMAL(18,2)) AS monthly_revenue,
    CAST(previous_month_revenue AS DECIMAL(18,2)) AS previous_month_revenue,
    CAST(100.0 * (monthly_revenue - previous_month_revenue) / NULLIF(previous_month_revenue, 0) AS DECIMAL(10,2)) AS revenue_mom_percent
FROM with_lag
ORDER BY year, month_of_year;
GO

/* Raport B: udział kategorii w całkowitym przychodzie i ranking kategorii. */
WITH category_revenue AS (
    SELECT
        ISNULL(dp.category_name_en, 'Unknown') AS category_name_en,
        COUNT(DISTINCT foi.order_id) AS total_orders,
        SUM(ISNULL(foi.total_item_value,0) + ISNULL(foi.freight_value,0)) AS total_revenue
    FROM dbo.fact_order_items foi
    JOIN dbo.dim_product dp ON foi.product_key = dp.product_key
    GROUP BY ISNULL(dp.category_name_en, 'Unknown')
)
SELECT TOP 30
    RANK() OVER (ORDER BY total_revenue DESC) AS category_rank,
    category_name_en,
    total_orders,
    CAST(total_revenue AS DECIMAL(18,2)) AS total_revenue,
    CAST(100.0 * total_revenue / NULLIF(SUM(total_revenue) OVER (), 0) AS DECIMAL(10,2)) AS revenue_share_percent,
    CAST(100.0 * SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        / NULLIF(SUM(total_revenue) OVER (), 0) AS DECIMAL(10,2)) AS cumulative_revenue_share_percent
FROM category_revenue
ORDER BY category_rank;
GO

/* Raport C: segmentacja klientów na podstawie wartości zakupów i liczby zamówień. */
WITH customer_metrics AS (
    SELECT
        dc.customer_id,
        dc.region_name,
        COUNT(DISTINCT fo.order_id) AS orders_count,
        SUM(fo.total_order_value) AS total_spent,
        MAX(CONVERT(DATE, fo.order_purchase_timestamp)) AS last_order_date
    FROM dbo.fact_orders fo
    JOIN dbo.dim_customer dc ON fo.customer_key = dc.customer_key
    GROUP BY dc.customer_id, dc.region_name
), scored AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY total_spent) AS monetary_quartile,
        NTILE(4) OVER (ORDER BY orders_count) AS frequency_quartile
    FROM customer_metrics
)
SELECT
    CASE
        WHEN monetary_quartile = 4 AND frequency_quartile >= 3 THEN 'High value'
        WHEN monetary_quartile >= 3 THEN 'Medium value'
        ELSE 'Low value'
    END AS customer_segment,
    region_name,
    COUNT(*) AS customers_count,
    CAST(AVG(CAST(orders_count AS DECIMAL(10,2))) AS DECIMAL(10,2)) AS avg_orders_count,
    CAST(AVG(CAST(total_spent AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS avg_total_spent,
    CAST(SUM(total_spent) AS DECIMAL(18,2)) AS segment_revenue
FROM scored
GROUP BY
    CASE
        WHEN monetary_quartile = 4 AND frequency_quartile >= 3 THEN 'High value'
        WHEN monetary_quartile >= 3 THEN 'Medium value'
        ELSE 'Low value'
    END,
    region_name
ORDER BY segment_revenue DESC;
GO

/* Raport D: wpływ opóźnień dostaw na ocenę klienta. */
SELECT
    CASE WHEN fo.is_delayed = 1 THEN 'Delayed' ELSE 'On time / no delay' END AS delivery_status,
    COUNT(DISTINCT fo.order_id) AS orders_count,
    CAST(AVG(CAST(fo.days_to_delivery AS DECIMAL(10,2))) AS DECIMAL(10,2)) AS avg_days_to_delivery,
    CAST(AVG(CAST(fr.review_score AS DECIMAL(5,2))) AS DECIMAL(3,2)) AS avg_review_score,
    SUM(fo.total_order_value) AS total_revenue
FROM dbo.fact_orders fo
LEFT JOIN dbo.fact_reviews fr ON fo.order_id = fr.order_id
GROUP BY CASE WHEN fo.is_delayed = 1 THEN 'Delayed' ELSE 'On time / no delay' END
ORDER BY delivery_status;
GO

/* Raport E: zamówienia przeliczone na PLN po kursie pobranym z API NBP. */
IF OBJECT_ID('dbo.vw_fact_orders_pln', 'V') IS NOT NULL
BEGIN
    SELECT TOP 20
        order_id,
        total_order_value_brl,
        brl_to_pln_rate,
        total_order_value_pln,
        exchange_rate_date
    FROM dbo.vw_fact_orders_pln
    ORDER BY total_order_value_pln DESC;
END
ELSE
BEGIN
    SELECT 'Najpierw uruchom fetch_nbp_brl_rate.py oraz 8_external_nbp_rates.sql' AS message;
END;
GO
