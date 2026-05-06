USE olist_dwh;
GO

/*
Raport 1: Miesięczna sprzedaż według kategorii produktu.
Cel biznesowy: identyfikacja kategorii generujących największy przychód w czasie.
Wykorzystuje tabelę agregującą agg_monthly_sales.
*/
SELECT TOP 50
    year,
    month,
    category_name_en,
    SUM(total_orders) AS total_orders,
    SUM(total_items) AS total_items,
    SUM(total_revenue) AS total_revenue,
    CAST(SUM(total_revenue) / NULLIF(SUM(total_orders), 0) AS DECIMAL(10, 2)) AS avg_order_value,
    SUM(orders_delayed) AS delayed_orders
FROM agg_monthly_sales
GROUP BY year, month, category_name_en
ORDER BY year, month, total_revenue DESC;
GO

/*
Raport 2: Opóźnienia dostaw według regionu klienta.
Cel biznesowy: wskazanie regionów, w których logistyka działa najgorzej.
*/
SELECT
    dc.region_name,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN fo.is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_orders,
    CAST(100.0 * SUM(CASE WHEN fo.is_delayed = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5, 2)) AS delayed_percent,
    CAST(AVG(CAST(fo.days_to_delivery AS DECIMAL(10, 2))) AS DECIMAL(10, 2)) AS avg_days_to_delivery,
    SUM(fo.total_order_value) AS total_revenue
FROM fact_orders fo
JOIN dim_customer dc
    ON fo.customer_key = dc.customer_key
GROUP BY dc.region_name
ORDER BY delayed_percent DESC, total_orders DESC;
GO

/*
Raport 3: Ranking sprzedawców według przychodu, liczby zamówień i średniej oceny.
Cel biznesowy: identyfikacja najlepszych sprzedawców oraz sprzedawców z wysoką sprzedażą, ale słabszą oceną.
Wykorzystuje CTE oraz funkcję okna RANK().
*/
WITH seller_sales AS (
    SELECT
        ds.seller_id,
        ds.seller_city,
        ds.seller_state,
        ds.region_name,
        COUNT(DISTINCT foi.order_id) AS total_orders,
        SUM(ISNULL(foi.total_item_value, 0) + ISNULL(foi.freight_value, 0)) AS total_revenue,
        CAST(AVG(CAST(fr.review_score AS DECIMAL(5, 2))) AS DECIMAL(3, 2)) AS avg_review_score
    FROM fact_order_items foi
    JOIN dim_seller ds
        ON foi.seller_key = ds.seller_key
    LEFT JOIN fact_reviews fr
        ON foi.order_id = fr.order_id
       AND foi.seller_key = fr.seller_key
    GROUP BY
        ds.seller_id,
        ds.seller_city,
        ds.seller_state,
        ds.region_name
)
SELECT TOP 20
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    seller_id,
    seller_city,
    seller_state,
    region_name,
    total_orders,
    total_revenue,
    avg_review_score
FROM seller_sales
ORDER BY revenue_rank;
GO

/*
Raport 4: Analiza metod płatności.
Cel biznesowy: sprawdzenie, które metody płatności odpowiadają za największą wartość transakcji i jak często używane są raty.
*/
SELECT
    dpt.payment_type_name,
    dpt.payment_category,
    COUNT(*) AS payment_records,
    COUNT(DISTINCT fp.order_id) AS paid_orders,
    SUM(fp.payment_value) AS total_payment_value,
    CAST(AVG(CAST(fp.payment_value AS DECIMAL(10, 2))) AS DECIMAL(10, 2)) AS avg_payment_value,
    CAST(AVG(CAST(fp.payment_installments AS DECIMAL(10, 2))) AS DECIMAL(10, 2)) AS avg_installments
FROM fact_payments fp
JOIN dim_payment_type dpt
    ON fp.payment_type_key = dpt.payment_type_key
GROUP BY dpt.payment_type_name, dpt.payment_category
ORDER BY total_payment_value DESC;
GO

/*
Raport 5: Kategorie produktów — przychód, fracht, ocena i udział ciężkich produktów.
Cel biznesowy: porównanie kategorii pod względem sprzedaży, kosztów dostawy i satysfakcji klientów.
*/
SELECT TOP 30
    dp.category_name_en,
    COUNT(DISTINCT foi.order_id) AS total_orders,
    SUM(foi.total_item_value) AS product_revenue,
    SUM(foi.freight_value) AS freight_revenue,
    CAST(AVG(CAST(fr.review_score AS DECIMAL(5, 2))) AS DECIMAL(3, 2)) AS avg_review_score,
    CAST(100.0 * SUM(CASE WHEN dp.is_heavy = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5, 2)) AS heavy_products_percent
FROM fact_order_items foi
JOIN dim_product dp
    ON foi.product_key = dp.product_key
LEFT JOIN fact_reviews fr
    ON foi.order_id = fr.order_id
   AND foi.product_key = fr.product_key
GROUP BY dp.category_name_en
HAVING COUNT(DISTINCT foi.order_id) >= 20
ORDER BY product_revenue DESC;
GO
