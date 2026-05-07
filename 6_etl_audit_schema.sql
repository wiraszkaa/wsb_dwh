USE olist_dwh;
GO

/*
Zaawansowane logowanie i walidacje ETL.
Uruchom po wykonaniu main_staging.py oraz main_dwh.py --etl.
Skrypt tworzy tabele audytowe i zapisuje wynik ostatniego sprawdzenia hurtowni.
*/

IF OBJECT_ID('dbo.etl_run_log', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_run_log (
        etl_run_id INT IDENTITY(1,1) PRIMARY KEY,
        run_name VARCHAR(100) NOT NULL,
        started_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at DATETIME2 NULL,
        status VARCHAR(30) NOT NULL,
        fact_orders_rows INT NULL,
        fact_order_items_rows INT NULL,
        fact_payments_rows INT NULL,
        fact_reviews_rows INT NULL,
        agg_monthly_sales_rows INT NULL,
        error_message VARCHAR(1000) NULL
    );
END;
GO

IF OBJECT_ID('dbo.etl_validation_result', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.etl_validation_result (
        validation_result_id INT IDENTITY(1,1) PRIMARY KEY,
        etl_run_id INT NOT NULL,
        validation_name VARCHAR(200) NOT NULL,
        validation_status VARCHAR(20) NOT NULL,
        expected_value VARCHAR(200) NULL,
        actual_value VARCHAR(200) NULL,
        details VARCHAR(1000) NULL,
        checked_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT fk_validation_etl_run
            FOREIGN KEY (etl_run_id) REFERENCES dbo.etl_run_log(etl_run_id)
    );
END;
GO

DECLARE @run_id INT;
DECLARE @fact_orders_rows INT = (SELECT COUNT(*) FROM dbo.fact_orders);
DECLARE @fact_order_items_rows INT = (SELECT COUNT(*) FROM dbo.fact_order_items);
DECLARE @fact_payments_rows INT = (SELECT COUNT(*) FROM dbo.fact_payments);
DECLARE @fact_reviews_rows INT = (SELECT COUNT(*) FROM dbo.fact_reviews);
DECLARE @agg_rows INT = (SELECT COUNT(*) FROM dbo.agg_monthly_sales);

INSERT INTO dbo.etl_run_log (
    run_name, status, fact_orders_rows, fact_order_items_rows,
    fact_payments_rows, fact_reviews_rows, agg_monthly_sales_rows
)
VALUES (
    'manual_validation_after_etl', 'STARTED', @fact_orders_rows, @fact_order_items_rows,
    @fact_payments_rows, @fact_reviews_rows, @agg_rows
);

SET @run_id = SCOPE_IDENTITY();

INSERT INTO dbo.etl_validation_result (
    etl_run_id, validation_name, validation_status, expected_value, actual_value, details
)
SELECT
    @run_id,
    'fact_orders is not empty',
    CASE WHEN @fact_orders_rows > 0 THEN 'OK' ELSE 'ERROR' END,
    '> 0',
    CAST(@fact_orders_rows AS VARCHAR(50)),
    'Tabela fact_orders powinna zawierać załadowane zamówienia.'
UNION ALL
SELECT
    @run_id,
    'fact_order_items is not empty',
    CASE WHEN @fact_order_items_rows > 0 THEN 'OK' ELSE 'ERROR' END,
    '> 0',
    CAST(@fact_order_items_rows AS VARCHAR(50)),
    'Tabela fact_order_items powinna zawierać pozycje zamówień.'
UNION ALL
SELECT
    @run_id,
    'agg_monthly_sales is not empty',
    CASE WHEN @agg_rows > 0 THEN 'OK' ELSE 'ERROR' END,
    '> 0',
    CAST(@agg_rows AS VARCHAR(50)),
    'Tabela agregująca powinna być zasilona po ETL.'
UNION ALL
SELECT
    @run_id,
    'missing customer keys in fact_orders',
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ERROR' END,
    '0',
    CAST(COUNT(*) AS VARCHAR(50)),
    'Sprawdzenie, czy fact_orders ma poprawne klucze klientów.'
FROM dbo.fact_orders fo
LEFT JOIN dbo.dim_customer dc ON fo.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL
UNION ALL
SELECT
    @run_id,
    'missing product keys in fact_order_items',
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ERROR' END,
    '0',
    CAST(COUNT(*) AS VARCHAR(50)),
    'Sprawdzenie, czy fact_order_items ma poprawne klucze produktów.'
FROM dbo.fact_order_items foi
LEFT JOIN dbo.dim_product dp ON foi.product_key = dp.product_key
WHERE dp.product_key IS NULL
UNION ALL
SELECT
    @run_id,
    'duplicate product business keys',
    CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'ERROR' END,
    '0',
    CAST(COUNT(*) AS VARCHAR(50)),
    'Sprawdzenie duplikatów product_id w dim_product.'
FROM (
    SELECT product_id
    FROM dbo.dim_product
    GROUP BY product_id
    HAVING COUNT(*) > 1
) duplicates;

UPDATE dbo.etl_run_log
SET
    finished_at = SYSUTCDATETIME(),
    status = CASE
        WHEN EXISTS (
            SELECT 1 FROM dbo.etl_validation_result
            WHERE etl_run_id = @run_id AND validation_status = 'ERROR'
        ) THEN 'FINISHED_WITH_ERRORS'
        ELSE 'SUCCESS'
    END
WHERE etl_run_id = @run_id;

SELECT TOP 10 *
FROM dbo.etl_run_log
ORDER BY etl_run_id DESC;

SELECT *
FROM dbo.etl_validation_result
WHERE etl_run_id = @run_id
ORDER BY validation_result_id;
GO
