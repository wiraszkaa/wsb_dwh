USE olist_dwh;
GO

/*
Integracja z dodatkowym źródłem danych: aktualny kurs BRL/PLN z API NBP.
Najpierw uruchom fetch_nbp_brl_rate.py, a potem ten skrypt.
*/

IF OBJECT_ID('dbo.dim_exchange_rate', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_exchange_rate (
        exchange_rate_key INT IDENTITY(1,1) PRIMARY KEY,
        source_system VARCHAR(50) NOT NULL,
        currency_code CHAR(3) NOT NULL,
        base_currency CHAR(3) NOT NULL,
        rate_date DATE NOT NULL,
        rate_value DECIMAL(18,6) NOT NULL,
        fetched_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        is_current BIT NOT NULL DEFAULT 1,
        CONSTRAINT uq_exchange_rate UNIQUE (source_system, currency_code, base_currency, rate_date)
    );
END;
GO

CREATE OR ALTER VIEW dbo.vw_fact_orders_pln AS
SELECT
    fo.order_id,
    fo.customer_key,
    fo.order_date_key,
    fo.total_order_value AS total_order_value_brl,
    er.rate_value AS brl_to_pln_rate,
    CAST(fo.total_order_value * er.rate_value AS DECIMAL(18,2)) AS total_order_value_pln,
    er.rate_date AS exchange_rate_date
FROM dbo.fact_orders fo
CROSS APPLY (
    SELECT TOP 1
        rate_value,
        rate_date
    FROM dbo.dim_exchange_rate
    WHERE source_system = 'NBP'
      AND currency_code = 'BRL'
      AND base_currency = 'PLN'
    ORDER BY rate_date DESC, fetched_at DESC
) er;
GO

SELECT TOP 10
    order_id,
    total_order_value_brl,
    brl_to_pln_rate,
    total_order_value_pln,
    exchange_rate_date
FROM dbo.vw_fact_orders_pln
ORDER BY total_order_value_pln DESC;
GO
