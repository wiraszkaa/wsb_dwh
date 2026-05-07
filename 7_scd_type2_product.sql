USE olist_dwh;
GO

/*
SCD Type 2 dla wymiaru produktu.
Istniejące tabele dim_customer/dim_seller/dim_product działają jak SCD Type 1/current state.
Ten skrypt dodaje osobny wymiar dim_product_scd2, który przechowuje historię zmian produktu.
*/

IF OBJECT_ID('dbo.dim_product_scd2', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.dim_product_scd2 (
        product_scd_key INT IDENTITY(1,1) PRIMARY KEY,
        product_id VARCHAR(32) NOT NULL,
        category_name_pt VARCHAR(200) NULL,
        category_name_en VARCHAR(200) NULL,
        product_weight_g DECIMAL(10,2) NULL,
        product_length_cm DECIMAL(10,2) NULL,
        product_height_cm DECIMAL(10,2) NULL,
        product_width_cm DECIMAL(10,2) NULL,
        size_class VARCHAR(50) NULL,
        valid_from DATETIME2 NOT NULL,
        valid_to DATETIME2 NULL,
        is_current BIT NOT NULL,
        scd_hash VARBINARY(32) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX idx_dim_product_scd2_product_id
        ON dbo.dim_product_scd2(product_id);

    CREATE INDEX idx_dim_product_scd2_current
        ON dbo.dim_product_scd2(product_id, is_current);
END;
GO

/* Initial load: wstawiamy aktualny stan produktów, ale tylko jeśli tabela jest pusta. */
IF NOT EXISTS (SELECT 1 FROM dbo.dim_product_scd2)
BEGIN
    INSERT INTO dbo.dim_product_scd2 (
        product_id, category_name_pt, category_name_en,
        product_weight_g, product_length_cm, product_height_cm, product_width_cm,
        size_class, valid_from, valid_to, is_current, scd_hash
    )
    SELECT
        dp.product_id,
        dp.category_name_pt,
        dp.category_name_en,
        dp.product_weight_g,
        dp.product_length_cm,
        dp.product_height_cm,
        dp.product_width_cm,
        dp.size_class,
        CAST('1900-01-01' AS DATETIME2) AS valid_from,
        NULL AS valid_to,
        1 AS is_current,
        HASHBYTES('SHA2_256', CONCAT(
            ISNULL(dp.category_name_pt, ''), '|',
            ISNULL(dp.category_name_en, ''), '|',
            ISNULL(CAST(dp.product_weight_g AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(dp.product_length_cm AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(dp.product_height_cm AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(dp.product_width_cm AS VARCHAR(50)), ''), '|',
            ISNULL(dp.size_class, '')
        )) AS scd_hash
    FROM dbo.dim_product dp
    WHERE dp.product_id IS NOT NULL;
END;
GO

/* Demo zmiany SCD2: symulujemy zmianę kategorii dla jednego produktu, bez modyfikowania danych staging. */
DECLARE @demo_product_id VARCHAR(32);
DECLARE @change_time DATETIME2 = SYSUTCDATETIME();

SELECT TOP 1 @demo_product_id = product_id
FROM dbo.dim_product_scd2
WHERE is_current = 1
  AND ISNULL(category_name_pt, '') NOT LIKE '%_SCD_DEMO'
ORDER BY product_id;

IF @demo_product_id IS NOT NULL
BEGIN
    IF OBJECT_ID('tempdb..#changed_product') IS NOT NULL DROP TABLE #changed_product;

    SELECT
        product_id,
        CONCAT(ISNULL(category_name_pt, 'unknown'), '_SCD_DEMO') AS category_name_pt,
        CONCAT(ISNULL(category_name_en, 'unknown'), ' SCD Demo') AS category_name_en,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        size_class,
        HASHBYTES('SHA2_256', CONCAT(
            CONCAT(ISNULL(category_name_pt, 'unknown'), '_SCD_DEMO'), '|',
            CONCAT(ISNULL(category_name_en, 'unknown'), ' SCD Demo'), '|',
            ISNULL(CAST(product_weight_g AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(product_length_cm AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(product_height_cm AS VARCHAR(50)), ''), '|',
            ISNULL(CAST(product_width_cm AS VARCHAR(50)), ''), '|',
            ISNULL(size_class, '')
        )) AS new_hash
    INTO #changed_product
    FROM dbo.dim_product_scd2
    WHERE product_id = @demo_product_id
      AND is_current = 1;

    UPDATE target_product
    SET
        valid_to = DATEADD(SECOND, -1, @change_time),
        is_current = 0,
        updated_at = @change_time
    FROM dbo.dim_product_scd2 target_product
    JOIN #changed_product source_product
        ON target_product.product_id = source_product.product_id
    WHERE target_product.is_current = 1
      AND target_product.scd_hash <> source_product.new_hash;

    INSERT INTO dbo.dim_product_scd2 (
        product_id, category_name_pt, category_name_en,
        product_weight_g, product_length_cm, product_height_cm, product_width_cm,
        size_class, valid_from, valid_to, is_current, scd_hash
    )
    SELECT
        product_id,
        category_name_pt,
        category_name_en,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        size_class,
        @change_time,
        NULL,
        1,
        new_hash
    FROM #changed_product source_product
    WHERE NOT EXISTS (
        SELECT 1
        FROM dbo.dim_product_scd2 current_product
        WHERE current_product.product_id = source_product.product_id
          AND current_product.is_current = 1
          AND current_product.scd_hash = source_product.new_hash
    );
END;
GO

/* Wynik do screena: produkt ma wersję historyczną i bieżącą. */
SELECT TOP 20
    product_id,
    category_name_pt,
    category_name_en,
    valid_from,
    valid_to,
    is_current
FROM dbo.dim_product_scd2
WHERE product_id IN (
    SELECT TOP 5 product_id
    FROM dbo.dim_product_scd2
    GROUP BY product_id
    HAVING COUNT(*) > 1
    ORDER BY product_id
)
ORDER BY product_id, valid_from;
GO

SELECT
    COUNT(*) AS all_scd2_rows,
    COUNT(DISTINCT product_id) AS distinct_products,
    SUM(CASE WHEN is_current = 1 THEN 1 ELSE 0 END) AS current_rows,
    SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) AS historical_rows
FROM dbo.dim_product_scd2;
GO
