/**
 * E-COMMERCE STAR SCHEMA (Data Warehouse)
 * 
 * This schema implements a star schema for analytical queries on the Olist dataset.
 * 
 * Design Decisions:
 * - Fact table grain: ORDER-ITEM level (most granular for flexibility)
 * - Slowly Changing Dimensions: Type 1 (overwrites) for all dimensions
 * - Conformed Dimensions: Reusable across multiple fact tables
 * 
 * Key Metrics Available:
 * - Order volume and trends
 * - Revenue analysis (product, seller, category, geography)
 * - Payment method adoption
 * - Customer satisfaction (via reviews)
 * - Shipping performance (freight costs)
 * - Regional performance
 */

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

/**
 * dim_date: Time dimension with calendar hierarchy
 * Grain: One row per day
 * Used for: Temporal analysis, trend reporting, forecasting
 */
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD format for easy sorting
    calendar_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL, -- 1=Monday, 7=Sunday
    day_name VARCHAR(20) NOT NULL,
    week_of_year INT NOT NULL,
    month_of_year INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INT NOT NULL, -- 1-4
    year INT NOT NULL,
    is_weekend BIT NOT NULL, -- 1 if Saturday or Sunday
    is_holiday BIT NOT NULL DEFAULT 0, -- Can be populated with Brazilian holidays
    INDEX idx_calendar_date (calendar_date),
    INDEX idx_year_month (year, month_of_year)
);

/**
 * dim_customer: Customer dimension
 * Grain: One row per unique customer_id
 * Used for: Customer segmentation, RFM analysis, geographic analysis
 */
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY IDENTITY(1, 1),
    customer_id VARCHAR(32) NOT NULL UNIQUE,
    customer_zip_code_prefix VARCHAR(10),
    customer_city VARCHAR(100),
    customer_state CHAR(2),
    region_name VARCHAR(100), -- Derived from state
    customer_segment VARCHAR(50), -- Can be populated: VIP, Regular, One-time
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    INDEX idx_customer_id (customer_id),
    INDEX idx_state (customer_state),
    INDEX idx_city (customer_city)
);

/**
 * dim_seller: Seller dimension
 * Grain: One row per unique seller_id
 * Used for: Seller performance analysis, market analysis
 */
CREATE TABLE dim_seller (
    seller_key INT PRIMARY KEY IDENTITY(1, 1),
    seller_id VARCHAR(32) NOT NULL UNIQUE,
    seller_zip_code_prefix VARCHAR(10),
    seller_city VARCHAR(100),
    seller_state CHAR(2),
    region_name VARCHAR(100), -- Derived from state
    seller_segment VARCHAR(50), -- Can be populated: Premium, Standard, Emerging
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    INDEX idx_seller_id (seller_id),
    INDEX idx_seller_state (seller_state),
    INDEX idx_seller_city (seller_city)
);

/**
 * dim_product: Product dimension
 * Grain: One row per unique product_id
 * Used for: Product performance, category analysis, inventory decisions
 */
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY IDENTITY(1, 1),
    product_id VARCHAR(32) NOT NULL UNIQUE,
    category_id VARCHAR(100),
    category_name_en VARCHAR(200), -- English translation
    category_name_pt VARCHAR(200), -- Portuguese original
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10, 2),
    product_length_cm DECIMAL(10, 2),
    product_height_cm DECIMAL(10, 2),
    product_width_cm DECIMAL(10, 2),
    volume_cm3 DECIMAL(15, 2), -- Calculated: length × width × height
    is_heavy BIT NOT NULL DEFAULT 0, -- 1 if weight_g > 10000
    size_class VARCHAR(50), -- Small, Medium, Large, Extra Large
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    INDEX idx_product_id (product_id),
    INDEX idx_category_id (category_id),
    INDEX idx_category_name_en (category_name_en)
);

/**
 * dim_payment_type: Payment method dimension
 * Grain: One row per unique payment type
 * Used for: Payment method analysis, revenue by channel
 */
CREATE TABLE dim_payment_type (
    payment_type_key INT PRIMARY KEY IDENTITY(1, 1),
    payment_type_code VARCHAR(50) NOT NULL UNIQUE,
    payment_type_name VARCHAR(100) NOT NULL,
    payment_category VARCHAR(50), -- Digital, Card, Cash
    risk_level VARCHAR(20), -- Low, Medium, High
    processing_days INT, -- Average processing time
    INDEX idx_payment_type_code (payment_type_code)
);

/**
 * dim_order_status: Order status dimension
 * Grain: One row per unique order status
 * Used for: Order flow analysis, fulfillment tracking
 */
CREATE TABLE dim_order_status (
    order_status_key INT PRIMARY KEY IDENTITY(1, 1),
    status_code VARCHAR(50) NOT NULL UNIQUE,
    status_name VARCHAR(100) NOT NULL,
    status_description VARCHAR(500),
    is_final_status BIT NOT NULL, -- 1 if order journey ends here
    is_problem_status BIT NOT NULL, -- 1 if indicates issue (canceled, unavailable)
    INDEX idx_status_code (status_code)
);

/**
 * dim_geolocation: Geographic dimension
 * Grain: One row per unique zip_code_prefix
 * Used for: Regional analysis, shipping patterns, market expansion
 */
CREATE TABLE dim_geolocation (
    geolocation_key INT PRIMARY KEY IDENTITY(1, 1),
    zip_code_prefix VARCHAR(10) NOT NULL UNIQUE,
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    city VARCHAR(100),
    state CHAR(2),
    region_name VARCHAR(100), -- South, Southeast, Northeast, North, Center-West
    distance_from_sao_paulo_km INT, -- Reference point
    INDEX idx_zip_code (zip_code_prefix),
    INDEX idx_city (city),
    INDEX idx_state (state)
);

-- =====================================================
-- FACT TABLES
-- =====================================================

/**
 * fact_order_items: Order-item fact table
 * Grain: One row per item in an order
 * This granular level allows maximum flexibility for analysis
 * Complemented by fact_orders for order-level metrics
 */
CREATE TABLE fact_order_items (
    order_item_key INT PRIMARY KEY IDENTITY(1,1),

-- Dimension keys
order_id VARCHAR(32) NOT NULL, -- Denormalized for traceability
order_item_id INT NOT NULL,
customer_key INT NOT NULL,
product_key INT NOT NULL,
seller_key INT NOT NULL,
order_date_key INT NOT NULL,

-- Facts: Quantities and prices
quantity INT NOT NULL,
price DECIMAL(10, 2) NOT NULL,
freight_value DECIMAL(10, 2),
total_item_value DECIMAL(10, 2), -- price × quantity

-- Flags and dimensions
is_out_of_stock BIT NOT NULL DEFAULT 0,

-- Metadata
created_at DATETIME2 DEFAULT GETDATE(),

-- Constraints and indexes
CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_fact_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_fact_seller FOREIGN KEY (seller_key) REFERENCES dim_seller(seller_key),
    CONSTRAINT fk_fact_date FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    
    INDEX idx_order_id (order_id),
    INDEX idx_customer_key (customer_key),
    INDEX idx_product_key (product_key),
    INDEX idx_seller_key (seller_key),
    INDEX idx_order_date_key (order_date_key),
    INDEX idx_total_value (total_item_value)
);

/**
 * fact_orders: Order-level fact table
 * Grain: One row per order
 * Used for: Order volume, order value, fulfillment tracking
 */
CREATE TABLE fact_orders (
    order_key INT PRIMARY KEY IDENTITY(1,1),

-- Dimension keys
order_id VARCHAR(32) NOT NULL UNIQUE,
customer_key INT NOT NULL,
order_date_key INT NOT NULL,
estimated_delivery_date_key INT,
actual_delivery_date_key INT,

-- Facts: Aggregated metrics
order_status_key INT NOT NULL,
total_items INT NOT NULL,
total_price DECIMAL(10, 2) NOT NULL,
total_freight_value DECIMAL(10, 2),
total_order_value DECIMAL(10, 2), -- price + freight

-- Order details
order_purchase_timestamp DATETIME2,
order_approved_at DATETIME2,
order_delivered_carrier_date DATETIME2,
order_delivered_customer_date DATETIME2,
order_estimated_delivery_date DATETIME2,
days_to_delivery INT, -- Actual delivery days
is_delayed BIT NOT NULL DEFAULT 0, -- 1 if delivered after estimated

-- Metadata
created_at DATETIME2 DEFAULT GETDATE(),

-- Constraints and indexes
CONSTRAINT fk_fact_order_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_order_status FOREIGN KEY (order_status_key) REFERENCES dim_order_status(order_status_key),
    CONSTRAINT fk_fact_order_est_date FOREIGN KEY (estimated_delivery_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_order_act_date FOREIGN KEY (actual_delivery_date_key) REFERENCES dim_date(date_key),
    
    INDEX idx_order_id (order_id),
    INDEX idx_customer_key (customer_key),
    INDEX idx_order_date_key (order_date_key),
    INDEX idx_order_status_key (order_status_key),
    INDEX idx_order_value (total_order_value),
    INDEX idx_is_delayed (is_delayed)
);

/**
 * fact_reviews: Customer review fact table
 * Grain: One row per review
 * Used for: Customer satisfaction analysis, product/seller ratings
 */
CREATE TABLE fact_reviews (
    review_key INT PRIMARY KEY IDENTITY(1,1),

-- Dimension keys
order_id VARCHAR(32) NOT NULL,
customer_key INT NOT NULL,
product_key INT NOT NULL,
seller_key INT NOT NULL,
review_date_key INT NOT NULL,

-- Facts: Review metrics
review_score INT NOT NULL, -- 1-5 stars
review_comment_length INT,
is_positive_review BIT NOT NULL, -- 1 if score >= 4
has_comment BIT NOT NULL, -- 1 if review_comment_length > 0

-- Metadata
review_creation_time DATETIME2,
review_answer_timestamp DATETIME2,
days_to_answer INT, -- Days before seller answered
created_at DATETIME2 DEFAULT GETDATE(),

-- Constraints and indexes
CONSTRAINT fk_review_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_review_product FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    CONSTRAINT fk_review_seller FOREIGN KEY (seller_key) REFERENCES dim_seller(seller_key),
    CONSTRAINT fk_review_date FOREIGN KEY (review_date_key) REFERENCES dim_date(date_key),
    
    INDEX idx_order_id (order_id),
    INDEX idx_review_score (review_score),
    INDEX idx_is_positive (is_positive_review),
    INDEX idx_seller_key (seller_key),
    INDEX idx_review_date_key (review_date_key)
);

/**
 * fact_payments: Payment-level fact table
 * Grain: One row per payment installment
 * Used for: Payment method analysis, revenue recognition, installment tracking
 */
CREATE TABLE fact_payments (
    payment_key INT PRIMARY KEY IDENTITY(1,1),

-- Dimension keys
order_id VARCHAR(32) NOT NULL,
customer_key INT NOT NULL,
payment_date_key INT NOT NULL,
payment_type_key INT NOT NULL,

-- Facts: Payment details
payment_sequential INT NOT NULL, -- Installment number (1-based)
payment_installments INT NOT NULL, -- Total installments for this payment
payment_value DECIMAL(10, 2) NOT NULL,
payment_fee DECIMAL(10, 2), -- Processing fee if applicable
net_payment_value DECIMAL(10, 2), -- payment_value - fee

-- Metadata
payment_created_at DATETIME2,
created_at DATETIME2 DEFAULT GETDATE(),

-- Constraints and indexes
CONSTRAINT fk_payment_customer FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_payment_date FOREIGN KEY (payment_date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_payment_type FOREIGN KEY (payment_type_key) REFERENCES dim_payment_type(payment_type_key),
    
    INDEX idx_order_id (order_id),
    INDEX idx_customer_key (customer_key),
    INDEX idx_payment_date_key (payment_date_key),
    INDEX idx_payment_type_key (payment_type_key),
    INDEX idx_payment_value (payment_value)
);

-- =====================================================
-- AGGREGATE TABLES (Optional - for performance)
-- =====================================================

/**
 * agg_monthly_sales: Pre-aggregated monthly sales data
 * Used for: Fast dashboard queries on monthly trends
 * Refresh: Nightly ETL
 */
CREATE TABLE agg_monthly_sales (
    year INT NOT NULL,
    month INT NOT NULL,
    category_name_en VARCHAR(200) NOT NULL,
    seller_key INT NOT NULL,
    total_orders INT,
    total_items INT,
    total_revenue DECIMAL(15, 2),
    total_freight DECIMAL(15, 2),
    avg_order_value DECIMAL(10, 2),
    avg_review_score DECIMAL(3, 2),
    orders_delayed INT,
    PRIMARY KEY (
        year,
        month,
        category_name_en,
        seller_key
    ),
    INDEX idx_category (category_name_en)
);

GO