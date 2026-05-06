-- Active: 1778009861543@@127.0.0.1@1433@olist
-- Brazilian E-Commerce Public Dataset by Olist - Staging Tables
-- SQL Server CREATE TABLE Statements

-- Customers Table
CREATE TABLE olist_customers (
    customer_id VARCHAR(32) NOT NULL PRIMARY KEY,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(10) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL
);

-- Geolocation Table
CREATE TABLE olist_geolocation (
    geolocation_zip_code_prefix VARCHAR(10) NOT NULL,
    geolocation_lat FLOAT NOT NULL,
    geolocation_lng FLOAT NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state CHAR(2) NOT NULL,
    PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
);

-- Orders Table
CREATE TABLE olist_orders (
    order_id VARCHAR(32) NOT NULL PRIMARY KEY,
    customer_id VARCHAR(32) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp DATETIME2 NOT NULL,
    order_approved_at DATETIME2 NULL,
    order_delivered_carrier_date DATETIME2 NULL,
    order_delivered_customer_date DATETIME2 NULL,
    order_estimated_delivery_date DATETIME2 NULL,
    FOREIGN KEY (customer_id) REFERENCES olist_customers(customer_id)
);

-- Order Items Table
CREATE TABLE olist_order_items (
    order_id VARCHAR(32) NOT NULL,
    order_item_id INT NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    seller_id VARCHAR(32) NOT NULL,
    shipping_limit_date DATETIME2 NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
);

-- Order Payments Table
CREATE TABLE olist_order_payments (
    order_id VARCHAR(32) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
);

-- Order Reviews Table
CREATE TABLE olist_order_reviews (
    review_id VARCHAR(32) NOT NULL PRIMARY KEY,
    order_id VARCHAR(32) NOT NULL,
    review_score INT NOT NULL,
    review_comment_title VARCHAR(500) NULL,
    review_comment_message VARCHAR(MAX) NULL,
    review_creation_date DATETIME2 NOT NULL,
    review_answer_timestamp DATETIME2 NULL,
    FOREIGN KEY (order_id) REFERENCES olist_orders(order_id)
);

-- Products Table
CREATE TABLE olist_products (
    product_id VARCHAR(32) NOT NULL PRIMARY KEY,
    product_category_name VARCHAR(100) NULL,
    product_name_lenght INT NULL,
    product_description_lenght INT NULL,
    product_photos_qty INT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL
);

-- Sellers Table
CREATE TABLE olist_sellers (
    seller_id VARCHAR(32) NOT NULL PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(10) NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL
);

-- Product Category Translation Table
CREATE TABLE product_category_name_translation (
    product_category_name VARCHAR(100) NOT NULL PRIMARY KEY,
    product_category_name_english VARCHAR(100) NOT NULL
);

-- Create Indexes for better query performance
CREATE INDEX idx_orders_customer_id ON olist_orders(customer_id);
CREATE INDEX idx_orders_order_status ON olist_orders(order_status);
CREATE INDEX idx_order_items_product_id ON olist_order_items(product_id);
CREATE INDEX idx_order_items_seller_id ON olist_order_items(seller_id);
CREATE INDEX idx_order_reviews_order_id ON olist_order_reviews(order_id);
CREATE INDEX idx_products_category ON olist_products(product_category_name);
CREATE INDEX idx_customers_zip_code ON olist_customers(customer_zip_code_prefix);
CREATE INDEX idx_sellers_zip_code ON olist_sellers(seller_zip_code_prefix);
CREATE INDEX idx_geolocation_zip_code ON olist_geolocation(geolocation_zip_code_prefix);
