-- ==================================================
-- SCHEMA SETUP
-- ==================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;

-- ==================================================
-- STAGING LAYER (raw data)
-- ==================================================

-- stg_orders (Source: olist_orders_dataset.csv)
DROP TABLE IF EXISTS staging.stg_orders;
CREATE TABLE staging.stg_orders (
    order_id TEXT,
    customer_id TEXT,
    order_status TEXT,
    order_purchase_timestamp TEXT,
    order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT
);

-- stg_order_items (Source: olist_order_items_dataset.csv)
DROP TABLE IF EXISTS staging.stg_order_items;
CREATE TABLE staging.stg_order_items (
    order_id TEXT,
    order_item_id TEXT,
    product_id TEXT,
    seller_id TEXT,
    shipping_limit_date TEXT,
    price TEXT,
    freight_value TEXT
);

-- stg_customers (Source: olist_customers_dataset.csv)
DROP TABLE IF EXISTS staging.stg_customers;
CREATE TABLE staging.stg_customers (
    customer_id TEXT,
    customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT,
    customer_state TEXT
);

-- stg_products (Source: olist_products_dataset.csv)
DROP TABLE IF EXISTS staging.stg_products;
CREATE TABLE staging.stg_products (
    product_id TEXT,
    product_category_name TEXT,
    product_name_lenght TEXT,
    product_description_lenght TEXT,
    product_photos_qty TEXT,
    product_weight_g TEXT,
    product_length_cm TEXT,
    product_height_cm TEXT,
    product_width_cm TEXT
);

-- ==================================================
-- WAREHOUSE LAYER (clean data, star schema)
-- ==================================================

-- dim_customers
DROP TABLE IF EXISTS warehouse.dim_customers CASCADE;
CREATE TABLE warehouse.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    customer_unique_id TEXT,
    zip_code_prefix TEXT,
    city TEXT,
    state VARCHAR(2),
    created_at TIMESTAMP DEFAULT NOW()
);

-- dim_products
DROP TABLE IF EXISTS warehouse.dim_products CASCADE;
CREATE TABLE warehouse.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    category_name TEXT,
    category_name_en TEXT,
    weight_g NUMERIC,
    length_cm NUMERIC,
    height_cm NUMERIC,
    width_cm NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);

-- dim_time (Generated from order_purchase_timestamp values in fact load)
DROP TABLE IF EXISTS warehouse.dim_time CASCADE;
CREATE TABLE warehouse.dim_time (
    time_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- fact_orders (Grain: one row per order_item)
DROP TABLE IF EXISTS warehouse.fact_orders;
CREATE TABLE warehouse.fact_orders (
    order_item_key SERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    
    customer_key INTEGER REFERENCES warehouse.dim_customers(customer_key),
    product_key INTEGER REFERENCES warehouse.dim_products(product_key),
    time_key INTEGER REFERENCES warehouse.dim_time(time_key),
    
    price NUMERIC(10, 2),
    freight_value NUMERIC(10, 2),
    total_revenue NUMERIC(10, 2),
    order_status TEXT,
    purchase_timestamp TIMESTAMP,
    delivery_timestamp TIMESTAMP,
    estimated_delivery_date TIMESTAMP,
    delivery_days INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (order_id, order_item_id)
);