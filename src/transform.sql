-- ==================================================
-- TRANSFORM: staging -> warehouse
-- ==================================================

-- dim_customers (Source: staging.stg_customers)
TRUNCATE warehouse.dim_customers RESTART IDENTITY CASCADE;

INSERT INTO warehouse.dim_customers (
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    city,
    state
)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix AS zip_code_prefix,
    INITCAP(TRIM(customer_city)) AS city,
    UPPER(TRIM(customer_state)) AS state
FROM staging.stg_customers
WHERE customer_id IS NOT NULL;

-- dim_products (Source: staging.stg_products)
TRUNCATE warehouse.dim_products RESTART IDENTITY CASCADE;

INSERT INTO warehouse.dim_products (
    product_id,
    category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm
)
SELECT
    product_id,
    INITCAP(REPLACE(TRIM(product_category_name), '_', ' ')) AS category_name,
    NULLIF(TRIM(product_weight_g), '')::NUMERIC AS weight_g, 
    NULLIF(TRIM(product_length_cm), '')::NUMERIC AS length_cm,
    NULLIF(TRIM(product_height_cm), '')::NUMERIC AS height_cm,
    NULLIF(TRIM(product_width_cm), '')::NUMERIC AS width_cm
FROM staging.stg_products
WHERE product_id IS NOT NULL;

-- dim_time (Source: order_purchase_timestamp in staging.stg_orders)
TRUNCATE warehouse.dim_time RESTART IDENTITY CASCADE;

INSERT INTO warehouse.dim_time (
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend
)
SELECT DISTINCT
    purchase_date AS full_date,
    EXTRACT(YEAR FROM purchase_date)::SMALLINT AS year,
    EXTRACT(QUARTER FROM purchase_date)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM purchase_date)::SMALLINT AS month,
    TO_CHAR(purchase_date, 'Month') AS month_name,
    EXTRACT(WEEK FROM purchase_date)::SMALLINT AS week_of_year,
    EXTRACT(DAY FROM purchase_date)::SMALLINT AS day_of_month,
    EXTRACT(ISODOW FROM purchase_date)::SMALLINT AS day_of_week,
    TO_CHAR(purchase_date, 'Day') AS day_name,
    EXTRACT(ISODOW FROM purchase_date) IN (6, 7) AS is_weekend
FROM (
    SELECT
        order_purchase_timestamp::DATE AS purchase_date
    FROM staging.stg_orders
    WHERE order_purchase_timestamp IS NOT NULL
) dates;

-- fact_orders (Source: staging.stg_order_items + stg_orders)
INSERT INTO warehouse.fact_orders (
    order_id,
    order_item_id,
    customer_key,
    product_key,
    time_key,
    price,
    freight_value,
    total_revenue,
    order_status,
    purchase_timestamp,
    delivery_timestamp,
    estimated_delivery_date,
    delivery_days
)
SELECT
    oi.order_id,
    oi.order_item_id::INTEGER,
    c.customer_key,
    p.product_key,
    t.time_key,
    NULLIF(TRIM(oi.price), '')::NUMERIC(10,2) AS price,
    NULLIF(TRIM(oi.freight_value), '')::NUMERIC(10,2) AS freight_value,
    NULLIF(TRIM(oi.price), '')::NUMERIC(10,2) + NULLIF(TRIM(oi.freight_value), '')::NUMERIC(10,2) AS total_revenue,
    o.order_status,
    o.order_purchase_timestamp::TIMESTAMP AS purchase_timestamp,
    o.order_delivered_customer_date::TIMESTAMP AS delivery_timestamp,
    o.order_estimated_delivery_date::DATE AS estimated_delivery_date,
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL AND o.order_purchase_timestamp IS NOT NULL
        THEN EXTRACT(Day FROM(o.order_delivered_customer_date::TIMESTAMP - o.order_purchase_timestamp::TIMESTAMP))::INTEGER
    END AS delivery_days
FROM staging.stg_order_items oi
JOIN staging.stg_orders o ON oi.order_id = o.order_id
JOIN warehouse.dim_products p ON oi.product_id = p.product_id
JOIN warehouse.dim_customers c ON o.customer_id = c.customer_id
JOIN warehouse.dim_time t ON o.order_purchase_timestamp::DATE = t.full_date
WHERE oi.order_id IS NOT NULL
    AND oi.product_id IS NOT NULL;