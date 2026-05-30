-- ==================================================
-- TRANSFORM: staging -> warehouse
-- ==================================================

-- dim_customers SCD2 (Source: staging.stg_customers)

-- Step 1: Update changed record
UPDATE warehouse.dim_customers AS d
SET
    is_current = FALSE,
    effective_to = CURRENT_DATE - 1,
    batch_id = current_setting('app.batch_id')::INT
FROM staging.stg_customers AS s
WHERE d.customer_id = s.customer_id
    AND d.is_current = TRUE
    AND (
        d.city IS DISTINCT FROM INITCAP(TRIM(s.customer_city))
        OR d.state IS DISTINCT FROM UPPER(TRIM(s.customer_state))
        OR d.zip_code_prefix IS DISTINCT FROM s.customer_zip_code_prefix
    );

-- Step 2: Insert new record
INSERT INTO warehouse.dim_customers (
    customer_id,
    customer_unique_id,
    zip_code_prefix,
    city,
    state,
    effective_from,
    effective_to,
    is_current,
    batch_id
)
SELECT
    s.customer_id,
    s.customer_unique_id,
    s.customer_zip_code_prefix AS zip_code_prefix,
    INITCAP(TRIM(s.customer_city)) AS city,
    UPPER(TRIM(s.customer_state)) AS state,
    CURRENT_DATE,
    NULL,
    TRUE,
    current_setting('app.batch_id')::INT
FROM staging.stg_customers s
WHERE s.customer_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_customers d
        WHERE d.customer_id = s.customer_id
            AND d.is_current = TRUE
    );

-- dim_products (Source: staging.stg_products)

-- Step 1: Update changed record
UPDATE warehouse.dim_products AS d
SET
    is_current = FALSE,
    effective_to = CURRENT_DATE - 1,
    batch_id = current_setting('app.batch_id')::INT
FROM staging.stg_products s
WHERE d.product_id = s.product_id
    AND d.is_current = TRUE
    AND (
        d.category_name IS DISTINCT FROM INITCAP(REPLACE(TRIM(s.product_category_name), '_', ' '))
        OR d.weight_g IS DISTINCT FROM NULLIF(TRIM(s.product_weight_g), '')::NUMERIC
        OR d.length_cm IS DISTINCT FROM NULLIF(TRIM(s.product_length_cm), '')::NUMERIC
        OR d.height_cm IS DISTINCT FROM NULLIF(TRIM(s.product_height_cm), '')::NUMERIC
        OR d.width_cm IS DISTINCT FROM NULLIF(TRIM(s.product_width_cm), '')::NUMERIC
    );

-- Step 2: Insert new record
INSERT INTO warehouse.dim_products (
    product_id,
    category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    effective_from,
    effective_to,
    is_current,
    batch_id
)
SELECT
    s.product_id,
    INITCAP(REPLACE(TRIM(s.product_category_name), '_', ' ')) AS category_name,
    NULLIF(TRIM(s.product_weight_g), '')::NUMERIC AS weight_g, 
    NULLIF(TRIM(s.product_length_cm), '')::NUMERIC AS length_cm,
    NULLIF(TRIM(s.product_height_cm), '')::NUMERIC AS height_cm,
    NULLIF(TRIM(s.product_width_cm), '')::NUMERIC AS width_cm,
    CURRENT_DATE,
    NULL,
    TRUE,
    current_setting('app.batch_id')::INT
FROM staging.stg_products s
WHERE s.product_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1 FROM warehouse.dim_products d
        WHERE d.product_id = s.product_id
          AND d.is_current = TRUE
    );

-- dim_time (Source: order_purchase_timestamp in staging.stg_orders)
-- Incremental: insert only dates not already in warehouse
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
    is_weekend,
    batch_id
)
SELECT DISTINCT
    purchase_date AS full_date,
    EXTRACT(YEAR FROM purchase_date)::SMALLINT,
    EXTRACT(QUARTER FROM purchase_date)::SMALLINT,
    EXTRACT(MONTH FROM purchase_date)::SMALLINT,
    TO_CHAR(purchase_date, 'Month'),
    EXTRACT(WEEK FROM purchase_date)::SMALLINT,
    EXTRACT(DAY FROM purchase_date)::SMALLINT,
    EXTRACT(ISODOW FROM purchase_date)::SMALLINT,
    TO_CHAR(purchase_date, 'Day'),
    EXTRACT(ISODOW FROM purchase_date) IN (6, 7),
    current_setting('app.batch_id')::INT
FROM (
    SELECT
        order_purchase_timestamp::DATE AS purchase_date
    FROM staging.stg_orders
    WHERE order_purchase_timestamp IS NOT NULL
) dates
WHERE purchase_date NOT IN (SELECT full_date FROM warehouse.dim_time);

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
    delivery_days,
    batch_id
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
    END AS delivery_days,
    current_setting('app.batch_id')::INT
FROM staging.stg_order_items oi
JOIN staging.stg_orders o ON oi.order_id = o.order_id
JOIN warehouse.dim_products p ON oi.product_id = p.product_id
JOIN warehouse.dim_customers c ON o.customer_id = c.customer_id
JOIN warehouse.dim_time t ON o.order_purchase_timestamp::DATE = t.full_date
WHERE oi.order_id IS NOT NULL
    AND oi.product_id IS NOT NULL
    AND (oi.order_id, oi.order_item_id::INTEGER) NOT IN (
        SELECT order_id, order_item_id FROM warehouse.fact_orders
    );