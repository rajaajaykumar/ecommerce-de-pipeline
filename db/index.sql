CREATE INDEX IF NOT EXISTS idx_dim_customers_id
    ON warehouse.dim_customers(customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_products_id
    ON warehouse.dim_products(product_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id
    ON warehouse.fact_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_key
    ON warehouse.fact_orders(customer_key);

CREATE INDEX IF NOT EXISTS idx_fact_orders_time_key
    ON warehouse.fact_orders(time_key);