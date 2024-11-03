-- Buat table untuk menyimpan data orders
CREATE TABLE IF NOT EXISTS public.olist_orders (
    order_id VARCHAR(255) PRIMARY KEY,
    customer_id VARCHAR(255),
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- Buat table untuk menyimpan data customers
CREATE TABLE IF NOT EXISTS public.olist_customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_unique_id VARCHAR(255),
    customer_zip_code_prefix VARCHAR(20),
    customer_city VARCHAR(100),
    customer_state VARCHAR(2)
);

-- Buat table untuk menyimpan data order items
CREATE TABLE IF NOT EXISTS public.olist_order_items (
    order_id VARCHAR(255),
    order_item_id INT,
    product_id VARCHAR(255),
    seller_id VARCHAR(255),
    shipping_limit_date TIMESTAMP,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

-- Buat table untuk hasil analisis
CREATE TABLE IF NOT EXISTS public.olist_sales_performance (
    region VARCHAR(2),
    total_orders INT,
    total_revenue DECIMAL(15,2),
    avg_order_value DECIMAL(10,2),
    total_customers INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);