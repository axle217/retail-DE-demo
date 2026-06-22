CREATE DATABASE retail_dw;
-- Connect to the new database
\c retail_dw

-- -- Optional: create a dedicated schema
-- CREATE SCHEMA dw;

-- -- Set the search path for this session
-- SET search_path TO dw, public;

-- Create a star schema for a retail data warehouse in PostgreSQL

--------------- Dimension tables ---------------
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,   -- e.g., 20260101
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(10),
    day_number      INT,
    month           INT,
    month_name      VARCHAR(15),
    quarter         INT,
    year            INT,
    is_weekend      BOOLEAN
);

CREATE TABLE dim_product (
    product_key     SERIAL PRIMARY KEY,
    product_id      VARCHAR(50) UNIQUE NOT NULL,
    product_name    VARCHAR(255) NOT NULL,
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    unit_price      DECIMAL(10,2)
);

CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     VARCHAR(50),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(100),
    country         VARCHAR(100),
    signup_date     DATE
);

CREATE TABLE dim_store (
    store_key       INT PRIMARY KEY,
    store_id        VARCHAR(50),
    store_name      VARCHAR(255),
    store_type      VARCHAR(50),   -- online, marketplace, etc.
    country         VARCHAR(100)
);

CREATE TABLE dim_payment (
    payment_key     INT PRIMARY KEY,
    payment_type    VARCHAR(50),  -- credit card, PayPal, etc.
    provider        VARCHAR(100)
);


--------------- Fact table ---------------
CREATE TABLE fact_sales (
    sales_key       BIGINT PRIMARY KEY,

    date_key        INT,
    customer_key    INT,
    product_key     INT,
    store_key       INT,
    payment_key     INT,

    order_id        VARCHAR(50),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount      DECIMAL(10,2),
    total_amount    DECIMAL(10,2),

    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (store_key) REFERENCES dim_store(store_key),
    FOREIGN KEY (payment_key) REFERENCES dim_payment(payment_key)
);
