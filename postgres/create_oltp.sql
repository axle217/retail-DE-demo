CREATE DATABASE retail_oltp;
-- Connect to the new database
\c retail_oltp

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255) UNIQUE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    signup_date DATE
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    unit_price NUMERIC(10,2)
);

CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(255),
    store_type VARCHAR(50),  -- online, physical
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    payment_type VARCHAR(50),
    provider VARCHAR(50)
);

CREATE TABLE orders (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    store_id INT REFERENCES stores(store_id),
    payment_id INT REFERENCES payments(payment_id),
    order_date TIMESTAMP,
    quantity INT,
    unit_price NUMERIC(10,2),
    discount_amount NUMERIC(10,2),
    tax_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2)
);