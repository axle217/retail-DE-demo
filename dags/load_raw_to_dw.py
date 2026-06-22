from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

SRC = "retail_oltp"
TGT = "retail_dw"


@dag(
    dag_id="load_raw_to_dw",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def load_raw_to_dw():

    # -------------------------
    # DIM DATE (from OLTP orders)
    # -------------------------
    @task
    def load_dim_date():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT DISTINCT order_date
            FROM orders
        """)

        rows = src_cur.fetchall()

        dim_rows = []
        for (order_date,) in rows:
            dim_rows.append((
                int(order_date.strftime("%Y%m%d")),
                order_date.date(),
                order_date.strftime("%A"),
                order_date.day,
                order_date.month,
                order_date.strftime("%B"),
                (order_date.month - 1) // 3 + 1,
                order_date.year,
                order_date.weekday() >= 5
            ))

        tgt_cur.executemany("""
            INSERT INTO dim_date (
                date_key, full_date, day_of_week,
                day_number, month, month_name,
                quarter, year, is_weekend
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_key) DO NOTHING;
        """, dim_rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # DIM CUSTOMER
    # -------------------------
    @task
    def load_dim_customer():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT customer_id, first_name, last_name, email,
                   city, state, country, signup_date
            FROM customers
        """)

        # Transform rows to match target schema and generate surrogate keys (customer_key = customer_id in this case)
        rows = [
            (
                customer_id,
                str(customer_id),
                first_name,
                last_name,
                email,
                city,
                state,
                country,
                signup_date
            )
            for customer_id, first_name, last_name, email, city, state, country, signup_date in src_cur.fetchall()
        ]

        tgt_cur.executemany("""
            INSERT INTO dim_customer (
                customer_key, customer_id,
                first_name, last_name, email,
                city, state, country, signup_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_key) DO NOTHING;
        """, rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # DIM PRODUCT
    # -------------------------
    @task
    def load_dim_product():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT product_id, product_name, category, unit_price
            FROM products
        """)

        rows = src_cur.fetchall()

        tgt_cur.executemany("""
            INSERT INTO dim_product (
                product_id, product_name,
                category, subcategory, brand, unit_price
            )
            VALUES (%s,%s,%s,NULL,NULL,%s)
            ON CONFLICT (product_id) DO NOTHING;
        """, rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # DIM STORE
    # -------------------------
    @task
    def load_dim_store():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        # Select only what you need. Unused columns caused issues.
        src_cur.execute("""
            SELECT store_id, store_name, store_type, country
            FROM stores
        """)

        # Transform rows to match target schema and generate surrogate keys (store_key = store_id in this case)
        rows = [
            (
                store_id,
                str(store_id),
                store_name,
                store_type,
                country
            )
            for store_id, store_name, store_type, country in src_cur.fetchall()
        ]

        tgt_cur.executemany("""
            INSERT INTO dim_store (
                store_key, store_id, store_name, store_type, country
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (store_key) DO NOTHING;
        """, rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # DIM PAYMENT
    # -------------------------
    @task
    def load_dim_payment():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT payment_id, payment_type, provider
            FROM payments
        """)

        rows = src_cur.fetchall()

        tgt_cur.executemany("""
            INSERT INTO dim_payment (
                payment_key, payment_type, provider
            )
            VALUES (%s,%s,%s)
            ON CONFLICT (payment_key) DO NOTHING;
        """, rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # FACT SALES (JOIN SOURCE → TARGET)
    # -------------------------
    @task
    def load_fact_sales():
        src = PostgresHook(postgres_conn_id=SRC)
        tgt = PostgresHook(postgres_conn_id=TGT)

        src_conn = src.get_conn()
        src_cur = src_conn.cursor()

        tgt_conn = tgt.get_conn()
        tgt_cur = tgt_conn.cursor()

        src_cur.execute("""
            SELECT order_id, order_date,
                   customer_id, product_id,
                   store_id, payment_id,
                   quantity, unit_price,
                   discount_amount, tax_amount,
                   total_amount
            FROM orders
        """)

        rows = src_cur.fetchall()

        fact_rows = []

        for r in rows:
            order_id, order_date, cid, pid, sid, payid, qty, price, disc, tax, total = r

            date_key = int(order_date.strftime("%Y%m%d"))

            fact_rows.append((
                order_id,
                date_key,
                cid,
                pid,
                sid,
                payid,
                str(order_id),
                qty,
                price,
                disc,
                tax,
                total
            ))

        tgt_cur.executemany("""
            INSERT INTO fact_sales (
                sales_key,
                date_key,
                customer_key,
                product_key,
                store_key,
                payment_key,
                order_id,
                quantity,
                unit_price,
                discount_amount,
                tax_amount,
                total_amount
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (sales_key) DO NOTHING;
        """, fact_rows)

        tgt_conn.commit()

        src_cur.close()
        src_conn.close()
        tgt_cur.close()
        tgt_conn.close()

        return True

    # -------------------------
    # FLOW
    # -------------------------
    date = load_dim_date()

    customer = load_dim_customer()
    product = load_dim_product()
    store = load_dim_store()
    payment = load_dim_payment()

    fact = load_fact_sales()

    date >> [customer, product, store, payment] >> fact


dag = load_raw_to_dw()