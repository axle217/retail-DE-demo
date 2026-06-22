from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from faker import Faker
from decimal import Decimal

fake = Faker()

CONN_ID = "retail_oltp"


@dag(
    dag_id="generate_oltp_data",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
)
def generate_oltp_data():

    # -------------------------
    # OPTIONAL: schema validation only
    # -------------------------
    @task
    def validate_schema():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)

        tables = {row[0] for row in cur.fetchall()}

        required = {"customers", "products", "stores", "payments", "orders"}

        missing = required - tables
        if missing:
            raise ValueError(f"Missing tables: {missing}")

        cur.close()
        conn.close()

        return True

    # -------------------------
    # CUSTOMERS
    # -------------------------
    @task
    def generate_customers(n=100):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        rows = []
        for i in range(n):
            first = fake.first_name()
            last = fake.last_name()

            email = f"{first.lower()}.{last.lower()}.{i}@example.com"

            rows.append((
                first,
                last,
                email,
                fake.city(),
                fake.state(),
                fake.country(),
                fake.date_between("-3y", "today")
            ))

        cur.executemany("""
            INSERT INTO customers (
                first_name, last_name, email, city, state, country, signup_date
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (email) DO NOTHING;
        """, rows)

        conn.commit()
        cur.close()
        conn.close()

        return True

    # -------------------------
    # PRODUCTS
    # -------------------------
    @task
    def generate_products(n=50):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        rows = [
            (
                f"Product-{i}",
                fake.random_element(["electronics", "clothing", "food", "home"]),
                round(fake.random_number(digits=3), 2)
            )
            for i in range(n)
        ]

        cur.executemany("""
            INSERT INTO products (
                product_name, category, unit_price
            )
            VALUES (%s,%s,%s)
        """, rows)

        conn.commit()
        cur.close()
        conn.close()

        return True

    # -------------------------
    # STORES
    # -------------------------
    @task
    def generate_stores(n=10):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        rows = [
            ("Store-0", "online", None, None, None)
            ] + [
            (f"Store-{i}", "physical", fake.city(), fake.state(), fake.country() )
            for i in range(1, n)
        ]

        cur.executemany("""
            INSERT INTO stores (
                store_name, store_type, city, state, country
            )
            VALUES (%s,%s,%s,%s,%s)
        """, rows)

        conn.commit()
        cur.close()
        conn.close()

        return True

    # -------------------------
    # PAYMENTS
    # -------------------------
    @task
    def generate_payments():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        rows = [
            ("credit_card", "visa"),
            ("credit_card", "mastercard"),
            ("paypal", "paypal"),
            ("bank_transfer", "swift")
        ]

        cur.executemany("""
            INSERT INTO payments (payment_type, provider)
            VALUES (%s,%s)
        """, rows)

        conn.commit()
        cur.close()
        conn.close()

        return True

    # -------------------------
    # ORDERS (FK SAFE)
    # -------------------------
    @task
    def generate_orders(n=200):
        hook = PostgresHook(postgres_conn_id=CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT customer_id FROM customers")
        customers = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT product_id, unit_price FROM products")
        products = cur.fetchall()

        cur.execute("SELECT store_id FROM stores")
        stores = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT payment_id FROM payments")
        payments = [r[0] for r in cur.fetchall()]

        if not all([customers, products, stores, payments]):
            raise ValueError("One or more dimension tables are empty")

        rows = []

        for _ in range(n):
            cid = fake.random_element(customers)
            pid, price = fake.random_element(products)
            sid = fake.random_element(stores)
            payid = fake.random_element(payments)

            qty = fake.random_int(1, 5)

            unit_price = Decimal(str(price))

            tax = unit_price * Decimal("0.10")
            discount = Decimal(round(fake.random_number(digits=1) / 10, 2))

            total = qty * price + tax - discount

            rows.append((
                cid, pid, sid, payid,
                fake.date_time_this_year(),
                qty, unit_price, discount, tax, total
            ))

        cur.executemany("""
            INSERT INTO orders (
                customer_id, product_id, store_id, payment_id,
                order_date, quantity, unit_price,
                discount_amount, tax_amount, total_amount
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, rows)

        conn.commit()
        cur.close()
        conn.close()

        return True

    # -------------------------
    # FLOW
    # -------------------------
    validate = validate_schema()

    cust = generate_customers()
    prod = generate_products()
    store = generate_stores()
    pay = generate_payments()

    orders = generate_orders()

    validate >> [cust, prod, store, pay] >> orders


dag = generate_oltp_data()