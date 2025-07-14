import sqlite3
import pandas as pd

DB_NAME = "company.db"

def create_tables():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS products(
        product_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        price FLOAT NOT NULL,
        stock INTEGER NOT NULL,
        supplier TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customers(
        customer_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        phone TEXT,
        region TEXT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales(
        sale_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        product_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        total_amount FLOAT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS workers(
        worker_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        department TEXT NOT NULL,
        salary FLOAT NOT NULL,
        performance_rating FLOAT NOT NULL
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS inventory(
        inventory_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        last_updated DATE NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS materials(
        material_id INTEGER PRIMARY KEY,
        product_id INTEGER NOT NULL,
        material_name TEXT NOT NULL,
        material_type TEXT NOT NULL,
        mass FLOAT NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );
    """)

    conn.commit()
    conn.close()
    print("Tables created successfully.")

def load_csv_data():
    conn = sqlite3.connect(DB_NAME)

    pd.read_csv("data/products_full_tesla.csv").to_sql("products", conn, if_exists="replace", index=False)
    pd.read_csv("data/customers.csv").to_sql("customers", conn, if_exists="replace", index=False)
    pd.read_csv("data/sales_full_tesla.csv").to_sql("sales", conn, if_exists="replace", index=False)
    pd.read_csv("data/workers entries.csv").to_sql("workers", conn, if_exists="replace", index=False)
    pd.read_csv("data/inventory_full_tesla.csv").to_sql("inventory", conn, if_exists="replace", index=False)
    pd.read_csv("data/materials_tesla.csv").to_sql("materials", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()
    print("Data loaded into the database successfully.")

# ------------------------ #
# Functions for NLP Queries (basic ones)

def fetch_table(table_name):
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def fetch_custom_query(query):
    conn = sqlite3.connect(DB_NAME)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_latest_sales_summary():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT p.name AS product_name, SUM(s.quantity) AS total_quantity, SUM(s.total_amount) AS total_sales
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY s.product_id
    ORDER BY total_sales DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def get_inventory_status():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT p.name AS product_name, i.quantity, i.last_updated
    FROM inventory i
    JOIN products p ON i.product_id = p.product_id
    ORDER BY i.last_updated DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# ------------------------

if __name__ == "__main__":
    create_tables()
    load_csv_data()
    print("Database setup complete. You can now use the database for your queries.")
   