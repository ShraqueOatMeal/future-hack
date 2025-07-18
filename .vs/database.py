import sqlite3
import pandas as pd
from datetime import datetime

DB_NAME = "company.db"

def create_tables():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS products(
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price FLOAT NOT NULL,
            stock INTEGER NOT NULL,
            supplier TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers(
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT,
            region TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
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
            performance_rating FLOAT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS inventory(
            inventory_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            last_updated DATE NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS materials(
            material_id INTEGER PRIMARY KEY,
            material_name TEXT NOT NULL,
            material_type TEXT NOT NULL,
            mass FLOAT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bill_of_materials(
    bom_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    material_id INTEGER,
    subassembly_id INTEGER,
    quantity FLOAT NOT NULL,
    unit TEXT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (material_id) REFERENCES materials(material_id),
    FOREIGN KEY (subassembly_id) REFERENCES products(product_id),
    CHECK ((material_id IS NOT NULL AND subassembly_id IS NULL) OR (material_id IS NULL AND subassembly_id IS NOT NULL))
);
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_tags(
            product_id INTEGER,
            tag TEXT,
            PRIMARY KEY (product_id, tag),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS material_tags(
            material_id INTEGER,
            tag TEXT,
            PRIMARY KEY (material_id, tag),
            FOREIGN KEY (material_id) REFERENCES materials(material_id)
        );
        """)

        cursor.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(name, content='products', content_rowid='product_id');
        """)

        conn.commit()
        print("Tables created successfully.")
    except sqlite3.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        conn.close()

def load_csv_data():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Load products
        try:
            products_df = pd.read_csv("data/products_full_tesla.csv")
            products_df = products_df.drop(columns=['tags'], errors='ignore')
            products_df['created_at'] = products_df.get('created_at', current_time)
            products_df['updated_at'] = products_df.get('updated_at', current_time)
            products_df.to_sql("products", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Error: 'products_full_tesla.csv' not found in data/ directory.")
            return

        # Load product_tags
        product_tags_data = []
        try:
            for _, row in pd.read_csv("data/products_full_tesla.csv").iterrows():
                product_id = row['product_id']
                tags = row['tags'].split(',') if pd.notna(row['tags']) else []
                for tag in tags:
                    product_tags_data.append((product_id, tag.strip()))
            cursor.executemany("INSERT OR REPLACE INTO product_tags (product_id, tag) VALUES (?, ?)", product_tags_data)
        except FileNotFoundError:
            print("Error: 'products_full_tesla.csv' not found for product_tags.")
            return
        except KeyError:
            print("Warning: 'tags' column missing in products_full_tesla.csv. Skipping product_tags.")

        # Populate products_fts
        try:
            cursor.execute("INSERT INTO products_fts(rowid, name) SELECT product_id, name FROM products;")
        except sqlite3.Error as e:
            print(f"Error populating products_fts: {e}")
            return

        # Load materials
        try:
            materials_df = pd.read_csv("data/materials_tesla.csv")
            materials_df = materials_df.drop(columns=['tags', 'product_id'], errors='ignore')
            materials_df['created_at'] = materials_df.get('created_at', current_time)
            materials_df['updated_at'] = materials_df.get('updated_at', current_time)
            materials_df.to_sql("materials", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Error: 'materials_tesla.csv' not found in data/ directory.")
            return

        # Load material_tags
        material_tags_data = []
        try:
            for _, row in pd.read_csv("data/materials_tesla.csv").iterrows():
                material_id = row['material_id']
                tags = row['tags'].split(',') if pd.notna(row['tags']) else []
                for tag in tags:
                    material_tags_data.append((material_id, tag.strip()))
            cursor.executemany("INSERT OR REPLACE INTO material_tags (material_id, tag) VALUES (?, ?)", material_tags_data)
        except FileNotFoundError:
            print("Error: 'materials_tesla.csv' not found for material_tags.")
        except KeyError:
            print("Warning: 'tags' column missing in materials_tesla.csv. Skipping material_tags.")

        # Load bill_of_materials from materials_tesla.csv
        try:
            materials_full_df = pd.read_csv("data/materials_tesla.csv")
            product_ids = set(pd.read_sql_query("SELECT product_id FROM products", conn)['product_id'])
            material_ids = set(pd.read_sql_query("SELECT material_id FROM materials", conn)['material_id'])
            
            bom_data = []
            for _, row in materials_full_df.iterrows():
                if row['product_id'] not in product_ids:
                    print(f"Warning: Skipping product_id {row['product_id']} in materials_tesla.csv (not in products)")
                    continue
                if row['material_id'] not in material_ids:
                    print(f"Warning: Skipping material_id {row['material_id']} in materials_tesla.csv (not in materials)")
                    continue
                bom_data.append((
                    row['product_id'],
                    row['material_id'],
                    None,  # subassembly_id
                    row['mass'],
                    'kg'
                ))
            if bom_data:
                cursor.executemany("""
                    INSERT INTO bill_of_materials (product_id, material_id, subassembly_id, quantity, unit)
                    VALUES (?, ?, ?, ?, ?)
                """, bom_data)
        except FileNotFoundError:
            print("Error: 'materials_tesla.csv' not found for bill_of_materials.")
        except sqlite3.Error as e:
            print(f"Error inserting into bill_of_materials from materials_tesla.csv: {e}")

        # Load bom_tesla.csv
        try:
            bom_df = pd.read_csv("data/bom_tesla.csv")
            required_columns = {'bom_id', 'product_id', 'material_id', 'subassembly_id', 'quantity', 'unit'}
            if not required_columns.issubset(bom_df.columns):
                missing = required_columns - set(bom_df.columns)
                print(f"Warning: Missing columns in bom_tesla.csv: {missing}. Skipping BOM loading.")
            else:
                product_ids = set(pd.read_sql_query("SELECT product_id FROM products", conn)['product_id'])
                material_ids = set(pd.read_sql_query("SELECT material_id FROM materials", conn)['material_id'])
                
                bom_insert_data = []
                invalid_bom_rows = []
                for _, row in bom_df.iterrows():
                    if row['product_id'] not in product_ids:
                        invalid_bom_rows.append(row)
                        continue
                    if pd.notna(row['material_id']) and row['material_id'] not in material_ids:
                        invalid_bom_rows.append(row)
                        continue
                    if pd.notna(row['subassembly_id']) and row['subassembly_id'] not in product_ids:
                        invalid_bom_rows.append(row)
                        continue
                    bom_insert_data.append((
                        row['bom_id'],
                        row['product_id'],
                        row['material_id'] if pd.notna(row['material_id']) else None,
                        row['subassembly_id'] if pd.notna(row['subassembly_id']) else None,
                        row['quantity'],
                        row['unit']
                    ))
                if invalid_bom_rows:
                    pd.DataFrame(invalid_bom_rows).to_csv("data/invalid_bom_rows.csv", index=False)
                    print(f"Invalid BOM rows saved to 'data/invalid_bom_rows.csv'")
                if bom_insert_data:
                    cursor.executemany("""
                        INSERT OR REPLACE INTO bill_of_materials (bom_id, product_id, material_id, subassembly_id, quantity, unit)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, bom_insert_data)
        except FileNotFoundError:
            print("Warning: 'bom_tesla.csv' not found in data/ directory. Skipping.")
        except sqlite3.Error as e:
            print(f"Error inserting into bill_of_materials from bom_tesla.csv: {e}")

        # Load customers, sales, workers, inventory
        try:
            customers_df = pd.read_csv("data/customers.csv")
            customers_df['created_at'] = customers_df.get('created_at', current_time)
            customers_df['updated_at'] = customers_df.get('updated_at', current_time)
            customers_df.to_sql("customers", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Warning: 'customers.csv' not found in data/ directory. Skipping.")

        try:
            sales_df = pd.read_csv("data/sales_full_tesla.csv")
            sales_df['created_at'] = sales_df.get('created_at', current_time)
            sales_df['updated_at'] = sales_df.get('updated_at', current_time)
            sales_df.to_sql("sales", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Warning: 'sales_full_tesla.csv' not found in data/ directory. Skipping.")

        try:
            workers_df = pd.read_csv("data/workers entries.csv")
            workers_df['created_at'] = workers_df.get('created_at', current_time)
            workers_df['updated_at'] = workers_df.get('updated_at', current_time)
            workers_df.to_sql("workers", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Warning: 'workers entries.csv' not found in data/ directory. Skipping.")

        try:
            inventory_df = pd.read_csv("data/inventory_full_tesla.csv")
            inventory_df['created_at'] = inventory_df.get('created_at', current_time)
            inventory_df['updated_at'] = inventory_df.get('updated_at', current_time)
            inventory_df.to_sql("inventory", conn, if_exists="replace", index=False)
        except FileNotFoundError:
            print("Warning: 'inventory_full_tesla.csv' not found in data/ directory. Skipping.")

        conn.commit()
        print("Data loaded into the database successfully.")
    except sqlite3.Error as e:
        print(f"Error loading data: {e}")
    finally:
        conn.close()


def fetch_table(table_name):
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()
        return df
    except sqlite3.Error as e:
        print(f"Error fetching table {table_name}: {e}")
        return None

def fetch_custom_query(query):
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except sqlite3.Error as e:
        print(f"Error executing query: {e}")
        return None

def get_latest_sales_summary():
    try:
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
    except sqlite3.Error as e:
        print(f"Error fetching sales summary: {e}")
        return None

def get_inventory_status():
    try:
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
    except sqlite3.Error as e:
        print(f"Error fetching inventory status: {e}")
        return None

if __name__ == "__main__":
    create_tables()
    load_csv_data()
    print("Database setup complete. You can now use the database for your queries.")