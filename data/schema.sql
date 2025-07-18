CREATE TABLE products(
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price FLOAT NOT NULL,
    stock INTEGER NOT NULL,
    supplier TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE customers(
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT,
    region TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sales(
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

CREATE TABLE workers(
    worker_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL,
    salary FLOAT NOT NULL,
    performance_rating FLOAT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory(
    inventory_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    last_updated DATE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE materials(
    material_id INTEGER PRIMARY KEY,
    material_name TEXT NOT NULL,
    material_type TEXT NOT NULL,
    mass FLOAT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

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

CREATE TABLE product_tags(
    product_id INTEGER,
    tag TEXT,
    PRIMARY KEY (product_id, tag),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE material_tags(
    material_id INTEGER,
    tag TEXT,
    PRIMARY KEY (material_id, tag),
    FOREIGN KEY (material_id) REFERENCES materials(material_id)
);

CREATE VIRTUAL TABLE products_fts USING fts5(name, content='products', content_rowid='product_id');

CREATE INDEX idx_sales_product_id ON sales(product_id);
CREATE INDEX idx_sales_customer_id ON sales(customer_id);
CREATE INDEX idx_sales_date ON sales(date);
CREATE INDEX idx_inventory_product_id ON inventory(product_id);
CREATE INDEX idx_customers_region ON customers(region);
CREATE INDEX idx_workers_department ON workers(department);
CREATE INDEX idx_bom_product_id ON bill_of_materials(product_id);
CREATE INDEX idx_bom_material_id ON bill_of_materials(material_id);
CREATE INDEX idx_bom_subassembly_id ON bill_of_materials(subassembly_id);