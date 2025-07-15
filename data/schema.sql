CREATE TABLE products(
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price FLOAT NOT NULL,
    stock INTEGER NOT NULL,
    supplier TEXT NOT NULL,
    tags TEXT
);

CREATE TABLE customers(
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    phone TEXT
    region TEXT NOT NULL,
);

CREATE TABLE sales(
    sale_id INTEGER PRIMARY KEY,
    date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    total_amount FLOAT NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE workers(
    worker_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT NOT NULL,
    salary FLOAT NOT NULL,
    performance_rating FLOAT NOT NULL
);

CREATE TABLE inventory(
    inventory_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    last_updated DATE NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
)

CREATE TABLE materials(
    material_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    material_name TEXT NOT NULL,
    material_type TEXT NOT NULL,
    mass FLOAT NOT NULL,
    tags TEXT,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);