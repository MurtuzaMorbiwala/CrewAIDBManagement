import sqlite3
import datetime
import os
import random

# Define database name and table names
DATABASE_NAME_BASE = "sources"
DATABASE_EXTENSION = ".db"
MASTER_PRODUCT_TABLE = "master_products"
MASTER_CUSTOMER_TABLE = "master_customers"
SALES_PST_TABLE = "sales_pst"
SALES_IST_TABLE = "sales_ist"
PRODUCT_PST_TABLE = "product_pst"
PRODUCT_IST_TABLE = "product_ist"
CUSTOMER_PST_TABLE = "customer_pst"
CUSTOMER_IST_TABLE = "customer_ist"
PRODUCT_XREF_TABLE = "product_xref"
CUSTOMER_XREF_TABLE = "customer_xref"

# Define Master Product data
master_product_data = [
    (1, "Camera XV1", "Camera description 1"),
    (2, "Camera VV2", "Camera description 2"),
    (3, "Camera GG1", "Camera description 3"),
]


# Define product data PST
products_pst_data= [
    (4,"Camera XV1", "Camera description 1"),
    (5,"Camera VV2", "Camera description 2"),
    (6,"Camera GG1", "Camera description 3")
]

# Define product data PST
products_ist_data = [
    (7,"Camera XV1", "Camera description 1"),
    (8,"Camera VV2", "Camera description 2"),
    (9,"Camera GG1", "Camera description 3")
]



# Insert data into Master CProduct table
master_customer_data = [
    (10,"Bestbuy", "Bestbuy address"),
    (11,"Walmart", "Walmart address"),
    (12,"Amazon", "Amazon address")
]

# Define customer pst data
customers_pst_data = [
    (13,"Bestbuy", "Bestbuy address"),
    (14,"Walmart", "Walmart address"),
    (15,"Amazon", "Amazon address")
]


# Define customer ist data
customers_ist_data = [
    (16,"Bestbuy", "Bestbuy address"),
    (17,"Walmart", "Walmart address"),
    (18,"Amazon", "Amazon address")
]

# Insert data into Product Xref table
product_xref_data = [
    (1, "PST", 4),
    (2, "PST", 5),
    (3, "PST", 6),
    (1, "IST", 7),
    (2, "IST", 8),
    (3, "IST", 9),
]

# Insert data into Customer Xref table
customer_xref_data = [
    (10, "PST", 13),
    (11, "PST", 14),
    (12, "PST", 15),
    (10, "IST", 16),
    (11, "IST", 17),
    (12, "IST", 18),
]


# Function to generate a sample PST timezone datetime
def generate_pst_datetime():
    now = datetime.datetime.now()
    pst_timezone = datetime.timezone(datetime.timedelta(hours=-8))
    return now.astimezone(pst_timezone)

# Function to generate a sample IST timezone datetime
def generate_ist_datetime():
    now = datetime.datetime.now()
    ist_timezone = datetime.timezone(datetime.timedelta(hours=5.5))
    return now.astimezone(ist_timezone)

sales_data_pst = [
    (13, 4, generate_pst_datetime(),100),  # Customer 13 buys product 4 at PST time
    (14, 5, generate_pst_datetime(),200),  # Customer 14 buys product 5 at PST time
]

sales_data_ist = [
    (16, 7, generate_ist_datetime(),300),  # Customer 16 buys product 7 at IST time
    (17, 8, generate_ist_datetime(),400),  # Customer 17 buys product 8 at IST time
]





# Function to create database tables
def create_tables(conn):
    cursor = conn.cursor()

    # Create master product table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MASTER_PRODUCT_TABLE} (
            master_product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            description TEXT
        );
    """)

    # Create master customer table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {MASTER_CUSTOMER_TABLE} (
            master_customer_id INTEGER PRIMARY KEY,
            customer_name TEXT NOT NULL,
            address TEXT
        );
    """)

    # Create product cross-reference table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_XREF_TABLE} (
            master_product_id INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            FOREIGN KEY (master_product_id) REFERENCES {MASTER_PRODUCT_TABLE}(master_product_id),
            FOREIGN KEY (source_id) REFERENCES {PRODUCT_PST_TABLE}(id),
            FOREIGN KEY (source_id) REFERENCES {PRODUCT_IST_TABLE}(id)
        );
    """)

    # Create customer cross-reference table
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CUSTOMER_XREF_TABLE} (
            master_customer_id INTEGER NOT NULL,
            source_name TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            FOREIGN KEY (master_customer_id) REFERENCES {MASTER_CUSTOMER_TABLE}(master_customer_id),
            FOREIGN KEY (source_id) REFERENCES {CUSTOMER_PST_TABLE}(id),
            FOREIGN KEY (source_id) REFERENCES {CUSTOMER_IST_TABLE}(id)
        );
    """)

    # Create source-specific product tables
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_PST_TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL
        );
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {PRODUCT_IST_TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL
        );
    """)

    # Create source-specific customer tables
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CUSTOMER_PST_TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL
        );
    """)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {CUSTOMER_IST_TABLE} (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT NOT NULL
        );
    """)

    # Create sales tables (PST timezone)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SALES_PST_TABLE} (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            sale_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sale_amount integer not null,
            FOREIGN KEY (product_id) REFERENCES {PRODUCT_PST_TABLE}(id),
            FOREIGN KEY (customer_id) REFERENCES {CUSTOMER_PST_TABLE}(id)
        );
    """)

    # Create sales tables (IST timezone)
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SALES_IST_TABLE} (
            transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            sale_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            sale_amount integer not null,
            FOREIGN KEY (product_id) REFERENCES {PRODUCT_IST_TABLE}(id),
            FOREIGN KEY (customer_id) REFERENCES {CUSTOMER_IST_TABLE}(id)
        );
    """)

    conn.commit()

def insert_data(conn):
    cursor = conn.cursor()


    cursor.executemany(f"""
    INSERT INTO {MASTER_PRODUCT_TABLE} (master_product_id, product_name, description)
    VALUES (?, ?, ?)
    """, master_product_data)

    cursor.executemany(f"""
    INSERT INTO {MASTER_CUSTOMER_TABLE} (master_customer_id, customer_name, address)
    VALUES (?, ?, ?)
    """, master_customer_data)

    cursor.executemany(f"""
    INSERT INTO {PRODUCT_XREF_TABLE} (master_product_id, source_name, source_id)
    VALUES (?, ?, ?)
    """, product_xref_data)

    cursor.executemany(f"""
    INSERT INTO {CUSTOMER_XREF_TABLE} (master_customer_id, source_name, source_id)
    VALUES (?, ?, ?)
    """, customer_xref_data)

    cursor.executemany(f"""
    INSERT INTO {PRODUCT_PST_TABLE} (id, name, description)
    VALUES (?, ?, ?)
    """, products_pst_data)

    cursor.executemany(f"""
    INSERT INTO {PRODUCT_IST_TABLE} (id, name, description)
    VALUES (?, ?, ?)
    """, products_ist_data)
    
    cursor.executemany(f"""
    INSERT INTO {CUSTOMER_PST_TABLE} (id, name, address)
    VALUES (?, ?, ?)
    """, customers_pst_data)

    cursor.executemany(f"""
    INSERT INTO {CUSTOMER_IST_TABLE} (id, name, address)
    VALUES (?, ?, ?)
    """, customers_ist_data)
    
    cursor.executemany(f"""
    INSERT INTO {SALES_PST_TABLE} (customer_id,product_id, sale_datetime, sale_amount)
    VALUES (?, ?, ?, ?)
    """, sales_data_pst)

    cursor.executemany(f"""
    INSERT INTO {SALES_IST_TABLE} (customer_id,product_id, sale_datetime, sale_amount)
    VALUES (?, ?, ?, ?)
    """, sales_data_ist)

    conn.commit()





def create_database():
  """
  Creates a new database with the specified name, 
  checking for existing files and dropping them if necessary.
  """
  

  # Check for existing database and drop it (optional)
  if os.path.exists(DATABASE_NAME_BASE + DATABASE_EXTENSION):
      print(f"Database '{DATABASE_NAME_BASE + DATABASE_EXTENSION}' already exists. Dropping it.")
      print('Dropped DB')
      os.remove(DATABASE_NAME_BASE + DATABASE_EXTENSION)

  # Create a connection to the new database
  conn = sqlite3.connect(DATABASE_NAME_BASE + DATABASE_EXTENSION)
  return conn

conn = create_database()
print('Created DB')
create_tables(conn)
insert_data(conn)
print('Inserted Data')