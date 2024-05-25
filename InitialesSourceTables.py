import sqlite3
import datetime
import os
import random
# Define database name and table names
DATABASE_NAME_BASE = "sales_data"
DATABASE_EXTENSION = ".db"
PRODUCT_TABLE = "products"
CUSTOMER_TABLE = "customers"
SALES_PST_TABLE = "sales_pst"
SALES_IST_TABLE = "sales_ist"

# Define product data
products = [
    ("Product 1", "Description 1"),
    ("Product 2", "Description 2")
]

# Define customer data
customers = [
    ("Customer 1", "Address 1"),
    ("Customer 2", "Address 2")
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

# Function to create database tables
def create_tables(conn):
  cursor = conn.cursor()
  print('I was here')
  # Create product table
  cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {PRODUCT_TABLE} (
      product_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      description TEXT
    );
  """)

  # Create customer table
  cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {CUSTOMER_TABLE} (
      customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      address TEXT
    );
  """)

  # Create sales table (PST timezone)
  cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {SALES_PST_TABLE} (
      transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      customer_id INTEGER NOT NULL,
      sale_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (product_id) REFERENCES {PRODUCT_TABLE}(product_id),
      FOREIGN KEY (customer_id) REFERENCES {CUSTOMER_TABLE}(customer_id)
    );
  """)

  # Create sales table (IST timezone)
  cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {SALES_IST_TABLE} (
      transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
      product_id INTEGER NOT NULL,
      customer_id INTEGER NOT NULL,
      sale_datetime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (product_id) REFERENCES {PRODUCT_TABLE}(product_id),
      FOREIGN KEY (customer_id) REFERENCES {CUSTOMER_TABLE}(customer_id)
    );
  """)

  conn.commit()

# Function to insert data into tables
def insert_data(conn):
  cursor = conn.cursor()

  # Insert product data
  for name, description in products:
    cursor.execute(f"""
      INSERT INTO {PRODUCT_TABLE} (name, description) VALUES (?, ?)
    """, (name, description))

  # Insert customer data
  for name, address in customers:
    cursor.execute(f"""
      INSERT INTO {CUSTOMER_TABLE} (name, address) VALUES (?, ?)
    """, (name, address))

  # Insert sales data (PST timezone)
  for _ in range(20):
    product_id = random.randint(1, len(products))
    customer_id = random.randint(1, len(customers))
    sale_datetime = generate_pst_datetime()
    cursor.execute(f"""
      INSERT INTO {SALES_PST_TABLE} (product_id, customer_id, sale_datetime) VALUES (?, ?, ?)
    """, (product_id, customer_id, sale_datetime))

    # Insert sales data (IST timezone)
  for _ in range(20):
    product_id = random.randint(1, len(products))
    customer_id = random.randint(1, len(customers))
    sale_datetime = generate_ist_datetime()
    cursor.execute(f"""
      INSERT INTO {SALES_IST_TABLE} (product_id, customer_id, sale_datetime) VALUES (?, ?, ?)
    """, (product_id, customer_id, sale_datetime))

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