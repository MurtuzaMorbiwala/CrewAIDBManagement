import sqlite3

# Define database name
DATABASE_NAME = "sales_data.db"

# Connect to the database
conn = sqlite3.connect(DATABASE_NAME)

# Get a cursor object
cursor = conn.cursor()

# Get a list of all tables
tables = [table[0] for table in cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")]

# Loop through each table and print its rows
for table in tables:
    print(f"\nTable: {table}")
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()

    # Print column names (optional)
    # column_names = [col_desc[0] for col_desc in cursor.description]
    # print(column_names)  # Uncomment to print column names

    for row in rows:
        print(row)

# Close the connection
conn.close()