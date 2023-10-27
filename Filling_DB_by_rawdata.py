import psycopg2
import csv

# Replace these with your database connection details
database = "datalake_db"
user = "malik"
password = "kurdi17"
host = "localhost"
port = "5433"
table_name = "stream_data"

# Establish a connection to the database
conn = psycopg2.connect(
    database=database, user=user, password=password, host=host, port=port
)

# Create a cursor object
cur = conn.cursor()

# Define the path to your CSV file
csv_file = "scanner_data.csv"

# Open the CSV file and read data
# Open the CSV file and read data
with open(csv_file, "r") as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row if it exists

   # Open the CSV file and read data
with open(csv_file, "r") as file:
    csv_data = csv.reader(file)
    next(csv_data)  # Skip the header row if it exists

    for row in csv_data:
        cur.execute(f"INSERT INTO {table_name} (Date, Customer_ID, Transaction_ID, SKU_Category, SKU, Quantity, Sales_Amount) VALUES (%s, %s, %s, %s, %s, %s, %s)", (row[1], int(row[2]), int(row[3]),
                                                                                                                                                                     row[4], row[5], float(row[6], float(row[7])
                                                                                                                                                                                           )))


# Commit the changes and close the cursor and connection
conn.commit()
cur.close()
conn.close()
