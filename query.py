import snowflake.connector
import csv
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch password from environment variable
username = os.getenv('SNOWFLAKE_USERNAME')
password = os.getenv('SNOWFLAKE_PASSWORD')
account = os.getenv("SNOWFLAKE_ACCOUNT")

# Connect to
conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account             #Account locator - Region - Cloud
)

# Querry to 
cursor = conn.cursor()
sandbox = "SNOWFLAKE_SANDBOX_DB.PUBLIC."
table_name = "EMPLOYEERS"

query = f"SELECT * FROM {sandbox}{table_name}"  # Using f-string for string interpolation

cursor.execute(query)

# Extract rows data
rows = cursor.fetchall()

# Write to csv
with open('query_output.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow([desc[0] for desc in cursor.description])  # Ghi tiêu đề cột
    writer.writerows(rows)

# Close connection
cursor.close()
conn.close()
