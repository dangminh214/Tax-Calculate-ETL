import pandas as pd
import snowflake.connector
import csv
import os
from dotenv import load_dotenv

# Function to get rows from Snowflake
def get_snowflake_rows(): 
    # Load environment variables from .env file
    load_dotenv()

    # Fetch credentials from environment variables
    username = os.getenv('SNOWFLAKE_USERNAME')
    password = os.getenv('SNOWFLAKE_PASSWORD')
    account = os.getenv("SNOWFLAKE_ACCOUNT")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=username,
        password=password,
        account=account              
    )

    # Query to fetch data
    cursor = conn.cursor()
    sandbox = "SNOWFLAKE_SANDBOX_DB.PUBLIC."
    table_name = "EMPLOYEERS"
    query = f"SELECT * FROM {sandbox}{table_name}"

    cursor.execute(query)

    # Extract data and column descriptions
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    # Close the cursor and connection
    cursor.close()
    conn.close()

    return rows, column_names

# Extract data from Snowflake and write to CSV
def extract(): 
    rows, column_names = get_snowflake_rows()  # Get data and column names
    
    with open('combine_etl.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        
        # Write column names (header)
        writer.writerow(column_names)
        
        # Write rows data
        writer.writerows(rows)

# Define the pipeline function
def pipeline(): 
    extract()

if __name__ == "__main__": 
    pipeline()  # Execute the pipeline
