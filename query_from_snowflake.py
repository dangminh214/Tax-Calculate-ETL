import snowflake.connector  
import csv
import os
from dotenv import load_dotenv
from decimal import Decimal

# Load environment variables
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

# Query data
cursor = conn.cursor()
sandbox = "SNOWFLAKE_SANDBOX_DB.PUBLIC."
table_name = "EMPLOYEERS"
query = f"SELECT * FROM {sandbox}{table_name}"
cursor.execute(query)

# Extract data
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]  # Column names

def predict_future_salary(salary, years=5, annual_increase=0.05):
    """ Predict salary growth over time. Convert Decimal to float. """
    salary = float(salary)  # Convert to float
    return round(salary * ((1 + annual_increase) ** years), 2)

def calculate_bonus(net_salary, department):
    """ Assign bonus based on department. """
    bonus_rates = {"Sales": 0.10, "IT": 0.12, "HR": 0.08}
    net_salary = float(net_salary)  # Convert to float
    return round(net_salary * bonus_rates.get(department, 0.05), 2)  # Default bonus is 5%

def classify_tax_bracket(salary):
    """ Categorize tax brackets. """
    salary = float(salary)  # Convert to float
    if salary < 5000:
        return "Low"
    elif 5000 <= salary < 7000:
        return "Medium"
    else:
        return "High"
    
def determine_experience_level(age):
    """ Categorize experience level based on age. """
    if age < 26:
        return "Junior"
    elif 26 <= age <= 35:
        return "Mid"
    else:
        return "Senior"

# Add new column headers
new_columns = columns + ["Future_Salary", "Experience_Level", "Bonus", "Tax_Bracket"]

# Process and write to CSV
output_file = 'query_output_extended.csv'
with open(output_file, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(new_columns)  # Write column headers

    for row in rows:
        id, name, age, salary, department, tax, net_salary = row  # Unpack original data
        
        # Perform calculations
        future_salary = predict_future_salary(salary)
        experience_level = determine_experience_level(age)
        bonus = calculate_bonus(net_salary, department)
        tax_bracket = classify_tax_bracket(salary)

        # Append new values to row
        new_row = list(row) + [future_salary, experience_level, bonus, tax_bracket]
        writer.writerow(new_row)

# Close connection
cursor.close()
conn.close()

print(f"✅ Extended output written to: {output_file}")
