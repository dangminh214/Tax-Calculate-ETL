import pandas as pd
import sqlite3
import logging

# Set up logging notification to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_data(file_path):
    """
    Read data from csv file
    """
    try:
        df = pd.read_csv(file_path)
        logging.info("Import successfully from file: {}".format(file_path))
        return df
    except Exception as e:
        logging.error("Failed to read from file {}: {}".format(file_path, e))
        return None

def transform_data(df):
    """
    Transform: Process and transform data
    - Remove empty field in columns 'age' and 'salary'
    - Change fields in 'age' and 'salary' to numeric
    - Calculate tax and  net_salary 
    - Calculate bas on department:  Calculate average net salaray(avg_net_salary)
    """
    # Remove lines which has empty values
    df_clean = df.dropna(subset=['age', 'salary'])
    
    # Change type of field to numeric
    df_clean['age'] = pd.to_numeric(df_clean['age'])
    df_clean['salary'] = pd.to_numeric(df_clean['salary'])
    
    # Calculate tax (10% Salary) and netto salary
    df_clean['tax'] = df_clean['salary'] * 0.1
    df_clean['net_salary'] = df_clean['salary'] - df_clean['tax']
    
    logging.info("Data has been transformed and calculated in a new column.")
    
    # Create summary table base on department
    dept_summary = df_clean.groupby('department', as_index=False)['net_salary'].mean()
    dept_summary.rename(columns={'net_salary': 'avg_net_salary'}, inplace=True)
    logging.info("Has been calculated base on department.")
    
    return df_clean, dept_summary

def load_to_csv(df, file_path):
    """
    Load: Write new cleaned data in a new csv
    """
    try:
        df.to_csv(file_path, index=False, encoding='utf-8')
        logging.info("Written in a new csv: {}".format(file_path))
    except Exception as e:
        logging.error("Failed when write into a new csv {}: {}".format(file_path, e))

def load_to_sqlite(df, db_path, table_name):
    """
    Load: Sava data into SQLite
    """
    try:
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        logging.info("Saved into SQLite, Table '{}' in file '{}'".format(table_name, db_path))
    except Exception as e:
        logging.error("Cannot write in SQLite: {}".format(e))

def etl_pipeline():
    # --- Extract ---
    raw_file = 'raw_data.csv'
    df = extract_data(raw_file)
    if df is None:
        logging.error("Cannot extract from ETL. ETL end...")
        return

    # --- Transform ---
    df_clean, dept_summary = transform_data(df)

    # --- Load: Write cleaned data to new csv ---
    load_to_csv(df_clean, 'cleaned_data.csv')
    load_to_csv(dept_summary, 'department_summary.csv')

    # --- Load: Save cleaned data to sq lite ---
    load_to_sqlite(df_clean, 'etl_data.db', 'employee_data')
    load_to_sqlite(dept_summary, 'etl_data.db', 'department_summary')

if __name__ == "__main__":
    etl_pipeline()
