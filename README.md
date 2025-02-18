# ETL Pipeline Project

This project is a simple **ETL (Extract, Transform, Load) pipeline** that processes employee salary data from a CSV file, performs transformations, and loads the cleaned data into a new CSV file and an SQLite database.


## Features
- **Extract**: Reads employee salary data from a CSV file.
- **Transform**:  
  - Removes rows with missing values in `age` and `salary`.  
  - Converts `age` and `salary` columns to numeric format.  
  - Computes tax (10% of salary) and net salary.  
  - Calculates the average net salary for each department.  
- **Load**:  
  - Saves cleaned data into `cleaned_data.csv` and department summary into `department_summary.csv`.  
  - Stores data in an SQLite database (`etl_data.db`), creating two tables:  
    - `employee_data` (Cleaned employee salary data)  
    - `department_summary` (Average net salary per department)  

## Installation

### 1. Clone the repository
```bash
git clone <repository-url>
cd etl-pipeline
```

### Install Depencies
Ensure you have Python 3 and install required packages:
```bash
pip install pandas sqlite3

```

### Run: 
Run the ETL pipeline using:
To run etl and export csv file
```bash
py etl.py
```

To query saved data from snowflake
```bash
py query_from_snowflake.py
```
