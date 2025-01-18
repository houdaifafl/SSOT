import pandas as pd
import pyodbc
import db_config as db_config
from file_paths import path_KSReport

# Helper functions:
def get_year_month_columns(file_path, sheet_name, years_to_load):
    """
    Extract the month columns for the pre-defined years from an Excel sheet
    and format them as `jan_yy` for each year.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (str): Name of the sheet in the Excel file.

    Returns:
        list: A list of formatted month columns for the pre-defined years
    """

    # Read the header of the Excel sheet to get the column names
    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)

    print("Column headers in Excel file:", df.columns)  # Debug print to check column names

    # Extract columns related to the pre-defined years and format them
    month_columns = []
    for column in df.columns:
        # Check if the column is datetime-like and belongs to the current year
        if isinstance(column, pd.Timestamp) or pd.to_datetime(column, errors='coerce') is not pd.NaT:
            dt_column = pd.to_datetime(column, errors='coerce')
            if dt_column.year in years_to_load:  # Only include columns from the pre-defined years
                month = dt_column.strftime('%b_%y').lower()  # Format as 'jan_yy' and make lowercase
                month_columns.append(month)
        elif isinstance(column, str) and any(str(year)[-2:] in column for year in years_to_load):  # Handle string columns with the year
            try:
                dt_column = pd.to_datetime(column, format='%b.%y', errors='coerce')
                if dt_column and dt_column.year in years_to_load:
                    month = dt_column.strftime('%b_%y').lower()  # Format as 'jan_yy' and make lowercase
                    month_columns.append(month)
            except ValueError:
                continue  # Skip if the string cannot be parsed

    print(f"Detected month columns for the pre-defined years:", month_columns)  # Debug print to check extracted month columns
    return month_columns


# dimensional table creators:
def create_dim_material_table(cursor):
    """
    Create a dimension table for saving the material details.

    Args:
        cursor: pyodbc cursor object.
    """
    create_table_query = f"""
        IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dim_material')
        BEGIN
            CREATE TABLE dim_material (
                dim_material_id VARCHAR(20) PRIMARY KEY,
                material_name VARCHAR(100),
                material_type VARCHAR(50),
                category VARCHAR(10)
            );
        END;
        """
    cursor.execute(create_table_query)
    print("dim_material table created.")


def create_dim_time_table(cursor):
    """
    Create a dimension table for saving the mm_yy time elements in budget and forecast data.
    (the day element is only for shutdown hours).

    Args:
        cursor: pyodbc cursor object.
    """
    create_table_query = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dim_time')
        BEGIN
            CREATE TABLE dim_time (
                dim_time_id INT PRIMARY KEY IDENTITY(1,1),
                year INT NOT NULL,
                quarter INT NOT NULL,
                month INT NOT NULL,
                day INT NOT NULL
            );
        END
        """
    # Execute the query to create the table
    cursor.execute(create_table_query)
    print("dim_time table created successfully.")


# fact table creator:
def create_fact_table(cursor, month_columns):
    """
    Create a fact table for saving the budget and forecast data.

    Args:
        cursor: pyodbc cursor object.
        month_columns (list): List of formatted month columns.
    """
    create_table_query = """
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'fact_table')
        BEGIN
            CREATE TABLE fact_table (
                id INT PRIMARY KEY IDENTITY(1,1),
                material_id VARCHAR(20),
                time_id INT,
                version_name VARCHAR(100),
                bdgt_shutdown_hours FLOAT,
                fcst_shutdown_hours FLOAT,
                inserted_date DATETIME DEFAULT GETDATE(),
                FOREIGN KEY (material_id) REFERENCES dim_material(dim_material_id),
                FOREIGN KEY (time_id) REFERENCES dim_time(dim_time_id)
    """

    # Add two columns for each month in the current year: one for budget (b_) and one for forecast (f_)
    for month in month_columns:
        create_table_query += f",\n[b_{month}] DECIMAL(18, 2)"  # Budget value columns
        create_table_query += f",\n[f_{month}] DECIMAL(18, 2)"  # Forecast value columns

    # Close the CREATE TABLE query
    create_table_query += "\n); END;"

    # Execute the query
    print("Executing fact_table creation query...")
    cursor.execute(create_table_query)
    print("fact_table created.")


# Main function to create tables:
def create_tables():
    """
    Create the dimension and fact tables in the database.
    """
    conn = None
    cursor = None
    try:
        # Get the database connection
        conn = db_config.get_db_connection()
        cursor = conn.cursor()

        # Define the years to load
        years_to_load = [2023, 2024, 2025, 2026, 2027, 2028]

        # Create the dimension tables
        create_dim_material_table(cursor)
        create_dim_time_table(cursor)

        # Get the month columns for the current year
        sheet_name = "Cons_Budget"
        month_columns = get_year_month_columns(path_KSReport, sheet_name, years_to_load)

        # Create the fact table
        create_fact_table(cursor, month_columns)

        # Commit the transaction
        conn.commit()
        print("All tables created successfully.")

    # Error handling
    except pyodbc.Error as e:
        print("Error creating tables:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    create_tables()