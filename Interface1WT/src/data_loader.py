import glob
import os

import pandas as pd
import datetime
import math
from file_paths import path_KSReport

import db_config


# Helper functions to load the material table:
def extract_material_df(file_path):
    """
    Extract and classify material data from an Excel file based on Rohstoffnummer validation and classification.

    Args:
        file_path (str): Path to the Excel file.

    Returns:
        pd.DataFrame: DataFrame with dim_material_id, material_name, material_type, and category.
    """
    # Read the Excel file (sheet "Cons_Budget")
    df = pd.read_excel(file_path, sheet_name="Cons_Budget", dtype=str)

    # Rename columns for clarity
    df.columns = ['material_name', 'dim_material_id', 'Dummy', 'category'] + list(df.columns[4:])
    df = df.drop(columns=['Dummy'])  # Drop unnecessary column

    # Function to validate dim_material_id
    def is_valid_rohstoffnummer(value):
        if pd.isna(value):
            return False
        value = str(value)
        return (value.isdigit() and len(value) == 5 and value.startswith("1")) or \
               ("-b" in value and len(value.split('-')[0]) == 5)

    # Filter valid rows
    valid_rows = df[df['dim_material_id'].apply(is_valid_rohstoffnummer)].copy()

    # Drop duplicate rows
    valid_rows = valid_rows.drop_duplicates()

    # Material type classification
    def classify_material_type(rohstoff_id):
        rohstoff_id = str(rohstoff_id)
        if rohstoff_id.startswith("11"):
            return "Cons"
        elif rohstoff_id.startswith("121"):
            return "Paste"
        else:
            return "Others"

    # Add material_type column
    valid_rows['material_type'] = valid_rows['dim_material_id'].apply(classify_material_type)

    # Reorder columns to match the desired order
    df_final = valid_rows[['dim_material_id', 'material_name', 'material_type', 'category']]

    # Reset index for clean output
    df_final = df_final.reset_index(drop=True)

    print("Final Extracted Material Data:")
    print(df_final)  # Display the entire DataFrame once
    return df_final


# Helper functions to load shutdown hour data in  the fact table:
def extract_shutdown_hours_df(file_path, years_to_load, is_budget=True):
    """
    Extract shutdown hours data from the Excel file, including year, month, and day,
    specifically from the 'Help_FC' sheet.

    Args:
        file_path (str): Path to the Excel file.
        years_to_load (list): List of years (int) to filter the data.

    Returns:
        pd.DataFrame: DataFrame with year, month, day, and shutdown hours for the specified years.
    """
    # Select the version sheet name baed on is_budget
    version_sheet = "Cons_Forecast"

    # Extract version name from the version sheet
    version_df = pd.read_excel(file_path, sheet_name=version_sheet, dtype=str)

    # Keep only the first column (Column A in Excel)
    version_name = version_df.iloc[0, 0]

    # Select the sheet name based on is_budget
    sheet_name = "Help" if is_budget else "Help_FC"

    # Load the specified sheet from the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)  # Adjust header to row 2 (0-based index)

    # Select only the date and 'shutdown h' columns
    df = df.iloc[:, [0, 4]]  # Assuming the first column is 'date' and the fifth column is 'shutdown h'
    df.columns = ["date", "shutdown h"]  # Rename columns for clarity

    # Parse the date column
    df["date"] = pd.to_datetime(df["date"], format="%d.%m.%Y", errors="coerce")

    # Drop rows with invalid or missing dates
    df = df.dropna(subset=["date"])

    # Extract year, month, and day from the 'date' column
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    # Ensure 'shutdown h' is numeric and fill NaN with 0
    df["shutdown h"] = pd.to_numeric(df["shutdown h"], errors="coerce").fillna(0)

    # Filter rows for the specified years
    filtered_df = df[df["year"].isin(years_to_load)]

    # Add the version name column
    filtered_df = filtered_df.assign(version_name=version_name)

    # Reset the index
    filtered_df = filtered_df.reset_index(drop=True)

    # Select and reorder columns
    shutdown_hours_df = filtered_df[["year", "month", "day", "shutdown h", "version_name"]]

    print(f"Extracted version name for sheet '{sheet_name}':", version_name)
    print("Extracted Shutdown Hours Data:")
    print(shutdown_hours_df.head())  # Debugging output

    return shutdown_hours_df


def process_shutdown_data(cursor, shutdown_df, is_budget=True):
    """
    Processes and inserts shutdown hours data into the fact_table.

    Args:
        cursor: Database cursor for executing SQL.
        shutdown_df (pd.DataFrame): DataFrame containing shutdown hours data.
        is_budget (bool): If True, process as budget shutdown hours; if False, process as forecast shutdown hours.
    """
    # Determine the column names for shutdown hours
    target_column = "bdgt_shutdown_hours" if is_budget else "fcst_shutdown_hours"

    # Ensure year, month, and day are integers
    shutdown_df[['year', 'month', 'day']] = shutdown_df[['year', 'month', 'day']].astype(int)

    for _, row in shutdown_df.iterrows():
        shutdown_hours = row['shutdown h']
        version_name = row["version_name"]
        year, month, day = row['year'], row['month'], row['day']

        # Fetch time_id
        cursor.execute("SELECT dim_time_id FROM dim_time WHERE year = ? AND month = ? AND day = ?", (year, month, day))
        time_id_result = cursor.fetchone()

        if not time_id_result:
            print(f"[ERROR] No dim_time_id for {day}-{month}-{year}")
            continue

        time_id = time_id_result[0]

        # Insert a new row into fact_table
        print(
            f"Inserting {target_column.upper()}: time_id={time_id}, shutdown_hours={shutdown_hours}, version_name={version_name}")
        cursor.execute(
            f"""
                    INSERT INTO fact_table (material_id, time_id, version_name, bdgt_shutdown_hours, fcst_shutdown_hours, inserted_date)
                    VALUES (NULL, ?, ?, ?, ?, GETDATE())
                    """,
            (time_id, version_name, shutdown_hours if is_budget else None, shutdown_hours if not is_budget else None)
        )


# Helper functions to load budget and forecast data in  the fact table:
def extract_bdgt_fcst_df(file_path, years_to_load, is_budget):
    """
    Extract and filter data for budget or forecast based on the input flag.

    Args:
        file_path (str): Path to the Excel file.
        years_to_load (list): List of years to extract, e.g., [2023, 2024].
        is_budget (bool): Flag to indicate whether to extract budget (True) or forecast (False).

    Returns:
        pd.DataFrame: Filtered DataFrame with renamed date columns and version.
    """
    # Select the version sheet name based on is_budget
    version_sheet = "Cons_Forecast"

    # Extract version name from the version sheet
    version_df = pd.read_excel(file_path, sheet_name=version_sheet, dtype=str)

    # Keep only the first column (Column A in Excel)
    version_name = version_df.iloc[0, 0]

    # Select the correct sheet
    sheet_name = 'Cons_Budget' if is_budget else 'Cons_Forecast'

    # Prefix for the column names
    prefix = "b_" if is_budget else "f_"

    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)

    # Rename columns and drop unnecessary ones
    df.columns = ['Rohstoffname', 'Rohstoffnummer', 'Dummy', 'Kategorie'] + list(df.columns[4:])
    df = df.drop(columns=['Dummy'])

    # Filter valid rows based on Rohstoffnummer
    def is_valid_rohstoffnummer(value):
        if pd.isna(value):
            return False
        value = str(value)
        return (value.isdigit() and len(value) == 5 and value.startswith("1")) or (
                "-b" in value and len(value.split('-')[0]) == 5)

    valid_rows = df[df['Rohstoffnummer'].apply(is_valid_rohstoffnummer)].copy()

    # Identify date columns and filter for specific years
    date_columns = [
        col for col in valid_rows.columns[4:]
        if pd.to_datetime(col, errors='coerce', dayfirst=True).year in years_to_load
    ]

    # Rename date columns with 'b_' or 'f_' prefix (e.g., 'b_jan_23', 'f_jan_23')
    month_mapping = {
        1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun",
        7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec"
    }

    new_date_columns = {
        col: f"{prefix}{month_mapping[pd.to_datetime(col, errors='coerce', dayfirst=True).month]}_"
             f"{str(pd.to_datetime(col, errors='coerce', dayfirst=True).year)[2:]}"
        for col in date_columns
    }

    # Rename columns and filter relevant ones
    valid_rows = valid_rows.rename(columns=new_date_columns)

    valid_rows_with_dates = valid_rows[
        ['Rohstoffname', 'Rohstoffnummer', 'Kategorie'] + list(new_date_columns.values())
    ]

    # Add version column
    valid_rows_with_dates = valid_rows_with_dates.assign(Version=version_name)
    valid_rows_with_dates = valid_rows_with_dates.reset_index(drop=True)  # Reset index for safety

    print(f"Extracted version name for sheet '{sheet_name}':", version_name)
    print(f"Extracted data from sheet '{sheet_name}' for years: {years_to_load}")
    print(valid_rows_with_dates.head())

    return valid_rows_with_dates


def process_data(cursor, df, columns, month_mapping, is_budget=True):
    """
    Processes and inserts budget or forecast data into the fact_table.

    Args:
        cursor: Database cursor for executing SQL.
        df (pd.DataFrame): DataFrame containing budget or forecast data.
        columns (list): List of column names to process.
        month_mapping (dict): Mapping of month abbreviations to integers.
        is_budget (bool): Flag to indicate whether to process budget (True) or forecast (False) data.
    """
    data_type = "BUDGET" if is_budget else "FORECAST"

    for _, row in df.iterrows():
        material_id = row['Rohstoffnummer']
        version_name = row['Version']

        # Verify material_id exists in dim_material
        cursor.execute("SELECT dim_material_id FROM dim_material WHERE dim_material_id = ?", (material_id,))
        if not cursor.fetchone():
            print(f"Material ID {material_id} not found in dim_material. Skipping...")
            continue

        for col in columns:
            try:
                _, month_abbr, year_suffix = col.split('_')
                month = month_mapping[month_abbr]
                year = int(f"20{year_suffix}")

                # Fetch time_id
                cursor.execute("SELECT dim_time_id FROM dim_time WHERE year = ? AND month = ?", (year, month))
                time_id_result = cursor.fetchone()
                if not time_id_result:
                    print(f"No dim_time_id for {month_abbr}-{year}")
                    continue
                time_id = time_id_result[0]

                # Extract value
                value = float(row[col]) if not pd.isna(row[col]) else 0

                # Insert data into fact_table
                print(f"Inserting {data_type}: material_id={material_id}, time_id={time_id}, version={version_name}, {col}={value}")
                cursor.execute(
                    f"""
                    INSERT INTO fact_table (material_id, time_id, version_name, bdgt_shutdown_hours, fcst_shutdown_hours, {col}, inserted_date)
                    VALUES (?, ?, ?, 0, 0, ?, GETDATE())
                    """,
                    (material_id, time_id, version_name, value)
                )
            except Exception as e:
                print(f"Error inserting {col} for material_id={material_id}: {e}")


# Helper functions to check and detect changes the fact table:
def is_fact_table_empty(cursor):
    """
    Check if the fact_table is empty by selecting the first row.
    :param cursor: Database cursor
    :return: True if empty, False otherwise
    """
    cursor.execute("SELECT TOP 1 1 FROM fact_table")
    return cursor.fetchone() is None


def has_new_materials(cursor, df):
    """
    Check if there are new materials not in fact_table.
    :param cursor: Database cursor
    :param df: DataFrame containing the source data
    :return: True if new materials exist, False otherwise
    """
    # Fetch all material_ids from fact_table as strings
    cursor.execute("SELECT DISTINCT material_id FROM fact_table")
    existing_materials = set(str(row[0]).strip() for row in cursor.fetchall() if row[0])
    print(f"Existing materials in database: {existing_materials}")

    # Check for new materials in the DataFrame
    for material_id in df['Rohstoffnummer'].unique():
        material_id_str = str(material_id).strip()  # Convert to string and remove whitespace
        print(f"Checking material_id: {material_id_str}")
        if material_id_str not in existing_materials:
            print(f"New material detected: material_id={material_id_str}")
            return True
    print("No new materials found.")
    return False


def get_dim_time_id(cursor, month_abbr, year_suffix):
    """
    Retrieve dim_time_id for a given month and year.
    :param cursor: Database cursor
    :param month_abbr: Month abbreviation (e.g., 'jan')
    :param year_suffix: Year suffix (e.g., '23')
    :return: dim_time_id or None
    """
    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    month = month_mapping[month_abbr]
    year = int(f"20{year_suffix}")

    cursor.execute("SELECT dim_time_id FROM dim_time WHERE year = ? AND month = ?", (year, month))
    result = cursor.fetchone()
    return result[0] if result else None


def get_latest_fact_data(cursor, material_id, time_id, column_name):
    """
    Retrieve the latest value for a specific material, time, and column in the fact table.

    Args:
        cursor: Database cursor
        material_id: Material ID to check
        time_id: Time ID corresponding to year and month
        column_name: Name of the column to check (e.g., 'b_jan_23', 'f_feb_24')

    Returns:
        Latest value for the column or None if no record exists.
    """
    # Debug: Print inputs
    print(f"Fetching latest data: material_id={material_id}, time_id={time_id}, column_name={column_name}")

    # Debug: Print query
    query = f"""
           SELECT TOP 1 {column_name}
           FROM fact_table
           WHERE material_id = ? AND time_id = ? AND {column_name} IS NOT NULL
           ORDER BY inserted_date DESC
       """
    print(f"Executing query: {query}")

    try:
        # Execute the query
        cursor.execute(query, (material_id, time_id))
        result = cursor.fetchone()

        # Debug: Print query result
        print(f"Query result: {result}")

        # Return the result
        return result[0] if result else None
    except Exception as e:
        print(f"[ERROR] Failed to fetch data: {e}")
        return None


def get_latest_version(cursor, material_id, is_budget, month_columns):
    """
    Retrieve the latest version in the fact table for a given material by identifying non-null budget or forecast values.

    Args:
        cursor: Database cursor
        material_id: Material ID
        is_budget: True for budget data, False for forecast data
        month_columns: List of all relevant month columns (e.g., ['b_jan_23', 'f_feb_23'])

    Returns:
        Latest version string or None.
    """
    # Filter the month columns for budget or forecast
    column_prefix = 'b_' if is_budget else 'f_'
    relevant_columns = [col for col in month_columns if col.startswith(column_prefix)]

    if not relevant_columns:
        print(f"No relevant columns found for {'budget' if is_budget else 'forecast'} data.")
        return None

    # Construct the dynamic column checks
    column_checks = " OR ".join([f"{col} IS NOT NULL" for col in relevant_columns])

    # Query to find the latest version for budget or forecast data
    query = f"""
        SELECT TOP 1 version_name
        FROM fact_table
        WHERE material_id = ? 
          AND ({column_checks})
        ORDER BY inserted_date DESC
    """
    print(f"Executing query: {query}")  # Debugging query
    cursor.execute(query, (material_id,))
    result = cursor.fetchone()
    return result[0] if result else None


def has_version_or_value_changes(cursor, df_new, is_budget=True):
    """
    Compare the new input DataFrame with the latest fact table records to detect changes.

    Args:
        cursor: Database cursor
        df_new: New DataFrame containing the budget or forecast data
        is_budget: True for budget data, False for forecast data

    Returns:
        True if changes are detected, False otherwise.
    """
    data_type = "Budget" if is_budget else "Forecast"

    for _, row in df_new.iterrows():
        material_id = row['Rohstoffnummer']
        version_name = row['Version']

        print(f"Checking material_id: {material_id}, Version: {version_name}")

        # Compare the new version with the latest version in the fact table
        latest_version = get_latest_version(cursor, material_id, is_budget, df_new.columns)

        if latest_version != version_name:
            print(f"  [CHANGE] Version change detected: latest_version={latest_version}, new_version={version_name}")
            return True  # Version has changed

        for column in df_new.columns:
            if column.startswith('b_') if is_budget else column.startswith('f_'):
                try:
                    # Extract time_id (dim_time) from column name
                    _, month_abbr, year_suffix = column.split('_')
                    dim_time_id = get_dim_time_id(cursor, month_abbr, year_suffix)

                    if dim_time_id is None:
                        print(f"  [SKIP] No time_id for column: {column}")
                        continue

                    print(f"Checking: material_id={material_id}, dim_time_id={dim_time_id}, column={column}")

                    # Get latest value from fact table
                    latest_value = get_latest_fact_data(cursor, material_id, dim_time_id, column)
                    current_value = float(row[column]) if not pd.isna(row[column]) else None

                    # Debug information
                    print(f"  Comparing column={column} | latest_value={latest_value}, current_value={current_value}")

                    # Compare the values
                    if (latest_value is None or latest_value == 0) and (current_value is None or current_value == 0):
                        print(f"  [INFO] Both values are NULL or 0. Skipping...")
                        continue  # Treat NULL and 0 as equal

                    if (latest_value is None and current_value is not None) or \
                            (latest_value is not None and current_value is None) or \
                            (latest_value is not None and current_value is not None and not math.isclose(
                                float(latest_value), float(current_value), rel_tol=1e-5)):
                        print(
                            f"  [CHANGE] Detected in column={column}: latest_value={latest_value}, current_value={current_value}")
                        return True  # Change detected

                except Exception as e:
                    print(f"  [ERROR] Error comparing values for column={column}: {e}")

    print(f"No changes detected in {data_type} data.")
    return False


def has_shutdown_hours_changes(cursor, shutdown_df):
    """
    Compare the new shutdown hours DataFrame with the latest fact table records to detect changes.

    Args:
        cursor: Database cursor
        shutdown_df: New DataFrame containing the shutdown hours data

    Returns:
        True if changes are detected or new rows are found, False otherwise.
    """
    print("Checking for changes or new entries in shutdown hours data.")

    for _, row in shutdown_df.iterrows():
        year, month, day = row['year'], row['month'], row['day']
        shutdown_hours_new = row['shutdown h']

        # Fetch time_id
        cursor.execute("SELECT dim_time_id FROM dim_time WHERE year = ? AND month = ? AND day = ?", (year, month, day))
        time_id_result = cursor.fetchone()
        if not time_id_result:
            print(f"[SKIP] No dim_time_id for {day}-{month}-{year}. This date might be missing in dim_time.")
            continue

        time_id = time_id_result[0]

        # Fetch shutdown hours for the time_id
        cursor.execute(
            """
            SELECT shutdown_hours 
            FROM fact_table
            WHERE time_id = ? AND material_id IS NULL
            """,
            (time_id,)
        )
        shutdown_hours_latest = cursor.fetchone()

        if not shutdown_hours_latest:
            # New shutdown hours found for this date
            print(f"[NEW ENTRY] Detected for {day}-{month}-{year}: new shutdown_hours={shutdown_hours_new}")
            return True

        shutdown_hours_latest = shutdown_hours_latest[0]

        # Compare the shutdown hours
        if not math.isclose(shutdown_hours_latest, shutdown_hours_new, rel_tol=1e-5):
            print(f"[CHANGE] Detected for {day}-{month}-{year}: latest={shutdown_hours_latest}, new={shutdown_hours_new}")
            return True

    print("No changes or new entries detected in shutdown hours data.")
    return False


def is_version_unique(cursor, version_name):
    """
    Check if the version name is unique in the fact_table.
    Args:
        cursor: Database cursor
        version_name: The version name from the Excel file
    Returns:
        bool: True if the version name is unique, False otherwise
    """
    cursor.execute("SELECT COUNT(*) FROM fact_table WHERE version_name = ?", (version_name,))
    result = cursor.fetchone()
    if result[0] > 0:
        print(f"[ERROR] Version name '{version_name}' already exists in the database.")
        return False
    return True


def checker_function(cursor, bdgt_df_new, fcst_df_new, shutdown_df):
    """
    Check if the fact_table is empty, has new materials, or if data has changed.

    Args:
        cursor: Database cursor
        bdgt_df_new: New budget DataFrame
        fcst_df_new: New forecast DataFrame
        shutdown_df: New shutdown hours DataFrame

    Returns:
        True if data should be inserted, False otherwise
    """
    # 0 Check if version names in the budget and forecast data are unique
    bdgt_version_name = bdgt_df_new['Version'].iloc[0]
    fcst_version_name = fcst_df_new['Version'].iloc[0]

    if not is_version_unique(cursor, bdgt_version_name):
        print(f"[ERROR] Budget version name '{bdgt_version_name}' already exists in the database.")
        return False

    if not is_version_unique(cursor, fcst_version_name):
        print(f"[ERROR] Forecast version name '{fcst_version_name}' already exists in the database.")
        return False

    # 1. Check if fact_table is empty
    if is_fact_table_empty(cursor):
        print("Fact table is empty. Proceeding to insert all data.")
        return True

    # 2. Check for new materials
    if has_new_materials(cursor, bdgt_df_new) or has_new_materials(cursor, fcst_df_new):
        print("New materials found in the source data. Proceeding to insert all data.")
        return True

    # 3. Check for version or value changes in budget data
    if has_version_or_value_changes(cursor, bdgt_df_new, is_budget=True) is True:
        print("Changes detected in budget data. Proceeding to insert updated data.")
        return True

    # 4. Check for version or value changes in forecast data
    if has_version_or_value_changes(cursor, fcst_df_new, is_budget=False) is True:
        print("Changes detected in forecast data. Proceeding to insert updated data.")
        return True

    # 5. Check for changes or new entries in shutdown hours data
    if has_shutdown_hours_changes(cursor, shutdown_df) is True:
        print("Changes or new entries detected in shutdown hours data. Proceeding to insert updated data.")
        return True

    print("No changes detected. Skipping data insertion.")
    return False


# Dimension table loader functions:
def load_dim_material_table(cursor, material_df):
    """
    Load material data into the dim_material table using a MERGE query.

    Args:
        cursor: Database cursor to execute SQL commands.
        material_df (pd.DataFrame): DataFrame containing dim_material_id, material_name,
                                    material_type, and category.
    """
    # Clean the DataFrame
    material_df = material_df.dropna(subset=['dim_material_id'])  # Drop rows with missing IDs
    material_df = material_df.astype({
        'dim_material_id': str,
        'material_name': str,
        'material_type': str
    })

    # Check for empty or NaN values in the 'category' column
    if material_df['category'].isnull().any() or material_df['category'].str.strip().eq('').any():
        invalid_rows = material_df[material_df['category'].isnull() | material_df['category'].str.strip().eq('')]
        raise ValueError(
            f"Error: Found rows with missing 'category' values. Cannot proceed.\n"
            f"Invalid rows:\n{invalid_rows[['dim_material_id', 'material_name', 'material_type', 'category']]}"
        )

    # Iterate through the cleaned material_df rows
    for _, row in material_df.iterrows():
        merge_query = """
                MERGE INTO dim_material AS target
                USING (VALUES (?, ?, ?, ?)) AS source (dim_material_id, material_name, material_type, category)
                ON target.dim_material_id = source.dim_material_id
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.material_name = source.material_name,
                        target.material_type = source.material_type,
                        target.category = source.category
                WHEN NOT MATCHED THEN 
                    INSERT (dim_material_id, material_name, material_type, category)
                    VALUES (source.dim_material_id, source.material_name, source.material_type, source.category);
            """

        # Extract row values for the query
        values = (
            row['dim_material_id'],  # Primary Key
            row['material_name'],    # Name
            row['material_type'],    # Material Type
            row['category']          # Category
        )

        try:
            # Execute the query
            cursor.execute(merge_query, values)
        except Exception as e:
            print(f"Error inserting row {row['dim_material_id']}: {e}")

    print(f"Successfully loaded {len(material_df)} material records into the database.")


def load_dim_time_table_monthly(cursor, file_path, years_to_load):
    """
    Load time data from an Excel file into dim_time table for budegt and forecast data.

    Args:
        cursor: Database cursor for executing SQL.
        file_path: Path to the Excel file.
        years_to_load: List of years to filter (e.g., [2023, 2024]).
    """
    sheet_name = "Cons_Budget"
    df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=1)

    # Identify valid date columns (date-like headers)
    date_columns = [
        col for col in df.columns if isinstance(col, pd.Timestamp) or isinstance(col, datetime.datetime)
    ]

    # Filter only columns corresponding to the specified years
    date_columns = [col for col in date_columns if col.year in years_to_load]

    # Convert columns to 'MM-YYYY' format
    month_columns = [col.strftime('%m-%Y') for col in date_columns if not pd.isna(col)]

    # Debugging Output
    print(f"Detected month columns for years {years_to_load}:", month_columns)

    time_data = []
    for col in month_columns:
        month, year = map(int, col.split('-'))  # Split 'MM-YYYY' into integers
        quarter = (month - 1) // 3 + 1  # Calculate the quarter
        time_data.append((year, quarter, month, 0))  # Static day=0 when loading budget and forecast data

    # Insert or merge the extracted data into the dim_time table
    for year, quarter, month, day in time_data:
        cursor.execute(
            """
            MERGE INTO dim_time AS target
            USING (SELECT ? AS year, ? AS quarter, ? AS month, ? AS day) AS source
            ON target.year = source.year AND target.month = source.month
            WHEN NOT MATCHED THEN
                INSERT (year, quarter, month, day)
                VALUES (source.year, source.quarter, source.month, source.day);
            """,
            (year, quarter, month, day)
        )

    print(f"Successfully loaded {len(time_data)} unique time records for years {years_to_load} into dim_time.")


def load_dim_time_daily(cursor, file_path, years_to_load):
    """
    Load daily time data (shutdown hours) into dim_time table using the first column as the date.

    Args:
        cursor: Database cursor.
        file_path: Path to the Excel file.
        years_to_load: List of years to filter (e.g., [2023, 2024]).
    """
    # Load the sheet for shutdown hours
    sheet_name = "Help_FC"  # Adjust if the sheet name differs
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)

    # Access the first column directly
    date_column = df.iloc[:, 0]  # First column as date

    # Drop rows where the first column is empty
    df = df.dropna(subset=[df.columns[0]])

    # Parse the first column as dates
    df["date"] = pd.to_datetime(df.iloc[:, 0], format="%d.%m.%Y", errors="coerce")

    # Filter out rows with invalid dates
    invalid_rows = df[df["date"].isna()]
    if not invalid_rows.empty:
        print(f"Skipping {len(invalid_rows)} rows with invalid or unparsable dates.")
        df = df[~df["date"].isna()]  # Keep only rows with valid dates

    # Extract year, month, and day
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    # Filter for the specified years
    df_filtered = df[df["year"].isin(years_to_load)]

    # Calculate quarter
    df_filtered["quarter"] = (df_filtered["month"] - 1) // 3 + 1

    # Convert to tuples
    time_data = df_filtered[["year", "quarter", "month", "day"]].drop_duplicates().itertuples(index=False, name=None)

    # Insert or merge into the dim_time table
    for year, quarter, month, day in time_data:
        cursor.execute(
            """
            MERGE INTO dim_time AS target
            USING (SELECT ? AS year, ? AS quarter, ? AS month, ? AS day) AS source
            ON target.year = source.year AND target.month = source.month AND target.day = source.day
            WHEN NOT MATCHED THEN
                INSERT (year, quarter, month, day)
                VALUES (source.year, source.quarter, source.month, source.day);
            """,
            (year, quarter, month, day)
        )

    print(f"Successfully loaded {len(df_filtered)} daily time records into dim_time.")


# Fact table loader function:
def load_fact_table(cursor, file_path, years_to_load):
    """
    Load fact_table only if conditions are met.
    Budget and Forecast data are in a monthly basis and have the same structure.
    Shutdown hours data is in a daily basis and needs to be loaded separately for budget and forecast.

    Args:
        cursor: Database cursor
        file_path: Path to the source Excel file
        years_to_load: Years of data to load
    """
    # Extract Budget and Forecast original data
    bdgt_df = extract_bdgt_fcst_df(file_path, years_to_load, is_budget=True)
    fcst_df = extract_bdgt_fcst_df(file_path, years_to_load, is_budget=False)

    # Extract Shutdown Hours data separately for budget and forecast
    bdgt_shutdown_df = extract_shutdown_hours_df(file_path, years_to_load, is_budget=True)
    fcst_shutdown_df = extract_shutdown_hours_df(file_path, years_to_load, is_budget=False)

    # Month abbreviation mapping
    month_mapping = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }

    # Extract month-year columns that contain budget and forecast values
    bdgt_value_columns = [col for col in bdgt_df.columns if col.startswith('b_')]
    fcst_value_columns = [col for col in fcst_df.columns if col.startswith('f_')]

    # Filter DataFrame to include only relevant columns
    df_budget_filtered = bdgt_df.loc[:, ['Rohstoffnummer', 'Version'] + bdgt_value_columns]
    df_forecast_filtered = fcst_df.loc[:, ['Rohstoffnummer', 'Version'] + fcst_value_columns]

    # Check if conditions are met to load data
    load_conditions = checker_function(cursor, df_budget_filtered, df_forecast_filtered, bdgt_shutdown_df)

    if load_conditions:
        # Process Budget Data
        process_data(cursor, df_budget_filtered, bdgt_value_columns, month_mapping, is_budget=True)

        # Process Forecast Data
        process_data(cursor, df_forecast_filtered, fcst_value_columns, month_mapping, is_budget=False)

        # Process Shutdown Hours Data
        process_shutdown_data(cursor, bdgt_shutdown_df, is_budget=True)
        process_shutdown_data(cursor, fcst_shutdown_df, is_budget=False)

        print("Fact table successfully updated with Budget, Forecast, and Shutdown Hours data.")
    else:
        print("No data inserted. Conditions not met.")


# Main function to load all tables:
def load_tables():
    conn = None
    cursor = None
    try:
        conn = db_config.get_db_connection()  # Establish database connection
        cursor = conn.cursor()  # Get database cursor

        # File path and years to load
        years_to_load = [2023, 2024, 2025, 2026, 2027, 2028]
        file_path = path_KSReport

        # Extract and load material data
        material_df = extract_material_df(file_path)

        # Extract budget and forecast data
        bdgt_df = extract_bdgt_fcst_df(file_path, years_to_load, is_budget=True)
        fcst_df = extract_bdgt_fcst_df(file_path, years_to_load, is_budget=False)


        print("Budget DataFrame:")
        print(bdgt_df.head())
        print("Forecast DataFrame:")
        print(fcst_df.head())

        # Load tables
        load_dim_material_table(cursor, material_df)
        load_dim_time_table_monthly(cursor, file_path, years_to_load)
        load_dim_time_daily(cursor, file_path, years_to_load)
        load_fact_table(cursor, file_path, years_to_load)

        # Commit changes to the database
        conn.commit()
        print("All tables loaded successfully.")

    except Exception as e:
        print(f"Error loading tables: {e}")
        if conn:
            conn.rollback()  # Rollback changes on error

    finally:
        if cursor:
            cursor.close()  # Close cursor
        if conn:
            conn.close()  # Close connection


if __name__ == "__main__":
    load_tables()
