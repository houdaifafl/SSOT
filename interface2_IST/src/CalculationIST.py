from datetime import datetime
import pandas as pd
from Interface1WT.src.calculations import get_dates_and_version_from_excel
from db_config import get_db_connection
from file_paths import path_variables

def total_menge_for_interval(conn, start_date, end_date):
    """
    Function to calculate the total used raw materials (ROH) for each day in a given date interval.

    Args:
        conn: The database connection object.
        start_date (str): The start date of the interval in 'DD.MM.YYYY' format.
        end_date (str): The end date of the interval in 'DD.MM.YYYY' format.

    Returns:
        List[Dict]: A list of dictionaries containing date and total values in tons.
    """
    try:
        # Create a cursor to execute queries
        cursor = conn.cursor()

        # Convert start_date and end_date to datetime objects
        from datetime import datetime, timedelta
        start_date = datetime.strptime(start_date, "%d.%m.%Y")
        end_date = datetime.strptime(end_date, "%d.%m.%Y")

        # Debugging: Print the parsed date range
        print(f"Processing interval from {start_date.date()} to {end_date.date()}...")

        daily_totals = []  # To store results for each day


        current_date = start_date
        while current_date <= end_date:
            # Format the current date for processing (SQL compatible)
            date_str_sql = current_date.strftime("%Y-%m-%d")

            # Debugging: Print the current date being processed
            print(f"Processing date: {date_str_sql}")

            # SQL query to calculate the sum for the current day
            query = """
            SELECT SUM(MngD) AS total_sum
            FROM dbo.dim_lagerbewegung
            WHERE LPlzIdt = 55 AND MatArt = 'ROH' AND BucDat = ?
            """
            # Execute the query with the provided date
            cursor.execute(query, (date_str_sql,))
            result = cursor.fetchone()

            # Debugging: Print the raw result from the query
            print(f"Raw query result for {date_str_sql}: {result}")

            # Get the total sum (handle None if no rows are returned)
            total_sum_kg = abs(result[0]) if result[0] is not None else 0

            # Debugging: Print the total sum in kilograms
            print(f"Total sum in kg for {date_str_sql}: {total_sum_kg}")

            total_sum_tons = round(total_sum_kg / 1000)  # Convert to tons and round

            # Debugging: Print the total sum in tons
            print(f"Total sum in tons for {date_str_sql}: {total_sum_tons}")

            # Reformat the date for output
            date_str_output = current_date.strftime("%d.%m.%Y")

            # Append the result to the list
            daily_totals.append({'date': date_str_output, 'tons': total_sum_tons})

            # Move to the next day
            current_date += timedelta(days=1)

        # Debugging: Print the final daily totals and monthly totals
        print("Final daily totals:")
        for daily_total in daily_totals:
            print(daily_total)

        # Return the daily totals with monthly totals included
        return daily_totals

    except Exception as e:
        print(f"An error occurred: {e}")
        return []


def grouped_summary(conn, date):
    """
    Function to summarize MngD values grouped by KeziBez and MatIdt for a specific date
    and print the results alongside the total in tons.
    Args:
        conn: The database connection object.
        date (str): The date to filter the data.
    Returns:
        None
    """
    try:
        # Create a cursor to execute the query
        cursor = conn.cursor()

        # SQL query to fetch all necessary columns
        query = """
        SELECT MatIdt, KeziBez, MngD
        FROM dbo.dim_lagerbewegung
        WHERE LPlzIdt = 55 AND MatArt = 'ROH' AND BucDat = ?
        """

        # Execute the query with the provided date
        cursor.execute(query, (date,))
        rows = cursor.fetchall()

        # Check if rows exist
        if not rows:
            print(f"Keine Daten für den angegebenen Tag ({date}) gefunden.")
            return

        # Debug: Print the raw rows to verify their structure
        print("Rows returned by the query:", rows)

        # Filter out rows with null values (if any)
        clean_rows = [row for row in rows if row[0] is not None and row[1] is not None and row[2] is not None]

        # Debug: Print the cleaned rows
        print("Cleaned rows (no null values):", clean_rows)

        # Initialize a list to store valid data for the DataFrame
        valid_rows = []

        # Iterate through the rows and handle any issues
        for row in clean_rows:
            try:
                # Extract and process each row
                mat_id, kezi_bez, mngd = row

                # Correct the material ID (remove 3rd and 5th "0" only if it starts with "1")
                """if str(mat_id).startswith("1"):
                    mat_id_str = str(mat_id)
                    mat_id_corrected = mat_id_str[:2] + mat_id_str[3:4] + mat_id_str[5:]  # Remove 3rd and 5th positions
                else:
                    mat_id_corrected = mat_id"""

                mngd_in_tons = round((mngd / 1000), 1)  # Convert to tons and round
                valid_rows.append((mat_id, kezi_bez, mngd_in_tons))
            except Exception as e:
                # Handle any errors for individual rows and continue
                print(f"Error processing row {row}: {e}")
                continue

        # Create a DataFrame from the valid rows
        df = pd.DataFrame(valid_rows, columns=["MatIdt", "KeziBez", "MngD"])

        # Debug: Print the constructed DataFrame
        print("\nConstructed DataFrame:")
        print(df)

        # Calculate the total sum in tons directly from the DataFrame
        total_sum_tons = df["MngD"].sum()

        # Print the total sum in tons
        print(f"\nDie Summe der benutzten Rohstoffe am Tag ({date}) ist: {total_sum_tons:.1f} t")

        # Group by KeziBez and MatIdt and sum MngD
        grouped = df.groupby(["MatIdt", "KeziBez"], as_index=False).sum()

        # Print the grouped results
        print(f"\nZusammenfassung der Rohstoffe am {date}:")
        for _, row in grouped.iterrows():
            print(f"- Material ID: {row['MatIdt']} ({row['KeziBez']}): {row['MngD']:.1f} t")
    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")



def grouped_summary_for_interval(conn, start_date, end_date):
    """
    Function to calculate daily values for a specified time interval, mapped to the 'dim_material' table IDs.
    Ensures that all concentrates in 'dim_material' are included, with missing values defaulting to 0.

    Args:
        conn: The database connection object.
        start_date (str): The start date of the interval in 'DD.MM.YYYY' format.
        end_date (str): The end date of the interval in 'DD.MM.YYYY' format.

    Returns:
        dict: A dictionary with dates as keys and nested dictionaries of dim_material IDs and their values in tons.
    """
    try:
        from datetime import datetime, timedelta

        # Convert start_date and end_date to datetime objects
        start_date = datetime.strptime(start_date, "%d.%m.%Y")
        end_date = datetime.strptime(end_date, "%d.%m.%Y")

        print(f"Processing interval from {start_date.date()} to {end_date.date()}...")

        # Fetch all dim_material IDs and map to names
        cursor = conn.cursor()
        cursor.execute("SELECT dim_material_id, material_name FROM dbo.dim_material")
        material_map = {str(row[0]): row[1] for row in cursor.fetchall()}  # Map dim_material_id to names (as strings)

        if not material_map:
            raise ValueError("No materials found in the 'dim_material' table.")

        print(f"Fetched material mapping from 'dim_material': {material_map}")

        daily_values_by_date = {}

        # Loop through each day in the interval
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            print(f"Processing date: {date_str}")

            # Query for daily values grouped by MatIdt
            query = """
            SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
            FROM dbo.dim_lagerbewegung dl
            WHERE dl.LPlzIdt = 55 AND dl.MatArt = 'ROH' AND dl.BucDat = ?
            GROUP BY dl.MatIdt
            """
            cursor.execute(query, (date_str,))
            rows = cursor.fetchall()

            # Debugging: Print query results
            print(f"Query results for {date_str}: {rows}")

            # Prepare the dictionary for this date
            daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in material_map.keys()}  # Initialize all to 0

            for row in rows:
                try:
                    raw_mat_id, value_in_kg = row

                    # Debug: Print raw material ID before transformation
                    print(f"Raw Material ID: {raw_mat_id}")

                    # Correct the material ID (remove 3rd and 5th "0" if it starts with "1")
                    """if str(raw_mat_id).startswith("1"):
                        mat_id_str = str(raw_mat_id)
                        mat_id_corrected = mat_id_str[:2] + mat_id_str[3:4] + mat_id_str[5:]
                    else:
                        mat_id_corrected = str(raw_mat_id)"""

                    # Debug: Print corrected material ID
                    print(f"Corrected Material ID: {raw_mat_id}")

                    # Map the corrected ID to dim_material IDs
                    if raw_mat_id in material_map:
                        daily_values[raw_mat_id]["value"] = round((value_in_kg / 1000), 1)  # Convert kg to tons and round
                    else:
                        print(f"Material ID {raw_mat_id} not found in 'dim_material'.")
                except Exception as row_error:
                    print(f"Error processing row {row}: {row_error}")
                    continue

            # Debugging: Print daily values dictionary
            print(f"Daily values for {date_str}: {daily_values}")

            daily_values_by_date[date_str] = daily_values

            # Move to the next day
            current_date += timedelta(days=1)

        # Debugging: Print the full dictionary at the end
        print("Final daily values by date:")
        for date, values in daily_values_by_date.items():
            print(f"{date}: {values}")

        return daily_values_by_date

    except Exception as e:
        print(f"An error occurred during grouped summary: {e}")
        return {}


def calculate_all_concs(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for all concentrates of type 'Concs'.
    """
    return _calculate_concentrates(conn, start_date, end_date, category=None)


def calculate_category_h_concs(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for concentrates of type 'Concs' with category 'H'.
    """
    return _calculate_concentrates(conn, start_date, end_date, category="H")


def calculate_category_n_concs(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for concentrates of type 'Concs' with category 'N'.
    """
    return _calculate_concentrates(conn, start_date, end_date, category="N")


def calculate_category_PK_concs(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for concentrates of type 'Concs' with category 'PK'.
    """
    return _calculate_concentrates(conn, start_date, end_date, category="PK")

def _calculate_concentrates(conn, start_date, end_date, category=None):
    """
    Internal function to calculate daily and monthly values for concentrates and their percentages.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    # Fetch material mappings based on category
    category_filter = "AND category = ?" if category else ""
    query = f"""
        SELECT dim_material_id, material_name 
        FROM dbo.dim_material 
        WHERE material_type = 'Cons' {category_filter}
    """
    params = (category,) if category else ()
    cursor.execute(query, params)
    material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
    label = f"Concs with Category '{category}'" if category else "All Cons"

    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"), end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

    # Debugging print to confirm 'ist_values_by_date' mapping
    print("IST Values by Date:", ist_values_by_date)


    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}



    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

                # Calculate the percentage if the ist_total is available
        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals, percentage, and results
        daily_values["daily_total"] = daily_total
        daily_values_by_date[date_str] = daily_values
        percentages_by_date[date_str] = daily_percentage

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }

def _calculate_al_materials_and_name(conn, start_date, end_date, material_name=None):
    """
    Internal function to calculate daily and monthly values for concentrates and their percentages based on a specific material name.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    # Fetch material mappings based on material name
    material_filter = "AND material_name = ?" if material_name else ""
    query = f"""
            SELECT dim_material_id, material_name 
            FROM dbo.dim_material 
            WHERE 1=1 {material_filter}
        """
    params = (material_name,) if material_name else ()
    cursor.execute(query, params)
    material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
    label = f"Concs for Material '{material_name}'" if material_name else "All Materials"
    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"), end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

    # Debugging print to confirm 'ist_values_by_date' mapping
    print("IST Values by Date:", ist_values_by_date)

    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}

    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

        # Calculate the percentage if the ist_total is available
        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals, percentage, and results
        daily_values["daily_total"] = daily_total
        daily_values_by_date[date_str] = daily_values
        percentages_by_date[date_str] = daily_percentage

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }

def calculate_category_P_pastes(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for materials of type 'Paste' with category 'P'.
    """
    return _calculate_pastes(conn, start_date, end_date, category="P")

def _calculate_pastes(conn, start_date, end_date, category=None):
    """
    Internal function to calculate daily and monthly values for pastes and their percentages.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    # Fetch material mappings based on category
    category_filter = "AND category = ?" if category else ""
    query = f"""
        SELECT dim_material_id, material_name 
        FROM dbo.dim_material 
        WHERE material_type = 'Paste' {category_filter}
    """
    params = (category,) if category else ()
    cursor.execute(query, params)
    material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
    label = f"Paste with Category '{category}'" if category else "All Paste"

    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"), end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

    # Debugging print to confirm 'ist_values_by_date' mapping
    print("IST Values by Date:", ist_values_by_date)


    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}



    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

                # Calculate the percentage if the ist_total is available
        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals, percentage, and results
        daily_values["daily_total"] = daily_total
        daily_values_by_date[date_str] = daily_values
        percentages_by_date[date_str] = daily_percentage

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }


def _calculate_all_materials(conn, start_date, end_date, category=None):
    """
    Internal function to calculate daily and monthly values for all materials and their percentages.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    # Fetch material mappings based on category
    category_filter = "AND category = ?" if category else ""
    query = f"""
        SELECT dim_material_id, material_name 
        FROM dbo.dim_material 
    """
    params = (category,) if category else ()
    cursor.execute(query, params)
    material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
    label = f"Concs with Category '{category}'" if category else "All Cons"

    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"), end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

    # Debugging print to confirm 'ist_values_by_date' mapping
    print("IST Values by Date:", ist_values_by_date)


    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}



    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

                # Calculate the percentage if the ist_total is available
        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals, percentage, and results
        daily_values["daily_total"] = daily_total
        daily_values_by_date[date_str] = daily_values
        percentages_by_date[date_str] = daily_percentage

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }


def _calculate_recirculate_concentrates(conn, start_date, end_date):
    """
    Internal function to calculate daily and monthly values for recirculate materials.
    The material ID transformation is applied directly, and the percentage calculation is removed.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date = datetime.strptime(start_date, "%d.%m.%Y")
    end_date = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    daily_values_by_date = {}
    monthly_totals = defaultdict(float)

    # Loop through each day in the interval
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55) AND dl.MatArt = 'KRSM' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        # Initialize daily totals
        daily_values = {}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            # Directly transform material ID (if needed)

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            # Convert kg to tons and update totals
            value_in_tons = abs(round(value_in_kg / 1000))
            daily_values[raw_mat_id] = value_in_tons
            daily_total += value_in_tons

        # Store daily totals and results
        daily_values_by_date[date_str] = {
            "daily_total": daily_total,
            "materials": daily_values,
        }

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print("\n=== Monthly Totals ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print("\n=== Daily Values ===")
    for date, data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'Material ID':<15}{'Value (tons)':<15}")
        print("-" * 30)
        for mat_id, value in data["materials"].items():
            print(f"{mat_id:<15}{value:<15.1f}")
        print("-" * 30)
        print(f"{'Daily Total:':<15}{data['daily_total']:<15.1f}")

    return {
        "daily_values": daily_values_by_date,
        #"monthly_totals": dict(monthly_totals),
    }

def _calculate_fluxes_concentrates(conn, start_date, end_date):
    """
    Internal function to calculate daily and monthly values for fluxes materials.
    The material ID transformation is applied directly, and the percentage calculation is removed.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date = datetime.strptime(start_date, "%d.%m.%Y")
    end_date = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    daily_values_by_date = {}
    monthly_totals = defaultdict(float)

    # Loop through each day in the interval
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'HIBE' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        # Initialize daily totals
        daily_values = {}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            # Directly transform material ID (if needed)
            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            # Convert kg to tons and update totals
            value_in_tons = abs(round(value_in_kg / 1000))
            daily_values[raw_mat_id] = value_in_tons
            daily_total += value_in_tons

        # Store daily totals and results
        daily_values_by_date[date_str] = {
            "daily_total": daily_total,
            "materials": daily_values,
        }

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print("\n=== Monthly Totals ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print("\n=== Daily Values ===")
    for date, data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'Material ID':<15}{'Value (tons)':<15}")
        print("-" * 30)
        for mat_id, value in data["materials"].items():
            print(f"{mat_id:<15}{value:<15.1f}")
        print("-" * 30)
        print(f"{'Daily Total:':<15}{data['daily_total']:<15.1f}")

    return {
        "daily_values": daily_values_by_date
    }





def calculate_category_all_others(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for all materials of type 'Others'.
    """
    return _calculate_others(conn, start_date, end_date, categories=None)

def calculate_category_RI_others(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for materials of type 'Others' with category 'RI'.
    """
    return _calculate_others(conn, start_date, end_date, categories=["RI"])

def calculate_category_RE_others(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for materials of type 'Others' with category 'RE'.
    """
    return _calculate_others(conn, start_date, end_date, categories=["RE"])

def calculate_category_OX_others_category(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for materials of type 'Others' with category 'Ox'.
    """
    return _calculate_others(conn, start_date, end_date, categories=["Ox"])

def calculate_category_OX_others(conn, start_date, end_date):
    """
    Function to calculate daily and monthly values for materials with category 'OX',
    including percentages
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    categories = ['OX','P']
    # Fetch material mappings for category "OX"
    query = """
        SELECT dim_material_id, material_name 
        FROM dbo.dim_material 
        WHERE category IN ({})
    """.format(", ".join(["?"] * len(categories)))
    cursor.execute(query, categories)
    material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
    label = f"Materials with Categories '{categories}'"

    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"),end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

        # Debugging print to confirm 'ist_values_by_date' mapping
        print("IST Values by Date:", ist_values_by_date)

    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}



    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
            SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
            FROM dbo.dim_lagerbewegung dl
            WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
            GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(
            f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals, percentages, and results
        daily_values["daily_total"] = daily_total
        percentages_by_date[date_str] = daily_percentage
        daily_values_by_date[date_str] = daily_values

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }





def _calculate_others(conn, start_date, end_date, categories=None):
    """
    Internal function to calculate daily and monthly values for materials von type "Others",
    including percentages

    Args:
        conn: The database connection object.
        start_date (str): The start date of the interval in 'DD.MM.YYYY' format.
        end_date (str): The end date of the interval in 'DD.MM.YYYY' format.
        categories (list or None): Filter for specific categories (e.g., ['H', 'N'] or None for all).

    Returns:
        dict: A dictionary with:
              - dates as keys and nested dictionaries of dim_material IDs, values in tons, daily totals, and percentages,
              - aggregated monthly totals,
              - percentages by date.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict

    # Convert dates to datetime objects
    start_date_dt = datetime.strptime(start_date, "%d.%m.%Y")
    end_date_dt = datetime.strptime(end_date, "%d.%m.%Y")

    cursor = conn.cursor()

    # Fetch material mappings based on category
    if categories:
        query = """
            SELECT dim_material_id, material_name 
            FROM dbo.dim_material 
            WHERE material_type = 'Others' AND category IN ({})
        """.format(", ".join(["?"] * len(categories)))
        cursor.execute(query, categories)
        material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
        label = f"Others with Categories '{categories}'"
    else:
        query = "SELECT dim_material_id, material_name FROM dbo.dim_material WHERE material_type = 'Others'"
        cursor.execute(query)
        material_map = {str(row[0]): row[1] for row in cursor.fetchall()}
        label = "All Others"

    if not material_map:
        raise ValueError(f"No materials found for {label} in the 'dim_material' table.")

    # Retrieve daily IST values from 'total_menge_for_interval'
    daily_totals = total_menge_for_interval(conn, start_date_dt.strftime("%d.%m.%Y"),end_date_dt.strftime("%d.%m.%Y"))

    # Debugging print to inspect the structure of daily_totals and keys
    print("Debugging daily_totals:", daily_totals)

    # Ensure 'date' and 'tons' keys exist and align date formats
    ist_values_by_date = {}
    for entry in daily_totals:
        try:
            # Standardize date format to match loop
            date = datetime.strptime(entry['date'], "%d.%m.%Y").strftime("%Y-%m-%d")
            tons = entry['tons']
            ist_values_by_date[date] = tons
        except KeyError as e:
            print(f"KeyError in entry {entry}: Missing key {e}")
        except ValueError as e:
            print(f"ValueError in entry {entry}: {e}")

            # Debugging print to confirm 'ist_values_by_date' mapping
        print("IST Values by Date:", ist_values_by_date)

    daily_values_by_date = {}
    monthly_totals = defaultdict(float)
    percentages_by_date = {}



    # Loop through each day in the interval
    current_date = start_date_dt
    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y-%m-%d")

        # Query for daily values grouped by material ID
        query = """
        SELECT dl.MatIdt, SUM(dl.MngD) AS total_value
        FROM dbo.dim_lagerbewegung dl
        WHERE dl.LPlzIdt IN (55, 53) AND dl.MatArt = 'ROH' AND dl.BucDat = ?
        GROUP BY dl.MatIdt
        """
        cursor.execute(query, (date_str,))
        rows = cursor.fetchall()

        ist_total = ist_values_by_date.get(date_str, 0)
        # Initialize daily values
        daily_values = {material_id: {"name": material_map[material_id], "value": 0} for material_id in
                        material_map.keys()}
        daily_total = 0

        for row in rows:
            raw_mat_id, value_in_kg = row

            """if str(raw_mat_id).startswith("1"):
                mat_id_corrected = str(raw_mat_id)[:2] + str(raw_mat_id)[3:4] + str(raw_mat_id)[5:]
            else:
                mat_id_corrected = str(raw_mat_id)"""

            if raw_mat_id in material_map:
                value_in_tons = abs(round((value_in_kg / 1000), 1))  # Convert kg to tons
                daily_values[raw_mat_id]["value"] = value_in_tons
                daily_total += value_in_tons

        if ist_total > 0:
            daily_percentage = round((daily_total * 100 / ist_total), 1)
        else:
            daily_percentage = 0  # Handle division by zero case

        # Debugging: Print ist_total, daily_total, and the calculated percentage
        print(
            f"Debugging: Date: {date_str}, IST Total: {ist_total}, Daily Total: {daily_total}, Percentage: {daily_percentage}")

        # Store daily totals and percentages
        daily_values["daily_total"] = daily_total
        percentages_by_date[date_str] = daily_percentage
        daily_values_by_date[date_str] = daily_values

        # Add daily totals to monthly totals
        month_str = current_date.strftime("%Y-%m")
        monthly_totals[month_str] += daily_total

        current_date += timedelta(days=1)

    # Print organized output
    print(f"\n=== Monthly Totals for {label} ===")
    print(f"{'Month':<15}{'Total (tons)':<15}")
    print("-" * 30)
    for month, total in monthly_totals.items():
        print(f"{month:<15}{total:<15.1f}")

    print(f"\n=== Daily Values for {label} ===")
    for date, daily_data in daily_values_by_date.items():
        print(f"\nDate: {date}")
        print(f"{'ID':<10}{'Name':<30}{'Value (tons)':<15}")
        print("-" * 55)
        daily_total = daily_data.pop("daily_total", 0)
        percentage = percentages_by_date.get(date, 0)
        for mat_id, data in daily_data.items():
            print(f"{mat_id:<10}{data['name']:<30}{data['value']:<15.1f}")
        print("-" * 55)
        print(f"{'Daily Total:':<40}{daily_total:<15.1f}")
        print(f"{'Percentage:':<40}{percentage:<15.2f}")

    return {
        "daily_values": daily_values_by_date,
        "monthly_totals": dict(monthly_totals),
        "percentages": percentages_by_date,
    }


def get_dates_from_excel(file_path, sheet_name=0):
    """
    Read start_date and end_date from the Excel file.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (int/str): Sheet name or index to read from.

    Returns:
        tuple: (start_date, end_date) as strings.
    """
    # Read the Excel sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)

    # Extract start_date and end_date
    start_date = df.iloc[0, 1]  # First row, second column
    end_date = df.iloc[1, 1]  # Second row, second column

    print(f"Start Date: {start_date}, End Date: {end_date}")
    return start_date, end_date


def calculateIST():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        variables_file_path = path_variables

        start_date, end_date, version = get_dates_and_version_from_excel(variables_file_path)
        # Convert start_date and end_date to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")  # Adjust format if necessary
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

        # Format the dates to the desired format
        start_date_formatted = start_date.strftime("%d.%m.%Y")
        end_date_formatted = end_date.strftime("%d.%m.%Y")

        result1 = calculate_category_OX_others(conn, start_date_formatted, end_date_formatted)
        print("result: ", result1)


    except Exception as e:
        print(f"Error by the calculations: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise


    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    calculateIST()




