from db_config import get_db_connection
from file_paths import path_variables


#from file_paths import path_variables


def calculate_total_budget_and_forecast(conn, start_date, end_date, material_name=None, material_type=None, category=None, version_name=None):
    import pandas as pd
    from pandas.tseries.offsets import MonthEnd

    try:
        # Parse the dates
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        print(f"Debug: Start Date: {start_date}, End Date: {end_date}")

        # Build and execute the material filter query
        material_filter_query = "SELECT dim_material_id FROM dim_material WHERE 1=1"
        params = []

        if material_name:
            material_filter_query += " AND material_name = ?"
            params.append(material_name)
        if material_type:
            material_filter_query += " AND material_type = ?"
            params.append(material_type)
        if category:
            material_filter_query += " AND category = ?"
            params.append(category)

        material_ids_result = pd.read_sql_query(material_filter_query, conn, params=params)
        if material_ids_result.empty:
            raise ValueError("No materials found with the specified criteria.")

        material_ids = material_ids_result['dim_material_id'].tolist()
        print(f"Debug: Material IDs: {material_ids}")

        # Modify the main query to include version_name filtering
        query = "SELECT * FROM fact_table WHERE material_id IN ({})".format(','.join(['?'] * len(material_ids)))
        if version_name:
            query += " AND version_name = ?"
            params.append(version_name)

        df = pd.read_sql_query(query, conn, params=material_ids + ([version_name] if version_name else []))
        print(f"Debug: Retrieved DataFrame shape: {df.shape}")

        # Assign rows to budget and forecast cycles
        budget_data = pd.DataFrame()
        forecast_data = pd.DataFrame()

        total_rows = len(df)
        rows_per_material = 24
        num_materials = len(material_ids)

        for material_idx in range(num_materials):
            start_idx_budget = material_idx * rows_per_material
            end_idx_budget = start_idx_budget + rows_per_material

            start_idx_forecast = total_rows // 2 + material_idx * rows_per_material
            end_idx_forecast = start_idx_forecast + rows_per_material

            # Ensure indices are within range
            if start_idx_budget < total_rows:
                budget_data = pd.concat([budget_data, df.iloc[start_idx_budget:end_idx_budget]])

            if start_idx_forecast < total_rows:
                forecast_data = pd.concat([forecast_data, df.iloc[start_idx_forecast:end_idx_forecast]])

        print(f"Debug: Budget DataFrame shape: {budget_data.shape}")
        print(f"Debug: Forecast DataFrame shape: {forecast_data.shape}")

        # Ensure 'b_' and 'f_' columns exist
        budget_columns = [col for col in budget_data.columns if col.startswith("b_")]
        forecast_columns = [col for col in forecast_data.columns if col.startswith("f_")]

        print(f"Debug: Budget Columns: {budget_columns}")
        print(f"Debug: Forecast Columns: {forecast_columns}")

        if not budget_columns:
            raise ValueError("No budget columns found in the dataset.")
        if not forecast_columns:
            raise ValueError("No forecast columns found in the dataset.")

        results = {}
        total_budget_sum = 0
        total_forecast_sum = 0

        # Generate column names for the relevant months
        relevant_months = pd.date_range(start=start_date, end=end_date, freq='MS')
        print(f"Debug: Relevant Months: {relevant_months}")

        # Process each material
        for material_id in material_ids:
            print(f"Debug: Processing Material ID: {material_id}")
            material_budget_rows = budget_data[budget_data['material_id'] == material_id]
            material_forecast_rows = forecast_data[forecast_data['material_id'] == material_id]

            print(f"Debug: Budget Rows for Material {material_id}: {material_budget_rows.shape[0]}")
            print(f"Debug: Forecast Rows for Material {material_id}: {material_forecast_rows.shape[0]}")

            total_budget = 0
            total_forecast = 0

            for month_start in relevant_months:
                month_str = month_start.strftime('%b_%y').lower()

                # Extract budget and forecast for the month
                budget_column = f"b_{month_str}"
                forecast_column = f"f_{month_str}"

                monthly_budget = (
                    material_budget_rows[budget_column].sum()
                    if budget_column in material_budget_rows.columns
                    else 0
                )
                monthly_forecast = (
                    material_forecast_rows[forecast_column].sum()
                    if forecast_column in material_forecast_rows.columns
                    else 0
                )

                print(f"Debug: Material ID {material_id}, Budget Column {budget_column}, Monthly Budget {monthly_budget}")
                print(f"Debug: Material ID {material_id}, Forecast Column {forecast_column}, Monthly Forecast {monthly_forecast}")

                days_in_month = (month_start + MonthEnd(0)).day
                budget_hourly_value = monthly_budget / (days_in_month * 24)
                forecast_hourly_value = monthly_forecast / (days_in_month * 24)

                start_of_month = max(start_date, month_start)
                month_end = month_start + MonthEnd(0)

                if month_end.month == end_date.month and month_end.year == end_date.year:
                    end_of_month = end_date
                else:
                    end_of_month = month_end.replace(hour=23, minute=59, second=59)

                interval_hours = (end_of_month - start_of_month).total_seconds() / 3600
                total_budget += interval_hours * budget_hourly_value
                total_forecast += interval_hours * forecast_hourly_value

                print(f"Debug: Material ID {material_id}, Interval Hours {interval_hours}, Budget Contribution {total_budget}, Forecast Contribution {total_forecast}")

            # Store results for the material
            results[material_id] = {
                "total_budget": total_budget,
                "total_forecast": total_forecast,
            }

            # Update the overall totals
            total_budget_sum += total_budget
            total_forecast_sum += total_forecast

        # Modified Return Logic
        conn.close()
        print(f"Debug: Final Total Budget {total_budget_sum}, Final Total Forecast {total_forecast_sum}")
        return total_budget_sum, total_forecast_sum

    except Exception as e:
        print(f"Error calculating material budget and forecast: {e}")
        return 0, 0



def retrieve_budget_and_forecast_by_material(conn):
    """
    Retrieve budget and forecast values for each material.

    Args:
        conn: Database connection object.

    Returns:
        A dictionary with material IDs as keys and their budget and forecast values as values.
        Example:
        {
            '11107': {'budget': {month: value, ...}, 'forecast': {month: value, ...}},
            ...
        }
    """
    import pandas as pd

    try:
        # Query the data
        query = "SELECT * FROM fact_table"
        df = pd.read_sql_query(query, conn)
        print(f"Debug: Retrieved DataFrame shape: {df.shape}")

        # Extract material IDs
        material_ids = df['material_id'].dropna().unique()
        print(f"Debug: Unique Material IDs: {material_ids}")

        # Identify budget and forecast columns
        budget_columns = [col for col in df.columns if col.startswith("b_")]
        forecast_columns = [col for col in df.columns if col.startswith("f_")]

        print(f"Debug: Budget Columns: {budget_columns}")
        print(f"Debug: Forecast Columns: {forecast_columns}")

        # Initialize the results dictionary
        results = {}

        # Process each material
        for material_id in material_ids:
            print(f"Debug: Processing Material ID: {material_id}")

            # Filter rows for the current material
            material_rows = df[df['material_id'] == material_id]

            # Split into budget and forecast cycles
            first_row_idx = material_rows.index[0]
            last_row_idx = material_rows.index[-1]
            num_rows = len(material_rows)

            # Determine split point for cycles
            split_idx = (num_rows // 2) + first_row_idx

            # Extract budget and forecast rows
            budget_rows = material_rows.loc[first_row_idx:split_idx - 1]
            forecast_rows = material_rows.loc[split_idx:last_row_idx]

            # Aggregate budget and forecast values
            budget_values = budget_rows[budget_columns].sum(axis=0).to_dict()
            forecast_values = forecast_rows[forecast_columns].sum(axis=0).to_dict()

            # Store in results
            results[str(material_id)] = {
                "budget": budget_values,
                "forecast": forecast_values,
            }

            print(f"Debug: Material ID {material_id}, Budget Values: {budget_values}")
            print(f"Debug: Material ID {material_id}, Forecast Values: {forecast_values}")

        return results

    except Exception as e:
        print(f"Error retrieving budget and forecast by material: {e}")
        return {}


def get_shutdown_dates(conn, start_date, end_date, version_name=None):
    """
    Retrieve the dates with shutdown hours for both budget and forecast from the fact_table
    and link them to the dim_time table within a specific date range, filtered by version name.

    Args:
        conn: Database connection object.
        start_date: Start date of the date range (inclusive).
        end_date: End date of the date range (inclusive).
        version_name: Specific version of the data to filter by.

    Returns:
        A DataFrame containing the shutdown dates and their respective shutdown hours for budget and forecast.
    """
    import pandas as pd

    try:
        # Base query to fetch relevant data from fact_table and dim_time
        query = """
            SELECT 
                ft.bdgt_shutdown_hours AS budget_shutdown_hours,
                ft.fcst_shutdown_hours AS forecast_shutdown_hours,
                dt.year,
                dt.month,
                dt.day,
                ft.version_name
            FROM 
                fact_table ft
            INNER JOIN 
                dim_time dt
            ON 
                ft.time_id = dt.dim_time_id
            WHERE 
                (ft.bdgt_shutdown_hours > 0 OR ft.fcst_shutdown_hours > 0)
        """

        # Execute the query and fetch data
        shutdown_data = pd.read_sql_query(query, conn)

        # Create a new column with the full date
        shutdown_data['date'] = pd.to_datetime(shutdown_data[['year', 'month', 'day']])

        # Filter shutdown data within the specified date range
        shutdown_data = shutdown_data[
            (shutdown_data['date'] >= pd.to_datetime(start_date)) &
            (shutdown_data['date'] <= pd.to_datetime(end_date))
        ]

        # If version_name is provided, filter by version_name
        if version_name:
            shutdown_data = shutdown_data[shutdown_data['version_name'] == version_name]

        # Replace NaN values with 0 for shutdown hours
        shutdown_data['budget_shutdown_hours'] = shutdown_data['budget_shutdown_hours'].fillna(0)
        shutdown_data['forecast_shutdown_hours'] = shutdown_data['forecast_shutdown_hours'].fillna(0)

        # Select and reorder the relevant columns
        result = shutdown_data[['date', 'budget_shutdown_hours', 'forecast_shutdown_hours']]

        # Debugging output
        print("Debug: Filtered Shutdown Data within Date Range:")
        print(result)

        return result

    except Exception as e:
        print(f"Error retrieving shutdown dates: {e}")
        return None



def calculate_with_shutdown_from_db(material_type=None, start_date=None, end_date=None, material_name=None, category=None, version_name=None):
    """
    Calculate daily budget and forecast values considering shutdown hours.

    Args:
        material_type: Filter by material type.
        start_date: Start date of the calculation period.
        end_date: End date of the calculation period.
        material_name: Specific material name.
        category: Specific category.
        version_name: Specific version of the data to use.

    Returns:
        List of daily values with budget and forecast.
    """
    import pandas as pd

    conn = get_db_connection()

    try:
        print(f"Starting calculation for material_type={material_type}, start_date={start_date}, end_date={end_date}, material_name={material_name}, category={category}, version_name={version_name}")

        # Retrieve shutdown hours for the date range and version
        shutdown_data = get_shutdown_dates(conn, start_date, end_date, version_name=version_name)
        if shutdown_data is None or shutdown_data.empty:
            raise ValueError("No shutdown data found for the specified date range and version.")

        # Replace NaN shutdown hours with 0
        shutdown_data['budget_shutdown_hours'] = shutdown_data['budget_shutdown_hours'].fillna(0)
        shutdown_data['forecast_shutdown_hours'] = shutdown_data['forecast_shutdown_hours'].fillna(0)

        # Debug: Display filtered shutdown data
        print("Debug: Filtered Shutdown Data within Date Range:")
        print(shutdown_data)

        # Sum shutdown hours within the date range
        total_budget_shutdown_hours = shutdown_data['budget_shutdown_hours'].sum()
        total_forecast_shutdown_hours = shutdown_data['forecast_shutdown_hours'].sum()

        # Debug: Display summed shutdown hours
        print(f"Total budget shutdown hours in date range: {total_budget_shutdown_hours}")
        print(f"Total forecast shutdown hours in date range: {total_forecast_shutdown_hours}")

        # Call calculate_total_budget_and_forecast to get the total monthly sums
        print("Calculating monthly totals using calculate_total_budget_and_forecast...")
        monthly_totals = calculate_total_budget_and_forecast(
            conn=conn,
            start_date=start_date,
            end_date=end_date,
            material_name=material_name,
            material_type=material_type,
            category=category,
            version_name=version_name
        )

        # Unpack the tuple returned by calculate_total_budget_and_forecast
        budget_total, forecast_total = monthly_totals

        # Round the totals to the nearest integer
        budget_total = round(budget_total,3)
        forecast_total = round(forecast_total,3)

        print(f"Budget total: {budget_total}")
        print(f"Forecast total: {forecast_total}")

        if budget_total == 0 and forecast_total == 0:
            raise ValueError("Both budget and forecast totals are invalid; cannot proceed.")

        # Get the total hours in the interval
        total_hours_in_interval = ((pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1) * 24
        print(f"Total hours in interval: {total_hours_in_interval}")

        # Calculate hourly values with shutdown
        hourly_budget_with_shutdown = (
            round(budget_total / max(total_hours_in_interval - total_budget_shutdown_hours, 3), 3)
            if budget_total > 0 else 0
        )
        hourly_forecast_with_shutdown = (
            round(forecast_total / max(total_hours_in_interval - total_forecast_shutdown_hours, 3), 3)
            if forecast_total > 0 else 0
        )

        print(f"Budget hourly value with shutdown: {hourly_budget_with_shutdown}")
        print(f"Forecast hourly value with shutdown: {hourly_forecast_with_shutdown}")

        # Daily values
        daily_values = []

        # Iterate through each day in the interval
        print("Calculating daily values with shutdown hours...")
        for single_date in pd.date_range(start=start_date, end=end_date):
            # Initialize daily values
            daily_budget_value = 0
            daily_forecast_value = 0

            # Budget calculations
            budget_shutdown_hours = shutdown_data.loc[shutdown_data['date'] == single_date, 'budget_shutdown_hours'].sum()
            hours_worked_budget = max(24 - budget_shutdown_hours, 0)
            if budget_total > 0:
                daily_budget_value = round((hours_worked_budget * hourly_budget_with_shutdown), 3)

            # Forecast calculations
            forecast_shutdown_hours = shutdown_data.loc[shutdown_data['date'] == single_date, 'forecast_shutdown_hours'].sum()
            hours_worked_forecast = max(24 - forecast_shutdown_hours, 0)
            if forecast_total > 0:
                daily_forecast_value = round((hours_worked_forecast * hourly_forecast_with_shutdown), 3)

            print(f"Daily budget value for {single_date.strftime('%Y-%m-%d')}: {daily_budget_value}")
            print(f"Daily forecast value for {single_date.strftime('%Y-%m-%d')}: {daily_forecast_value}")

            # Append daily values
            daily_values.append({
                'date': single_date.strftime('%Y-%m-%d'),
                'budget': daily_budget_value,
                'forecast': daily_forecast_value
            })

        print("Daily values calculated successfully.")

    except Exception as e:
        print(f"Error during calculation: {e}")
        raise
    finally:
        if conn and not conn.closed:
            conn.close()
        print("Database connection closed.")

    return daily_values


def get_dates_and_version_from_excel(file_path, sheet_name=0):
    """
    Read start_date, end_date, and version_name from the Excel file.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (int/str): Sheet name or index to read from.

    Returns:
        tuple: (start_date, end_date, version_name) as strings.
    """
    import pandas as pd

    # Read the Excel sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, dtype=str)

    # Extract start_date, end_date, and version_name
    start_date = df.iloc[0, 1]  # First row, second column
    end_date = df.iloc[1, 1]  # Second row, second column
    version_name = df.iloc[2, 1]  # Third row, second column

    print(f"Start Date: {start_date}, End Date: {end_date}, Version Name: {version_name}")
    return start_date, end_date, version_name


def calculate():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        import os

        # Path to the Variables.xlsx file
        variables_file_path = path_variables

        # Retrieve start and end dates from the Excel file
        start_date, end_date, version_name = get_dates_and_version_from_excel(variables_file_path)
        # result = calculate_total_budget_and_forecast(conn, start_date, end_date, version_name)
        # print("total:", result)

        # result1 = get_shutdown_dates(conn, start_date, end_date, version_name="for_ver_2")
        # print("total:", result1)

        result2 = calculate_with_shutdown_from_db(None, start_date, end_date,material_name="MISSOURI DOE RUN", version_name=version_name)
        print("result2:", result2)

        # values =retrieve_budget_and_forecast_by_material(conn)
        # print(values)
        # shutdown = get_shutdown_dates(conn)
        # print('days: ', shutdown)

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
   calculate()
