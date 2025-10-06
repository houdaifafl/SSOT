import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import os
from Interface1WT.src.calculations import calculate_with_shutdown_from_db
from db_config import get_db_connection
from interface2_IST.src.CalculationIST import (
    total_menge_for_interval,
    calculate_category_h_concs,
    calculate_category_n_concs,
    calculate_category_RI_others,
    calculate_category_RE_others,
    calculate_category_OX_others,
    _calculate_fluxes_concentrates,
    _calculate_recirculate_concentrates,
    _calculate_all_materials,
    calculate_all_concs,
    calculate_category_all_others,
    calculate_category_PK_concs,
    _calculate_pastes,
    calculate_category_P_pastes, _calculate_al_materials_and_name, calculate_category_OX_others_category,
)

def combined_export_to_excel(conn, start_date, end_date, version_name):
    """
    Combines the outputs of both methods into a single table and exports to an Excel file.
    """
    try:
        print(f"Starting data export from {start_date} to {end_date}...\n")

        # Adjust the start_date and end_date for budget and forecast
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Adjust to the full month range
        full_month_start_date = pd.to_datetime(start_date).replace(day=1).strftime("%Y-%m-%d 00:00")
        full_month_end_date = (
                pd.to_datetime(start_date).replace(day=1) + pd.offsets.MonthEnd(0)
        ).strftime("%Y-%m-%d 23:59")


        print(f"Debug: Adjusted Start Date: {start_date_str}, Adjusted End Date: {end_date_str}\n")
        print(f"Debug: Full Month Range: {full_month_start_date} to {full_month_end_date}\n")

        # Step 1: Process Budget and Forecast
        print("Calculating budget and forecast data...")
        daily_budget_forecast = calculate_with_shutdown_from_db(
            material_type=None,
            start_date=full_month_start_date,
            end_date=full_month_end_date,
            material_name=None,
            category=None,
            version_name=version_name
        )

        # Filter results to the desired date range
        budget_forecast_data = []
        if daily_budget_forecast:
            for row in daily_budget_forecast:
                try:
                    formatted_date = pd.to_datetime(row['date'], format="%Y-%m-%d", errors='coerce')
                    if start_date <= formatted_date <= end_date:  # Only include the desired date range
                        budget_forecast_data.append({
                            'Day': formatted_date.strftime("%Y-%m-%d"),
                            'Budget in t': f"{round(row['budget'])}",
                            'Forecast in t': f"{round(row['forecast'])}",
                        })
                except Exception as e:
                    print(f"Error processing budget/forecast row: {row} - {e}")

        # Assuming the strings are in "YYYY-MM-DD" format
        start_date_formatted = datetime.strptime(full_month_start_date, "%Y-%m-%d %H:%S").strftime("%d.%m.%Y")
        end_date_formatted = datetime.strptime(full_month_end_date, "%Y-%m-%d %H:%S").strftime("%d.%m.%Y")

        # Step 2: Prepare Data for Additional Attributes
        daily_values = total_menge_for_interval(conn, start_date_formatted, end_date_formatted)
        ist_actual_dict = {}
        if daily_values:
            for entry in daily_values:
                try:
                    date = entry.get('date')
                    tons = entry.get('tons')
                    if not date or tons is None:
                        continue
                    formatted_date = pd.to_datetime(date, format="%d.%m.%Y", errors='coerce').strftime("%Y-%m-%d")
                    if start_date_str <= formatted_date <= end_date_str:
                        ist_actual_dict[formatted_date] = tons
                except Exception as e:
                    print(f"Failed to process IST_Actual entry {entry}: {e}")

        main_data = calculate_category_h_concs(conn, start_date_formatted, end_date_formatted).get("percentages", {})
        side_data = calculate_category_n_concs(conn, start_date_formatted, end_date_formatted).get("percentages", {})
        intern_data = calculate_category_RI_others(conn, start_date_formatted, end_date_formatted).get("percentages",
                                                                                                       {})
        extern_data = calculate_category_RE_others(conn, start_date_formatted, end_date_formatted).get("percentages",
                                                                                                       {})
        other_second_data = calculate_category_OX_others(conn, start_date_formatted, end_date_formatted).get(
            "percentages", {})
        fluxes_data = _calculate_fluxes_concentrates(conn, start_date_formatted, end_date_formatted).get("daily_values",
                                                                                                         {})
        recirculate_data = _calculate_recirculate_concentrates(conn, start_date_formatted, end_date_formatted).get(
            "daily_values", {})


        # Query reactor data
        print("Fetching reactor data...")
        query = f"""
                    SELECT * 
                    FROM reactor_data
                    WHERE CONVERT(DATE, Zeitstempel) BETWEEN '{full_month_start_date}' AND '{(pd.to_datetime(full_month_end_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")}'
        """
        df = pd.read_sql(query, conn)

        combined_data = []

        if not df.empty:
            df['Zeitstempel'] = pd.to_datetime(df['Zeitstempel'])
            df['DayGroup'] = df['Zeitstempel'] - pd.to_timedelta(
                (df['Zeitstempel'].dt.hour < 6).astype(int), unit='day'
            )
            df['DayGroup'] = df['DayGroup'].dt.strftime("%Y-%m-%d")

            grouped = df.groupby('DayGroup')
            for name, group in grouped:
                try:
                    day_str = name
                    if not (start_date_str <= day_str <= end_date_str):  # Skip dates outside desired range
                        continue

                    availability = round((group['ANZEIGEREAKTORINBETRIEB'].sum() / 24) * 100, 2)
                    avg_feed = round(group['MATERIALAUFGABEREAKTOR'].sum() / 24, 2)
                    sollwert_fuer_flugstaub = group['SOLLWERTFUERFLUGSTAUB'].sum()
                    austrag_flugstaubmenge = group['AustragFlugstaubmenge21B001'].sum()
                    kreisläufer = sollwert_fuer_flugstaub + austrag_flugstaubmenge
                    recirculate_value = kreisläufer + group['FEINKOHLEDOSIERUNG'].sum() + float(
                        recirculate_data.get(day_str, {}).get('daily_total', 0))
                    pb_in_slag = round(group['SchlackeSchlackenstich'].sum() / 24, 2)

                    ist_actual_value = ist_actual_dict.get(day_str, 0)
                    main = main_data.get(day_str, 0)
                    side = side_data.get(day_str, 0)
                    intern = intern_data.get(day_str, 0)
                    extern = extern_data.get(day_str, 0)
                    other_second = other_second_data.get(day_str, 0)
                    fluxes = fluxes_data.get(day_str, {}).get('daily_total', 0)

                    combined_data.append({
                        'Day': day_str,
                        'Forecast in t': None,
                        'Budget in t': None,
                        'Actual in t': f"{round(ist_actual_value, 2)}",
                        'Concentrates Main': f"{round(main, 2)} %",
                        'Concentrates Side': f"{round(side, 1)} %",
                        'Leach Products Intern': f"{round(intern, 1)} %",
                        'Leach Products Extern': f"{round(extern, 1)} %",
                        'Other Secondary': f"{round(other_second, 1)} %",
                        'Non Raw Materials Recirculates in t/d': f"{round(recirculate_value)}",
                        'Non Raw Materials Fluxes in t/d': f"{round(fluxes)}",
                        'Availability': f"{round(availability, 1)} %",
                        'Average Feed in t/h': f"{round(avg_feed)}",
                        'Average Pb in Slag': f"{round(pb_in_slag, 1)} %"
                    })
                except Exception as e:
                    print(f"Error processing group {name}: {e}")

        # Step 3: Merge Budget/Forecast with Combined Data
        print("Merging budget and forecast data...")
        for row in budget_forecast_data:
            matched_row = next((item for item in combined_data if item['Day'] == row['Day']), None)
            if matched_row:
                matched_row.update({'Budget in t': row['Budget in t'], 'Forecast in t': row['Forecast in t']})
            else:
                combined_data.append({
                    'Day': row['Day'],
                    'Forecast in t': row['Forecast in t'],
                    'Budget in t': row['Budget in t'],
                    'Actual in t': "0",
                    'Concentrates Main': "0 %",
                    'Concentrates Side': "0 %",
                    'Leach Products Intern': "0 %",
                    'Leach Products Extern': "0 %",
                    'Other Secondary': "0 %",
                    'Non Raw Materials Recirculates in t/d': "0",
                    'Non Raw Materials Fluxes in t/d': "0",
                    'Availability': "0 %",
                    'Average Feed in t/h': "0",
                    'Average Pb in Slag': "0 %"
                })

        # Reorder attributes
        adjusted_data = []
        for row in combined_data:
            adjusted_data.append({
                'Day': row['Day'],
                'Forecast in t': row['Forecast in t'],
                'Budget in t': row['Budget in t'],
                'Actual in t': row['Actual in t'],
                'Concentrates Main': row['Concentrates Main'],
                'Concentrates Side': row['Concentrates Side'],
                'Leach Products Intern': row['Leach Products Intern'],
                'Leach Products Extern': row['Leach Products Extern'],
                'Other Secondary': row['Other Secondary'],
                'Non Raw Materials Recirculates in t/d': row['Non Raw Materials Recirculates in t/d'],
                'Non Raw Materials Fluxes in t/d': row['Non Raw Materials Fluxes in t/d'],
                'Availability': row['Availability'],
                'Average Feed in t/h': row['Average Feed in t/h'],
                'Average Pb in Slag': row['Average Pb in Slag']
            })

        combined_df = pd.DataFrame(adjusted_data)
        combined_df['Day'] = pd.to_datetime(combined_df['Day'], format="%Y-%m-%d", errors='coerce')
        combined_df.sort_values('Day', inplace=True)
        combined_df['Day'] = combined_df['Day'].dt.strftime("%Y-%m-%d")

        return combined_df

    except Exception as e:
        print(f"Error in combined export: {e}")

def summarized_report(start_date, end_date, version_name):
    """
    Sums daily budget, forecast, and Ist values for all materials, material types, and categories,
    and exports the result to a single Excel file.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        output_folder (str): Folder path for the generated Excel file.
        version_name (str): Data version.
    """
    try:
        print(f"Starting sum calculation from {start_date} to {end_date}...\n")

        # Define filters for all materials, material types, and categories
        filters = [
            {"label": "Paid Raw Materials", "material_type": None, "category": None,
             "ist_method": _calculate_all_materials},
            {"label": "Delta Concentrates", "material_type": "Cons", "category": None,
             "ist_method": calculate_all_concs},
            {"label": "Delta Paste", "material_type": None, "category": "P", "ist_method": _calculate_pastes},
            {"label": "Delta Others", "material_type": "Others", "category": None,
             "ist_method": calculate_category_all_others},
            #{"label": "Other Secondary", "material_type": None, "category": None,
            # "ist_method": calculate_category_OX_others},
            {"label": "Category H", "material_type": None, "category": "H", "ist_method": calculate_category_h_concs},
            {"label": "Category N", "material_type": None, "category": "N", "ist_method": calculate_category_n_concs},
            {"label": "Category PK", "material_type": None, "category": "PK",
             "ist_method": calculate_category_PK_concs},
            {"label": "Category P", "material_type": None, "category": "P", "ist_method": calculate_category_P_pastes},
            {"label": "Category Ox", "material_type": None, "category": "Ox",
             "ist_method": calculate_category_OX_others_category},
            {"label": "Category RI", "material_type": None, "category": "RI",
             "ist_method": calculate_category_RI_others},
            {"label": "Category RE", "material_type": None, "category": "RE",
             "ist_method": calculate_category_RE_others}
        ]

        results = []

        # Establish database connection for Ist methods
        conn = get_db_connection()
        if not conn:
            raise ValueError("Failed to establish database connection for Ist methods.")

        # Calculate the start and end dates of the entire month
        month_start = pd.to_datetime(start_date).replace(day=1).strftime("%Y-%m-%d 00:00")
        month_end = (
                pd.to_datetime(start_date).replace(day=1) + pd.offsets.MonthEnd(0)
        ).strftime("%Y-%m-%d 23:59")

        for f in filters:
            try:
                # Call the method to calculate daily budget and forecast values
                daily_values = calculate_with_shutdown_from_db(
                    material_type=f["material_type"],
                    start_date=month_start,
                    end_date=month_end,
                    material_name=None,
                    category=f["category"],
                    version_name=version_name,
                )

                # Filter daily_values based on the user-specified interval
                total_budget = 0
                total_forecast = 0
                if daily_values:
                    df = pd.DataFrame(daily_values)
                    df["date"] = pd.to_datetime(df["date"])
                    filtered_df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]
                    total_budget = filtered_df["budget"].sum()
                    total_forecast = filtered_df["forecast"].sum()

                # Format the dates for "Ist-values" methods
                formatted_start_date = pd.to_datetime(start_date).strftime("%d.%m.%Y")
                formatted_end_date = pd.to_datetime(end_date).strftime("%d.%m.%Y")

                # Handle Ist-values
                ist_total = 0
                if f["ist_method"]:
                    print(
                        f"Calling Ist method for filter: {f['label']} with dates {formatted_start_date} to {formatted_end_date}")
                    ist_daily_values = f["ist_method"](conn, formatted_start_date, formatted_end_date)

                    # Debugging: Check the return value
                    print(f"Ist method raw return for filter {f['label']}: {ist_daily_values}")

                    if isinstance(ist_daily_values, dict):
                        # Iterate over the daily values and sum them correctly
                        flattened_values = []
                        for date, materials in ist_daily_values["daily_values"].items():
                            for material_id, material_data in materials.items():
                                if isinstance(material_data, dict) and "value" in material_data:
                                    flattened_values.append(material_data["value"])

                        # Sum the flattened values directly
                        ist_total = sum(flattened_values)

                        # Debugging: Check the summed total
                        print(f"Summed Ist total for filter {f['label']}: {ist_total}")

                print(
                    f"Filter: {f['label']} - Total Budget: {total_budget}, Total Forecast: {total_forecast}, Total Ist: {ist_total}"
                )

                # Append results
                results.append(
                    {
                        "Filter": f["label"],
                        "Total Budget": total_budget,
                        "Total Forecast": total_forecast,
                        "Total Ist": ist_total,
                    }
                )

            except Exception as filter_error:
                print(f"Error processing filter {f['label']}: {filter_error}")
                results.append(
                    {"Filter": f["label"], "Total Budget": 0, "Total Forecast": 0, "Total Ist": 0}
                )

        # Create a DataFrame for the combined results
        combined_df = pd.DataFrame(results)
        return  combined_df

    except Exception as e:
        print(f"An error occurred during the calculation: {e}")



def fetch_available_versions():
    """
    Fetch all unique version names from the database.
    """
    try:
        conn = get_db_connection()
        query = "SELECT DISTINCT version_name FROM fact_table"
        versions = pd.read_sql(query, conn)['version_name'].tolist()
        conn.close()
        return versions
    except Exception as e:
        print(f"Fehler beim Abrufen der verfügbaren Versionen: {e}")
        return []

def create_material_report(start_date, end_date, version_name):
    """
    Creates an Excel sheet for all materials containing budget, forecast, and Ist values.

    Args:
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        version_name (str): Data version for budget and forecast calculations.
    """
    try:
        print(f"Starting data export from {start_date} to {end_date}...\n")

        # Adjust the start_date and end_date for budget and forecast
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)

        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Adjust to the full month range
        full_month_start_date = pd.to_datetime(start_date).replace(day=1).strftime("%Y-%m-%d 00:00")
        full_month_end_date = (
                pd.to_datetime(start_date).replace(day=1) + pd.offsets.MonthEnd(0)
        ).strftime("%Y-%m-%d 23:59")

        print(f"Debug: Adjusted Start Date: {start_date_str}, Adjusted End Date: {end_date_str}\n")
        print(f"Debug: Full Month Range: {full_month_start_date} to {full_month_end_date}\n")

        # Get all materials
        conn = get_db_connection()
        material_query = "SELECT dim_material_id, material_name, material_type, category FROM dbo.dim_material"
        materials = pd.read_sql_query(material_query, conn)

        report_data = []
        for _, material in materials.iterrows():
            try:
                material_id = material['dim_material_id']
                material_name = material['material_name']
                material_type = material['material_type']
                category = material['category']

                print(f"Processing material: {material_name} (ID: {material_id})")

                # Initialize variables to hold budget/forecast data
                budget_forecast_data = []

                # Fetch budget and forecast data
                try:
                    daily_budget_forecast = calculate_with_shutdown_from_db(
                        start_date=full_month_start_date,
                        end_date=full_month_end_date,
                        material_name=material_name,
                        version_name=version_name
                    )

                    # Filter results to the desired date range
                    if daily_budget_forecast:
                        for row in daily_budget_forecast:
                            try:
                                if row.get('date') is None:
                                    continue
                                formatted_date = pd.to_datetime(row['date'], format="%Y-%m-%d", errors='coerce')
                                if pd.isnull(formatted_date) or not (start_date <= formatted_date <= end_date):
                                    continue

                                budget = row.get('budget', 0)  # Default to 0 if null
                                forecast = row.get('forecast', 0)  # Default to 0 if null
                                budget_forecast_data.append({
                                    'Day': formatted_date.strftime("%Y-%m-%d"),
                                    'Budget': budget if budget else 0,  # Handle null values
                                    'Forecast': forecast if forecast else 0  # Handle null values
                                })
                            except Exception as e:
                                print(f"Error processing budget/forecast row for {material_name}: {e}")
                except Exception as e:
                    print(f"Error processing budget/forecast data for {material_name}: {e}")

                # Fetch Ist values

                ist_data = {}
                try:
                    # Assuming the strings are in "YYYY-MM-DD" format
                    start_date_formatted = datetime.strptime(full_month_start_date, "%Y-%m-%d %H:%S").strftime(
                        "%d.%m.%Y")
                    end_date_formatted = datetime.strptime(full_month_end_date, "%Y-%m-%d %H:%S").strftime("%d.%m.%Y")

                    ist_data = _calculate_al_materials_and_name(conn, start_date_formatted, end_date_formatted,
                                                                material_name)
                except Exception as e:
                    print(f"Error processing Ist values for {material_name}: {e}")

                # Combine data
                days_in_interval = pd.date_range(start=start_date, end=end_date).strftime("%Y-%m-%d")
                for day in days_in_interval:
                    try:
                        budget_entry = next((entry for entry in budget_forecast_data if entry['Day'] == day), None)
                        budget = budget_entry['Budget'] if budget_entry else 0
                        forecast = budget_entry['Forecast'] if budget_entry else 0

                        # Extract Ist value for the specific material from the nested structure
                        ist_value = 0  # Default to 0 if not found
                        daily_data = ist_data.get("daily_values", {}).get(day, {})
                        for mat_id, mat_data in daily_data.items():
                            if mat_data.get("name") == material_name:
                                ist_value = mat_data.get("value", 0)
                                break  # Stop searching once the material is found

                        report_data.append({
                            "Date": day,
                            "Material Name": material_name,
                            "Material ID": material_id,
                            "Material Type": material_type,
                            "Category": category,
                            "Budget": budget,
                            "Forecast": forecast,
                            "Ist": ist_value
                        })
                    except Exception as e:
                        print(f"Error combining data for {material_name} on {day}: {e}")
                        continue

            except Exception as e:
                print(f"Error processing material {material_name}: {e}")
                continue  # Skip to the next material if an error occurs

        # Convert to DataFrame
        report_df = pd.DataFrame(report_data)
        return report_df

    except Exception as e:
        print(f"An error occurred during report generation: {e}")

def combined_report(start_date, end_date, version_name, output_file):
    """
    Generates a single Excel file with multiple sheets: Combined Report, Summarized Report, and Material Report.
    """
    try:
        #connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=10.2.144.12,1433;Database=master;UID=FHaachenP;PWD=HEjMxRdctaAo1!!;"

        # Sheet 1: Combined Export Report
        combined_export_df = combined_export_to_excel(get_db_connection(), start_date, end_date,
                                                      version_name)

        # Sheet 2: Summarized Report
        summarized_df = summarized_report(start_date, end_date, version_name)

        # Sheet 3: Material values
        material_df = create_material_report(start_date, end_date, version_name)

        # Save to Excel
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

            # Combined Export Sheet
            if not combined_export_df.empty:
                material_metadata = pd.DataFrame({
                    "Description": ["Start Date:", "End Date:", "Version Name:"],
                    "Value": [start_date, end_date, version_name]
                })
                material_metadata.to_excel(writer, sheet_name="Report", index=False, startrow=0)
                combined_export_df.to_excel(writer, sheet_name="Report", index=False, startrow=5)
            else:
                print("Combined Report is empty.")

            # Summarized Report Sheet
            if not summarized_df.empty:
                material_metadata = pd.DataFrame({
                    "Description": ["Start Date:", "End Date:", "Version Name:"],
                    "Value": [start_date, end_date, version_name]
                })
                material_metadata.to_excel(writer, sheet_name="Category Sums", index=False, startrow=0)
                summarized_df.to_excel(writer, sheet_name="Category Sums", index=False, startrow=5)
            else:
                print("Summarized Report is empty.")


            if not material_df.empty:
                material_metadata = pd.DataFrame({
                    "Description": ["Start Date:", "End Date:", "Version Name:"],
                    "Value": [start_date, end_date, version_name]
                })
                material_metadata.to_excel(writer, sheet_name="Report per Material", index=False, startrow=0)
                material_df.to_excel(writer, sheet_name= "Report per Material", index=False, startrow=5)
            else:
                print("Material Report is empty.")



        print(f"Excel report generated successfully at {output_file}")

    except Exception as e:
        print(f"An error occurred during report generation: {e}")


if __name__ == "__main__":
    import re

    def validate_date_format(date_str):
        # Match format DD.MM.YYYY
        return re.match(r"\d{2}\.\d{2}\.\d{4}", date_str)

    print("Willkommen zum Report-Generator\n")

    try:
        # Ask user for input
        start_date_raw = input("Bitte Startdatum eingeben (Format: DD.MM.YYYY): ").strip()
        end_date_raw = input("Bitte Enddatum eingeben (Format: DD.MM.YYYY): ").strip()

        if not validate_date_format(start_date_raw):
            raise ValueError("Ungültiges Startdatum-Format. Bitte DD.MM.YYYY verwenden.")
        if not validate_date_format(end_date_raw):
            raise ValueError("Ungültiges Enddatum-Format. Bitte DD.MM.YYYY verwenden.")

        print("\nVerfügbare Versionen werden abgerufen...\n")
        versions = fetch_available_versions()
        if not versions:
            raise ValueError("Keine Versionen gefunden.")

        print("Verfügbare Versionen:")
        for i, version in enumerate(versions, 1):
            print(f"{i}. {version}")

        version_name = input("\nBitte geben Sie den gewünschten Version-Namen ein: ").strip()
        if version_name not in versions:
            raise ValueError("Ungültige Version eingegeben. Bitte wählen Sie eine gültige Version aus der Liste.")

        # Convert date strings
        start_date_parsed = pd.to_datetime(start_date_raw, format="%d.%m.%Y")
        end_date_parsed = pd.to_datetime(end_date_raw, format="%d.%m.%Y")

        start_date = start_date_parsed.strftime("%Y-%m-%d") + " 00:00"
        end_date = end_date_parsed.strftime("%Y-%m-%d") + " 23:59"

        # Excel file name
        start_excel = start_date_parsed.strftime("%Y-%m-%d")
        end_excel = end_date_parsed.strftime("%Y-%m-%d")


        # --- Report directory setup ---

        # Detect project root depending on execution context
        def get_project_root():
            if getattr(sys, 'frozen', False):
                # Running as .exe → use folder where the exe is
                return Path(sys.executable).parent
            else:
                # Running as script → use script’s folder
                return Path(__file__).resolve().parent.parent


        PROJECT_ROOT = get_project_root()

        # Default: Report folder next to project root (works on server & laptop)
        default_report_dir = PROJECT_ROOT / "Report"

        # Allow override with environment variable REPORT_DIR
        REPORT_DIR = Path(os.getenv("REPORT_DIR", default_report_dir))

        # Ensure directory exists
        REPORT_DIR.mkdir(parents=True, exist_ok=True)

        # Final output file
        output_file = REPORT_DIR / f"Report_{start_excel}_{end_excel}.xlsx"

        # Call report generation
        combined_report(start_date, end_date, version_name, output_file)

    except Exception as e:
        print(f"Ein Fehler ist aufgetreten: {e}")
