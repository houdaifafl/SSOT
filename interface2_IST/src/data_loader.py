import os
import pandas as pd
from datetime import datetime

from Interface1WT.src.calculations import get_dates_and_version_from_excel
from db_config import get_db_connection
from file_paths import path_LgrBwg, path_variables
from interface2_IST.src.CalculationIST import total_menge_for_interval, calculate_category_h_concs, calculate_category_n_concs, \
    calculate_category_RI_others, calculate_category_RE_others, calculate_category_OX_others, \
    _calculate_fluxes_concentrates



def clean_data(df):
    """
    Clean and process the DataFrame using the logic from load_dim_lagerbewegung.
    """
    # Drop unnamed and empty columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed') & (df.notna().any())]

    # Clean column names
    df.columns = df.columns.str.strip("'").str.strip()
    print("Cleaned Columns:", df.columns.tolist())

    # Restrict to expected columns
    expected_columns = [
        "BpzIdt", "MatIdt", "KeziBez", "ChgNmr", "BpdSort", "MatArt", "KstUrsache", "PrzPre",
        "LgrDW", "AbrSammler", "BucPer", "VrgGrp", "MngEinhIdt", "BucDat", "KwBezJahr_de",
        "KwBezMon_de", "LOrtIdt", "LOrtBez", "LPlzIdt", "LPlzBez", "LgrFirmIdt", "LgrFirmName",
        "MngW", "MngD", "Pb", "Ag", "Au", "Cu", "S", "Zn", "FeO", "SiO2", "CaO", "As", "Sb", "Bi", "Se", "Cl", "Cd"
    ]
    df = df[expected_columns]

    # Define column groups
    varchar_columns = [
        "BpzIdt", "MatIdt", "KeziBez", "ChgNmr", "BpdSort", "MatArt", "KstUrsache",
        "PrzPre", "LgrDW", "AbrSammler", "KwBezJahr_de", "KwBezMon_de",
        "LOrtBez", "LPlzBez", "LgrFirmName"
    ]
    int_columns = ["VrgGrp", "MngEinhIdt", "LOrtIdt", "LPlzIdt", "LgrFirmIdt"]
    date_columns = ["BucDat"]
    float_columns = ["MngW", "MngD", "Pb", "Ag", "Au", "Cu", "S", "Zn", "FeO", "SiO2", "CaO", "As", "Sb", "Bi", "Se", "Cl", "Cd"]

    # Clean VARCHAR columns
    for col in varchar_columns:
        df[col] = df[col].fillna(" ").astype(str)

    # Clean INT columns
    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(-1).astype(int)

    # Clean DATE columns
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').fillna(pd.Timestamp("1970-01-01"))

    # Clean FLOAT columns
    for col in float_columns:
        df[col] = df[col].astype(str).str.replace(",", ".").astype(float)
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        df[col] = df[col].clip(-1.79e308, 1.79e308).round(6)

    print("Sample Cleaned DataFrame Rows:\n", df.head())
    return df


def load_dim_lagerbewegung_incre():
    """
    Load the table `Lgr_Bwg` with new data from Excel files that were not already loaded in the database.
    """
    # Database connection (example: SQL Server)
    conn = get_db_connection()
    cursor = conn.cursor()

    # Define the absolute path to the folder containing the Excel files
    folder_path = path_LgrBwg

    # Get all Excel files in the folder
    excel_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.Xls')]

    # Fetch existing keys from the database
    cursor.execute("SELECT BpzIdt, BucDat FROM dim_lagerbewegung")
    existing_keys = set((str(row[0]).strip(), str(row[1]).strip()) for row in cursor.fetchall())

    new_rows_list = []
    for file_path in excel_files:
        print(f"Processing file: {file_path}")
        df = pd.read_excel(file_path, sheet_name=0, header=0, dtype=str)
        df = clean_data(df)

        df["BpzIdt"] = df["BpzIdt"].astype(str)
        df["BucDat"] = df["BucDat"].astype(str)

        # Filter new rows
        new_rows = df[~df.apply(lambda row: (row["BpzIdt"], row["BucDat"]) in existing_keys, axis=1)]
        new_rows_list.append(new_rows)

    # Combine all new rows from all files
    combined_new_rows = pd.concat(new_rows_list, ignore_index=True)

    # Insert new rows
    if not combined_new_rows.empty:
        insert_query = """
        INSERT INTO dim_lagerbewegung (
            BpzIdt, MatIdt, KeziBez, ChgNmr, BpdSort, MatArt, KstUrsache, PrzPre,
            LgrDW, AbrSammler, BucPer, VrgGrp, MngEinhIdt, BucDat, KwBezJahr_de,
            KwBezMon_de, LOrtIdt, LOrtBez, LPlzIdt, LPlzBez, LgrFirmIdt, LgrFirmName,
            MngW, MngD, Pb, Ag, Au, Cu, S, Zn, FeO, SiO2, CaO, [As], Sb, Bi, Se, Cl, Cd
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
         ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.executemany(insert_query, [tuple(row) for row in combined_new_rows.itertuples(index=False, name=None)])
        conn.commit()
        print(f"Successfully inserted {len(combined_new_rows)} new records.")

    else:
        print("No new records to insert.")

    cursor.close()
    conn.close()

def load_Report_table(conn, start_date, end_date):
    """
    Loads IST data into the Report table.

    Args:
        conn: Database connection object.
        start_date (str): Start date in the format 'YYYY-MM-DD'.
        end_date (str): End date in the format 'YYYY-MM-DD'.
    """
    from datetime import datetime

    try:
        cursor = conn.cursor()

        print(f"Loading data into Report from {start_date} to {end_date}...\n")

        # Retrieve the calculated IST_Actual values
        daily_values = total_menge_for_interval(conn, start_date, end_date)
        if not daily_values:
            print("No IST_Actual data returned. Exiting.")
            return

        # Fetch data from individual calculation methods
        main_data = calculate_category_h_concs(conn, start_date, end_date).get("percentages", {})
        side_data = calculate_category_n_concs(conn, start_date, end_date).get("percentages", {})
        intern_data = calculate_category_RI_others(conn, start_date, end_date).get("percentages", {})
        extern_data = calculate_category_RE_others(conn, start_date, end_date).get("percentages", {})
        other_second_data = calculate_category_OX_others(conn, start_date, end_date).get("percentages", {})
        fluxes_data = _calculate_fluxes_concentrates(conn, start_date, end_date).get("daily_values", {})

        # First loop for IST_Actual values
        ist_actual_dict = {}
        for entry in daily_values:
            try:
                date = entry.get('date')
                tons = entry.get('tons')
                if not date or tons is None:
                    print(f"Skipping invalid IST_Actual entry: {entry}")
                    continue

                # Format the date for IST_Actual
                formatted_date = datetime.strptime(date, "%d.%m.%Y").strftime("%Y-%m-%d")
                ist_actual_dict[formatted_date] = f"{tons} t"
            except Exception as e:
                print(f"Failed to process IST_Actual entry {entry}: {e}")

        # Second loop for other calculations
        for date in main_data.keys():
            try:
                # Use original date format for other calculations
                formatted_date = date

                # Retrieve IST_Actual value for the date
                ist_actual_value = ist_actual_dict.get(formatted_date, "0 t")

                # Prepare data for the combined table
                main = f"{main_data.get(date, '0')} %"
                side = f"{side_data.get(date, '0')} %"
                intern = f"{intern_data.get(date, '0')} %"
                extern = f"{extern_data.get(date, '0')} %"
                other_second = f"{other_second_data.get(date, '0')} %"
                fluxes = f"{fluxes_data.get(date, {}).get('daily_total', '0')} t"

                # Insert data into the Report table
                insert_query = """
                MERGE INTO Report AS target
                USING (SELECT ? AS monthday, ? AS Ist_Actual, ? AS Main, ? AS Side, ? AS Intern, ? AS Extern, ? AS OtherSecond, ? AS fluxes) AS source
                ON target.monthday = source.monthday
                WHEN MATCHED THEN
                    UPDATE SET 
                        Ist_Actual = source.Ist_Actual,
                        Main = source.Main,
                        Side = source.Side,
                        Intern = source.Intern,
                        Extern = source.Extern,
                        OtherSecond = source.OtherSecond,
                        fluxes = source.fluxes
                WHEN NOT MATCHED THEN
                    INSERT (monthday, Ist_Actual, Main, Side, Intern, Extern, OtherSecond, fluxes)
                    VALUES (source.monthday, source.Ist_Actual, source.Main, source.Side, source.Intern, source.Extern, source.OtherSecond, source.fluxes);
                """
                cursor.execute(insert_query, (formatted_date, ist_actual_value, main, side, intern, extern, other_second, fluxes))

                print(f"Inserted data for date {formatted_date}. IST_Actual: {ist_actual_value}")

            except Exception as e:
                print(f"Failed to insert data for date {date}: {e}")

        # Commit the transaction
        conn.commit()
        print("All data loaded into Report successfully.")

    except Exception as e:
        print(f"Error loading data into Report: {e}")
    finally:
        cursor.close()



def load_tables():
    conn = None
    curser = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        load_dim_lagerbewegung_incre()
        # Define the absolute path to the Variables.xlsx file
        variables_file_path = path_variables

        start_date, end_date, version = get_dates_and_version_from_excel(variables_file_path)
        # Convert start_date and end_date to datetime objects
        start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

        # Format the dates to the desired format
        start_date_formatted = start_date.strftime("%d.%m.%Y")
        end_date_formatted = end_date.strftime("%d.%m.%Y")
        #load_Report_table(conn, start_date_formatted, end_date_formatted)



        conn.commit()
        print("All tables loaded successfully.")

    except Exception as e:
        print(f"Error loading tables: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
        load_tables()