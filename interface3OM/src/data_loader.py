import os
import pandas as pd
import pyodbc
import json
from db_config import get_db_connection
from file_paths import path_Reaktordata, jsonfile_reactor


def connect_to_database():
    """
    Establish a connection to the database.
    """
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            #'SERVER=10.2.144.12,1433;'
            'SERVER=STLDB02,1433;'
            'DATABASE=master;'  # Replace with your database name
            #'UID=FHaachenP;'
            #'PWD=HEjMxRdctaAo1!!;'
            'Trusted_Connection=yes;'  # Use Windows credentials
            #'Encrypt=yes;'
            #'TrustServerCertificate = yes;'  # Use Windows credentials
        )
        print("Database connection established.")
        return connection.cursor(), connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None, None



def load_reactor_data(cursor, file_path):
    """
    Load data from an Excel file into the reactor_data table in the database.

    Args:
        cursor: Database cursor object.
        file_path: Path to the Excel file.
    """
    try:
        # Lade die Excel-Datei ab Zeile 6 (Index 5)
        df = pd.read_excel(file_path, header=None, skiprows=5)

        # Definiere die Spaltennamen korrekt
        df.columns = [
            "Zeitstempel",
            "ANZEIGEREAKTORINBETRIEB",
            "MATERIALAUFGABEREAKTOR",
            "FEINKOHLEDOSIERUNG",
            "SOLLWERTFUERFLUGSTAUB",
            "FEUCHTEREGELUNG",
            "AustragFlugstaubmenge21B001",
            "SchlackeSchlackenstich"
        ]

        # Iteriere durch die Zeilen und füge die Daten ein
        for row in df.itertuples(index=False):
            if pd.isna(row[0]):  # Überprüfe Zeitstempel
                print(f"Skipping row due to missing timestamp: {row}")
                continue

            values = [
                row[0],
                float(row[1]) if pd.notna(row[1]) else None,
                float(row[2]) if pd.notna(row[2]) else None,
                float(row[3]) if pd.notna(row[3]) else None,
                float(row[4]) if pd.notna(row[4]) else None,
                float(row[5]) if pd.notna(row[5]) else None,
                float(row[6]) if pd.notna(row[6]) else None,
                float(row[7]) if pd.notna(row[7]) else None
            ]

            insert_query = """
            INSERT INTO reactor_data (
                [Zeitstempel],
                [ANZEIGEREAKTORINBETRIEB],
                [MATERIALAUFGABEREAKTOR],
                [FEINKOHLEDOSIERUNG],
                [SOLLWERTFUERFLUGSTAUB],
                [FEUCHTEREGELUNG],
                [AustragFlugstaubmenge21B001],
                [SchlackeSchlackenstich]
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """

            try:
                cursor.execute(insert_query, values)
            except Exception as e:
                print(f"Failed to insert row {row}: {e}")

        cursor.connection.commit()
        print("Data loaded successfully into reactor_data table.")

    except Exception as e:
        print(f"Error loading data from Excel: {e}")



def load_processed_files(loaded_files_path):
    """
    Load the list of already processed files from a JSON file.
    """
    if os.path.exists(loaded_files_path):
        with open(loaded_files_path, 'r') as f:
            return set(json.load(f))
    return set()


def save_processed_files(loaded_files_path, processed_files):
    """
    Save the list of processed files to a JSON file.
    """
    with open(loaded_files_path, 'w') as f:
        json.dump(list(processed_files), f)


def process_folder(folder_path, loaded_files_path):
    """
    Process all files in the folder and update the JSON file with processed files.
    """
    # Load the list of already processed files
    processed_files = load_processed_files(loaded_files_path)

    # Establish database connection
    cursor, connection = connect_to_database()
    if not cursor or not connection:
        print("Failed to establish a database connection. Exiting...")
        return

    try:
        # Get all Excel files in the folder
        excel_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.xlsx')]

        # Process each file that hasn't been processed yet
        for file_path in excel_files:
            file_name = os.path.basename(file_path)
            if file_name not in processed_files:
                print(f"Processing new file: {file_name}")
                try:
                    load_reactor_data(cursor, file_path)  # Use the existing loader function
                    processed_files.add(file_name)  # Mark the file as processed
                except Exception as e:
                    print(f"Error loading data from {file_path}: {e}")

        # Save the updated list of processed files
        save_processed_files(loaded_files_path, processed_files)

    finally:
        # Close the database connection
        connection.close()
        print("Database connection closed.")




def load_tables():
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        folder_to_watch = path_Reaktordata
        loaded_files_path = jsonfile_reactor

        # Ensure the JSON file exists and is empty on the first run
        if not os.path.exists(loaded_files_path):
            with open(loaded_files_path, 'w') as f:
                json.dump([], f)

        print(f"Starting processing for folder: {folder_to_watch}")
        process_folder(folder_to_watch, loaded_files_path)
        print("Processing completed.")


        conn.commit()
        print("All tables loaded successfully.")

    except Exception as e:
        print(f"Error loading filtered material data: {e}")
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