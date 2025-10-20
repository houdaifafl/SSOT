import os
import sys

from db_config import get_db_connection

# Pfad zum aktuellen Verzeichnis oder Hauptverzeichnis hinzufügen
#sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.schema_creator import create_tables
from src.data_loader import  load_tables


def main():
    try:
        conn = get_db_connection()
        print("Connected to the database!")

        # Call create_tables function
        create_tables()

        load_tables()
        #Test query to check if connection is working
        cursor = conn.cursor()

        cursor.execute("SELECT 1")
        print("Test query executed successfully!")
        conn.close()

    except Exception as e:
        print("Error connecting to the database:", e)

if __name__ == "__main__":
    main()