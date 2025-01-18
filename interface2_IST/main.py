import os
from db_config import get_db_connection
from src.CalculationIST import calculateIST, get_dates_from_excel
from src.ScheduledLoading import schedule_job, load_dim_lagerbewegung_incremental
from src.data_loader import load_tables
from src.schema_creator import create_tables


def main():
    try:
        conn = get_db_connection()
        print("Connected to the database!")

        # Call create_tables function
        #create_tables()
        #load_tables()

        #schedule_job(load_dim_lagerbewegung_incremental, run_time="15:59")
        #calculateIST()

        #Test query to check if connection is working
        cursor = conn.cursor()

        cursor.execute("SELECT 1")

        print("Test query executed successfully!")
        conn.close()


    except Exception as e:
        print("Error connecting to the database:", e)

if __name__ == "__main__":
    main()