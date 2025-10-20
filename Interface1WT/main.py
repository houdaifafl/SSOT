from db_config import get_db_connection
from src.calculations import calculate
from src.schema_creator import create_tables
from src.data_loader import load_tables


def main():
    """
    Main function to test the connection to the database
    """
    try:
        conn = get_db_connection()
        print("Connected to the database!")

        # Call create_tables function
        create_tables()

        load_tables()

        # Test query to check if connection is working
        cursor = conn.cursor()

        cursor.execute("SELECT 1")
        print("Test query executed successfully!")
        conn.close()

    except Exception as e:
        print("Error connecting to the database:", e)


if __name__ == "__main__":
    main()
