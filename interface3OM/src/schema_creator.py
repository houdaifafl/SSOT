import pyodbc
import db_config


def create_reactor_data_table(cursor):
    """
    Create a table in the database for reactor data.

    Args:
        cursor: Database cursor object.
    """
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'reactor_data')
    BEGIN
        CREATE TABLE reactor_data (
            Zeitstempel DATETIME NOT NULL,  -- Zeitstempel (Timestamp)
            ANZEIGEREAKTORINBETRIEB DECIMAL(10, 2) NULL,  -- ANZEIGE REAKTOR IN BETRIEB
            MATERIALAUFGABEREAKTOR DECIMAL(10, 2) NULL,  -- MATERIALAUFGABE REAKTOR
            FEINKOHLEDOSIERUNG DECIMAL(10, 2) NULL,  -- FEINKOHLE DOSIERUNG
            SOLLWERTFUERFLUGSTAUB DECIMAL(10, 2) NULL,  -- SOLLWERT FUER FLUGSTAUB
            FEUCHTEREGELUNG DECIMAL(10, 2) NULL,  -- FEUCHTEREGELUNG
            AustragFlugstaubmenge21B001 DECIMAL(10, 2) NULL,  -- Austrag Flugstaubmenge 21B001
            SchlackeSchlackenstich DECIMAL(10, 2) NULL  -- Schlacke Schlackenstich
        );
    END
    """

    try:
        cursor.execute(create_table_query)
        print("Table 'reactor_data' created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")



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

        create_reactor_data_table(cursor)

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