import pyodbc

def get_db_connection():
    connection_string = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=MSI\SQLEXPRESS01;"
        r"DATABASE=NyrstarDB_1;"
        r"UID=sa;"
        r"PWD=Beldi-2002100;"
        r"Trusted_Connection=no;"
    )
    connection = pyodbc.connect(connection_string)
    return connection

conn = None
try:
    conn = get_db_connection()
    print("Connection to forecast and budget DB successful!")
except pyodbc.OperationalError as e:
    print("Database connection failed:", e)
finally:
    if conn:
        conn.close()
        print("Connection closed.")