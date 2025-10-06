import pyodbc

def get_db_connection():
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        #'SERVER=10.2.144.12,1433;'
        'SERVER=STLDB02.saturnus.ads,1433;'
        'DATABASE=master;'  # Replace with your database name
        'UID=FHaachenP;'
        'PWD=HEjMxRdctaAo1!!;'
        #'Trusted_Connection=no;'  # Use Windows credentials
     	'Trusted_Connection=yes;'
 	    'Encrypt=yes;'
        'TrustServerCertificate=yes;'  # Use Windows credentials
    )
    return connection

conn = None
try:
    conn = get_db_connection()
    print("Connection to forecast and budget DB successful!")
except pyodbc.OperationalError as e:
    print("Database connection failed:", e)
    # Do database operations here
finally:
    if conn:
        conn.close()
        print("Connection closed.")