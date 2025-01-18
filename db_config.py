import pyodbc

def get_db_connection():
    # Define the connection string using Windows Authentication
    connection = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost,1433;'
        'DATABASE=master;'  # Replace with your database name
        'Trusted_Connection=yes;'  # Use Windows credentials
    )
    return connection

# Example usage of the connection
try:
    conn = get_db_connection()
    print("Connection to forecast and budget DB successful!")
    # Do database operations here
finally:
    conn.close()
    print("Connection closed.")