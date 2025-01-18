import pyodbc
from db_config import get_db_connection

def create_dim_lagerbewegung_table(cursor):
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dim_lagerbewegung')
    BEGIN
        CREATE TABLE dim_lagerbewegung (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            BpzIdt VARCHAR(20),
            MatIdt VARCHAR(20),
            KeziBez VARCHAR(255),
            ChgNmr VARCHAR(50),
            BpdSort VARCHAR(50),
            MatArt VARCHAR(50),
            KstUrsache VARCHAR(255),
            PrzPre VARCHAR(20),
            LgrDW VARCHAR(50),
            AbrSammler VARCHAR(50),
            BucPer VARCHAR(50),
            VrgGrp INT,
            MngEinhIdt INT,
            BucDat DATE,
            KwBezJahr_de VARCHAR(50),
            KwBezMon_de VARCHAR(50),
            LOrtIdt INT,
            LOrtBez VARCHAR(255),
            LPlzIdt INT,
            LPlzBez VARCHAR(255),
            LgrFirmIdt INT,
            LgrFirmName VARCHAR(255),
            MngW FLOAT,
            MngD FLOAT,
            Pb FLOAT,
            Ag FLOAT,
            Au FLOAT,
            Cu FLOAT,
            S FLOAT,
            Zn FLOAT,
            FeO FLOAT,
            SiO2 FLOAT,
            CaO FLOAT,
            [As] FLOAT,
            Sb FLOAT,
            Bi FLOAT,
            Se FLOAT,
            Cl FLOAT,
            Cd FLOAT
        );
    END;
    """
    cursor.execute(create_table_query)
    print("dim_lagerbewegung table created successfully.")


def create_Report_table(cursor):
    """
    Creates the combined table in the database.

    Args:
        conn: Database connection object.
    """
    create_table_query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Report')
    BEGIN
        CREATE TABLE Report (
            monthday DATE PRIMARY KEY,
            Ist_Actual NVARCHAR(50) NOT NULL,
            Main NVARCHAR(50) NOT NULL,
            Side NVARCHAR(50) NOT NULL,
            Intern NVARCHAR(50) NOT NULL,
            Extern NVARCHAR(50) NOT NULL,
            OtherSecond NVARCHAR(50) NOT NULL,
            fluxes VARCHAR(50) NOT NULL
        );
    END
    """
    cursor.execute(create_table_query)
    print("the report table was created successfully.")




def create_tables():

    conn = None
    cursor = None
    try:
        conn =  get_db_connection()
        cursor = conn.cursor()

        create_dim_lagerbewegung_table(cursor)

        create_Report_table(cursor)

        conn.commit()
        print("All tables created successfully")

    except pyodbc.Error as e:
        print("Error creating tables:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    create_tables()
