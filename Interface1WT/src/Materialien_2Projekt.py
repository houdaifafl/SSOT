import pandas as pd

import db_config
from file_paths import path_materials

def create_material_table(cursor):
    query = """IF NOT EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'dim_material_2')
            BEGIN 
                CREATE TABLE dbo.dim_material_2 (
                    dim_material_id VARCHAR(20) PRIMARY KEY,
                    material_name VARCHAR(100),
                    material_type VARCHAR(50),
                    category VARCHAR(10)
                );
            END;
            """
    cursor.execute(query)
    print("dim_material number 2 table created.")

def import_new_materialien_namen(file_path):
    df = pd.read_excel(file_path, sheet_name="Materialien", dtype= str)
    for _, row in df.iterrows():
        trimmer_list = list(row['dim_material_id'])
        trimmer_list[2] = ""
        trimmer_list[4] = ""
        trimmer_string = "".join(trimmer_list)
        print(f"material_id is {trimmer_string}")
        row['dim_material_id'] = trimmer_string

        if pd.isna(row['category']):
            row['category'] = ""
    print(df)
    return df

def load_tableDB(cursor, dataframe):
    # Check for empty or NaN values in the 'category' column
    """if dataframe['category'].isnull().any() or dataframe['category'].str.strip().eq('').any():
        invalid_rows = dataframe[dataframe['category'].isnull() | dataframe['category'].str.strip().eq('')]
        raise ValueError(
            f"Error: Found rows with missing 'category' values. Cannot proceed.\n"
            f"Invalid rows:\n{invalid_rows[['dim_material_id', 'material_name', 'material_type', 'category']]}"
        )"""

    for _, row in dataframe.iterrows():
        material_id = row['dim_material_id']
        material_name = row['material_name']
        material_type = row['material_type']
        material_kategorie = row['category']
        try:
            query = f"INSERT INTO dim_material_2(dim_material_id, material_name, material_type, category) VALUES (?, ?, ?, ?)"
            cursor.execute(query, (material_id, material_name,material_type,material_kategorie))
        except Exception as e:
            print(f"Error inserting for material {material_id}: {e}")

def create_and_load():
    conn = None
    cursor = None
    try:
        conn = db_config.get_db_connection()
        cursor = conn.cursor()

        #create_material_table(cursor)
        dataframe = import_new_materialien_namen(path_materials)
        #load_tableDB(cursor, dataframe)
        #print(dataframe)

        #commit transaction
        conn.commit()

        print("All materials were loaded successfully.")
    except Exception as e:
        print(f"Error creating or loading: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    create_and_load()



