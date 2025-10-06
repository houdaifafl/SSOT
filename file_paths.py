from pathlib import Path
import glob
import os
import sys

def get_latest_excel_file(directory):
    try:
        files = glob.glob(os.path.join(directory, '*.xls*'))

        files = [f for f in files if f.lower().endswith(('.xlsx', '.xlsm' ))]
        if not files:
            raise FileNotFoundError(f"No Excel files found in folder: {directory}")
        return max(files, key=os.path.getctime)

    except Exception as e:
        print(f"Error while searching for Excel files: {e}")
        raise

# Determine project root (outside Python_Project)
def get_project_root():
    if getattr(sys, 'frozen', False):
        # Running as EXE
        return Path(sys.executable).resolve().parent.parent.parent
    else:
        # Running from source
        return Path(__file__).resolve().parent.parent

# Root = "C:\Single Source of Truth - Fh Aachen"
PROJECT_ROOT = get_project_root()

#BASE_DIR = PROJECT_ROOT.parent

# Input_Data is outside Python_Project
INPUT_DATA = PROJECT_ROOT / "Input_Data"

# Paths
path_KSReport = get_latest_excel_file(str(INPUT_DATA / "KS_Report"))
path_Reaktordata = str(INPUT_DATA / "Reaktor_Data")
path_LgrBwg = str(INPUT_DATA / "Lgr_Bwg")

jsonfile_reactor = str(PROJECT_ROOT / "Python_Project" / "interface3OM" / "JsonFile" / "loaded_files.json")

# For testing
path_variables = str(INPUT_DATA / "Variable" / "Variables.xlsx")