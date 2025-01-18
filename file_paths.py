import glob
import os


# Helper function to get the latest excel file
def get_latest_excel_file(directory):
    """
    Sucht die neueste Excel-Datei im angegebenen Verzeichnis.

    Args:
        directory (str): Pfad zum Ordner, der durchsucht werden soll.

    Returns:
        str: Vollständiger Pfad zur neuesten Excel-Datei.
    """
    try:
        # Suchmuster für Excel-Dateien im Verzeichnis
        files = glob.glob(os.path.join(directory, "*.xlsx"))
        if not files:
            raise FileNotFoundError(f"Keine Excel-Dateien im Ordner {directory} gefunden.")
        # Rückgabe der neuesten Datei nach Änderungsdatum
        return max(files, key=os.path.getmtime)
    except Exception as e:
        print(f"Fehler beim Suchen der Datei: {e}")
        raise


path_KSReport = get_latest_excel_file(r"C:\Single Source of Truth - Fh Aachen\Input_Data\KS_Report")
path_Reaktordata = r"C:\Single Source of Truth - Fh Aachen\Input_Data\Reaktor_Data"
path_LgrBwg = r"C:\Single Source of Truth - Fh Aachen\Input_Data\Lgr_Bwg"
jsonfile_reactor= r"C:\Single Source of Truth - Fh Aachen\Python_Project\interface3OM\JsonFile\loaded_files.json"

# Only for testing cases
path_variables = r"C:\Single Source of Truth - Fh Aachen\Input_Data\Variablen\Variables.xlsx"