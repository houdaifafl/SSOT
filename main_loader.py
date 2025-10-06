from Interface1WT.src.data_loader import load_tables as load_tables_1wt
from interface2_IST.src.data_loader import load_tables as load_tables_ist
from interface3OM.src.data_loader import load_tables as load_tables_3om



def main():
    print("Starting data loading for all interfaces...\n")


    print("Loading tables for Interface1WT...")
    try:
        load_tables_1wt()
    except Exception as e:
        print(f"Error loading tables for Interface1WT: {e}")


    print("\nLoading tables for interface2_IST...")
    try:
        load_tables_ist()
    except Exception as e:
        print(f"Error loading tables for interface2_IST: {e}")


    print("\nLoading tables for interface3OM...")
    try:
        load_tables_3om()
    except Exception as e:
        print(f"Error loading tables for interface3OM: {e}")

    print("\nData loading completed for all interfaces.")


if __name__ == "__main__":
    main()