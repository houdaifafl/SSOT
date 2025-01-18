from Interface1WT.src.schema_creator import create_tables as create_tables_1wt
from interface2_IST.src.schema_creator import create_tables as create_tables_ist
from interface3OM.src.schema_creator import create_tables as create_tables_3om


def main():
    print("Starting schema creation for all interfaces...\n")

    # Call schema creation for Interface1WT
    print("Creating tables for Interface1WT...")
    try:
        create_tables_1wt()
    except Exception as e:
        print(f"Error creating tables for Interface1WT: {e}")

    # Call schema creation for interface2_IST
    print("\nCreating tables for interface2_IST...")
    try:
        create_tables_ist()
    except Exception as e:
        print(f"Error creating tables for interface2_IST: {e}")

    # Call schema creation for interface3OM
    print("\nCreating tables for interface3OM...")
    try:
        create_tables_3om()
    except Exception as e:
        print(f"Error creating tables for interface3OM: {e}")

    print("\nSchema creation completed for all interfaces.")


if __name__ == "__main__":
    main()
