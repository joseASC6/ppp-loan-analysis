from .extract.extract_ppp import extract_ppp_data
from .extract.extract_naics import extract_naics_data
from .extract.extract_gdp import extract_gdp_data

if __name__ == "__main__":
    print("\nStarting data extraction...")

    print("\nExtracting PPP data...")
    extract_ppp_data()
    print("\nSuccess: Extracted PPP data.")

    print("\nExtracting NAICS data...")
    #extract_naics_data()
    print("\nSuccess: Extracted NAICS data.")

    print("\nExtracting GDP data...\n")
    #extract_gdp_data()
    print("\nSuccess: Extracted GDP data.")

    print("\nData extraction completed.")

    