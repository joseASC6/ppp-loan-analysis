import pandas as pd
import io
from utils.common import download_url_to_bytes, upload_to_azure, df_to_bytesio

def extract_naics_data():
    """
    Extracts NAICS data from the Census Bureau, and uploads it to Azure Blob Storage.
    """
    print("Starting NAICS data extraction...\n")
    container_name = "raw-data"
    # NAICS data from the Census Bureau
    naics_url = "https://www.census.gov/naics/2022NAICS/2022_NAICS_Descriptions.xlsx"
    file_name = naics_url.split("/")[-1].replace(".xlsx", ".csv")

    # Download the NAICS file
    file_content = download_url_to_bytes(naics_url)

    # Read the Excel file into a DataFrame
    df = pd.read_excel(file_content)
    print(f"NAICS file has {len(df)} rows and {len(df.columns)} columns.")

    # Upload to Azure Blob Storage
    output = df_to_bytesio(df, index=False, encoding='utf-8')
    file_name = "NAICS-data/" + file_name
    upload_to_azure(output, file_name, container_name)

    print(f"\nNAICS data extraction completed.\n")