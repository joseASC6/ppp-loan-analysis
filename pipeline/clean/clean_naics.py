import pandas as pd
import io
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud
from config.config import RAW_CONTAINER, CLEAN_CONTAINER

def clean_naics_data():
    """
    Cleans NAICS data by removing rows with NAICS codes that are not a number.
    Converts the datatypes.
    Uploads the cleaned data to Azure Blob Storage.
    """
    print("Starting NAICS data cleaning...\n")
    naics_blob_name = "NAICS-data/2022_NAICS_Descriptions.xlsx"

    # Download the NAICS data from Azure Blob Storage
    naics_data = download_from_cloud(blob_name=naics_blob_name, container_name=RAW_CONTAINER)

    # Read the Excel file into a DataFrame
    print(f"Reading NAICS data from {naics_blob_name}...")
    df = pd.read_excel(naics_data, engine='openpyxl')
    print(f"NAICS data has {len(df)} rows and {len(df.columns)} columns.")

    # Clean the NAICS codes
    print("Cleaning NAICS codes...")

    df.rename(columns={
        'Code': 'naics_code',
        'Title': 'naics_title',
        'Description': 'description'
    }, inplace=True)

    # Remove rows where 'naics_code' is not a number
    # There are some generic NAICS codes that are not numbers, e.g. '31-33'
    # Step 1: Remove rows where 'naics_code' is missing or null
    df = df[df['naics_code'].notnull()]

    # Step 2: Remove rows where 'naics_code' is not a number
    df = df[df['naics_code'].astype(str).str.isnumeric()]
    
    # Remove T from the end of 'naics_title'
    # Some naics_titles end with a T, e.g. 'Tile and Terrazzo ContractorsT'
    df['naics_title'] = df['naics_title'].str.rstrip('T')

    # Convert datatypes
    df['naics_code'] = df['naics_code'].astype(int)
    df['naics_title'] = df['naics_title'].astype(pd.StringDtype('pyarrow'))
    df['description'] = df['description'].astype(pd.StringDtype('pyarrow'))
    
    print(f"Cleaned NAICS data has {len(df)} rows and {len(df.columns)} columns.")

    # Upload the cleaned data to Azure Blob Storage
    cleaned_blob_name = "NAICS-data/cleaned_naics_data.csv"
    clean_container = "cleaned-data"
    output = df_to_bytesio(df, index=False, encoding='utf-8')
    upload_to_cloud(data=output, blob_name=cleaned_blob_name, container_name=clean_container)
    print(f"\nCleaned NAICS data uploaded to {cleaned_blob_name} in {clean_container} container.\n")
    