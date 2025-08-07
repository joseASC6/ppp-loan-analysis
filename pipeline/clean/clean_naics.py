import pandas as pd
import io
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud, drop_and_log
from config.config import RAW_CONTAINER, CLEAN_CONTAINER, DROPPED_CONTAINER

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

    dropped_df = pd.DataFrame(columns=df.columns.tolist() + ['drop_reason'])

    # Remove rows where 'naics_code' is not a number
    # There are some generic NAICS codes that are not numbers, e.g. '31-33'
    # Step 1: Remove rows where 'naics_code' is missing or null
    mask_null = df['naics_code'].isnull()
    df, dropped_df = drop_and_log(df, dropped_df, mask_null, 'naics_code_null')

    # Step 2: Remove rows where 'naics_code' is not a number
    
    mask_not_numeric = ~df['naics_code'].astype(str).str.isnumeric()
    df, dropped_df = drop_and_log(df, dropped_df, mask_not_numeric, 'naics_code_not_numeric')
    
    # Remove T from the end of 'naics_title'
    # Some naics_titles end with a T, e.g. 'Tile and Terrazzo ContractorsT'
    df['naics_title'] = df['naics_title'].str.rstrip('T')

    # Convert datatypes
    df['naics_code'] = df['naics_code'].astype(int)
    df['naics_title'] = df['naics_title'].astype(pd.StringDtype('pyarrow'))
    df['description'] = df['description'].astype(pd.StringDtype('pyarrow'))
    
    print(f"Cleaned NAICS data has {len(df)} rows and {len(df.columns)} columns.")

    # Upload the cleaned data to Azure Blob Storage
    naics_folder = "NAICS-data"
    cleaned_blob_name = f"{naics_folder}/cleaned_naics_data.csv"
    output = df_to_bytesio(df, index=False, encoding='utf-8')
    upload_to_cloud(data=output, blob_name=cleaned_blob_name, container_name=CLEAN_CONTAINER)
    print(f"\nCleaned NAICS data uploaded to {cleaned_blob_name} in {CLEAN_CONTAINER} container.\n")

    # If there are any dropped rows, upload them to the dropped container
    if not dropped_df.empty:
        print(f"\nDropped NAICS data has {len(dropped_df)} rows.")
        dropped_blob_name = f"{naics_folder}/dropped_naics_data.csv"
        dropped_output = df_to_bytesio(dropped_df, index=False, encoding='utf-8')
        upload_to_cloud(data=dropped_output, blob_name=dropped_blob_name, container_name=DROPPED_CONTAINER)
        print(f"\nDropped NAICS data uploaded to {dropped_blob_name} in {DROPPED_CONTAINER} container.\n")