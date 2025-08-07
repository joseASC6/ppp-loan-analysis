import pandas as pd
from utils.common import download_from_azure, upload_to_azure, df_to_bytesio
from config.config import CLEAN_CONTAINER, DROPPED_CONTAINER, FINAL_CONTAINER
import io

def transform_naics_data():
    """
    Loads clean NAICS data from Azure Blob Storage.
    Transforms the data to fit facts and dimensions schema:
    - dim_naics
    Uploads the transformed data to Azure Blob Storage
    """
    print("Transforming NAICS data...\n")
    # Download the cleaned NAICS data from Azure Blob Storage
    naics_folder = "NAICS-data/"
    clean_naics_blob_name = f"{naics_folder}cleaned_naics_data.csv"
    naics_data = download_from_azure(clean_naics_blob_name, CLEAN_CONTAINER)

    # Read the CSV file into a DataFrame
    print(f"Reading cleaned NAICS data from {clean_naics_blob_name}...")
    df = pd.read_csv(naics_data, encoding="utf-8")
    print(f"Cleaned NAICS data has {len(df)} rows and {len(df.columns)} columns.")

    # Reset index 
    df.reset_index(drop=True, inplace=True)

    print(f"Transformed dim_naics has {len(df)} rows and {len(df.columns)} columns.")

    # Upload the DataFrame to Azure Blob Storage
    final_blob_name = "dim_naics.csv"
    output = df_to_bytesio(df, index=False, encoding='utf-8')
    upload_to_azure(output, final_blob_name, FINAL_CONTAINER)
    print("NAICS data transformation completed.\n")