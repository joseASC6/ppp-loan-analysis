import pandas as pd
from utils.common import download_from_azure, upload_to_azure, df_to_bytesio
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
    cleaned_container = "cleaned-data"
    clean_naics_blob_name = "NAICS-data/cleaned_naics_data.csv"
    naics_data = download_from_azure(clean_naics_blob_name, cleaned_container)

    # Read the CSV file into a DataFrame
    print(f"Reading cleaned NAICS data from {clean_naics_blob_name}...")
    df = pd.read_csv(naics_data, encoding="utf-8")
    print(f"Cleaned NAICS data has {len(df)} rows and {len(df.columns)} columns.")

    # Reset index 
    df.reset_index(drop=True, inplace=True)

    print(f"Transformed dim_naics has {len(df)} rows and {len(df.columns)} columns.")

    # Upload the DataFrame to Azure Blob Storage
    final_container = "final-data"
    final_blob_name = "dim_naics.csv"
    output = df_to_bytesio(df, index=False, encoding='utf-8')
    upload_to_azure(output, final_blob_name, final_container)
    print("NAICS data transformation completed.\n")