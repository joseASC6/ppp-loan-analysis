import pandas as pd
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud
from config.config import CLEAN_CONTAINER, FINAL_CONTAINER, NAICS_FOLDER

def transform_naics_data():
    """
    Loads clean NAICS data from Azure Blob Storage.
    Transforms the data to fit facts and dimensions schema:
    - dim_naics
    Uploads the transformed data to Azure Blob Storage
    """
    print("Transforming NAICS data...\n")
    # Download the cleaned NAICS data from Azure Blob Storage
    # naics_data files: cleaned_naics_data_2022.csv, cleaned_naics_data_2017.csv
    # the dim_naics will be based on the 2022 data, 2017 will be added only if a naics_code is missing in the 2022 data
    # naics_files = ['cleaned_naics_data_2022.csv', 'cleaned_naics_data_2017.csv']

    clean_naics_2022_blob_name = f"{NAICS_FOLDER}/cleaned_naics_data_2022.csv"
    naics_2022_data = download_from_cloud(blob_name=clean_naics_2022_blob_name, container_name=CLEAN_CONTAINER)

    # Read the CSV file into a DataFrame
    print(f"Reading cleaned NAICS data from {clean_naics_2022_blob_name}...")
    dim_naics = pd.read_csv(naics_2022_data, encoding="utf-8")

    # Get the 2017 data
    clean_naics_2017_blob_name = f"{NAICS_FOLDER}/cleaned_naics_data_2017.csv"
    naics_2017_data = download_from_cloud(blob_name=clean_naics_2017_blob_name, container_name=CLEAN_CONTAINER)
    naics_2017_df = pd.read_csv(naics_2017_data, encoding="utf-8")
    # Append the rows from 2017 where naics_code is in 2017 and not in 2022
    # Rows in 2017 but not in 2022
    naics_fallback_df = naics_2017_df[~naics_2017_df['naics_code'].isin(dim_naics['naics_code'])]
    dim_naics = pd.concat([dim_naics, naics_fallback_df], ignore_index=True)

    # Reset index 
    dim_naics.reset_index(drop=True, inplace=True)

    print(f"Transformed dim_naics has {len(dim_naics)} rows and {len(dim_naics.columns)} columns.")

    # Upload the DataFrame to Azure Blob Storage
    final_blob_name = "dim_naics.csv"
    output = df_to_bytesio(dim_naics, index=False, encoding='utf-8')
    upload_to_cloud(data=output, blob_name=final_blob_name, container_name=FINAL_CONTAINER)
    print("NAICS data transformation completed.\n")