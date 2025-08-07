import pandas as pd
import io, zipfile
from utils.common import download_url_to_bytes, upload_to_cloud, df_to_bytesio
from config.config import RAW_CONTAINER

def extract_gdp_data():
    """
    Extracts GDP data from the U.S. Bureau of Economic Analysis (BEA), and uploads it to Azure Blob Storage.
    """
    print("Starting GDP data extraction...\n")
    gdp_url = "https://apps.bea.gov/regional/zip/CAGDP1.zip"
    zip_content = download_url_to_bytes(gdp_url)

    with zipfile.ZipFile(zip_content) as zip_ref:
        for filename in zip_ref.namelist():
            if "CAGDP1__ALL_AREAS" in filename:
                print(f"Processing file: {filename}")
                with zip_ref.open(filename) as f:
                    # Read the CSV file into a DataFrame
                    print(f"Reading file: {filename}")
                    df = pd.read_csv(f, encoding="latin-1")
                    print(f"CSV file has {len(df)} rows and {len(df.columns)} columns.")    

                    # Upload to Azure Blob Storage
                    output = df_to_bytesio(df, index=False, encoding='utf-8')
                    upload_to_cloud(data=output, blob_name=filename, container_name=RAW_CONTAINER)

    print(f"\nGDP data extraction completed.\n")

