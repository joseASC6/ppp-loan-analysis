import pandas as pd
import io, zipfile
from .common import download_file, upload_to_azure

def extract_gdp_data():
    """
    Extracts GDP data from the U.S. Bureau of Economic Analysis (BEA), and uploads it to Azure Blob Storage.
    """
    container_name = "raw-data"
    gdp_url = "https://apps.bea.gov/regional/zip/CAGDP1.zip"
    zip_content = download_file(gdp_url)

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
                    output = io.StringIO()
                    df.to_csv(output, index=False)
                    output.seek(0)
                    filename = "GDP-data/" + filename
                    upload_to_azure(output, filename, container_name)


