from bs4 import BeautifulSoup, Tag
import pandas as pd
import requests
from utils.common import download_file, upload_to_azure, df_to_bytesio

def extract_ppp_data():
    """
    Extracts PPP data from the U.S. Small Business Administration (SBA) website, and uploads it to Azure Blob Storage.
    """
    container_name = "raw-data"
    ppp_url = "https://data.sba.gov/dataset/ppp-foia"
    response = requests.get(ppp_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    for link in soup.find_all('a', href=True):
        if isinstance(link, Tag):
            href = link.get('href')
            href_str = str(href)
            if href and href_str.endswith('.csv'):
                file_url = href_str
                file_name = file_url.split('/')[-1]

                # Download the file
                file_content = download_file(file_url)

                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_content, encoding="latin-1")
                print(f"CSV file has {len(df)} rows and {len(df.columns)} columns.")

                # Upload to Azure Blob Storage
                file_name = "PPP-data/" + file_name
                output = df_to_bytesio(df, index=False, encoding='utf-8')
                upload_to_azure(output, file_name, container_name)
            