from bs4 import BeautifulSoup, Tag
import pandas as pd
import io
import requests
from .common import download_file, upload_to_azure

def extract_ppp_data():
    """
    Extracts PPP data from the U.S. Small Business Administration (SBA) website, and uploads it to Azure Blob Storage.
    """
    container_name = "pppdata"
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
                df = pd.read_csv(file_content)
                print(f"CSV file has {len(df)} rows and {len(df.columns)} columns.")

                # Upload to Azure Blob Storage
                output = io.StringIO()
                df.to_csv(output, index=False)
                output.seek(0)
                upload_to_azure(output, file_name, container_name)
            