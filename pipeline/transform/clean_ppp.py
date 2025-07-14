import pandas as pd
import io
from .common import download_from_azure, upload_to_azure, get_blob_list

def clean_ppp_data():
    """
    Cleans PPP data by
    Converts the datatypes.
    Uploads the cleaned data to Azure Blob Storage.
    """
    raw_container = "raw-data"
    ppp_blob_name = "PPP-data/"