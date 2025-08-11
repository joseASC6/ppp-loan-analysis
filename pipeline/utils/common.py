import io
import pandas as pd
import requests
from config.config import CLOUD_PROVIDER
from utils.azure_utils import download_from_azure, upload_to_azure, get_azure_blob_list, upload_to_sql
from utils.gcs_utils import download_from_gcs, upload_to_gcs, get_gcs_blob_list, upload_to_bigquery

def drop_and_log(df: pd.DataFrame, dropped_df: pd.DataFrame, mask: pd.Series, reason: str) -> tuple:
    """Append dropped rows with reason to dropped_df and return filtered df and updated dropped_df."""
    if mask.any():
        dropped_rows = df[mask].copy()
        dropped_rows['drop_reason'] = reason
        dropped_df = pd.concat([dropped_df, dropped_rows], ignore_index=True)
    return df[~mask], dropped_df

def download_url_to_bytes(url: str) -> io.BytesIO:
    """Download a file from the specified URL and return it as a BytesIO object."""
    print(f"\nDownloading file from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    print(f"Success: Downloaded file from {url}.\n")
    return io.BytesIO(response.content)

def upload_to_cloud(data: io.BytesIO, blob_name: str, container_name: str) -> None:
    """Upload a BytesIO object to cloud storage (Azure or GCS)."""
    if CLOUD_PROVIDER == "azure":
        upload_to_azure(data, blob_name, container_name)
    elif CLOUD_PROVIDER == "gcs":
        upload_to_gcs(data, blob_name, container_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")

def download_from_cloud(blob_name: str, container_name: str) -> io.BytesIO:
    """Download a file from cloud storage (Azure or GCS) and return it as a BytesIO object."""
    if CLOUD_PROVIDER == "azure":
        return download_from_azure(blob_name, container_name)
    elif CLOUD_PROVIDER == "gcs":
        return download_from_gcs(blob_name, container_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    
def get_blob_list_from_cloud(container_name: str, prefix: str = "") -> list:
    """Retrieve a list of blobs in the specified cloud storage container, optionally filtered by prefix."""
    if CLOUD_PROVIDER == "azure":
        return get_azure_blob_list(container_name, prefix)
    elif CLOUD_PROVIDER == "gcs":
        return get_gcs_blob_list(container_name, prefix)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    
def upload_to_dw(df: pd.DataFrame, table_name: str) -> None:
    """Upload a DataFrame to cloud data warehouse (Azure SQL or BigQuery)."""
    if CLOUD_PROVIDER == "azure":
        upload_to_sql(df, table_name)
    elif CLOUD_PROVIDER == "gcs":
        upload_to_bigquery(df, table_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    
def df_to_bytesio(df: pd.DataFrame, encoding: str = 'utf-8', index: bool = False) -> io.BytesIO:
    """Convert a DataFrame to a BytesIO object."""
    output = io.BytesIO()
    df.to_csv(output, index=index, encoding=encoding)
    output.seek(0)
    return output