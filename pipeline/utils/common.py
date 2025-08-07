import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
import requests
from sqlalchemy import create_engine
from config.config import AZURE_CONNECTION_STRING, DW_CONNECTION_STRING, DB_SCHEMA, CLOUD_PROVIDER
from utils.azure_utils import download_from_azure, upload_to_azure, get_blob_list, upload_to_sql
from utils.gcs_utils import download_from_gcs, upload_to_gcs, get_gcs_blob_list, upload_to_bigquery

# Download file from web
def download_file(url: str) -> io.BytesIO:
    """Download a file from the specified URL and return it as a BytesIO object."""
    print(f"\nDownloading file from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    print(f"Success: Downloaded file from {url}.\n")
    return io.BytesIO(response.content)

def download_from_cloud(blob_name: str, container_name: str) -> io.BytesIO:
    """Download a file from cloud storage (Azure or GCS) and return it as a BytesIO object."""
    if CLOUD_PROVIDER == "azure":
        return download_from_azure(blob_name, container_name)
    elif CLOUD_PROVIDER == "gcs":
        return download_from_gcs(container_name, blob_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    
def upload_to_cloud(data: io.BytesIO, blob_name: str, container_name: str) -> None:
    """Upload a BytesIO object to cloud storage (Azure or GCS)."""
    if CLOUD_PROVIDER == "azure":
        upload_to_azure(data, blob_name, container_name)
    elif CLOUD_PROVIDER == "gcs":
        upload_to_gcs(data, container_name, blob_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")

def get_blob_list_from_cloud(container_name: str, prefix: str = "") -> list:
    """Retrieve a list of blobs in the specified cloud storage container, optionally filtered by prefix."""
    if CLOUD_PROVIDER == "azure":
        return get_blob_list(container_name, prefix)
    elif CLOUD_PROVIDER == "gcs":
        return get_gcs_blob_list(container_name, prefix)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")
    
def upload_to_cloud_dw(df: pd.DataFrame, table_name: str) -> None:
    """Upload a DataFrame to cloud data warehouse (Azure SQL or BigQuery)."""
    if CLOUD_PROVIDER == "azure":
        upload_to_sql(df, table_name)
    elif CLOUD_PROVIDER == "gcs":
        upload_to_bigquery(df, table_name)
    else:
        raise ValueError(f"Unsupported cloud provider: {CLOUD_PROVIDER}")

# Download file from Azure Blob Storage
def download_from_azure(blob_name: str, container_name: str) -> io.BytesIO:
    """Download a file from Azure Blob Storage and return it as a BytesIO object."""
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    print(f"\nDownloading {blob_name} from container {container_name}...")
    download_stream = blob_client.download_blob()
    data = b""
    for chunk in download_stream.chunks():
        data += chunk
    print(f"Success: Downloaded {blob_name} from Azure container {container_name}.\n")
    
    return io.BytesIO(data)

# Get blob list from Azure Blob Storage
def get_blob_list(container_name: str, prefix: str = "") -> list:
    """Retrieve a list of blobs in the specified Azure container, optionally filtered by prefix."""
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(container_name)

    print(f"\nRetrieving blob list from container {container_name} with prefix '{prefix}'...")
    blob_list = [blob.name for blob in container_client.list_blobs(name_starts_with=prefix)]
    if not blob_list:
        print(f"No blobs found in container {container_name} with prefix '{prefix}'.")
        return []
    print(f"Success: Retrieved {len(blob_list)} blobs from Azure container {container_name} with prefix '{prefix}'.\n")

    return blob_list

# Upload file to Azure Blob Storage
def upload_to_azure(data: io.BytesIO, blob_name: str, container_name: str) -> None:
    """Upload a BytesIO object to Azure Blob Storage."""
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    print(f"\nUploading {blob_name} to container {container_name}...")
    blob_client.upload_blob(data.getvalue(), overwrite=True)
    print(f"Success: Uploaded {blob_name} to Azure container {container_name}.\n")

# Upload data to Azure SQL Database
def upload_to_sql(df: pd.DataFrame, table_name: str) -> None:
    """Upload a DataFrame to Azure SQL Database."""
    engine = create_engine(DW_CONNECTION_STRING)

    print(f"\nUploading data to SQL table {table_name}...")
    df.to_sql(table_name, con=engine, schema=DB_SCHEMA, if_exists='append', index=False)
    print(f"Success: Uploaded data to SQL table {table_name}.\n")

# Convert a DataFrame to a BytesIO object
def df_to_bytesio(df: pd.DataFrame, encoding: str = 'utf-8', index: bool = False) -> io.BytesIO:
    """Convert a DataFrame to a BytesIO object."""
    output = io.BytesIO()
    df.to_csv(output, index=index, encoding=encoding)
    output.seek(0)
    return output