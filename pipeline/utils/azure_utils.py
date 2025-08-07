import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from config.config import AZURE_CONNECTION_STRING, DW_CONNECTION_STRING, DB_SCHEMA
from sqlalchemy import create_engine

# Upload file to Azure Blob Storage
def upload_to_azure(data: io.BytesIO, blob_name: str, container_name: str) -> None:
    """Upload a BytesIO object to Azure Blob Storage."""
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    print(f"\nUploading {blob_name} to container {container_name}...")
    blob_client.upload_blob(data.getvalue(), overwrite=True)
    print(f"Success: Uploaded {blob_name} to Azure container {container_name}.\n")

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
def get_azure_blob_list(container_name: str, prefix: str = "") -> list:
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

# Upload data to Azure SQL Database
def upload_to_sql(df: pd.DataFrame, table_name: str) -> None:
    """Upload a DataFrame to Azure SQL Database."""
    engine = create_engine(DW_CONNECTION_STRING)

    print(f"\nUploading data to SQL table {table_name}...")
    df.to_sql(table_name, con=engine, schema=DB_SCHEMA, if_exists='append', index=False)
    print(f"Success: Uploaded data to SQL table {table_name}.\n")
