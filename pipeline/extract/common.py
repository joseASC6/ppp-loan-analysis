import requests
import io
from azure.storage.blob import BlobServiceClient
from config.config import AZURE_CONNECTION_STRING

# Download file from web
def download_file(url: str) -> io.BytesIO:
    """Download a file from the specified URL and return it as a BytesIO object."""
    print(f"Downloading file from {url}...")
    response = requests.get(url)
    response.raise_for_status()
    print(f"Success: Downloaded file from {url}.")
    return io.BytesIO(response.content)

# Upload file to Azure Blob Storage
def upload_to_azure(data, blob_name: str, container_name: str) -> None:
    # data: data to be uploaded
    # blob_name: name of the blob
    # container_name: name of the container in the blob storage
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    # Upload the data
    print(f"Uploading {blob_name} to container {container_name}...")
    blob_client.upload_blob(data.getvalue(), overwrite=True)
    print(f"Success: Uploaded {blob_name} to Azure container {container_name}.")

