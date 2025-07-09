import requests
import io, socket
from azure.storage.blob import BlobServiceClient
from etl.config import AZURE_CONNECTION_STRING

# Download file from web
def download_file(url: str) -> io.BytesIO:
    """Download a file from the specified URL and return it as a BytesIO object."""
    print(f"Downloading file from {url}...")
    # Force IPv4 to avoid issues with IPv6
    orig_getaddrinfo = socket.getaddrinfo
    def getaddrinfo_ipv4(*args, **kwargs):
        return [ai for ai in orig_getaddrinfo(*args, **kwargs) if ai[0] == socket.AF_INET]
    socket.getaddrinfo = getaddrinfo_ipv4
    # Set headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive"
    }
    try:
        with requests.get(url, timeout=120, headers=headers, stream=True) as response:
            response.raise_for_status()
            file_content = io.BytesIO()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file_content.write(chunk)
            file_content.seek(0)
    except requests.exceptions.RequestException as e:
        print(f"Failed to download file from {url}.")
        print(f"Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Status code: {e.response.status_code}")
            print(f"Response text: {e.response.text[:500]}")
        raise
    finally:
        # Restore original getaddrinfo function
        socket.getaddrinfo = orig_getaddrinfo
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

