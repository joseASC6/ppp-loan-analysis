import pandas as pd
import io
from google.cloud import storage
from google.cloud import bigquery
from config.config import GCS_BUCKET_NAME, GC_DATASET_ID, GC_PROJECT_ID

def upload_to_gcs(data: io.BytesIO, blob_name: str, folder: str) -> None:
    """Upload a BytesIO object to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{folder}/{blob_name}")

    print(f"\nUploading {blob_name} to GCS bucket {GCS_BUCKET_NAME}...")
    blob.upload_from_file(data)
    print(f"Success: Uploaded {blob_name} to GCS bucket {GCS_BUCKET_NAME}.\n")

def download_from_gcs(blob_name: str, folder: str) -> io.BytesIO:
    """Download a file from Google Cloud Storage and return it as a BytesIO object."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    
    print(f"\nDownloading {blob_name} from GCS bucket {GCS_BUCKET_NAME}...")

    if folder in blob_name:
        blob = bucket.blob(blob_name)
    else:
        blob = bucket.blob(f"{folder}/{blob_name}")

    data = io.BytesIO()
    blob.download_to_file(data)
    data.seek(0)  # Reset the stream position to the beginning
    print(f"Success: Downloaded {blob_name} from GCS bucket {GCS_BUCKET_NAME}.\n")

    return data

def get_gcs_blob_list(folder: str, prefix: str = "") -> list:
    """Retrieve a list of blobs in the specified GCS bucket, optionally filtered by prefix."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)

    print(f"\nRetrieving blob list from GCS bucket {GCS_BUCKET_NAME} with prefix '{prefix}'...")
    blobs = bucket.list_blobs(prefix=f"{folder}/{prefix}")
    blob_list = [blob.name for blob in blobs]

    if not blob_list:
        print(f"No blobs found in GCS bucket {GCS_BUCKET_NAME} with prefix '{prefix}'.")
        return []
    
    print(f"Success: Retrieved {len(blob_list)} blobs from GCS bucket {GCS_BUCKET_NAME} with prefix '{prefix}'.\n")
    return blob_list

def upload_to_bigquery(table_name: str, gcs_uri: str) -> None:
    """Load GCS CSV to Google BigQuery."""
    client = bigquery.Client(project=GC_PROJECT_ID)
    table_ref = f"{GC_PROJECT_ID}.{GC_DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,        
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    job = client.load_table_from_uri(source_uris=gcs_uri, destination=table_ref, job_config=job_config)
    print(f"Loading data from {gcs_uri} to BigQuery table {table_name}...")
    job.result()  # Wait for the job to complete
    print(f"Success: Loaded data from {gcs_uri} to BigQuery table {table_name}.\n")
