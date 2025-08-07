import pandas as pd
import io
from google.cloud import storage
from google.cloud import bigquery
from config.config import DB_SCHEMA, GCS_BUCKET_NAME

def upload_to_gcs(data: io.BytesIO, folder: str, blob_name: str) -> None:
    """Upload a BytesIO object to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{folder}/{blob_name}")

    print(f"\nUploading {blob_name} to GCS bucket {GCS_BUCKET_NAME}...")
    blob.upload_from_file(data)
    print(f"Success: Uploaded {blob_name} to GCS bucket {GCS_BUCKET_NAME}.\n")

def download_from_gcs(folder: str, blob_name: str) -> io.BytesIO:
    """Download a file from Google Cloud Storage and return it as a BytesIO object."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(f"{folder}/{blob_name}")

    print(f"\nDownloading {blob_name} from GCS bucket {GCS_BUCKET_NAME}...")
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

def upload_to_bigquery(df: pd.DataFrame, table_name: str) -> None:
    """Upload a DataFrame to Google BigQuery."""
    client = bigquery.Client()
    dataset_ref = client.dataset(DB_SCHEMA)
    table_ref = dataset_ref.table(table_name)

    print(f"\nUploading data to BigQuery table {table_name}...")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Success: Uploaded data to BigQuery table {table_name}.\n")
    