import pandas as pd
from utils.common import upload_to_dw, download_from_azure, get_blob_list_from_cloud
from utils.gcs_utils import upload_to_bigquery
from config.config import FINAL_CONTAINER, GCS_BUCKET_NAME, CLOUD_PROVIDER
def load_to_PostgreSQL():
    """Upload a dimensions and facts to PostgreSQL database."""
    tables = [
        "dim_loan_status",
        "dim_processing_method",
        "dim_business_type",
        "dim_sba_office",
        "dim_term",
        "dim_business_age",
        "dim_originating_lender",
        "dim_borrower",
        "dim_servicing_lender",
        "dim_date",
        "dim_naics",
        "dim_geography",
        "facts_gdp"
    ]
    # Append ppp tables to tables list
    ppp_blobs = get_blob_list_from_cloud(FINAL_CONTAINER, prefix="facts_ppp")
    if not ppp_blobs:
        print("No facts_ppp blobs found.")
    print(f"Found {len(ppp_blobs)} facts_ppp blobs.")
    # Append ppp blobs from FINAL_CONTAINER to tables list -> "facts_ppp/facts_ppp_1.csv"
    tables.extend(ppp_blobs)
    
    if not tables:
        print("No tables found.")
    else:
        print(f"Found {len(tables)} tables to upload to PostgreSQL.")

    print("Starting to load data to PostgreSQL...\n")
    for table in tables:
        # Check if table string contains "facts_ppp"
        if "facts_ppp" in table:
            table_name, blob_name = "facts_ppp", table
        else:
            table_name, blob_name = table, f"{table}.csv"

        data = download_from_azure(blob_name=blob_name, container_name=FINAL_CONTAINER)
        df = pd.read_csv(data, encoding="utf-8")
        print(f"\nUploading: {blob_name} to {table_name} in PostgreSQL...")
        upload_to_dw(df=df, table_name=table_name)
        print(f"Successfully uploaded {table_name} data to PostgreSQL\n")
    print("All data loaded to PostgreSQL successfully.\n")

def load_to_BigQuery():
    """Upload dimensions and facts to BigQuery."""
    tables = [
        "dim_loan_status",
        "dim_processing_method",
        "dim_business_type",
        "dim_sba_office",
        "dim_term",
        "dim_business_age",
        "dim_originating_lender",
        "dim_borrower",
        "dim_servicing_lender",
        "dim_date",
        "dim_naics",
        "dim_geography",
        "facts_gdp"
    ]
    # Append ppp tables to tables list
    ppp_blobs = get_blob_list_from_cloud(FINAL_CONTAINER, prefix="facts_ppp")
    if not ppp_blobs:
        print("No facts_ppp blobs found.")
    print(f"Found {len(ppp_blobs)} facts_ppp blobs.")
    tables.extend(ppp_blobs)
    # Append ppp blobs from FINAL_CONTAINER to tables list -> "final-data/facts_ppp/facts_ppp_1.csv"
    tables.extend(ppp_blobs)
    if not tables:
        print("No tables found.")
    else:
        print(f"Found {len(tables)} tables to upload to Big Query.")

    base_gcs_uri = f"gs://{GCS_BUCKET_NAME}"
    print("Starting to load data to Big Query...\n")
    for table in tables:
        # Check if table string contains "facts_ppp"
        if "facts_ppp" in table:
            table_name, blob_name = "facts_ppp", table
        else:
            table_name, blob_name = table, f"{FINAL_CONTAINER}/{table}.csv"
        gcs_uri = f"{base_gcs_uri}/{blob_name}"
        print(f"\nUploading: {blob_name} to {table_name} in Big Query...")
        upload_to_bigquery(gcs_uri=gcs_uri, table_name=table_name)
        print(f"Successfully uploaded {table_name} data to Big Query\n")

    print("All data loaded to Big Query successfully.\n")

    
