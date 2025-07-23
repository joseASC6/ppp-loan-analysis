from sqlalchemy import create_engine
import pandas as pd
from utils.common import upload_to_sql, df_to_bytesio, download_from_azure, get_blob_list
import io

def load_to_PostgreSQL():
    """
    Upload a dimensions and facts to PostgreSQL database.
        - dim_loan_status
        - dim_processing_method
        - dim_business_type
        - dim_sba_office
        - dim_term
        - dim_business_age
        - dim_originating_lender
        - dim_borrower
        - dim_servicing_lender
        - facts_ppp

        - dim_date

        - facts_gdp
        - dim_geography
    """

    final_container = "final-data"

    dimensions = [
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
        "dim_geography"
    ]

    facts = [
        "facts_ppp",
        "facts_gdp"
    ]

    print("Starting to load data to PostgreSQL...\n")
    print("Uploading dimensions to PostgreSQL...")
    for dim in dimensions:
        data = download_from_azure(blob_name=f"{dim}.csv", container_name=final_container)
        df = pd.read_csv(data, encoding="utf-8")
        upload_to_sql(df=df, table_name=f"{dim}")

    print("All dimensions uploaded to PostgreSQL successfully.\n")

    print("Uploading facts to PostgreSQL...")
    gdp_data = download_from_azure(blob_name="facts_gdp.csv", container_name=final_container)
    df = pd.read_csv(gdp_data, encoding="utf-8")
    upload_to_sql(df=df, table_name="facts_gdp")

    print("Uploaded facts_gdp to PostgreSQL successfully.\n")

    print("Uploading facts_ppp to PostgreSQL...\n")
    # Get the list of blobs in the final container /facts_ppp
    ppp_blobs = get_blob_list(final_container, prefix="facts_ppp")
    if not ppp_blobs:
        print("No facts_ppp blobs found.")
        return
    print(f"Found {len(ppp_blobs)} facts_ppp blobs.")
    for blob_name in ppp_blobs:
        data = download_from_azure(blob_name=blob_name, container_name=final_container)
        df = pd.read_csv(data, encoding="utf-8")
        upload_to_sql(df=df, table_name="facts_ppp")
    print("All facts_ppp uploaded to PostgreSQL successfully.\n")

    print("All data loaded to PostgreSQL successfully.\n")