import pandas as pd
import io
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud, get_blob_list_from_cloud
from config.config import FINAL_CONTAINER, CLEAN_CONTAINER, PPP_FOLDER


def create_dim_loan_status() -> pd.DataFrame:
    """Create dim_loan_status DataFrame from the provided DataFrame."""
    dim_loan_status = pd.DataFrame({
        'loan_status_id': [1, 2],
        'loan_status': ['Paid in Full', 'Charged Off']
    })
    dim_loan_status['loan_status'] = dim_loan_status['loan_status'].astype(pd.StringDtype("pyarrow"))
    dim_loan_status['loan_status_id'] = dim_loan_status['loan_status_id'].astype(int)
    return dim_loan_status

def create_dim_processing_method() -> pd.DataFrame:
    dim_processing_method = pd.DataFrame({
        'processing_method_id': [1, 2],
        'processing_method': ['PPP', 'PPS']
    })
    dim_processing_method['processing_method'] = dim_processing_method['processing_method'].astype(pd.StringDtype("pyarrow"))
    dim_processing_method['processing_method_id'] = dim_processing_method['processing_method_id'].astype(int)
    return dim_processing_method

def transform_dim_ppp_data():
    """
    Downloads cleaned PPP data from Azure Blob Storage,
    Find unique values for various dimensions,
    Transforms PPP data into dimensions:
        - dim_loan_status
        - dim_processing_method
        - dim_business_type
        - dim_sba_office
        - dim_term
        - dim_business_age
        - dim_originating_lender
        - dim_borrower
        - dim_servicing_lender
    Uploads the transformed data to Azure Blob Storage
    """
    print("Transforming PPP data into dimensions...\n")

    # Get the list of cleaned PPP data blobs
    ppp_blobs = get_blob_list_from_cloud(CLEAN_CONTAINER, prefix=PPP_FOLDER)
    if not ppp_blobs:
        print("No cleaned PPP data blobs found.")
        return
    print(f"Found {len(ppp_blobs)} cleaned PPP data blobs.")

    
    dim_originating_lender = pd.DataFrame(columns=[
        'originating_lender_id', 'originating_lender_location_id',
        'originating_lender', 'originating_lender_city', 'originating_lender_state'
    ])
    dim_borrower = pd.DataFrame(columns=[
        'borrower_id', 'borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip',
        'race', 'ethnicity', 'gender', 'veteran', 'nonprofit', 'franchise_name', 'jobs_reported'
    ])
    dim_servicing_lender = pd.DataFrame(columns=[
        'servicing_lender_location_id', 'servicing_lender_name',
        'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip'
    ])

    unique_business_type = set()
    unique_sba_office = set()
    unique_term = set()
    unique_business_age = set()

    for blob_name in ppp_blobs:
        blob_data = download_from_cloud(blob_name=blob_name, container_name=CLEAN_CONTAINER)
        df = pd.read_csv(blob_data, encoding="utf-8", low_memory=False)

        # Set cols veteran and nonprofit as boolean
        df['veteran'] = df['veteran'].astype(bool)
        df['nonprofit'] = df['nonprofit'].astype(bool)

        # Find unique values for dimensions, add them to respective sets
        unique_business_type.update(df['business_type'].dropna().unique())
        unique_sba_office.update(df['sba_office_code'].dropna().unique())
        unique_term.update(df['term_month'].dropna().unique())
        unique_business_age.update(df['business_age_description'].dropna().unique())

        # Special case: dim_originating_lender, dim_borrower, dim_servicing_lender
        dim_originating_lender = pd.concat([
            dim_originating_lender,
            df[['originating_lender_location_id', 'originating_lender', 'originating_lender_city', 'originating_lender_state']].drop_duplicates()
        ]).drop_duplicates()

        dim_borrower = pd.concat([
            dim_borrower,
            df[['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip',
                'race', 'ethnicity', 'gender', 'veteran', 'nonprofit', 'franchise_name', 'jobs_reported']]
                .drop_duplicates(subset=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'])
        ]).drop_duplicates(subset=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'])

        dim_servicing_lender = pd.concat([
            dim_servicing_lender,
            df[['servicing_lender_location_id', 'servicing_lender_name', 'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip']].drop_duplicates()
        ]).drop_duplicates()
    
    # Reset index for all dimensions
    dim_originating_lender.reset_index(drop=True, inplace=True)
    dim_borrower.reset_index(drop=True, inplace=True)
    dim_servicing_lender.reset_index(drop=True, inplace=True)

    # Assign unique IDs
    dim_originating_lender['originating_lender_id'] = dim_originating_lender.index + 1
    dim_borrower['borrower_id'] = dim_borrower.index + 1
    dim_servicing_lender['servicing_lender_id'] = dim_servicing_lender.index + 1

    # dim_loan_status
    dim_loan_status = create_dim_loan_status()

    # dim_processing_method
    dim_processing_method = create_dim_processing_method()
    
    # dim_business_type
    dim_business_type = pd.DataFrame({
        'business_type_id': range(1, len(unique_business_type) + 1),
        'business_type': list(unique_business_type)
    })
    dim_business_type['business_type'] = dim_business_type['business_type'].astype(pd.StringDtype("pyarrow"))
    dim_business_type['business_type_id'] = dim_business_type['business_type_id'].astype(int)

    # dim_sba_office
    dim_sba_office = pd.DataFrame({
        'sba_office_code': list(unique_sba_office)
    })
    dim_sba_office['sba_office_code'] = dim_sba_office['sba_office_code'].astype(int)

    # dim_term
    dim_term = pd.DataFrame({
        'term_id': range(1, len(unique_term) + 1),
        'term_month': sorted(list(unique_term))
    })
    dim_term['term_month'] = dim_term['term_month'].astype(pd.StringDtype("pyarrow"))
    dim_term['term_id'] = dim_term['term_id'].astype(int)

    # dim_business_age
    dim_business_age = pd.DataFrame({
        'business_age_id': range(1, len(unique_business_age) + 1),
        'business_age_description': sorted(list(unique_business_age))
    })

    dim_business_age['business_age_description'] = dim_business_age['business_age_description'].astype(pd.StringDtype("pyarrow"))
    dim_business_age['business_age_id'] = dim_business_age['business_age_id'].astype(int)

    # Upload the DataFrames to Azure Blob Storage
    dim_loan_status_blob_name = "dim_loan_status.csv"
    dim_processing_method_blob_name = "dim_processing_method.csv"
    dim_business_type_blob_name = "dim_business_type.csv"
    dim_sba_office_blob_name = "dim_sba_office.csv"
    dim_term_blob_name = "dim_term.csv"
    dim_business_age_blob_name = "dim_business_age.csv"
    dim_originating_lender_blob_name = "dim_originating_lender.csv"
    dim_borrower_blob_name = "dim_borrower.csv"
    dim_servicing_lender_blob_name = "dim_servicing_lender.csv"

    # Upload each DataFrame to Azure Blob Storage
    print("Uploading dimension tables to Azure Blob Storage...\n")
    upload_to_cloud(data=df_to_bytesio(dim_loan_status), blob_name=dim_loan_status_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_processing_method), blob_name=dim_processing_method_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_business_type), blob_name=dim_business_type_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_sba_office), blob_name=dim_sba_office_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_term), blob_name=dim_term_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_business_age), blob_name=dim_business_age_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_originating_lender), blob_name=dim_originating_lender_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_borrower), blob_name=dim_borrower_blob_name, container_name=FINAL_CONTAINER)
    upload_to_cloud(data=df_to_bytesio(dim_servicing_lender), blob_name=dim_servicing_lender_blob_name, container_name=FINAL_CONTAINER)
    print("\nTransformation completed.")