import pandas as pd
import io
from utils.common import download_from_azure, upload_to_azure, get_blob_list, upload_to_sql

def transform_ppp_data():
    """
    Loads clean PPP data from Azure Blob Storage.
    Transforms the data to fit facts and dimensions schema:
    - facts_ppp
    - dim_loan_status
    - dim_processing_method
    - dim_business_type
    - dim_sba_office
    - dim_term
    - dim_business_age
    - dim_originating_lender
    - dim_borrower
    - dim_servicing_lender
    and upload the transformed data to Azure Blob Storage
    """
    # Download the PPP data from Azure Blob Storage
    cleaned_container = "cleaned-data"
    clean_ppp_blob_name = "PPP-data/"

    # Get the list of cleaned PPP data blobs
    ppp_blobs = get_blob_list(clean_ppp_blob_name, prefix=clean_ppp_blob_name)
    if not ppp_blobs:
        print("No cleaned PPP data blobs found.")
        return
    
    # Get each blob, download in chunks, append and concatenate into a single DataFrame
    ppp_data_frames = []
    for blob in ppp_blobs:
        blob_data = download_from_azure(cleaned_container, blob)
        if isinstance(blob_data, bytes):
            df_chunks = pd.read_csv(io.BytesIO(blob_data), encoding="utf-8", chunksize=10000)
        else:
            df_chunks = pd.read_csv(blob_data, encoding="utf-8", chunksize=10000)

        for df in df_chunks:
            ppp_data_frames.append(df)
            print(f"Downloaded and processed {blob.name} with {len(df)} rows.")

    # Concatenate all DataFrames into a single DataFrame
    if ppp_data_frames:
        ppp_df = pd.concat(ppp_data_frames, ignore_index=True)
        print(f"Concatenated PPP data with {len(ppp_df)} rows.")
    else:
        print("No data frames were created from the downloaded blobs.")
        return
    
    # Transform the data to fit facts and dimensions schema

    # dim_loan_status
    dim_loan_status = pd.DataFrame({
        'loan_status_id': [1, 2],
        'loan_status': ['Paid in Full', 'Charged Off']
    })
    dim_loan_status['loan_status'] = dim_loan_status['loan_status'].astype(pd.StringDtype("pyarrow"))
    dim_loan_status['loan_status_id'] = dim_loan_status['loan_status_id'].astype(int)
    # Merge the ppp_df with dim_loan_status
    ppp_df = ppp_df.merge(dim_loan_status[['loan_status', 'loan_status_id']], on='loan_status', how='left', suffixes=('', '_dim_loan_status'))
    ppp_df.drop(columns=['loan_status'], inplace=True)

    # dim_processing_method
    dim_processing_method = pd.DataFrame({
        'processing_method_id': [1, 2],
        'processing_method': ['PPP', 'PPS']
    })
    dim_processing_method['processing_method'] = dim_processing_method['processing_method'].astype(pd.StringDtype("pyarrow"))
    dim_processing_method['processing_method_id'] = dim_processing_method['processing_method_id'].astype(int)
    # Merge the ppp_df with dim_processing_method
    ppp_df = ppp_df.merge(dim_processing_method[['processing_method', 'processing_method_id']], on='processing_method', how='left', suffixes=('', '_dim_processing_method'))
    ppp_df.drop(columns=['processing_method'], inplace=True)

    # dim_business_type
    dim_business_type = (
        ppp_df[['business_type']]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(business_type_id=lambda df: df.index + 1)
    )
    dim_business_type['business_type'] = dim_business_type['business_type'].astype(pd.StringDtype("pyarrow"))
    dim_business_type['business_type_id'] = dim_business_type['business_type_id'].astype(int)
    # Merge the ppp_df with dim_business_type
    ppp_df = ppp_df.merge(dim_business_type[['business_type', 'business_type_id']], on='business_type', how='left', suffixes=('', '_dim_business_type'))
    ppp_df.drop(columns=['business_type'], inplace=True)

    # dim_sba_office
    dim_sba_office = ppp_df[['sba_office_code']].drop_duplicates().reset_index(drop=True)

    #dim_term 
    dim_term = (
        ppp_df[['term_month']]
        .drop_duplicates()
        .sort_values(by='term_month')
        .reset_index(drop=True)
        .assign(term_id=lambda df: df.index + 1)
    )
    dim_term['term_month'] = dim_term['term_month'].astype(int)
    dim_term['term_id'] = dim_term['term_id'].astype(int)
    # Merge the ppp_df with dim_term
    ppp_df = ppp_df.merge(dim_term[['term_month', 'term_id']], on='term_month', how='left', suffixes=('', '_dim_term'))
    ppp_df.drop(columns=['term_month'], inplace=True)

    # dim_business_age
    dim_business_age = (
        ppp_df[['business_age_description']]
        .drop_duplicates()
        .sort_values(by='business_age_description')
        .reset_index(drop=True)
        .assign(business_age_id=lambda df: df.index + 1)
    )
    dim_business_age['business_age_description'] = dim_business_age['business_age_description'].astype(int)
    dim_business_age['business_age_id'] = dim_business_age['business_age_id'].astype(int)
    # Merge the ppp_df with dim_business_age
    ppp_df = ppp_df.merge(dim_business_age[['business_age_description', 'business_age_id']], on='business_age_description', how='left', suffixes=('', '_dim_business_age_description'))
    ppp_df.drop(columns=['business_age_description'], inplace=True)

    # dim_originating_lender
    dim_originating_lender = (
        ppp_df[['originating_lender_location_id', 'originating_lender', 'originating_lender_city', 'originating_lender_state']]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(originating_lender_id=lambda df: df.index + 1)
        [['originating_lender_id', 'originating_lender_location_id', 'originating_lender', 'originating_lender_city', 'originating_lender_state']]
    )
    # Merge the ppp_df with dim_originating_lender
    ppp_df = ppp_df.merge(dim_originating_lender[['originating_lender_location_id', 'originating_lender_id']], on='originating_lender_location_id', how='left', suffixes=('', '_dim_originating_lender'))
    ppp_df.drop(columns=['originating_lender_location_id', 'originating_lender', 'originating_lender_city', 'originating_lender_state'], inplace=True)

    # dim_borrower
    dim_borrower = (
        ppp_df[['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip', 'race', 'ethnicity', 'gender', 'veteran', 'franchise_name', 'nonprofit', 'jobs_reported']]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(borrower_id=lambda df: df.index + 1)
        [['borrower_id', 'borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip', 'race', 'ethnicity', 'gender', 'veteran', 'franchise_name', 'nonprofit', 'jobs_reported']]
    )
    # Merge the ppp_df with dim_borrower
    ppp_df = ppp_df.merge(dim_borrower[['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip', 'borrower_id']], on=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'], how='left', suffixes=('', '_dim_borrower'))
    ppp_df.drop(columns=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'], inplace=True)

    # dim_servicing_lender
    dim_servicing_lender = (
        ppp_df[['servicing_lender_location_id', 'servicing_lender_name', 'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip']]
        .drop_duplicates()
        .reset_index(drop=True)
        .assign(servicing_lender_id=lambda df: df.index + 1)
        [['servicing_lender_id', 'servicing_lender_location_id', 'servicing_lender_name', 'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip']]
    )
    # Merge the ppp_df with dim_servicing_lender
    ppp_df = ppp_df.merge(dim_servicing_lender[['servicing_lender_location_id', 'servicing_lender_id']], on='servicing_lender_location_id', how='left', suffixes=('', '_dim_servicing_lender'))
    ppp_df.drop(columns=['servicing_lender_location_id', 'servicing_lender_name', 'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip'], inplace=True)
    
    # facts_ppp
    facts_ppp = ppp_df[['facts_ppp_id', 'loan_number', 'naics_code', 'geofips', 'date_approved_id', 'loan_status_date_id', 'forgiveness_date_id', 'borrower_id', 'originating_lender_id', 'servicing_lender_id', 'term_id', 'loan_status_id', 'processing_method_id', 'sba_office_code', 'business_age_id', 'business_type_id', 'sba_guaranty_percentage', 'initial_approval_amount', 'current_approval_amount', 'undisbursed_amount', 'forgiveness_amount']]
    # Reset the index
    facts_ppp.reset_index(drop=True, inplace=True)

    # Upload the dimensions and facts to Azure Blob Storage
    final_container = "final-data"
    dim_loan_status_blob_name = "dim_loan_status.csv"
    dim_processing_method_blob_name = "dim_processing_method.csv"
    dim_business_type_blob_name = "dim_business_type.csv"
    dim_sba_office_blob_name = "dim_sba_office.csv"
    dim_term_blob_name = "dim_term.csv"
    dim_business_age_blob_name = "dim_business_age.csv"
    dim_originating_lender_blob_name = "dim_originating_lender.csv"
    dim_borrower_blob_name = "dim_borrower.csv"
    dim_servicing_lender_blob_name = "dim_servicing_lender.csv"
    facts_ppp_blob_name = "facts_ppp.csv"

    # Upload each DataFrame to Azure Blob Storage
    