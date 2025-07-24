import pandas as pd
import io
from utils.common import download_from_azure, upload_to_azure, get_blob_list, df_to_bytesio

def transform_facts_ppp_data():
    """
    Downloads cleaned PPP data from Azure Blob Storage,
    Transforms PPP data into fact tables:
        - fact_ppp
    Uploads the transformed data to Azure Blob Storage
    """
    print("Transforming PPP data into facts...\n")
    cleaned_container = "cleaned-data"
    clean_ppp_blob_name = "PPP-data/"
    final_container = "final-data"
    final_blob_name = "facts_ppp/facts_ppp"

    # Download All Dimension Tables from Azure Blob Storage
    dim_borrower = pd.read_csv(download_from_azure(blob_name='dim_borrower.csv', container_name=final_container))
    dim_loan_status = pd.read_csv(download_from_azure(blob_name='dim_loan_status.csv', container_name=final_container))
    dim_processing_method = pd.read_csv(download_from_azure(blob_name='dim_processing_method.csv', container_name=final_container))
    dim_business_type = pd.read_csv(download_from_azure(blob_name='dim_business_type.csv', container_name=final_container))
    dim_term = pd.read_csv(download_from_azure(blob_name='dim_term.csv', container_name=final_container))
    dim_business_age = pd.read_csv(download_from_azure(blob_name='dim_business_age.csv', container_name=final_container))
    dim_originating_lender = pd.read_csv(download_from_azure(blob_name='dim_originating_lender.csv', container_name=final_container))
    dim_servicing_lender = pd.read_csv(download_from_azure(blob_name='dim_servicing_lender.csv', container_name=final_container))
    dim_geography = pd.read_csv(download_from_azure(blob_name='dim_geography.csv', container_name=final_container))
    dim_naics = pd.read_csv(download_from_azure(blob_name='dim_naics.csv', container_name=final_container))
    # Download the cleaned PPP data
    # Get the list of cleaned PPP data blobs
    ppp_blobs = get_blob_list(cleaned_container, prefix=clean_ppp_blob_name)
    if not ppp_blobs:
        print("No cleaned PPP data blobs found.")
        return
    print(f"Found {len(ppp_blobs)} cleaned PPP data blobs.")
    file_count = 0
    for blob_name in ppp_blobs:
        blob_data = download_from_azure(blob_name=blob_name, container_name=cleaned_container)
        ppp_df = pd.read_csv(blob_data, encoding="utf-8", low_memory=False)

        # Make project_state and project_county_name as string
        ppp_df['project_state'] = ppp_df['project_state'].astype(str)
        ppp_df['project_county_name'] = ppp_df['project_county_name'].astype(str)
        ppp_df['geo_name'] = ppp_df['project_county_name'] + ', ' + ppp_df['project_state']

        # Merge with dimension tables
        ppp_df = ppp_df.merge(dim_loan_status[['loan_status', 'loan_status_id']], on='loan_status', how='left', suffixes=('', '_dim_loan_status'))
        ppp_df.drop(columns=['loan_status'], inplace=True)

        ppp_df = ppp_df.merge(dim_processing_method[['processing_method', 'processing_method_id']], on='processing_method', how='left', suffixes=('', '_dim_processing_method'))
        ppp_df.drop(columns=['processing_method'], inplace=True)
        
        ppp_df = ppp_df.merge(dim_business_type[['business_type', 'business_type_id']], on='business_type', how='left', suffixes=('', '_dim_business_type'))
        ppp_df.drop(columns=['business_type'], inplace=True)

        ppp_df = ppp_df.merge(dim_term[['term_month', 'term_id']], on='term_month', how='left', suffixes=('', '_dim_term'))
        ppp_df.drop(columns=['term_month'], inplace=True)

        ppp_df = ppp_df.merge(dim_business_age[['business_age_description', 'business_age_id']], on='business_age_description', how='left', suffixes=('', '_dim_business_age_description'))
        ppp_df.drop(columns=['business_age_description'], inplace=True)

        ppp_df = ppp_df.merge(dim_originating_lender[['originating_lender_location_id', 'originating_lender_id']], on='originating_lender_location_id', how='left', suffixes=('', '_dim_originating_lender'))
        ppp_df.drop(columns=['originating_lender_location_id', 'originating_lender', 'originating_lender_city', 'originating_lender_state'], inplace=True)
        
        ppp_df = ppp_df.merge(dim_borrower[['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip', 'borrower_id']], on=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'], how='left', suffixes=('', '_dim_borrower'))
        ppp_df.drop(columns=['borrower_name', 'borrower_address', 'borrower_city', 'borrower_state', 'borrower_zip'], inplace=True)
        
        ppp_df = ppp_df.merge(dim_servicing_lender[['servicing_lender_location_id', 'servicing_lender_id']], on='servicing_lender_location_id', how='left', suffixes=('', '_dim_servicing_lender'))
        ppp_df.drop(columns=['servicing_lender_location_id', 'servicing_lender_name', 'servicing_lender_address', 'servicing_lender_city', 'servicing_lender_state', 'servicing_lender_zip'], inplace=True)
        
        ppp_df = ppp_df.merge(dim_geography[['geo_name', 'geofips']], on='geo_name', how='left', suffixes=('', '_dim_geography'))
        ppp_df.drop(columns=['geo_name'], inplace=True)

        ppp_df = ppp_df.merge(dim_naics[['naics_code', 'naics_title']], on='naics_code', how='left', suffixes=('', '_dim_naics'))
        ppp_df = ppp_df[ppp_df['naics_title'].notnull()]
        ppp_df.drop(columns=['naics_title'], inplace=True)

        # Delete the records that have no geofips in the clean_ppp_data
        ppp_df = ppp_df[ppp_df['geofips'].notnull()]
        
        # Select and reorder the final columns for the fact table
        final_columns = [
            'facts_ppp_id', 'loan_number', 'naics_code', 'geofips', 'date_approved_id', 'loan_status_date_id', 'forgiveness_date_id',
            'borrower_id', 'originating_lender_id', 'servicing_lender_id', 'term_id', 'loan_status_id', 'processing_method_id',
            'sba_office_code', 'business_age_id', 'business_type_id', 'sba_guaranty_percentage', 'initial_approval_amount',
            'current_approval_amount', 'undisbursed_amount', 'forgiveness_amount'
        ]
        facts_ppp_df = ppp_df[final_columns]
        # Print the number of rows with a missing value
        print(f"Number of rows with missing values: {facts_ppp_df.isnull().any(axis=1).sum()}")

        # Upload the fact table to Azure Blob Storage
        final_blob_data = df_to_bytesio(facts_ppp_df)
        blob_name = f"{final_blob_name}_{file_count + 1}.csv"
        upload_to_azure(blob_name=blob_name, container_name=final_container, data=final_blob_data)
        file_count += 1
    print(f"Transformed {file_count} files and uploaded to Azure Blob Storage.\n")