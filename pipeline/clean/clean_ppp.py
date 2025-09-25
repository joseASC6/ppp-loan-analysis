import pandas as pd
import io
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud, get_blob_list_from_cloud, drop_and_log
from config.config import RAW_CONTAINER, CLEAN_CONTAINER, DROPPED_CONTAINER, PPP_FOLDER

def clean_ppp_data():
    """
    Cleans PPP data by
    Converts the datatypes.
    Uploads the cleaned data to Azure Blob Storage.
    """
    print("Starting PPP data cleaning...\n")
    # Find all the files in the PPP-data folder in the raw container
    ppp_blob_list = get_blob_list_from_cloud(RAW_CONTAINER, prefix=PPP_FOLDER)
    if not ppp_blob_list:
        print("No PPP data found in the raw container.")
        return
    print(f"Found {len(ppp_blob_list)} PPP data files in the raw container.")

    facts_ppp_id = 1

    for blob_name in ppp_blob_list:
        # Download the PPP data from Azure Blob Storage
        ppp_data = download_from_cloud(blob_name=blob_name, container_name=RAW_CONTAINER)
        if not ppp_data:
            print(f"Failed to download PPP data from {blob_name}.")
            continue

        # Read the CSV file into a DataFrame
        print(f"\nReading PPP data from {blob_name}...")
        df = pd.read_csv(ppp_data, encoding="utf-8", low_memory=False)
        print(f"PPP data has {len(df)} rows and {len(df.columns)} columns.")

        # Clean the PPP data
        print(f"Cleaning PPP data from {blob_name}...")

        # Remove unnecessary columns
        df.drop(columns=[
            'UTILITIES_PROCEED',
            'PAYROLL_PROCEED',
            'MORTGAGE_INTEREST_PROCEED',
            'RENT_PROCEED',
            'REFINANCE_EIDL_PROCEED',
            'HEALTH_CARE_PROCEED',
            'DEBT_INTEREST_PROCEED',
            'RuralUrbanIndicator',
            'HubzoneIndicator',
            'LMIIndicator',
            'ProjectCity',
            'ProjectZip',
            'CD'
        ], inplace=True, errors='ignore')

        # Rename columns for consistency
        df.rename(columns={
            'LoanNumber': 'loan_number',
            'DateApproved': 'date_approved_id',
            'SBAOfficeCode': 'sba_office_code',
            'ProcessingMethod': 'processing_method',
            'BorrowerName': 'borrower_name',
            'BorrowerAddress': 'borrower_address',
            'BorrowerCity': 'borrower_city',
            'BorrowerState': 'borrower_state',
            'BorrowerZip': 'borrower_zip',
            'LoanStatusDate': 'loan_status_date_id',
            'LoanStatus': 'loan_status',
            'Term': 'term_month',
            'SBAGuarantyPercentage': 'sba_guaranty_percentage',
            'InitialApprovalAmount': 'initial_approval_amount',
            'CurrentApprovalAmount': 'current_approval_amount',
            'UndisbursedAmount': 'undisbursed_amount',
            'FranchiseName': 'franchise_name',
            'ServicingLenderLocationID': 'servicing_lender_location_id',
            'ServicingLenderName': 'servicing_lender_name',
            'ServicingLenderAddress': 'servicing_lender_address',
            'ServicingLenderCity': 'servicing_lender_city',
            'ServicingLenderState': 'servicing_lender_state',
            'ServicingLenderZip': 'servicing_lender_zip',
            'BusinessAgeDescription': 'business_age_description',
            'ProjectState': 'project_state',
            'ProjectCountyName': 'project_county_name',
            'Race': 'race',
            'Ethnicity': 'ethnicity',
            'Gender': 'gender',
            'BusinessType': 'business_type',
            'OriginatingLenderLocationID': 'originating_lender_location_id',
            'OriginatingLender': 'originating_lender',
            'OriginatingLenderCity': 'originating_lender_city',
            'OriginatingLenderState': 'originating_lender_state',
            'Veteran': 'veteran',
            'NonProfit': 'nonprofit',
            'ForgivenessAmount': 'forgiveness_amount',
            'ForgivenessDate': 'forgiveness_date_id',
            'JobsReported': 'jobs_reported',
            'NAICSCode': 'naics_code'
        }, inplace=True)

        dropped_df = pd.DataFrame(columns=df.columns.tolist() + ['drop_reason'])

        # Columns that can not contain empty values
        required_columns = [
            'borrower_state',
            'naics_code',
            'date_approved_id',
            'loan_status_date_id',
            'forgiveness_date_id',
            'jobs_reported',
            'business_type',
            'business_age_description'
        ]
        
        # Drop all rows where any of the required columns are empty
        for col in required_columns:
            mask = df[col].isnull()
            df, dropped_df = drop_and_log(df, dropped_df, mask, f"{col}_null_or_empty")
        
        # Drop the rows where business_age_description is 'Unanswered'
        mask_unanswered = df['business_age_description'] == 'Unanswered'
        df, dropped_df = drop_and_log(df, dropped_df, mask_unanswered, "business_age_description_unanswered")

        # Change date columns: 'date_approved_id', 'loan_status_date_id', 'forgiveness_date_id' to the date dimension format
        df['forgiveness_date_id'] = pd.to_datetime(df['forgiveness_date_id']).dt.strftime('%Y%m%d%H')
        df['date_approved_id'] = pd.to_datetime(df['date_approved_id']).dt.strftime('%Y%m%d%H')
        df['loan_status_date_id'] = pd.to_datetime(df['loan_status_date_id']).dt.strftime('%Y%m%d%H')

        # Change nonprofit to boolean
        df['nonprofit'] = df['nonprofit'].map({'Y': True})
        df['nonprofit'] = df['nonprofit'].fillna(False).astype(bool)

        # Change veteran to boolean
        df['veteran'] = df['veteran'].map({'veteran': True, 'Non-veteran': False, 'Unanswered':None})

        # Title case the string columns
        df['borrower_address'] = df['borrower_address'].str.title()
        df['borrower_city'] = df['borrower_city'].str.title()
        df['originating_lender_city'] = df['originating_lender_city'].str.title()
        df['servicing_lender_city'] = df['servicing_lender_city'].str.title()
        df['project_county_name'] = df['project_county_name'].str.title()

        #df['loan_number'] = df['loan_number'].astype(int)
        df['date_approved_id'] = df['date_approved_id'].astype(int)
        df['sba_office_code'] = df['sba_office_code'].astype(int)
        df['processing_method'] = df['processing_method'].astype(pd.StringDtype("pyarrow"))
        df['borrower_name'] = df['borrower_name'].astype(pd.StringDtype("pyarrow"))
        df['borrower_address'] = df['borrower_address'].astype(pd.StringDtype("pyarrow"))
        df['borrower_city'] = df['borrower_city'].astype(pd.StringDtype("pyarrow"))
        df['borrower_state'] = df['borrower_state'].astype(pd.StringDtype("pyarrow"))
        df['borrower_zip'] = df['borrower_zip'].astype(pd.StringDtype("pyarrow"))
        df['loan_status_date_id'] = df['loan_status_date_id'].astype(int)
        df['loan_status'] = df['loan_status'].astype(pd.StringDtype("pyarrow"))
        df['term_month'] = df['term_month'].astype(int)
        df['sba_guaranty_percentage'] = df['sba_guaranty_percentage'].astype(float)
        df['initial_approval_amount'] = df['initial_approval_amount'].astype(float)
        df['current_approval_amount'] = df['current_approval_amount'].astype(float)
        df['undisbursed_amount'] = df['undisbursed_amount'].astype(float)
        df['franchise_name'] = df['franchise_name'].astype(pd.StringDtype("pyarrow"))
        df['servicing_lender_location_id'] = df['servicing_lender_location_id'].astype(int)
        df['servicing_lender_name'] = df['servicing_lender_name'].astype(pd.StringDtype("pyarrow"))
        df['servicing_lender_address'] = df['servicing_lender_address'].astype(pd.StringDtype("pyarrow"))
        df['servicing_lender_city'] = df['servicing_lender_city'].astype(pd.StringDtype("pyarrow"))
        df['servicing_lender_state'] = df['servicing_lender_state'].astype(pd.StringDtype("pyarrow"))
        df['servicing_lender_zip'] = df['servicing_lender_zip'].astype(pd.StringDtype("pyarrow"))
        df['business_age_description'] = df['business_age_description'].astype(pd.StringDtype("pyarrow"))
        df['project_state'] = df['project_state'].astype(pd.StringDtype("pyarrow"))
        df['project_county_name'] = df['project_county_name'].astype(pd.StringDtype("pyarrow"))
        df['race'] = df['race'].astype(pd.StringDtype("pyarrow"))
        df['ethnicity'] = df['ethnicity'].astype(pd.StringDtype("pyarrow"))
        df['gender'] = df['gender'].astype(pd.StringDtype("pyarrow"))
        df['business_type'] = df['business_type'].astype(pd.StringDtype("pyarrow"))
        df['originating_lender_location_id'] = df['originating_lender_location_id'].astype(int)
        df['originating_lender'] = df['originating_lender'].astype(pd.StringDtype("pyarrow"))
        df['originating_lender_city'] = df['originating_lender_city'].astype(pd.StringDtype("pyarrow"))
        df['originating_lender_state'] = df['originating_lender_state'].astype(pd.StringDtype("pyarrow"))
        df['veteran'] = df['veteran'].astype(bool)
        df['nonprofit'] = df['nonprofit'].astype(bool)
        df['forgiveness_amount'] = df['forgiveness_amount'].astype(float)
        df['forgiveness_date_id'] = df['forgiveness_date_id'].astype(int)
        df['jobs_reported'] = df['jobs_reported'].astype(int)
        df['naics_code'] = df['naics_code'].astype(int)

        # Assing a facts_ppp_id to each row
        df['facts_ppp_id'] = [facts_ppp_id + i for i in range(len(df))]
        facts_ppp_id += len(df)
        

        print(f"Cleaned PPP data has {len(df)} rows and {len(df.columns)} columns.")

        # Save the cleaned data to a CSV file
        cleaned_blob_name = f"{PPP_FOLDER}/cleaned_{blob_name.split('/')[-1]}"
        output = df_to_bytesio(df, index=False, encoding='utf-8')
        # Upload the cleaned data to Azure Blob Storage
        upload_to_cloud(data=output, blob_name=cleaned_blob_name, container_name=CLEAN_CONTAINER)

        if not dropped_df.empty:
            print(f"\nDropped PPP data has {len(dropped_df)} rows.")
            dropped_blob_name = f"{PPP_FOLDER}/dropped_raw_{blob_name.split('/')[-1]}"
            dropped_output = df_to_bytesio(dropped_df, index=False, encoding='utf-8')
            upload_to_cloud(data=dropped_output, blob_name=dropped_blob_name, container_name=DROPPED_CONTAINER)
            print(f"\nDropped PPP data uploaded to {dropped_blob_name} in {DROPPED_CONTAINER} container.\n")

    print("\nPPP data cleaning completed.\n")
