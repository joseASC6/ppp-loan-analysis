import pandas as pd
import io
from .common import download_from_azure, upload_to_azure, get_blob_list

def clean_ppp_data():
    """
    Cleans PPP data by
    Converts the datatypes.
    Uploads the cleaned data to Azure Blob Storage.
    """
    raw_container = "raw-data"
    ppp_blob_name = "PPP-data/"
    # Find all the files in the PPP-data folder in the raw container
    ppp_blob_list = get_blob_list(raw_container, ppp_blob_name)
    if not ppp_blob_list:
        print("No PPP data found in the raw container.")
        return
    print(f"Found {len(ppp_blob_list)} PPP data files in the raw container.")

    for blob_name in ppp_blob_list:
        # Download the PPP data from Azure Blob Storage
        ppp_data = download_from_azure(blob_name, raw_container)
        if not ppp_data:
            print(f"Failed to download PPP data from {blob_name}.")
            continue

        # Read the CSV file into a DataFrame
        print(f"Reading PPP data from {blob_name}...")
        df = pd.read_csv(ppp_data, encoding="latin-1")
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
        df.dropna(subset=required_columns, inplace=True)

        # Change date columns: 'date_approved_id', 'loan_status_date_id', 'forgiveness_date_id' to the date dimension format
        df['forgiveness_date_id'] = pd.to_datetime(df['forgiveness_date_id']).dt.strftime('%Y%m%d%H')
        df['date_approved_id'] = pd.to_datetime(df['date_approved_id']).dt.strftime('%Y%m%d%H')
        df['loan_status_date_id'] = pd.to_datetime(df['loan_status_date_id']).dt.strftime('%Y%m%d%H')
