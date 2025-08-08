import pandas as pd
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud, get_blob_list_from_cloud
from config.config import FINAL_CONTAINER
import io
import calendar

def transform_dim_date():
    """
    Custom function to load dimension date data.
    Start date: 2017-01-01 00:00:00
    2017 is the minimum year in the GDP data

    End date: 2023-10-1 00:00:00 
    October 2023 is the maximum date in the PPP data
    """
    print("Transforming dim_date...\n")
    # Function to get the week of the month
    def week_of_month(dt):
        year = dt.year
        month = dt.month
        day = dt.day

        cal = calendar.monthcalendar(year, month)
        week_number = (day - 1) // 7 + 1
        return week_number
    final_container = "final-data"

    # Use the facts_ppp data to determine the start and end dates
    print("Determining start and end dates for dim_date...")
    start_date = pd.Timestamp("2017-01-01 00:00:00") # Minimum date in the GDP data
    end_date = pd.Timestamp("2024-09-30 00:00:00") # Last date in the PPP data

    ppp_blobs = get_blob_list_from_cloud(final_container, prefix="facts_ppp")
    if not ppp_blobs:
        print("No facts_ppp blobs found. Using default start and end dates.")
    else:
        date_cols = ['date_approved_id', 'loan_status_date_id', 'forgiveness_date_id']
        for blob_name in ppp_blobs:
            data = download_from_cloud(blob_name=blob_name, container_name=final_container)
            df = pd.read_csv(data, encoding="utf-8") 
            for col in date_cols:
                if col in df.columns:
                    # dates are in the following format: 2021072200 '%Y%m%d%H'
                    dates = pd.to_datetime(df[col], format='%Y%m%d%H', errors='coerce')
                    col_min_date = dates.min()
                    col_max_date = dates.max()
                    print(f"Blob: {blob_name}, {col} - Min: {col_min_date}, Max: {col_max_date}")
                    start_date = min(start_date, col_min_date)
                    end_date = max(end_date, col_max_date)
                    
    print(f"Start date: {start_date}, End date: {end_date}")
    # Create a DataFrame for the date dimension
    dim_date = pd.DataFrame({'date': pd.date_range(start_date, end_date, freq='h')})

    # Extract attributes
    dim_date['year_number'] = dim_date['date'].dt.year
    dim_date['quarter_number'] = dim_date['date'].dt.quarter #quarter_number
    dim_date['month_number'] = dim_date['date'].dt.month
    dim_date['month_name'] = dim_date['date'].dt.strftime('%B')
    dim_date['day_number'] = dim_date['date'].dt.day #day_number
    dim_date['day_name'] = dim_date['date'].dt.strftime('%A') #day_name
    dim_date['hour_number'] = dim_date['date'].dt.hour #hour_number
    dim_date['date_iso_format'] = dim_date['date'].apply(lambda x: x.isoformat())
    dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d%H')

    # Add week of the month and week of the year
    dim_date['week_of_month'] = dim_date['date'].apply(week_of_month) #week_of_month
    dim_date['week_of_year'] = dim_date['date'].dt.strftime('%U') #week_of_year

    # Reorder columns for final output
    new_order = [
        'date_id',
        'date_iso_format',
        'year_number',
        'quarter_number',
        'month_number',
        'day_number',
        'hour_number',
        'month_name',
        'day_name',
        'week_of_year',
        'week_of_month'
    ]
    dim_date = dim_date[new_order]

    print(f"Transformed dim_date has {len(dim_date)} rows and {len(dim_date.columns)} columns.")

    # Upload the dimension date data to Cloud Storage
    dim_date_blob_name = "dim_date.csv"
    output = df_to_bytesio(dim_date, index=False, encoding='utf-8')
    upload_to_cloud(data=output, blob_name=dim_date_blob_name, container_name=FINAL_CONTAINER)
    print("dim_date transformation completed.\n")

