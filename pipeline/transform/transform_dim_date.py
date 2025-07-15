import pandas as pd
from utils.common import upload_to_azure, df_to_bytesio
import calendar
import io

def transform_dim_date():
    """
    Custom function to load dimension date data.
    Start date: 2017-01-01 00:00:00
    2017 is the minimum year in the GDP data

    End date: 2023-10-1 00:00:00 
    October 2023 is the maximum date in the PPP data
    """
    # Function to get the week of the month
    def week_of_month(dt):
        year = dt.year
        month = dt.month
        day = dt.day

        cal = calendar.monthcalendar(year, month)
        week_number = (day - 1) // 7 + 1
        return week_number


    start_date = pd.Timestamp("2017-01-01 00:00:00")
    end_date = pd.Timestamp("2023-10-01 00:00:00")
   

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

    # Upload the dimension date data to Azure Blob Storage
    final_container = "final-data"
    dim_date_blob_name = "dim_date.csv"
    output = df_to_bytesio(dim_date, index=False, encoding='utf-8')
    upload_to_azure(output, dim_date_blob_name, final_container)

