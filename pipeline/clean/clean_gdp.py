import pandas as pd
import io
from .common import download_from_azure, upload_to_azure, get_blob_list

def clean_gdp_data():
    """
    Cleans GDP data by 
    Converts the datatypes.
    Uploads the cleaned data to Azure Blob Storage.
    """
    raw_container = "raw-data"
    gdp_blob_name = "GDP-data/CAGDP1__ALL_AREAS_2001_2023.csv"
    # Download the GDP data from Azure Blob Storage
    gdp_data = download_from_azure(gdp_blob_name, raw_container)
    # Read the CSV file into a DataFrame
    print(f"Reading GDP data from {gdp_blob_name}...")
    df = pd.read_csv(gdp_data, encoding="latin-1")
    print(f"GDP data has {len(df)} rows and {len(df.columns)} columns.")
    # Clean the GDP data
    print("Cleaning GDP data...")
    #Drop all the records where 2017, 2018, 2019, 2020, 2021, 2022 = "(NA)" 
    # Ex: df_gdp = df_gdp[df_gdp['2017'] != "(NA)"]
    for year in ['2017', '2018', '2019', '2020', '2021', '2022']:
        df = df[df[year] != "(NA)"]

    # Keep only the columns we need
    selected_columns = ['GeoFIPS', 'GeoName', 'Region', 'Description', '2017', '2018', '2019', '2020', '2021', '2022']
    df = df[selected_columns]

    # Pivot the dataframe
    df = df.melt(id_vars=["GeoFIPS", "GeoName", "Region", "Description"],
                                    value_vars=["2017", "2018", "2019", "2020", "2021", "2022"],
                                    var_name="date_id",
                                    value_name="Value")
    df = df.pivot_table(index=["GeoFIPS", "GeoName", "Region", "date_id"], columns="Description", values="Value", aggfunc='first').reset_index()
    df = df.sort_values(by=["GeoFIPS", "date_id"])

    # Rename the columns
    df.rename(columns={
       "Chain-type quantity indexes for real GDP ": "chain_type_index_gdp",
        "Current-dollar GDP (thousands of current dollars) ": "current_dollar_gdp",
        "Real GDP (thousands of chained 2017 dollars) ": "real_gdp",
        "GeoFIPS": "geofips",
        "GeoName": "geo_name",
        "date_id": "year_id",
        "Region": "region"
    }, inplace=True)

    df['facts_gdp_id'] = range(1, len(df) + 1)
    df = df.drop(columns='Description', errors='ignore')
    df = df[["facts_gdp_id", "geofips", "geo_name", "region", "year_id", "chain_type_index_gdp", "current_dollar_gdp", "real_gdp"]]
    print(f"Cleaned GDP data has {len(df)} rows and {len(df.columns)} columns.")

    # Change the datatypes
    # Remove quotation marks from the geofips
    df['geofips'] = df['geofips'].str.replace('"', '', regex=False)

    # Change year_id to match format in dim_date table
    df['year_id'] = pd.to_datetime(df['year_id'], format='%Y').dt.strftime('%Y%m%d%H')

    df['year_id'] = df['year_id'].astype(int)
    df['geofips'] = df['geofips'].astype(int)
    df['geo_name'] = df['geo_name'].astype(pd.StringDtype("pyarrow"))
    df['region'] = df['region'].astype(pd.StringDtype("pyarrow"))    
    df['chain_type_index_gdp'] = df['chain_type_index_gdp'].astype(float)
    df['current_dollar_gdp'] = df['current_dollar_gdp'].astype(float)
    df['real_gdp'] = df['real_gdp'].astype(float)

    # Upload the cleaned data to Azure Blob Storage
    output = io.BytesIO()
    df.to_csv(output, index=False, encoding='utf-8')
    output.seek(0)
    cleaned_blob_name = "GDP-data/cleaned_gdp_data.csv"
    clean_container = "cleaned-data"
    upload_to_azure(output, cleaned_blob_name, clean_container)