import pandas as pd
import io
from .common import download_from_azure, upload_to_azure, get_blob_list, upload_to_sql

def load_gdp_data():
    """
    Loads clean GDP data from Azure Blob Storage
    Formats the data to fit facts and dimensions schema:
    - facts_gdp
    - dim_geography
    Uploads the formatted data to Azure Blob Storage and Azure SQL Database.
    """
    cleaned_container = "cleaned-data"
    clean_gdp_blob_name = "GDP-data/cleaned_gdp_data.csv"

    # Download the cleaned GDP data from Azure Blob Storage
    gdp_data = download_from_azure(clean_gdp_blob_name, cleaned_container)

    # Read the CSV file into a DataFrame
    print(f"Reading cleaned GDP data from {clean_gdp_blob_name}...")
    df = pd.read_csv(gdp_data, encoding="latin-1")
    print(f"Cleaned GDP data has {len(df)} rows and {len(df.columns)} columns.")

    dim_geography = df[['geofips', 'geo_name', 'region']].drop_duplicates()
    dim_geography = dim_geography.reset_index(drop=True)

    #Remove these phrase from the geo_name: City and Borough, Borough, Census Area, Municipality, (Independent City)
    remove_phrases = [
        " City and Borough",
        " Borough",
        " Census Area",
        " Municipality",
        " (Independent City)"
    ]
    for phrase in remove_phrases:
        dim_geography['geo_name'] = dim_geography['geo_name'].str.replace(phrase, '', regex=False)

    # Special cases. Ex: Augusta, Staunton + Waynesboro, VA -> Augusta, VA
    dim_geography['geo_name'] = dim_geography['geo_name'].str.replace(r'(.+),.+,', r'\1,')

    # Split the geo_name into project_state and project_county_name
    dim_geography['project_state'] = dim_geography['geo_name'].str.split(',').str[1].str.strip()
    dim_geography['project_county_name'] = dim_geography['geo_name'].str.split(',').str[0].str.strip()

    # Temporarily set geofips to string
    dim_geography['geofips'] = dim_geography['geofips'].astype(str)
    dim_geography['geo_name'] = dim_geography['geo_name'].astype(str)

    # Set the project_state and project_county_name for the United States
    dim_geography.loc[dim_geography['geofips'] == '0', 'project_state'] = 'All States'
    dim_geography.loc[dim_geography['geofips'] == '0', 'project_county_name'] = 'All Counties'

    # Set the project_state and project_county_name for the States
    dim_geography.loc[dim_geography['geofips'].str.endswith('000'), 'project_state'] = dim_geography['geo_name']
    dim_geography.loc[dim_geography['geofips'].str.endswith('000'), 'project_county_name'] = 'All Counties'

    # Set the data types
    dim_geography['project_state'] = dim_geography['project_state'].astype(pd.StringDtype("pyarrow"))
    dim_geography['project_county_name'] = dim_geography['project_county_name'].astype(pd.StringDtype("pyarrow"))

    facts_gdp = df[['facts_gdp_id', 'year_id', 'real_gdp', 'chain_type_index_gdp', 'current_dollar_gdp', 'geofips']]
    # Reset the index
    facts_gdp.reset_index(drop=True, inplace=True)
    # Re order the columns
    facts_gdp = facts_gdp[['facts_gdp_id', 'geofips', 'year_id', 'chain_type_index_gdp', 'current_dollar_gdp', 'real_gdp']]
    
    # Upload dim_geography and facts_gdp to Azure Blob Storage
    final_container = "final-data"
    dim_geography_blob_name = "dim_geography.csv"
    facts_gdp_blob_name = "facts_gdp.csv"

    output_geography = io.BytesIO()
    dim_geography.to_csv(output_geography, index=False, encoding='utf-8')
    output_geography.seek(0)
    upload_to_azure(output_geography, dim_geography_blob_name, final_container)

    output_gdp = io.BytesIO()
    facts_gdp.to_csv(output_gdp, index=False, encoding='utf-8')
    output_gdp.seek(0)
    upload_to_azure(output_gdp, facts_gdp_blob_name, final_container)

    # Upload dim_geography and facts_gdp to Azure SQL Database
    upload_to_sql(dim_geography, 'dim_geography')
    upload_to_sql(facts_gdp, 'facts_gdp')


    