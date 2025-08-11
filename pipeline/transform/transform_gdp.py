import pandas as pd
import io
from utils.common import df_to_bytesio, download_from_cloud, upload_to_cloud
from config.config import CLEAN_CONTAINER, FINAL_CONTAINER, GDP_FOLDER

def create_dim_geography(df):
    dim_geography = df[['geofips', 'geo_name', 'region']].drop_duplicates()
    dim_geography = dim_geography.reset_index(drop=True)

    # Remove the * from the geo_name
    dim_geography['geo_name'] = dim_geography['geo_name'].str.replace('*', '')


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

    return dim_geography

def create_facts_gdp(df):
    facts_gdp = df[['facts_gdp_id', 'year_id', 'real_gdp', 'chain_type_index_gdp', 'current_dollar_gdp', 'geofips']]
    facts_gdp.reset_index(drop=True, inplace=True)
    facts_gdp = facts_gdp[['facts_gdp_id', 'geofips', 'year_id', 'chain_type_index_gdp', 'current_dollar_gdp', 'real_gdp']]
    return facts_gdp

def transform_gdp_data():
    """
    Loads clean GDP data from cloud storage.
    Transforms the data to fit facts and dimensions schema:
    - facts_gdp
    - dim_geography
    Uploads the transformed data to cloud storage.
    """
    print("Transforming GDP data...\n")
    clean_gdp_blob_name = f"{GDP_FOLDER}/cleaned_gdp_data.csv"

    # Download the cleaned GDP data from cloud storage
    gdp_data = download_from_cloud(blob_name=clean_gdp_blob_name, container_name=CLEAN_CONTAINER)

    # Read the CSV file into a DataFrame
    print(f"Reading cleaned GDP data from {clean_gdp_blob_name}...")
    df = pd.read_csv(gdp_data, encoding="utf-8")
    print(f"Cleaned GDP data has {len(df)} rows and {len(df.columns)} columns.")

    facts_gdp = create_facts_gdp(df)
    dim_geography = create_dim_geography(df)

    print(f"Transformed facts_gdp has {len(facts_gdp)} rows and {len(facts_gdp.columns)} columns.")
    print(f"Transformed dim_geography has {len(dim_geography)} rows and {len(dim_geography.columns)} columns.")

    # Upload dim_geography and facts_gdp to cloud storage
    dim_geography_blob_name = "dim_geography.csv"
    facts_gdp_blob_name = "facts_gdp.csv"
    
    output_geography = df_to_bytesio(dim_geography, index=False, encoding='utf-8')
    upload_to_cloud(data=output_geography, blob_name=dim_geography_blob_name, container_name=FINAL_CONTAINER)

    output_gdp = df_to_bytesio(facts_gdp, index=False, encoding='utf-8')
    upload_to_cloud(data=output_gdp, blob_name=facts_gdp_blob_name, container_name=FINAL_CONTAINER)

    print("GDP data transformation completed.\n")