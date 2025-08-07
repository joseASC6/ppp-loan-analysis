import os
from dotenv import load_dotenv
from typing import Optional
from pathlib import Path
# Load environment variables from .env file
load_dotenv(dotenv_path=Path(__file__).parents[2] / ".env")

def get_env_var(key: str, fallback: Optional[str] = None) -> str:
    """Retrieve an environment variable with optional fallback."""
    value = os.getenv(key, fallback)
    if value is None:
        raise ValueError(f"Missing required environment variable: {key}")
    return value

# Common environment variables
AZURE_CONNECTION_STRING = get_env_var("AZURE_CONNECTION_STRING")
DW_CONNECTION_STRING = get_env_var("DW_CONNECTION_STRING")
DB_SCHEMA = get_env_var("DB_SCHEMA")
CLOUD_PROVIDER = get_env_var("CLOUD_PROVIDER", "azure")
GCS_BUCKET_NAME = get_env_var("GCS_BUCKET_NAME", "ppp-loan-analysis")