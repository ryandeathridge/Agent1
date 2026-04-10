"""Handles reading/writing data to Azure Blob Storage."""
import os
import io
import json
import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta


def _get_blob_service_client() -> BlobServiceClient:
    """Get blob service client from environment variables."""
    connection_string = os.environ.get("BLOB_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("BLOB_CONNECTION_STRING environment variable not set")
    return BlobServiceClient.from_connection_string(connection_string)


def _get_container_name() -> str:
    """Get container name from environment variables."""
    container_name = os.environ.get("BLOB_CONTAINER_NAME", "scp-cleaning")
    return container_name


def upload_dataframe(df: pd.DataFrame, blob_name: str, format: str = "parquet") -> str:
    """Upload a pandas DataFrame to blob storage. Returns blob URL.
    
    Supports parquet (fast, compact) and csv formats.
    Uses BLOB_CONNECTION_STRING and BLOB_CONTAINER_NAME from environment.
    """
    blob_service_client = _get_blob_service_client()
    container_name = _get_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    buffer = io.BytesIO()
    if format == "parquet":
        df.to_parquet(buffer, index=False)
    elif format == "csv":
        df.to_csv(buffer, index=False)
    else:
        raise ValueError(f"Unsupported format: {format}")
    
    buffer.seek(0)
    blob_client.upload_blob(buffer, overwrite=True)
    
    return blob_client.url


def download_dataframe(blob_url: str) -> pd.DataFrame:
    """Download a DataFrame from blob storage. Auto-detects format from extension."""
    blob_client = BlobClient.from_blob_url(blob_url)
    
    stream = io.BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)
    
    if blob_url.endswith(".parquet"):
        return pd.read_parquet(stream)
    elif blob_url.endswith(".csv"):
        return pd.read_csv(stream)
    elif blob_url.endswith(".xlsx"):
        return pd.read_excel(stream)
    else:
        try:
            return pd.read_parquet(stream)
        except Exception:
            stream.seek(0)
            return pd.read_csv(stream)


def upload_json(data: dict, blob_name: str) -> str:
    """Upload a JSON object to blob storage. Returns blob URL."""
    blob_service_client = _get_blob_service_client()
    container_name = _get_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    json_str = json.dumps(data, indent=2)
    blob_client.upload_blob(json_str, overwrite=True)
    
    return blob_client.url


def download_json(blob_url: str) -> dict:
    """Download a JSON object from blob storage."""
    blob_client = BlobClient.from_blob_url(blob_url)
    
    stream = io.BytesIO()
    blob_client.download_blob().readinto(stream)
    stream.seek(0)
    
    return json.loads(stream.read().decode('utf-8'))


def generate_sas_url(blob_name: str, expiry_hours: int = 24) -> str:
    """Generate a SAS URL for temporary access to a blob. Used for download links."""
    blob_service_client = _get_blob_service_client()
    container_name = _get_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    account_name = blob_service_client.account_name
    account_key = None
    
    connection_string = os.environ.get("BLOB_CONNECTION_STRING")
    if "AccountKey=" in connection_string:
        account_key = connection_string.split("AccountKey=")[1].split(";")[0]
    
    if not account_key:
        return blob_client.url
    
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=expiry_hours)
    )
    
    return f"{blob_client.url}?{sas_token}"
