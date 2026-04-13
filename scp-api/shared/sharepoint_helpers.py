"""Reads/writes the self-updating instruction files and dictionaries in Azure Blob Storage."""
import os
import json
import io
from typing import Optional, Union
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError


def _get_blob_service_client() -> BlobServiceClient:
    """Get blob service client from environment variables."""
    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable not set")
    return BlobServiceClient.from_connection_string(connection_string)


def _get_config_container_name() -> str:
    """Get config container name from environment variables."""
    return os.environ.get("CONFIG_CONTAINER_NAME", "config")


def read_config_json(file_path: str) -> Union[dict, list]:
    """Read a JSON file from the config blob container.
    
    file_path is relative to the config container, e.g. 'vendor_dictionary.json'
    Returns empty dict {} if file not found (for dict files) or empty list [] (for list files).
    """
    blob_service_client = _get_blob_service_client()
    container_name = _get_config_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
    
    try:
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        content = json.loads(stream.read().decode('utf-8'))
        return content
    except ResourceNotFoundError:
        if 'examples' in file_path:
            return []
        return {}
    except Exception as e:
        raise ValueError(f"Error reading config file {file_path}: {str(e)}")


def write_config_json(file_path: str, data: Union[dict, list]) -> None:
    """Write/overwrite a JSON file in the config blob container."""
    blob_service_client = _get_blob_service_client()
    container_name = _get_config_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
    
    json_str = json.dumps(data, indent=2)
    blob_client.upload_blob(json_str, overwrite=True)


def read_config_text(file_path: str) -> str:
    """Read a text/markdown file from the config blob container.
    
    Returns empty string if file not found.
    """
    blob_service_client = _get_blob_service_client()
    container_name = _get_config_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
    
    try:
        stream = io.BytesIO()
        blob_client.download_blob().readinto(stream)
        stream.seek(0)
        return stream.read().decode('utf-8')
    except ResourceNotFoundError:
        return ""
    except Exception as e:
        raise ValueError(f"Error reading config file {file_path}: {str(e)}")


def write_config_text(file_path: str, content: str) -> None:
    """Write/overwrite a text/markdown file in the config blob container."""
    blob_service_client = _get_blob_service_client()
    container_name = _get_config_container_name()
    
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_path)
    
    blob_client.upload_blob(content.encode('utf-8'), overwrite=True)
