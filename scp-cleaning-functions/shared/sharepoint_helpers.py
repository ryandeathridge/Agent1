"""Reads/writes the self-updating instruction files and dictionaries on SharePoint."""
import os
import json
import requests
from typing import Optional


def get_access_token() -> str:
    """Get OAuth2 token for Microsoft Graph API using client credentials."""
    tenant_id = os.environ.get("SHAREPOINT_TENANT_ID")
    client_id = os.environ.get("SHAREPOINT_CLIENT_ID")
    client_secret = os.environ.get("SHAREPOINT_CLIENT_SECRET")
    
    if not all([tenant_id, client_id, client_secret]):
        raise ValueError("SharePoint credentials not configured in environment variables")
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials"
    }
    
    response = requests.post(token_url, data=data)
    response.raise_for_status()
    
    return response.json()["access_token"]


def _get_file_url(file_path: str) -> str:
    """Build Microsoft Graph API URL for a file."""
    site_id = os.environ.get("SHAREPOINT_SITE_ID")
    if not site_id:
        raise ValueError("SHAREPOINT_SITE_ID environment variable not set")
    
    return f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}"


def read_sharepoint_json(file_path: str) -> dict:
    """Read a JSON file from SharePoint document library.
    
    file_path is relative to the site's default document library, 
    e.g. 'SCP/vendor_dictionary.json'
    """
    access_token = get_access_token()
    file_url = _get_file_url(file_path)
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(file_url, headers=headers)
    
    if response.status_code == 404:
        return {}
    
    response.raise_for_status()
    
    download_url = response.json().get("@microsoft.graph.downloadUrl")
    if not download_url:
        raise ValueError(f"Could not get download URL for {file_path}")
    
    content_response = requests.get(download_url)
    content_response.raise_for_status()
    
    return content_response.json()


def write_sharepoint_json(file_path: str, data: dict) -> None:
    """Write/overwrite a JSON file on SharePoint."""
    access_token = get_access_token()
    file_url = f"{_get_file_url(file_path)}:/content"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    json_content = json.dumps(data, indent=2)
    
    response = requests.put(file_url, headers=headers, data=json_content.encode('utf-8'))
    response.raise_for_status()


def read_sharepoint_text(file_path: str) -> str:
    """Read a text/markdown file from SharePoint."""
    access_token = get_access_token()
    file_url = _get_file_url(file_path)
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(file_url, headers=headers)
    
    if response.status_code == 404:
        return ""
    
    response.raise_for_status()
    
    download_url = response.json().get("@microsoft.graph.downloadUrl")
    if not download_url:
        raise ValueError(f"Could not get download URL for {file_path}")
    
    content_response = requests.get(download_url)
    content_response.raise_for_status()
    
    return content_response.text


def write_sharepoint_text(file_path: str, content: str) -> None:
    """Write/overwrite a text/markdown file on SharePoint."""
    access_token = get_access_token()
    file_url = f"{_get_file_url(file_path)}:/content"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "text/plain"
    }
    
    response = requests.put(file_url, headers=headers, data=content.encode('utf-8'))
    response.raise_for_status()


def append_to_sharepoint_json(file_path: str, new_entries: dict, merge_key: str) -> int:
    """Read existing JSON, merge new entries (by merge_key to avoid duplicates), write back.
    
    Returns count of new entries added.
    """
    existing_data = read_sharepoint_json(file_path)
    
    if not existing_data:
        existing_data = {}
    
    new_count = 0
    for key, value in new_entries.items():
        if key not in existing_data:
            existing_data[key] = value
            new_count += 1
    
    write_sharepoint_json(file_path, existing_data)
    
    return new_count
