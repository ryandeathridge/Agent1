"""Upload data function - accepts file uploads and writes to blob storage."""
import logging
import json
import uuid
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Upload data endpoint - accepts xlsx or csv files."""
    logging.info('Upload data function triggered')
    
    try:
        file = req.files.get('file')
        
        if not file:
            return func.HttpResponse(
                json.dumps({"error": "No file provided. Expected 'file' field in multipart/form-data"}),
                status_code=400,
                mimetype="application/json"
            )
        
        original_filename = file.filename
        if not original_filename:
            return func.HttpResponse(
                json.dumps({"error": "Filename not provided"}),
                status_code=400,
                mimetype="application/json"
            )
        
        file_extension = original_filename.lower().split('.')[-1] if '.' in original_filename else ''
        if file_extension not in ['xlsx', 'csv']:
            return func.HttpResponse(
                json.dumps({"error": f"Invalid file type. Only .xlsx and .csv files are accepted. Got: .{file_extension}"}),
                status_code=400,
                mimetype="application/json"
            )
        
        file_content = file.read()
        file_size = len(file_content)
        
        unique_filename = f"{uuid.uuid4()}_{original_filename}"
        
        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            return func.HttpResponse(
                json.dumps({"error": "AZURE_STORAGE_CONNECTION_STRING not configured"}),
                status_code=500,
                mimetype="application/json"
            )
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = "dirty-data"
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=unique_filename)
        
        blob_client.upload_blob(file_content, overwrite=True)
        
        blob_url = blob_client.url
        
        logging.info(f"File uploaded successfully: {unique_filename} ({file_size} bytes)")
        
        return func.HttpResponse(
            json.dumps({
                "blob_url": blob_url,
                "filename": unique_filename,
                "size_bytes": file_size
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
