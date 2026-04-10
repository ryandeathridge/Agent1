"""Format output function - formats cleaned data for download."""
import logging
import json
import azure.functions as func
import pandas as pd
import io
from datetime import datetime, timezone
from shared.blob_helpers import download_dataframe, generate_sas_url
from azure.storage.blob import BlobServiceClient
import os


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Format output endpoint."""
    logging.info('Format output function triggered')
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    
    blob_url = req_body.get('blob_url')
    format_type = req_body.get('format', 'xlsx')
    include_changes_log = req_body.get('include_changes_log', False)
    changes_blob_url = req_body.get('changes_blob_url')
    
    if not blob_url:
        return func.HttpResponse(
            json.dumps({"error": "blob_url is required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        df = download_dataframe(blob_url)
        
        connection_string = os.environ.get("BLOB_CONNECTION_STRING")
        container_name = os.environ.get("BLOB_CONTAINER_NAME", "scp-cleaning")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        import uuid
        run_id = str(uuid.uuid4())[:8]
        
        if format_type == 'xlsx':
            output_blob_name = f"output_cleaned_{run_id}.xlsx"
            
            buffer = io.BytesIO()
            
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Cleaned Data', index=False)
                
                if include_changes_log and changes_blob_url:
                    try:
                        changes_df = download_dataframe(changes_blob_url)
                        changes_df.to_excel(writer, sheet_name='Changes Log', index=False)
                    except Exception as e:
                        logging.warning(f"Could not load changes log: {e}")
                
                summary_data = {
                    'Metric': [
                        'Total Records',
                        'Total Columns',
                        'Generated At'
                    ],
                    'Value': [
                        len(df),
                        len(df.columns),
                        datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
                    ]
                }
                
                confidence_cols = [col for col in df.columns if col.endswith('_confidence')]
                if confidence_cols:
                    for col in confidence_cols:
                        field_name = col.replace('_confidence', '')
                        avg_confidence = df[col].mean()
                        summary_data['Metric'].append(f'{field_name} Avg Confidence')
                        summary_data['Value'].append(f'{avg_confidence:.2%}')
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            buffer.seek(0)
            
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=output_blob_name
            )
            blob_client.upload_blob(buffer, overwrite=True)
            
            download_url = generate_sas_url(output_blob_name, expiry_hours=24)
            
            file_size_mb = len(buffer.getvalue()) / (1024 * 1024)
            
        elif format_type == 'csv':
            output_blob_name = f"output_cleaned_{run_id}.csv"
            
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode('utf-8')
            
            blob_client = blob_service_client.get_blob_client(
                container=container_name, 
                blob=output_blob_name
            )
            blob_client.upload_blob(csv_bytes, overwrite=True)
            
            download_url = generate_sas_url(output_blob_name, expiry_hours=24)
            
            file_size_mb = len(csv_bytes) / (1024 * 1024)
        
        else:
            return func.HttpResponse(
                json.dumps({"error": f"Unsupported format: {format_type}"}),
                status_code=400,
                mimetype="application/json"
            )
        
        expires = (datetime.now(timezone.utc).replace(microsecond=0) + 
                  pd.Timedelta(hours=24)).isoformat().replace('+00:00', 'Z')
        
        return func.HttpResponse(
            json.dumps({
                "download_url": download_url,
                "expires": expires,
                "format": format_type,
                "file_size_mb": round(file_size_mb, 2)
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error formatting output: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
