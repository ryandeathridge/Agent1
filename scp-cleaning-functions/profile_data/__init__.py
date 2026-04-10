"""Profile data function - analyzes data quality and structure."""
import logging
import json
import azure.functions as func
import pandas as pd
import numpy as np
import chardet
from shared.blob_helpers import download_dataframe, download_json
from shared.deduplicator import Deduplicator
from shared.models import ProfileResult, ColumnProfile


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Profile data endpoint."""
    logging.info('Profile data function triggered')
    
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON in request body"}),
            status_code=400,
            mimetype="application/json"
        )
    
    blob_url = req_body.get('blob_url')
    config = req_body.get('config')
    config_blob_url = req_body.get('config_blob_url')
    
    if not blob_url:
        return func.HttpResponse(
            json.dumps({"error": "blob_url is required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    if not config and config_blob_url:
        config = download_json(config_blob_url)
    
    try:
        df = download_dataframe(blob_url)
        
        columns = []
        encoding_issues = 0
        
        for col in df.columns:
            series = df[col]
            
            dtype_detected = _detect_dtype(series)
            null_count = int(series.isna().sum())
            null_rate = float(null_count / len(series))
            unique_count = int(series.nunique())
            cardinality = float(unique_count / len(series)) if len(series) > 0 else 0.0
            
            sample_values = series.dropna().head(5).astype(str).tolist()
            
            anomaly_flags = []
            distribution_summary = {}
            
            if dtype_detected == "string":
                for val in series.dropna().head(100):
                    if isinstance(val, str):
                        try:
                            val_bytes = val.encode('utf-8')
                            detected = chardet.detect(val_bytes)
                            if detected['encoding'] and detected['encoding'].lower() not in ['utf-8', 'ascii']:
                                encoding_issues += 1
                                anomaly_flags.append("encoding issues detected")
                                break
                        except Exception:
                            pass
                
                lengths = series.dropna().astype(str).str.len()
                distribution_summary = {
                    'min_length': int(lengths.min()) if len(lengths) > 0 else 0,
                    'max_length': int(lengths.max()) if len(lengths) > 0 else 0,
                    'mean_length': float(lengths.mean()) if len(lengths) > 0 else 0.0
                }
            
            elif dtype_detected == "numeric":
                numeric_series = pd.to_numeric(series, errors='coerce')
                distribution_summary = {
                    'min': float(numeric_series.min()) if not numeric_series.isna().all() else None,
                    'max': float(numeric_series.max()) if not numeric_series.isna().all() else None,
                    'mean': float(numeric_series.mean()) if not numeric_series.isna().all() else None,
                    'std': float(numeric_series.std()) if not numeric_series.isna().all() else None
                }
            
            elif dtype_detected == "categorical":
                value_counts = series.value_counts().head(10)
                distribution_summary = {
                    'top_values': {str(k): int(v) for k, v in value_counts.items()}
                }
            
            elif dtype_detected == "date":
                date_formats = set()
                for val in series.dropna().head(50):
                    val_str = str(val)
                    if '-' in val_str:
                        date_formats.add('dash-separated')
                    elif '/' in val_str:
                        date_formats.add('slash-separated')
                
                if len(date_formats) > 1:
                    anomaly_flags.append("mixed date formats")
            
            columns.append(ColumnProfile(
                name=col,
                dtype_detected=dtype_detected,
                null_count=null_count,
                null_rate=null_rate,
                unique_count=unique_count,
                cardinality=cardinality,
                sample_values=sample_values,
                anomaly_flags=anomaly_flags,
                distribution_summary=distribution_summary
            ))
        
        deduplicator = Deduplicator()
        exact_groups, near_groups = deduplicator.find_duplicates(df)
        
        duplicate_count = sum(len(group) - 1 for group in exact_groups)
        near_duplicate_count = sum(len(group) - 1 for group in near_groups)
        
        required_fields = ['record_id', 'date', 'amount', 'supplier_name']
        quality_scores = []
        
        for field in required_fields:
            if field in df.columns:
                null_rate = df[field].isna().sum() / len(df)
                quality_scores.append(1.0 - null_rate)
        
        overall_quality_score = float(np.mean(quality_scores)) if quality_scores else 0.0
        
        profile = ProfileResult(
            total_rows=len(df),
            total_columns=len(df.columns),
            columns=columns,
            duplicate_count=duplicate_count,
            near_duplicate_count=near_duplicate_count,
            encoding_issues=encoding_issues,
            overall_quality_score=overall_quality_score
        )
        
        return func.HttpResponse(
            json.dumps({"profile": profile.model_dump()}, default=str),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error profiling data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def _detect_dtype(series: pd.Series) -> str:
    """Detect the data type of a series."""
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return "string"
    
    numeric_count = 0
    date_count = 0
    
    for val in non_null.head(100):
        try:
            float(val)
            numeric_count += 1
        except (ValueError, TypeError):
            pass
        
        try:
            pd.to_datetime(val)
            date_count += 1
        except Exception:
            pass
    
    sample_size = min(100, len(non_null))
    
    if numeric_count / sample_size > 0.8:
        return "numeric"
    elif date_count / sample_size > 0.8:
        return "date"
    elif series.nunique() / len(series) < 0.05:
        return "categorical"
    elif numeric_count > 0 and numeric_count / sample_size > 0.3:
        return "mixed"
    else:
        return "string"
