"""Validate output function - validates cleaned data against schema."""
import logging
import json
import azure.functions as func
import pandas as pd
import numpy as np
from shared.blob_helpers import download_dataframe, download_json
from shared.schema_validator import validate_dataframe
from shared.consistency_checker import check_consistency


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Validate output endpoint."""
    logging.info('Validate output function triggered')
    
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
    
    if not blob_url or not config:
        return func.HttpResponse(
            json.dumps({"error": "blob_url and config are required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        df = download_dataframe(blob_url)
        
        validation_result = validate_dataframe(df, config)
        
        consistency_issues = check_consistency(df, config)
        validation_result.consistency_issues = consistency_issues
        
        outliers = _detect_outliers(df, config)
        validation_result.outliers = outliers
        
        error_count = len(validation_result.schema_errors)
        warning_count = sum(1 for issue in consistency_issues if issue.get('severity') == 'warning')
        error_count += sum(1 for issue in consistency_issues if issue.get('severity') == 'error')
        
        total_records = validation_result.total_records
        
        schema_valid_pct = validation_result.stats.get('schema_valid_pct', 0)
        
        affected_by_errors = set()
        for issue in consistency_issues:
            if issue.get('severity') == 'error':
                for rid in issue.get('affected_record_ids', []):
                    affected_by_errors.add(rid)
        consistent_records = total_records - len(affected_by_errors)
        consistent_pct = (consistent_records / total_records * 100) if total_records > 0 else 0
        
        outlier_pct = (len(outliers) / total_records * 100) if total_records > 0 else 0
        
        validation_result.stats.update({
            'consistent_pct': consistent_pct,
            'outlier_pct': outlier_pct
        })
        
        return func.HttpResponse(
            json.dumps({
                "is_valid": validation_result.is_valid and error_count == 0,
                "total_records": total_records,
                "error_count": error_count,
                "warning_count": warning_count,
                "schema_errors": validation_result.schema_errors[:100],
                "consistency_issues": consistency_issues[:100],
                "outliers": outliers[:100],
                "stats": validation_result.stats
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error validating output: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )


def _detect_outliers(df: pd.DataFrame, config: dict) -> list:
    """Detect outliers in the data."""
    outliers = []
    
    if 'amount' in df.columns and 'category_l1' in df.columns:
        for category in df['category_l1'].unique():
            if pd.isna(category):
                continue
            
            category_df = df[df['category_l1'] == category]
            amounts = pd.to_numeric(category_df['amount'], errors='coerce').dropna()
            
            if len(amounts) > 10:
                mean = amounts.mean()
                std = amounts.std()
                
                if std > 0:
                    z_scores = np.abs((amounts - mean) / std)
                    
                    outlier_mask = z_scores > 3
                    outlier_indices = amounts[outlier_mask].index
                    
                    for idx in outlier_indices:
                        record_id = df.loc[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}'
                        amount = df.loc[idx, 'amount']
                        z_score = z_scores.loc[idx]
                        
                        outliers.append({
                            'record_id': record_id,
                            'field': 'amount',
                            'value': float(amount),
                            'reason': f'z-score {z_score:.1f} within {category}'
                        })
    
    if 'date' in df.columns:
        date_range = config.get('date_range', {})
        
        for idx, row in df.iterrows():
            date_val = row.get('date')
            
            if pd.notna(date_val):
                try:
                    date_obj = pd.to_datetime(date_val)
                    
                    if date_obj > pd.Timestamp.now():
                        record_id = row.get('record_id', f'row_{idx}')
                        outliers.append({
                            'record_id': record_id,
                            'field': 'date',
                            'value': str(date_val),
                            'reason': 'Future date'
                        })
                    
                    if date_range:
                        min_date = pd.to_datetime(date_range.get('min'))
                        max_date = pd.to_datetime(date_range.get('max'))
                        
                        if date_obj < min_date or date_obj > max_date:
                            record_id = row.get('record_id', f'row_{idx}')
                            outliers.append({
                                'record_id': record_id,
                                'field': 'date',
                                'value': str(date_val),
                                'reason': f'Outside expected range {date_range.get("min")} to {date_range.get("max")}'
                            })
                except Exception:
                    pass
    
    if 'quantity' in df.columns:
        for idx, row in df.iterrows():
            quantity = row.get('quantity')
            
            if pd.notna(quantity):
                try:
                    quantity_float = float(quantity)
                    
                    if quantity_float < 0:
                        record_id = row.get('record_id', f'row_{idx}')
                        outliers.append({
                            'record_id': record_id,
                            'field': 'quantity',
                            'value': quantity_float,
                            'reason': 'Negative quantity'
                        })
                    
                    if quantity_float > 1000000:
                        record_id = row.get('record_id', f'row_{idx}')
                        outliers.append({
                            'record_id': record_id,
                            'field': 'quantity',
                            'value': quantity_float,
                            'reason': 'Extremely large quantity'
                        })
                except (ValueError, TypeError):
                    pass
    
    return outliers
