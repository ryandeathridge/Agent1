"""Clean deterministic function - applies rule-based cleaning."""
import logging
import json
import azure.functions as func
import pandas as pd
from shared.blob_helpers import download_dataframe, download_json, upload_dataframe
from shared.sharepoint_helpers import read_sharepoint_json
from shared.encoding_fixer import fix_encoding
from shared.date_normaliser import normalise_date
from shared.amount_normaliser import normalise_amount
from shared.unit_standardiser import standardise_unit
from shared.vendor_matcher import VendorMatcher
from shared.deduplicator import Deduplicator
from shared.triangulator import triangulate_dataframe
from shared.models import FieldChange


def main(req: func.HttpRequest) -> func.HttpResponse:
    """Clean deterministic endpoint."""
    logging.info('Clean deterministic function triggered')
    
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
    strategy = req_body.get('strategy', {})
    vendor_dict_url = req_body.get('vendor_dict_url')
    abbrev_dict_url = req_body.get('abbrev_dict_url')
    
    if not blob_url or not config:
        return func.HttpResponse(
            json.dumps({"error": "blob_url and config are required"}),
            status_code=400,
            mimetype="application/json"
        )
    
    try:
        df = download_dataframe(blob_url)
        input_rows = len(df)
        
        vendor_dictionary = {}
        if vendor_dict_url:
            try:
                if vendor_dict_url.startswith('http'):
                    vendor_dictionary = download_json(vendor_dict_url)
                else:
                    vendor_dictionary = read_sharepoint_json(vendor_dict_url)
            except Exception as e:
                logging.warning(f"Could not load vendor dictionary: {e}")
        
        changes_log = []
        fields_modified = {}
        
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, col]):
                    original = df.at[idx, col]
                    fixed, was_modified = fix_encoding(str(original))
                    if was_modified:
                        df.at[idx, col] = fixed
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': col,
                            'original': str(original),
                            'new': fixed,
                            'method': 'encoding_fix',
                            'confidence': 1.0,
                            'agent': 'encoding_fixer'
                        })
            if modified_count > 0:
                fields_modified[col] = modified_count
                logging.info(f"Fixed encoding in {modified_count} records for column {col}")
        
        if 'date' in df.columns:
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'date']):
                    original = df.at[idx, 'date']
                    normalised, confidence = normalise_date(original)
                    if normalised and str(normalised) != str(original):
                        df.at[idx, 'date'] = normalised
                        df.at[idx, 'date_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'date',
                            'original': str(original),
                            'new': normalised,
                            'method': 'date_normalisation',
                            'confidence': confidence,
                            'agent': 'date_normaliser'
                        })
            if modified_count > 0:
                fields_modified['date'] = modified_count
                logging.info(f"Normalised {modified_count} dates")
        
        for amount_col in ['amount', 'unit_price', 'amount_usd']:
            if amount_col in df.columns:
                modified_count = 0
                for idx in df.index:
                    if pd.notna(df.at[idx, amount_col]):
                        original = df.at[idx, amount_col]
                        normalised, confidence = normalise_amount(original)
                        if normalised is not None and normalised != original:
                            df.at[idx, amount_col] = normalised
                            df.at[idx, f'{amount_col}_confidence'] = confidence
                            modified_count += 1
                            changes_log.append({
                                'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                                'field': amount_col,
                                'original': str(original),
                                'new': str(normalised),
                                'method': 'amount_normalisation',
                                'confidence': confidence,
                                'agent': 'amount_normaliser'
                            })
                if modified_count > 0:
                    fields_modified[amount_col] = modified_count
                    logging.info(f"Normalised {modified_count} amounts in {amount_col}")
        
        # Stage 3: Triangulate amount, quantity, and unit_price
        triangulated_df, human_review_df, triangulation_changes = triangulate_dataframe(
            df,
            amount_col='amount',
            quantity_col='quantity',
            unit_price_col='unit_price'
        )
        
        # Update df with triangulated values
        df = triangulated_df
        
        # Add triangulation changes to changes_log
        changes_log.extend(triangulation_changes)
        
        # Track triangulation stats
        if len(triangulation_changes) > 0:
            triangulation_count = len(triangulation_changes)
            fields_modified['triangulated_fields'] = triangulation_count
            logging.info(f"Derived {triangulation_count} values via triangulation")
        
        if len(human_review_df) > 0:
            logging.info(f"Flagged {len(human_review_df)} records for human review (2+ missing fields)")
        
        if 'unit' in df.columns:
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'unit']):
                    original = df.at[idx, 'unit']
                    standardised, confidence = standardise_unit(original)
                    if standardised and standardised != original:
                        df.at[idx, 'unit'] = standardised
                        df.at[idx, 'unit_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'unit',
                            'original': str(original),
                            'new': standardised,
                            'method': 'unit_standardisation',
                            'confidence': confidence,
                            'agent': 'unit_standardiser'
                        })
            if modified_count > 0:
                fields_modified['unit'] = modified_count
                logging.info(f"Standardised {modified_count} units")
        
        if 'supplier_name' in df.columns:
            vendor_master = config.get('top_20_suppliers', [])
            matcher = VendorMatcher(vendor_master, vendor_dictionary)
            
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'supplier_name']):
                    original = df.at[idx, 'supplier_name']
                    matched_vendor, confidence, candidates = matcher.match(original)
                    
                    if matched_vendor:
                        df.at[idx, 'supplier_name'] = matched_vendor.get('supplier_name')
                        df.at[idx, 'supplier_id'] = matched_vendor.get('supplier_id')
                        df.at[idx, 'supplier_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'supplier_name',
                            'original': str(original),
                            'new': matched_vendor.get('supplier_name'),
                            'method': 'vendor_matching',
                            'confidence': confidence,
                            'agent': 'vendor_matcher'
                        })
                    else:
                        df.at[idx, 'supplier_confidence'] = confidence
                        df.at[idx, 'supplier_candidates'] = json.dumps(candidates[:3])
            
            if modified_count > 0:
                fields_modified['supplier_name'] = modified_count
                logging.info(f"Matched {modified_count} vendors")
        
        deduplicator = Deduplicator()
        deduped_df, removed_df = deduplicator.deduplicate(df)
        duplicates_removed = len(removed_df)
        
        logging.info(f"Removed {duplicates_removed} duplicates")
        
        confidence_threshold = 0.70
        confidence_cols = [col for col in deduped_df.columns if col.endswith('_confidence')]
        
        if confidence_cols:
            mask = (deduped_df[confidence_cols] >= confidence_threshold).all(axis=1)
            cleaned_df = deduped_df[mask].copy()
            flagged_df = deduped_df[~mask].copy()
        else:
            cleaned_df = deduped_df.copy()
            flagged_df = pd.DataFrame()
        
        import uuid
        run_id = str(uuid.uuid4())[:8]
        
        cleaned_blob_url = upload_dataframe(cleaned_df, f"cleaned_batch_{run_id}.parquet")
        flagged_blob_url = upload_dataframe(flagged_df, f"flagged_batch_{run_id}.parquet") if len(flagged_df) > 0 else None
        human_review_blob_url = upload_dataframe(human_review_df, f"human_review_{run_id}.parquet") if len(human_review_df) > 0 else None
        
        changes_df = pd.DataFrame(changes_log)
        changes_blob_url = upload_dataframe(changes_df, f"changes_log_{run_id}.parquet") if len(changes_df) > 0 else None
        
        return func.HttpResponse(
            json.dumps({
                "cleaned_blob_url": cleaned_blob_url,
                "flagged_blob_url": flagged_blob_url,
                "human_review_blob_url": human_review_blob_url,
                "changes_blob_url": changes_blob_url,
                "stats": {
                    "input_rows": input_rows,
                    "cleaned_rows": len(cleaned_df),
                    "flagged_rows": len(flagged_df),
                    "human_review_rows": len(human_review_df),
                    "duplicates_removed": duplicates_removed,
                    "fields_modified": fields_modified
                }
            }),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"Error cleaning data: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )
