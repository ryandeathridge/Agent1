"""Clean deterministic function - applies rule-based cleaning."""
import logging
import json
import azure.functions as func
import pandas as pd
from shared.blob_helpers import download_dataframe, download_json, upload_dataframe
from shared.sharepoint_helpers import read_config_json
from shared.encoding_fixer import fix_encoding
from shared.date_normaliser import normalise_date
from shared.amount_normaliser import normalise_amount
from shared.unit_standardiser import standardise_unit
from shared.vendor_matcher import VendorMatcher
from shared.deduplicator import Deduplicator
from shared.triangulator import triangulate_dataframe
from shared.models import FieldChange

CHUNK_SIZE = 50000


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
        
        logging.info(f"Processing {input_rows} rows (chunking into batches of {CHUNK_SIZE})")
        
        vendor_dictionary = {}
        try:
            vendor_dictionary = read_config_json('vendor_dictionary.json')
        except Exception as e:
            logging.warning(f"Could not load vendor dictionary from config: {e}")
        
        if vendor_dict_url and vendor_dict_url.startswith('http'):
            try:
                vendor_dictionary = download_json(vendor_dict_url)
            except Exception as e:
                logging.warning(f"Could not load vendor dictionary from URL: {e}")
        
        all_changes_log = []
        fields_modified = {}
        all_cleaned_chunks = []
        all_flagged_chunks = []
        total_duplicates_removed = 0
        
        num_chunks = (input_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        logging.info(f"Processing {num_chunks} chunks")
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * CHUNK_SIZE
            end_idx = min((chunk_idx + 1) * CHUNK_SIZE, input_rows)
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            logging.info(f"Processing chunk {chunk_idx + 1}/{num_chunks} (rows {start_idx} to {end_idx})")
            
            changes_log = []
            
            string_columns = chunk_df.select_dtypes(include=['object']).columns
            for col in string_columns:
                modified_count = 0
                for idx in chunk_df.index:
                    if pd.notna(chunk_df.at[idx, col]):
                        original = chunk_df.at[idx, col]
                        fixed, was_modified = fix_encoding(str(original))
                        if was_modified:
                            chunk_df.at[idx, col] = fixed
                            modified_count += 1
                            changes_log.append({
                                'record_id': chunk_df.at[idx, 'record_id'] if 'record_id' in chunk_df.columns else f'row_{idx}',
                                'field': col,
                                'original': str(original),
                                'new': fixed,
                                'method': 'encoding_fix',
                                'confidence': 1.0,
                                'agent': 'encoding_fixer'
                            })
                if modified_count > 0:
                    fields_modified[col] = fields_modified.get(col, 0) + modified_count
            
            if 'date' in chunk_df.columns:
                modified_count = 0
                for idx in chunk_df.index:
                    if pd.notna(chunk_df.at[idx, 'date']):
                        original = chunk_df.at[idx, 'date']
                        normalised, confidence = normalise_date(original)
                        if normalised and str(normalised) != str(original):
                            chunk_df.at[idx, 'date'] = normalised
                            chunk_df.at[idx, 'date_confidence'] = confidence
                            modified_count += 1
                            changes_log.append({
                                'record_id': chunk_df.at[idx, 'record_id'] if 'record_id' in chunk_df.columns else f'row_{idx}',
                                'field': 'date',
                                'original': str(original),
                                'new': normalised,
                                'method': 'date_normalisation',
                                'confidence': confidence,
                                'agent': 'date_normaliser'
                            })
                if modified_count > 0:
                    fields_modified['date'] = fields_modified.get('date', 0) + modified_count
            
            for amount_col in ['amount', 'unit_price', 'amount_usd']:
                if amount_col in chunk_df.columns:
                    modified_count = 0
                    for idx in chunk_df.index:
                        if pd.notna(chunk_df.at[idx, amount_col]):
                            original = chunk_df.at[idx, amount_col]
                            normalised, confidence = normalise_amount(original)
                            if normalised is not None and normalised != original:
                                chunk_df.at[idx, amount_col] = normalised
                                chunk_df.at[idx, f'{amount_col}_confidence'] = confidence
                                modified_count += 1
                                changes_log.append({
                                    'record_id': chunk_df.at[idx, 'record_id'] if 'record_id' in chunk_df.columns else f'row_{idx}',
                                    'field': amount_col,
                                    'original': str(original),
                                    'new': str(normalised),
                                    'method': 'amount_normalisation',
                                    'confidence': confidence,
                                    'agent': 'amount_normaliser'
                                })
                    if modified_count > 0:
                        fields_modified[amount_col] = fields_modified.get(amount_col, 0) + modified_count
            
            # Stage 3: Triangulate amount, quantity, and unit_price
            triangulated_chunk, human_review_chunk, triangulation_changes = triangulate_dataframe(
                chunk_df,
                amount_col='amount',
                quantity_col='quantity',
                unit_price_col='unit_price'
            )
            
            # Update chunk_df with triangulated values
            chunk_df = triangulated_chunk
            
            # Add triangulation changes to changes_log
            changes_log.extend(triangulation_changes)
            
            # Track triangulation stats
            if len(triangulation_changes) > 0:
                triangulation_count = len(triangulation_changes)
                fields_modified['triangulated_fields'] = fields_modified.get('triangulated_fields', 0) + triangulation_count
                logging.info(f"Derived {triangulation_count} values via triangulation in chunk {chunk_idx + 1}")
            
            if len(human_review_chunk) > 0:
                logging.info(f"Flagged {len(human_review_chunk)} records for human review (2+ missing fields) in chunk {chunk_idx + 1}")
                all_flagged_chunks.append(human_review_chunk)
            
            if 'unit' in chunk_df.columns:
                modified_count = 0
                for idx in chunk_df.index:
                    if pd.notna(chunk_df.at[idx, 'unit']):
                        original = chunk_df.at[idx, 'unit']
                        standardised, confidence = standardise_unit(original)
                        if standardised and standardised != original:
                            chunk_df.at[idx, 'unit'] = standardised
                            chunk_df.at[idx, 'unit_confidence'] = confidence
                            modified_count += 1
                            changes_log.append({
                                'record_id': chunk_df.at[idx, 'record_id'] if 'record_id' in chunk_df.columns else f'row_{idx}',
                                'field': 'unit',
                                'original': str(original),
                                'new': standardised,
                                'method': 'unit_standardisation',
                                'confidence': confidence,
                                'agent': 'unit_standardiser'
                            })
                if modified_count > 0:
                    fields_modified['unit'] = fields_modified.get('unit', 0) + modified_count
            
            if 'supplier_name' in chunk_df.columns:
                vendor_master = config.get('top_20_suppliers', [])
                matcher = VendorMatcher(vendor_master, vendor_dictionary)
                
                modified_count = 0
                for idx in chunk_df.index:
                    if pd.notna(chunk_df.at[idx, 'supplier_name']):
                        original = chunk_df.at[idx, 'supplier_name']
                        matched_vendor, confidence, candidates = matcher.match(original)
                        
                        if matched_vendor:
                            chunk_df.at[idx, 'supplier_name'] = matched_vendor.get('supplier_name')
                            chunk_df.at[idx, 'supplier_id'] = matched_vendor.get('supplier_id')
                            chunk_df.at[idx, 'supplier_confidence'] = confidence
                            modified_count += 1
                            changes_log.append({
                                'record_id': chunk_df.at[idx, 'record_id'] if 'record_id' in chunk_df.columns else f'row_{idx}',
                                'field': 'supplier_name',
                                'original': str(original),
                                'new': matched_vendor.get('supplier_name'),
                                'method': 'vendor_matching',
                                'confidence': confidence,
                                'agent': 'vendor_matcher'
                            })
                        else:
                            chunk_df.at[idx, 'supplier_confidence'] = confidence
                            chunk_df.at[idx, 'supplier_candidates'] = json.dumps(candidates[:3])
                
                if modified_count > 0:
                    fields_modified['supplier_name'] = fields_modified.get('supplier_name', 0) + modified_count
            
            deduplicator = Deduplicator()
            deduped_chunk, removed_chunk = deduplicator.deduplicate(chunk_df)
            total_duplicates_removed += len(removed_chunk)
            
            confidence_threshold = 0.70
            confidence_cols = [col for col in deduped_chunk.columns if col.endswith('_confidence')]
            
            if confidence_cols:
                mask = (deduped_chunk[confidence_cols] >= confidence_threshold).all(axis=1)
                cleaned_chunk = deduped_chunk[mask].copy()
                flagged_chunk = deduped_chunk[~mask].copy()
            else:
                cleaned_chunk = deduped_chunk.copy()
                flagged_chunk = pd.DataFrame()
            
            all_cleaned_chunks.append(cleaned_chunk)
            if len(flagged_chunk) > 0:
                all_flagged_chunks.append(flagged_chunk)
            all_changes_log.extend(changes_log)
            
            logging.info(f"Chunk {chunk_idx + 1} complete: {len(cleaned_chunk)} cleaned, {len(flagged_chunk)} flagged")
        
        cleaned_df = pd.concat(all_cleaned_chunks, ignore_index=True) if all_cleaned_chunks else pd.DataFrame()
        flagged_df = pd.concat(all_flagged_chunks, ignore_index=True) if all_flagged_chunks else pd.DataFrame()
        
        logging.info(f"All chunks processed. Total: {len(cleaned_df)} cleaned, {len(flagged_df)} flagged, {total_duplicates_removed} duplicates removed")
        
        import uuid
        run_id = str(uuid.uuid4())[:8]
        
        cleaned_blob_url = upload_dataframe(cleaned_df, f"cleaned_batch_{run_id}.parquet") if len(cleaned_df) > 0 else None
        flagged_blob_url = upload_dataframe(flagged_df, f"flagged_batch_{run_id}.parquet") if len(flagged_df) > 0 else None
        human_review_blob_url = None
        
        changes_df = pd.DataFrame(all_changes_log)
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
                    "duplicates_removed": total_duplicates_removed,
                    "fields_modified": fields_modified,
                    "chunks_processed": num_chunks
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
