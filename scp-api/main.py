"""FastAPI app for SCP data cleaning - Railway deployment."""
import logging
import json
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import chardet
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import io
from datetime import datetime, timezone
import os
import uuid

from shared.blob_helpers import download_dataframe, download_json, upload_dataframe, generate_sas_url
from shared.sharepoint_helpers import read_config_json, write_config_json, read_config_text, write_config_text
from shared.encoding_fixer import fix_encoding
from shared.date_normaliser import normalise_date
from shared.amount_normaliser import normalise_amount
from shared.unit_standardiser import standardise_unit
from shared.vendor_matcher import VendorMatcher
from shared.deduplicator import Deduplicator
from shared.triangulator import triangulate_dataframe
from shared.models import ProfileResult, ColumnProfile, FieldChange
from shared.schema_validator import validate_dataframe
from shared.consistency_checker import check_consistency

from azure.storage.blob import BlobServiceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="SCP Cleaning API", version="1.0.0")

CHUNK_SIZE = 50000


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "scp-cleaning-api"}


@app.post("/api/profile-data")
async def profile_data(request: Request):
    """Profile data endpoint - analyzes data quality and structure."""
    logger.info('Profile data function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    blob_url = req_body.get('blob_url')
    config = req_body.get('config')
    config_blob_url = req_body.get('config_blob_url')
    
    if not blob_url:
        return JSONResponse(
            content={"error": "blob_url is required"},
            status_code=400
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
        
        return JSONResponse(
            content={"profile": profile.model_dump()},
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error profiling data: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


@app.post("/api/clean-deterministic")
async def clean_deterministic(request: Request):
    """Clean deterministic endpoint - applies rule-based cleaning."""
    logger.info('Clean deterministic function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    blob_url = req_body.get('blob_url')
    config = req_body.get('config')
    strategy = req_body.get('strategy', {})
    vendor_dict_url = req_body.get('vendor_dict_url')
    abbrev_dict_url = req_body.get('abbrev_dict_url')
    
    if not blob_url or not config:
        return JSONResponse(
            content={"error": "blob_url and config are required"},
            status_code=400
        )
    
    try:
        df = download_dataframe(blob_url)
        input_rows = len(df)
        
        logger.info(f"Processing {input_rows} rows (chunking into batches of {CHUNK_SIZE})")
        
        vendor_dictionary = {}
        try:
            vendor_dictionary = read_config_json('vendor_dictionary.json')
        except Exception as e:
            logger.warning(f"Could not load vendor dictionary from config: {e}")
        
        if vendor_dict_url and vendor_dict_url.startswith('http'):
            try:
                vendor_dictionary = download_json(vendor_dict_url)
            except Exception as e:
                logger.warning(f"Could not load vendor dictionary from URL: {e}")
        
        all_changes_log = []
        fields_modified = {}
        all_cleaned_chunks = []
        all_flagged_chunks = []
        total_duplicates_removed = 0
        
        num_chunks = (input_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        logger.info(f"Processing {num_chunks} chunks")
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * CHUNK_SIZE
            end_idx = min((chunk_idx + 1) * CHUNK_SIZE, input_rows)
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            logger.info(f"Processing chunk {chunk_idx + 1}/{num_chunks} (rows {start_idx} to {end_idx})")
            
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
            
            triangulated_chunk, human_review_chunk, triangulation_changes = triangulate_dataframe(
                chunk_df,
                amount_col='amount',
                quantity_col='quantity',
                unit_price_col='unit_price'
            )
            
            chunk_df = triangulated_chunk
            
            changes_log.extend(triangulation_changes)
            
            if len(triangulation_changes) > 0:
                triangulation_count = len(triangulation_changes)
                fields_modified['triangulated_fields'] = fields_modified.get('triangulated_fields', 0) + triangulation_count
                logger.info(f"Derived {triangulation_count} values via triangulation in chunk {chunk_idx + 1}")
            
            if len(human_review_chunk) > 0:
                logger.info(f"Flagged {len(human_review_chunk)} records for human review (2+ missing fields) in chunk {chunk_idx + 1}")
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
            
            logger.info(f"Chunk {chunk_idx + 1} complete: {len(cleaned_chunk)} cleaned, {len(flagged_chunk)} flagged")
        
        cleaned_df = pd.concat(all_cleaned_chunks, ignore_index=True) if all_cleaned_chunks else pd.DataFrame()
        flagged_df = pd.concat(all_flagged_chunks, ignore_index=True) if all_flagged_chunks else pd.DataFrame()
        
        logger.info(f"All chunks processed. Total: {len(cleaned_df)} cleaned, {len(flagged_df)} flagged, {total_duplicates_removed} duplicates removed")
        
        run_id = str(uuid.uuid4())[:8]
        
        cleaned_blob_url = upload_dataframe(cleaned_df, f"cleaned_batch_{run_id}.parquet") if len(cleaned_df) > 0 else None
        flagged_blob_url = upload_dataframe(flagged_df, f"flagged_batch_{run_id}.parquet") if len(flagged_df) > 0 else None
        human_review_blob_url = None
        
        changes_df = pd.DataFrame(all_changes_log)
        changes_blob_url = upload_dataframe(changes_df, f"changes_log_{run_id}.parquet") if len(changes_df) > 0 else None
        
        return JSONResponse(
            content={
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
            },
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error cleaning data: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


@app.post("/api/classify-categories")
async def classify_categories(request: Request):
    """Classify categories endpoint - deterministic category classification."""
    logger.info('Classify categories function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    records = req_body.get('records', [])
    taxonomy = req_body.get('taxonomy', {})
    examples = req_body.get('examples', [])
    
    if not records or not taxonomy:
        return JSONResponse(
            content={"error": "records and taxonomy are required"},
            status_code=400
        )
    
    try:
        keyword_map = _build_keyword_map(taxonomy)
        
        supplier_map = _build_supplier_map()
        
        example_vectorizer = None
        example_vectors = None
        example_categories = []
        
        if examples:
            example_texts = [ex.get('description', '') for ex in examples]
            example_categories = []
            for ex in examples:
                if ex.get('l1'):
                    example_categories.append({
                        'l1': ex.get('l1'),
                        'l2': ex.get('l2'),
                        'l3': ex.get('l3')
                    })
                elif ex.get('category'):
                    parts = [p.strip() for p in ex['category'].split('>')]
                    example_categories.append({
                        'l1': parts[0] if len(parts) > 0 else None,
                        'l2': parts[1] if len(parts) > 1 else None,
                        'l3': parts[2] if len(parts) > 2 else None
                    })
                else:
                    example_categories.append({'l1': None, 'l2': None, 'l3': None})
            
            if example_texts:
                example_vectorizer = TfidfVectorizer(max_features=500, ngram_range=(1, 2))
                example_vectors = example_vectorizer.fit_transform(example_texts)
        
        classifications = []
        auto_classified = 0
        needs_llm = 0
        
        for record in records:
            record_id = record.get('record_id', '')
            description = record.get('description', '').lower()
            supplier_name = record.get('supplier_name', '').lower()
            
            signals = []
            
            keyword_match = _match_keywords(description, keyword_map)
            if keyword_match:
                signals.append(('keyword', keyword_match))
            
            supplier_match = _match_supplier(supplier_name, supplier_map)
            if supplier_match:
                signals.append(('supplier', supplier_match))
            
            example_match = None
            if example_vectorizer and example_vectors is not None:
                example_match = _match_examples(
                    description, 
                    example_vectorizer, 
                    example_vectors, 
                    example_categories
                )
                if example_match:
                    signals.append(('example', example_match))
            
            category, confidence = _resolve_signals(signals, taxonomy)
            
            if confidence >= 0.80:
                classifications.append({
                    'record_id': record_id,
                    'category_l1': category.get('l1'),
                    'category_l2': category.get('l2'),
                    'category_l3': category.get('l3'),
                    'confidence': confidence,
                    'needs_llm': False
                })
                auto_classified += 1
            else:
                best_guess = None
                if signals:
                    best_guess = f"{signals[0][1].get('l1')} > {signals[0][1].get('l2')} > {signals[0][1].get('l3')}"
                
                classifications.append({
                    'record_id': record_id,
                    'category_l1': None,
                    'category_l2': None,
                    'category_l3': None,
                    'confidence': confidence,
                    'needs_llm': True,
                    'best_guess': best_guess
                })
                needs_llm += 1
        
        return JSONResponse(
            content={
                "classifications": classifications,
                "stats": {
                    "auto_classified": auto_classified,
                    "needs_llm": needs_llm,
                    "total": len(records)
                }
            },
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error classifying categories: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


@app.post("/api/validate-output")
async def validate_output(request: Request):
    """Validate output endpoint - validates cleaned data against schema."""
    logger.info('Validate output function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    blob_url = req_body.get('blob_url')
    config = req_body.get('config')
    
    if not blob_url or not config:
        return JSONResponse(
            content={"error": "blob_url and config are required"},
            status_code=400
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
        
        return JSONResponse(
            content={
                "is_valid": validation_result.is_valid and error_count == 0,
                "total_records": total_records,
                "error_count": error_count,
                "warning_count": warning_count,
                "schema_errors": validation_result.schema_errors[:100],
                "consistency_issues": consistency_issues[:100],
                "outliers": outliers[:100],
                "stats": validation_result.stats
            },
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error validating output: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


@app.post("/api/format-output")
async def format_output(request: Request):
    """Format output endpoint - formats cleaned data for download."""
    logger.info('Format output function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    blob_url = req_body.get('blob_url')
    format_type = req_body.get('format', 'xlsx')
    include_changes_log = req_body.get('include_changes_log', False)
    changes_blob_url = req_body.get('changes_blob_url')
    
    if not blob_url:
        return JSONResponse(
            content={"error": "blob_url is required"},
            status_code=400
        )
    
    try:
        df = download_dataframe(blob_url)
        
        connection_string = os.environ.get("BLOB_CONNECTION_STRING")
        container_name = os.environ.get("BLOB_CONTAINER_NAME", "scp-cleaning")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
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
                        logger.warning(f"Could not load changes log: {e}")
                
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
            return JSONResponse(
                content={"error": f"Unsupported format: {format_type}"},
                status_code=400
            )
        
        expires = (datetime.now(timezone.utc).replace(microsecond=0) + 
                  pd.Timedelta(hours=24)).isoformat().replace('+00:00', 'Z')
        
        return JSONResponse(
            content={
                "download_url": download_url,
                "expires": expires,
                "format": format_type,
                "file_size_mb": round(file_size_mb, 2)
            },
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error formatting output: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
        )


@app.post("/api/update-learning-state")
async def update_learning_state(request: Request):
    """Update learning state endpoint - updates blob storage dictionaries and instructions."""
    logger.info('Update learning state function triggered')
    
    try:
        req_body = await request.json()
    except Exception:
        return JSONResponse(
            content={"error": "Invalid JSON in request body"},
            status_code=400
        )
    
    vendor_mappings = req_body.get('vendor_mappings', [])
    abbreviations = req_body.get('abbreviations', [])
    classification_examples = req_body.get('classification_examples', [])
    instructions_append = req_body.get('instructions_append', '')
    
    try:
        new_vendor_mappings = 0
        new_abbreviations = 0
        new_examples = 0
        instructions_updated = False
        
        if vendor_mappings:
            try:
                vendor_dict = read_config_json('vendor_dictionary.json')
            except Exception:
                vendor_dict = {}
            
            for mapping in vendor_mappings:
                dirty = mapping.get('dirty')
                canonical = mapping.get('canonical')
                
                if dirty and canonical and dirty not in vendor_dict:
                    vendor_dict[dirty] = canonical
                    new_vendor_mappings += 1
            
            if new_vendor_mappings > 0:
                write_config_json('vendor_dictionary.json', vendor_dict)
                logger.info(f"Added {new_vendor_mappings} new vendor mappings")
        
        if abbreviations:
            try:
                abbrev_dict = read_config_json('abbreviation_dictionary.json')
            except Exception:
                abbrev_dict = {}
            
            for abbrev_entry in abbreviations:
                abbrev = abbrev_entry.get('abbrev')
                expansion = abbrev_entry.get('expansion')
                
                if abbrev and expansion and abbrev not in abbrev_dict:
                    abbrev_dict[abbrev] = expansion
                    new_abbreviations += 1
            
            if new_abbreviations > 0:
                write_config_json('abbreviation_dictionary.json', abbrev_dict)
                logger.info(f"Added {new_abbreviations} new abbreviations")
        
        if classification_examples:
            try:
                examples_list = read_config_json('few_shot_examples.json')
                if not isinstance(examples_list, list):
                    examples_list = []
            except Exception:
                examples_list = []
            
            existing_descriptions = {ex.get('description') for ex in examples_list}
            
            for example in classification_examples:
                description = example.get('description')
                
                if description and description not in existing_descriptions:
                    examples_list.append({
                        'description': description,
                        'l1': example.get('l1'),
                        'l2': example.get('l2'),
                        'l3': example.get('l3'),
                        'verified': example.get('verified', True)
                    })
                    new_examples += 1
                    existing_descriptions.add(description)
            
            if new_examples > 0:
                write_config_json('few_shot_examples.json', examples_list)
                logger.info(f"Added {new_examples} new classification examples")
        
        if instructions_append:
            try:
                instructions = read_config_text('agent_instructions.md')
            except Exception:
                instructions = """# Data Cleaning Agent Instructions

## Your Role
You are a procurement data cleaning agent. You orchestrate a pipeline that cleans messy procurement records into standardised, categorised data.

## Process
1. When the user provides a file, upload it and call the profile-data function
2. Review the profile results. Decide which fields need deterministic cleaning vs LLM reasoning.
3. Call clean-deterministic to process the bulk of records
4. Review the flagged records. For each batch of ~20 flagged records, classify them using the taxonomy and examples below.
5. Call validate-output to check the final data
6. If validation passes, call format-output and provide the download link
7. Call update-learning-state with any new vendor mappings or abbreviation expansions you discovered

## Learned Rules
"""
            
            if '## Learned Rules' not in instructions:
                instructions += '\n\n## Learned Rules\n'
            
            instructions += f'\n- {instructions_append}\n'
            
            write_config_text('agent_instructions.md', instructions)
            instructions_updated = True
            logger.info("Updated agent instructions")
        
        try:
            vendor_dict = read_config_json('vendor_dictionary.json')
            total_vendor_dictionary_size = len(vendor_dict)
        except Exception:
            total_vendor_dictionary_size = 0
        
        try:
            abbrev_dict = read_config_json('abbreviation_dictionary.json')
            total_abbreviation_dictionary_size = len(abbrev_dict)
        except Exception:
            total_abbreviation_dictionary_size = 0
        
        try:
            examples_list = read_config_json('few_shot_examples.json')
            total_examples_size = len(examples_list) if isinstance(examples_list, list) else 0
        except Exception:
            total_examples_size = 0
        
        return JSONResponse(
            content={
                "updated": True,
                "new_vendor_mappings": new_vendor_mappings,
                "new_abbreviations": new_abbreviations,
                "new_examples": new_examples,
                "instructions_updated": instructions_updated,
                "total_vendor_dictionary_size": total_vendor_dictionary_size,
                "total_abbreviation_dictionary_size": total_abbreviation_dictionary_size,
                "total_examples_size": total_examples_size
            },
            status_code=200
        )
        
    except Exception as e:
        logger.error(f"Error updating learning state: {str(e)}")
        return JSONResponse(
            content={"error": str(e)},
            status_code=500
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


def _build_keyword_map(taxonomy: dict) -> dict:
    """Build a map of keywords to categories."""
    keyword_map = {}
    
    for l1, l2_dict in taxonomy.items():
        for l2, l3_list in l2_dict.items():
            for l3 in l3_list:
                keywords = l3.lower().split()
                for keyword in keywords:
                    if len(keyword) > 3:
                        if keyword not in keyword_map:
                            keyword_map[keyword] = []
                        keyword_map[keyword].append({'l1': l1, 'l2': l2, 'l3': l3})
    
    return keyword_map


def _build_supplier_map() -> dict:
    """Build a map of suppliers to typical categories."""
    return {
        'caterpillar': {'l1': 'Equipment & Parts', 'l2': 'Mobile Equipment', 'l3': 'Haul truck parts'},
        'komatsu': {'l1': 'Equipment & Parts', 'l2': 'Mobile Equipment', 'l3': 'Excavator parts'},
        'orica': {'l1': 'Raw Materials & Consumables', 'l2': 'Chemical Reagents', 'l3': 'Explosives'},
        'shell': {'l1': 'Energy & Fuel', 'l2': 'Diesel', 'l3': 'Bulk diesel'},
        'bp': {'l1': 'Energy & Fuel', 'l2': 'Diesel', 'l3': 'Bulk diesel'},
    }


def _match_keywords(description: str, keyword_map: dict) -> dict:
    """Match description against keyword map."""
    matches = []
    
    words = description.split()
    for word in words:
        if word in keyword_map:
            matches.extend(keyword_map[word])
    
    if not matches:
        return None
    
    category_counts = {}
    for match in matches:
        key = (match['l1'], match['l2'], match['l3'])
        category_counts[key] = category_counts.get(key, 0) + 1
    
    best_match = max(category_counts.items(), key=lambda x: x[1])
    return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}


def _match_supplier(supplier_name: str, supplier_map: dict) -> dict:
    """Match supplier name to typical category."""
    for supplier_keyword, category in supplier_map.items():
        if supplier_keyword in supplier_name:
            return category
    return None


def _match_examples(description: str, vectorizer, example_vectors, example_categories) -> dict:
    """Match description against few-shot examples."""
    try:
        query_vec = vectorizer.transform([description])
        similarities = cosine_similarity(query_vec, example_vectors).flatten()
        
        best_idx = np.argmax(similarities)
        best_score = similarities[best_idx]
        
        if best_score >= 0.5:
            return example_categories[best_idx]
    except Exception:
        pass
    
    return None


def _resolve_signals(signals: list, taxonomy: dict) -> tuple:
    """Resolve multiple signals into a single category with confidence."""
    if not signals:
        return {}, 0.30
    
    if len(signals) == 1:
        return signals[0][1], 0.60
    
    signal_types = [s[0] for s in signals]
    categories = [s[1] for s in signals]
    
    if len(set(tuple(c.items()) for c in categories)) == 1:
        if len(signals) >= 3:
            return categories[0], 0.95
        elif len(signals) == 2:
            return categories[0], 0.80
    
    category_counts = {}
    for cat in categories:
        key = (cat.get('l1'), cat.get('l2'), cat.get('l3'))
        category_counts[key] = category_counts.get(key, 0) + 1
    
    best_match = max(category_counts.items(), key=lambda x: x[1])
    agreement_count = best_match[1]
    
    if agreement_count >= 2:
        return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}, 0.80
    else:
        return {'l1': best_match[0][0], 'l2': best_match[0][1], 'l3': best_match[0][2]}, 0.60


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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
