"""Validates cleaned records against the target schema."""
from typing import List
import pandas as pd
import re
from datetime import datetime
from shared.models import ValidationResult


def validate_record(record: dict, config: dict) -> List[dict]:
    """Validate a single record against the schema defined in config.
    Returns list of errors, each: {field, error_type, details}
    
    Checks:
    - Required fields are not null (record_id, date, amount, supplier_name, etc.)
    - Dates are valid ISO format and within expected range (config date_range)
    - Amounts are positive floats (unless credit note)
    - category_l1 exists in config taxonomy
    - category_l2 exists under its l1 parent in taxonomy
    - category_l3 exists under its l2 parent
    - currency is one of the valid currencies in config
    - supplier_id format matches SUP-XXXXX if present
    - record_id format matches BHP-PO-XXXXXXX
    - financial_year is correct for the date (Jul-Jun FY)
    """
    errors = []
    
    required_fields = ['record_id', 'date', 'amount', 'supplier_name']
    for field in required_fields:
        if field not in record or record[field] is None or record[field] == '':
            errors.append({
                'field': field,
                'error_type': 'missing_required',
                'details': f'Required field {field} is missing or null'
            })
    
    if 'record_id' in record and record['record_id']:
        if not re.match(r'^BHP-PO-\d{7}$', str(record['record_id'])):
            errors.append({
                'field': 'record_id',
                'error_type': 'invalid_format',
                'details': 'record_id must match format BHP-PO-XXXXXXX'
            })
    
    if 'date' in record and record['date']:
        try:
            date_obj = pd.to_datetime(record['date'])
            
            if not re.match(r'^\d{4}-\d{2}-\d{2}', str(record['date'])):
                errors.append({
                    'field': 'date',
                    'error_type': 'invalid_format',
                    'details': 'Date must be in ISO format YYYY-MM-DD'
                })
            
            if 'date_range' in config and config['date_range']:
                min_date = pd.to_datetime(config['date_range'].get('min'))
                max_date = pd.to_datetime(config['date_range'].get('max'))
                if date_obj < min_date or date_obj > max_date:
                    errors.append({
                        'field': 'date',
                        'error_type': 'out_of_range',
                        'details': f'Date {record["date"]} outside expected range'
                    })
        except Exception as e:
            errors.append({
                'field': 'date',
                'error_type': 'invalid_date',
                'details': f'Invalid date value: {str(e)}'
            })
    
    if 'amount' in record and record['amount'] is not None:
        try:
            amount = float(record['amount'])
            if amount < 0 and not record.get('is_credit_note', False):
                errors.append({
                    'field': 'amount',
                    'error_type': 'negative_amount',
                    'details': 'Amount is negative but not marked as credit note'
                })
        except (ValueError, TypeError):
            errors.append({
                'field': 'amount',
                'error_type': 'invalid_type',
                'details': 'Amount must be a numeric value'
            })
    
    taxonomy = config.get('category_taxonomy', {})
    
    if 'category_l1' in record and record['category_l1']:
        l1 = record['category_l1']
        if l1 not in taxonomy:
            errors.append({
                'field': 'category_l1',
                'error_type': 'invalid_taxonomy',
                'details': f'category_l1 "{l1}" not found in taxonomy'
            })
        else:
            if 'category_l2' in record and record['category_l2']:
                l2 = record['category_l2']
                if l2 not in taxonomy[l1]:
                    errors.append({
                        'field': 'category_l2',
                        'error_type': 'invalid_taxonomy',
                        'details': f'category_l2 "{l2}" not found under "{l1}"'
                    })
                else:
                    if 'category_l3' in record and record['category_l3']:
                        l3 = record['category_l3']
                        if l3 not in taxonomy[l1][l2]:
                            errors.append({
                                'field': 'category_l3',
                                'error_type': 'invalid_taxonomy',
                                'details': f'category_l3 "{l3}" not found under "{l1} > {l2}"'
                            })
    
    valid_currencies = config.get('valid_currencies', ['AUD', 'USD', 'CLP'])
    if 'currency' in record and record['currency']:
        if record['currency'] not in valid_currencies:
            errors.append({
                'field': 'currency',
                'error_type': 'invalid_currency',
                'details': f'Currency {record["currency"]} not in valid list: {valid_currencies}'
            })
    
    if 'supplier_id' in record and record['supplier_id']:
        if not re.match(r'^SUP-\d{5}$', str(record['supplier_id'])):
            errors.append({
                'field': 'supplier_id',
                'error_type': 'invalid_format',
                'details': 'supplier_id must match format SUP-XXXXX'
            })
    
    if 'date' in record and record['date'] and 'financial_year' in record and record['financial_year']:
        try:
            date_obj = pd.to_datetime(record['date'])
            if date_obj.month >= 7:
                expected_fy = date_obj.year + 1
            else:
                expected_fy = date_obj.year
            
            if str(record['financial_year']) != str(expected_fy):
                errors.append({
                    'field': 'financial_year',
                    'error_type': 'incorrect_fy',
                    'details': f'Financial year {record["financial_year"]} incorrect for date {record["date"]} (expected {expected_fy})'
                })
        except Exception:
            pass
    
    return errors


def validate_dataframe(df: pd.DataFrame, config: dict) -> ValidationResult:
    """Validate entire DataFrame. Returns ValidationResult model."""
    all_errors = []
    
    for idx, row in df.iterrows():
        record = row.to_dict()
        errors = validate_record(record, config)
        
        for error in errors:
            error['record_id'] = record.get('record_id', f'row_{idx}')
            all_errors.append(error)
    
    error_count = len(all_errors)
    total_records = len(df)
    
    records_with_errors = len({e['record_id'] for e in all_errors})
    schema_valid_pct = ((total_records - records_with_errors) / total_records * 100) if total_records > 0 else 0
    
    return ValidationResult(
        is_valid=(error_count == 0),
        total_records=total_records,
        schema_errors=all_errors,
        consistency_issues=[],
        outliers=[],
        stats={
            'schema_valid_pct': schema_valid_pct,
            'error_count': error_count
        }
    )
