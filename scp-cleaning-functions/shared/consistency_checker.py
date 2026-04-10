"""Checks cross-record consistency."""
from typing import List
import pandas as pd
import numpy as np
from collections import defaultdict


def check_consistency(df: pd.DataFrame, config: dict) -> List[dict]:
    """Check for cross-record inconsistencies. Returns list of issues.
    
    Checks:
    1. Vendor consistency: same supplier_name should always map to same supplier_id
       Flag: "Supplier X has 3 different IDs across records"
    2. Category consistency: similar descriptions should have similar categories
       Flag: "Description containing 'hydraulic hose' categorised as Equipment in 95% of records but Mining Services in 5"
    3. Amount × quantity consistency: where both quantity and unit_price exist,
       amount should ≈ quantity × unit_price (within 1% tolerance)
       Flag: "Record X: amount=$5000 but quantity×unit_price=$500"
    4. Currency-site consistency: AUD for AU sites, USD for Houston, CLP for Chile
       Flag: "Record at WAIO Newman has CLP currency"
    5. No duplicate record_ids
    6. Dates are business days (weekends flagged as warning, not error)
    
    Each issue: {check_name, severity ('error'|'warning'), description, affected_record_ids}
    """
    issues = []
    
    if 'supplier_name' in df.columns and 'supplier_id' in df.columns:
        supplier_map = defaultdict(set)
        
        for idx, row in df.iterrows():
            supplier_name = row.get('supplier_name')
            supplier_id = row.get('supplier_id')
            
            if pd.notna(supplier_name) and pd.notna(supplier_id):
                supplier_map[supplier_name].add(supplier_id)
        
        for supplier_name, ids in supplier_map.items():
            if len(ids) > 1:
                affected = df[df['supplier_name'] == supplier_name]['record_id'].tolist()
                issues.append({
                    'check_name': 'vendor_id_consistency',
                    'severity': 'error',
                    'description': f"Supplier '{supplier_name}' has {len(ids)} different IDs: {', '.join(ids)}",
                    'affected_record_ids': affected[:10]
                })
    
    if 'description' in df.columns and 'category_l1' in df.columns:
        keyword_categories = defaultdict(lambda: defaultdict(int))
        
        for idx, row in df.iterrows():
            description = str(row.get('description', '')).lower()
            category = row.get('category_l1')
            
            if pd.notna(description) and pd.notna(category):
                words = description.split()
                for word in words:
                    if len(word) > 4:
                        keyword_categories[word][category] += 1
        
        for keyword, categories in keyword_categories.items():
            total = sum(categories.values())
            if total >= 10:
                sorted_cats = sorted(categories.items(), key=lambda x: x[1], reverse=True)
                if len(sorted_cats) > 1:
                    dominant_pct = sorted_cats[0][1] / total * 100
                    minority_pct = sorted_cats[1][1] / total * 100
                    
                    if dominant_pct >= 80 and minority_pct >= 5:
                        affected = df[
                            (df['description'].str.contains(keyword, case=False, na=False)) &
                            (df['category_l1'] == sorted_cats[1][0])
                        ]['record_id'].tolist()
                        
                        if affected:
                            issues.append({
                                'check_name': 'category_consistency',
                                'severity': 'warning',
                                'description': f"Keyword '{keyword}' categorised as {sorted_cats[0][0]} in {dominant_pct:.0f}% of records but {sorted_cats[1][0]} in {minority_pct:.0f}%",
                                'affected_record_ids': affected[:10]
                            })
    
    if 'amount' in df.columns and 'quantity' in df.columns and 'unit_price' in df.columns:
        for idx, row in df.iterrows():
            amount = row.get('amount')
            quantity = row.get('quantity')
            unit_price = row.get('unit_price')
            
            if pd.notna(amount) and pd.notna(quantity) and pd.notna(unit_price):
                try:
                    amount_float = float(amount)
                    quantity_float = float(quantity)
                    unit_price_float = float(unit_price)
                    
                    expected_amount = quantity_float * unit_price_float
                    
                    if expected_amount > 0:
                        diff_pct = abs(amount_float - expected_amount) / expected_amount * 100
                        
                        if diff_pct > 1:
                            issues.append({
                                'check_name': 'amount_calculation_consistency',
                                'severity': 'error',
                                'description': f"Record amount=${amount_float:.2f} but quantity×unit_price=${expected_amount:.2f} (diff: {diff_pct:.1f}%)",
                                'affected_record_ids': [row.get('record_id', f'row_{idx}')]
                            })
                except (ValueError, TypeError):
                    pass
    
    if 'site' in df.columns and 'currency' in df.columns:
        site_currency_map = {
            'WAIO': 'AUD',
            'Newman': 'AUD',
            'Houston': 'USD',
            'Chile': 'CLP'
        }
        
        for idx, row in df.iterrows():
            site = row.get('site', '')
            currency = row.get('currency')
            
            if pd.notna(site) and pd.notna(currency):
                for site_keyword, expected_currency in site_currency_map.items():
                    if site_keyword.lower() in str(site).lower():
                        if currency != expected_currency:
                            issues.append({
                                'check_name': 'currency_site_consistency',
                                'severity': 'warning',
                                'description': f"Record at {site} has {currency} currency (expected {expected_currency})",
                                'affected_record_ids': [row.get('record_id', f'row_{idx}')]
                            })
    
    if 'record_id' in df.columns:
        duplicate_ids = df[df['record_id'].duplicated(keep=False)]['record_id'].unique()
        
        if len(duplicate_ids) > 0:
            issues.append({
                'check_name': 'duplicate_record_ids',
                'severity': 'error',
                'description': f"Found {len(duplicate_ids)} duplicate record IDs",
                'affected_record_ids': duplicate_ids.tolist()[:10]
            })
    
    if 'date' in df.columns:
        weekend_records = []
        
        for idx, row in df.iterrows():
            date_val = row.get('date')
            
            if pd.notna(date_val):
                try:
                    date_obj = pd.to_datetime(date_val)
                    if date_obj.weekday() >= 5:
                        weekend_records.append(row.get('record_id', f'row_{idx}'))
                except Exception:
                    pass
        
        if weekend_records:
            issues.append({
                'check_name': 'weekend_dates',
                'severity': 'warning',
                'description': f"Found {len(weekend_records)} records with weekend dates",
                'affected_record_ids': weekend_records[:10]
            })
    
    return issues
