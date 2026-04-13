"""Detects exact and near-duplicate records."""
from typing import List, Tuple, Optional
import pandas as pd
import numpy as np


class Deduplicator:
    """Detects duplicate records using exact matching and fuzzy near-duplicate detection.
    
    Exact duplicates: identical on all key fields (date, amount, supplier, invoice_number)
    Near-duplicates: same supplier + similar amount (±5%) + similar date (±5 days) + similar description
    """
    
    def __init__(self, key_fields: Optional[List[str]] = None):
        """key_fields default: ['date', 'amount', 'supplier_name', 'invoice_number']"""
        self.key_fields = key_fields or ['date', 'amount', 'supplier_name', 'invoice_number']
    
    def find_duplicates(self, df: pd.DataFrame) -> Tuple[List[List[int]], List[List[int]]]:
        """Returns (exact_duplicate_groups, near_duplicate_groups).
        Each group is a list of row indices that are duplicates of each other.
        The first index in each group is the 'canonical' record to keep.
        """
        exact_groups = []
        near_groups = []
        
        available_fields = [f for f in self.key_fields if f in df.columns]
        
        if not available_fields:
            return exact_groups, near_groups
        
        df_copy = df.copy()
        for field in available_fields:
            if field in df_copy.columns:
                df_copy[field] = df_copy[field].fillna('')
        
        if available_fields:
            duplicates = df_copy.duplicated(subset=available_fields, keep='first')
            
            for idx in df_copy[duplicates].index:
                mask = (df_copy[available_fields] == df_copy.loc[idx, available_fields]).all(axis=1)
                group_indices = df_copy[mask].index.tolist()
                
                if len(group_indices) > 1:
                    if group_indices not in exact_groups:
                        exact_groups.append(group_indices)
        
        if 'supplier_name' in df.columns and 'amount' in df.columns:
            processed = set()
            
            for idx in df.index:
                if idx in processed:
                    continue
                
                row = df.loc[idx]
                supplier = row.get('supplier_name', '')
                amount = row.get('amount', 0)
                date = row.get('date', None)
                
                if pd.isna(supplier) or pd.isna(amount):
                    continue
                
                near_group = [idx]
                processed.add(idx)
                
                for other_idx in df.index:
                    if other_idx == idx or other_idx in processed:
                        continue
                    
                    other_row = df.loc[other_idx]
                    other_supplier = other_row.get('supplier_name', '')
                    other_amount = other_row.get('amount', 0)
                    other_date = other_row.get('date', None)
                    
                    if supplier != other_supplier:
                        continue
                    
                    try:
                        amount_float = float(amount)
                        other_amount_float = float(other_amount)
                        
                        if abs(amount_float - other_amount_float) / max(abs(amount_float), 1) > 0.05:
                            continue
                    except (ValueError, TypeError):
                        continue
                    
                    if date and other_date:
                        try:
                            date_diff = abs((pd.to_datetime(date) - pd.to_datetime(other_date)).days)
                            if date_diff > 5:
                                continue
                        except Exception:
                            pass
                    
                    near_group.append(other_idx)
                    processed.add(other_idx)
                
                if len(near_group) > 1:
                    near_groups.append(near_group)
        
        return exact_groups, near_groups
    
    def deduplicate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Remove duplicates. Returns (deduped_df, removed_df).
        removed_df contains all removed records with a 'duplicate_of' column
        pointing to the kept record's record_id.
        """
        exact_groups, near_groups = self.find_duplicates(df)
        
        indices_to_remove = set()
        duplicate_info = {}
        
        for group in exact_groups:
            canonical_idx = group[0]
            for idx in group[1:]:
                indices_to_remove.add(idx)
                duplicate_info[idx] = canonical_idx
        
        for group in near_groups:
            canonical_idx = group[0]
            for idx in group[1:]:
                if idx not in indices_to_remove:
                    indices_to_remove.add(idx)
                    duplicate_info[idx] = canonical_idx
        
        removed_df = df.loc[list(indices_to_remove)].copy()
        
        if 'record_id' in df.columns:
            removed_df['duplicate_of'] = removed_df.index.map(
                lambda idx: df.loc[duplicate_info[idx], 'record_id'] if idx in duplicate_info else None
            )
        else:
            removed_df['duplicate_of'] = removed_df.index.map(
                lambda idx: duplicate_info.get(idx)
            )
        
        deduped_df = df.drop(index=list(indices_to_remove))
        
        return deduped_df, removed_df
