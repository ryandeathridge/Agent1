"""Triangulates amount, quantity, and unit_price fields."""
from typing import Optional, Tuple, Dict
import pandas as pd


def triangulate_amount_qty_price(
    amount: Optional[float],
    quantity: Optional[float],
    unit_price: Optional[float]
) -> Tuple[Optional[float], Optional[float], Optional[float], bool, str]:
    """Triangulate amount, quantity, and unit_price.
    
    If exactly one of the three fields is missing and the other two are present
    and numeric, derive the missing field:
    - Missing unit_price = amount / quantity
    - Missing quantity = amount / unit_price
    - Missing amount = quantity × unit_price
    
    Args:
        amount: The total amount (may be None)
        quantity: The quantity (may be None)
        unit_price: The unit price (may be None)
    
    Returns:
        Tuple of (amount, quantity, unit_price, was_derived, derivation_method)
        - was_derived: True if a value was derived
        - derivation_method: Description of what was derived (empty if nothing derived)
    """
    # Count how many fields are missing
    missing_count = sum([
        amount is None or pd.isna(amount),
        quantity is None or pd.isna(quantity),
        unit_price is None or pd.isna(unit_price)
    ])
    
    # If 0 or 2+ fields are missing, do not derive
    if missing_count == 0 or missing_count >= 2:
        return amount, quantity, unit_price, False, ""
    
    # Exactly one field is missing - derive it
    if amount is None or pd.isna(amount):
        # Derive amount = quantity × unit_price
        if quantity is not None and unit_price is not None:
            try:
                derived_amount = float(quantity) * float(unit_price)
                return round(derived_amount, 2), quantity, unit_price, True, "derived_amount"
            except (ValueError, TypeError):
                return amount, quantity, unit_price, False, ""
    
    elif quantity is None or pd.isna(quantity):
        # Derive quantity = amount / unit_price
        if amount is not None and unit_price is not None:
            try:
                if float(unit_price) == 0:
                    return amount, quantity, unit_price, False, ""
                derived_quantity = float(amount) / float(unit_price)
                return amount, round(derived_quantity, 4), unit_price, True, "derived_quantity"
            except (ValueError, TypeError, ZeroDivisionError):
                return amount, quantity, unit_price, False, ""
    
    elif unit_price is None or pd.isna(unit_price):
        # Derive unit_price = amount / quantity
        if amount is not None and quantity is not None:
            try:
                if float(quantity) == 0:
                    return amount, quantity, unit_price, False, ""
                derived_unit_price = float(amount) / float(quantity)
                return amount, quantity, round(derived_unit_price, 2), True, "derived_unit_price"
            except (ValueError, TypeError, ZeroDivisionError):
                return amount, quantity, unit_price, False, ""
    
    return amount, quantity, unit_price, False, ""


def triangulate_dataframe(
    df: pd.DataFrame,
    amount_col: str = 'amount',
    quantity_col: str = 'quantity',
    unit_price_col: str = 'unit_price'
) -> Tuple[pd.DataFrame, pd.DataFrame, list]:
    """Apply triangulation to a dataframe.
    
    Args:
        df: Input dataframe
        amount_col: Name of amount column
        quantity_col: Name of quantity column
        unit_price_col: Name of unit_price column
    
    Returns:
        Tuple of (processed_df, human_review_df, changes_log)
        - processed_df: DataFrame with derived values filled in
        - human_review_df: DataFrame with records that have 2+ missing fields
        - changes_log: List of change records for provenance tracking
    """
    processed_df = df.copy()
    human_review_records = []
    changes_log = []
    
    # Ensure columns exist
    if amount_col not in processed_df.columns:
        processed_df[amount_col] = None
    if quantity_col not in processed_df.columns:
        processed_df[quantity_col] = None
    if unit_price_col not in processed_df.columns:
        processed_df[unit_price_col] = None
    
    for idx in processed_df.index:
        amount = processed_df.at[idx, amount_col]
        quantity = processed_df.at[idx, quantity_col]
        unit_price = processed_df.at[idx, unit_price_col]
        
        # Count missing fields
        missing_count = sum([
            amount is None or pd.isna(amount),
            quantity is None or pd.isna(quantity),
            unit_price is None or pd.isna(unit_price)
        ])
        
        # If 2 or 3 fields are missing, flag for human review
        if missing_count >= 2:
            human_review_records.append(idx)
            continue
        
        # Try triangulation
        new_amount, new_quantity, new_unit_price, was_derived, method = triangulate_amount_qty_price(
            amount, quantity, unit_price
        )
        
        if was_derived:
            record_id = processed_df.at[idx, 'record_id'] if 'record_id' in processed_df.columns else f'row_{idx}'
            
            # Update the dataframe
            if method == "derived_amount":
                processed_df.at[idx, amount_col] = new_amount
                changes_log.append({
                    'record_id': record_id,
                    'field': amount_col,
                    'original': str(amount) if amount is not None else 'null',
                    'new': str(new_amount),
                    'method': 'derived',
                    'confidence': 0.95,
                    'agent': 'triangulator'
                })
            elif method == "derived_quantity":
                processed_df.at[idx, quantity_col] = new_quantity
                changes_log.append({
                    'record_id': record_id,
                    'field': quantity_col,
                    'original': str(quantity) if quantity is not None else 'null',
                    'new': str(new_quantity),
                    'method': 'derived',
                    'confidence': 0.95,
                    'agent': 'triangulator'
                })
            elif method == "derived_unit_price":
                processed_df.at[idx, unit_price_col] = new_unit_price
                changes_log.append({
                    'record_id': record_id,
                    'field': unit_price_col,
                    'original': str(unit_price) if unit_price is not None else 'null',
                    'new': str(new_unit_price),
                    'method': 'derived',
                    'confidence': 0.95,
                    'agent': 'triangulator'
                })
    
    # Create human review dataframe
    if human_review_records:
        human_review_df = processed_df.loc[human_review_records].copy()
        # Remove these records from processed_df
        processed_df = processed_df.drop(human_review_records)
    else:
        human_review_df = pd.DataFrame()
    
    return processed_df, human_review_df, changes_log
