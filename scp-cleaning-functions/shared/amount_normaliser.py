"""Normalises monetary amounts to clean floats."""
import re
from typing import Optional, Tuple
import pandas as pd


def normalise_amount(value, expected_currency: str = "AUD") -> Tuple[Optional[float], float]:
    """Parse a monetary amount and return (float value, confidence).
    
    Must handle:
    - Already numeric (passthrough, confidence 1.0)
    - Currency symbols: "$1,500.00", "AUD 1500", "USD$1,500"
    - Missing decimal: "150000" when it should be "1500.00" — flag as ambiguous
    - European format: "1.500,00" (dot as thousands sep, comma as decimal)
    - Negative conventions: "(1500)", "-1500", "1500-", "1500 CR"
    - Whitespace and junk chars: " $ 1,500.00 "
    
    Confidence: 1.0 for clean numeric, 0.8 for parsed with symbols,
    0.5 for ambiguous (could be missing decimal), 0.0 for unparseable.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, 0.0
    
    if isinstance(value, (int, float)):
        return float(value), 1.0
    
    value_str = str(value).strip()
    
    is_negative = False
    if value_str.startswith("(") and value_str.endswith(")"):
        is_negative = True
        value_str = value_str[1:-1].strip()
    elif value_str.endswith("-") or " CR" in value_str.upper():
        is_negative = True
        value_str = value_str.replace("-", "").replace("CR", "").replace("cr", "").strip()
    
    value_str = re.sub(r'[A-Za-z$£€¥\s]', '', value_str)
    
    if not value_str:
        return None, 0.0
    
    has_comma = ',' in value_str
    has_dot = '.' in value_str
    
    confidence = 0.8
    
    if has_comma and has_dot:
        last_comma = value_str.rfind(',')
        last_dot = value_str.rfind('.')
        
        if last_dot > last_comma:
            value_str = value_str.replace(',', '')
        else:
            value_str = value_str.replace('.', '').replace(',', '.')
    elif has_comma:
        comma_parts = value_str.split(',')
        if len(comma_parts[-1]) == 2:
            value_str = value_str.replace(',', '.')
        else:
            value_str = value_str.replace(',', '')
    
    try:
        amount = float(value_str)
        
        if is_negative:
            amount = -amount
        
        if '.' not in str(value).replace(',', '') and amount > 10000:
            confidence = 0.5
        
        return amount, confidence
        
    except ValueError:
        return None, 0.0


def normalise_amount_column(series: pd.Series) -> pd.DataFrame:
    """Batch normalise. Returns DataFrame with 'normalised_amount', 'confidence'."""
    results = []
    
    for value in series:
        normalised, confidence = normalise_amount(value)
        results.append({
            'normalised_amount': normalised,
            'confidence': confidence
        })
    
    return pd.DataFrame(results)
