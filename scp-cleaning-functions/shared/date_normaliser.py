"""Normalises dates from any format to ISO 8601 (YYYY-MM-DD)."""
import re
from typing import Optional, Tuple
import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta


def _parse_excel_serial_date(value: float) -> Optional[datetime]:
    """Convert Excel serial date to datetime."""
    try:
        excel_epoch = datetime(1899, 12, 30)
        return excel_epoch + timedelta(days=value)
    except Exception:
        return None


def normalise_date(value: str, locale_hint: str = "AU") -> Tuple[Optional[str], float]:
    """Parse a date string in any format and return (ISO date string, confidence).
    
    Must handle at minimum:
    - YYYY-MM-DD (already clean)
    - DD/MM/YYYY, DD-MM-YYYY (AU standard)
    - MM/DD/YYYY, MM-DD-YY (US format — flag as ambiguous if day ≤ 12)
    - D-Mon-YY, D Mon YYYY ("5-Jan-25", "5 January 2025")
    - YYYYMMDD (no separators)
    - YYYY-MM-DD HH:MM:SS (strip time component)
    - DD/MM (missing year — return None, confidence 0)
    - Excel serial dates (numeric like 45678)
    
    locale_hint determines ambiguity resolution: "AU" means DD/MM/YYYY is preferred.
    Confidence: 1.0 for unambiguous, 0.7 for ambiguous (e.g. 03/04/2025), 0.0 for unparseable.
    
    Uses python-dateutil with dayfirst=True for AU locale.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return None, 0.0
    
    value_str = str(value).strip()
    
    if isinstance(value, (int, float)) and value > 1000:
        dt = _parse_excel_serial_date(float(value))
        if dt:
            return dt.strftime("%Y-%m-%d"), 1.0
    
    if isinstance(value, str) and re.match(r'^\d+\.?\d*$', value_str):
        try:
            numeric_val = float(value_str)
            if 1000 < numeric_val < 200000:
                dt = _parse_excel_serial_date(numeric_val)
                if dt and 1900 <= dt.year <= 2100:
                    return dt.strftime("%Y-%m-%d"), 1.0
        except (ValueError, OverflowError):
            pass
    
    if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
        try:
            datetime.strptime(value_str, "%Y-%m-%d")
            return value_str, 1.0
        except ValueError:
            return None, 0.0
    
    if re.match(r'^\d{8}$', value_str):
        try:
            dt = datetime.strptime(value_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d"), 1.0
        except ValueError:
            return None, 0.0
    
    if re.match(r'^\d{1,2}[/-]\d{1,2}$', value_str):
        return None, 0.0
    
    dayfirst = (locale_hint == "AU")
    
    try:
        dt = parser.parse(value_str, dayfirst=dayfirst, fuzzy=True)
        
        is_ambiguous = False
        if re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', value_str):
            parts = re.split(r'[/-]', value_str)
            if len(parts) >= 2:
                first_num = int(parts[0])
                second_num = int(parts[1])
                if first_num <= 12 and second_num <= 12:
                    is_ambiguous = True
        
        confidence = 0.7 if is_ambiguous else 1.0
        return dt.strftime("%Y-%m-%d"), confidence
        
    except Exception:
        return None, 0.0


def normalise_date_column(series: pd.Series, locale_hint: str = "AU") -> pd.DataFrame:
    """Batch normalise an entire column. Returns DataFrame with columns:
    'normalised_date', 'confidence', 'original_format_detected'
    """
    results = []
    
    for value in series:
        normalised, confidence = normalise_date(value, locale_hint)
        
        original_format = "unknown"
        if value is not None:
            value_str = str(value).strip()
            if re.match(r'^\d{4}-\d{2}-\d{2}$', value_str):
                original_format = "ISO"
            elif re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', value_str):
                original_format = "DD/MM/YYYY or MM/DD/YYYY"
            elif re.match(r'^\d{8}$', value_str):
                original_format = "YYYYMMDD"
            elif isinstance(value, (int, float)) and value > 1000:
                original_format = "Excel serial"
        
        results.append({
            'normalised_date': normalised,
            'confidence': confidence,
            'original_format_detected': original_format
        })
    
    return pd.DataFrame(results)
