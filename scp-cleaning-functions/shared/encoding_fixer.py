"""Repairs encoding damage."""
from typing import Tuple
import pandas as pd
import ftfy


def fix_encoding(text: str) -> Tuple[str, bool]:
    """Fix encoding issues in a string. Returns (fixed_string, was_modified).
    
    Uses ftfy.fix_text() as primary repair tool.
    Also handles:
    - BOM character removal (U+FEFF)
    - Smart quote normalisation (curly → straight, or vice versa — configurable)
    - Null byte removal
    - Normalise line endings to \n
    
    Returns the fixed string and whether any changes were made.
    """
    if not text or not isinstance(text, str):
        return text, False
    
    original = text
    
    fixed = ftfy.fix_text(text)
    
    fixed = fixed.replace('\ufeff', '')
    
    fixed = fixed.replace('\x00', '')
    
    fixed = fixed.replace('\r\n', '\n').replace('\r', '\n')
    
    fixed = fixed.replace('"', '"').replace('"', '"')
    fixed = fixed.replace(''', "'").replace(''', "'")
    
    was_modified = (fixed != original)
    
    return fixed, was_modified


def fix_encoding_column(series: pd.Series) -> pd.DataFrame:
    """Batch fix. Returns DataFrame with 'fixed_text', 'was_modified'."""
    results = []
    
    for value in series:
        if pd.isna(value):
            results.append({
                'fixed_text': value,
                'was_modified': False
            })
        else:
            fixed, was_modified = fix_encoding(str(value))
            results.append({
                'fixed_text': fixed,
                'was_modified': was_modified
            })
    
    return pd.DataFrame(results)
