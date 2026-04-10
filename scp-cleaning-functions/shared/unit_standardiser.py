"""Maps unit variants to canonical forms."""
from typing import Optional, Tuple
import pandas as pd
from rapidfuzz import fuzz


UNIT_MAP = {
    "each": "EA", "ea": "EA", "pcs": "EA", "piece": "EA", "pieces": "EA", "item": "EA", "items": "EA",
    "litre": "L", "litres": "L", "liter": "L", "liters": "L", "ltr": "L", "lt": "L",
    "kilolitre": "KL", "kilolitres": "KL",
    "tonne": "T", "tonnes": "T", "ton": "T", "tons": "T", "mt": "T",
    "kilogram": "KG", "kilograms": "KG", "kgs": "KG", "kg": "KG",
    "metre": "M", "metres": "M", "meter": "M", "meters": "M", "mtr": "M",
    "sqm": "M2", "sq m": "M2", "square metre": "M2", "square metres": "M2", "m2": "M2",
    "cubic metre": "M3", "cubic metres": "M3", "cum": "M3", "m3": "M3",
    "hour": "HR", "hours": "HR", "hr": "HR", "hrs": "HR",
    "day": "DAY", "days": "DAY",
    "week": "WK", "weeks": "WK", "wk": "WK",
    "month": "MTH", "months": "MTH", "mth": "MTH", "mo": "MTH",
    "set": "SET", "sets": "SET",
    "roll": "ROLL", "rolls": "ROLL",
    "drum": "DRUM", "drums": "DRUM",
    "lot": "LOT", "lots": "LOT",
    "lump sum": "LS", "lumpsum": "LS", "ls": "LS", "lsum": "LS",
}


def standardise_unit(value: str) -> Tuple[Optional[str], float]:
    """Standardise a unit string. Returns (canonical_unit, confidence).
    Confidence 1.0 for exact match, 0.8 for fuzzy match, 0.0 for no match.
    """
    if not value or not isinstance(value, str):
        return None, 0.0
    
    value_lower = value.strip().lower()
    
    if value_lower in UNIT_MAP:
        return UNIT_MAP[value_lower], 1.0
    
    if value.strip().upper() in set(UNIT_MAP.values()):
        return value.strip().upper(), 1.0
    
    best_match = None
    best_score = 0.0
    
    for variant, canonical in UNIT_MAP.items():
        score = fuzz.ratio(value_lower, variant) / 100.0
        if score > best_score:
            best_score = score
            best_match = canonical
    
    if best_score >= 0.85:
        return best_match, 0.8
    
    return None, 0.0


def standardise_unit_column(series: pd.Series) -> pd.DataFrame:
    """Batch standardise. Returns DataFrame with 'standardised_unit', 'confidence'."""
    results = []
    
    for value in series:
        if pd.isna(value):
            results.append({
                'standardised_unit': None,
                'confidence': 0.0
            })
        else:
            standardised, confidence = standardise_unit(str(value))
            results.append({
                'standardised_unit': standardised,
                'confidence': confidence
            })
    
    return pd.DataFrame(results)
