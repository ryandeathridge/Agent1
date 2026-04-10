"""Validate agent — compares pipeline-cleaned output against known-clean reference data.

Used in end-to-end testing (step 9) to measure overall cleaning accuracy by
scoring each record field-by-field against the ground-truth dataset
(e.g. bhp_clean_50k.xlsx).
"""
import logging
import re
from typing import Optional

import numpy as np
import pandas as pd
from dateutil.parser import parse as dateutil_parse
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field-level matchers
# ---------------------------------------------------------------------------

def exact_match(a, b) -> bool:
    """Case-insensitive exact string match after whitespace normalisation."""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    return str(a).strip().lower() == str(b).strip().lower()


def fuzzy_match(a, b, threshold: int = 85) -> bool:
    """Token-sort fuzzy match above *threshold*."""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    return fuzz.token_sort_ratio(str(a).strip(), str(b).strip()) >= threshold


def numeric_match(a, b, rel_tol: float = 0.01) -> bool:
    """Numeric match within *rel_tol* relative tolerance."""
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    try:
        fa, fb = float(a), float(b)
        if fa == 0 and fb == 0:
            return True
        denom = max(abs(fa), abs(fb))
        return abs(fa - fb) / denom <= rel_tol
    except (ValueError, TypeError):
        return False


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")


def _parse_date(value):
    """Parse a date value using AU day-first convention.

    ISO-format strings (YYYY-MM-DD...) are parsed without dayfirst to
    avoid dateutil re-interpreting the month as the day.
    """
    s = str(value).strip()
    if _ISO_DATE_RE.match(s):
        return dateutil_parse(s, dayfirst=False)
    return dateutil_parse(s, dayfirst=True)


def date_match(a, b, tolerance_days: int = 1) -> bool:
    """Date match within *tolerance_days*.

    Parses both values with AU day-first convention so that ambiguous
    strings like '03/04/2025' are read as 3-Apr rather than 4-Mar.
    """
    if pd.isna(a) and pd.isna(b):
        return True
    if pd.isna(a) or pd.isna(b):
        return False
    try:
        da = _parse_date(a)
        db = _parse_date(b)
        return abs((da - db).days) <= tolerance_days
    except Exception:
        return False


def category_match(a, b) -> bool:
    """Exact match for taxonomy categories (case-insensitive)."""
    return exact_match(a, b)


# ---------------------------------------------------------------------------
# Per-field scoring weights & matcher selection
# ---------------------------------------------------------------------------

FIELD_CONFIG = {
    "date":          {"weight": 1.0, "matcher": date_match},
    "amount":        {"weight": 1.5, "matcher": numeric_match},
    "supplier_name": {"weight": 1.5, "matcher": fuzzy_match},
    "supplier_id":   {"weight": 1.0, "matcher": exact_match},
    "description":   {"weight": 0.5, "matcher": fuzzy_match},
    "unit":          {"weight": 0.5, "matcher": exact_match},
    "currency":      {"weight": 0.5, "matcher": exact_match},
    "category_l1":   {"weight": 1.0, "matcher": category_match},
    "category_l2":   {"weight": 0.8, "matcher": category_match},
    "category_l3":   {"weight": 0.6, "matcher": category_match},
}


def _pick_matcher(field: str):
    """Return (weight, matcher) for a field, with sensible defaults."""
    cfg = FIELD_CONFIG.get(field, {"weight": 0.5, "matcher": exact_match})
    return cfg["weight"], cfg["matcher"]


# ---------------------------------------------------------------------------
# Record & dataset scoring
# ---------------------------------------------------------------------------

def score_record(cleaned: dict, reference: dict, fields: Optional[list] = None) -> dict:
    """Score a single cleaned record against its reference.

    Returns a dict with per-field match booleans and a weighted score.
    """
    if fields is None:
        fields = [f for f in reference if f != "record_id"]

    total_weight = 0.0
    weighted_hits = 0.0
    field_results = {}

    for field in fields:
        weight, matcher = _pick_matcher(field)
        matched = matcher(cleaned.get(field), reference.get(field))
        field_results[field] = matched
        total_weight += weight
        if matched:
            weighted_hits += weight

    score = weighted_hits / total_weight if total_weight else 0.0
    return {"record_id": cleaned.get("record_id"), "score": score, **field_results}


def score_records(
    cleaned_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    join_key: str = "record_id",
    fields: Optional[list] = None,
) -> pd.DataFrame:
    """Score every record in *cleaned_df* against *reference_df*.

    Returns a DataFrame with one row per matched record containing per-field
    results and an overall weighted score.
    """
    merged = cleaned_df.merge(
        reference_df,
        on=join_key,
        how="inner",
        suffixes=("_cleaned", "_ref"),
    )

    if fields is None:
        base_fields = [
            c.replace("_cleaned", "")
            for c in merged.columns
            if c.endswith("_cleaned")
        ]
    else:
        base_fields = fields

    rows = []

    for _, row in merged.iterrows():
        cleaned_vals = {f: row.get(f"{f}_cleaned", row.get(f)) for f in base_fields}
        ref_vals = {f: row.get(f"{f}_ref", row.get(f)) for f in base_fields}
        cleaned_vals["record_id"] = row[join_key]
        result = score_record(cleaned_vals, ref_vals, fields=base_fields)
        rows.append(result)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Aggregate reporting
# ---------------------------------------------------------------------------

def compute_accuracy_report(
    cleaned_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    join_key: str = "record_id",
    fields: Optional[list] = None,
) -> dict:
    """High-level accuracy report comparing cleaned output to reference.

    Returns a dict with overall accuracy, per-field accuracy, and a
    breakdown DataFrame (available under key ``'details'``).
    """
    details = score_records(cleaned_df, reference_df, join_key=join_key, fields=fields)

    if details.empty:
        return {
            "overall_accuracy": 0.0,
            "records_matched": 0,
            "records_total": len(cleaned_df),
            "per_field_accuracy": {},
            "details": details,
        }

    overall = details["score"].mean()

    field_cols = [c for c in details.columns if c not in ("record_id", "score")]
    per_field = {col: details[col].mean() for col in field_cols}

    return {
        "overall_accuracy": round(overall, 4),
        "records_matched": len(details),
        "records_total": len(cleaned_df),
        "per_field_accuracy": {k: round(v, 4) for k, v in per_field.items()},
        "details": details,
    }


# ---------------------------------------------------------------------------
# CLI entry-point (for ad-hoc local testing)
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Validate cleaning pipeline output")
    parser.add_argument("cleaned", help="Path to cleaned output (CSV/Parquet/Excel)")
    parser.add_argument("reference", help="Path to reference clean data")
    parser.add_argument("--join-key", default="record_id")
    parser.add_argument("--threshold", type=float, default=0.85,
                        help="Minimum overall accuracy to pass (default 0.85)")
    args = parser.parse_args()

    cleaned_df = _load(args.cleaned)
    reference_df = _load(args.reference)

    report = compute_accuracy_report(cleaned_df, reference_df, join_key=args.join_key)

    print(f"\nRecords matched : {report['records_matched']} / {report['records_total']}")
    print(f"Overall accuracy: {report['overall_accuracy']:.2%}")
    print("\nPer-field accuracy:")
    for field, acc in sorted(report["per_field_accuracy"].items()):
        print(f"  {field:20s} {acc:.2%}")

    passed = report["overall_accuracy"] >= args.threshold
    print(f"\nResult: {'PASS' if passed else 'FAIL'} (threshold {args.threshold:.0%})")
    return 0 if passed else 1


def _load(path: str) -> pd.DataFrame:
    """Load a DataFrame from CSV, Parquet, or Excel based on extension."""
    path_lower = path.lower()
    if path_lower.endswith(".parquet"):
        return pd.read_parquet(path)
    elif path_lower.endswith(".csv"):
        return pd.read_csv(path)
    elif path_lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(path)
    else:
        raise ValueError(f"Unsupported file format: {path}")


if __name__ == "__main__":
    raise SystemExit(main())
