"""Deterministic cleaning passes.

Runs all rule-based transformations on procurement data rows.  Every pass
is a pure function that takes a row dict and returns a (row, changes, flags)
tuple.  The orchestrator calls them in order and accumulates results.

Design: self-contained — does NOT import from scp-cleaning-functions/ so the
Modal image only needs the packages listed in this repo's requirements.txt.
Logic is adapted from the shared/ modules where applicable.
"""

from __future__ import annotations

import math
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import ftfy
from dateutil import parser as dateutil_parser
from rapidfuzz import fuzz

try:
    from . import job_store
except ImportError:
    import job_store  # type: ignore[no-redef]

# =========================================================================
# Individual cleaning passes
# =========================================================================

# -------------------------------------------------------------------------
# 1. Encoding artefact fix
# -------------------------------------------------------------------------


def fix_encoding(value: str) -> Tuple[str, bool]:
    if not value or not isinstance(value, str):
        return value, False
    original = value
    fixed = ftfy.fix_text(value)
    fixed = fixed.replace("\ufeff", "").replace("\x00", "")
    fixed = fixed.replace("\r\n", "\n").replace("\r", "\n")
    fixed = fixed.replace("\u201c", '"').replace("\u201d", '"')
    fixed = fixed.replace("\u2018", "'").replace("\u2019", "'")
    return fixed, fixed != original


TEXT_FIELDS = {
    "description", "supplier_name", "site", "business_unit",
    "category_l1", "category_l2", "category_l3", "payment_terms",
    "approver", "cost_centre",
}


def pass_encoding(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    for col in TEXT_FIELDS:
        val = row.get(col)
        if val is None or not isinstance(val, str):
            continue
        fixed, modified = fix_encoding(val)
        if modified:
            changes.append({"field": col, "original": val, "new": fixed, "rule": "encoding_fix"})
            row[col] = fixed
    return row, changes, []


# -------------------------------------------------------------------------
# 2. Date normalisation → YYYY-MM-DD
# -------------------------------------------------------------------------

def _parse_excel_serial(value: float) -> Optional[datetime]:
    try:
        return datetime(1899, 12, 30) + timedelta(days=value)
    except Exception:
        return None


def normalise_date(value: Any, locale_hint: str = "AU") -> Tuple[Optional[str], float]:
    if value is None:
        return None, 0.0
    if isinstance(value, str) and not value.strip():
        return None, 0.0

    value_str = str(value).strip()

    if isinstance(value, (int, float)) and value > 1000:
        dt = _parse_excel_serial(float(value))
        if dt:
            return dt.strftime("%Y-%m-%d"), 1.0

    if re.match(r"^\d+\.?\d*$", value_str):
        try:
            nv = float(value_str)
            if 1000 < nv < 200000:
                dt = _parse_excel_serial(nv)
                if dt and 1900 <= dt.year <= 2100:
                    return dt.strftime("%Y-%m-%d"), 1.0
        except (ValueError, OverflowError):
            pass

    if re.match(r"^\d{4}-\d{2}-\d{2}$", value_str):
        try:
            datetime.strptime(value_str, "%Y-%m-%d")
            return value_str, 1.0
        except ValueError:
            return None, 0.0

    if re.match(r"^\d{8}$", value_str):
        try:
            dt = datetime.strptime(value_str, "%Y%m%d")
            return dt.strftime("%Y-%m-%d"), 1.0
        except ValueError:
            return None, 0.0

    if re.match(r"^\d{1,2}[/-]\d{1,2}$", value_str):
        return None, 0.0

    dayfirst = locale_hint == "AU"
    try:
        dt = dateutil_parser.parse(value_str, dayfirst=dayfirst, fuzzy=True)
        is_ambiguous = False
        if re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", value_str):
            parts = re.split(r"[/-]", value_str)
            if len(parts) >= 2 and int(parts[0]) <= 12 and int(parts[1]) <= 12:
                is_ambiguous = True
        return dt.strftime("%Y-%m-%d"), 0.7 if is_ambiguous else 1.0
    except Exception:
        return None, 0.0


DATE_COLUMNS = {"date", "invoice_date", "po_date", "delivery_date"}


def pass_dates(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    flags: List[dict] = []
    for col in DATE_COLUMNS:
        val = row.get(col)
        if val is None:
            continue
        normalised, confidence = normalise_date(val)
        if normalised and normalised != str(val).strip():
            changes.append({"field": col, "original": val, "new": normalised, "rule": "date_normalise"})
            row[col] = normalised
        elif normalised is None and val is not None:
            flags.append({"field": col, "reason": "unparseable_date", "value": val})

    # Recompute BHP financial_year if date present
    date_val = row.get("date")
    if date_val and isinstance(date_val, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", date_val):
        try:
            dt = datetime.strptime(date_val, "%Y-%m-%d")
            fy = f"FY{dt.year + 1}" if dt.month >= 7 else f"FY{dt.year}"
            old_fy = row.get("financial_year")
            if old_fy != fy:
                if old_fy is not None:
                    changes.append({"field": "financial_year", "original": old_fy, "new": fy, "rule": "fy_recompute"})
                row["financial_year"] = fy
        except ValueError:
            pass

    return row, changes, flags


# -------------------------------------------------------------------------
# 3. Currency / amount stripping
# -------------------------------------------------------------------------

def normalise_amount(value: Any) -> Tuple[Optional[float], float]:
    if value is None:
        return None, 0.0
    if isinstance(value, (int, float)):
        if math.isnan(value):
            return None, 0.0
        return float(value), 1.0
    value_str = str(value).strip()
    if not value_str:
        return None, 0.0

    is_negative = False
    if value_str.startswith("(") and value_str.endswith(")"):
        is_negative = True
        value_str = value_str[1:-1].strip()
    elif value_str.endswith("-") or " CR" in value_str.upper():
        is_negative = True
        value_str = value_str.replace("-", "").upper().replace("CR", "").strip()

    value_str = re.sub(r"[A-Za-z$£€¥\s]", "", value_str)
    if not value_str:
        return None, 0.0

    has_comma = "," in value_str
    has_dot = "." in value_str
    confidence = 0.8

    if has_comma and has_dot:
        if value_str.rfind(".") > value_str.rfind(","):
            value_str = value_str.replace(",", "")
        else:
            value_str = value_str.replace(".", "").replace(",", ".")
    elif has_comma:
        parts = value_str.split(",")
        if len(parts[-1]) == 2:
            value_str = value_str.replace(",", ".")
        else:
            value_str = value_str.replace(",", "")

    try:
        amount = float(value_str)
        if is_negative:
            amount = -amount
        original_str = str(value)
        if "." not in original_str and "," not in original_str and abs(amount) > 10000:
            confidence = 0.5
        return amount, confidence
    except ValueError:
        return None, 0.0


AMOUNT_COLUMNS = {"amount", "unit_price", "amount_usd"}


def pass_amounts(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    flags: List[dict] = []
    for col in AMOUNT_COLUMNS:
        val = row.get(col)
        if val is None:
            continue
        normalised, confidence = normalise_amount(val)
        if normalised is not None:
            if normalised != val:
                changes.append({"field": col, "original": val, "new": normalised, "rule": "amount_normalise"})
                row[col] = normalised
        else:
            flags.append({"field": col, "reason": "unparseable_amount", "value": val})
    return row, changes, flags


# -------------------------------------------------------------------------
# 4. Triangulation
# -------------------------------------------------------------------------

def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def pass_triangulation(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    flags: List[dict] = []

    amount = _safe_float(row.get("amount"))
    quantity = _safe_float(row.get("quantity"))
    unit_price = _safe_float(row.get("unit_price"))

    missing = [amount is None, quantity is None, unit_price is None]
    n_missing = sum(missing)

    if n_missing == 0 or n_missing >= 2:
        if n_missing >= 2:
            missing_fields = []
            if amount is None:
                missing_fields.append("amount")
            if quantity is None:
                missing_fields.append("quantity")
            if unit_price is None:
                missing_fields.append("unit_price")
            if missing_fields:
                flags.append({
                    "field": ",".join(missing_fields),
                    "reason": "triangulation_insufficient",
                    "value": None,
                })
        return row, changes, flags

    if amount is None and quantity and unit_price:
        derived = round(quantity * unit_price, 2)
        changes.append({"field": "amount", "original": None, "new": derived, "rule": "triangulate_amount"})
        row["amount"] = derived
    elif quantity is None and amount is not None and unit_price and unit_price != 0:
        derived = round(amount / unit_price, 4)
        changes.append({"field": "quantity", "original": None, "new": derived, "rule": "triangulate_quantity"})
        row["quantity"] = derived
    elif unit_price is None and amount is not None and quantity and quantity != 0:
        derived = round(amount / quantity, 2)
        changes.append({"field": "unit_price", "original": None, "new": derived, "rule": "triangulate_unit_price"})
        row["unit_price"] = derived

    return row, changes, flags


# -------------------------------------------------------------------------
# 5. Abbreviation expansion
# -------------------------------------------------------------------------

def pass_abbreviations(
    row: Dict[str, Any], abbrev_dict: Dict[str, str]
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    if not abbrev_dict:
        return row, [], []
    changes: List[dict] = []
    for col in ("description", "category_l1", "category_l2", "category_l3"):
        val = row.get(col)
        if not val or not isinstance(val, str):
            continue
        new_val = val
        for abbr, expansion in abbrev_dict.items():
            pattern = re.compile(r"\b" + re.escape(abbr) + r"\b", re.IGNORECASE)
            new_val = pattern.sub(expansion, new_val)
        if new_val != val:
            changes.append({"field": col, "original": val, "new": new_val, "rule": "abbreviation_expansion"})
            row[col] = new_val
    return row, changes, []


# -------------------------------------------------------------------------
# 6. Vendor fuzzy match
# -------------------------------------------------------------------------

def pass_vendor_match(
    row: Dict[str, Any], vendor_dict: Dict[str, str], threshold: int = 88
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    flags: List[dict] = []
    supplier = row.get("supplier_name")
    if not supplier or not isinstance(supplier, str):
        return row, changes, flags

    supplier_clean = supplier.strip()

    # Exact dict lookup
    if supplier_clean in vendor_dict:
        canonical = vendor_dict[supplier_clean]
        if canonical != supplier_clean:
            changes.append({
                "field": "supplier_name", "original": supplier_clean,
                "new": canonical, "rule": "vendor_dict_exact",
            })
            row["supplier_name"] = canonical
        return row, changes, flags

    # Case-insensitive dict lookup
    lower_map = {k.lower(): v for k, v in vendor_dict.items()}
    if supplier_clean.lower() in lower_map:
        canonical = lower_map[supplier_clean.lower()]
        if canonical != supplier_clean:
            changes.append({
                "field": "supplier_name", "original": supplier_clean,
                "new": canonical, "rule": "vendor_dict_ci",
            })
            row["supplier_name"] = canonical
        return row, changes, flags

    # Fuzzy match against vendor_dict values (canonical names)
    canonical_names = list(set(vendor_dict.values()))
    if not canonical_names:
        return row, changes, flags

    best_score = 0
    best_match = None
    for canonical in canonical_names:
        score = fuzz.token_sort_ratio(supplier_clean.lower(), canonical.lower())
        if score > best_score:
            best_score = score
            best_match = canonical

    if best_score >= threshold and best_match:
        changes.append({
            "field": "supplier_name", "original": supplier_clean,
            "new": best_match, "rule": f"vendor_fuzzy_{best_score}",
        })
        row["supplier_name"] = best_match
    elif best_score < threshold:
        flags.append({
            "field": "supplier_name",
            "reason": f"vendor_fuzzy_below_threshold (best={best_score})",
            "value": supplier_clean,
        })

    return row, changes, flags


# -------------------------------------------------------------------------
# 7. Taxonomy mapping
# -------------------------------------------------------------------------

def _build_keyword_index(taxonomy: Dict[str, Any]) -> List[Tuple[str, str, str, str]]:
    """Return [(keyword_lower, l1, l2, l3), ...] sorted longest-first."""
    entries = []
    for l1, l2_dict in taxonomy.items():
        if not isinstance(l2_dict, dict):
            continue
        for l2, l3_list in l2_dict.items():
            if isinstance(l3_list, list):
                for l3 in l3_list:
                    for kw in _keywords_from(l3):
                        entries.append((kw.lower(), l1, l2, l3))
            for kw in _keywords_from(l2):
                entries.append((kw.lower(), l1, l2, ""))
        for kw in _keywords_from(l1):
            entries.append((kw.lower(), l1, "", ""))
    entries.sort(key=lambda x: -len(x[0]))
    return entries


def _keywords_from(text: str) -> List[str]:
    parts = [text]
    for sep in [",", "&", "/", "(", ")"]:
        new_parts = []
        for p in parts:
            new_parts.extend(p.split(sep))
        parts = new_parts
    return [p.strip() for p in parts if len(p.strip()) > 2]


def pass_taxonomy(
    row: Dict[str, Any], taxonomy: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    if not taxonomy:
        return row, [], []

    changes: List[dict] = []
    flags: List[dict] = []

    cat_l1 = row.get("category_l1", "")
    cat_l2 = row.get("category_l2", "")
    cat_l3 = row.get("category_l3", "")

    has_valid_cat = bool(cat_l1 and cat_l1 != "nan")

    # Validate existing categories against taxonomy
    if has_valid_cat:
        if cat_l1 in taxonomy:
            return row, changes, flags
        # Check if it's a known L2 or L3 that lets us correct L1
        kw_index = _build_keyword_index(taxonomy)
        search_text = f"{cat_l1} {cat_l2} {cat_l3}".lower()
        for kw, l1, l2, l3 in kw_index:
            if kw in search_text:
                if cat_l1 != l1:
                    changes.append({"field": "category_l1", "original": cat_l1, "new": l1, "rule": "taxonomy_remap"})
                    row["category_l1"] = l1
                if l2 and cat_l2 != l2:
                    changes.append({"field": "category_l2", "original": cat_l2, "new": l2, "rule": "taxonomy_remap"})
                    row["category_l2"] = l2
                if l3 and cat_l3 != l3:
                    changes.append({"field": "category_l3", "original": cat_l3, "new": l3, "rule": "taxonomy_remap"})
                    row["category_l3"] = l3
                return row, changes, flags

    # Try to infer from description/supplier
    desc = str(row.get("description", "")).lower()
    supplier = str(row.get("supplier_name", "")).lower()
    search_text = f"{desc} {supplier} {cat_l1} {cat_l2} {cat_l3}".lower()

    kw_index = _build_keyword_index(taxonomy)
    for kw, l1, l2, l3 in kw_index:
        if kw in search_text:
            if not has_valid_cat or cat_l1 == "nan":
                if cat_l1 != l1:
                    changes.append({"field": "category_l1", "original": cat_l1, "new": l1, "rule": "taxonomy_infer"})
                    row["category_l1"] = l1
                if l2:
                    if cat_l2 != l2:
                        changes.append({"field": "category_l2", "original": cat_l2, "new": l2, "rule": "taxonomy_infer"})
                        row["category_l2"] = l2
                if l3:
                    if cat_l3 != l3:
                        changes.append({"field": "category_l3", "original": cat_l3, "new": l3, "rule": "taxonomy_infer"})
                        row["category_l3"] = l3
            return row, changes, flags

    if not has_valid_cat or cat_l1 == "nan":
        flags.append({"field": "category_l1", "reason": "unknown_taxonomy", "value": cat_l1})

    return row, changes, flags


# -------------------------------------------------------------------------
# 8. Unit standardisation
# -------------------------------------------------------------------------

UNIT_MAP = {
    "each": "EA", "ea": "EA", "pcs": "EA", "piece": "EA", "pieces": "EA",
    "item": "EA", "items": "EA", "no.": "EA",
    "litre": "L", "litres": "L", "liter": "L", "liters": "L", "ltr": "L", "lt": "L",
    "kilolitre": "KL", "kilolitres": "KL",
    "tonne": "T", "tonnes": "T", "ton": "T", "tons": "T", "mt": "T",
    "kilogram": "KG", "kilograms": "KG", "kgs": "KG", "kg": "KG",
    "metre": "M", "metres": "M", "meter": "M", "meters": "M", "mtr": "M",
    "sqm": "M2", "sq m": "M2", "square metre": "M2", "square metres": "M2", "m2": "M2",
    "cubic metre": "M3", "cubic metres": "M3", "cum": "M3", "m3": "M3", "cbm": "M3",
    "hour": "HR", "hours": "HR", "hr": "HR", "hrs": "HR",
    "day": "DAY", "days": "DAY",
    "week": "WK", "weeks": "WK", "wk": "WK",
    "month": "MTH", "months": "MTH", "mth": "MTH", "mo": "MTH",
    "set": "SET", "sets": "SET",
    "roll": "ROLL", "rolls": "ROLL",
    "drum": "DRUM", "drums": "DRUM",
    "lot": "LOT", "lots": "LOT",
    "lump sum": "LS", "lumpsum": "LS", "ls": "LS", "lsum": "LS", "l/s": "LS",
}

CANONICAL_UNITS = set(UNIT_MAP.values())


def standardise_unit(value: Any) -> Tuple[Optional[str], float]:
    if not value or not isinstance(value, str):
        return None, 0.0
    v = value.strip().lower()
    if v in UNIT_MAP:
        return UNIT_MAP[v], 1.0
    if value.strip().upper() in CANONICAL_UNITS:
        return value.strip().upper(), 1.0
    best_match, best_score = None, 0.0
    for variant, canonical in UNIT_MAP.items():
        score = fuzz.ratio(v, variant) / 100.0
        if score > best_score:
            best_score = score
            best_match = canonical
    if best_score >= 0.85:
        return best_match, 0.8
    return None, 0.0


def pass_units(row: Dict[str, Any]) -> Tuple[Dict[str, Any], List[dict], List[dict]]:
    changes: List[dict] = []
    unit = row.get("unit")
    if unit is None or not isinstance(unit, str):
        return row, changes, []
    canonical, confidence = standardise_unit(unit)
    if canonical and canonical != unit.strip():
        changes.append({"field": "unit", "original": unit, "new": canonical, "rule": "unit_standardise"})
        row["unit"] = canonical
    return row, changes, []


# =========================================================================
# Pipeline orchestrator
# =========================================================================


def run_deterministic_pass(job: Dict[str, Any]) -> None:
    """Execute all deterministic cleaning rules on every row in the job.

    Updates job in-place: cleaned_data, changes, flagged_rows, rule_stats,
    pass_timings.
    """
    job_store.transition(job, job_store.JobState.DETERMINISTIC_PASS)

    rows = job.get("cleaned_data") or [dict(r) for r in job["input_data"]]
    taxonomy = job.get("taxonomy", {})
    abbrev_dict = job.get("abbreviation_dictionary", {})
    vendor_dict = job.get("vendor_dictionary", {})

    all_changes: List[dict] = list(job.get("changes", []))
    flagged: Dict[int, dict] = {}
    rule_stats: Dict[str, int] = dict(job.get("rule_stats", {}))

    passes = [
        ("encoding", lambda r: pass_encoding(r)),
        ("dates", lambda r: pass_dates(r)),
        ("amounts", lambda r: pass_amounts(r)),
        ("triangulation", lambda r: pass_triangulation(r)),
        ("abbreviations", lambda r: pass_abbreviations(r, abbrev_dict)),
        ("vendor_match", lambda r: pass_vendor_match(r, vendor_dict)),
        ("taxonomy", lambda r: pass_taxonomy(r, taxonomy)),
        ("units", lambda r: pass_units(r)),
    ]

    pass_timings = list(job.get("pass_timings", []))

    for pass_name, pass_fn in passes:
        t0 = time.time()
        pass_changes = 0
        pass_flags = 0

        for idx, row in enumerate(rows):
            row, changes, flags = pass_fn(row)
            rows[idx] = row

            for c in changes:
                c["row_id"] = idx
                all_changes.append(c)
                pass_changes += 1
                rule_stats[c["rule"]] = rule_stats.get(c["rule"], 0) + 1

            for f in flags:
                if idx not in flagged:
                    flagged[idx] = {"row_id": idx, "fields": {}, "reasons": {}, "original_values": {}}
                flagged[idx]["fields"][f["field"]] = f.get("value")
                flagged[idx]["reasons"][f["field"]] = f["reason"]
                flagged[idx]["original_values"][f["field"]] = f.get("value")
                pass_flags += 1

        elapsed = time.time() - t0
        pass_timings.append({
            "pass": pass_name,
            "duration_s": round(elapsed, 3),
            "changes": pass_changes,
            "flags": pass_flags,
            "iteration": job.get("iteration", 0),
        })
        job_store.log(job, f"Pass '{pass_name}': {pass_changes} changes, {pass_flags} flags in {elapsed:.2f}s")

    job["cleaned_data"] = rows
    job["changes"] = all_changes
    job["flagged_rows"] = list(flagged.values())
    job["flagged_count"] = len(flagged)
    job["rows_processed"] = len(rows)
    job["progress_pct"] = 100.0 if not flagged else round((1 - len(flagged) / max(len(rows), 1)) * 100, 1)
    job["rule_stats"] = rule_stats
    job["pass_timings"] = pass_timings
    job_store.log(job, f"Deterministic pass complete: {len(all_changes)} total changes, {len(flagged)} flagged rows")


def apply_llm_responses(
    job: Dict[str, Any],
    responses: List[Dict[str, Any]],
) -> None:
    """Apply LLM-provided values to cleaned_data and re-run deterministic
    passes on the affected rows only.
    """
    job_store.transition(job, job_store.JobState.APPLYING_LLM_RESPONSE)
    rows = job["cleaned_data"]
    changes = list(job.get("changes", []))

    affected_indices = set()

    for resp in responses:
        rid = resp.get("row_id")
        if rid is None or rid < 0 or rid >= len(rows):
            continue
        field_values = resp.get("field_values", {})
        confidence = resp.get("confidence_per_field", {})
        for field_name, new_val in field_values.items():
            old_val = rows[rid].get(field_name)
            conf = confidence.get(field_name, 1.0)
            rows[rid][field_name] = new_val
            changes.append({
                "row_id": rid,
                "field": field_name,
                "original": old_val,
                "new": new_val,
                "rule": "llm_response",
                "confidence": conf,
            })
            affected_indices.add(rid)

    job["cleaned_data"] = rows
    job["changes"] = changes
    job_store.log(job, f"Applied LLM responses for {len(affected_indices)} rows")

    # Re-run deterministic passes on affected rows
    taxonomy = job.get("taxonomy", {})
    abbrev_dict = job.get("abbreviation_dictionary", {})
    vendor_dict = job.get("vendor_dictionary", {})

    mini_passes = [
        ("dates", lambda r: pass_dates(r)),
        ("amounts", lambda r: pass_amounts(r)),
        ("triangulation", lambda r: pass_triangulation(r)),
        ("taxonomy", lambda r: pass_taxonomy(r, taxonomy)),
        ("units", lambda r: pass_units(r)),
    ]

    for idx in affected_indices:
        row = rows[idx]
        for _, fn in mini_passes:
            row, ch, _ = fn(row)
            for c in ch:
                c["row_id"] = idx
                changes.append(c)
        rows[idx] = row

    # Rebuild flagged set from scratch on ALL rows
    flagged: Dict[int, dict] = {}
    all_passes_for_flag = [
        ("dates", lambda r: pass_dates(r)),
        ("amounts", lambda r: pass_amounts(r)),
        ("triangulation", lambda r: pass_triangulation(r)),
        ("vendor_match", lambda r: pass_vendor_match(r, vendor_dict)),
        ("taxonomy", lambda r: pass_taxonomy(r, taxonomy)),
    ]

    for idx, row in enumerate(rows):
        row_copy = dict(row)
        for _, fn in all_passes_for_flag:
            _, _, flags = fn(row_copy)
            for f in flags:
                if idx not in flagged:
                    flagged[idx] = {"row_id": idx, "fields": {}, "reasons": {}, "original_values": {}}
                flagged[idx]["fields"][f["field"]] = f.get("value")
                flagged[idx]["reasons"][f["field"]] = f["reason"]
                flagged[idx]["original_values"][f["field"]] = f.get("value")

    job["cleaned_data"] = rows
    job["changes"] = changes
    job["flagged_rows"] = list(flagged.values())
    job["flagged_count"] = len(flagged)
    job["progress_pct"] = round((1 - len(flagged) / max(len(rows), 1)) * 100, 1)
    job_store.log(job, f"Post-LLM re-check: {len(flagged)} flagged rows remain")
