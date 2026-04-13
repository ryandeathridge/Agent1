"""Generate dirty/degraded procurement data from a clean dataset.

Applies realistic degradation patterns to clean BHP procurement data
to produce a dirty dataset for testing the SCP data-cleaning pipeline.

Degradation types:
  - Date format randomisation (ISO → AU slash, D-Mon-YY, Excel serial, etc.)
  - Amount formatting noise (currency symbols, European format, missing decimals)
  - Vendor name typos and abbreviation damage (realistic, not random garbling)
  - Encoding corruption on description fields (light touch)
  - Unit variant injection
  - Null injection on optional fields
  - Exact and near-duplicate injection

Description handling:
  Descriptions are left as-is from the clean dataset. A separate process
  will generate better descriptions. Null injection still applies at the
  standard rate so some description fields will be blank.
"""

import argparse
import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser

# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

RANDOM_DATE_FORMATS = [
    "iso",          # 2025-07-04
    "au_slash",     # 04/07/2025
    "au_dash",      # 04-07-2025
    "us_slash",     # 07/04/2025
    "d_mon_yy",     # 4-Jul-25
    "d_mon_yyyy",   # 4 July 2025
    "compact",      # 20250704
    "excel_serial", # 45842
]


def _excel_serial(dt: datetime) -> int:
    excel_epoch = datetime(1899, 12, 30)
    return (dt - excel_epoch).days


def _format_date(date_obj: datetime, fmt: str) -> str:
    if fmt == "iso":
        return date_obj.strftime("%Y-%m-%d")
    elif fmt == "au_slash":
        return date_obj.strftime("%d/%m/%Y")
    elif fmt == "au_dash":
        return date_obj.strftime("%d-%m-%Y")
    elif fmt == "us_slash":
        return date_obj.strftime("%m/%d/%Y")
    elif fmt == "d_mon_yy":
        return f"{date_obj.day}-{date_obj.strftime('%b-%y')}"
    elif fmt == "d_mon_yyyy":
        return f"{date_obj.day} {date_obj.strftime('%B %Y')}"
    elif fmt == "compact":
        return date_obj.strftime("%Y%m%d")
    elif fmt == "excel_serial":
        return str(_excel_serial(date_obj))
    return date_obj.strftime("%Y-%m-%d")


def _degrade_date(value: Any) -> Any:
    if pd.isna(value):
        return value
    try:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, pd.Timestamp):
            dt = value.to_pydatetime()
        else:
            dt = pd.to_datetime(value)
            if isinstance(dt, pd.Timestamp):
                dt = dt.to_pydatetime()
    except Exception:
        return value
    return _format_date(dt, random.choice(RANDOM_DATE_FORMATS))


# ---------------------------------------------------------------------------
# Amount helpers
# ---------------------------------------------------------------------------

CURRENCY_NOISE = [
    "${amount}",
    "AUD {amount}",
    "AUD${amount}",
    "$ {amount}",
    "{amount}",
]


def _add_amount_noise(amount: float) -> str:
    fmt = random.choice(CURRENCY_NOISE)
    if random.random() < 0.15:
        # European format: dot as thousands separator, comma as decimal
        formatted = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    elif random.random() < 0.10:
        formatted = str(int(amount))
    else:
        formatted = f"{amount:,.2f}"
    return fmt.format(amount=formatted)


# ---------------------------------------------------------------------------
# Vendor name corruption — realistic dirty patterns only
#
# Realistic dirty patterns seen in real procurement systems:
#   - Abbreviations (Cat, BHP, ABB, SKF)
#   - Missing legal suffix (Caterpillar instead of Caterpillar Inc)
#   - Wrong legal suffix (Pty Ltd vs P/L vs Pty. Ltd.)
#   - All-caps data entry (KOMATSU AUSTRALIA)
#   - Common one-letter typos in the core name
#   - Local entity name vs parent (WesTrac vs Caterpillar)
#   - Old/trading name vs registered name
#
# Intentionally avoided:
#   - Random character insertion that produces unrecognisable strings
#   - Word salad / completely invented names
# ---------------------------------------------------------------------------

# Each entry: canonical name → list of realistic dirty variants
VENDOR_DIRTY_VARIANTS: Dict[str, List[str]] = {
    "Caterpillar": [
        "Cat", "CAT", "Caterpiller", "Caterpillar Inc", "Caterpillar Inc.",
        "CATERPILLAR", "Caterpillar Australia", "Cat Equipment",
    ],
    "Komatsu": [
        "KOMATSU", "Komatsu Australia", "Komatsu Aust", "KOMATSU AUST P/L",
        "Komatsu Australia Pty Ltd", "Komatsu Ltd",
    ],
    "WesTrac": [
        "Westrac", "WESTRAC", "WesTrac Pty Ltd", "WesTrac Equipment",
        "Westrac Equipment Pty Ltd", "WESTRAC PTY LTD",
    ],
    "Hitachi Construction Machinery": [
        "Hitachi", "HITACHI", "Hitachi Construction", "HCM",
        "Hitachi Machinery", "Hitachi Construction Machinery Aust",
    ],
    "Sandvik Mining": [
        "Sandvik", "SANDVIK", "Sandvk", "Sandvik AB", "Sandvik Mining & Rock",
        "Sandvik Mining & Rock Technology", "SANDVIK MINING",
    ],
    "Atlas Copco": [
        "Atlas", "ATLAS COPCO", "Atlas Copco Aust", "Atlas Copco Australia Pty Ltd",
        "AtlasCopco", "Atlas-Copco",
    ],
    "Epiroc": [
        "EPIROC", "Epiroc Australia", "Epiroc Pty Ltd", "EPIROC PTY LTD",
        "Epiroc Australia Pty Ltd",
    ],
    "Metso Outotec": [
        "Metso", "METSO", "Outotec", "Metso:Outotec", "Metso Outotec Pty Ltd",
        "Metso Minerals", "METSO OUTOTEC",
    ],
    "Liebherr": [
        "LIEBHERR", "Liebherr Australia", "Liebherr Pty Ltd",
        "Liebherr-Australia Pty Ltd", "LIEBHERR AUSTRALIA",
    ],
    "Terex": [
        "TEREX", "Terex Corporation", "Terex Australia", "Terex Corp",
    ],
    "Thyssenkrupp": [
        "ThyssenKrupp", "THYSSENKRUPP", "Thyssen Krupp", "thyssenkrupp",
        "ThyssenKrupp Industrial Solutions",
    ],
    "FLSmidth": [
        "FL Smidth", "FLSmidth Pty Ltd", "FLSMIDTH", "F.L.Smidth",
        "FL Smidth & Co",
    ],
    "ABB": [
        "ABB Ltd", "ABB Australia", "ABB Pty Ltd", "A.B.B", "ABB Group",
        "ABB Australia Pty Ltd", "ABB LIMITED",
    ],
    "Siemens": [
        "SIEMENS", "Siemens Ltd", "Siemens Australia", "Siemens Pty Ltd",
        "Siemens AG", "SIEMENS AUSTRALIA",
    ],
    "Schneider Electric": [
        "Schneider", "SCHNEIDER ELECTRIC", "Schneider Electric Aust",
        "Schneider Electric Australia Pty Ltd", "SE Australia",
    ],
    "Honeywell": [
        "HONEYWELL", "Honeywell Ltd", "Honeywell Australia", "Honeywell Pty Ltd",
        "Honeywell Process Solutions",
    ],
    "Emerson Electric": [
        "Emerson", "EMERSON", "Emerson Process", "Emerson Electric Co",
        "Emerson Automation Solutions",
    ],
    "Parker Hannifin": [
        "Parker", "PARKER HANNIFIN", "Parker Hannifin Aust", "Parker Hannifin Corp",
        "Parker Hannifin Australia Pty Ltd",
    ],
    "SKF": [
        "S.K.F", "SKF Australia", "SKF Pty Ltd", "SKF GROUP",
        "SKF Australia Pty Ltd", "skf",
    ],
    "Weir Group": [
        "Weir", "WEIR", "Weir Minerals", "Weir Group PLC",
        "Weir Minerals Australia", "WEIR MINERALS",
    ],
}

# Fallback dirty patterns for any supplier name NOT in the lookup above
_SUFFIX_VARIANTS = [
    ("Pty Ltd", "P/L"), ("Pty Ltd", "Pty. Ltd."), ("Pty Ltd", ""),
    ("Limited", "Ltd"), ("Limited", "Ltd."), ("Inc", "Inc."), ("Inc.", "Inc"),
    ("& Co", "and Co"), ("Corporation", "Corp"), ("Corporation", "Corp."),
]

_CASE_TRANSFORMS = [str.upper, str.lower, str.title]


def _corrupt_vendor(name: str) -> str:
    """Return a realistic dirty variant of *name*.

    Preference order:
    1. Known dirty variant from lookup table (60% chance if available)
    2. Legal suffix swap
    3. Case transform
    4. Single plausible typo in the core word (swap adjacent letters)
    5. Return unchanged
    """
    # Try lookup table first
    if name in VENDOR_DIRTY_VARIANTS and random.random() < 0.65:
        return random.choice(VENDOR_DIRTY_VARIANTS[name])

    # Try partial match against lookup keys (handles "Caterpillar Inc" → lookup "Caterpillar")
    for canonical, variants in VENDOR_DIRTY_VARIANTS.items():
        if canonical.lower() in name.lower() and random.random() < 0.50:
            return random.choice(variants)

    # Legal suffix swap
    if random.random() < 0.25:
        for original_suffix, replacement in _SUFFIX_VARIANTS:
            if original_suffix in name:
                return name.replace(original_suffix, replacement).strip()

    # Case transform
    if random.random() < 0.20:
        return random.choice(_CASE_TRANSFORMS)(name)

    # Adjacent letter swap (single realistic typo)
    if random.random() < 0.10 and len(name) > 4:
        words = name.split()
        word = random.choice(words)
        if len(word) > 3:
            i = random.randint(0, len(word) - 2)
            typo = word[:i] + word[i + 1] + word[i] + word[i + 2:]
            return name.replace(word, typo, 1)

    return name


# ---------------------------------------------------------------------------
# Encoding corruption (descriptions only, very light touch)
# ---------------------------------------------------------------------------

ENCODING_DAMAGE = [
    ("\u2019", "'"),    # curly apostrophe → mojibake
    ("\u201c", '"'),    # left double quote → mojibake
    ("\u201d", '"'),    # right double quote → mojibake
    ("\u00e9", "Ã©"),  # é → mojibake
    ("\u00f1", "Ã±"),  # ñ → mojibake
    ("\ufeff", ""),     # BOM
]


def _corrupt_encoding(text: str) -> str:
    """Inject a single encoding artefact into *text* (3% of rows)."""
    if not text or random.random() > 0.03:
        return text
    original, replacement = random.choice(ENCODING_DAMAGE)
    if original in text:
        return text.replace(original, replacement)
    return text + "\ufeff"  # append BOM as minimal corruption


# ---------------------------------------------------------------------------
# Unit variants
# ---------------------------------------------------------------------------

UNIT_VARIANTS = {
    "EA":  ["each", "pcs", "piece", "EA", "items", "no."],
    "L":   ["litre", "litres", "ltr", "L"],
    "T":   ["tonne", "tonnes", "mt", "T"],
    "KG":  ["kg", "kgs", "kilogram", "KG"],
    "HR":  ["hour", "hours", "hr", "hrs", "HRS"],
    "M":   ["metre", "metres", "mtr", "M"],
    "M2":  ["sqm", "sq m", "square metres", "M2"],
    "M3":  ["m3", "cubic metres", "cbm", "M3"],
    "DAY": ["day", "days", "DAY"],
    "LS":  ["lump sum", "lumpsum", "LS", "L/S"],
}


def _corrupt_unit(unit: str) -> str:
    if unit in UNIT_VARIANTS:
        return random.choice(UNIT_VARIANTS[unit])
    return unit


# ---------------------------------------------------------------------------
# Null injection
# ---------------------------------------------------------------------------

NULL_INJECTION_RATE = 0.03
NULL_CANDIDATE_COLS = [
    "description", "unit", "unit_price", "quantity",
    "currency", "payment_terms", "site",
]


def _inject_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Randomly null out ~3% of values in optional columns."""
    for col in NULL_CANDIDATE_COLS:
        if col not in df.columns:
            continue
        mask = np.random.random(len(df)) < NULL_INJECTION_RATE
        df.loc[mask, col] = np.nan
    return df


# ---------------------------------------------------------------------------
# Duplicate injection
# ---------------------------------------------------------------------------

EXACT_DUP_RATE = 0.03
NEAR_DUP_RATE  = 0.02


def _inject_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    n_dups = int(len(df) * EXACT_DUP_RATE)
    dup_indices = np.random.choice(df.index, size=n_dups, replace=True)
    return pd.concat([df, df.loc[dup_indices].copy()], ignore_index=True)


def _inject_near_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Append near-duplicate rows with small date/amount shifts."""
    n_near = int(len(df) * NEAR_DUP_RATE)
    near_indices = np.random.choice(df.index, size=n_near, replace=True)
    near_dups = df.loc[near_indices].copy()

    for idx in near_dups.index:
        row = near_dups.loc[idx]

        if "date" in near_dups.columns and pd.notna(row["date"]):
            try:
                dt = dateutil_parser.parse(str(row["date"]), dayfirst=True)
                shifted = dt + timedelta(days=random.randint(-3, 3))
                near_dups.at[idx, "date"] = _format_date(shifted, random.choice(RANDOM_DATE_FORMATS))
            except (ValueError, TypeError):
                pass

        if "amount" in near_dups.columns and pd.notna(row["amount"]):
            try:
                amt = float(
                    str(row["amount"])
                    .replace(",", "").replace("$", "").replace("AUD", "").strip()
                )
                near_dups.at[idx, "amount"] = _add_amount_noise(amt * random.uniform(0.97, 1.03))
            except (ValueError, TypeError):
                pass

    return pd.concat([df, near_dups], ignore_index=True)


# ---------------------------------------------------------------------------
# Core degradation pipeline
# ---------------------------------------------------------------------------

def degrade_dataframe(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """Apply all degradation passes to *df* and return the dirty copy."""
    random.seed(seed)
    np.random.seed(seed)

    dirty = df.copy()

    if "date" in dirty.columns:
        dirty["date"] = dirty["date"].apply(_degrade_date)

    for col in ("amount", "unit_price", "amount_usd"):
        if col in dirty.columns:
            dirty[col] = dirty[col].apply(
                lambda v: _add_amount_noise(float(v)) if pd.notna(v) else v
            )

    if "supplier_name" in dirty.columns:
        dirty["supplier_name"] = dirty["supplier_name"].apply(
            lambda v: _corrupt_vendor(str(v)) if pd.notna(v) else v
        )

    # Descriptions: encoding corruption only (no rewrites, no word salad).
    # A separate process will generate better descriptions.
    if "description" in dirty.columns:
        dirty["description"] = dirty["description"].apply(
            lambda v: _corrupt_encoding(str(v)) if pd.notna(v) else v
        )

    if "unit" in dirty.columns:
        dirty["unit"] = dirty["unit"].apply(
            lambda v: _corrupt_unit(str(v)) if pd.notna(v) else v
        )

    dirty = _inject_nulls(dirty)
    dirty = _inject_duplicates(dirty)
    dirty = _inject_near_duplicates(dirty)

    return dirty.sample(frac=1, random_state=seed).reset_index(drop=True)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate dirty procurement data from a clean Excel/CSV file."
    )
    ap.add_argument("input", help="Path to clean data file (.xlsx or .csv)")
    ap.add_argument(
        "-o", "--output", default="bhp_dirty_50k.xlsx",
        help="Output file path (default: bhp_dirty_50k.xlsx)",
    )
    ap.add_argument(
        "-s", "--seed", type=int, default=42,
        help="Random seed for reproducibility",
    )
    args = ap.parse_args()

    ext = args.input.rsplit(".", 1)[-1].lower()
    df = pd.read_excel(args.input) if ext in ("xlsx", "xls") else pd.read_csv(args.input)
    print(f"Loaded {len(df)} clean rows from {args.input}")

    dirty = degrade_dataframe(df, seed=args.seed)
    print(f"Degraded dataset: {len(dirty)} rows (incl. {int(len(df) * EXACT_DUP_RATE)} exact dupes, "
          f"{int(len(df) * NEAR_DUP_RATE)} near-dupes)")

    out_ext = args.output.rsplit(".", 1)[-1].lower()
    if out_ext in ("xlsx", "xls"):
        dirty.to_excel(args.output, index=False)
    else:
        dirty.to_csv(args.output, index=False)
    print(f"Written to {args.output}")


if __name__ == "__main__":
    main()
