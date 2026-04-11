"""Generate dirty/degraded procurement data from a clean dataset.

Applies realistic degradation patterns to clean BHP procurement data
to produce a dirty dataset for testing the SCP data-cleaning pipeline.

Degradation types:
  - Date format randomisation (ISO → AU slash, D-Mon-YY, Excel serial, etc.)
  - Amount formatting noise (currency symbols, European format, missing decimals)
  - Vendor name typos and abbreviation damage
  - Encoding corruption on description fields
  - Unit variant injection
  - Null injection on optional fields
  - Exact and near-duplicate injection
"""

import argparse
import math
import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from dateutil import parser as dateutil_parser

# ---------------------------------------------------------------------------
# Helpers
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

CURRENCY_NOISE = [
    "${amount}",
    "AUD {amount}",
    "AUD${amount}",
    "$ {amount}",
    "{amount}",
]

VENDOR_TYPOS = {
    "Caterpillar Inc": ["Caterpilalr Inc", "CATERPILLAR INC", "Catepillar Inc", "Cat Inc"],
    "Komatsu Australia Pty Ltd": ["KOMATSU AUST P/L", "Komatsu Aust Pty Ltd", "Komatsu Australi"],
    "BHP Group": ["BHP GROUUP", "BHP Grp", "BHP Group Ltd"],
    "Orica Limited": ["ORICA LTD", "Orica Ltd", "Orcica Limited"],
    "Sandvik AB": ["SANDVIK AB", "Sandvk AB", "Sandvik"],
}

ENCODING_DAMAGE = [
    ("\u2019", "'"),     # curly apostrophe
    ("\u201c", '"'),     # left double curly quote
    ("\u201d", '"'),     # right double curly quote
    ("\u00e9", "Ã©"),   # mojibake for é
    ("\u00f1", "Ã±"),   # mojibake for ñ
    ("\ufeff", ""),      # BOM char
]


def _excel_serial(dt: datetime) -> int:
    """Convert a datetime to an Excel serial date number."""
    excel_epoch = datetime(1899, 12, 30)
    return (dt - excel_epoch).days


def _format_date(date_obj: datetime, fmt: str) -> str:
    """Return *date_obj* formatted according to the named format style.

    Bug 5 fix (Windows compatibility):
    Previously used ``date_obj.strftime("%-d-%b-%y")`` which relies on a
    GNU libc extension and crashes on Windows (including Azure Windows
    consumption-plan hosts).  Replaced with manual ``str(date_obj.day)``
    concatenation so behaviour is identical on every platform.
    """
    if fmt == "iso":
        return date_obj.strftime("%Y-%m-%d")
    elif fmt == "au_slash":
        return date_obj.strftime("%d/%m/%Y")
    elif fmt == "au_dash":
        return date_obj.strftime("%d-%m-%Y")
    elif fmt == "us_slash":
        return date_obj.strftime("%m/%d/%Y")
    elif fmt == "d_mon_yy":
        day = str(date_obj.day)
        return f"{day}-{date_obj.strftime('%b-%y')}"
    elif fmt == "d_mon_yyyy":
        day = str(date_obj.day)
        return f"{day} {date_obj.strftime('%B %Y')}"
    elif fmt == "compact":
        return date_obj.strftime("%Y%m%d")
    elif fmt == "excel_serial":
        return str(_excel_serial(date_obj))
    else:
        return date_obj.strftime("%Y-%m-%d")


def _add_amount_noise(amount: float) -> str:
    """Return *amount* formatted with random currency/thousands noise."""
    fmt = random.choice(CURRENCY_NOISE)
    if random.random() < 0.15:
        formatted = f"{amount:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    elif random.random() < 0.1:
        formatted = str(int(amount))
    else:
        formatted = f"{amount:,.2f}"
    return fmt.format(amount=formatted)


def _corrupt_vendor(name: str) -> str:
    """Return a noisy variant of a vendor name."""
    if name in VENDOR_TYPOS and random.random() < 0.6:
        return random.choice(VENDOR_TYPOS[name])
    if random.random() < 0.15:
        return name.upper()
    if random.random() < 0.10:
        idx = random.randint(0, max(0, len(name) - 2))
        return name[:idx] + random.choice(string.ascii_lowercase) + name[idx + 1:]
    return name


def _corrupt_encoding(text: str) -> str:
    """Randomly inject encoding damage into *text*."""
    if not text or random.random() > 0.05:
        return text
    original, replacement = random.choice(ENCODING_DAMAGE)
    if original in text:
        return text.replace(original, replacement)
    return text + random.choice(["\ufeff", "\x00", ""])


UNIT_VARIANTS = {
    "EA": ["each", "pcs", "piece", "EA", "items"],
    "L": ["litre", "litres", "ltr", "L"],
    "T": ["tonne", "tonnes", "mt", "T"],
    "KG": ["kg", "kgs", "kilogram", "KG"],
    "HR": ["hour", "hours", "hr", "hrs"],
    "M": ["metre", "metres", "mtr", "M"],
    "M2": ["sqm", "sq m", "square metres", "M2"],
    "DAY": ["day", "days", "DAY"],
    "LS": ["lump sum", "lumpsum", "LS"],
}


def _corrupt_unit(unit: str) -> str:
    """Return a random variant spelling of *unit*."""
    if unit in UNIT_VARIANTS:
        return random.choice(UNIT_VARIANTS[unit])
    return unit

# ---------------------------------------------------------------------------
# Core degradation pipeline
# ---------------------------------------------------------------------------


def degrade_dataframe(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """Apply degradation to every applicable column in *df*.

    Returns a new DataFrame with dirty values.
    """
    rng = random.Random(seed)
    random.seed(seed)
    np.random.seed(seed)

    dirty = df.copy()

    if "date" in dirty.columns:
        dirty["date"] = dirty["date"].apply(_degrade_date)

    if "amount" in dirty.columns:
        dirty["amount"] = dirty["amount"].apply(
            lambda v: _add_amount_noise(v) if pd.notna(v) else v
        )

    for col in ("unit_price", "amount_usd"):
        if col in dirty.columns:
            dirty[col] = dirty[col].apply(
                lambda v: _add_amount_noise(v) if pd.notna(v) else v
            )

    if "supplier_name" in dirty.columns:
        dirty["supplier_name"] = dirty["supplier_name"].apply(
            lambda v: _corrupt_vendor(v) if pd.notna(v) else v
        )

    if "description" in dirty.columns:
        dirty["description"] = dirty["description"].apply(
            lambda v: _corrupt_encoding(v) if pd.notna(v) else v
        )

    if "unit" in dirty.columns:
        dirty["unit"] = dirty["unit"].apply(
            lambda v: _corrupt_unit(v) if pd.notna(v) else v
        )

    dirty = _inject_nulls(dirty)

    dirty = _inject_duplicates(dirty)

    dirty = _inject_near_duplicates(dirty)

    return dirty.sample(frac=1, random_state=seed).reset_index(drop=True)


def _degrade_date(value: Any) -> Any:
    """Convert a single date value into a randomly degraded format."""
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
    fmt = random.choice(RANDOM_DATE_FORMATS)
    return _format_date(dt, fmt)


NULL_INJECTION_RATE = 0.03
NULL_CANDIDATE_COLS = [
    "description", "unit", "unit_price", "quantity",
    "currency", "payment_terms", "site",
]


def _inject_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """Randomly null out ~3 % of values in optional columns."""
    for col in NULL_CANDIDATE_COLS:
        if col not in df.columns:
            continue
        mask = np.random.random(len(df)) < NULL_INJECTION_RATE
        df.loc[mask, col] = np.nan
    return df


EXACT_DUP_RATE = 0.03


def _inject_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Append exact-duplicate rows (≈3 % of total)."""
    n_dups = int(len(df) * EXACT_DUP_RATE)
    dup_indices = np.random.choice(df.index, size=n_dups, replace=True)
    dups = df.loc[dup_indices].copy()
    return pd.concat([df, dups], ignore_index=True)


NEAR_DUP_RATE = 0.02


def _inject_near_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Append near-duplicate rows with small date / amount shifts.

    Bug 6 fix (strptime on degraded dates):
    Previously used ``datetime.strptime(str(row['date']), '%Y-%m-%d')`` to
    parse the date when applying the near-duplicate shift.  At this point in
    the pipeline the date column has *already been degraded* to random
    formats (e.g. ``07/04/2025``, ``4-Jul-25``, ``45842``), so a fixed
    ``%Y-%m-%d`` pattern would raise ``ValueError`` on most rows.
    Now uses ``dateutil.parser.parse(str(row['date']), dayfirst=True)``
    which handles every format emitted by ``_degrade_date``.
    """
    n_near = int(len(df) * NEAR_DUP_RATE)
    near_indices = np.random.choice(df.index, size=n_near, replace=True)
    near_dups = df.loc[near_indices].copy()

    for idx in near_dups.index:
        row = near_dups.loc[idx]

        if "date" in near_dups.columns and pd.notna(row["date"]):
            try:
                dt = dateutil_parser.parse(str(row["date"]), dayfirst=True)
                shifted = dt + timedelta(days=random.randint(-3, 3))
                fmt = random.choice(RANDOM_DATE_FORMATS)
                near_dups.at[idx, "date"] = _format_date(shifted, fmt)
            except (ValueError, TypeError):
                pass

        if "amount" in near_dups.columns and pd.notna(row["amount"]):
            try:
                amt = float(str(row["amount"]).replace(",", "").replace("$", "")
                            .replace("AUD", "").strip())
                amt *= random.uniform(0.97, 1.03)
                near_dups.at[idx, "amount"] = _add_amount_noise(amt)
            except (ValueError, TypeError):
                pass

    return pd.concat([df, near_dups], ignore_index=True)

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
    if ext in ("xlsx", "xls"):
        df = pd.read_excel(args.input)
    else:
        df = pd.read_csv(args.input)

    print(f"Loaded {len(df)} clean rows from {args.input}")

    dirty = degrade_dataframe(df, seed=args.seed)

    print(f"Degraded dataset has {len(dirty)} rows (incl. duplicates)")

    out_ext = args.output.rsplit(".", 1)[-1].lower()
    if out_ext in ("xlsx", "xls"):
        dirty.to_excel(args.output, index=False)
    else:
        dirty.to_csv(args.output, index=False)

    print(f"Written to {args.output}")


if __name__ == "__main__":
    main()
