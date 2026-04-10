#!/usr/bin/env python3
"""Generate a clean synthetic BHP procurement dataset.

This script produces a realistic 50K-row procurement dataset with proper
Zipf-distributed supplier selection, correct Australian financial-year
labelling, ABN numbers for all Australian sites, and reproducible Faker
output.
"""

import argparse
import json
import string
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SITES_CONFIG = [
    {"site": "WAIO Newman",       "business_unit": "Iron Ore",  "currency": "AUD", "weight": 0.224},
    {"site": "WAIO South Flank",  "business_unit": "Iron Ore",  "currency": "AUD", "weight": 0.132},
    {"site": "WAIO Mining Area C","business_unit": "Iron Ore",  "currency": "AUD", "weight": 0.089},
    {"site": "Escondida",         "business_unit": "Copper",    "currency": "CLP", "weight": 0.158},
    {"site": "BMA Goonyella",     "business_unit": "Coal",      "currency": "AUD", "weight": 0.071},
    {"site": "Olympic Dam",       "business_unit": "Copper",    "currency": "AUD", "weight": 0.044},
    {"site": "BMA Blackwater",    "business_unit": "Coal",      "currency": "AUD", "weight": 0.044},
    {"site": "Spence",            "business_unit": "Copper",    "currency": "CLP", "weight": 0.043},
    {"site": "Nickel West",       "business_unit": "Nickel",    "currency": "AUD", "weight": 0.035},
    {"site": "Corporate Melbourne","business_unit": "Corporate","currency": "AUD", "weight": 0.026},
    {"site": "Corporate Houston", "business_unit": "Corporate", "currency": "USD", "weight": 0.010},
    {"site": "Corporate Santiago","business_unit": "Corporate",  "currency": "CLP", "weight": 0.009},
]

CATEGORY_TAXONOMY = {
    "Energy & Fuel": {
        "Diesel": ["Bulk diesel delivery", "Diesel storage", "Fuel management systems"],
        "Electricity": ["Grid electricity", "On-site generation", "Renewable PPAs", "Transmission charges"],
        "Natural Gas": ["Gas processing", "LNG supply", "Pipeline gas"],
    },
    "Equipment & Parts": {
        "Electrical & Instrumentation": ["Cables & wiring", "PLCs", "Sensors & instrumentation", "Switchgear", "Transformers"],
        "Fixed Plant": ["Conveyor components", "Crusher parts", "Mill liners", "Pump parts", "Screen media"],
        "Mobile Equipment": ["Dozer parts", "Excavator parts", "Grader parts", "Haul truck parts", "Light vehicle fleet"],
    },
    "Facilities & Administration": {
        "Insurance": ["Liability insurance", "Marine cargo", "Property insurance", "Workers comp"],
        "Office & Administration": ["Corporate travel", "Furniture", "Office supplies", "Printing & copying"],
        "Utilities": ["Internet services", "Sewage", "Telecommunications", "Water supply"],
    },
    "Maintenance & Repair": {
        "Breakdown & Emergency": ["Crane hire", "Emergency repair", "Mechanical contractors", "Welding services"],
        "Building & Civil": ["Building maintenance", "Concrete", "Earthworks", "Road maintenance"],
        "Planned Maintenance": ["Condition monitoring", "Lubrication services", "Predictive maintenance", "Shutdown services"],
    },
    "Mining Services & Contractors": {
        "Drill & Blast": ["Blast design services", "Drill rig hire", "Drilling consumables", "Explosives & detonators"],
        "Load & Haul": ["Excavator hire", "Loader hire", "Operator labour", "Truck hire"],
        "Mine Planning & Technical": ["Geological consulting", "Geotechnical services", "Mine planning software", "Survey services"],
    },
    "People & Labour": {
        "Contract Labour": ["Semi-skilled operators", "Skilled trades (electrical, mechanical, boilermaking)", "Unskilled labour"],
        "FIFO & Accommodation": ["Camp accommodation", "Catering", "Charter flights", "Village management"],
        "Recruitment & Training": ["Competency assessments", "Recruitment agencies", "Safety inductions", "Training providers"],
    },
    "Professional Services": {
        "Engineering & Design": ["Electrical engineering", "Process engineering", "Project management", "Structural engineering"],
        "Environmental & Compliance": ["Environmental monitoring", "Rehabilitation services", "Waste management", "Water treatment"],
        "IT & Technology": ["Cloud services", "Cybersecurity", "Hardware", "Network infrastructure", "Software licences"],
        "Legal & Advisory": ["Audit services", "Legal counsel", "Management consulting", "Tax advisory"],
    },
    "Raw Materials & Consumables": {
        "Chemical Reagents": ["Ammonia", "Flocculants", "Flotation reagents", "Lime", "Sulphuric acid"],
        "Grinding Media": ["Rod charge", "SAG mill liners", "Steel balls"],
        "Structural Steel & Fabrication": ["Custom fabrication", "Pipe & fittings", "Steel plate", "Structural steel"],
        "Tyres & Rubber": ["Conveyor belting", "Haul truck tyres", "Light vehicle tyres", "Rubber lining"],
    },
    "Safety & Environment": {
        "Emergency Response": ["Emergency vehicles", "Fire suppression", "First aid supplies", "Rescue equipment"],
        "Environmental Management": ["Dust suppression", "Monitoring equipment", "Rehabilitation materials", "Tailings management"],
        "PPE & Safety Equipment": ["Fall protection", "Hard hats & helmets", "Hi-vis clothing", "Respirators", "Safety boots"],
    },
    "Transport & Logistics": {
        "Rail": ["Port handling", "Rail haulage", "Rolling stock maintenance", "Track maintenance"],
        "Road Transport": ["Bulk material haulage", "Courier services", "General freight", "Oversize/overmass loads"],
        "Shipping & Port": ["Demurrage", "Port fees", "Stevedoring", "Vessel charter"],
    },
}

UNITS = ["EA", "L", "KL", "T", "KG", "M", "M2", "M3", "HR", "DAY", "WK", "MTH", "SET", "ROLL", "DRUM", "LOT", "LS"]
PAYMENT_TERMS = ["Net 30", "Net 45", "Net 60", "Net 90", "Net 14", "COD", "Prepaid", "Progress"]
STATUSES = ["Approved", "Paid", "Pending Approval", "On Hold", "Cancelled", "Disputed"]

DATE_START = datetime(2024, 4, 1)
DATE_END = datetime(2026, 3, 31)

# ---------------------------------------------------------------------------
# Helper: Zipf-distributed supplier selection
# ---------------------------------------------------------------------------


def zipf_supplier_distribution(rng: np.random.Generator, n_suppliers: int, n_records: int) -> np.ndarray:
    """Return *n_records* supplier indices drawn from a Zipf(1.5) distribution
    clamped to [0, n_suppliers-1].
    """
    zipf_idx = rng.zipf(1.5, n_records)
    return np.minimum(zipf_idx - 1, n_suppliers - 1)


# ---------------------------------------------------------------------------
# Helper: Australian financial-year label from a date
# ---------------------------------------------------------------------------


def fy_from_date(date_obj: datetime) -> str:
    """Return the Australian financial-year label for *date_obj*.

    The Australian FY runs Jul–Jun.  A date in Jul 2024 belongs to FY2025;
    a date in Jun 2025 also belongs to FY2025.
    """
    if date_obj.month >= 7:
        return f"FY{date_obj.year + 1}"
    return f"FY{date_obj.year}"


# ---------------------------------------------------------------------------
# Helper: Generate a valid-looking ABN (11-digit, formatted with spaces)
# ---------------------------------------------------------------------------


def generate_abn(rng: np.random.Generator) -> str:
    """Return a random 11-digit ABN formatted as 'XX XXX XXX XXX'."""
    digits = "".join(str(d) for d in rng.integers(0, 10, size=11))
    return f"{digits[:2]} {digits[2:5]} {digits[5:8]} {digits[8:11]}"


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------


def build_supplier_pool(fake: Faker, rng: np.random.Generator, n: int = 500) -> list[dict]:
    """Create a pool of *n* synthetic suppliers with IDs and optional ABNs."""
    suppliers = []
    for i in range(n):
        suppliers.append({
            "supplier_name": fake.company(),
            "supplier_id": f"SUP-{i:05d}",
        })
    return suppliers


def generate_clean_dataset(args: argparse.Namespace) -> pd.DataFrame:
    rng = np.random.default_rng(args.seed)

    # Bug 4 fix: Faker(seed=...) constructor is broken in modern Faker.
    # Use the class-method seeding API instead.
    fake = Faker()
    Faker.seed(args.seed)

    n_records = args.n_records

    all_suppliers = build_supplier_pool(fake, rng, n=args.n_suppliers)

    # Bug 1 fix: use Zipf distribution instead of uniform supplier selection
    supplier_indices = zipf_supplier_distribution(rng, len(all_suppliers), n_records)
    selected_suppliers = [all_suppliers[i] for i in supplier_indices]

    # Flatten the taxonomy into a list of (l1, l2, l3) tuples
    flat_categories: list[tuple[str, str, str]] = []
    for l1, l2_dict in CATEGORY_TAXONOMY.items():
        for l2, l3_list in l2_dict.items():
            for l3 in l3_list:
                flat_categories.append((l1, l2, l3))

    cat_indices = rng.integers(0, len(flat_categories), size=n_records)
    categories = [flat_categories[i] for i in cat_indices]

    # Generate random dates within the range
    total_days = (DATE_END - DATE_START).days
    day_offsets = rng.integers(0, total_days + 1, size=n_records)
    dates = [DATE_START + timedelta(days=int(d)) for d in day_offsets]

    # Site selection (weighted)
    site_names = [s["site"] for s in SITES_CONFIG]
    site_weights = np.array([s["weight"] for s in SITES_CONFIG], dtype=float)
    site_weights /= site_weights.sum()
    site_indices = rng.choice(len(SITES_CONFIG), size=n_records, p=site_weights)

    # Bug 3 fix: derive AU sites from config where currency == "AUD"
    # instead of substring matching on site name.
    au_site_names = {s["site"] for s in SITES_CONFIG if s["currency"] == "AUD"}

    rows = []
    po_counter = 4_500_000_001

    for i in range(n_records):
        site_cfg = SITES_CONFIG[site_indices[i]]
        date_obj = dates[i]
        cat_l1, cat_l2, cat_l3 = categories[i]
        supplier = selected_suppliers[i]

        quantity = round(rng.uniform(1.01, 999.99), 2)
        unit_price = round(rng.uniform(0.37, 50_000.0), 2)
        amount = round(quantity * unit_price, 2)
        amount_usd = round(amount * _fx_rate(site_cfg["currency"]), 2)

        # ABN: only for Australian sites (derived from config currency)
        supplier_abn = None
        if site_cfg["site"] in au_site_names and rng.random() < 0.15:
            supplier_abn = generate_abn(rng)

        row = {
            "record_id": f"BHP-PO-{i + 1:07d}",
            "date": date_obj.strftime("%Y-%m-%d"),
            "financial_year": fy_from_date(date_obj),
            "invoice_number": _random_invoice(rng),
            "purchase_order": float(po_counter + i // 5),
            "description": fake.sentence(nb_words=5),
            "quantity": quantity,
            "unit": rng.choice(UNITS),
            "unit_price": unit_price,
            "amount": amount,
            "currency": site_cfg["currency"],
            "amount_usd": amount_usd,
            "supplier_name": supplier["supplier_name"],
            "supplier_id": supplier["supplier_id"],
            "supplier_abn": supplier_abn,
            "cost_centre": f"CC-{rng.integers(1, 100):04d}" if rng.random() > 0.15 else None,
            "site": site_cfg["site"],
            "business_unit": site_cfg["business_unit"],
            "category_l1": cat_l1,
            "category_l2": cat_l2,
            "category_l3": cat_l3,
            "payment_terms": rng.choice(PAYMENT_TERMS) if rng.random() > 0.1 else None,
            "approver": fake.name() if rng.random() > 0.25 else None,
            "status": rng.choice(STATUSES),
        }
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Tiny helpers
# ---------------------------------------------------------------------------

_FX_RATES = {"AUD": 0.632, "USD": 1.0, "CLP": 0.00106}


def _fx_rate(currency: str) -> float:
    return _FX_RATES.get(currency, 1.0)


def _random_invoice(rng: np.random.Generator) -> str:
    chars = list(string.ascii_uppercase + string.digits)
    return "".join(rng.choice(chars) for _ in range(8))


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a clean BHP procurement dataset.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--n-records", type=int, default=51_500, help="Number of records to generate")
    parser.add_argument("--n-suppliers", type=int, default=500, help="Size of the synthetic supplier pool")
    parser.add_argument("-o", "--output", type=str, default="bhp_clean_50k.xlsx", help="Output file path (.xlsx or .csv)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(f"Generating {args.n_records} clean records (seed={args.seed}) …")

    df = generate_clean_dataset(args)

    if args.output.endswith(".csv"):
        df.to_csv(args.output, index=False)
    else:
        df.to_excel(args.output, index=False, engine="openpyxl")

    print(f"Wrote {len(df)} records to {args.output}")

    # Quick sanity summary
    print(f"\nSupplier distribution (top 10):")
    for name, count in df["supplier_name"].value_counts().head(10).items():
        print(f"  {name}: {count} ({count / len(df) * 100:.1f}%)")

    print(f"\nFY distribution:")
    for fy, count in df["financial_year"].value_counts().sort_index().items():
        print(f"  {fy}: {count}")

    print(f"\nABN coverage by site:")
    for site in sorted(df["site"].unique()):
        mask = df["site"] == site
        total = mask.sum()
        abn_count = df.loc[mask, "supplier_abn"].notna().sum()
        pct = abn_count / total * 100 if total else 0
        print(f"  {site}: {abn_count}/{total} ({pct:.1f}%)")


if __name__ == "__main__":
    main()
