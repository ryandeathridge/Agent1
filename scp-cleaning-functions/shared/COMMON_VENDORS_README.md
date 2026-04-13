# Common Vendors Dictionary

## Overview

The `common_vendors.json` file provides a baseline vendor dictionary containing ~200-300 well-known companies that appear in procurement data across all industries. This ensures every deployment gets out-of-the-box vendor matching without requiring extensive configuration.

## Purpose

- **Cross-industry coverage**: Works for any company, not just mining or a specific sector
- **Baseline matching**: Provides instant vendor resolution for common suppliers before falling back to company-specific vendor masters
- **Reduced configuration**: Companies don't need to manually add Microsoft, KPMG, DHL, etc. to their vendor masters
- **Abbreviation handling**: Automatically resolves common abbreviations (e.g., "PwC" → "PricewaterhouseCoopers")

## How It Works

The `VendorMatcher` class uses a three-stage matching approach:

1. **Common vendor abbreviations** - Checks if the input matches any abbreviation in `common_vendors.json` (instant resolution)
2. **Vendor dictionary** - Checks learned mappings from previous runs (instant resolution)
3. **Fuzzy matching** - Uses TF-IDF + rapidfuzz against combined vendor master (company-specific + common vendors)

### Priority Rules

- **Company-specific vendors take precedence**: If a company defines their own "Microsoft Corporation" in their vendor master, it will be used instead of the common vendor entry
- **Exact matches prioritized**: Company-specific exact matches override common vendor abbreviation matches
- **Fuzzy matching order**: Company-specific vendors are checked first, then common vendors

## Categories Included

The common vendors dictionary covers these industries:

- **Professional Services**: Big 4 (KPMG, Deloitte, PwC, EY), consulting (McKinsey, BCG, Bain, Accenture), law firms
- **IT & Software**: Microsoft, SAP, Oracle, Salesforce, ServiceNow, Adobe, Atlassian, IBM, AWS, Google Cloud
- **Logistics & Freight**: Toll Holdings, Linfox, DHL, FedEx, TNT, StarTrack, Brambles, UPS, Maersk
- **Industrial Gas & Chemicals**: BOC Gas, Air Liquide, Linde, BASF, Dow, DuPont, Orica
- **Office & Facilities**: Officeworks, Bunnings, Staples, CBRE, JLL, Cushman & Wakefield, ISS, Sodexo
- **Insurance**: QBE, Allianz, Zurich, AIG, Marsh, Aon, Willis Towers Watson, Chubb
- **Banks & Finance**: NAB, CBA, Westpac, ANZ, HSBC, Macquarie, Citibank, JPMorgan
- **Travel**: Qantas, Virgin Australia, Flight Centre, Hilton, Accor, Marriott, Hertz, Avis
- **Telecommunications**: Telstra, Optus, TPG, Vocus, NEC, Ericsson, Nokia, Vodafone
- **Utilities**: AGL, Origin, EnergyAustralia, Jemena, AusNet, Ausgrid
- **Engineering**: Worley, Jacobs, AECOM, Aurecon, GHD, Bechtel, Fluor, KBR, Siemens, ABB
- **Labour Hire**: Hays, Randstad, Adecco, Programmed, Chandler Macleod, Manpower
- **Safety & PPE**: Blackwoods, Total Tools, Honeywell, 3M, Ansell, MSA Safety
- **Vehicles**: Toyota, Hilux, Mitsubishi, Isuzu, Ford, Holden, Nissan, Mazda
- **Heavy Equipment**: Caterpillar, Komatsu, Hitachi, Volvo CE, Sandvik, Epiroc, Weir, Metso

## JSON Structure

Each vendor entry contains:

```json
{
  "canonical_name": "PricewaterhouseCoopers",
  "common_abbreviations": ["PwC", "PricewaterhouseCoopers", "PWC", "Price Waterhouse Coopers"],
  "typical_category_l1": "Professional Services"
}
```

- **canonical_name**: The standardized vendor name to use
- **common_abbreviations**: List of variations that should match this vendor (case-insensitive)
- **typical_category_l1**: The category this vendor typically falls under

## Usage Example

```python
from shared.vendor_matcher import VendorMatcher

# Empty company vendor master - will still match common vendors
vendor_master = []
matcher = VendorMatcher(vendor_master)

# Matches via abbreviation
matched, confidence, candidates = matcher.match('PwC')
# Returns: canonical_name='PricewaterhouseCoopers', confidence=1.0

# Matches via fuzzy matching
matched, confidence, candidates = matcher.match('Deloite')  # typo
# Returns: canonical_name='Deloitte', confidence=~0.95
```

## Maintenance

To add new common vendors:

1. Edit `scp-cleaning-functions/shared/common_vendors.json`
2. Add a new entry with canonical name, abbreviations, and category
3. Ensure abbreviations cover common variations (acronyms, full names, alternate spellings)
4. Run tests: `pytest tests/test_common_vendors.py -v`

## Special Cases

### Hilux as a Vendor

Yes, "Hilux" is included as a vendor! In procurement data, people sometimes write the vehicle model (Toyota Hilux) as the vendor name instead of "Toyota". The common vendors dictionary handles this.

### Australian Focus

The dictionary includes Australian-specific vendors (NAB, CBA, Telstra, Linfox, etc.) since the system was initially deployed in Australia. These can coexist with global vendors.

### Company Override Example

If your company has a specific agreement with "Microsoft Australia Pty Ltd" and defines it in their vendor master:

```python
vendor_master = [
    {'supplier_name': 'Microsoft Australia Pty Ltd', 'supplier_id': 'SUP-MS-001'}
]
matcher = VendorMatcher(vendor_master)

# Exact match uses company-specific vendor
matched, confidence, _ = matcher.match('Microsoft Australia Pty Ltd')
# Returns: supplier_id='SUP-MS-001' (not COMMON-xxx)

# Abbreviation "Microsoft" still matches common vendor
matched, confidence, _ = matcher.match('Microsoft')
# Returns: supplier_id='COMMON-009' (common vendor)
```

## Synthetic Supplier IDs

Common vendors are assigned synthetic IDs in the format `COMMON-001`, `COMMON-002`, etc. These IDs are stable across runs (based on position in the JSON file) and can be used for reporting and analytics.

## Testing

Comprehensive tests are available in `tests/test_common_vendors.py`:

```bash
cd scp-cleaning-functions
pytest tests/test_common_vendors.py -v
```

Tests cover:
- Loading common vendors
- Abbreviation matching (case-insensitive)
- Multiple abbreviations per vendor
- Category assignment
- Company vendor override behavior
- Cross-industry coverage
- Fuzzy matching with common vendors
- Integration with vendor dictionary
