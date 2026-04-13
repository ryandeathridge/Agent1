#!/usr/bin/env python3
"""
Local CLI Agent for Data Cleaning Pipeline
Runs entirely in-process — no LLM API calls required.
Strategy and classification are handled with built-in heuristics.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any, Tuple

import pandas as pd
import numpy as np

try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.prompt import Prompt, Confirm
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    print("Warning: 'rich' not installed. Install with: pip install rich")
    print("Continuing with basic terminal output...\n")

# Add shared modules to path
sys.path.insert(0, str(Path(__file__).parent / "scp-cleaning-functions"))

from shared.deduplicator import Deduplicator
from shared.encoding_fixer import fix_encoding
from shared.date_normaliser import normalise_date
from shared.amount_normaliser import normalise_amount
from shared.unit_standardiser import standardise_unit
from shared.vendor_matcher import VendorMatcher
from shared.triangulator import triangulate_dataframe
from shared.schema_validator import validate_dataframe
from shared.consistency_checker import check_consistency
import chardet


class CLIAgent:
    """Local CLI agent for data cleaning pipeline — no external LLM required."""

    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.console = Console() if RICH_AVAILABLE else None
        self.df: Optional[pd.DataFrame] = None
        self.profile: Optional[Dict] = None
        self.cleaning_strategy: Optional[Dict] = None
        self.cleaned_df: Optional[pd.DataFrame] = None
        self.flagged_df: Optional[pd.DataFrame] = None
        self.changes_log: List[Dict] = []
        self.validation_result: Optional[Dict] = None

    def _load_config(self) -> Dict:
        with open(self.config_path, 'r') as f:
            return json.load(f)

    def print(self, message: str, style: str = ""):
        if self.console:
            self.console.print(message, style=style)
        else:
            import re
            clean = re.sub(r'\[/?[a-z_ ]+\]', '', message)
            print(clean)

    def print_table(self, data: List[Dict], title: str = ""):
        if not data:
            return
        if self.console:
            table = Table(title=title)
            for key in data[0].keys():
                table.add_column(str(key))
            for row in data:
                table.add_row(*[str(v) for v in row.values()])
            self.console.print(table)
        else:
            if title:
                print(f"\n{title}")
                print("-" * 40)
            for row in data:
                print("  " + "  |  ".join(f"{k}: {v}" for k, v in row.items()))

    def confirm(self, message: str, default: bool = True) -> bool:
        if self.console:
            return Confirm.ask(message, default=default)
        else:
            default_str = "Y/n" if default else "y/N"
            response = input(f"{message} ({default_str}): ").lower().strip()
            if not response:
                return default
            return response in ['y', 'yes']

    # ========== STAGE 1: PROFILE ==========

    def stage1_profile(self, file_path: str) -> Dict:
        self.print("\n[bold cyan]═══ Stage 1: Profile Data ═══[/bold cyan]")
        self.print(f"Loading: {file_path}")

        ext = Path(file_path).suffix.lower()
        if ext == '.xlsx':
            self.df = pd.read_excel(file_path, engine='openpyxl')
        elif ext == '.csv':
            self.df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        self.print(f"Loaded {len(self.df):,} rows x {len(self.df.columns)} columns")

        columns = []
        encoding_issues = 0

        for col in self.df.columns:
            series = self.df[col]
            dtype_detected = self._detect_dtype(series)
            null_count = int(series.isna().sum())
            null_rate = float(null_count / len(series)) if len(series) > 0 else 0.0
            unique_count = int(series.nunique())
            cardinality = float(unique_count / len(series)) if len(series) > 0 else 0.0
            sample_values = series.dropna().head(5).astype(str).tolist()
            anomaly_flags = []
            distribution_summary = {}

            if dtype_detected == "string":
                for val in series.dropna().head(50).astype(str):
                    try:
                        detected = chardet.detect(val.encode('utf-8'))
                        enc = (detected.get('encoding') or '').lower()
                        if enc and enc not in ('utf-8', 'ascii', ''):
                            encoding_issues += 1
                            anomaly_flags.append("encoding_issues")
                            break
                    except Exception:
                        pass
                lengths = series.dropna().astype(str).str.len()
                if len(lengths) > 0:
                    distribution_summary = {
                        'min_length': int(lengths.min()),
                        'max_length': int(lengths.max()),
                        'mean_length': round(float(lengths.mean()), 1)
                    }
            elif dtype_detected == "numeric":
                numeric_series = pd.to_numeric(series, errors='coerce')
                if not numeric_series.isna().all():
                    distribution_summary = {
                        'min': float(numeric_series.min()),
                        'max': float(numeric_series.max()),
                        'mean': round(float(numeric_series.mean()), 2),
                        'std': round(float(numeric_series.std()), 2)
                    }
                if any(k in col.lower() for k in ('amount', 'price', 'cost')):
                    neg = int((numeric_series < 0).sum())
                    if neg > 0:
                        anomaly_flags.append(f"{neg}_negative_values")
            elif dtype_detected == "categorical":
                vc = series.value_counts().head(5)
                distribution_summary = {'top_values': {str(k): int(v) for k, v in vc.items()}}
            elif dtype_detected == "date":
                formats_seen = set()
                for val in series.dropna().head(100).astype(str):
                    if '/' in val:
                        formats_seen.add('slash')
                    elif '-' in val:
                        formats_seen.add('dash')
                if len(formats_seen) > 1:
                    anomaly_flags.append("mixed_date_formats")

            if null_rate > 0.3:
                anomaly_flags.append(f"high_nulls_{null_rate:.0%}")

            columns.append({
                'name': col,
                'dtype_detected': dtype_detected,
                'null_count': null_count,
                'null_rate': null_rate,
                'unique_count': unique_count,
                'cardinality': cardinality,
                'sample_values': sample_values,
                'anomaly_flags': anomaly_flags,
                'distribution_summary': distribution_summary
            })

        # Fast exact-dupe count via pandas; skip O(n²) near-dupe scan for large files
        key_fields = [f for f in ['date', 'amount', 'supplier_name', 'invoice_number']
                      if f in self.df.columns]
        if key_fields:
            duplicate_count = int(self.df.duplicated(subset=key_fields, keep='first').sum())
        else:
            duplicate_count = int(self.df.duplicated(keep='first').sum())
        near_duplicate_count = 0  # Skipped for performance on large files

        key_fields = ['record_id', 'date', 'amount', 'supplier_name']
        quality_scores = []
        for f in key_fields:
            if f in self.df.columns:
                quality_scores.append(1.0 - self.df[f].isna().sum() / len(self.df))
        overall_quality_score = float(np.mean(quality_scores)) if quality_scores else 0.0

        self.profile = {
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'columns': columns,
            'duplicate_count': duplicate_count,
            'near_duplicate_count': near_duplicate_count,
            'encoding_issues': encoding_issues,
            'overall_quality_score': overall_quality_score
        }

        summary_data = [
            {"Metric": "Total Rows", "Value": f"{len(self.df):,}"},
            {"Metric": "Total Columns", "Value": len(self.df.columns)},
            {"Metric": "Exact Duplicates", "Value": duplicate_count},
            {"Metric": "Near Duplicates", "Value": near_duplicate_count},
            {"Metric": "Encoding Issues", "Value": encoding_issues},
            {"Metric": "Quality Score", "Value": f"{overall_quality_score:.1%}"},
        ]
        self.print_table(summary_data, "Data Profile")

        flagged_cols = [c for c in columns if c['anomaly_flags']]
        if flagged_cols:
            self.print("\n[yellow]Flagged Columns:[/yellow]")
            for col in flagged_cols:
                self.print(f"  - {col['name']}: {', '.join(col['anomaly_flags'])}")

        return self.profile

    def _detect_dtype(self, series: pd.Series) -> str:
        non_null = series.dropna()
        if len(non_null) == 0:
            return "string"
        sample = non_null.head(100)
        total = len(sample)
        numeric_count = 0
        date_count = 0
        for val in sample:
            try:
                float(str(val).replace(',', '').replace('$', '').replace('AUD', '').strip())
                numeric_count += 1
            except (ValueError, TypeError):
                pass
            try:
                pd.to_datetime(val)
                date_count += 1
            except Exception:
                pass
        if numeric_count / total > 0.8:
            return "numeric"
        if date_count / total > 0.8 and numeric_count / total < 0.5:
            return "date"
        if series.nunique() / max(len(series), 1) < 0.05:
            return "categorical"
        return "string"

    # ========== STAGE 2: HEURISTIC STRATEGY ==========

    def stage2_generate_strategy(self) -> Dict:
        """Auto-generate cleaning strategy from profile — no LLM required."""
        self.print("\n[bold cyan]===  Stage 2: Generate Cleaning Strategy ====[/bold cyan]")

        deterministic_fields = []
        llm_fields = []

        for c in self.profile['columns']:
            name = c['name'].lower()
            dtype = c['dtype_detected']
            if dtype == 'date' or any(k in name for k in ('date', 'dt', '_at')):
                deterministic_fields.append(c['name'])
            elif dtype == 'numeric' or any(k in name for k in ('amount', 'price', 'cost', 'value', 'usd', 'aud')):
                deterministic_fields.append(c['name'])
            elif any(k in name for k in ('supplier', 'vendor', 'contractor')):
                deterministic_fields.append(c['name'])
            elif any(k in name for k in ('unit', 'uom')):
                deterministic_fields.append(c['name'])
            elif any(k in name for k in ('category',)) and c['null_rate'] > 0.05:
                llm_fields.append(c['name'])

        # Deduplicate
        seen = set()
        dedup = []
        for f in deterministic_fields:
            if f not in seen:
                seen.add(f)
                dedup.append(f)
        deterministic_fields = dedup

        high_null_fields = [c['name'] for c in self.profile['columns'] if c['null_rate'] > 0.3]

        date_format = "YYYY-MM-DD"
        for c in self.profile['columns']:
            if c['dtype_detected'] == 'date' and c['sample_values']:
                sample = c['sample_values'][0]
                if '/' in sample:
                    date_format = "DD/MM/YYYY -> YYYY-MM-DD"
                break

        strategy = {
            'deterministic_fields': deterministic_fields,
            'llm_fields': llm_fields,
            'vendor_normalization': any(
                'supplier' in c['name'].lower() or 'vendor' in c['name'].lower()
                for c in self.profile['columns']
            ),
            'deduplication': self.profile['duplicate_count'] > 0 or self.profile['near_duplicate_count'] > 0,
            'encoding_fix': self.profile['encoding_issues'] > 0,
            'high_null_fields': high_null_fields,
            'date_format': date_format,
            'notes': (
                f"Auto-generated. {len(deterministic_fields)} fields for rule-based cleaning. "
                f"{self.profile['duplicate_count']} exact dupes to remove."
            )
        }
        self.cleaning_strategy = strategy

        self.print(f"  Deterministic fields ({len(deterministic_fields)}): "
                   f"{', '.join(deterministic_fields[:8])}{'...' if len(deterministic_fields) > 8 else ''}")
        self.print(f"  Vendor normalization: {strategy['vendor_normalization']}")
        self.print(f"  Deduplication: {strategy['deduplication']}")
        self.print(f"  Date format: {strategy['date_format']}")
        if high_null_fields:
            self.print(f"  [yellow]High-null fields: {', '.join(high_null_fields)}[/yellow]")
        self.print(f"  Notes: {strategy['notes']}")

        if not self.confirm("\nProceed with this strategy?"):
            self.print("[yellow]Continuing with auto-strategy.[/yellow]")

        return strategy

    # ========== STAGE 3: DETERMINISTIC CLEANING ==========

    def stage3_clean_deterministic(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        self.print("\n[bold cyan]===  Stage 3: Deterministic Cleaning ====[/bold cyan]")

        df = self.df.copy()
        changes_log = []
        stats = {k: 0 for k in ('encoding_fixes', 'date_fixes', 'amount_fixes',
                                  'unit_fixes', 'vendor_matches', 'triangulations', 'dupes_removed')}

        # 1. Encoding fixes
        self.print("  [1/6] Fixing encoding...")
        for col in df.select_dtypes(include=['object']).columns:
            for idx in df.index:
                val = df.at[idx, col]
                if pd.notna(val):
                    fixed, was_modified = fix_encoding(str(val))
                    if was_modified:
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': col, 'original': str(val), 'new': fixed,
                            'method': 'encoding_fix', 'confidence': 1.0
                        })
                        df.at[idx, col] = fixed
                        stats['encoding_fixes'] += 1

        # 2. Date normalization
        date_cols = [c for c in df.columns
                     if any(k in c.lower() for k in ('date', 'dt', '_at'))]
        self.print(f"  [2/6] Normalizing dates in {len(date_cols)} column(s)...")
        for col in date_cols:
            if col not in df.columns:
                continue
            for idx in df.index:
                val = df.at[idx, col]
                if pd.notna(val):
                    normalised, confidence = normalise_date(val)
                    if normalised:
                        # Force YYYY-MM-DD string (strip time component if present)
                        try:
                            normalised_str = pd.to_datetime(normalised).strftime('%Y-%m-%d')
                        except Exception:
                            normalised_str = str(normalised)[:10]
                        if normalised_str != str(val):
                            changes_log.append({
                                'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                                'field': col, 'original': str(val), 'new': normalised_str,
                                'method': 'date_normalisation', 'confidence': confidence
                            })
                            df.at[idx, col] = normalised_str
                            stats['date_fixes'] += 1

        # 2b. Recalculate financial_year from normalized date (BHP FY = Jul–Jun)
        # Jul 2024 → FY 2025, Jan 2026 → FY 2026. Stored as plain integer.
        if 'financial_year' in df.columns and 'date' in df.columns:
            for idx in df.index:
                date_val = df.at[idx, 'date']
                if pd.notna(date_val):
                    try:
                        dt = pd.to_datetime(date_val)
                        fy = dt.year + 1 if dt.month >= 7 else dt.year
                        df.at[idx, 'financial_year'] = fy
                    except Exception:
                        pass

        # 3. Amount normalization
        amount_cols = [c for c in df.columns
                       if any(k in c.lower() for k in ('amount', 'price', 'cost', 'value', 'usd', 'aud'))]
        self.print(f"  [3/6] Normalizing amounts in {len(amount_cols)} column(s)...")
        for col in amount_cols:
            if col not in df.columns:
                continue
            for idx in df.index:
                val = df.at[idx, col]
                if pd.notna(val):
                    normalised, confidence = normalise_amount(val)
                    if normalised is not None and normalised != val:
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': col, 'original': str(val), 'new': str(normalised),
                            'method': 'amount_normalisation', 'confidence': confidence
                        })
                        df.at[idx, col] = normalised
                        stats['amount_fixes'] += 1

        # 4. Triangulation
        self.print("  [4/6] Triangulating amount/quantity/unit_price...")
        if all(c in df.columns for c in ['amount', 'quantity', 'unit_price']):
            triangulated_df, human_review_df, tri_changes = triangulate_dataframe(
                df, amount_col='amount', quantity_col='quantity', unit_price_col='unit_price'
            )
            # Reattach unfixable rows so they're not silently dropped from output
            if len(human_review_df) > 0:
                df = pd.concat([triangulated_df, human_review_df], ignore_index=True)
            else:
                df = triangulated_df
            changes_log.extend(tri_changes)
            stats['triangulations'] = len(tri_changes)
            self.print(f"    Derived {len(tri_changes)} values; {len(human_review_df)} flagged (kept in output)")
        else:
            self.print("    Skipped (no amount/quantity/unit_price columns)")

        # 5. Unit standardization
        unit_cols = [c for c in df.columns if any(k in c.lower() for k in ('unit', 'uom'))]
        self.print(f"  [5/6] Standardizing units in {len(unit_cols)} column(s)...")
        for col in unit_cols:
            if col not in df.columns:
                continue
            for idx in df.index:
                val = df.at[idx, col]
                if pd.notna(val):
                    standardised, confidence = standardise_unit(val)
                    if standardised and standardised != val:
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': col, 'original': str(val), 'new': standardised,
                            'method': 'unit_standardisation', 'confidence': confidence
                        })
                        df.at[idx, col] = standardised
                        stats['unit_fixes'] += 1

        # 6. Vendor matching
        vendor_cols = [c for c in df.columns if any(k in c.lower() for k in ('supplier', 'vendor'))]
        if vendor_cols and self.cleaning_strategy.get('vendor_normalization', True):
            self.print(f"  [6/6] Matching vendors in {len(vendor_cols)} column(s)...")
            vendor_master = self.config.get('top_20_suppliers', [])
            vendor_dict_path = Path(self.config_path).parent / 'vendor_dictionary.json'
            vendor_dictionary = {}
            if vendor_dict_path.exists():
                with open(vendor_dict_path, 'r') as f:
                    try:
                        vendor_dictionary = json.load(f)
                    except json.JSONDecodeError:
                        vendor_dictionary = {}

            matcher = VendorMatcher(vendor_master, vendor_dictionary)
            for col in vendor_cols:
                if col not in df.columns:
                    continue
                for idx in df.index:
                    val = df.at[idx, col]
                    if pd.notna(val):
                        matched_vendor, confidence, candidates = matcher.match(val)
                        if matched_vendor:
                            new_name = matched_vendor.get('supplier_name', val)
                            if str(new_name) != str(val):
                                changes_log.append({
                                    'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                                    'field': col, 'original': str(val), 'new': str(new_name),
                                    'method': 'vendor_matching', 'confidence': confidence
                                })
                                df.at[idx, col] = new_name
                                stats['vendor_matches'] += 1
                            if 'supplier_id' not in df.columns:
                                df['supplier_id'] = None
                            if pd.isna(df.at[idx, 'supplier_id']) if 'supplier_id' in df.columns else True:
                                df.at[idx, 'supplier_id'] = matched_vendor.get('supplier_id')
        else:
            self.print("  [6/6] Vendor matching skipped")

        # Deduplication — exact only (near-dupe scan is O(n²), unusable on large files)
        key_fields = [f for f in ['date', 'amount', 'supplier_name', 'invoice_number']
                      if f in df.columns]
        if key_fields:
            dupe_mask = df.duplicated(subset=key_fields, keep='first')
        else:
            dupe_mask = df.duplicated(keep='first')
        removed_df = df[dupe_mask].copy()
        df = df[~dupe_mask].copy()
        stats['dupes_removed'] = len(removed_df)

        # Flag low-confidence records
        confidence_threshold = 0.70
        confidence_cols = [c for c in df.columns if c.endswith('_confidence')]
        if confidence_cols:
            low_conf_mask = (
                df[confidence_cols]
                .apply(pd.to_numeric, errors='coerce')
                .lt(confidence_threshold)
                .any(axis=1)
            )
            cleaned_df = df[~low_conf_mask].copy()
            flagged_df = df[low_conf_mask].copy()
        else:
            cleaned_df = df.copy()
            flagged_df = pd.DataFrame(columns=df.columns)

        # Scrub "nan" string artefacts from category columns
        for col in ['category_l1', 'category_l2', 'category_l3']:
            for frame in [cleaned_df, flagged_df]:
                if col in frame.columns:
                    frame[col] = frame[col].astype(str).replace({'nan': '', 'NaN': '', 'None': ''})
                    frame.loc[frame[col] == '', col] = None

        self.cleaned_df = cleaned_df
        self.flagged_df = flagged_df
        self.changes_log = changes_log

        stats_data = [
            {"Metric": "Input rows", "Value": f"{len(self.df):,}"},
            {"Metric": "Cleaned rows", "Value": f"{len(cleaned_df):,}"},
            {"Metric": "Flagged (low confidence)", "Value": f"{len(flagged_df):,}"},
            {"Metric": "Duplicates removed", "Value": stats['dupes_removed']},
            {"Metric": "Encoding fixes", "Value": stats['encoding_fixes']},
            {"Metric": "Date normalizations", "Value": stats['date_fixes']},
            {"Metric": "Amount normalizations", "Value": stats['amount_fixes']},
            {"Metric": "Unit fixes", "Value": stats['unit_fixes']},
            {"Metric": "Vendor matches", "Value": stats['vendor_matches']},
            {"Metric": "Triangulated values", "Value": stats['triangulations']},
        ]
        self.print_table(stats_data, "Deterministic Cleaning Results")
        return cleaned_df, flagged_df

    # ========== STAGE 4: HEURISTIC CLASSIFICATION ==========

    def stage4_classify_flagged(self) -> pd.DataFrame:
        """Classify flagged records using keyword matching against taxonomy — no LLM required."""
        self.print("\n[bold cyan]===  Stage 4: Classify Flagged Records ====[/bold cyan]")

        if self.flagged_df is None or len(self.flagged_df) == 0:
            self.print("  No flagged records.")
            return pd.DataFrame()

        # Determine which rows need classification
        if 'category_l1' not in self.flagged_df.columns:
            self.flagged_df['category_l1'] = None
            self.flagged_df['category_l2'] = None
            self.flagged_df['category_l3'] = None
            self.flagged_df['category_confidence'] = 0.0

        needs_cat_mask = (
            self.flagged_df['category_l1'].isna() |
            (self.flagged_df['category_l1'].astype(str).str.strip() == '')
        )
        needs_cat_count = int(needs_cat_mask.sum())

        self.print(f"  Classifying {needs_cat_count:,} / {len(self.flagged_df):,} flagged records via keyword matching...")

        if needs_cat_count == 0:
            self.print("  All flagged records already have categories.")
            return self.flagged_df

        taxonomy = self.config.get('category_taxonomy', {})

        # Build keyword -> (l1, l2, l3) lookup
        keyword_map = []
        for l1, l2_dict in taxonomy.items():
            if isinstance(l2_dict, dict):
                for l2, l3_list in l2_dict.items():
                    if isinstance(l3_list, list):
                        for l3 in l3_list:
                            for kw in self._keywords_from(l3):
                                keyword_map.append((kw, l1, l2, l3))
                    for kw in self._keywords_from(l2):
                        keyword_map.append((kw, l1, l2, ''))
            for kw in self._keywords_from(l1):
                keyword_map.append((kw, l1, '', ''))
        # Longer keywords first (more specific wins)
        keyword_map.sort(key=lambda x: -len(x[0]))

        desc_col = next((c for c in self.flagged_df.columns
                         if any(k in c.lower() for k in ('desc', 'item', 'detail', 'material'))), None)
        vendor_col = next((c for c in self.flagged_df.columns
                           if any(k in c.lower() for k in ('supplier', 'vendor'))), None)

        first_l1 = next(iter(taxonomy), 'Uncategorised')
        classified = 0
        fallback = 0

        for idx in self.flagged_df[needs_cat_mask].index:
            parts = []
            if desc_col and pd.notna(self.flagged_df.at[idx, desc_col]):
                parts.append(str(self.flagged_df.at[idx, desc_col]))
            if vendor_col and pd.notna(self.flagged_df.at[idx, vendor_col]):
                parts.append(str(self.flagged_df.at[idx, vendor_col]))
            text = ' '.join(parts).lower()

            matched = False
            for kw, l1, l2, l3 in keyword_map:
                if kw in text:
                    self.flagged_df.at[idx, 'category_l1'] = l1
                    self.flagged_df.at[idx, 'category_l2'] = l2
                    self.flagged_df.at[idx, 'category_l3'] = l3
                    self.flagged_df.at[idx, 'category_confidence'] = 0.75
                    classified += 1
                    matched = True
                    break

            if not matched:
                self.flagged_df.at[idx, 'category_l1'] = first_l1
                self.flagged_df.at[idx, 'category_l2'] = ''
                self.flagged_df.at[idx, 'category_l3'] = ''
                self.flagged_df.at[idx, 'category_confidence'] = 0.3
                fallback += 1

        # Clean up any "nan" strings that may have appeared in category columns
        for col in ['category_l1', 'category_l2', 'category_l3']:
            if col in self.flagged_df.columns:
                self.flagged_df[col] = self.flagged_df[col].replace('nan', '').replace('NaN', '')

        self.print(f"  Keyword-matched: {classified:,}  |  Fallback assigned: {fallback:,}")
        return self.flagged_df

    def _keywords_from(self, name: str) -> List[str]:
        import re
        parts = re.split(r'[\s/&,()]+', name)
        keywords = [p.strip().lower() for p in parts if len(p.strip()) >= 4]
        if name.lower() not in keywords:
            keywords.append(name.lower())
        return keywords

    # ========== STAGE 5: VALIDATION ==========

    def stage5_validate(self) -> Dict:
        self.print("\n[bold cyan]===  Stage 5: Validation ====[/bold cyan]")

        frames = [df for df in [self.cleaned_df, self.flagged_df]
                  if df is not None and len(df) > 0]
        if not frames:
            self.print("  No data to validate.")
            return {}
        combined_df = pd.concat(frames, ignore_index=True)

        self.print(f"  Validating {len(combined_df):,} rows...")
        validation_result = validate_dataframe(combined_df, self.config)
        consistency_issues = check_consistency(combined_df, self.config)
        outliers = self._detect_outliers(combined_df)

        error_count = len(validation_result.schema_errors)
        c_errors = sum(1 for i in consistency_issues if i.get('severity') == 'error')
        c_warnings = sum(1 for i in consistency_issues if i.get('severity') == 'warning')

        self.validation_result = {
            'is_valid': validation_result.is_valid,
            'total_records': len(combined_df),
            'error_count': error_count + c_errors,
            'warning_count': c_warnings,
            'outlier_count': len(outliers),
            'schema_valid_pct': validation_result.stats.get('schema_valid_pct', 0)
        }

        validation_data = [
            {"Metric": "Total records", "Value": f"{len(combined_df):,}"},
            {"Metric": "Schema valid %", "Value": f"{self.validation_result['schema_valid_pct']:.1f}%"},
            {"Metric": "Errors", "Value": self.validation_result['error_count']},
            {"Metric": "Warnings", "Value": self.validation_result['warning_count']},
            {"Metric": "Outliers detected", "Value": self.validation_result['outlier_count']},
        ]
        self.print_table(validation_data, "Validation Results")

        if error_count > 0:
            self.print("\n  Sample errors:")
            for err in validation_result.schema_errors[:5]:
                self.print(f"    - [{err.get('record_id','')}] {err.get('field')}: "
                           f"{err.get('error_type')} - {err.get('details','')}")
            if not self.confirm(f"\nProceed despite {self.validation_result['error_count']} errors?"):
                raise ValueError("Stopped at user request after validation errors.")

        return self.validation_result

    def _detect_outliers(self, df: pd.DataFrame) -> List[Dict]:
        outliers = []
        if 'amount' not in df.columns:
            return outliers
        amounts = pd.to_numeric(df['amount'], errors='coerce').dropna()
        if len(amounts) > 20:
            mean, std = amounts.mean(), amounts.std()
            if std > 0:
                z = (amounts - mean).abs() / std
                for idx in z[z > 3].index[:20]:
                    outliers.append({
                        'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                        'field': 'amount',
                        'value': float(df.at[idx, 'amount']),
                        'z_score': round(float(z[idx]), 1)
                    })
        return outliers

    # ========== STAGE 6: OUTPUT ==========

    def stage6_output(self, input_path: str) -> str:
        self.print("\n[bold cyan]===  Stage 6: Write Output ====[/bold cyan]")

        frames = [df for df in [self.cleaned_df, self.flagged_df]
                  if df is not None and len(df) > 0]
        output_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        input_p = Path(input_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = input_p.parent / f"{input_p.stem}_cleaned_{ts}.xlsx"

        self.print(f"  Writing {len(output_df):,} rows to {output_path.name}...")
        output_df.to_excel(output_path, index=False, engine='openpyxl')
        self.print(f"  Saved: {output_path}")

        if self.changes_log:
            log_path = input_p.parent / f"{input_p.stem}_changes_{ts}.xlsx"
            pd.DataFrame(self.changes_log).to_excel(log_path, index=False, engine='openpyxl')
            self.print(f"  Changes log ({len(self.changes_log):,} changes): {log_path.name}")

        return str(output_path)

    # ========== STAGE 7: UPDATE LEARNING STATE ==========

    def stage7_update_learning_state(self) -> Dict:
        self.print("\n[bold cyan]===  Stage 7: Update Learning State ====[/bold cyan]")

        vendor_dict_path = Path(self.config_path).parent / 'vendor_dictionary.json'
        vendor_dict = {}
        if vendor_dict_path.exists():
            with open(vendor_dict_path, 'r') as f:
                try:
                    vendor_dict = json.load(f)
                except json.JSONDecodeError:
                    pass

        new_vendor_mappings = 0
        for change in self.changes_log:
            if change['method'] == 'vendor_matching' and change.get('confidence', 0) >= 0.8:
                if change['original'] not in vendor_dict:
                    vendor_dict[change['original']] = change['new']
                    new_vendor_mappings += 1

        if new_vendor_mappings > 0:
            with open(vendor_dict_path, 'w') as f:
                json.dump(vendor_dict, f, indent=2)

        examples_path = Path(self.config_path).parent / 'few_shot_examples.json'
        examples = []
        if examples_path.exists():
            with open(examples_path, 'r') as f:
                try:
                    examples = json.load(f)
                except json.JSONDecodeError:
                    pass

        existing_descs = {ex.get('description') for ex in examples}
        new_examples = 0

        if self.flagged_df is not None and len(self.flagged_df) > 0:
            desc_col = next((c for c in self.flagged_df.columns if 'desc' in c.lower()), None)
            for _, row in self.flagged_df.iterrows():
                if row.get('category_confidence', 0) >= 0.75:
                    desc = str(row.get(desc_col, '')) if desc_col else ''
                    if desc and desc not in existing_descs:
                        examples.append({
                            'description': desc,
                            'l1': row.get('category_l1', ''),
                            'l2': row.get('category_l2', ''),
                            'l3': row.get('category_l3', ''),
                            'verified': False
                        })
                        existing_descs.add(desc)
                        new_examples += 1
                        if new_examples >= 100:
                            break

        if new_examples > 0:
            with open(examples_path, 'w') as f:
                json.dump(examples, f, indent=2)

        result = {
            'new_vendor_mappings': new_vendor_mappings,
            'new_examples': new_examples,
            'total_vendor_dict': len(vendor_dict),
            'total_examples': len(examples)
        }

        learn_data = [
            {"Metric": "New vendor mappings", "Value": new_vendor_mappings},
            {"Metric": "New classification examples", "Value": new_examples},
            {"Metric": "Total vendor dict size", "Value": len(vendor_dict)},
            {"Metric": "Total examples", "Value": len(examples)},
        ]
        self.print_table(learn_data, "Learning State")
        return result

    # ========== MAIN PIPELINE ==========

    def run_pipeline(self, input_path: str) -> str:
        try:
            self.stage1_profile(input_path)
            self.stage2_generate_strategy()
            self.stage3_clean_deterministic()
            self.stage4_classify_flagged()
            self.stage5_validate()
            output_path = self.stage6_output(input_path)
            self.stage7_update_learning_state()
            self.print("\n[bold green]Pipeline complete![/bold green]")
            return output_path

        except KeyboardInterrupt:
            self.print("\n[yellow]Interrupted by user.[/yellow]")
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
            temp = Path(input_path).parent / f"temp_{ts}.xlsx"
            if self.cleaned_df is not None and len(self.cleaned_df) > 0:
                self.cleaned_df.to_excel(temp, index=False, engine='openpyxl')
                self.print(f"Progress saved: {temp}")
            sys.exit(0)

        except Exception as e:
            self.print(f"\n[bold red]Pipeline error: {e}[/bold red]")
            raise


def main():
    parser = argparse.ArgumentParser(
        description="SCP Local Data Cleaning Agent — no LLM API required"
    )
    parser.add_argument('input_file', nargs='?', help='Path to dirty data file (.xlsx or .csv)')
    parser.add_argument('--config', default='scp-cleaning-functions/config/bhp_config.json',
                        help='Path to config JSON file')
    args = parser.parse_args()

    input_path = args.input_file or input("Enter path to dirty data file: ").strip()
    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)

    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Error: Config not found: {config_path}")
        sys.exit(1)

    print(f"\n{'='*55}")
    print(f"  SCP Data Cleaning Agent")
    print(f"{'='*55}")
    print(f"  Input:  {input_path}")
    print(f"  Config: {config_path}")
    print(f"{'='*55}\n")

    agent = CLIAgent(config_path)
    output_path = agent.run_pipeline(input_path)

    print(f"\n{'='*55}")
    print(f"  Done!  Output: {output_path}")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    main()
