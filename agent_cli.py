#!/usr/bin/env python3
"""
Local CLI Agent for Data Cleaning Pipeline
Replaces the HTTP-based ChatGPT + Railway API approach.
Runs entirely in-process with no timeout constraints.
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
from shared.models import ProfileResult, ColumnProfile, ValidationResult
import chardet


class CLIAgent:
    """Local CLI agent for data cleaning pipeline."""
    
    def __init__(self, config_path: str, llm_provider: str, api_key: str):
        self.config_path = config_path
        self.llm_provider = llm_provider
        self.api_key = api_key
        self.config = self._load_config()
        self.console = Console() if RICH_AVAILABLE else None
        self.df: Optional[pd.DataFrame] = None
        self.profile: Optional[Dict] = None
        self.cleaning_strategy: Optional[Dict] = None
        self.cleaned_df: Optional[pd.DataFrame] = None
        self.flagged_df: Optional[pd.DataFrame] = None
        self.changes_log: List[Dict] = []
        self.validation_result: Optional[Dict] = None
        
        # Initialize LLM client
        if llm_provider == "openai":
            import openai
            self.llm_client = openai.OpenAI(api_key=api_key)
            self.llm_model = "gpt-4o"
        elif llm_provider == "anthropic":
            import anthropic
            self.llm_client = anthropic.Anthropic(api_key=api_key)
            self.llm_model = "claude-3-5-sonnet-20241022"
        else:
            raise ValueError(f"Unknown LLM provider: {llm_provider}")
    
    def _load_config(self) -> Dict:
        """Load configuration from JSON file."""
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def print(self, message: str, style: str = ""):
        """Print message with optional rich styling."""
        if self.console:
            self.console.print(message, style=style)
        else:
            print(message)
    
    def print_table(self, data: List[Dict], title: str = ""):
        """Print data as a table."""
        if not data:
            return
        
        if self.console:
            table = Table(title=title)
            if data:
                for key in data[0].keys():
                    table.add_column(str(key))
                for row in data:
                    table.add_row(*[str(v) for v in row.values()])
            self.console.print(table)
        else:
            if title:
                print(f"\n{title}")
            for row in data:
                print(row)
    
    def prompt(self, message: str, default: str = "") -> str:
        """Prompt user for input."""
        if self.console:
            return Prompt.ask(message, default=default)
        else:
            response = input(f"{message} [{default}]: ")
            return response if response else default
    
    def confirm(self, message: str) -> bool:
        """Ask user for yes/no confirmation."""
        if self.console:
            return Confirm.ask(message)
        else:
            response = input(f"{message} (y/n): ").lower()
            return response in ['y', 'yes']
    
    def call_llm(self, prompt: str, system_prompt: str = "", json_mode: bool = True, max_retries: int = 3) -> Dict:
        """Call LLM with retry logic and structured output."""
        for attempt in range(max_retries):
            try:
                if self.llm_provider == "openai":
                    messages = []
                    if system_prompt:
                        messages.append({"role": "system", "content": system_prompt})
                    messages.append({"role": "user", "content": prompt})
                    
                    kwargs = {
                        "model": self.llm_model,
                        "messages": messages,
                    }
                    if json_mode:
                        kwargs["response_format"] = {"type": "json_object"}
                    
                    response = self.llm_client.chat.completions.create(**kwargs)
                    content = response.choices[0].message.content
                    
                    if json_mode:
                        return json.loads(content)
                    else:
                        return {"response": content}
                
                elif self.llm_provider == "anthropic":
                    kwargs = {
                        "model": self.llm_model,
                        "max_tokens": 4096,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                    if system_prompt:
                        kwargs["system"] = system_prompt
                    
                    response = self.llm_client.messages.create(**kwargs)
                    content = response.content[0].text
                    
                    if json_mode:
                        # Extract JSON from markdown code blocks if present
                        if "```json" in content:
                            content = content.split("```json")[1].split("```")[0].strip()
                        elif "```" in content:
                            content = content.split("```")[1].split("```")[0].strip()
                        return json.loads(content)
                    else:
                        return {"response": content}
            
            except Exception as e:
                if attempt < max_retries - 1:
                    self.print(f"[yellow]LLM call failed (attempt {attempt + 1}/{max_retries}): {e}[/yellow]")
                else:
                    self.print(f"[red]LLM call failed after {max_retries} attempts: {e}[/red]", style="bold red")
                    raise
        
        return {}
    
    # ========== STAGE 1: PROFILE ==========
    
    def stage1_profile(self, file_path: str) -> Dict:
        """Stage 1: Load and profile the data."""
        self.print("\n[bold cyan]═══ Stage 1: Profile Data ═══[/bold cyan]")
        
        # Load file
        self.print(f"Loading file: {file_path}")
        if file_path.endswith('.xlsx'):
            self.df = pd.read_excel(file_path, engine='openpyxl')
        elif file_path.endswith('.csv'):
            self.df = pd.read_csv(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path}")
        
        self.print(f"Loaded {len(self.df)} rows, {len(self.df.columns)} columns")
        
        # Profile columns
        columns = []
        encoding_issues = 0
        
        for col in self.df.columns:
            series = self.df[col]
            
            dtype_detected = self._detect_dtype(series)
            null_count = int(series.isna().sum())
            null_rate = float(null_count / len(series))
            unique_count = int(series.nunique())
            cardinality = float(unique_count / len(series)) if len(series) > 0 else 0.0
            
            sample_values = series.dropna().head(5).astype(str).tolist()
            
            anomaly_flags = []
            distribution_summary = {}
            
            if dtype_detected == "string":
                for val in series.dropna().head(100):
                    if isinstance(val, str):
                        try:
                            val_bytes = val.encode('utf-8')
                            detected = chardet.detect(val_bytes)
                            if detected['encoding'] and detected['encoding'].lower() not in ['utf-8', 'ascii']:
                                encoding_issues += 1
                                anomaly_flags.append("encoding issues detected")
                                break
                        except Exception:
                            pass
                
                lengths = series.dropna().astype(str).str.len()
                distribution_summary = {
                    'min_length': int(lengths.min()) if len(lengths) > 0 else 0,
                    'max_length': int(lengths.max()) if len(lengths) > 0 else 0,
                    'mean_length': float(lengths.mean()) if len(lengths) > 0 else 0.0
                }
            
            elif dtype_detected == "numeric":
                numeric_series = pd.to_numeric(series, errors='coerce')
                distribution_summary = {
                    'min': float(numeric_series.min()) if not numeric_series.isna().all() else None,
                    'max': float(numeric_series.max()) if not numeric_series.isna().all() else None,
                    'mean': float(numeric_series.mean()) if not numeric_series.isna().all() else None,
                    'std': float(numeric_series.std()) if not numeric_series.isna().all() else None
                }
            
            elif dtype_detected == "categorical":
                value_counts = series.value_counts().head(10)
                distribution_summary = {
                    'top_values': {str(k): int(v) for k, v in value_counts.items()}
                }
            
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
        
        # Find duplicates
        deduplicator = Deduplicator()
        exact_groups, near_groups = deduplicator.find_duplicates(self.df)
        
        duplicate_count = sum(len(group) - 1 for group in exact_groups)
        near_duplicate_count = sum(len(group) - 1 for group in near_groups)
        
        # Calculate quality score
        required_fields = ['record_id', 'date', 'amount', 'supplier_name']
        quality_scores = []
        
        for field in required_fields:
            if field in self.df.columns:
                null_rate = self.df[field].isna().sum() / len(self.df)
                quality_scores.append(1.0 - null_rate)
        
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
        
        # Print summary
        self.print("\n[bold]Profile Summary:[/bold]")
        summary_data = [
            {"Metric": "Total Rows", "Value": self.profile['total_rows']},
            {"Metric": "Total Columns", "Value": self.profile['total_columns']},
            {"Metric": "Duplicate Count", "Value": duplicate_count},
            {"Metric": "Near Duplicate Count", "Value": near_duplicate_count},
            {"Metric": "Encoding Issues", "Value": encoding_issues},
            {"Metric": "Quality Score", "Value": f"{overall_quality_score:.2%}"},
        ]
        self.print_table(summary_data, "Data Profile")
        
        # Print flagged columns
        flagged_cols = [c for c in columns if c['anomaly_flags'] or c['null_rate'] > 0.3]
        if flagged_cols:
            self.print("\n[yellow]Flagged Columns:[/yellow]")
            for col in flagged_cols:
                self.print(f"  • {col['name']}: null_rate={col['null_rate']:.1%}, flags={col['anomaly_flags']}")
        
        return self.profile
    
    def _detect_dtype(self, series: pd.Series) -> str:
        """Detect the data type of a series."""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return "string"
        
        numeric_count = 0
        date_count = 0
        
        for val in non_null.head(100):
            try:
                float(val)
                numeric_count += 1
            except (ValueError, TypeError):
                pass
            
            try:
                pd.to_datetime(val)
                date_count += 1
            except Exception:
                pass
        
        sample_size = min(100, len(non_null))
        
        if numeric_count / sample_size > 0.8:
            return "numeric"
        elif date_count / sample_size > 0.8:
            return "date"
        elif series.nunique() / len(series) < 0.05:
            return "categorical"
        elif numeric_count > 0 and numeric_count / sample_size > 0.3:
            return "mixed"
        else:
            return "string"
    
    # ========== STAGE 2: LLM STRATEGY ==========
    
    def stage2_llm_strategy(self) -> Dict:
        """Stage 2: Generate cleaning strategy using LLM."""
        self.print("\n[bold cyan]═══ Stage 2: LLM Cleaning Strategy ═══[/bold cyan]")
        
        # Prepare profile summary for LLM
        profile_summary = {
            'total_rows': self.profile['total_rows'],
            'total_columns': self.profile['total_columns'],
            'quality_score': self.profile['overall_quality_score'],
            'columns': [
                {
                    'name': c['name'],
                    'type': c['dtype_detected'],
                    'null_rate': c['null_rate'],
                    'unique_count': c['unique_count'],
                    'anomaly_flags': c['anomaly_flags']
                }
                for c in self.profile['columns']
            ]
        }
        
        system_prompt = """You are a data cleaning strategy expert. Given a profile of procurement data, 
decide which columns need cleaning and what approach to use. Return a JSON object with:
- deterministic_fields: list of column names that can be cleaned with rules (dates, amounts, vendor names, etc.)
- llm_fields: list of column names that need LLM reasoning (complex categorization, ambiguous text)
- vendor_normalization: boolean, whether to apply vendor name matching
- date_format: target date format (e.g., "YYYY-MM-DD")
- estimated_llm_records: estimated number of records that will need LLM classification
- notes: brief explanation of the strategy"""
        
        prompt = f"""Analyze this data profile and create a cleaning strategy:

{json.dumps(profile_summary, indent=2)}

Available deterministic cleaning methods:
- Encoding fixes (for text corruption)
- Date normalization (various formats to ISO)
- Amount normalization (currency symbols, thousands separators)
- Unit standardization (EA, KG, L, etc.)
- Vendor name matching (fuzzy matching to master list)
- Deduplication (exact and near-duplicate detection)
- Triangulation (derive missing values from amount = quantity × unit_price)

Return your strategy as JSON."""
        
        self.print("Calling LLM to generate cleaning strategy...")
        strategy = self.call_llm(prompt, system_prompt, json_mode=True)
        
        self.cleaning_strategy = strategy
        
        # Print strategy
        self.print("\n[bold]Cleaning Strategy:[/bold]")
        self.print(f"Deterministic fields: {', '.join(strategy.get('deterministic_fields', []))}")
        self.print(f"LLM fields: {', '.join(strategy.get('llm_fields', []))}")
        self.print(f"Vendor normalization: {strategy.get('vendor_normalization', False)}")
        self.print(f"Date format: {strategy.get('date_format', 'YYYY-MM-DD')}")
        self.print(f"Estimated LLM records: {strategy.get('estimated_llm_records', 0)}")
        self.print(f"Notes: {strategy.get('notes', '')}")
        
        # Ask user to confirm or edit
        if self.confirm("\nProceed with this strategy?"):
            return strategy
        else:
            self.print("Strategy rejected. Please edit the strategy manually.")
            # In a real implementation, allow editing
            return strategy
    
    # ========== STAGE 3: DETERMINISTIC CLEANING ==========
    
    def stage3_clean_deterministic(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Stage 3: Apply deterministic cleaning rules."""
        self.print("\n[bold cyan]═══ Stage 3: Deterministic Cleaning ═══[/bold cyan]")
        
        df = self.df.copy()
        changes_log = []
        fields_modified = {}
        
        # 1. Encoding fixes
        self.print("Fixing encoding issues...")
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, col]):
                    original = df.at[idx, col]
                    fixed, was_modified = fix_encoding(str(original))
                    if was_modified:
                        df.at[idx, col] = fixed
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': col,
                            'original': str(original),
                            'new': fixed,
                            'method': 'encoding_fix',
                            'confidence': 1.0,
                            'agent': 'encoding_fixer'
                        })
            if modified_count > 0:
                fields_modified[col] = modified_count
                self.print(f"  Fixed {modified_count} encoding issues in {col}")
        
        # 2. Date normalization
        if 'date' in df.columns:
            self.print("Normalizing dates...")
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'date']):
                    original = df.at[idx, 'date']
                    normalised, confidence = normalise_date(original)
                    if normalised and str(normalised) != str(original):
                        df.at[idx, 'date'] = normalised
                        df.at[idx, 'date_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'date',
                            'original': str(original),
                            'new': normalised,
                            'method': 'date_normalisation',
                            'confidence': confidence,
                            'agent': 'date_normaliser'
                        })
            if modified_count > 0:
                fields_modified['date'] = modified_count
                self.print(f"  Normalized {modified_count} dates")
        
        # 3. Amount normalization
        for amount_col in ['amount', 'unit_price', 'amount_usd']:
            if amount_col in df.columns:
                self.print(f"Normalizing {amount_col}...")
                modified_count = 0
                for idx in df.index:
                    if pd.notna(df.at[idx, amount_col]):
                        original = df.at[idx, amount_col]
                        normalised, confidence = normalise_amount(original)
                        if normalised is not None and normalised != original:
                            df.at[idx, amount_col] = normalised
                            df.at[idx, f'{amount_col}_confidence'] = confidence
                            modified_count += 1
                            changes_log.append({
                                'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                                'field': amount_col,
                                'original': str(original),
                                'new': str(normalised),
                                'method': 'amount_normalisation',
                                'confidence': confidence,
                                'agent': 'amount_normaliser'
                            })
                if modified_count > 0:
                    fields_modified[amount_col] = modified_count
                    self.print(f"  Normalized {modified_count} amounts")
        
        # 4. Triangulation
        self.print("Triangulating amount/quantity/unit_price...")
        triangulated_df, human_review_df, triangulation_changes = triangulate_dataframe(
            df,
            amount_col='amount',
            quantity_col='quantity',
            unit_price_col='unit_price'
        )
        df = triangulated_df
        changes_log.extend(triangulation_changes)
        if len(triangulation_changes) > 0:
            self.print(f"  Derived {len(triangulation_changes)} values via triangulation")
        if len(human_review_df) > 0:
            self.print(f"  Flagged {len(human_review_df)} records for review (2+ missing fields)")
        
        # 5. Unit standardization
        if 'unit' in df.columns:
            self.print("Standardizing units...")
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'unit']):
                    original = df.at[idx, 'unit']
                    standardised, confidence = standardise_unit(original)
                    if standardised and standardised != original:
                        df.at[idx, 'unit'] = standardised
                        df.at[idx, 'unit_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'unit',
                            'original': str(original),
                            'new': standardised,
                            'method': 'unit_standardisation',
                            'confidence': confidence,
                            'agent': 'unit_standardiser'
                        })
            if modified_count > 0:
                fields_modified['unit'] = modified_count
                self.print(f"  Standardized {modified_count} units")
        
        # 6. Vendor matching
        if 'supplier_name' in df.columns and self.cleaning_strategy.get('vendor_normalization', True):
            self.print("Matching vendor names...")
            vendor_master = self.config.get('top_20_suppliers', [])
            
            # Load vendor dictionary
            vendor_dict_path = Path(self.config_path).parent / 'vendor_dictionary.json'
            vendor_dictionary = {}
            if vendor_dict_path.exists():
                with open(vendor_dict_path, 'r') as f:
                    vendor_dictionary = json.load(f)
            
            matcher = VendorMatcher(vendor_master, vendor_dictionary)
            
            modified_count = 0
            for idx in df.index:
                if pd.notna(df.at[idx, 'supplier_name']):
                    original = df.at[idx, 'supplier_name']
                    matched_vendor, confidence, candidates = matcher.match(original)
                    
                    if matched_vendor:
                        df.at[idx, 'supplier_name'] = matched_vendor.get('supplier_name')
                        df.at[idx, 'supplier_id'] = matched_vendor.get('supplier_id')
                        df.at[idx, 'supplier_confidence'] = confidence
                        modified_count += 1
                        changes_log.append({
                            'record_id': df.at[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}',
                            'field': 'supplier_name',
                            'original': str(original),
                            'new': matched_vendor.get('supplier_name'),
                            'method': 'vendor_matching',
                            'confidence': confidence,
                            'agent': 'vendor_matcher'
                        })
                    else:
                        df.at[idx, 'supplier_confidence'] = confidence
                        df.at[idx, 'supplier_candidates'] = json.dumps(candidates[:3])
            
            if modified_count > 0:
                fields_modified['supplier_name'] = modified_count
                self.print(f"  Matched {modified_count} vendor names")
        
        # 7. Deduplication
        self.print("Removing duplicates...")
        deduplicator = Deduplicator()
        deduped_df, removed_df = deduplicator.deduplicate(df)
        df = deduped_df
        self.print(f"  Removed {len(removed_df)} duplicates")
        
        # 8. Separate high-confidence vs flagged records
        confidence_threshold = 0.70
        confidence_cols = [col for col in df.columns if col.endswith('_confidence')]
        
        if confidence_cols:
            mask = (df[confidence_cols] >= confidence_threshold).all(axis=1)
            cleaned_df = df[mask].copy()
            flagged_df = df[~mask].copy()
        else:
            cleaned_df = df.copy()
            flagged_df = pd.DataFrame()
        
        self.cleaned_df = cleaned_df
        self.flagged_df = flagged_df
        self.changes_log = changes_log
        
        # Print stats
        self.print("\n[bold]Cleaning Stats:[/bold]")
        stats_data = [
            {"Metric": "Input Rows", "Value": len(self.df)},
            {"Metric": "Cleaned Rows", "Value": len(cleaned_df)},
            {"Metric": "Flagged Rows", "Value": len(flagged_df)},
            {"Metric": "Duplicates Removed", "Value": len(removed_df)},
            {"Metric": "Fields Modified", "Value": sum(fields_modified.values())},
        ]
        self.print_table(stats_data, "Deterministic Cleaning Results")
        
        return cleaned_df, flagged_df
    
    # ========== STAGE 4: LLM CLASSIFICATION ==========
    
    def stage4_llm_classify_flagged(self) -> pd.DataFrame:
        """Stage 4: Classify flagged records using LLM."""
        self.print("\n[bold cyan]═══ Stage 4: LLM Classification of Flagged Records ═══[/bold cyan]")
        
        if len(self.flagged_df) == 0:
            self.print("No flagged records to classify.")
            return self.flagged_df
        
        # Filter records that need LLM classification (missing category)
        needs_classification = self.flagged_df[
            self.flagged_df['category_l1'].isna() | 
            (self.flagged_df['category_l1'] == '')
        ].copy()
        
        if len(needs_classification) == 0:
            self.print("No records need category classification.")
            return self.flagged_df
        
        self.print(f"Classifying {len(needs_classification)} records...")
        
        # Prepare taxonomy for LLM
        taxonomy_str = json.dumps(self.config['category_taxonomy'], indent=2)
        
        # Load few-shot examples
        examples_path = Path(self.config_path).parent / 'few_shot_examples.json'
        examples = []
        if examples_path.exists():
            with open(examples_path, 'r') as f:
                examples = json.load(f)
        
        examples_str = json.dumps(examples[:10], indent=2) if examples else "[]"
        
        system_prompt = f"""You are a procurement data classification expert. Given a record with description and supplier name,
classify it into the appropriate category from this taxonomy:

{taxonomy_str}

Few-shot examples:
{examples_str}

Return JSON with an array of classifications, each containing:
- record_id: the record ID
- category_l1: level 1 category
- category_l2: level 2 category
- category_l3: level 3 category
- confidence: confidence score (0-1)"""
        
        # Process in batches of 50
        batch_size = 50
        num_batches = (len(needs_classification) + batch_size - 1) // batch_size
        
        all_classifications = []
        
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, len(needs_classification))
            batch = needs_classification.iloc[start_idx:end_idx]
            
            self.print(f"Classifying batch {batch_idx + 1}/{num_batches}...")
            
            # Prepare batch records
            batch_records = []
            for _, row in batch.iterrows():
                batch_records.append({
                    'record_id': row.get('record_id', ''),
                    'description': row.get('description', ''),
                    'supplier_name': row.get('supplier_name', ''),
                    'amount': float(row.get('amount', 0)) if pd.notna(row.get('amount')) else None
                })
            
            prompt = f"""Classify these {len(batch_records)} procurement records:

{json.dumps(batch_records, indent=2)}

Return JSON with key "classifications" containing an array of classification results."""
            
            try:
                result = self.call_llm(prompt, system_prompt, json_mode=True)
                classifications = result.get('classifications', [])
                all_classifications.extend(classifications)
            except Exception as e:
                self.print(f"[red]Error classifying batch {batch_idx + 1}: {e}[/red]")
                continue
        
        # Apply classifications to flagged_df
        self.print(f"\nApplying {len(all_classifications)} classifications...")
        for classification in all_classifications:
            record_id = classification.get('record_id')
            mask = self.flagged_df['record_id'] == record_id
            if mask.any():
                self.flagged_df.loc[mask, 'category_l1'] = classification.get('category_l1')
                self.flagged_df.loc[mask, 'category_l2'] = classification.get('category_l2')
                self.flagged_df.loc[mask, 'category_l3'] = classification.get('category_l3')
                self.flagged_df.loc[mask, 'category_confidence'] = classification.get('confidence', 0.5)
        
        # Show summary
        classified_count = len(all_classifications)
        self.print(f"[green]Successfully classified {classified_count} records[/green]")
        
        # Ask user to review
        if self.confirm("\nReview flagged classifications?"):
            # Show sample
            sample = self.flagged_df[['record_id', 'description', 'category_l1', 'category_l2', 'category_l3']].head(10)
            self.print("\nSample classifications:")
            self.print(str(sample))
            self.print("\n(In a full implementation, allow editing here)")
        
        return self.flagged_df
    
    # ========== STAGE 5: VALIDATION ==========
    
    def stage5_validate(self) -> Dict:
        """Stage 5: Validate the cleaned data."""
        self.print("\n[bold cyan]═══ Stage 5: Validation ═══[/bold cyan]")
        
        # Combine cleaned and flagged dataframes
        combined_df = pd.concat([self.cleaned_df, self.flagged_df], ignore_index=True)
        
        # Validate schema
        self.print("Validating schema...")
        validation_result = validate_dataframe(combined_df, self.config)
        
        # Check consistency
        self.print("Checking consistency...")
        consistency_issues = check_consistency(combined_df, self.config)
        
        # Detect outliers
        self.print("Detecting outliers...")
        outliers = self._detect_outliers(combined_df)
        
        error_count = len(validation_result.schema_errors)
        warning_count = sum(1 for issue in consistency_issues if issue.get('severity') == 'warning')
        error_count += sum(1 for issue in consistency_issues if issue.get('severity') == 'error')
        
        self.validation_result = {
            'is_valid': validation_result.is_valid and error_count == 0,
            'total_records': len(combined_df),
            'valid_records': validation_result.stats.get('schema_valid_pct', 0) * len(combined_df) / 100,
            'error_count': error_count,
            'warning_count': warning_count,
            'outlier_count': len(outliers)
        }
        
        # Print validation results
        self.print("\n[bold]Validation Results:[/bold]")
        validation_data = [
            {"Metric": "Total Records", "Value": self.validation_result['total_records']},
            {"Metric": "Valid Records", "Value": int(self.validation_result['valid_records'])},
            {"Metric": "Errors", "Value": error_count},
            {"Metric": "Warnings", "Value": warning_count},
            {"Metric": "Outliers", "Value": len(outliers)},
        ]
        self.print_table(validation_data, "Validation Summary")
        
        if error_count > 0:
            self.print(f"\n[yellow]Found {error_count} validation errors[/yellow]")
            # Show sample errors
            if validation_result.schema_errors:
                self.print("\nSample schema errors:")
                for error in validation_result.schema_errors[:5]:
                    self.print(f"  • {error}")
            
            if not self.confirm("\nProceed despite validation errors?"):
                raise ValueError("Validation failed and user chose not to proceed")
        
        return self.validation_result
    
    def _detect_outliers(self, df: pd.DataFrame) -> List[Dict]:
        """Detect outliers in the data."""
        outliers = []
        
        # Amount outliers by category
        if 'amount' in df.columns and 'category_l1' in df.columns:
            for category in df['category_l1'].unique():
                if pd.isna(category):
                    continue
                
                category_df = df[df['category_l1'] == category]
                amounts = pd.to_numeric(category_df['amount'], errors='coerce').dropna()
                
                if len(amounts) > 10:
                    mean = amounts.mean()
                    std = amounts.std()
                    
                    if std > 0:
                        z_scores = np.abs((amounts - mean) / std)
                        outlier_mask = z_scores > 3
                        outlier_indices = amounts[outlier_mask].index
                        
                        for idx in outlier_indices[:10]:  # Limit to 10 per category
                            record_id = df.loc[idx, 'record_id'] if 'record_id' in df.columns else f'row_{idx}'
                            amount = df.loc[idx, 'amount']
                            outliers.append({
                                'record_id': record_id,
                                'field': 'amount',
                                'value': float(amount),
                                'reason': f'z-score {z_scores.loc[idx]:.1f} within {category}'
                            })
        
        return outliers
    
    # ========== STAGE 6: OUTPUT ==========
    
    def stage6_output(self, input_path: str) -> str:
        """Stage 6: Write cleaned data to output file."""
        self.print("\n[bold cyan]═══ Stage 6: Output ═══[/bold cyan]")
        
        # Combine cleaned and flagged dataframes
        output_df = pd.concat([self.cleaned_df, self.flagged_df], ignore_index=True)
        
        # Generate output filename
        input_path_obj = Path(input_path)
        timestamp = datetime.now().strftime("%Y%m%d")
        output_filename = f"{input_path_obj.stem}_cleaned_{timestamp}.xlsx"
        output_path = input_path_obj.parent / output_filename
        
        # Write to Excel
        self.print(f"Writing {len(output_df)} rows to {output_path}...")
        output_df.to_excel(output_path, index=False, engine='openpyxl')
        
        self.print(f"[green]✓ Saved to: {output_path}[/green]")
        
        return str(output_path)
    
    # ========== STAGE 7: UPDATE LEARNING STATE ==========
    
    def stage7_update_learning_state(self) -> Dict:
        """Stage 7: Update learning state with new mappings and examples."""
        self.print("\n[bold cyan]═══ Stage 7: Update Learning State ═══[/bold cyan]")
        
        # Extract new vendor mappings from changes log
        vendor_mappings = []
        for change in self.changes_log:
            if change['method'] == 'vendor_matching' and change['confidence'] >= 0.8:
                vendor_mappings.append({
                    'dirty': change['original'],
                    'canonical': change['new']
                })
        
        # Extract classification examples from flagged records
        classification_examples = []
        if self.flagged_df is not None and len(self.flagged_df) > 0:
            for _, row in self.flagged_df.iterrows():
                if pd.notna(row.get('category_l1')) and row.get('category_confidence', 0) >= 0.8:
                    classification_examples.append({
                        'description': row.get('description', ''),
                        'l1': row.get('category_l1'),
                        'l2': row.get('category_l2'),
                        'l3': row.get('category_l3'),
                        'verified': True
                    })
        
        # Update vendor dictionary
        vendor_dict_path = Path(self.config_path).parent / 'vendor_dictionary.json'
        vendor_dict = {}
        if vendor_dict_path.exists():
            with open(vendor_dict_path, 'r') as f:
                vendor_dict = json.load(f)
        
        new_vendor_mappings = 0
        for mapping in vendor_mappings:
            if mapping['dirty'] not in vendor_dict:
                vendor_dict[mapping['dirty']] = mapping['canonical']
                new_vendor_mappings += 1
        
        if new_vendor_mappings > 0:
            with open(vendor_dict_path, 'w') as f:
                json.dump(vendor_dict, f, indent=2)
            self.print(f"Added {new_vendor_mappings} new vendor mappings")
        
        # Update few-shot examples
        examples_path = Path(self.config_path).parent / 'few_shot_examples.json'
        examples = []
        if examples_path.exists():
            with open(examples_path, 'r') as f:
                examples = json.load(f)
        
        existing_descriptions = {ex.get('description') for ex in examples}
        new_examples = 0
        
        for example in classification_examples[:50]:  # Limit to 50 new examples
            if example['description'] not in existing_descriptions:
                examples.append(example)
                new_examples += 1
                existing_descriptions.add(example['description'])
        
        if new_examples > 0:
            with open(examples_path, 'w') as f:
                json.dump(examples, f, indent=2)
            self.print(f"Added {new_examples} new classification examples")
        
        result = {
            'new_vendor_mappings': new_vendor_mappings,
            'new_examples': new_examples,
            'total_vendor_dictionary_size': len(vendor_dict),
            'total_examples_size': len(examples)
        }
        
        self.print("\n[bold]Learning State Updated:[/bold]")
        learning_data = [
            {"Metric": "New Vendor Mappings", "Value": new_vendor_mappings},
            {"Metric": "New Classification Examples", "Value": new_examples},
            {"Metric": "Total Vendor Dictionary Size", "Value": len(vendor_dict)},
            {"Metric": "Total Examples Size", "Value": len(examples)},
        ]
        self.print_table(learning_data, "Learning State")
        
        return result
    
    # ========== MAIN PIPELINE ==========
    
    def run_pipeline(self, input_path: str) -> str:
        """Run the complete cleaning pipeline."""
        try:
            # Stage 1: Profile
            self.stage1_profile(input_path)
            
            # Stage 2: LLM Strategy
            self.stage2_llm_strategy()
            
            # Stage 3: Deterministic Cleaning
            self.stage3_clean_deterministic()
            
            # Stage 4: LLM Classification
            self.stage4_llm_classify_flagged()
            
            # Stage 5: Validation
            self.stage5_validate()
            
            # Stage 6: Output
            output_path = self.stage6_output(input_path)
            
            # Stage 7: Update Learning State
            self.stage7_update_learning_state()
            
            self.print("\n[bold green]✓ Pipeline completed successfully![/bold green]")
            return output_path
        
        except KeyboardInterrupt:
            self.print("\n[yellow]Pipeline interrupted by user[/yellow]")
            # Save progress to temp file
            temp_path = Path(input_path).parent / f"temp_progress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            if self.cleaned_df is not None:
                self.cleaned_df.to_excel(temp_path, index=False)
                self.print(f"Progress saved to: {temp_path}")
            raise
        
        except Exception as e:
            self.print(f"\n[bold red]Pipeline failed: {e}[/bold red]")
            if self.confirm("Retry from this stage?"):
                # In a real implementation, allow retrying from failed stage
                pass
            raise


def main():
    """Main entry point for CLI agent."""
    parser = argparse.ArgumentParser(description="Local CLI Agent for Data Cleaning Pipeline")
    parser.add_argument('input_file', nargs='?', help='Path to dirty data file (.xlsx or .csv)')
    parser.add_argument('--config', default='scp-cleaning-functions/config/bhp_config.json',
                        help='Path to config JSON file')
    parser.add_argument('--provider', choices=['openai', 'anthropic'], help='LLM provider')
    parser.add_argument('--api-key', help='API key for LLM provider')
    
    args = parser.parse_args()
    
    # Check for API keys
    openai_key = args.api_key or os.environ.get('OPENAI_API_KEY')
    anthropic_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    
    if not openai_key and not anthropic_key:
        print("No API key found. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY environment variable,")
        print("or provide --api-key argument.")
        provider = input("Choose provider (openai/anthropic): ").lower()
        api_key = input(f"Enter {provider.upper()} API key: ")
    else:
        if args.provider:
            provider = args.provider
            api_key = args.api_key or (openai_key if provider == 'openai' else anthropic_key)
        else:
            if openai_key:
                provider = 'openai'
                api_key = openai_key
            else:
                provider = 'anthropic'
                api_key = anthropic_key
    
    # Get input file
    if args.input_file:
        input_path = args.input_file
    else:
        input_path = input("Enter path to dirty data file: ")
    
    if not os.path.exists(input_path):
        print(f"Error: File not found: {input_path}")
        sys.exit(1)
    
    # Check config file
    config_path = args.config
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print(f"Local CLI Data Cleaning Agent")
    print(f"{'='*60}")
    print(f"Input file: {input_path}")
    print(f"Config: {config_path}")
    print(f"LLM Provider: {provider}")
    print(f"{'='*60}\n")
    
    # Initialize agent
    agent = CLIAgent(config_path, provider, api_key)
    
    # Run pipeline
    output_path = agent.run_pipeline(input_path)
    
    print(f"\n{'='*60}")
    print(f"Cleaning complete!")
    print(f"Output: {output_path}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()
