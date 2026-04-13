# Local CLI Data Cleaning Agent

A local Python CLI agent that replaces the HTTP-based ChatGPT + Railway API approach for data cleaning. This agent runs entirely in-process with no timeout constraints, making it suitable for processing large files (50K–1M rows).

## Why CLI Instead of HTTP?

- **No timeouts**: Processes run locally without HTTP timeout constraints
- **Data privacy**: Client data stays on-machine
- **Scalability**: Handles 50K–1M row files without issues
- **Server-ready**: Designed to be easily converted to a server script later

## Features

- **7-stage pipeline**: Profile → Strategy → Clean → Classify → Validate → Output → Learn
- **LLM integration**: Supports both OpenAI (GPT-4o) and Anthropic (Claude 3.5 Sonnet)
- **Structured output**: All LLM calls use JSON mode for reliable parsing
- **Rich terminal UI**: Beautiful tables and progress indicators (optional)
- **Error handling**: Retry logic for LLM calls, graceful failure handling
- **Learning state**: Automatically updates vendor dictionaries and classification examples

## Installation

### Prerequisites

- Python 3.8+
- pip

### Install Dependencies

```bash
cd /workspace
pip install -r scp-cleaning-functions/requirements.txt
```

Or install individually:

```bash
pip install pandas numpy openpyxl rapidfuzz scikit-learn python-dateutil ftfy chardet recordlinkage pydantic pyarrow openai anthropic rich
```

## Configuration

### 1. Set API Key

The agent requires either an OpenAI or Anthropic API key.

**Option A: Environment variable (recommended)**

```bash
export OPENAI_API_KEY="sk-..."
# or
export ANTHROPIC_API_KEY="sk-ant-..."
```

**Option B: Command-line argument**

```bash
python3 agent_cli.py --api-key "sk-..." --provider openai input.xlsx
```

**Option C: Interactive prompt**

If no key is found, the agent will prompt you to enter one.

### 2. Prepare Config File

The agent uses `scp-cleaning-functions/config/bhp_config.json` by default. This file contains:

- **schema**: Field definitions (name, type, required)
- **category_taxonomy**: Hierarchical category structure (L1 → L2 → L3)
- **top_20_suppliers**: Master vendor list for matching
- **units**: Valid unit codes (EA, KG, L, etc.)
- **payment_terms**: Valid payment terms
- **valid_currencies**: Accepted currencies
- **date_range**: Expected date range for validation

You can override the config path with `--config`:

```bash
python3 agent_cli.py --config /path/to/custom_config.json input.xlsx
```

## Usage

### Basic Usage

```bash
python3 agent_cli.py path/to/dirty_data.xlsx
```

### With Options

```bash
python3 agent_cli.py \
  --config scp-cleaning-functions/config/bhp_config.json \
  --provider anthropic \
  --api-key "sk-ant-..." \
  bhp_dirty_50k.xlsx
```

### Interactive Mode

If you don't provide an input file, the agent will prompt you:

```bash
python3 agent_cli.py
# Enter path to dirty data file: bhp_dirty_50k.xlsx
```

## Pipeline Stages

### Stage 1: Profile

- Loads file into pandas DataFrame (supports `.xlsx` and `.csv`)
- Analyzes each column: data type, null rate, cardinality, sample values
- Detects duplicates (exact and near-duplicates)
- Calculates overall quality score
- Prints summary table with flagged columns

**Output**: Profile summary with row count, null rates, duplicate count, quality score

### Stage 2: LLM — Cleaning Strategy

- Sends profile summary to LLM
- LLM decides which columns to clean deterministically vs with LLM
- LLM suggests vendor normalization, date format, etc.
- Prints strategy to terminal
- Asks user to confirm or edit before proceeding

**Output**: Cleaning strategy JSON

### Stage 3: Clean Deterministic

- **Encoding fixes**: Corrects text corruption (smart quotes, mojibake)
- **Date normalization**: Converts various formats to ISO (YYYY-MM-DD)
- **Amount normalization**: Removes currency symbols, thousands separators
- **Triangulation**: Derives missing values (amount = quantity × unit_price)
- **Unit standardization**: Maps variants to canonical units (KG, L, EA)
- **Vendor matching**: Fuzzy matches to master vendor list
- **Deduplication**: Removes exact and near-duplicates

All changes are logged with confidence scores. Records with confidence < 0.70 are flagged for LLM review.

**Output**: Cleaned DataFrame + Flagged DataFrame + Changes log

### Stage 4: LLM — Classify Flagged Records

- For records where `needs_llm=True` (low confidence category classification):
- Batches into groups of 50
- Sends each batch to LLM with taxonomy from config
- LLM returns `category_l1/l2/l3` + confidence for each record
- Prints batch progress: "Classifying batch 3/12..."
- After all batches, prints summary of classifications applied
- Asks user: "Review flagged classifications? (y/n)" — if yes, shows table

**Output**: Updated Flagged DataFrame with LLM-assigned categories

### Stage 5: Validate

- **Schema validation**: Checks required fields, data types, value ranges
- **Consistency checks**: Cross-field validation (amount = quantity × unit_price)
- **Outlier detection**: Z-score analysis by category, date range checks
- Prints: valid row count, error count, warning count
- If errors > 0, asks user: "Proceed despite validation errors? (y/n)"

**Output**: Validation result with error/warning counts

### Stage 6: Output

- Combines cleaned + flagged DataFrames
- Writes to Excel file in same directory as input
- Output filename: `<original_name>_cleaned_<YYYYMMDD>.xlsx`
- Prints: "Saved to: /path/to/file_cleaned_20260413.xlsx"

**Output**: Cleaned Excel file

### Stage 7: Update Learning State

- Extracts confirmed vendor mappings (confidence ≥ 0.8)
- Extracts classification examples (confidence ≥ 0.8)
- Appends to local config files:
  - `scp-cleaning-functions/config/vendor_dictionary.json`
  - `scp-cleaning-functions/config/few_shot_examples.json`
- Prints count of new entries added

**Output**: Updated learning state files

## Error Handling

### LLM Call Failures

All LLM calls have retry logic (max 3 attempts). If all attempts fail, the agent prints a clear error message and stops.

### Stage Failures

If a stage fails, the agent asks: "Retry this stage? (y/n)"

### Keyboard Interrupt (Ctrl+C)

On Ctrl+C, the agent saves progress to a temp file and exits cleanly:

```
Progress saved to: /path/to/temp_progress_20260413_143022.xlsx
```

## Output Files

### Cleaned Data

- **Location**: Same directory as input file
- **Filename**: `<original_name>_cleaned_<YYYYMMDD>.xlsx`
- **Format**: Excel (.xlsx)
- **Contents**: All cleaned records (high-confidence + LLM-classified flagged records)

### Learning State

- **vendor_dictionary.json**: Maps dirty vendor names → canonical names
- **few_shot_examples.json**: Classification examples for LLM few-shot learning

## Server-Ready Design

The pipeline is structured so each stage is a callable function with clear inputs/outputs (DataFrame in, DataFrame + stats out). This makes it easy to wrap in an async API endpoint later:

```python
# Example: Convert to FastAPI endpoint
@app.post("/clean")
async def clean_endpoint(file: UploadFile):
    agent = CLIAgent(config_path, llm_provider, api_key)
    df = pd.read_excel(file.file)
    agent.df = df
    
    # Run stages
    agent.stage1_profile(file.filename)
    agent.stage2_llm_strategy()
    agent.stage3_clean_deterministic()
    # ... etc
    
    return {"cleaned_df": agent.cleaned_df.to_dict()}
```

## Examples

### Example 1: Basic Usage

```bash
python3 agent_cli.py bhp_dirty_50k.xlsx
```

**Output**:

```
═══ Stage 1: Profile Data ═══
Loading file: bhp_dirty_50k.xlsx
Loaded 50000 rows, 23 columns

Profile Summary:
┌─────────────────────┬────────┐
│ Metric              │ Value  │
├─────────────────────┼────────┤
│ Total Rows          │ 50000  │
│ Total Columns       │ 23     │
│ Duplicate Count     │ 234    │
│ Near Duplicate Count│ 89     │
│ Encoding Issues     │ 12     │
│ Quality Score       │ 94.2%  │
└─────────────────────┴────────┘

Flagged Columns:
  • supplier_name: null_rate=5.2%, flags=[]
  • description: null_rate=0.1%, flags=['encoding issues detected']

═══ Stage 2: LLM Cleaning Strategy ═══
Calling LLM to generate cleaning strategy...

Cleaning Strategy:
Deterministic fields: date, amount, unit_price, supplier_name, unit
LLM fields: category_l1, category_l2, category_l3
Vendor normalization: True
Date format: YYYY-MM-DD
Estimated LLM records: 2341
Notes: Most fields can be cleaned with rules. Category classification needs LLM for ~5% of records.

Proceed with this strategy? (y/n): y

═══ Stage 3: Deterministic Cleaning ═══
Fixing encoding issues...
  Fixed 12 encoding issues in description
Normalizing dates...
  Normalized 1234 dates
Normalizing amount...
  Normalized 567 amounts
...

Cleaning Stats:
┌──────────────────┬────────┐
│ Metric           │ Value  │
├──────────────────┼────────┤
│ Input Rows       │ 50000  │
│ Cleaned Rows     │ 47659  │
│ Flagged Rows     │ 2341   │
│ Duplicates Removed│ 234   │
│ Fields Modified  │ 3456   │
└──────────────────┴────────┘

═══ Stage 4: LLM Classification of Flagged Records ═══
Classifying 2341 records...
Classifying batch 1/47...
Classifying batch 2/47...
...
Successfully classified 2341 records

═══ Stage 5: Validation ═══
Validating schema...
Checking consistency...
Detecting outliers...

Validation Results:
┌─────────────────┬────────┐
│ Metric          │ Value  │
├─────────────────┼────────┤
│ Total Records   │ 50000  │
│ Valid Records   │ 49876  │
│ Errors          │ 0      │
│ Warnings        │ 124    │
│ Outliers        │ 89     │
└─────────────────┴────────┘

═══ Stage 6: Output ═══
Writing 50000 rows to bhp_dirty_50k_cleaned_20260413.xlsx...
✓ Saved to: bhp_dirty_50k_cleaned_20260413.xlsx

═══ Stage 7: Update Learning State ═══
Added 234 new vendor mappings
Added 89 new classification examples

Learning State Updated:
┌───────────────────────────────┬────────┐
│ Metric                        │ Value  │
├───────────────────────────────┼────────┤
│ New Vendor Mappings           │ 234    │
│ New Classification Examples   │ 89     │
│ Total Vendor Dictionary Size  │ 1456   │
│ Total Examples Size           │ 567    │
└───────────────────────────────┴────────┘

✓ Pipeline completed successfully!

═══════════════════════════════════════════════════════
Cleaning complete!
Output: bhp_dirty_50k_cleaned_20260413.xlsx
═══════════════════════════════════════════════════════
```

### Example 2: With Custom Config

```bash
python3 agent_cli.py \
  --config /path/to/custom_config.json \
  --provider anthropic \
  dirty_data.csv
```

### Example 3: Interactive Mode

```bash
python3 agent_cli.py

# No API key found. Please set OPENAI_API_KEY or ANTHROPIC_API_KEY environment variable,
# or provide --api-key argument.
# Choose provider (openai/anthropic): anthropic
# Enter ANTHROPIC API key: sk-ant-...
# Enter path to dirty data file: data.xlsx
```

## Troubleshooting

### ModuleNotFoundError

If you see `ModuleNotFoundError: No module named 'pandas'`, install dependencies:

```bash
pip install -r scp-cleaning-functions/requirements.txt
```

### API Key Errors

If you see authentication errors, check that your API key is valid:

```bash
# OpenAI
export OPENAI_API_KEY="sk-..."

# Anthropic
export ANTHROPIC_API_KEY="sk-ant-..."
```

### Config File Not Found

If you see `Config file not found`, check the path:

```bash
# Default location
ls scp-cleaning-functions/config/bhp_config.json

# Or specify custom path
python3 agent_cli.py --config /path/to/config.json input.xlsx
```

### LLM Call Failures

If LLM calls fail repeatedly, check:

1. API key is valid and has credits
2. Network connection is stable
3. LLM service is not experiencing downtime

## Performance

- **Small files (<10K rows)**: ~2-5 minutes
- **Medium files (10K-50K rows)**: ~5-15 minutes
- **Large files (50K-100K rows)**: ~15-30 minutes
- **Very large files (100K-1M rows)**: ~30-120 minutes

Performance depends on:

- Number of flagged records needing LLM classification
- LLM API response time
- File complexity (number of columns, data quality)

## Future Enhancements

- [ ] Add support for more file formats (Parquet, JSON)
- [ ] Add parallel processing for large files
- [ ] Add web UI for non-technical users
- [ ] Add support for custom cleaning rules
- [ ] Add support for incremental learning
- [ ] Convert to async server API (FastAPI)

## License

MIT

## Support

For issues or questions, please open a GitHub issue.
