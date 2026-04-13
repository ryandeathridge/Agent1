# SCP Data Cleaning Agent — Azure Functions

This project implements a data cleaning agent for procurement data using Azure Functions and Microsoft Copilot Studio.

## Project Structure

```
scp-cleaning-functions/
├── host.json                      # Azure Functions host configuration
├── requirements.txt               # Python dependencies
├── local.settings.json           # Local environment variables
├── openapi/
│   └── api_spec_v2.json          # OpenAPI v2 spec for Copilot Studio
├── profile_data/                 # Function: Profile data quality
├── clean_deterministic/          # Function: Apply rule-based cleaning
├── classify_categories/          # Function: Classify records
├── validate_output/              # Function: Validate cleaned data
├── format_output/                # Function: Format for download
├── update_learning_state/        # Function: Update learning dictionaries
├── shared/                       # Shared modules
│   ├── models.py                # Pydantic models
│   ├── blob_helpers.py          # Azure Blob Storage utilities (data files)
│   ├── sharepoint_helpers.py    # Config blob storage utilities (dictionaries/instructions)
│   ├── date_normaliser.py       # Date normalization
│   ├── amount_normaliser.py     # Amount normalization
│   ├── vendor_matcher.py        # Fuzzy vendor matching
│   ├── encoding_fixer.py        # Encoding repair
│   ├── deduplicator.py          # Duplicate detection
│   ├── unit_standardiser.py     # Unit standardization
│   ├── schema_validator.py      # Schema validation
│   └── consistency_checker.py   # Cross-record consistency checks
└── tests/                        # Test suite
```

## Functions

### 1. profile-data
Analyzes data quality and structure. Returns column profiles, duplicate counts, and quality scores.

**Endpoint:** `POST /api/profile-data`

### 2. clean-deterministic
Applies rule-based cleaning: encoding fixes, date/amount normalization, vendor matching, unit standardization, and deduplication.

**Endpoint:** `POST /api/clean-deterministic`

### 3. classify-categories
Performs deterministic category classification using keyword matching, supplier inference, and similarity to examples.

**Endpoint:** `POST /api/classify-categories`

### 4. validate-output
Validates cleaned data against schema, checks cross-record consistency, and detects outliers.

**Endpoint:** `POST /api/validate-output`

### 5. format-output
Formats cleaned data as Excel or CSV with optional changes log and summary sheet.

**Endpoint:** `POST /api/format-output`

### 6. update-learning-state
Updates blob storage-hosted dictionaries and instructions. Makes the agent self-improving over time.

**Endpoint:** `POST /api/update-learning-state`

## Setup

### Prerequisites

- Python 3.11
- Azure Functions Core Tools
- Azure Storage Account with two containers:
  - Data container (for input/output files)
  - Config container (for dictionaries and instructions)

### Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install Azure Functions Core Tools (if not already installed)
npm install -g azure-functions-core-tools@4

# Install Azurite for local blob storage testing
npm install -g azurite
```

### Configuration

1. Create or update `local.settings.json` with your credentials:

```json
{
  "Values": {
    "AZURE_STORAGE_CONNECTION_STRING": "<your-storage-connection-string>",
    "BLOB_CONTAINER_NAME": "scp-cleaning",
    "CONFIG_CONTAINER_NAME": "config"
  }
}
```

2. Upload seed config files to blob storage:

```bash
# Create the config container
az storage container create \
  --name config \
  --connection-string "<your-connection-string>"

# Upload seed files
az storage blob upload-batch \
  --account-name <storage-account-name> \
  --destination config \
  --source ./config \
  --overwrite
```

The seed files include:
   - `vendor_dictionary.json` (empty `{}` to start)
   - `abbreviation_dictionary.json` (empty `{}` to start)
   - `few_shot_examples.json` (empty `[]` to start)
   - `agent_instructions.md` (agent instructions template)

## Running Locally

```bash
# Start Azurite (local blob storage emulator)
azurite --silent &

# Start Azure Functions
func start
```

The functions will be available at `http://localhost:7071/api/`

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_profile_data.py -v

# Run integration tests
pytest tests/test_end_to_end.py -v -m integration
```

## Deployment

### Manual Deployment

```bash
# Deploy to Azure
func azure functionapp publish <function-app-name> --python

# Set environment variables in Azure
az functionapp config appsettings set \
  --name <app-name> \
  --resource-group <rg> \
  --settings \
    AZURE_STORAGE_CONNECTION_STRING="<conn>" \
    BLOB_CONTAINER_NAME="scp-cleaning" \
    CONFIG_CONTAINER_NAME="config"
```

### CI/CD Deployment

A GitHub Actions workflow is included at `.github/workflows/deploy.yml` that automatically deploys the function app on push to `main`. Configure the following secrets in your GitHub repository:

- `AZURE_FUNCTIONAPP_NAME`: Name of your Azure Function App
- `AZURE_FUNCTIONAPP_PUBLISH_PROFILE`: Publish profile XML (download from Azure Portal)

## Copilot Studio Integration

1. Deploy the functions to Azure
2. Register the API in Copilot Studio using the OpenAPI spec at `openapi/api_spec_v2.json`
3. Update the `host` field in the OpenAPI spec to match your deployed function app URL
4. Create actions in Copilot Studio for each function
5. Configure the agent instructions using the template in SharePoint

## Architecture

This implementation follows a modular, self-improving architecture:

1. **Deterministic Cleaning**: Rule-based cleaning handles 80%+ of records automatically
2. **LLM Reasoning**: Copilot Studio LLM handles ambiguous cases flagged by deterministic cleaning
3. **Self-Learning**: Each run updates blob-stored dictionaries and examples, improving future performance
4. **Provenance Tracking**: All changes are logged with confidence scores and methods
5. **Chunked Processing**: Large datasets (up to 1M rows) are processed in 50K row chunks to stay within Azure Functions timeout limits

## Performance

- Profile 50K rows: <10 seconds
- Clean 50K rows: <120 seconds (chunked processing for datasets up to 1M rows)
- Validate 50K rows: <30 seconds
- Format to Excel: <20 seconds

**Note**: For datasets larger than 50K rows, the clean-deterministic function automatically chunks the data into batches of 50K rows and processes them sequentially to stay within Azure Functions HTTP timeout limits (230 seconds).

## License

Proprietary - BHP Internal Use Only
