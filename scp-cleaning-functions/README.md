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
│   ├── blob_helpers.py          # Azure Blob Storage utilities
│   ├── sharepoint_helpers.py    # SharePoint integration
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
Updates SharePoint-hosted dictionaries and instructions. Makes the agent self-improving over time.

**Endpoint:** `POST /api/update-learning-state`

## Setup

### Prerequisites

- Python 3.9+
- Azure Functions Core Tools
- Azure Storage Account
- SharePoint site with appropriate permissions

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

1. Copy `local.settings.json` and update with your credentials:

```json
{
  "Values": {
    "BLOB_CONNECTION_STRING": "<your-blob-connection-string>",
    "BLOB_CONTAINER_NAME": "scp-cleaning",
    "SHAREPOINT_TENANT_ID": "<your-tenant-id>",
    "SHAREPOINT_CLIENT_ID": "<your-client-id>",
    "SHAREPOINT_CLIENT_SECRET": "<your-client-secret>",
    "SHAREPOINT_SITE_ID": "<your-site-id>"
  }
}
```

2. Create SharePoint files:
   - `SCP/vendor_dictionary.json` (empty `{}` to start)
   - `SCP/abbreviation_dictionary.json` (seeded with common abbreviations)
   - `SCP/few_shot_examples.json` (empty `[]` to start)
   - `SCP/agent_instructions.md` (agent instructions)

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

```bash
# Deploy to Azure
func azure functionapp publish <function-app-name> --python

# Set environment variables in Azure
az functionapp config appsettings set \
  --name <app-name> \
  --resource-group <rg> \
  --settings \
    BLOB_CONNECTION_STRING="<conn>" \
    BLOB_CONTAINER_NAME="scp-cleaning" \
    SHAREPOINT_TENANT_ID="<tid>" \
    SHAREPOINT_CLIENT_ID="<cid>" \
    SHAREPOINT_CLIENT_SECRET="<secret>" \
    SHAREPOINT_SITE_ID="<sid>"
```

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
3. **Self-Learning**: Each run updates dictionaries and examples, improving future performance
4. **Provenance Tracking**: All changes are logged with confidence scores and methods

## Performance

- Profile 50K rows: <10 seconds
- Clean 50K rows: <120 seconds
- Validate 50K rows: <30 seconds
- Format to Excel: <20 seconds

## License

Proprietary - BHP Internal Use Only
