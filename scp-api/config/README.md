# Configuration Files

This folder contains seed configuration files for the SCP Data Cleaning Agent.

## Deployment Instructions

On first deployment, upload these files to the `config` blob container in your Azure Storage Account:

```bash
# Using Azure CLI
az storage blob upload-batch \
  --account-name <storage-account-name> \
  --destination config \
  --source ./config \
  --overwrite

# Or using Azure Storage Explorer
# 1. Open Azure Storage Explorer
# 2. Navigate to your storage account
# 3. Create a container named "config" (if it doesn't exist)
# 4. Upload all files from this folder to the config container
```

## Files

- **vendor_dictionary.json**: Maps dirty vendor names to canonical names (starts empty, populated by learning)
- **abbreviation_dictionary.json**: Maps abbreviations to full expansions (starts empty, populated by learning)
- **few_shot_examples.json**: Classification examples for the agent (starts empty, populated by learning)
- **agent_instructions.md**: Instructions and learned rules for the Copilot Studio agent

## Environment Variables

The functions expect the following environment variable:

- `AZURE_STORAGE_CONNECTION_STRING`: Connection string for Azure Storage Account
- `CONFIG_CONTAINER_NAME` (optional): Name of the config container (defaults to "config")
