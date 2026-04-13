# SCP Cleaning API

FastAPI application for SCP data cleaning, designed for deployment on Railway or any standard Python hosting platform.

## Overview

This API provides the same functionality as the Azure Functions implementation in `scp-cleaning-functions/`, but wrapped in a FastAPI application that can run on any standard Python host.

## Endpoints

All endpoints accept JSON payloads and return JSON responses.

- `GET /` - Health check endpoint
- `POST /api/profile-data` - Analyzes data quality and structure
- `POST /api/clean-deterministic` - Applies rule-based cleaning
- `POST /api/classify-categories` - Deterministic category classification
- `POST /api/validate-output` - Validates cleaned data against schema
- `POST /api/format-output` - Formats cleaned data for download
- `POST /api/update-learning-state` - Updates blob storage dictionaries and instructions

## Environment Variables

Required environment variables:

- `BLOB_CONNECTION_STRING` - Azure Blob Storage connection string
- `BLOB_CONTAINER_NAME` - Blob container name (default: `scp-cleaning`)
- `PORT` - Port to run the server on (default: 8000)

## Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export BLOB_CONNECTION_STRING="your_connection_string"
export BLOB_CONTAINER_NAME="scp-cleaning"
```

3. Run the server:
```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

## Railway Deployment

1. Create a new project on Railway
2. Connect your GitHub repository
3. Set the root directory to `scp-api`
4. Add environment variables in Railway dashboard:
   - `BLOB_CONNECTION_STRING`
   - `BLOB_CONTAINER_NAME`
5. Deploy

Railway will automatically detect the `Procfile` and `railway.json` configuration.

## API Documentation

Once running, visit:
- `/docs` - Interactive Swagger UI documentation
- `/redoc` - ReDoc documentation

## Architecture

The API reuses all the core logic from `shared/`:
- `blob_helpers.py` - Azure Blob Storage operations
- `encoding_fixer.py` - Fix encoding issues
- `date_normaliser.py` - Normalize dates
- `amount_normaliser.py` - Normalize amounts
- `unit_standardiser.py` - Standardize units
- `vendor_matcher.py` - Match vendor names
- `deduplicator.py` - Remove duplicates
- `triangulator.py` - Triangulate amount/quantity/unit_price
- `schema_validator.py` - Validate against schema
- `consistency_checker.py` - Check data consistency
- `models.py` - Pydantic models

## Differences from Azure Functions

- Uses FastAPI `Request`/`JSONResponse` instead of `func.HttpRequest`/`func.HttpResponse`
- No authentication required (auth level: none)
- Same core logic and functionality
- Compatible with any Python hosting platform
