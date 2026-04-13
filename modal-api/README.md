# SCP Cleaning API — Modal-Hosted Hybrid Deterministic + LLM-in-the-Loop

A Modal-hosted data cleaning API for procurement data.  The LLM agent (caller)
orchestrates: it inspects dirty data, decides taxonomy, and hands everything to
this API.  Modal runs all deterministic cleaning, then flags uncertain rows for
the caller LLM to reason about.  Communication is via polling — no webhooks
required.

**Key design principle:** LLM reasoning stays on the caller side.  Modal never
calls an LLM directly — no API keys are stored in Modal.

## Architecture

```
┌──────────────────────┐      ┌──────────────────────────────┐
│  LLM Agent (Caller)  │      │  Modal API                    │
│  • inspects data     │─────▶│  POST /jobs                   │
│  • decides taxonomy  │      │  (deterministic passes run)    │
│  • reasons on flags  │◀─────│  GET /jobs/{id}/next_llm_batch│
│  • returns answers   │─────▶│  POST /jobs/{id}/llm_batch_.. │
│                      │◀─────│  GET /jobs/{id}/results       │
└──────────────────────┘      └──────────────────────────────┘
```

Loop: deterministic pass → flag uncertain rows → caller LLM reasons → Modal
applies + reruns deterministic → repeat until convergence.

## Folder Structure

```
modal-api/
├── app.py              # Modal app + FastAPI endpoints
├── pipeline.py         # Deterministic cleaning passes
├── job_store.py        # Modal Dict-backed job state machine
├── output_builder.py   # Assembles xlsx outputs with color coding
├── requirements.txt    # Python dependencies
├── test_end_to_end.py  # End-to-end test with mock LLM responder
└── README.md           # This file
```

## Deployment

### Prerequisites

- Python 3.11+
- A [Modal](https://modal.com) account with `modal` CLI installed and authenticated

### Install Dependencies

```bash
cd modal-api
pip install -r requirements.txt
```

### Deploy to Modal

```bash
modal deploy modal-api/app.py
```

This creates:
- A Modal **Volume** named `scp-cleaning-outputs` for file persistence
- A Modal **Dict** named `scp-job-store` for job state
- A web endpoint at `https://<your-workspace>--scp-cleaning-api-fastapi-app.modal.run`

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SCP_SIGNING_SECRET` | `dev-secret-change-me` | HMAC secret for signed download URLs |

Set via Modal secrets:
```bash
modal secret create scp-api-secret SCP_SIGNING_SECRET=your-secret-here
```

## API Endpoints

All endpoints use JSON.  Base URL: `https://<your-modal-url>`

### 1. POST /jobs — Submit a Cleaning Job

Submit dirty data with taxonomy and dictionaries.  Returns immediately with a
`job_id`.

```bash
curl -X POST https://<modal-url>/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "data": [
      {"record_id": "R001", "date": "04/07/2025", "amount": "$1,500.00",
       "supplier_name": "Cat", "description": "Diesel fuel", "unit": "litres",
       "quantity": 500, "unit_price": null, "category_l1": "", "category_l2": "", "category_l3": ""},
      ...
    ],
    "taxonomy": {
      "Energy & Fuel": {
        "Diesel": ["Bulk diesel delivery", "Diesel storage"]
      },
      "Equipment & Parts": {
        "Mobile Equipment": ["Haul truck parts", "Excavator parts"]
      }
    },
    "abbreviation_dictionary": {"Cat": "Caterpillar", "ABB": "ABB Ltd"},
    "vendor_dictionary": {"Cat": "Caterpillar", "KOMATSU": "Komatsu"},
    "max_iterations": 5,
    "llm_batch_size": 25
  }'
```

**Response:**
```json
{"job_id": "abc123def456", "state": "awaiting_llm_batch", "total_rows": 100}
```

### 2. GET /jobs/{id}/status — Poll Job State

```bash
curl https://<modal-url>/jobs/abc123def456/status
```

**Response:**
```json
{
  "job_id": "abc123def456",
  "state": "awaiting_llm_batch",
  "progress_pct": 85.0,
  "total_rows": 100,
  "rows_processed": 100,
  "flagged_count": 15,
  "iteration": 1,
  "log_lines": ["[2025-07-04T12:00:00] Pass 'dates': 42 changes...", ...]
}
```

**States:** `queued` → `deterministic_pass` → `awaiting_llm_batch` →
`applying_llm_response` → `finalizing` → `done` | `error`

### 3. GET /jobs/{id}/next_llm_batch — Get Flagged Rows for LLM

Poll this when state is `awaiting_llm_batch`.  Returns 204 if no batch ready.

```bash
curl https://<modal-url>/jobs/abc123def456/next_llm_batch
# Optional: ?batch_size=50
```

**Response (200):**
```json
{
  "batch": [
    {
      "row_id": 42,
      "current_values": {"supplier_name": "Cat Equipment Pty", "category_l1": "", ...},
      "fields_needing_reasoning": ["supplier_name", "category_l1"],
      "reasons": {"supplier_name": "vendor_fuzzy_below_threshold (best=75)", "category_l1": "unknown_taxonomy"},
      "original_values": {"supplier_name": "Cat Equipment Pty", "category_l1": ""}
    }
  ],
  "batch_size": 25,
  "remaining": 12,
  "iteration": 1
}
```

### 4. POST /jobs/{id}/llm_batch_response — Submit LLM Answers

```bash
curl -X POST https://<modal-url>/jobs/abc123def456/llm_batch_response \
  -H "Content-Type: application/json" \
  -d '{
    "responses": [
      {
        "row_id": 42,
        "field_values": {"supplier_name": "Caterpillar", "category_l1": "Equipment & Parts"},
        "confidence_per_field": {"supplier_name": 0.95, "category_l1": 0.8},
        "notes": "Cat Equipment is a Caterpillar dealer"
      }
    ]
  }'
```

**Response:**
```json
{
  "state": "awaiting_llm_batch",
  "flagged_count": 8,
  "iteration": 2,
  "converged": false
}
```

### 5. GET /jobs/{id}/results — Download Outputs

Available once state is `done`.

```bash
curl https://<modal-url>/jobs/abc123def456/results
```

**Response:**
```json
{
  "signed_urls": {
    "cleaned_data": "/files/abc123/cleaned_data.xlsx?expires=...&sig=...",
    "changes_only": "/files/abc123/changes_only.xlsx?expires=...&sig=...",
    "user_verification_required": "/files/abc123/user_verification_required.xlsx?expires=...&sig=...",
    "report_summary": "/files/abc123/report_summary.json?expires=...&sig=..."
  },
  "summary": {
    "total_rows": 100,
    "total_changes": 342,
    "flagged_remaining": 2,
    "iterations": 3,
    "rule_stats": {"date_normalise": 42, "amount_normalise": 38, ...}
  }
}
```

Signed URLs expire after 24 hours.

## Expected Polling Pattern for Callers

```
1. POST /jobs  →  get job_id
2. Loop:
   a. GET /jobs/{id}/status
   b. If state == "awaiting_llm_batch":
      - GET /jobs/{id}/next_llm_batch
      - If 204 → all batches consumed, wait for state change
      - If 200 → reason over batch, then POST /jobs/{id}/llm_batch_response
   c. If state == "done":
      - GET /jobs/{id}/results
      - Download files via signed URLs
      - Break
   d. If state == "error":
      - Read error from status log_lines
      - Break
   e. Sleep 1-2 seconds between polls
```

## Output Files

| File | Description |
|------|-------------|
| `cleaned_data.xlsx` | All rows with color-coded cells |
| `changes_only.xlsx` | Only changed rows, with before/after columns |
| `user_verification_required.xlsx` | Red-flagged rows with reason per field |
| `report_summary.json` | Counts, timings, rule stats, convergence trace |

### Color Coding

| Color | Hex | Meaning |
|-------|-----|---------|
| Green | `#B6D7A8` | Cell changed by deterministic or LLM rule |
| Yellow | `#FFF2CC` | LLM returned low confidence (< 0.7) |
| Red | `#E06666` | Could not clean — user review needed |

## Deterministic Cleaning Rules

1. **Encoding fix** — repairs mojibake, removes BOM, normalises quotes
2. **Date normalisation** — any format → YYYY-MM-DD; recomputes BHP financial_year
3. **Currency stripping** — strips symbols/commas, handles European format, negatives
4. **Triangulation** — derives missing amount/quantity/unit_price from the other two
5. **Abbreviation expansion** — applies caller-provided abbreviation dictionary
6. **Vendor fuzzy match** — rapidfuzz token_sort_ratio, threshold 88; below → flag for LLM
7. **Taxonomy mapping** — maps categories using caller-provided taxonomy; unknown → flag
8. **Unit standardisation** — maps variants to canonical forms (EA, KG, HR, etc.)

## Convergence / Loop Termination

The LLM loop stops when any of:
- Flagged count reaches 0
- Flagged count stops decreasing between iterations
- Flagged count < 1% of input rows
- Max iterations reached (configurable, default 5)

Remaining flagged rows go to `user_verification_required.xlsx`.

## Running Tests

```bash
cd modal-api
pip install -r requirements.txt
pytest test_end_to_end.py -v
```

The test uses the repo's data generators to create synthetic dirty data, runs
the full pipeline with a mock LLM responder, and verifies all outputs.
