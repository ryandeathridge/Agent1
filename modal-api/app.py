"""Modal app + FastAPI endpoints for the hybrid cleaning API.

Architecture
============
The LLM agent (caller) orchestrates.  It inspects dirty data, decides a
taxonomy, then hands everything to this Modal API.  Modal runs deterministic
cleaning, flags uncertain rows, and hands them back via polling endpoints.

Communication is entirely via polling — no webhooks — so a ChatGPT Custom
GPT can act as the caller.

LLM reasoning stays on the caller side.  Modal never calls an LLM directly
and requires no API keys.

Endpoints
---------
POST /jobs                      — submit a new cleaning job
GET  /jobs/{id}/status          — poll job state & progress
GET  /jobs/{id}/next_llm_batch  — get next batch of flagged rows
POST /jobs/{id}/llm_batch_response — submit LLM-cleaned values
GET  /jobs/{id}/results         — download signed URLs when done
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Any, Dict, List, Optional

import modal
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

try:
    from . import job_store, output_builder, pipeline
except ImportError:
    import job_store, output_builder, pipeline  # type: ignore[no-redef]

# ---------------------------------------------------------------------------
# Modal setup
# ---------------------------------------------------------------------------

app = modal.App("scp-cleaning-api")

volume = modal.Volume.from_name("scp-cleaning-outputs", create_if_missing=True)
VOLUME_MOUNT = "/data/outputs"

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "fastapi",
        "uvicorn",
        "openpyxl",
        "pandas",
        "numpy",
        "python-dateutil",
        "rapidfuzz",
        "ftfy",
        "pydantic",
    )
)

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

web_app = FastAPI(
    title="SCP Cleaning API",
    version="1.0.0",
    description="Hybrid deterministic + LLM-in-the-loop data cleaning for procurement data.",
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class JobCreateRequest(BaseModel):
    data: List[Dict[str, Any]] = Field(..., description="Dirty data rows (inline JSON)")
    taxonomy: Dict[str, Any] = Field(default_factory=dict, description="Category taxonomy tree")
    abbreviation_dictionary: Dict[str, str] = Field(default_factory=dict)
    vendor_dictionary: Dict[str, str] = Field(default_factory=dict)
    max_iterations: int = Field(default=5, ge=1, le=20)
    llm_batch_size: int = Field(default=25, ge=1, le=500)


class LLMRowResponse(BaseModel):
    row_id: int
    field_values: Dict[str, Any]
    confidence_per_field: Dict[str, float] = Field(default_factory=dict)
    notes: Optional[str] = None


class LLMBatchResponse(BaseModel):
    responses: List[LLMRowResponse]


# ---------------------------------------------------------------------------
# Signing helpers for output URLs
# ---------------------------------------------------------------------------

_SIGNING_SECRET = os.environ.get("SCP_SIGNING_SECRET", "dev-secret-change-me")
_URL_EXPIRY_SECONDS = 86400  # 24 hours


def _sign_path(job_id: str, filename: str, expires: int) -> str:
    payload = f"{job_id}/{filename}/{expires}"
    sig = hmac.new(_SIGNING_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
    return sig


def _make_signed_url(base_url: str, job_id: str, filename: str) -> str:
    expires = int(time.time()) + _URL_EXPIRY_SECONDS
    sig = _sign_path(job_id, filename, expires)
    return f"{base_url}/files/{job_id}/{filename}?expires={expires}&sig={sig}"


def _verify_signature(job_id: str, filename: str, expires: int, sig: str) -> bool:
    if time.time() > expires:
        return False
    expected = _sign_path(job_id, filename, expires)
    return hmac.compare_digest(expected, sig)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@web_app.post("/jobs")
async def create_job(req: JobCreateRequest):
    """Submit a new cleaning job."""
    if not req.data:
        raise HTTPException(status_code=400, detail="data must be a non-empty array of row objects")

    job_id = job_store.create_job(
        input_data=req.data,
        taxonomy=req.taxonomy,
        abbreviation_dictionary=req.abbreviation_dictionary,
        vendor_dictionary=req.vendor_dictionary,
        max_iterations=req.max_iterations,
        llm_batch_size=req.llm_batch_size,
    )

    # Run the first deterministic pass synchronously (fast enough for inline data)
    job = job_store.get_job(job_id)
    pipeline.run_deterministic_pass(job)

    # Check if any rows flagged
    if job["flagged_count"] > 0:
        job_store.transition(job, job_store.JobState.AWAITING_LLM_BATCH)
        job["iteration"] = 1
        job["convergence_trace"].append({
            "iteration": 0,
            "flagged_count": job["flagged_count"],
            "timestamp": time.time(),
        })
    else:
        _finalize_job(job)

    job_store.save_job(job)
    return {"job_id": job_id, "state": job["state"], "total_rows": job["total_rows"]}


@web_app.get("/jobs/{job_id}/status")
async def get_status(job_id: str):
    """Poll job state and progress."""
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "state": job["state"],
        "progress_pct": job.get("progress_pct", 0),
        "total_rows": job.get("total_rows", 0),
        "rows_processed": job.get("rows_processed", 0),
        "flagged_count": job.get("flagged_count", 0),
        "iteration": job.get("iteration", 0),
        "log_lines": job.get("log_lines", [])[-20:],
    }


@web_app.get("/jobs/{job_id}/next_llm_batch")
async def next_llm_batch(job_id: str, batch_size: Optional[int] = None):
    """When state is awaiting_llm_batch, return the next batch of flagged rows."""
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["state"] != job_store.JobState.AWAITING_LLM_BATCH.value:
        return Response(status_code=204)

    flagged = job.get("flagged_rows", [])
    if not flagged:
        return Response(status_code=204)

    bs = batch_size or job.get("llm_batch_size", 25)
    cursor = job.get("llm_batch_cursor", 0)

    batch = flagged[cursor: cursor + bs]
    if not batch:
        return Response(status_code=204)

    # Enrich batch with current row values
    rows = job.get("cleaned_data", [])
    enriched = []
    for item in batch:
        rid = item["row_id"]
        row_data = rows[rid] if rid < len(rows) else {}
        enriched.append({
            "row_id": rid,
            "current_values": row_data,
            "fields_needing_reasoning": list(item.get("fields", {}).keys()),
            "reasons": item.get("reasons", {}),
            "original_values": item.get("original_values", {}),
        })

    job["llm_batch_cursor"] = cursor + len(batch)
    job_store.save_job(job)

    return {
        "batch": enriched,
        "batch_size": len(enriched),
        "remaining": max(0, len(flagged) - cursor - len(batch)),
        "iteration": job.get("iteration", 0),
    }


@web_app.post("/jobs/{job_id}/llm_batch_response")
async def submit_llm_response(job_id: str, req: LLMBatchResponse):
    """Submit LLM-cleaned values.  Modal applies them and reruns deterministic passes."""
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["state"] not in (
        job_store.JobState.AWAITING_LLM_BATCH.value,
        job_store.JobState.APPLYING_LLM_RESPONSE.value,
    ):
        raise HTTPException(status_code=409, detail=f"Job is in state '{job['state']}', cannot accept LLM responses")

    pipeline.apply_llm_responses(job, [r.model_dump() for r in req.responses])

    prev_flagged = 0
    if job["convergence_trace"]:
        prev_flagged = job["convergence_trace"][-1].get("flagged_count", 0)

    current_flagged = job["flagged_count"]
    total = job.get("total_rows", 1)
    iteration = job.get("iteration", 1)

    job["convergence_trace"].append({
        "iteration": iteration,
        "flagged_count": current_flagged,
        "timestamp": time.time(),
    })

    # Loop termination checks
    stop = False
    if current_flagged == 0:
        stop = True
        job_store.log(job, "All rows clean — converged.")
    elif current_flagged >= prev_flagged and iteration > 1:
        stop = True
        job_store.log(job, f"Flagged count not decreasing ({prev_flagged}→{current_flagged}) — stopping.")
    elif current_flagged / max(total, 1) < 0.01:
        stop = True
        job_store.log(job, f"Flagged < 1% of input ({current_flagged}/{total}) — stopping.")
    elif iteration >= job.get("max_iterations", 5):
        stop = True
        job_store.log(job, f"Max iterations ({job.get('max_iterations', 5)}) reached — stopping.")

    if stop:
        _finalize_job(job)
    else:
        job["iteration"] = iteration + 1
        job["llm_batch_cursor"] = 0
        job_store.transition(job, job_store.JobState.AWAITING_LLM_BATCH)

    job_store.save_job(job)

    return {
        "state": job["state"],
        "flagged_count": current_flagged,
        "iteration": job.get("iteration", 0),
        "converged": stop,
    }


@web_app.get("/jobs/{job_id}/results")
async def get_results(job_id: str):
    """When done, return signed URLs to the 4 output files + summary."""
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job["state"] != job_store.JobState.DONE.value:
        raise HTTPException(status_code=409, detail=f"Job is in state '{job['state']}', not done yet")

    output_paths = job.get("output_paths", {})
    base_url = ""  # Relative — caller prepends the Modal function URL

    signed = {}
    for name, path in output_paths.items():
        filename = os.path.basename(path)
        signed[name] = _make_signed_url(base_url, job_id, filename)

    # Also return inline summary
    summary_path = output_paths.get("report_summary")
    summary = {}
    if summary_path and os.path.exists(summary_path):
        import json
        with open(summary_path) as f:
            summary = json.load(f)

    return {"signed_urls": signed, "summary": summary}


@web_app.get("/files/{job_id}/{filename}")
async def download_file(job_id: str, filename: str, expires: int = 0, sig: str = ""):
    """Serve an output file with signature verification."""
    if not _verify_signature(job_id, filename, expires, sig):
        raise HTTPException(status_code=403, detail="Invalid or expired signature")

    file_path = os.path.join(VOLUME_MOUNT, job_id, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    with open(file_path, "rb") as f:
        content = f.read()

    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    if filename.endswith(".json"):
        media_type = "application/json"

    return Response(content=content, media_type=media_type, headers={
        "Content-Disposition": f'attachment; filename="{filename}"',
    })


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _finalize_job(job: Dict[str, Any]) -> None:
    """Build output files and transition to done."""
    job_store.transition(job, job_store.JobState.FINALIZING)
    job_store.log(job, "Building output files…")

    output_dir = os.path.join(VOLUME_MOUNT, job["job_id"])
    paths = output_builder.build_all_outputs(job, output_dir)
    job["output_paths"] = paths

    job_store.transition(job, job_store.JobState.DONE)
    job_store.log(job, f"Job complete.  Outputs at {output_dir}")


# ---------------------------------------------------------------------------
# Modal ASGI entrypoint
# ---------------------------------------------------------------------------

@app.function(
    image=image,
    volumes={VOLUME_MOUNT: volume},
    timeout=3600,
)
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def fastapi_app():
    # Wire up Modal Dict as the job store backend
    try:
        modal_dict = modal.Dict.from_name("scp-job-store", create_if_missing=True)
        job_store.set_backend(modal_dict)
    except Exception:
        pass  # Fall back to in-process dict (useful in local dev)
    return web_app
