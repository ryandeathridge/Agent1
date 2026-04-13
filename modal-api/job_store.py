"""Modal Dict-backed job state machine.

Stores all job state in a Modal Dict so it survives container restarts.
Each job is isolated by job_id.  Thread-safe within a single container
via the GIL; cross-container consistency is handled by Modal Dict's
last-writer-wins semantics (acceptable for our polling model since only
one container processes a job at a time).
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional

import modal

# ---------------------------------------------------------------------------
# Job states
# ---------------------------------------------------------------------------


class JobState(str, Enum):
    QUEUED = "queued"
    DETERMINISTIC_PASS = "deterministic_pass"
    AWAITING_LLM_BATCH = "awaiting_llm_batch"
    APPLYING_LLM_RESPONSE = "applying_llm_response"
    FINALIZING = "finalizing"
    DONE = "done"
    ERROR = "error"


# ---------------------------------------------------------------------------
# Per-row flag record
# ---------------------------------------------------------------------------


@dataclass
class FlaggedRow:
    row_id: int
    fields: Dict[str, Any] = field(default_factory=dict)
    reasons: Dict[str, str] = field(default_factory=dict)
    original_values: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Job record
# ---------------------------------------------------------------------------

MAX_LOG_LINES = 200


@dataclass
class JobRecord:
    job_id: str
    state: str = JobState.QUEUED.value
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)

    total_rows: int = 0
    rows_processed: int = 0
    flagged_count: int = 0
    progress_pct: float = 0.0

    # Deterministic pass data stored as list-of-dicts for JSON safety
    input_data: List[Dict[str, Any]] = field(default_factory=list)
    cleaned_data: List[Dict[str, Any]] = field(default_factory=list)
    original_data: List[Dict[str, Any]] = field(default_factory=list)

    # Flagged rows awaiting LLM (list of FlaggedRow dicts)
    flagged_rows: List[Dict[str, Any]] = field(default_factory=list)

    # Caller-provided dictionaries
    taxonomy: Dict[str, Any] = field(default_factory=dict)
    abbreviation_dictionary: Dict[str, str] = field(default_factory=dict)
    vendor_dictionary: Dict[str, str] = field(default_factory=dict)

    # LLM loop tracking
    iteration: int = 0
    max_iterations: int = 5
    convergence_trace: List[Dict[str, Any]] = field(default_factory=list)
    llm_batch_cursor: int = 0
    llm_batch_size: int = 25

    # Output paths (relative to volume mount)
    output_paths: Dict[str, str] = field(default_factory=dict)

    # Change tracking
    changes: List[Dict[str, Any]] = field(default_factory=list)

    # Rule hit stats
    rule_stats: Dict[str, int] = field(default_factory=dict)
    pass_timings: List[Dict[str, Any]] = field(default_factory=list)

    # Structured log (ring buffer, capped at MAX_LOG_LINES)
    log_lines: List[str] = field(default_factory=list)

    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# Store API backed by a plain Python dict (works in-process for tests
# and is swapped to Modal Dict at deploy time via `set_backend`).
# ---------------------------------------------------------------------------

_backend: Dict[str, Dict[str, Any]] = {}
_use_modal_dict: bool = False
_modal_dict_ref: Any = None


def set_backend(modal_dict: Any) -> None:
    """Configure the module to use a Modal Dict instance as storage."""
    global _use_modal_dict, _modal_dict_ref
    _use_modal_dict = True
    _modal_dict_ref = modal_dict


def _get(key: str) -> Optional[Dict[str, Any]]:
    if _use_modal_dict:
        try:
            return _modal_dict_ref[key]
        except KeyError:
            return None
    return _backend.get(key)


def _put(key: str, value: Dict[str, Any]) -> None:
    if _use_modal_dict:
        _modal_dict_ref[key] = value
    else:
        _backend[key] = value


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def create_job(
    input_data: List[Dict[str, Any]],
    taxonomy: Dict[str, Any],
    abbreviation_dictionary: Dict[str, str],
    vendor_dictionary: Dict[str, str],
    max_iterations: int = 5,
    llm_batch_size: int = 25,
) -> str:
    job_id = uuid.uuid4().hex[:12]
    rec = JobRecord(
        job_id=job_id,
        total_rows=len(input_data),
        input_data=input_data,
        original_data=[dict(row) for row in input_data],
        taxonomy=taxonomy,
        abbreviation_dictionary=abbreviation_dictionary,
        vendor_dictionary=vendor_dictionary,
        max_iterations=max_iterations,
        llm_batch_size=llm_batch_size,
    )
    _put(job_id, asdict(rec))
    return job_id


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    return _get(job_id)


def save_job(job: Dict[str, Any]) -> None:
    job["updated_at"] = time.time()
    _put(job["job_id"], job)


def log(job: Dict[str, Any], message: str) -> None:
    ts = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    entry = f"[{ts}] {message}"
    job.setdefault("log_lines", []).append(entry)
    if len(job["log_lines"]) > MAX_LOG_LINES:
        job["log_lines"] = job["log_lines"][-MAX_LOG_LINES:]


def transition(job: Dict[str, Any], new_state: JobState) -> None:
    old = job["state"]
    job["state"] = new_state.value
    log(job, f"State: {old} → {new_state.value}")


def increment_rule_stat(job: Dict[str, Any], rule_name: str, count: int = 1) -> None:
    job.setdefault("rule_stats", {})
    job["rule_stats"][rule_name] = job["rule_stats"].get(rule_name, 0) + count


def reset_store() -> None:
    """Clear the in-process backend (useful for tests)."""
    global _backend
    _backend = {}
