from __future__ import annotations

import hashlib
import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, make_asgi_app


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
SERVICE_NAME = os.getenv("EXTERNAL_SERVICE_NAME", "partner-risk-service")
ADMIN_TOKEN = require_env("AUTOMATION_API_TOKEN")
INITIAL_TOKEN = require_env("EXTERNAL_AUTH_INITIAL_TOKEN")

REQUESTS = Counter(
    "opsai_external_auth_service_requests_total",
    "Requests received by the synthetic external partner service.",
    ["service", "status"],
)
AUTH_FAILURES = Counter(
    "opsai_external_auth_service_failures_total",
    "Authentication failures returned by the synthetic external service.",
    ["service", "reason"],
)
TOKEN_ROTATIONS = Counter(
    "opsai_external_auth_token_rotations_total",
    "Controlled external-service token rotations.",
    ["service", "reason"],
)
TOKEN_GENERATION = Gauge(
    "opsai_external_auth_token_generation",
    "Monotonic token generation for the synthetic external service.",
    ["service"],
)

app = FastAPI(title="PulseGuard External Partner Authentication Service", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())

active_token = INITIAL_TOKEN
token_generation = 1
last_rotated_at: str | None = None
last_rotation_reason = "startup"


class RiskRequest(BaseModel):
    customerId: str = Field(min_length=1, max_length=100)
    orderId: str = Field(min_length=1, max_length=100)
    amount: float = Field(gt=0)


class RotateRequest(BaseModel):
    reason: str = Field(default="controlled_rotation", min_length=2, max_length=100)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def emit_log(level: str, event: str, **fields: Any) -> None:
    print(
        json.dumps(
            {
                "timestamp": utc_now(),
                "level": level,
                "service": SERVICE_NAME,
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def require_admin(token: str | None) -> None:
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid internal automation token.")


def issue_new_token(reason: str) -> dict[str, Any]:
    global active_token, token_generation, last_rotated_at, last_rotation_reason
    previous = fingerprint(active_token)
    active_token = secrets.token_urlsafe(24)
    token_generation += 1
    last_rotated_at = utc_now()
    last_rotation_reason = reason
    TOKEN_ROTATIONS.labels(service=SERVICE_NAME, reason=reason).inc()
    TOKEN_GENERATION.labels(service=SERVICE_NAME).set(token_generation)
    emit_log(
        "WARNING" if reason == "scenario_auth_failure" else "INFO",
        "external_service_token_rotated",
        reason=reason,
        previousTokenFingerprint=previous,
        activeTokenFingerprint=fingerprint(active_token),
        tokenGeneration=token_generation,
    )
    return {
        "service": SERVICE_NAME,
        "token": active_token,
        "tokenFingerprint": fingerprint(active_token),
        "tokenGeneration": token_generation,
        "rotatedAt": last_rotated_at,
        "reason": reason,
    }


@app.on_event("startup")
async def startup() -> None:
    TOKEN_GENERATION.labels(service=SERVICE_NAME).set(token_generation)
    emit_log(
        "INFO",
        "service_started",
        tokenFingerprint=fingerprint(active_token),
        tokenGeneration=token_generation,
    )


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "purpose": "Synthetic external service protected by bearer-token authentication.",
    }


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": SERVICE_NAME,
        "version": SERVICE_VERSION,
        "tokenGeneration": token_generation,
        "tokenFingerprint": fingerprint(active_token),
        "lastRotatedAt": last_rotated_at,
        "lastRotationReason": last_rotation_reason,
    }


@app.post("/risk-check")
async def risk_check(
    request: RiskRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    supplied = ""
    if authorization and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()

    if not secrets.compare_digest(supplied, active_token):
        REQUESTS.labels(service=SERVICE_NAME, status="authentication_failed").inc()
        AUTH_FAILURES.labels(service=SERVICE_NAME, reason="invalid_bearer_token").inc()
        emit_log(
            "ERROR",
            "external_authentication_failed",
            orderId=request.orderId,
            customerId=request.customerId,
            suppliedTokenFingerprint=fingerprint(supplied) if supplied else "missing",
            expectedTokenGeneration=token_generation,
        )
        raise HTTPException(status_code=401, detail="External service authentication failed.")

    REQUESTS.labels(service=SERVICE_NAME, status="success").inc()
    emit_log(
        "INFO",
        "external_risk_check_completed",
        orderId=request.orderId,
        customerId=request.customerId,
        amount=round(request.amount, 2),
        tokenGeneration=token_generation,
    )
    return {
        "status": "approved",
        "service": SERVICE_NAME,
        "riskBand": "low",
        "tokenGeneration": token_generation,
        "checkedAt": utc_now(),
    }


@app.post("/admin/rotate")
async def rotate(
    request: RotateRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    return issue_new_token(request.reason)


@app.post("/admin/invalidate-client-token")
async def invalidate_client_token(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    return issue_new_token("scenario_auth_failure")


@app.post("/admin/current-token")
async def current_token(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    return {
        "service": SERVICE_NAME,
        "token": active_token,
        "tokenFingerprint": fingerprint(active_token),
        "tokenGeneration": token_generation,
    }
