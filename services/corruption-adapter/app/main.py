from __future__ import annotations

import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import httpx
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, make_asgi_app

SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.4.0")
UPSTREAM_PROFILE_URL = os.getenv("UPSTREAM_PROFILE_URL", "http://wikimedia-adapter:8000/profile")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "4"))
MAX_TARGET_USERS = max(1, int(os.getenv("MAX_TARGET_USERS", "80")))

Mode = Literal[
    "none",
    "missing_target",
    "wrong_type",
    "stale_timestamp",
    "outlier",
    "malformed_json",
    "http_503",
]

CORRUPTION_RESPONSES = Counter(
    "opsai_corruption_responses_total",
    "Traffic profile responses by corruption mode and result.",
    ["mode", "outcome"],
)
CORRUPTION_ACTIVE = Gauge(
    "opsai_corruption_mode_active",
    "Currently active payload corruption mode.",
    ["mode"],
)
UPSTREAM_FAILURES = Counter(
    "opsai_corruption_upstream_failures_total",
    "Failures reading the clean Wikimedia profile.",
)
TRAFFIC_OVERRIDE_ACTIVE = Gauge(
    "opsai_traffic_override_active",
    "One while a controlled traffic amplification override is active.",
)
TRAFFIC_OVERRIDE_MULTIPLIER = Gauge(
    "opsai_traffic_override_multiplier",
    "Current controlled traffic amplification multiplier.",
)

app = FastAPI(title="PulseGuard Payload Corruption Adapter", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())
current_mode: Mode = "none"
all_modes: tuple[Mode, ...] = (
    "none",
    "missing_target",
    "wrong_type",
    "stale_timestamp",
    "outlier",
    "malformed_json",
    "http_503",
)
traffic_override: dict[str, Any] = {
    "active": False,
    "multiplier": 1.0,
    "startedAt": None,
    "expiresAt": None,
    "durationSeconds": 0,
}


class ModeRequest(BaseModel):
    mode: Mode


class TrafficOverrideRequest(BaseModel):
    multiplier: float = Field(ge=1.0, le=10.0)
    durationSeconds: int = Field(ge=10, le=600)


def utc_now_dt() -> datetime:
    return datetime.now(timezone.utc)


def utc_now() -> str:
    return utc_now_dt().isoformat()


def set_metrics(mode: Mode) -> None:
    for item in all_modes:
        CORRUPTION_ACTIVE.labels(mode=item).set(1 if item == mode else 0)


def refresh_override_state() -> dict[str, Any]:
    if traffic_override["active"] and traffic_override["expiresAt"]:
        expires_at = datetime.fromisoformat(str(traffic_override["expiresAt"]).replace("Z", "+00:00"))
        if utc_now_dt() >= expires_at:
            reset_traffic_override_state()
    remaining_seconds = 0
    if traffic_override["active"] and traffic_override["expiresAt"]:
        expires_at = datetime.fromisoformat(str(traffic_override["expiresAt"]).replace("Z", "+00:00"))
        remaining_seconds = max(0, math.ceil((expires_at - utc_now_dt()).total_seconds()))
    return {**traffic_override, "remainingSeconds": remaining_seconds, "maxTargetUsers": MAX_TARGET_USERS}


def reset_traffic_override_state() -> None:
    traffic_override.update(
        {
            "active": False,
            "multiplier": 1.0,
            "startedAt": None,
            "expiresAt": None,
            "durationSeconds": 0,
        }
    )
    TRAFFIC_OVERRIDE_ACTIVE.set(0)
    TRAFFIC_OVERRIDE_MULTIPLIER.set(1)


def apply_traffic_override(payload: dict[str, Any]) -> dict[str, Any]:
    state = refresh_override_state()
    payload["trafficOverride"] = state
    if not state["active"]:
        return payload

    base_target = payload.get("targetUsers")
    if isinstance(base_target, bool) or not isinstance(base_target, int):
        return payload

    multiplier = float(state["multiplier"])
    amplified_target = min(MAX_TARGET_USERS, max(1, math.ceil(base_target * multiplier)))
    base_spawn_rate = payload.get("spawnRate", 5)
    if isinstance(base_spawn_rate, bool) or not isinstance(base_spawn_rate, (int, float)):
        base_spawn_rate = 5
    amplified_spawn_rate = min(20, max(1, math.ceil(float(base_spawn_rate) * min(multiplier, 3.0))))

    payload["baseTargetUsers"] = base_target
    payload["targetUsers"] = amplified_target
    payload["spawnRate"] = amplified_spawn_rate
    payload["profile"] = "forced-surge"
    payload["trafficAmplification"] = {
        "multiplier": multiplier,
        "baseTargetUsers": base_target,
        "amplifiedTargetUsers": amplified_target,
        "capped": amplified_target >= MAX_TARGET_USERS and base_target * multiplier > MAX_TARGET_USERS,
        "maxTargetUsers": MAX_TARGET_USERS,
        "remainingSeconds": state["remainingSeconds"],
    }
    return payload


@app.on_event("startup")
async def startup_event() -> None:
    set_metrics(current_mode)
    reset_traffic_override_state()


@app.get("/")
async def root() -> dict[str, object]:
    return {
        "service": "corruption-adapter",
        "version": SERVICE_VERSION,
        "mode": current_mode,
        "trafficOverride": refresh_override_state(),
    }


@app.get("/health")
async def health() -> dict[str, object]:
    return {
        "status": "healthy",
        "service": "corruption-adapter",
        "mode": current_mode,
        "trafficOverride": refresh_override_state(),
    }


@app.get("/mode")
async def get_mode() -> dict[str, str]:
    return {"mode": current_mode}


@app.post("/mode")
async def update_mode(request: ModeRequest) -> dict[str, str]:
    global current_mode
    current_mode = request.mode
    set_metrics(current_mode)
    return {"mode": current_mode}


@app.get("/traffic-override")
async def get_traffic_override() -> dict[str, Any]:
    return refresh_override_state()


@app.post("/traffic-override")
async def set_traffic_override(request: TrafficOverrideRequest) -> dict[str, Any]:
    started_at = utc_now_dt()
    expires_at = started_at + timedelta(seconds=request.durationSeconds)
    traffic_override.update(
        {
            "active": True,
            "multiplier": float(request.multiplier),
            "startedAt": started_at.isoformat(),
            "expiresAt": expires_at.isoformat(),
            "durationSeconds": request.durationSeconds,
        }
    )
    TRAFFIC_OVERRIDE_ACTIVE.set(1)
    TRAFFIC_OVERRIDE_MULTIPLIER.set(float(request.multiplier))
    return refresh_override_state()


@app.post("/traffic-override/reset")
async def reset_traffic_override() -> dict[str, Any]:
    reset_traffic_override_state()
    return refresh_override_state()


@app.get("/profile")
async def profile() -> Any:
    mode = current_mode
    if mode == "http_503":
        CORRUPTION_RESPONSES.labels(mode=mode, outcome="injected").inc()
        raise HTTPException(status_code=503, detail="Injected upstream profile failure.")

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.get(UPSTREAM_PROFILE_URL)
            response.raise_for_status()
            payload: dict[str, Any] = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        UPSTREAM_FAILURES.inc()
        CORRUPTION_RESPONSES.labels(mode=mode, outcome="upstream_failed").inc()
        raise HTTPException(status_code=502, detail=f"Clean profile unavailable: {exc}") from exc

    if mode == "missing_target":
        payload.pop("targetUsers", None)
    elif mode == "wrong_type":
        payload["targetUsers"] = "not-a-number"
    elif mode == "stale_timestamp":
        payload["generatedAt"] = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    elif mode == "outlier":
        payload["targetUsers"] = 10000
        payload["profile"] = "impossible-surge"
    elif mode == "malformed_json":
        CORRUPTION_RESPONSES.labels(mode=mode, outcome="injected").inc()
        return Response(content='{"generatedAt":', media_type="application/json", status_code=200)

    if mode != "none":
        payload["corruptionMode"] = mode
        CORRUPTION_RESPONSES.labels(mode=mode, outcome="injected").inc()
    else:
        CORRUPTION_RESPONSES.labels(mode=mode, outcome="clean").inc()

    return apply_traffic_override(payload)
