from __future__ import annotations

import asyncio
import json
import math
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Gauge, make_asgi_app


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.1")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090").rstrip("/")
OPSAI_AGENT_URL = os.getenv("OPSAI_AGENT_URL", "http://opsai-agent:8000").rstrip("/")
OPSAI_CORE_URL = os.getenv("OPSAI_CORE_URL", "http://opsai-core:8000").rstrip("/")
INTERNAL_TOKEN = require_env("AUTOMATION_API_TOKEN")

POLL_SECONDS = float(os.getenv("PREDICTOR_POLL_SECONDS", "15"))
RANGE_WINDOW_SECONDS = int(os.getenv("PREDICTOR_RANGE_WINDOW_SECONDS", "900"))
RANGE_STEP_SECONDS = int(os.getenv("PREDICTOR_RANGE_STEP_SECONDS", "15"))
MIN_SAMPLES = int(os.getenv("PREDICTOR_MIN_SAMPLES", "12"))
RECURRENCE_LOOKBACK_HOURS = int(os.getenv("PREDICTOR_RECURRENCE_LOOKBACK_HOURS", "24"))
RECURRENCE_THRESHOLD = int(os.getenv("PREDICTOR_RECURRENCE_THRESHOLD", "2"))
RECURRENCE_EVALUATION_SECONDS = int(
    os.getenv("PREDICTOR_RECURRENCE_EVALUATION_SECONDS", "60")
)

DISK_THRESHOLD = float(os.getenv("PREDICTOR_DISK_THRESHOLD_PERCENT", "85"))
DISK_MIN_SLOPE_PER_MINUTE = float(
    os.getenv("PREDICTOR_DISK_MIN_SLOPE_PER_MINUTE", "0.15")
)
DISK_HORIZON_SECONDS = int(os.getenv("PREDICTOR_DISK_HORIZON_SECONDS", "1800"))

LATENCY_THRESHOLD = float(
    os.getenv("PREDICTOR_LATENCY_THRESHOLD_SECONDS", "0.8")
)
LATENCY_MIN_SLOPE_PER_MINUTE = float(
    os.getenv("PREDICTOR_LATENCY_MIN_SLOPE_PER_MINUTE", "0.025")
)
LATENCY_HORIZON_SECONDS = int(
    os.getenv("PREDICTOR_LATENCY_HORIZON_SECONDS", "900")
)
LATENCY_PEER_RATIO = float(os.getenv("PREDICTOR_LATENCY_PEER_RATIO", "1.35"))

CERT_THRESHOLD_SECONDS = float(
    os.getenv("PREDICTOR_CERT_THRESHOLD_SECONDS", "900")
)
CERT_HORIZON_SECONDS = int(
    os.getenv("PREDICTOR_CERT_HORIZON_SECONDS", "604800")
)
MAX_EVENTS = int(os.getenv("PREDICTOR_MAX_EVENTS", "1000"))
DASHBOARD_REFRESH_SECONDS = max(
    2,
    int(os.getenv("PREDICTOR_DASHBOARD_REFRESH_SECONDS", "5")),
)

PROBLEM_REGISTER_TIMEOUT_SECONDS = float(
    os.getenv("PREDICTOR_PROBLEM_REGISTER_TIMEOUT_SECONDS", "10")
)

PREDICTIONS_RAISED = Counter(
    "opsai_predictions_raised_total",
    "Predictive events raised by type and scope.",
    ["type", "scope"],
)
PREDICTION_STATUS = Gauge(
    "opsai_prediction_active",
    "One while a prediction is active.",
    ["type", "scope", "prediction_id"],
)
PREDICTION_RISK = Gauge(
    "opsai_prediction_risk_score",
    "Latest deterministic prediction risk score.",
    ["type", "scope"],
)
PREDICTION_ETA = Gauge(
    "opsai_prediction_time_to_threshold_seconds",
    "Forecast time to the configured incident threshold.",
    ["type", "scope"],
)
AI_EXPLANATION_REQUESTS = Counter(
    "opsai_prediction_ai_explanation_requests_total",
    "AI explanation requests made after deterministic prediction creation.",
    ["type"],
)
FREQUENT_PATTERNS = Counter(
    "opsai_frequent_issue_patterns_total",
    "Frequent operational issue patterns identified.",
    ["incident_type", "scope"],
)
ISSUE_OCCURRENCES = Gauge(
    "opsai_issue_occurrences_24h",
    "Occurrences of an incident pattern inside the recurrence lookback window.",
    ["incident_type", "scope"],
)
LAST_EVALUATION = Gauge(
    "opsai_predictor_last_evaluation_timestamp_seconds",
    "Timestamp of the latest successful predictive evaluation.",
)

app = FastAPI(title="PulseGuard Predictive Analysis", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())

stop_event = asyncio.Event()
predictions: dict[str, dict[str, Any]] = {}
patterns: dict[str, dict[str, Any]] = {}
events: list[dict[str, Any]] = []
last_evaluation: dict[str, Any] = {}
live_signals: dict[str, Any] = {}
last_recurrence_evaluation = 0.0
last_ai_contact: dict[str, Any] | None = None
last_problem_sync: dict[str, Any] | None = None


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def emit_log(level: str, event: str, **fields: Any) -> None:
    print(
        json.dumps(
            {
                "timestamp": iso_now(),
                "level": level,
                "service": "opsai-predictor",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
            default=str,
        ),
        flush=True,
    )


def require_token(token: str | None) -> None:
    if token != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid internal token.")


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


async def prom_query(query: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
        )
        response.raise_for_status()
        payload = response.json()
    return payload.get("data", {}).get("result", [])


async def prom_query_range(
    query: str,
    start: float,
    end: float,
    step: int,
) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=25) as client:
        response = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={
                "query": query,
                "start": f"{start:.3f}",
                "end": f"{end:.3f}",
                "step": str(step),
            },
        )
        response.raise_for_status()
        payload = response.json()
    return payload.get("data", {}).get("result", [])


def instant_vector_map(rows: list[dict[str, Any]], label: str) -> dict[str, float]:
    result: dict[str, float] = {}
    for row in rows:
        try:
            scope = str(row.get("metric", {}).get(label) or "global")
            result[scope] = float(row.get("value", [0, 0])[1])
        except (TypeError, ValueError, IndexError):
            continue
    return result


def matrix_map(
    rows: list[dict[str, Any]],
    label: str,
) -> dict[str, list[tuple[float, float]]]:
    result: dict[str, list[tuple[float, float]]] = {}
    for row in rows:
        scope = str(row.get("metric", {}).get(label) or "global")
        samples: list[tuple[float, float]] = []
        for pair in row.get("values", []):
            try:
                timestamp = float(pair[0])
                value = float(pair[1])
                if math.isfinite(value):
                    samples.append((timestamp, value))
            except (TypeError, ValueError, IndexError):
                continue
        if samples:
            result[scope] = samples
    return result


def regression(samples: list[tuple[float, float]]) -> dict[str, float] | None:
    clean = [(float(ts), float(value)) for ts, value in samples if math.isfinite(value)]
    if len(clean) < MIN_SAMPLES:
        return None

    t0 = clean[0][0]
    xs = [item[0] - t0 for item in clean]
    ys = [item[1] for item in clean]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator <= 0:
        return None

    slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / denominator
    intercept = mean_y - slope * mean_x
    fitted = [intercept + slope * x for x in xs]
    ss_total = sum((y - mean_y) ** 2 for y in ys)
    ss_residual = sum((y - predicted) ** 2 for y, predicted in zip(ys, fitted))
    r_squared = 1.0 - ss_residual / ss_total if ss_total > 1e-9 else 1.0

    return {
        "slopePerSecond": slope,
        "slopePerMinute": slope * 60,
        "intercept": intercept,
        "rSquared": max(0.0, min(1.0, r_squared)),
        "current": ys[-1],
        "minimum": min(ys),
        "maximum": max(ys),
        "average": sum(ys) / len(ys),
        "sampleCount": float(len(ys)),
        "windowSeconds": xs[-1] - xs[0],
        "firstTimestamp": clean[0][0],
        "lastTimestamp": clean[-1][0],
    }


def time_to_threshold(model: dict[str, float], threshold: float) -> float | None:
    slope = model["slopePerSecond"]
    current = model["current"]
    if current >= threshold:
        return 0.0
    if slope <= 0:
        return None
    eta = (threshold - current) / slope
    return eta if eta >= 0 else None


def risk_score(
    current: float,
    threshold: float,
    eta: float,
    horizon: float,
    r2: float,
) -> float:
    closeness = max(0.0, min(1.0, current / max(threshold, 0.001)))
    urgency = max(0.0, min(1.0, 1.0 - eta / max(horizon, 1.0)))
    return round(min(1.0, 0.35 * closeness + 0.45 * urgency + 0.20 * r2), 3)


def compact_samples(samples: list[tuple[float, float]], limit: int = 60) -> list[dict[str, float]]:
    if len(samples) <= limit:
        selected = samples
    else:
        stride = max(1, len(samples) // limit)
        selected = samples[::stride][-limit:]
    return [
        {"timestamp": round(timestamp, 3), "value": round(value, 6)}
        for timestamp, value in selected
    ]


def live_trend(
    samples: list[tuple[float, float]],
    threshold: float | None,
    horizon_seconds: float | None,
    minimum_slope_per_minute: float | None = None,
    direction: str = "UP",
) -> dict[str, Any]:
    model = regression(samples)
    current = samples[-1][1] if samples else None
    eta: float | None = None
    state = "NO_DATA"

    if current is not None:
        state = "NORMAL"

    if model is not None and threshold is not None:
        if direction == "DOWN":
            if current is not None and current <= threshold:
                state = "THRESHOLD_BREACHED"
                eta = 0.0
            elif current is not None:
                eta = max(0.0, current - threshold)
                if horizon_seconds is not None and eta <= horizon_seconds:
                    state = "FORECAST_RISK"
        else:
            eta = time_to_threshold(model, threshold)
            if current is not None and current >= threshold:
                state = "THRESHOLD_BREACHED"
            elif (
                eta is not None
                and horizon_seconds is not None
                and 0 < eta <= horizon_seconds
                and (
                    minimum_slope_per_minute is None
                    or model["slopePerMinute"] >= minimum_slope_per_minute
                )
            ):
                state = "FORECAST_RISK"
            elif (
                minimum_slope_per_minute is not None
                and model["slopePerMinute"] >= minimum_slope_per_minute
            ):
                state = "TRENDING"

    return {
        "current": round(float(current), 6) if current is not None else None,
        "threshold": threshold,
        "state": state,
        "etaSeconds": round(float(eta)) if eta is not None else None,
        "slopePerMinute": (
            round(float(model["slopePerMinute"]), 6) if model is not None else None
        ),
        "rSquared": (
            round(float(model["rSquared"]), 4) if model is not None else None
        ),
        "sampleCount": int(model["sampleCount"]) if model is not None else len(samples),
        "samples": compact_samples(samples),
    }


ACTION_TEXT: dict[str, str] = {
    "collect_diagnostics": (
        "Collect and compare diagnostics across matching occurrences before "
        "changing service state."
    ),
    "scale_payment_capacity": (
        "Review bounded payment-capacity scaling and pass execution through "
        "deterministic governance."
    ),
    "cleanup_disk_space": (
        "Prepare archive-before-cleanup for allowlisted temporary storage; "
        "execution remains governed."
    ),
    "renew_certificate": (
        "Prepare renewal of the allowlisted certificate and verify hostname, SAN, "
        "issuer, binding and post-renewal expiry."
    ),
    "refresh_external_service_credentials": (
        "Refresh the allowlisted external-service credential and verify an "
        "authenticated probe without exposing the token."
    ),
}

PREDICTION_ACTION_POLICY: dict[str, str] = {
    "PREDICTED_DISK_PRESSURE": "cleanup_disk_space",
    "PREDICTED_PAYMENT_NODE_DEGRADATION": "collect_diagnostics",
    "PREDICTED_CAPACITY_SATURATION": "scale_payment_capacity",
    "PREDICTED_CERTIFICATE_EXPIRY": "renew_certificate",
}

INCIDENT_ACTION_POLICY: dict[str, str] = {
    "NODE_DISK_PRESSURE": "cleanup_disk_space",
    "TLS_CERTIFICATE_EXPIRING": "renew_certificate",
    "PAYMENT_FLEET_CAPACITY_DEGRADATION": "scale_payment_capacity",
    "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE": (
        "refresh_external_service_credentials"
    ),
    "PAYMENT_NODE_LATENCY": "collect_diagnostics",
    "PAYMENT_NODE_NETWORK_INSTABILITY": "collect_diagnostics",
    "PAYMENT_NODE_TIMEOUT": "collect_diagnostics",
    "PAYMENT_NODE_UNAVAILABLE": "collect_diagnostics",
    "PAYMENT_NODE_HUNG": "collect_diagnostics",
    "CHECKOUT_FAILURE_RATE": "collect_diagnostics",
    "PAYMENT_SHARED_DEPENDENCY_OUTAGE": "collect_diagnostics",
}


def governed_action(
    prediction_type: str,
    incident_type: str | None = None,
) -> str:
    if prediction_type == "FREQUENT_ISSUE_PATTERN":
        return INCIDENT_ACTION_POLICY.get(
            str(incident_type or ""),
            "collect_diagnostics",
        )
    return PREDICTION_ACTION_POLICY.get(
        prediction_type,
        "collect_diagnostics",
    )


def normalize_ai_explanation(
    raw: dict[str, Any] | None,
    *,
    prediction_type: str,
    deterministic_summary: str,
    deterministic_impact: str,
    incident_type: str | None = None,
    deterministic_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    explanation = dict(raw or {})
    model_action = str(
        explanation.get("actionName")
        or explanation.get("action_name")
        or ""
    ).strip()
    model_recommendation = str(
        explanation.get("recommendedPreventiveAction")
        or explanation.get("recommended_preventive_action")
        or ""
    ).strip()
    action_name = governed_action(prediction_type, incident_type)
    interpretation = str(
        explanation.get("likelyImpact")
        or explanation.get("likely_impact")
        or explanation.get("summary")
        or deterministic_impact
    ).strip()

    explanation["modelSummary"] = explanation.get("summary")
    explanation["modelSuggestedAction"] = model_action or None
    explanation["modelSuggestedRecommendation"] = model_recommendation or None
    explanation["summary"] = deterministic_summary
    explanation["operationalInterpretation"] = interpretation or deterministic_impact
    explanation["likelyImpact"] = deterministic_impact
    explanation["actionName"] = action_name
    explanation["recommendedPreventiveAction"] = ACTION_TEXT[action_name]
    explanation["recommendationPolicy"] = "TYPE_SCOPED_ALLOWLIST"
    explanation["recommendationValidation"] = (
        "ALLOWLIST_CONFIRMED"
        if model_action == action_name
        else "ALLOWLIST_CORRECTED"
    )
    explanation["factsSource"] = "DETERMINISTIC_CALCULATION"
    explanation["deterministicContext"] = deterministic_context or {}
    explanation["authorised"] = False
    explanation["executed"] = False
    explanation.setdefault("generatedAt", iso_now())
    return explanation


def walk_dicts(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk_dicts(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk_dicts(child)


def incident_origin(incident: dict[str, Any]) -> str:
    evidence = incident.get("evidence") or {}
    explicit = str(
        incident.get("origin")
        or evidence.get("origin")
        or evidence.get("incidentOrigin")
        or ""
    ).strip().upper()

    if explicit in {"ORGANIC", "PRODUCTION", "LIVE"}:
        return "ORGANIC"
    if explicit in {"SYNTHETIC", "SYNTHETIC_TEST", "TEST", "SCENARIO"}:
        return "SYNTHETIC_TEST"

    searchable = " ".join(
        [
            str(incident.get("service") or ""),
            str(incident.get("node") or ""),
            str(evidence.get("metric") or ""),
        ]
    ).lower()
    if "opsai-demo" in searchable or "opsai_demo" in searchable:
        return "SYNTHETIC_TEST"

    for item in walk_dicts(evidence):
        if item.get("synthetic") is True or item.get("capacitySimulation") is True:
            return "SYNTHETIC_TEST"
        if item.get("scenarioId") or item.get("testRunId"):
            return "SYNTHETIC_TEST"
        if str(item.get("source") or "").lower() in {
            "scenario-controller",
            "synthetic",
            "test",
        }:
            return "SYNTHETIC_TEST"

    return "UNCLASSIFIED"


async def ai_explanation(
    prediction: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    global last_ai_contact

    contact = {
        "contactedAt": iso_now(),
        "predictionId": prediction.get("predictionId"),
        "predictionType": prediction.get("predictionType"),
        "scope": prediction.get("scope"),
        "reason": "A new deterministic prediction or recurrent pattern crossed its configured trigger.",
        "endpoint": "/api/predictions/explain",
        "continuousStreaming": False,
        "payloadSummary": {
            "riskScore": prediction.get("riskScore"),
            "confidence": prediction.get("confidence"),
            "timeToThresholdSeconds": prediction.get("timeToThresholdSeconds"),
            "sampleCount": (prediction.get("trend") or {}).get("sampleCount"),
            "threshold": prediction.get("threshold"),
        },
    }
    last_ai_contact = contact
    AI_EXPLANATION_REQUESTS.labels(
        type=str(prediction.get("predictionType") or "unknown")
    ).inc()
    events.append(
        {
            "eventId": str(uuid.uuid4()),
            "timestamp": contact["contactedAt"],
            "stage": "REAL_AI_CONTACTED",
            "predictionId": prediction.get("predictionId"),
            "predictionType": prediction.get("predictionType"),
            "scope": prediction.get("scope"),
            "message": (
                "Deterministic trigger satisfied. A compact evidence package was "
                "sent once to the real AI agent for explanation and recommendation."
            ),
            "details": contact,
        }
    )
    del events[:-MAX_EVENTS]

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{OPSAI_AGENT_URL}/api/predictions/explain",
                json={"prediction": prediction, "evidence": evidence},
            )
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        emit_log(
            "WARNING",
            "prediction_ai_explanation_failed",
            predictionId=prediction.get("predictionId"),
            error=str(exc),
        )
        return {
            "analysisMode": "DETERMINISTIC_FALLBACK",
            "summary": prediction.get("summary"),
            "likelyImpact": prediction.get("expectedImpact"),
            "contributingFactors": prediction.get("supportingSignals", []),
            "confidence": prediction.get("confidence", 0.5),
            "recommendedPreventiveAction": (
                "Continue observation and collect diagnostics."
            ),
            "actionName": "collect_diagnostics",
            "authorised": False,
            "executed": False,
            "generatedAt": iso_now(),
            "fallbackReason": str(exc),
        }


def prediction_key(prediction_type: str, scope: str) -> str:
    return f"{prediction_type}:{scope}"


async def upsert_prediction(
    *,
    prediction_type: str,
    scope: str,
    summary: str,
    expected_impact: str,
    current_value: float,
    threshold: float,
    eta_seconds: float,
    horizon_seconds: float,
    model: dict[str, float],
    supporting_signals: list[str],
    evidence: dict[str, Any],
    calculation: dict[str, Any],
) -> dict[str, Any]:
    key = prediction_key(prediction_type, scope)
    score = risk_score(
        current_value,
        threshold,
        eta_seconds,
        horizon_seconds,
        model.get("rSquared", 0.0),
    )
    confidence = round(
        min(
            0.98,
            max(
                0.55,
                0.45
                + 0.35 * model.get("rSquared", 0.0)
                + 0.02 * min(model.get("sampleCount", 0.0), 10),
            ),
        ),
        3,
    )

    existing = predictions.get(key)
    created = existing is None or existing.get("status") in {
        "RISK_REDUCED",
        "EXPIRED",
        "INCIDENT_OCCURRED",
    }

    prediction = {
        "predictionId": str(uuid.uuid4()) if created else existing.get("predictionId"),
        "predictionType": prediction_type,
        "scope": scope,
        "status": "PREDICTED",
        "summary": summary,
        "expectedImpact": expected_impact,
        "riskScore": score,
        "confidence": confidence,
        "currentValue": round(current_value, 6),
        "threshold": threshold,
        "timeToThresholdSeconds": round(eta_seconds),
        "expectedImpactAt": datetime.fromtimestamp(
            time.time() + eta_seconds,
            tz=timezone.utc,
        ).isoformat(),
        "trend": model,
        "supportingSignals": supporting_signals,
        "evidence": evidence,
        "calculation": calculation,
        "dataSource": {
            "type": "PROMETHEUS_RANGE_QUERY",
            "windowSeconds": RANGE_WINDOW_SECONDS,
            "stepSeconds": RANGE_STEP_SECONDS,
            "minimumSamples": MIN_SAMPLES,
            "continuousAiStreaming": False,
        },
        "aiContact": {
            "triggered": bool(created),
            "triggerReason": (
                "New deterministic prediction crossed all configured conditions."
                if created
                else "Existing prediction updated numerically; AI was not contacted again."
            ),
        },
        "firstPredictedAt": iso_now() if created else existing.get("firstPredictedAt"),
        "updatedAt": iso_now(),
        "aiExplanation": None if created else existing.get("aiExplanation"),
        "preventiveActionExecuted": False,
    }

    predictions[key] = prediction
    PREDICTION_STATUS.labels(
        type=prediction_type,
        scope=scope,
        prediction_id=prediction["predictionId"],
    ).set(1)
    PREDICTION_RISK.labels(type=prediction_type, scope=scope).set(score)
    PREDICTION_ETA.labels(type=prediction_type, scope=scope).set(eta_seconds)

    if created:
        PREDICTIONS_RAISED.labels(type=prediction_type, scope=scope).inc()
        event = {
            "eventId": str(uuid.uuid4()),
            "timestamp": iso_now(),
            "stage": "PREDICTION_RAISED",
            "predictionId": prediction["predictionId"],
            "predictionType": prediction_type,
            "scope": scope,
            "riskScore": score,
            "confidence": confidence,
            "timeToThresholdSeconds": round(eta_seconds),
            "message": summary,
            "details": calculation,
        }
        events.append(event)
        del events[:-MAX_EVENTS]
        emit_log("WARNING", "prediction_raised", **event)
        raw_explanation = await ai_explanation(prediction, evidence)
        prediction["aiExplanation"] = normalize_ai_explanation(
            raw_explanation,
            prediction_type=prediction_type,
            deterministic_summary=summary,
            deterministic_impact=expected_impact,
            deterministic_context={
                "riskScoreAtContact": score,
                "currentValueAtContact": round(current_value, 6),
                "threshold": threshold,
                "timeToThresholdSecondsAtContact": round(eta_seconds),
                "sampleCountAtContact": int(model.get("sampleCount", 0)),
            },
        )
        prediction["aiContact"]["contactedAt"] = (
            prediction["aiExplanation"].get("generatedAt") or iso_now()
        )
        prediction["aiContact"]["analysisMode"] = prediction[
            "aiExplanation"
        ].get("analysisMode")
        events.append(
            {
                "eventId": str(uuid.uuid4()),
                "timestamp": iso_now(),
                "stage": "PREDICTION_EXPLAINED",
                "predictionId": prediction["predictionId"],
                "predictionType": prediction_type,
                "scope": scope,
                "message": prediction["aiExplanation"].get("summary"),
                "analysisMode": prediction["aiExplanation"].get("analysisMode"),
            }
        )
        del events[:-MAX_EVENTS]

    return prediction


def reduce_prediction(prediction_type: str, scope: str, reason: str) -> None:
    key = prediction_key(prediction_type, scope)
    prediction = predictions.get(key)
    if not prediction or prediction.get("status") != "PREDICTED":
        return
    prediction["status"] = "RISK_REDUCED"
    prediction["resolvedAt"] = iso_now()
    prediction["resolutionReason"] = reason
    PREDICTION_STATUS.labels(
        type=prediction_type,
        scope=scope,
        prediction_id=prediction["predictionId"],
    ).set(0)
    events.append(
        {
            "eventId": str(uuid.uuid4()),
            "timestamp": iso_now(),
            "stage": "RISK_REDUCED",
            "predictionId": prediction["predictionId"],
            "predictionType": prediction_type,
            "scope": scope,
            "message": reason,
        }
    )
    del events[:-MAX_EVENTS]


async def fetch_incidents() -> list[dict[str, Any]]:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{OPSAI_CORE_URL}/incidents",
                params={"status": "all", "limit": 500},
            )
            response.raise_for_status()
            payload = response.json()
        rows = payload.get("incidents", [])
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        emit_log("WARNING", "recurrence_incident_fetch_failed", error=str(exc))
        return []


def incident_scope(incident: dict[str, Any]) -> str:
    node = str(incident.get("node") or "").strip()
    if node:
        return node
    service = str(incident.get("service") or "").strip()
    return service or "global"



PROBLEM_CATEGORY_RULES: dict[str, tuple[str, str, str]] = {
    "PAYMENT_NODE_LATENCY": (
        "payment-platform-latency-capacity",
        "Payment platform latency and capacity degradation",
        (
            "Recurring latency and capacity symptoms may share a traffic, "
            "redundancy or processing-capacity cause."
        ),
    ),
    "PAYMENT_FLEET_CAPACITY_DEGRADATION": (
        "payment-platform-latency-capacity",
        "Payment platform latency and capacity degradation",
        (
            "Recurring latency and capacity symptoms may share a traffic, "
            "redundancy or processing-capacity cause."
        ),
    ),
    "CHECKOUT_FAILURE_RATE": (
        "payment-platform-latency-capacity",
        "Payment platform latency and capacity degradation",
        (
            "Recurring latency and capacity symptoms may share a traffic, "
            "redundancy or processing-capacity cause."
        ),
    ),
    "PAYMENT_NODE_TIMEOUT": (
        "payment-node-dependency-reliability",
        "Payment-node and dependency reliability instability",
        (
            "Repeated timeout, availability or dependency symptoms may indicate "
            "an unresolved node, network or shared-dependency reliability issue."
        ),
    ),
    "PAYMENT_NODE_NETWORK_INSTABILITY": (
        "payment-node-dependency-reliability",
        "Payment-node and dependency reliability instability",
        (
            "Repeated timeout, availability or dependency symptoms may indicate "
            "an unresolved node, network or shared-dependency reliability issue."
        ),
    ),
    "PAYMENT_NODE_UNAVAILABLE": (
        "payment-node-dependency-reliability",
        "Payment-node and dependency reliability instability",
        (
            "Repeated timeout, availability or dependency symptoms may indicate "
            "an unresolved node, network or shared-dependency reliability issue."
        ),
    ),
    "PAYMENT_NODE_HUNG": (
        "payment-node-dependency-reliability",
        "Payment-node and dependency reliability instability",
        (
            "Repeated timeout, availability or dependency symptoms may indicate "
            "an unresolved node, network or shared-dependency reliability issue."
        ),
    ),
    "PAYMENT_SHARED_DEPENDENCY_OUTAGE": (
        "payment-node-dependency-reliability",
        "Payment-node and dependency reliability instability",
        (
            "Repeated timeout, availability or dependency symptoms may indicate "
            "an unresolved node, network or shared-dependency reliability issue."
        ),
    ),
    "NODE_DISK_PRESSURE": (
        "storage-exhaustion-risk",
        "Storage exhaustion risk",
        (
            "Repeated disk-pressure incidents may indicate uncontrolled growth, "
            "insufficient retention or ineffective cleanup."
        ),
    ),
    "TLS_CERTIFICATE_EXPIRING": (
        "certificate-lifecycle-risk",
        "Certificate lifecycle risk",
        (
            "Repeated expiry warnings may indicate unreliable renewal scheduling "
            "or certificate binding."
        ),
    ),
    "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE": (
        "external-credential-reliability",
        "External dependency credential reliability",
        (
            "Repeated authentication failures may indicate token rotation or "
            "credential-synchronisation problems."
        ),
    ),
}


def problem_category(
    incident_type: str,
) -> tuple[str, str, str]:
    return PROBLEM_CATEGORY_RULES.get(
        incident_type,
        (
            f"recurring-{incident_type.lower().replace('_', '-')}",
            f"Recurring {incident_type}",
            "Repeated matching incidents may indicate an unresolved root cause.",
        ),
    )


def recurrence_pattern_risk(pattern: dict[str, Any]) -> dict[str, Any]:
    severities = {
        str(value).lower() for value in pattern.get("severities", [])
    }
    severity_weight = (
        22
        if "critical" in severities
        else 16
        if "high" in severities
        else 9
        if "medium" in severities
        else 0
    )
    occurrences = int(pattern.get("occurrences") or 0)
    threshold = int(pattern.get("threshold") or RECURRENCE_THRESHOLD)
    recurrence_weight = min(
        38,
        12 * max(1, occurrences - threshold + 1),
    )
    interval = float(pattern.get("averageIntervalSeconds") or 0)
    interval_weight = (
        20
        if interval and interval <= 3600
        else 13
        if interval and interval <= 14400
        else 7
        if interval and interval <= 43200
        else 0
    )
    score = min(100, 25 + severity_weight + recurrence_weight + interval_weight)
    return {
        "score": score,
        "level": "HIGH" if score >= 75 else "MEDIUM" if score >= 50 else "LOW",
    }


def build_problem_candidates() -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for pattern in patterns.values():
        if pattern.get("status") != "ACTIVE":
            continue
        incident_type = str(pattern.get("incidentType") or "UNKNOWN")
        category_key, title, hypothesis = problem_category(incident_type)
        risk = recurrence_pattern_risk(pattern)
        candidate = grouped.setdefault(
            category_key,
            {
                "problemKey": category_key,
                "title": title,
                "category": category_key,
                "summary": (
                    f"Repeated incident patterns indicate a potential "
                    f"{title.lower()} problem."
                ),
                "hypothesis": hypothesis,
                "riskScore": 0,
                "riskLevel": "LOW",
                "occurrenceCount": 0,
                "averageIntervals": [],
                "firstOccurrenceAt": None,
                "latestOccurrenceAt": None,
                "incidentTypes": set(),
                "scopes": set(),
                "linkedIncidentIds": set(),
                "originBreakdown": defaultdict(int),
                "patternEvidence": [],
            },
        )
        candidate["riskScore"] = max(
            int(candidate["riskScore"]),
            int(risk["score"]),
        )
        candidate["riskLevel"] = (
            "HIGH"
            if candidate["riskScore"] >= 75
            else "MEDIUM"
            if candidate["riskScore"] >= 50
            else "LOW"
        )
        candidate["occurrenceCount"] += int(pattern.get("occurrences") or 0)
        interval = pattern.get("averageIntervalSeconds")
        if interval is not None:
            candidate["averageIntervals"].append(float(interval))
        first_at = parse_timestamp(pattern.get("firstOccurrenceAt"))
        latest_at = parse_timestamp(pattern.get("latestOccurrenceAt"))
        current_first = parse_timestamp(candidate.get("firstOccurrenceAt"))
        current_latest = parse_timestamp(candidate.get("latestOccurrenceAt"))
        if first_at and (current_first is None or first_at < current_first):
            candidate["firstOccurrenceAt"] = first_at.isoformat()
        if latest_at and (current_latest is None or latest_at > current_latest):
            candidate["latestOccurrenceAt"] = latest_at.isoformat()
        candidate["incidentTypes"].add(incident_type)
        candidate["scopes"].add(str(pattern.get("scope") or "global"))
        candidate["linkedIncidentIds"].update(
            str(value) for value in pattern.get("incidentIds", []) if value
        )
        for origin, count in (pattern.get("originBreakdown") or {}).items():
            candidate["originBreakdown"][str(origin)] += int(count or 0)
        candidate["patternEvidence"].append(
            {
                "patternId": pattern.get("patternId"),
                "incidentType": incident_type,
                "scope": pattern.get("scope"),
                "occurrences": pattern.get("occurrences"),
                "threshold": pattern.get("threshold"),
                "averageIntervalSeconds": pattern.get("averageIntervalSeconds"),
                "originBreakdown": pattern.get("originBreakdown") or {},
            }
        )

    rows: list[dict[str, Any]] = []
    for candidate in grouped.values():
        origins = dict(candidate["originBreakdown"])
        organic_count = int(origins.get("ORGANIC") or 0)
        unclassified_count = int(origins.get("UNCLASSIFIED") or 0)
        record_class = (
            "OPERATIONAL_CANDIDATE"
            if organic_count > 0
            else "REVIEW_REQUIRED"
            if unclassified_count > 0
            else "DEMO_CANDIDATE"
        )
        intervals = candidate.pop("averageIntervals")
        evidence = {
            "source": "opsai-predictor",
            "calculation": (
                "Related frequent-incident patterns are grouped by problem category. "
                "The highest deterministic pattern risk becomes the candidate risk."
            ),
            "patterns": candidate.pop("patternEvidence"),
            "lookbackHours": RECURRENCE_LOOKBACK_HOURS,
            "recurrenceThreshold": RECURRENCE_THRESHOLD,
        }
        rows.append(
            {
                **candidate,
                "recordClass": record_class,
                "averageIntervalSeconds": (
                    round(sum(intervals) / len(intervals))
                    if intervals
                    else None
                ),
                "incidentTypes": sorted(candidate["incidentTypes"]),
                "scopes": sorted(candidate["scopes"]),
                "linkedIncidentIds": sorted(candidate["linkedIncidentIds"]),
                "originBreakdown": origins,
                "evidence": evidence,
            }
        )
    rows.sort(
        key=lambda item: (
            -int(item.get("riskScore") or 0),
            -int(item.get("occurrenceCount") or 0),
            str(item.get("title") or ""),
        )
    )
    return rows


async def sync_problem_register() -> None:
    global last_problem_sync

    candidates = build_problem_candidates()
    synced = 0
    errors: list[dict[str, str]] = []
    async with httpx.AsyncClient(
        timeout=PROBLEM_REGISTER_TIMEOUT_SECONDS
    ) as client:
        for candidate in candidates:
            try:
                response = await client.post(
                    f"{OPSAI_CORE_URL}/api/problems/candidates/upsert",
                    headers={"X-OpsAI-Automation-Token": INTERNAL_TOKEN},
                    json=candidate,
                )
                response.raise_for_status()
                synced += 1
            except Exception as exc:
                errors.append(
                    {
                        "problemKey": str(candidate.get("problemKey")),
                        "error": str(exc),
                    }
                )
    last_problem_sync = {
        "attemptedAt": iso_now(),
        "candidateCount": len(candidates),
        "synced": synced,
        "errors": errors,
    }
    emit_log(
        "INFO" if not errors else "WARNING",
        "problem_register_sync",
        candidateCount=len(candidates),
        synced=synced,
        errors=errors,
    )


async def evaluate_recurrence_patterns(now: float) -> None:
    global last_recurrence_evaluation

    if now - last_recurrence_evaluation < RECURRENCE_EVALUATION_SECONDS:
        return
    last_recurrence_evaluation = now

    incidents = await fetch_incidents()
    cutoff = utc_now() - timedelta(hours=RECURRENCE_LOOKBACK_HOURS)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)

    for incident in incidents:
        opened_at = parse_timestamp(incident.get("opened_at"))
        if opened_at is None or opened_at < cutoff:
            continue
        incident_type = str(incident.get("incident_type") or "UNKNOWN")
        scope = incident_scope(incident)
        grouped[(incident_type, scope)].append(incident)

    active_pattern_keys: set[str] = set()
    for (incident_type, scope), rows in grouped.items():
        count = len(rows)
        ISSUE_OCCURRENCES.labels(
            incident_type=incident_type,
            scope=scope,
        ).set(count)

        if count < RECURRENCE_THRESHOLD:
            continue

        key = f"{incident_type}:{scope}"
        active_pattern_keys.add(key)
        timestamps = sorted(
            [
                parsed
                for parsed in (
                    parse_timestamp(row.get("opened_at")) for row in rows
                )
                if parsed is not None
            ]
        )
        intervals = [
            (right - left).total_seconds()
            for left, right in zip(timestamps, timestamps[1:])
        ]
        average_interval = (
            sum(intervals) / len(intervals) if intervals else None
        )
        severities = sorted(
            {str(row.get("severity") or "unknown") for row in rows}
        )
        origin_breakdown: dict[str, int] = defaultdict(int)
        for row in rows:
            origin_breakdown[incident_origin(row)] += 1
        existing = patterns.get(key)
        created = existing is None or existing.get("status") != "ACTIVE"

        pattern = {
            "patternId": str(uuid.uuid4()) if created else existing.get("patternId"),
            "patternType": "FREQUENT_ISSUE_PATTERN",
            "incidentType": incident_type,
            "scope": scope,
            "status": "ACTIVE",
            "occurrences": count,
            "threshold": RECURRENCE_THRESHOLD,
            "lookbackHours": RECURRENCE_LOOKBACK_HOURS,
            "firstOccurrenceAt": timestamps[0].isoformat() if timestamps else None,
            "latestOccurrenceAt": timestamps[-1].isoformat() if timestamps else None,
            "averageIntervalSeconds": (
                round(average_interval) if average_interval is not None else None
            ),
            "severities": severities,
            "incidentIds": [str(row.get("id")) for row in rows[-10:]],
            "originBreakdown": dict(origin_breakdown),
            "calculation": {
                "formula": (
                    "Count incidents with the same incident type and scope "
                    f"inside the last {RECURRENCE_LOOKBACK_HOURS} hours."
                ),
                "observedCount": count,
                "triggerThreshold": RECURRENCE_THRESHOLD,
                "condition": f"{count} >= {RECURRENCE_THRESHOLD}",
                "averageIntervalSeconds": (
                    round(average_interval) if average_interval is not None else None
                ),
            },
            "summary": (
                f"{incident_type} occurred {count} times for {scope} "
                f"inside {RECURRENCE_LOOKBACK_HOURS} hours."
            ),
            "expectedImpact": (
                "Repeated operational symptoms may indicate an underlying "
                "degradation, inadequate capacity, unstable dependency or "
                "unresolved root cause."
            ),
            "updatedAt": iso_now(),
            "firstDetectedAt": iso_now() if created else existing.get("firstDetectedAt"),
            "aiExplanation": None if created else existing.get("aiExplanation"),
            "aiContact": {
                "triggered": created,
                "triggerReason": (
                    "The recurrence count crossed the configured frequency threshold."
                    if created
                    else "The pattern count was refreshed; AI was not contacted again."
                ),
            },
        }
        patterns[key] = pattern

        if created:
            FREQUENT_PATTERNS.labels(
                incident_type=incident_type,
                scope=scope,
            ).inc()
            pseudo_prediction = {
                "predictionId": pattern["patternId"],
                "predictionType": "FREQUENT_ISSUE_PATTERN",
                "scope": scope,
                "summary": pattern["summary"],
                "expectedImpact": pattern["expectedImpact"],
                "riskScore": min(1.0, 0.55 + 0.12 * (count - RECURRENCE_THRESHOLD + 1)),
                "confidence": min(0.95, 0.65 + 0.05 * count),
                "currentValue": count,
                "threshold": RECURRENCE_THRESHOLD,
                "timeToThresholdSeconds": 0,
                "trend": {
                    "sampleCount": float(count),
                    "rSquared": 1.0,
                    "windowSeconds": RECURRENCE_LOOKBACK_HOURS * 3600,
                },
                "supportingSignals": [
                    pattern["summary"],
                    (
                        f"Average interval is approximately "
                        f"{round(average_interval / 3600, 1)} hours."
                        if average_interval
                        else "Multiple matching incidents occurred in the lookback window."
                    ),
                ],
            }
            evidence = {
                "pattern": pattern,
                "incidents": [
                    {
                        "id": row.get("id"),
                        "incidentType": row.get("incident_type"),
                        "scope": incident_scope(row),
                        "openedAt": row.get("opened_at"),
                        "resolvedAt": row.get("resolved_at"),
                        "severity": row.get("severity"),
                        "resolutionReason": row.get("resolution_reason"),
                        "origin": incident_origin(row),
                    }
                    for row in rows[-10:]
                ],
            }
            raw_explanation = await ai_explanation(
                pseudo_prediction,
                evidence,
            )
            pattern["aiExplanation"] = normalize_ai_explanation(
                raw_explanation,
                prediction_type="FREQUENT_ISSUE_PATTERN",
                incident_type=incident_type,
                deterministic_summary=pattern["summary"],
                deterministic_impact=pattern["expectedImpact"],
                deterministic_context={
                    "incidentType": incident_type,
                    "scope": scope,
                    "occurrencesAtContact": count,
                    "threshold": RECURRENCE_THRESHOLD,
                    "lookbackHours": RECURRENCE_LOOKBACK_HOURS,
                    "originBreakdown": dict(origin_breakdown),
                },
            )
            pattern["aiContact"]["occurrencesAtContact"] = count
            pattern["aiContact"]["contactedAt"] = (
                pattern["aiExplanation"].get("generatedAt") or iso_now()
            )
            pattern["aiContact"]["analysisMode"] = pattern[
                "aiExplanation"
            ].get("analysisMode")
            events.append(
                {
                    "eventId": str(uuid.uuid4()),
                    "timestamp": iso_now(),
                    "stage": "FREQUENT_PATTERN_IDENTIFIED",
                    "predictionId": pattern["patternId"],
                    "predictionType": "FREQUENT_ISSUE_PATTERN",
                    "scope": scope,
                    "message": pattern["summary"],
                    "analysisMode": pattern["aiExplanation"].get("analysisMode"),
                }
            )
            del events[:-MAX_EVENTS]

    for key, pattern in patterns.items():
        if pattern.get("status") == "ACTIVE" and key not in active_pattern_keys:
            pattern["status"] = "NO_LONGER_FREQUENT"
            pattern["resolvedAt"] = iso_now()
            pattern["resolutionReason"] = (
                "The issue count fell below the configured recurrence threshold."
            )


    await sync_problem_register()


def calculation_catalog() -> list[dict[str, Any]]:
    return [
        {
            "predictionType": "PREDICTED_DISK_PRESSURE",
            "metric": "opsai_demo_disk_usage_percent",
            "prometheusQuery": "opsai_demo_disk_usage_percent",
            "windowMinutes": round(RANGE_WINDOW_SECONDS / 60),
            "stepSeconds": RANGE_STEP_SECONDS,
            "minimumSamples": MIN_SAMPLES,
            "calculation": (
                "Linear regression over Prometheus range samples; "
                "ETA = (threshold - current) / positive slope."
            ),
            "threshold": DISK_THRESHOLD,
            "horizonMinutes": round(DISK_HORIZON_SECONDS / 60),
            "additionalTrigger": (
                f"slope >= {DISK_MIN_SLOPE_PER_MINUTE} percentage points/minute"
            ),
            "aiContactRule": (
                "Contact AI once when ETA is positive, inside the horizon, "
                "the slope trigger passes and current usage is below the incident threshold."
            ),
        },
        {
            "predictionType": "PREDICTED_PAYMENT_NODE_DEGRADATION",
            "metric": "payment processing p95 by node",
            "prometheusQuery": (
                "histogram_quantile(0.95, sum by (le,node) "
                "(rate(opsai_payment_processing_duration_seconds_bucket[1m])))"
            ),
            "windowMinutes": round(RANGE_WINDOW_SECONDS / 60),
            "stepSeconds": RANGE_STEP_SECONDS,
            "minimumSamples": MIN_SAMPLES,
            "calculation": (
                "Linear regression per node plus peer comparison; "
                "ETA = (threshold - current) / positive slope."
            ),
            "threshold": LATENCY_THRESHOLD,
            "horizonMinutes": round(LATENCY_HORIZON_SECONDS / 60),
            "additionalTrigger": (
                f"slope >= {LATENCY_MIN_SLOPE_PER_MINUTE} seconds/minute and "
                f"node/peer ratio >= {LATENCY_PEER_RATIO}"
            ),
            "aiContactRule": (
                "Contact AI once when an isolated node is forecast to cross "
                "the latency threshold within the configured horizon."
            ),
        },
        {
            "predictionType": "PREDICTED_CAPACITY_SATURATION",
            "metric": "peer processing p95, node availability and capacity units",
            "prometheusQuery": (
                "Range query for peer processing p95 plus instant availability "
                "and capacity checks."
            ),
            "windowMinutes": round(RANGE_WINDOW_SECONDS / 60),
            "stepSeconds": RANGE_STEP_SECONDS,
            "minimumSamples": MIN_SAMPLES,
            "calculation": (
                "Forecast both remaining peers toward the latency threshold "
                "while node 3 is unavailable and both peers remain at one unit."
            ),
            "threshold": LATENCY_THRESHOLD,
            "horizonMinutes": round(LATENCY_HORIZON_SECONDS / 60),
            "additionalTrigger": "node 3 unavailable; node 1 and node 2 capacity <= 1",
            "aiContactRule": (
                "Contact AI once when both remaining peers are forecast to "
                "breach the threshold while redundancy is reduced."
            ),
        },
        {
            "predictionType": "PREDICTED_CERTIFICATE_EXPIRY",
            "metric": "opsai_demo_certificate_expiry_seconds",
            "prometheusQuery": "opsai_demo_certificate_expiry_seconds",
            "windowMinutes": round(RANGE_WINDOW_SECONDS / 60),
            "stepSeconds": RANGE_STEP_SECONDS,
            "minimumSamples": 1,
            "calculation": (
                "Countdown-based forecast: ETA to renewal incident threshold "
                "= expires-in seconds - renewal threshold seconds."
            ),
            "threshold": CERT_THRESHOLD_SECONDS,
            "horizonMinutes": round(CERT_HORIZON_SECONDS / 60),
            "additionalTrigger": "certificate is inside the configured renewal horizon",
            "aiContactRule": (
                "Contact AI once when the certificate enters the renewal horizon."
            ),
        },
        {
            "predictionType": "FREQUENT_ISSUE_PATTERN",
            "metric": "PulseGuard Core incident history",
            "prometheusQuery": "Not applicable; uses incident records.",
            "windowMinutes": RECURRENCE_LOOKBACK_HOURS * 60,
            "stepSeconds": RECURRENCE_EVALUATION_SECONDS,
            "minimumSamples": RECURRENCE_THRESHOLD,
            "calculation": (
                "Group incidents by incident type and scope, then count matching "
                "occurrences inside the rolling lookback window."
            ),
            "threshold": RECURRENCE_THRESHOLD,
            "horizonMinutes": RECURRENCE_LOOKBACK_HOURS * 60,
            "additionalTrigger": (
                f"occurrence count >= {RECURRENCE_THRESHOLD} in "
                f"{RECURRENCE_LOOKBACK_HOURS} hours"
            ),
            "aiContactRule": (
                "Contact AI once when the recurrence count first crosses the threshold."
            ),
        },
    ]


async def evaluate() -> dict[str, Any]:
    now = time.time()
    start = now - RANGE_WINDOW_SECONDS

    disk_query = "opsai_demo_disk_usage_percent"
    cert_query = "opsai_demo_certificate_expiry_seconds"
    processing_query = (
        "histogram_quantile(0.95, sum by (le,node) "
        "(rate(opsai_payment_processing_duration_seconds_bucket[1m])))"
    )
    throughput_query = "sum(rate(opsai_checkout_requests_total[1m]))"

    (
        disk_range_rows,
        cert_range_rows,
        processing_range_rows,
        throughput_range_rows,
        fault_rows,
        capacity_rows,
    ) = await asyncio.gather(
        prom_query_range(disk_query, start, now, RANGE_STEP_SECONDS),
        prom_query_range(cert_query, start, now, RANGE_STEP_SECONDS),
        prom_query_range(processing_query, start, now, RANGE_STEP_SECONDS),
        prom_query_range(throughput_query, start, now, RANGE_STEP_SECONDS),
        prom_query('opsai_payment_fault_mode_info{mode="unavailable"}'),
        prom_query("opsai_payment_capacity_units"),
    )

    disk_series = matrix_map(disk_range_rows, "none").get("global", [])
    cert_series = matrix_map(cert_range_rows, "hostname")
    processing_series = matrix_map(processing_range_rows, "node")
    throughput_series = matrix_map(throughput_range_rows, "none").get("global", [])

    disk = disk_series[-1][1] if disk_series else 0.0
    certs = {
        hostname: samples[-1][1]
        for hostname, samples in cert_series.items()
        if samples
    }
    processing = {
        node: samples[-1][1]
        for node, samples in processing_series.items()
        if samples
    }
    throughput = throughput_series[-1][1] if throughput_series else 0.0
    unavailable = instant_vector_map(fault_rows, "node")
    capacities = instant_vector_map(capacity_rows, "node")

    active_keys: set[str] = set()

    disk_model = regression(disk_series)
    if disk_model:
        eta = time_to_threshold(disk_model, DISK_THRESHOLD)
        trigger_passed = (
            eta is not None
            and 0 < eta <= DISK_HORIZON_SECONDS
            and disk_model["slopePerMinute"] >= DISK_MIN_SLOPE_PER_MINUTE
            and disk < DISK_THRESHOLD
        )
        if trigger_passed:
            await upsert_prediction(
                prediction_type="PREDICTED_DISK_PRESSURE",
                scope="opsai-demo-storage",
                summary="Application storage is forecast to cross the critical threshold.",
                expected_impact="Writes, logging and application stability may be affected.",
                current_value=disk,
                threshold=DISK_THRESHOLD,
                eta_seconds=float(eta),
                horizon_seconds=DISK_HORIZON_SECONDS,
                model=disk_model,
                supporting_signals=[
                    f"Disk usage is {disk:.1f}%.",
                    (
                        "Growth rate is "
                        f"{disk_model['slopePerMinute']:.2f} percentage points/minute."
                    ),
                    (
                        "Forecast threshold crossing in approximately "
                        f"{max(1, round(float(eta) / 60))} minutes."
                    ),
                ],
                evidence={
                    "metric": disk_query,
                    "rangeStart": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "rangeEnd": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    "stepSeconds": RANGE_STEP_SECONDS,
                    "samples": compact_samples(disk_series),
                    "model": disk_model,
                },
                calculation={
                    "formula": "ETA = (threshold - current) / slope",
                    "current": round(disk, 4),
                    "threshold": DISK_THRESHOLD,
                    "slopePerMinute": round(disk_model["slopePerMinute"], 6),
                    "rSquared": round(disk_model["rSquared"], 4),
                    "etaSeconds": round(float(eta)),
                    "horizonSeconds": DISK_HORIZON_SECONDS,
                    "sampleCount": int(disk_model["sampleCount"]),
                    "allConditionsPassed": True,
                },
            )
            active_keys.add(
                prediction_key("PREDICTED_DISK_PRESSURE", "opsai-demo-storage")
            )

    for node, samples in processing_series.items():
        value = processing.get(node, 0.0)
        model = regression(samples)
        if not model:
            continue
        eta = time_to_threshold(model, LATENCY_THRESHOLD)
        peer_values = [
            peer_value
            for peer_node, peer_value in processing.items()
            if peer_node != node and peer_value > 0
        ]
        peer_average = sum(peer_values) / len(peer_values) if peer_values else 0.0
        node_peer_ratio = value / max(peer_average, 0.001) if peer_average > 0 else 0.0
        isolated = peer_average > 0 and node_peer_ratio >= LATENCY_PEER_RATIO
        trigger_passed = (
            eta is not None
            and 0 < eta <= LATENCY_HORIZON_SECONDS
            and model["slopePerMinute"] >= LATENCY_MIN_SLOPE_PER_MINUTE
            and value < LATENCY_THRESHOLD
            and isolated
        )
        if trigger_passed:
            await upsert_prediction(
                prediction_type="PREDICTED_PAYMENT_NODE_DEGRADATION",
                scope=node,
                summary=f"{node} is trending toward the processing-latency threshold.",
                expected_impact=(
                    "Payments routed to the node may slow before a reactive incident is raised."
                ),
                current_value=value,
                threshold=LATENCY_THRESHOLD,
                eta_seconds=float(eta),
                horizon_seconds=LATENCY_HORIZON_SECONDS,
                model=model,
                supporting_signals=[
                    f"Processing p95 is {value:.3f} seconds.",
                    (
                        f"Trend is increasing by {model['slopePerMinute']:.3f} "
                        "seconds/minute."
                    ),
                    f"Peer average is {peer_average:.3f} seconds.",
                    f"Node-to-peer ratio is {node_peer_ratio:.2f}.",
                ],
                evidence={
                    "metric": processing_query,
                    "node": node,
                    "rangeStart": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "rangeEnd": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    "stepSeconds": RANGE_STEP_SECONDS,
                    "samples": compact_samples(samples),
                    "peerAverageSeconds": peer_average,
                    "model": model,
                },
                calculation={
                    "formula": "ETA = (threshold - current) / slope",
                    "current": round(value, 6),
                    "threshold": LATENCY_THRESHOLD,
                    "slopePerMinute": round(model["slopePerMinute"], 6),
                    "minimumSlopePerMinute": LATENCY_MIN_SLOPE_PER_MINUTE,
                    "rSquared": round(model["rSquared"], 4),
                    "peerAverage": round(peer_average, 6),
                    "nodePeerRatio": round(node_peer_ratio, 4),
                    "minimumNodePeerRatio": LATENCY_PEER_RATIO,
                    "etaSeconds": round(float(eta)),
                    "horizonSeconds": LATENCY_HORIZON_SECONDS,
                    "sampleCount": int(model["sampleCount"]),
                    "allConditionsPassed": True,
                },
            )
            active_keys.add(
                prediction_key("PREDICTED_PAYMENT_NODE_DEGRADATION", node)
            )

    node3_unavailable = unavailable.get("payment-node-3", 0.0) >= 1
    peer_nodes = ("payment-node-1", "payment-node-2")
    peer_models = {
        node: regression(processing_series.get(node, []))
        for node in peer_nodes
    }
    peer_current = [processing.get(node, 0.0) for node in peer_nodes]

    if (
        node3_unavailable
        and all(peer_models.values())
        and all(capacities.get(node, 1.0) <= 1 for node in peer_nodes)
    ):
        etas = [
            time_to_threshold(peer_models[node], LATENCY_THRESHOLD)
            for node in peer_nodes
        ]
        if all(
            eta is not None and 0 < eta <= LATENCY_HORIZON_SECONDS
            for eta in etas
        ):
            combined_model = {
                "slopePerSecond": sum(
                    peer_models[node]["slopePerSecond"] for node in peer_nodes
                ) / 2,
                "slopePerMinute": sum(
                    peer_models[node]["slopePerMinute"] for node in peer_nodes
                ) / 2,
                "rSquared": sum(
                    peer_models[node]["rSquared"] for node in peer_nodes
                ) / 2,
                "current": sum(peer_current) / 2,
                "sampleCount": min(
                    peer_models[node]["sampleCount"] for node in peer_nodes
                ),
                "windowSeconds": min(
                    peer_models[node]["windowSeconds"] for node in peer_nodes
                ),
            }
            eta = min(float(item) for item in etas if item is not None)
            await upsert_prediction(
                prediction_type="PREDICTED_CAPACITY_SATURATION",
                scope="payment-fleet",
                summary="Remaining payment capacity is forecast to become insufficient.",
                expected_impact=(
                    "Checkout latency may breach its threshold while redundancy is reduced."
                ),
                current_value=combined_model["current"],
                threshold=LATENCY_THRESHOLD,
                eta_seconds=eta,
                horizon_seconds=LATENCY_HORIZON_SECONDS,
                model=combined_model,
                supporting_signals=[
                    "payment-node-3 is unavailable.",
                    (
                        "payment-node-1 and payment-node-2 remain at one "
                        "bounded capacity unit."
                    ),
                    f"Peer processing p95 values are {peer_current}.",
                    f"Checkout throughput is {throughput:.2f} requests/second.",
                ],
                evidence={
                    "node3Unavailable": True,
                    "processingP95SecondsByNode": processing,
                    "capacityUnitsByNode": capacities,
                    "checkoutRequestsPerSecond": throughput,
                    "modelsByNode": peer_models,
                    "rangeStart": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "rangeEnd": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    "stepSeconds": RANGE_STEP_SECONDS,
                },
                calculation={
                    "formula": (
                        "Both peer ETA values must be inside the prediction "
                        "horizon while node 3 is unavailable."
                    ),
                    "currentPeerAverage": round(combined_model["current"], 6),
                    "threshold": LATENCY_THRESHOLD,
                    "peerEtasSeconds": [
                        round(float(item)) for item in etas if item is not None
                    ],
                    "node3Unavailable": True,
                    "capacityUnitsByNode": capacities,
                    "sampleCount": int(combined_model["sampleCount"]),
                    "allConditionsPassed": True,
                },
            )
            active_keys.add(
                prediction_key("PREDICTED_CAPACITY_SATURATION", "payment-fleet")
            )

    for hostname, remaining in certs.items():
        if CERT_THRESHOLD_SECONDS < remaining <= CERT_HORIZON_SECONDS:
            eta = remaining - CERT_THRESHOLD_SECONDS
            samples = cert_series.get(hostname, [])
            model = {
                "slopePerSecond": -1.0,
                "slopePerMinute": -60.0,
                "rSquared": 1.0,
                "current": remaining,
                "sampleCount": float(len(samples)),
                "windowSeconds": (
                    samples[-1][0] - samples[0][0] if len(samples) > 1 else 0.0
                ),
            }
            await upsert_prediction(
                prediction_type="PREDICTED_CERTIFICATE_EXPIRY",
                scope=hostname,
                summary=(
                    f"The TLS certificate for {hostname} is entering its renewal horizon."
                ),
                expected_impact=(
                    "Clients will reject the endpoint if renewal does not complete."
                ),
                current_value=CERT_HORIZON_SECONDS - remaining,
                threshold=CERT_HORIZON_SECONDS - CERT_THRESHOLD_SECONDS,
                eta_seconds=float(eta),
                horizon_seconds=CERT_HORIZON_SECONDS,
                model=model,
                supporting_signals=[
                    (
                        "Certificate expires in approximately "
                        f"{max(1, round(remaining / 60))} minutes."
                    ),
                    (
                        "Renewal incident threshold begins in approximately "
                        f"{max(1, round(eta / 60))} minutes."
                    ),
                ],
                evidence={
                    "metric": cert_query,
                    "hostname": hostname,
                    "expiresInSeconds": remaining,
                    "renewalThresholdSeconds": CERT_THRESHOLD_SECONDS,
                    "rangeStart": datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
                    "rangeEnd": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
                    "stepSeconds": RANGE_STEP_SECONDS,
                    "samples": compact_samples(samples),
                },
                calculation={
                    "formula": (
                        "ETA to renewal threshold = expiresInSeconds "
                        "- renewalThresholdSeconds"
                    ),
                    "expiresInSeconds": round(remaining),
                    "renewalThresholdSeconds": CERT_THRESHOLD_SECONDS,
                    "etaSeconds": round(eta),
                    "insideRenewalHorizon": True,
                    "allConditionsPassed": True,
                },
            )
            active_keys.add(
                prediction_key("PREDICTED_CERTIFICATE_EXPIRY", hostname)
            )

    for key, prediction in list(predictions.items()):
        if prediction.get("status") == "PREDICTED" and key not in active_keys:
            reduce_prediction(
                prediction["predictionType"],
                prediction["scope"],
                (
                    "The Prometheus range query no longer predicts a threshold "
                    "crossing inside the configured horizon."
                ),
            )

    await evaluate_recurrence_patterns(now)

    latency_series: list[dict[str, Any]] = []
    for node, samples in sorted(processing_series.items()):
        trend = live_trend(
            samples,
            LATENCY_THRESHOLD,
            LATENCY_HORIZON_SECONDS,
            LATENCY_MIN_SLOPE_PER_MINUTE,
        )
        peer_values = [
            value
            for peer_node, value in processing.items()
            if peer_node != node and value > 0
        ]
        peer_average = sum(peer_values) / len(peer_values) if peer_values else None
        trend.update(
            {
                "name": node,
                "peerAverage": (
                    round(float(peer_average), 6)
                    if peer_average is not None
                    else None
                ),
                "nodePeerRatio": (
                    round(
                        float(
                            processing.get(node, 0.0)
                            / max(peer_average, 0.001)
                        ),
                        3,
                    )
                    if peer_average
                    else None
                ),
            }
        )
        latency_series.append(trend)

    disk_trend = live_trend(
        disk_series,
        DISK_THRESHOLD,
        DISK_HORIZON_SECONDS,
        DISK_MIN_SLOPE_PER_MINUTE,
    )
    disk_trend["name"] = "opsai-demo-storage"

    certificate_series: list[dict[str, Any]] = []
    for hostname, samples in sorted(cert_series.items()):
        trend = live_trend(
            samples,
            CERT_THRESHOLD_SECONDS,
            CERT_HORIZON_SECONDS,
            direction="DOWN",
        )
        trend["name"] = hostname
        certificate_series.append(trend)

    throughput_trend = live_trend(
        throughput_series,
        None,
        None,
    )
    throughput_trend["name"] = "checkout requests"

    global live_signals
    live_signals = {
        "evaluatedAt": iso_now(),
        "dataSourceMode": "PROMETHEUS_RANGE_QUERIES",
        "rangeWindowSeconds": RANGE_WINDOW_SECONDS,
        "rangeStepSeconds": RANGE_STEP_SECONDS,
        "groups": [
            {
                "id": "payment-latency",
                "label": "Payment processing p95",
                "unit": "seconds",
                "threshold": LATENCY_THRESHOLD,
                "thresholdLabel": "Incident threshold",
                "direction": "UP",
                "series": latency_series,
            },
            {
                "id": "disk-usage",
                "label": "Application storage usage",
                "unit": "percent",
                "threshold": DISK_THRESHOLD,
                "thresholdLabel": "Disk incident threshold",
                "direction": "UP",
                "series": [disk_trend],
            },
            {
                "id": "certificate-expiry",
                "label": "Certificate time remaining",
                "unit": "seconds_remaining",
                "threshold": CERT_THRESHOLD_SECONDS,
                "thresholdLabel": "Renewal incident threshold",
                "direction": "DOWN",
                "series": certificate_series,
            },
            {
                "id": "checkout-throughput",
                "label": "Checkout throughput",
                "unit": "requests_per_second",
                "threshold": None,
                "thresholdLabel": None,
                "direction": "CONTEXT",
                "series": [throughput_trend],
            },
        ],
        "context": {
            "capacityUnitsByNode": capacities,
            "unavailableNodes": [
                node for node, value in unavailable.items() if value >= 1
            ],
            "checkoutRequestsPerSecond": round(throughput, 4),
        },
    }

    summary = {
        "evaluatedAt": iso_now(),
        "dataSourceMode": "PROMETHEUS_RANGE_QUERIES",
        "rangeWindowSeconds": RANGE_WINDOW_SECONDS,
        "rangeStepSeconds": RANGE_STEP_SECONDS,
        "minimumSamples": MIN_SAMPLES,
        "diskUsagePercent": disk,
        "certificateExpirySecondsByHostname": certs,
        "paymentProcessingP95SecondsByNode": processing,
        "paymentCapacityUnitsByNode": capacities,
        "unavailableNodes": [
            node for node, value in unavailable.items() if value >= 1
        ],
        "checkoutRequestsPerSecond": throughput,
        "activePredictions": sum(
            item.get("status") == "PREDICTED"
            for item in predictions.values()
        ),
        "activeFrequentPatterns": sum(
            item.get("status") == "ACTIVE" for item in patterns.values()
        ),
        "rangeQueries": {
            "disk": disk_query,
            "certificate": cert_query,
            "processingP95": processing_query,
            "throughput": throughput_query,
        },
    }

    global last_evaluation
    last_evaluation = summary
    LAST_EVALUATION.set(time.time())
    return summary


async def loop() -> None:
    while not stop_event.is_set():
        try:
            await evaluate()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            emit_log("ERROR", "prediction_evaluation_failed", error=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            continue


@app.on_event("startup")
async def startup() -> None:
    app.state.worker = asyncio.create_task(loop(), name="opsai-predictor")
    emit_log(
        "INFO",
        "service_started",
        pollSeconds=POLL_SECONDS,
        dataSourceMode="PROMETHEUS_RANGE_QUERIES",
        rangeWindowSeconds=RANGE_WINDOW_SECONDS,
        rangeStepSeconds=RANGE_STEP_SECONDS,
        recurrenceLookbackHours=RECURRENCE_LOOKBACK_HOURS,
        recurrenceThreshold=RECURRENCE_THRESHOLD,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    stop_event.set()
    worker = getattr(app.state, "worker", None)
    if worker:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": "opsai-predictor",
        "version": SERVICE_VERSION,
        "mode": "PROMETHEUS_RANGE_FORECAST_WITH_EVENT_DRIVEN_AI",
        "dataSourceMode": "PROMETHEUS_RANGE_QUERIES",
        "pollSeconds": POLL_SECONDS,
        "rangeWindowSeconds": RANGE_WINDOW_SECONDS,
        "rangeStepSeconds": RANGE_STEP_SECONDS,
        "minimumSamples": MIN_SAMPLES,
        "recurrenceLookbackHours": RECURRENCE_LOOKBACK_HOURS,
        "recurrenceThreshold": RECURRENCE_THRESHOLD,
        "activePredictions": sum(
            item.get("status") == "PREDICTED"
            for item in predictions.values()
        ),
        "activeFrequentPatterns": sum(
            item.get("status") == "ACTIVE" for item in patterns.values()
        ),
        "lastEvaluation": last_evaluation.get("evaluatedAt"),
        "continuousMetricsToAi": False,
        "aiContactMode": "NEW_DETERMINISTIC_TRIGGER_ONLY",
    }


@app.get("/summary")
async def summary() -> dict[str, Any]:
    active = [
        item for item in predictions.values()
        if item.get("status") == "PREDICTED"
    ]
    return {
        "activePredictions": len(active),
        "totalPredictions": len(predictions),
        "activeFrequentPatterns": sum(
            item.get("status") == "ACTIVE" for item in patterns.values()
        ),
        "totalFrequentPatterns": len(patterns),
        "realAiExplanations": sum(
            (item.get("aiExplanation") or {}).get("analysisMode") == "REAL_AI"
            for item in list(predictions.values()) + list(patterns.values())
        ),
        "averageLeadTimeSeconds": (
            round(
                sum(float(item.get("timeToThresholdSeconds") or 0) for item in active)
                / len(active)
            )
            if active
            else 0
        ),
        "latestEvaluation": last_evaluation,
        "lastAiContact": last_ai_contact,
        "calculationCatalog": calculation_catalog(),
        "dashboardRefreshSeconds": DASHBOARD_REFRESH_SECONDS,
        "problemCandidateCount": len(build_problem_candidates()),
        "lastProblemSync": last_problem_sync,
    }


@app.get("/calculations")
async def calculations() -> dict[str, Any]:
    return {
        "dataSourceMode": "PROMETHEUS_RANGE_QUERIES",
        "continuousMetricsToAi": False,
        "aiContactMode": "NEW_DETERMINISTIC_TRIGGER_ONLY",
        "catalog": calculation_catalog(),
        "latestEvaluation": last_evaluation,
    }


@app.get("/signals")
async def signals() -> dict[str, Any]:
    return live_signals or {
        "evaluatedAt": None,
        "dataSourceMode": "PROMETHEUS_RANGE_QUERIES",
        "rangeWindowSeconds": RANGE_WINDOW_SECONDS,
        "rangeStepSeconds": RANGE_STEP_SECONDS,
        "groups": [],
        "context": {},
    }


@app.get("/predictions")
async def list_predictions(
    status: str = Query(default="all"),
) -> dict[str, Any]:
    rows = list(predictions.values())
    if status != "all":
        rows = [item for item in rows if item.get("status") == status]
    rows.sort(key=lambda item: item.get("updatedAt") or "", reverse=True)
    return {"predictions": rows}


@app.get("/patterns")
async def list_patterns(
    status: str = Query(default="all"),
) -> dict[str, Any]:
    rows = list(patterns.values())
    if status != "all":
        rows = [item for item in rows if item.get("status") == status]
    rows.sort(key=lambda item: item.get("updatedAt") or "", reverse=True)
    return {"patterns": rows}



@app.get("/problem-candidates")
async def problem_candidates() -> dict[str, Any]:
    return {
        "candidates": build_problem_candidates(),
        "lastSync": last_problem_sync,
    }


async def core_problem_request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=PROBLEM_REGISTER_TIMEOUT_SECONDS
    ) as client:
        response = await client.request(
            method,
            f"{OPSAI_CORE_URL}{path}",
            json=payload,
        )
    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json()
        except Exception:
            detail = response.text
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


@app.get("/problem-register")
async def problem_register(
    status: str = Query(default="all"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    return await core_problem_request(
        "GET",
        f"/problems?status={status}&limit={limit}",
    )


@app.get("/problem-register/summary")
async def problem_register_summary() -> dict[str, Any]:
    return await core_problem_request("GET", "/problems/summary")


@app.get("/problem-register/{problem_id}")
async def problem_register_detail(problem_id: str) -> dict[str, Any]:
    return await core_problem_request("GET", f"/problems/{problem_id}")


@app.post("/problem-register/{problem_id}/transition")
async def problem_register_transition(
    problem_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return await core_problem_request(
        "POST",
        f"/problems/{problem_id}/transition",
        payload,
    )


@app.post("/problem-register/{problem_id}/assign")
async def problem_register_assign(
    problem_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    return await core_problem_request(
        "POST",
        f"/problems/{problem_id}/assign",
        payload,
    )


@app.get("/events")
async def list_events(limit: int = Query(default=200, ge=1, le=500)) -> dict[str, Any]:
    return {"events": events[-limit:]}


@app.post("/admin/evaluate")
async def evaluate_now(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_token(x_opsai_automation_token)
    return await evaluate()


@app.post("/admin/reset")
async def reset(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_token(x_opsai_automation_token)
    for prediction in predictions.values():
        PREDICTION_STATUS.labels(
            type=prediction.get("predictionType", "unknown"),
            scope=prediction.get("scope", "unknown"),
            prediction_id=prediction.get("predictionId", "unknown"),
        ).set(0)
    predictions.clear()
    patterns.clear()
    events.clear()
    emit_log("INFO", "predictor_state_reset")
    return {"status": "reset", "resetAt": iso_now()}


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    html = r'''<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PulseGuard - Predictive Analysis</title>
<style>
:root{color-scheme:dark;--bg:#07111f;--panel:#0f1d31;--border:#26384f;--text:#e7f0fb;--muted:#93a8c1;--blue:#48a7ff;--amber:#ffbf52;--green:#6ee7a5;--red:#ff9ea8;--violet:#c9a7ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.45 Segoe UI,Arial,sans-serif}
header{position:sticky;top:0;z-index:5;background:rgba(7,17,31,.96);border-bottom:1px solid var(--border);padding:18px 24px 14px}
h1{margin:0;font-size:23px}h2{margin:0 0 10px;font-size:18px}h3{margin:0;font-size:15px}.sub{color:var(--muted);margin-top:4px}
.refresh{display:flex;gap:10px;align-items:center;margin-top:8px;color:var(--muted)}.refresh b{color:var(--green)}
.wrap{max-width:1700px;margin:auto;padding:18px 24px 36px}.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}
.kpi{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:11px}.kpi b{display:block;font-size:22px;margin-top:3px}
.section{margin-top:20px}.panel{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px}
.signal-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}.signal-panel{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:14px;min-width:0}.signal-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:9px}.signal-head h3{margin:0}.signal-meta{color:var(--muted);font-size:12px;text-align:right}.signal-calcs{display:grid;gap:6px;margin-top:10px}.signal-calc{display:grid;grid-template-columns:minmax(140px,1.2fr) minmax(110px,.8fr) minmax(190px,1.4fr);gap:10px;padding:7px 0;border-top:1px solid var(--border);align-items:center}.signal-calc:first-child{border-top:0}.signal-name{font-weight:600}.signal-context{margin-top:9px;padding-top:9px;border-top:1px dashed var(--border);color:var(--muted);font-size:12px}
.chart{width:100%;height:235px;background:#081321;border:1px solid #20344d;border-radius:10px}.chart-empty{height:235px;display:flex;align-items:center;justify-content:center;color:var(--muted);background:#081321;border:1px solid #20344d;border-radius:10px}
.legend{display:flex;gap:12px;flex-wrap:wrap;margin-top:8px;color:var(--muted)}.legend span{display:flex;gap:6px;align-items:center}.dot{width:10px;height:10px;border-radius:999px;display:inline-block}
.status-list{display:grid;gap:8px}.status-row{border-bottom:1px solid var(--border);padding:8px 0}.status-row:last-child{border-bottom:0}
.status-head{display:flex;justify-content:space-between;gap:10px}.value{font-weight:600}.muted{color:var(--muted)}.good{color:var(--green)}.warn{color:var(--amber)}.bad{color:var(--red)}
.badge{display:inline-block;padding:3px 7px;border-radius:999px;background:#173451;color:#b9ddff;font-size:11px}.badge.ai{background:#382550;color:#ead5ff}.badge.calc{background:#153e34;color:#a5f0cf}.badge.high{background:#4d2028;color:#ffc2c8}.badge.medium{background:#4b3b17;color:#ffe19b}.badge.low{background:#183b32;color:#a3f0d0}
.table-wrap{overflow:auto;border:1px solid var(--border);border-radius:12px;background:var(--panel)}table{width:100%;border-collapse:collapse;min-width:980px}
th,td{text-align:left;padding:10px;border-bottom:1px solid var(--border);vertical-align:top}th{background:#13233a;color:#a8d8ff;position:sticky;top:92px}tr:last-child td{border-bottom:0}
.problem-chart{display:grid;gap:10px}.problem-row{display:grid;grid-template-columns:minmax(230px,1.3fr) minmax(280px,2fr) 90px 100px;gap:12px;align-items:center}
.track{height:12px;background:#26384f;border-radius:999px;overflow:hidden}.track i{display:block;height:100%;background:linear-gradient(90deg,var(--blue),var(--amber))}
details{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:12px}summary{cursor:pointer;font-weight:600}.empty{padding:30px;text-align:center;color:var(--muted)}
.note{padding:10px 12px;border-left:3px solid var(--blue);background:#0b1a2c;color:var(--muted);margin-bottom:10px}
.problem-actions{display:flex;gap:6px;flex-wrap:wrap}.problem-actions button{border:1px solid #395473;background:#173451;color:#d7ebff;border-radius:6px;padding:5px 8px;cursor:pointer;font:inherit}.problem-actions button:hover{background:#214568}.problem-actions button.secondary{background:#18263a}.problem-actions button.danger{background:#4d2028;border-color:#73404a}.problem-owner{min-width:170px}.problem-detail{max-width:340px}.problem-status{white-space:nowrap}
@media(max-width:1100px){.kpis{grid-template-columns:repeat(3,1fr)}.signal-grid{grid-template-columns:1fr}.problem-row{grid-template-columns:1fr}}
@media(max-width:700px){header{position:static}.wrap{padding:14px}.kpis{grid-template-columns:repeat(2,1fr)}th{position:static}.chart{height:220px}.signal-calc{grid-template-columns:1fr}.signal-meta{text-align:left}.signal-head{display:block}}
</style>
</head>
<body>
<header>
<h1>PulseGuard - Predictive Analysis</h1>
<div class="sub">Live Prometheus range data, deterministic forecasts, potential problem patterns and event-driven REAL_AI interpretation.</div>
<div class="refresh"><b id="refreshState">Auto-refresh active</b><span id="refreshDetail"></span><span id="refreshError" class="bad"></span></div>
</header>
<div class="wrap">
<div class="kpis">
<div class="kpi">Active predictions<b id="active">0</b></div>
<div class="kpi">Potential candidates<b id="candidateCount">0</b></div>
<div class="kpi">Open problem records<b id="problemCount">0</b></div>
<div class="kpi">Frequent patterns<b id="patternsCount">0</b></div>
<div class="kpi">Total predictions<b id="total">0</b></div>
<div class="kpi">REAL_AI reviews<b id="real">0</b></div>
<div class="kpi">Average lead time<b id="lead">0 min</b></div>
</div>

<section class="section">
<h2>Live risk monitor</h2>
<div class="note">All monitored trends are shown together and update automatically from the latest Prometheus range window. Dotted lines are reactive incident thresholds.</div>
<div id="signalGrid" class="signal-grid"></div>
</section>

<section class="section">
<h2>Forecast decisions</h2>
<div class="table-wrap"><table>
<thead><tr><th>State</th><th>Prediction / scope</th><th>Current</th><th>Threshold</th><th>Trend / R²</th><th>ETA</th><th>REAL_AI</th><th>Validated suggestion</th></tr></thead>
<tbody id="predictionRows"></tbody>
</table></div>
</section>

<section class="section">
<h2>Potential problem candidates</h2>
<div class="note">Related recurring incidents are grouped into broader investigation candidates. Scores are deterministic; these are not confirmed root causes.</div>
<div class="panel"><div id="problemChart" class="problem-chart"></div></div>
</section>

<section class="section">
<h2>Problem Register</h2>
<div class="note">Recurring evidence creates a persistent CANDIDATE record. An operator must review and confirm it before it becomes an active problem investigation. Synthetic-only records remain clearly marked as demo candidates.</div>
<div class="table-wrap"><table>
<thead><tr><th>Status</th><th>Risk</th><th>Problem</th><th>Recurring evidence</th><th>Origin</th><th>Owner</th><th>Investigation / action</th><th>Lifecycle actions</th></tr></thead>
<tbody id="problemRows"></tbody>
</table></div>
</section>

<section class="section">
<h2>Frequently occurring issues</h2>
<div class="table-wrap"><table>
<thead><tr><th>Risk</th><th>Incident / scope</th><th>Occurrences</th><th>Average interval</th><th>Origin</th><th>Potential problem</th><th>Validated next step</th></tr></thead>
<tbody id="patternRows"></tbody>
</table></div>
</section>

<section class="section">
<details>
<summary>Calculation rules and AI contact policy</summary>
<div class="muted" id="mode" style="margin:10px 0"></div>
<div id="lastAi" style="margin-bottom:10px"></div>
<div class="table-wrap"><table>
<thead><tr><th>Prediction</th><th>Prometheus source</th><th>Calculation</th><th>Threshold / trigger</th><th>When REAL_AI is contacted</th></tr></thead>
<tbody id="catalog"></tbody>
</table></div>
</details>
</section>
</div>

<script>
const REFRESH_MS=__REFRESH_MS__;
let refreshing=false,signalPayload={groups:[],context:{}};
const esc=s=>String(s??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
const fmt=(v,d=3)=>Number.isFinite(Number(v))?Number(v).toFixed(d):'n/a';
const minutes=s=>Number.isFinite(Number(s))?Math.max(1,Math.round(Number(s)/60)):'n/a';
const stateClass=s=>s==='THRESHOLD_BREACHED'?'bad':s==='FORECAST_RISK'||s==='TRENDING'?'warn':'good';
const palette=['#48a7ff','#6ee7a5','#c9a7ff','#ff9ea8','#ffbf52'];
const ACTION_TEXT={collect_diagnostics:'Collect and compare diagnostics before changing service state.',scale_payment_capacity:'Review bounded payment-capacity scaling through deterministic governance.',cleanup_disk_space:'Prepare archive-before-cleanup for allowlisted temporary storage.',renew_certificate:'Prepare allowlisted certificate renewal and verify binding and expiry.',refresh_external_service_credentials:'Refresh the allowlisted external credential and verify an authenticated probe.'};
const PREDICTION_ACTION={PREDICTED_DISK_PRESSURE:'cleanup_disk_space',PREDICTED_PAYMENT_NODE_DEGRADATION:'collect_diagnostics',PREDICTED_CAPACITY_SATURATION:'scale_payment_capacity',PREDICTED_CERTIFICATE_EXPIRY:'renew_certificate'};
const INCIDENT_ACTION={NODE_DISK_PRESSURE:'cleanup_disk_space',TLS_CERTIFICATE_EXPIRING:'renew_certificate',PAYMENT_FLEET_CAPACITY_DEGRADATION:'scale_payment_capacity',EXTERNAL_SERVICE_AUTHENTICATION_FAILURE:'refresh_external_service_credentials',PAYMENT_NODE_LATENCY:'collect_diagnostics',PAYMENT_NODE_NETWORK_INSTABILITY:'collect_diagnostics',PAYMENT_NODE_TIMEOUT:'collect_diagnostics',PAYMENT_NODE_UNAVAILABLE:'collect_diagnostics',PAYMENT_NODE_HUNG:'collect_diagnostics',CHECKOUT_FAILURE_RATE:'collect_diagnostics',PAYMENT_SHARED_DEPENDENCY_OUTAGE:'collect_diagnostics'};
const PROBLEM_NEXT={CANDIDATE:[['UNDER_REVIEW','Review'],['REJECTED','Reject']],UNDER_REVIEW:[['CONFIRMED','Confirm'],['REJECTED','Reject']],CONFIRMED:[['INVESTIGATING','Investigate']],INVESTIGATING:[['CORRECTIVE_ACTION_PLANNED','Plan action'],['REJECTED','Reject']],CORRECTIVE_ACTION_PLANNED:[['MONITORING','Start monitoring'],['INVESTIGATING','Return to investigation']],MONITORING:[['CLOSED','Close'],['INVESTIGATING','Reopen investigation']],CLOSED:[['MONITORING','Reopen monitoring']],REJECTED:[['UNDER_REVIEW','Review again']]};

async function getJson(url){const r=await fetch(`${url}${url.includes('?')?'&':'?'}_=${Date.now()}`,{cache:'no-store',headers:{'Cache-Control':'no-cache','Pragma':'no-cache'}});if(!r.ok)throw new Error(`${url}: HTTP ${r.status}`);return r.json();}
function unitValue(value,unit){if(!Number.isFinite(Number(value)))return 'n/a';const v=Number(value);if(unit==='percent')return `${v.toFixed(1)}%`;if(unit==='seconds')return `${v.toFixed(3)} s`;if(unit==='seconds_remaining'){if(v>=86400)return `${(v/86400).toFixed(1)} days`;if(v>=3600)return `${(v/3600).toFixed(1)} h`;return `${Math.round(v/60)} min`;}if(unit==='requests_per_second')return `${v.toFixed(1)} req/s`;return v.toFixed(3);}
function problemCategory(type){if(['PAYMENT_NODE_LATENCY','PAYMENT_FLEET_CAPACITY_DEGRADATION','CHECKOUT_FAILURE_RATE'].includes(type))return ['Payment platform latency and capacity degradation','Recurring latency and capacity symptoms may share a traffic, redundancy or processing-capacity cause.'];if(['PAYMENT_NODE_TIMEOUT','PAYMENT_NODE_NETWORK_INSTABILITY','PAYMENT_NODE_UNAVAILABLE','PAYMENT_NODE_HUNG','PAYMENT_SHARED_DEPENDENCY_OUTAGE'].includes(type))return ['Payment-node and dependency reliability instability','Repeated timeout, availability or dependency symptoms may indicate an unresolved node, network or shared-dependency reliability issue.'];if(type==='NODE_DISK_PRESSURE')return ['Storage exhaustion risk','Repeated disk-pressure incidents may indicate uncontrolled growth, insufficient retention or ineffective cleanup.'];if(type==='TLS_CERTIFICATE_EXPIRING')return ['Certificate lifecycle risk','Repeated expiry warnings may indicate unreliable renewal scheduling or certificate binding.'];if(type==='EXTERNAL_SERVICE_AUTHENTICATION_FAILURE')return ['External dependency credential reliability','Repeated authentication failures may indicate token rotation or credential-synchronisation problems.'];return [`Recurring ${type}`,'Repeated matching incidents may indicate an unresolved root cause.'];}
function patternRisk(x){const sev=(x.severities||[]).map(v=>String(v).toLowerCase()),sw=sev.includes('critical')?22:sev.includes('high')?16:sev.includes('medium')?9:0,rw=Math.min(38,12*Math.max(1,Number(x.occurrences||0)-Number(x.threshold||2)+1)),i=Number(x.averageIntervalSeconds||0),iw=!i?0:i<=3600?20:i<=14400?13:i<=43200?7:0,score=Math.min(100,25+sw+rw+iw);return {score,level:score>=75?'HIGH':score>=50?'MEDIUM':'LOW'};}
function buildCandidates(patterns){const groups={};for(const x of patterns.filter(p=>p.status==='ACTIVE')){const [name,reason]=problemCategory(x.incidentType);if(!groups[name])groups[name]={candidate:name,why:reason,patterns:0,occurrences:0,types:new Set(),scopes:new Set(),origins:{},score:0};const g=groups[name],risk=patternRisk(x);g.patterns++;g.occurrences+=Number(x.occurrences||0);g.score=Math.max(g.score,risk.score);g.types.add(x.incidentType);g.scopes.add(x.scope);for(const [o,c] of Object.entries(x.originBreakdown||{}))g.origins[o]=(g.origins[o]||0)+Number(c||0);}return Object.values(groups).map(g=>({...g,types:[...g.types],scopes:[...g.scopes]})).sort((a,b)=>b.score-a.score||b.occurrences-a.occurrences);}
function originText(origins){const e=Object.entries(origins||{});return e.length?e.map(([k,v])=>`${k}: ${v}`).join(', '):'UNCLASSIFIED';}

function chartMarkup(group){
 if(!group||!(group.series||[]).some(s=>(s.samples||[]).length>1))return {chart:'<div class="chart-empty">No range samples are available for this metric yet.</div>',legend:''};
 const all=(group.series||[]).flatMap(s=>(s.samples||[]).map(p=>Number(p.value))).filter(Number.isFinite);if(group.threshold!==null&&Number.isFinite(Number(group.threshold)))all.push(Number(group.threshold));
 const min=Math.min(...all),max=Math.max(...all),span=Math.max(.0001,max-min),pad=.08*span,yMin=Math.max(0,min-pad),yMax=max+pad,W=1000,H=235,L=72,R=18,T=16,B=34,plotW=W-L-R,plotH=H-T-B;
 const series=group.series.filter(s=>(s.samples||[]).length>1),timestamps=series.flatMap(s=>s.samples.map(p=>Number(p.timestamp))).filter(Number.isFinite),tMin=Math.min(...timestamps),tMax=Math.max(...timestamps),tSpan=Math.max(1,tMax-tMin),x=t=>L+((t-tMin)/tSpan)*plotW,y=v=>T+(1-(v-yMin)/(yMax-yMin))*plotH;
 let svg=`<svg class="chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(group.label)} historical graph">`;
 for(let i=0;i<=4;i++){const yy=T+i*plotH/4,val=yMax-i*(yMax-yMin)/4;svg+=`<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#20344d"/><text x="${L-8}" y="${yy+4}" fill="#93a8c1" text-anchor="end" font-size="12">${esc(unitValue(val,group.unit))}</text>`;}
 if(group.threshold!==null&&Number.isFinite(Number(group.threshold))){const yy=y(Number(group.threshold));svg+=`<line x1="${L}" y1="${yy}" x2="${W-R}" y2="${yy}" stroke="#ffbf52" stroke-width="2" stroke-dasharray="7 5"/><text x="${W-R}" y="${Math.max(T+12,yy-6)}" fill="#ffbf52" text-anchor="end" font-size="12">${esc(group.thresholdLabel)} ${esc(unitValue(group.threshold,group.unit))}</text>`;}
 series.forEach((s,i)=>{const pts=s.samples.map(p=>`${x(Number(p.timestamp)).toFixed(1)},${y(Number(p.value)).toFixed(1)}`).join(' ');svg+=`<polyline points="${pts}" fill="none" stroke="${palette[i%palette.length]}" stroke-width="3" vector-effect="non-scaling-stroke"/>`;});
 svg+=`<text x="${L}" y="${H-8}" fill="#93a8c1" font-size="12">-${Math.round((tMax-tMin)/60)} min</text><text x="${W-R}" y="${H-8}" fill="#93a8c1" text-anchor="end" font-size="12">now</text></svg>`;
 const legend=series.map((s,i)=>`<span><i class="dot" style="background:${palette[i%palette.length]}"></i>${esc(s.name)}</span>`).join('')+(group.threshold!==null?'<span><i class="dot" style="background:#ffbf52"></i>threshold</span>':'');
 return {chart:svg,legend};
}
function signalCalculationRow(s,group){
 const eta=s.etaSeconds===null?'n/a':`${esc(minutes(s.etaSeconds))} min`;
 return `<div class="signal-calc"><div><span class="signal-name">${esc(s.name)}</span><br><span class="${stateClass(s.state)}">${esc(s.state)}</span></div><div><b>${esc(unitValue(s.current,group.unit))}</b>${group.threshold!==null?`<br><span class="muted">threshold ${esc(unitValue(group.threshold,group.unit))}</span>`:''}</div><div class="muted">Trend ${s.slopePerMinute===null?'n/a':esc(fmt(s.slopePerMinute,4))+'/min'} | R² ${esc(fmt(s.rSquared,2))}<br>ETA ${eta} | ${esc(s.sampleCount)} samples</div></div>`;
}
function renderSignalPanel(group){
 const visual=chartMarkup(group),direction=group.direction==='DOWN'?'lower values approach risk':group.direction==='UP'?'higher values approach risk':'context metric',c=signalPayload.context||{};
 const context=group.id==='payment-latency'?`Capacity: ${esc(Object.entries(c.capacityUnitsByNode||{}).map(([n,v])=>`${n}=${v}`).join(', ')||'n/a')} | Unavailable: ${esc((c.unavailableNodes||[]).join(', ')||'none')}`:group.id==='checkout-throughput'?`Current checkout throughput: ${esc(unitValue(c.checkoutRequestsPerSecond,'requests_per_second'))}`:'';
 return `<article class="signal-panel"><div class="signal-head"><h3>${esc(group.label)}</h3><div class="signal-meta">${Math.round((signalPayload.rangeWindowSeconds||0)/60)} min range | ${signalPayload.rangeStepSeconds||0} sec step<br>${esc(direction)}</div></div>${visual.chart}<div class="legend">${visual.legend}</div><div class="signal-calcs">${(group.series||[]).map(s=>signalCalculationRow(s,group)).join('')}</div>${context?`<div class="signal-context">${context}</div>`:''}</article>`;
}
function renderSignals(){
 const groups=signalPayload.groups||[];
 signalGrid.innerHTML=groups.length?groups.map(renderSignalPanel).join(''):'<div class="panel empty">No live Prometheus signal groups are available.</div>';
}
function renderPredictions(rows){predictionRows.innerHTML=rows.length?rows.map(x=>{const c=x.calculation||{},ai=x.aiExplanation||{},a=ai.actionName||PREDICTION_ACTION[x.predictionType]||'collect_diagnostics',reduced=x.status==='RISK_REDUCED';return `<tr><td><span class="badge ${reduced?'low':'medium'}">${esc(x.status)}</span></td><td><b>${esc(x.predictionType)}</b><br><span class="muted">${esc(x.scope)}</span></td><td>${esc(fmt(c.current??x.currentValue,4))}${reduced?'<br><span class="muted">at trigger</span>':''}</td><td>${esc(fmt(c.threshold??x.threshold,4))}</td><td>${esc(fmt(c.slopePerMinute,4))}/min<br><span class="muted">R² ${esc(fmt(c.rSquared,2))}</span></td><td>${esc(minutes(x.timeToThresholdSeconds))} min${reduced?'<br><span class="muted">at trigger</span>':''}</td><td><span class="badge ai">${esc(ai.analysisMode||'PENDING')}</span><br><span class="muted">${esc(x.aiContact?.triggerReason||'')}</span></td><td>${esc(ai.recommendedPreventiveAction||ACTION_TEXT[a])}<br><span class="muted">${esc(a)} | ${esc(ai.recommendationValidation||'TYPE_SCOPED_ALLOWLIST')}</span></td></tr>`;}).join(''):'<tr><td colspan="8" class="empty">No prediction event is active or retained. The live graph above still shows the monitored metric and threshold.</td></tr>';}
function renderProblems(candidates){problemChart.innerHTML=candidates.length?candidates.map(x=>`<div class="problem-row"><div><b>${esc(x.title)}</b><br><span class="muted">${esc(x.hypothesis)}</span></div><div><div class="track"><i style="width:${Math.min(100,Number(x.riskScore||0))}%"></i></div><span class="muted">${esc((x.incidentTypes||[]).join(', '))}</span></div><div><b>${esc(x.riskScore)}%</b><br><span class="muted">${esc(x.recordClass)}</span></div><div><b>${esc(x.occurrenceCount)}</b><br><span class="muted">occurrences</span></div></div>`).join(''):'<div class="empty">No grouped problem candidate has crossed the recurrence threshold.</div>';}
function problemStatusBadge(status){const level=status==='CLOSED'?'low':status==='REJECTED'?'medium':['CONFIRMED','INVESTIGATING','CORRECTIVE_ACTION_PLANNED','MONITORING'].includes(status)?'high':'medium';return `<span class="badge ${level}">${esc(status)}</span>`;}
function problemActions(row){const transitions=PROBLEM_NEXT[row.status]||[];const buttons=transitions.map(([status,label])=>`<button type="button" data-problem-id="${esc(row.id)}" data-problem-status="${esc(status)}" class="${status==='REJECTED'?'danger':''}">${esc(label)}</button>`).join('');return `<div class="problem-actions"><button type="button" class="secondary" data-problem-id="${esc(row.id)}" data-problem-assign="1">Assign</button>${buttons}</div>`;}
function renderProblemRegister(rows){problemRows.innerHTML=rows.length?rows.map(x=>{const origins=originText(x.origin_breakdown),root=x.confirmed_root_cause||'',action=x.corrective_action||'',notes=x.monitoring_notes||'',owner=x.owner_name?`${x.owner_queue}<br><span class="muted">${x.owner_name}</span>`:x.owner_queue;return `<tr><td class="problem-status">${problemStatusBadge(x.status)}<br><span class="muted">${esc(x.record_class)}</span></td><td><span class="badge ${String(x.risk_level||'LOW').toLowerCase()}">${esc(x.risk_level)} ${esc(Math.round(Number(x.risk_score||0)))}%</span><br><span class="muted">recurrence after action: ${esc(x.recurrence_after_action||0)}</span></td><td class="problem-detail"><b>${esc(x.title)}</b><br><span class="muted">${esc(x.summary)}</span></td><td><b>${esc(x.occurrence_count)}</b> occurrences<br><span class="muted">${esc((x.incident_types||[]).join(', '))}<br>${esc((x.scopes||[]).join(', '))}<br>${esc((x.linked_incident_ids||[]).length)} linked incidents</span></td><td>${esc(origins)}</td><td class="problem-owner">${owner}</td><td>${root?`<b>Root cause</b><br>${esc(root)}<br>`:''}${action?`<b>Corrective action</b><br>${esc(action)}<br>`:''}${notes?`<span class="muted">${esc(notes)}</span>`:(!root&&!action?'<span class="muted">Not recorded yet</span>':'')}</td><td>${problemActions(x)}</td></tr>`;}).join(''):'<tr><td colspan="8" class="empty">No persistent problem record has been created yet.</td></tr>';}
async function postJson(url,payload){const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});if(!r.ok){let detail='';try{detail=JSON.stringify(await r.json());}catch{detail=await r.text();}throw new Error(`${url}: HTTP ${r.status} ${detail}`);}return r.json();}
async function handleProblemAction(button){const problemId=button.dataset.problemId;if(!problemId)return;button.disabled=true;try{if(button.dataset.problemAssign){const ownerQueue=prompt('Support queue / owner group','Problem Management');if(ownerQueue===null)return;const ownerName=prompt('Named owner (optional)','');if(ownerName===null)return;const actor=prompt('Assignment made by','demo-operator')||'demo-operator';await postJson(`/problem-register/${problemId}/assign`,{ownerQueue,ownerName,actor,note:`Assigned from Predictive Analysis dashboard.`});}else{const status=button.dataset.problemStatus;if(!status)return;const actor=prompt('Action performed by','demo-operator')||'demo-operator';const note=prompt(`Note for transition to ${status}`,`Problem moved to ${status}.`)||'';const payload={status,actor,note,confirmedRootCause:'',correctiveAction:'',monitoringNotes:'',rejectionReason:''};if(status==='CONFIRMED')payload.confirmedRootCause=prompt('Confirmed root cause (leave blank if still unknown)','')||'';if(status==='CORRECTIVE_ACTION_PLANNED')payload.correctiveAction=prompt('Corrective action plan','')||'';if(status==='MONITORING'||status==='CLOSED')payload.monitoringNotes=prompt('Monitoring / closure evidence','')||'';if(status==='REJECTED')payload.rejectionReason=prompt('Reason for rejecting this candidate','')||'';await postJson(`/problem-register/${problemId}/transition`,payload);}await refresh();}catch(e){alert(e.message||String(e));}finally{button.disabled=false;}}
function renderPatterns(rows){patternRows.innerHTML=rows.length?rows.map(x=>{const r=patternRisk(x),[candidate]=problemCategory(x.incidentType),ai=x.aiExplanation||{},a=ai.actionName||INCIDENT_ACTION[x.incidentType]||'collect_diagnostics',hours=x.averageIntervalSeconds?Math.round(x.averageIntervalSeconds/360)/10:null;return `<tr><td><span class="badge ${r.level.toLowerCase()}">${esc(r.level)} ${esc(r.score)}%</span></td><td><b>${esc(x.incidentType)}</b><br><span class="muted">${esc(x.scope)}</span></td><td><b>${esc(x.occurrences)}</b> / trigger ${esc(x.threshold)}</td><td>${hours===null?'n/a':esc(hours)+' h'}</td><td>${esc(originText(x.originBreakdown))}</td><td>${esc(candidate)}</td><td>${esc(ai.recommendedPreventiveAction||ACTION_TEXT[a])}<br><span class="muted">${esc(a)} | ${esc(ai.recommendationValidation||'TYPE_SCOPED_ALLOWLIST')}</span></td></tr>`;}).join(''):'<tr><td colspan="7" class="empty">No issue has crossed the recurrence threshold.</td></tr>';}

async function refresh(){
 if(refreshing)return;refreshing=true;refreshState.textContent='Refreshing…';refreshError.textContent='';
 try{
  const [s,p,pt,c,signals,pc,pr,prs]=await Promise.all([getJson('/summary'),getJson('/predictions'),getJson('/patterns'),getJson('/calculations'),getJson('/signals'),getJson('/problem-candidates'),getJson('/problem-register'),getJson('/problem-register/summary')]);
  const pats=pt.patterns||[],candidates=pc.candidates||[],problemRecords=pr.problems||[];signalPayload=signals;
  active.textContent=s.activePredictions||0;candidateCount.textContent=candidates.length;problemCount.textContent=prs.open||0;patternsCount.textContent=s.activeFrequentPatterns||0;total.textContent=s.totalPredictions||0;real.textContent=s.realAiExplanations||0;lead.textContent=Math.round((s.averageLeadTimeSeconds||0)/60)+' min';
  renderSignals();
  renderPredictions(p.predictions||[]);renderProblems(candidates);renderProblemRegister(problemRecords);renderPatterns(pats);
  mode.innerHTML=`<b>Data source:</b> ${esc(c.dataSourceMode)} | <b>Window:</b> ${Math.round((c.latestEvaluation?.rangeWindowSeconds||0)/60)} min | <b>Step:</b> ${esc(c.latestEvaluation?.rangeStepSeconds)} sec | <b>Continuous metrics sent to AI:</b> <span class="good">No</span>`;
  const ai=s.lastAiContact;lastAi.innerHTML=ai?`<b>Latest REAL_AI contact:</b> ${esc(ai.predictionType)} / ${esc(ai.scope)} at ${esc(ai.contactedAt)}<br><span class="muted">${esc(ai.reason)}</span>`:'<b>Latest REAL_AI contact:</b> none yet.';
  catalog.innerHTML=(c.catalog||[]).map(x=>`<tr><td><b>${esc(x.predictionType)}</b></td><td>${esc(x.prometheusQuery)}<br><span class="muted">${esc(x.windowMinutes)} min | ${esc(x.stepSeconds)} sec step</span></td><td>${esc(x.calculation)}</td><td><b>${esc(x.threshold)}</b><br>${esc(x.additionalTrigger)}</td><td>${esc(x.aiContactRule)}</td></tr>`).join('');
  refreshState.textContent='Auto-refresh active';refreshDetail.textContent=`Updated ${new Date().toLocaleTimeString()} | every ${Math.round(REFRESH_MS/1000)} seconds`;
 }catch(e){refreshState.textContent='Auto-refresh retrying';refreshError.textContent=e.message||String(e);}finally{refreshing=false;}
}
problemRows.addEventListener('click',e=>{const button=e.target.closest('button[data-problem-id]');if(button)handleProblemAction(button);});
refresh();setInterval(refresh,REFRESH_MS);window.addEventListener('focus',refresh);document.addEventListener('visibilitychange',()=>{if(!document.hidden)refresh();});
</script>
</body>
</html>'''.replace("__REFRESH_MS__", str(DASHBOARD_REFRESH_SECONDS * 1000))
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
        },
    )
