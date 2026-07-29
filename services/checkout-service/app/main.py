from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
PAYMENT_ROUTER_URL = os.getenv("PAYMENT_ROUTER_URL", "http://payment-router:8000").rstrip("/")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "8"))
EXTERNAL_AUTH_SERVICE_URL = os.getenv(
    "EXTERNAL_AUTH_SERVICE_URL",
    "http://external-auth-service:8000",
).rstrip("/")
EXTERNAL_AUTH_SERVICE_NAME = os.getenv(
    "EXTERNAL_AUTH_SERVICE_NAME",
    "partner-risk-service",
)
AUTOMATION_API_TOKEN = require_env("AUTOMATION_API_TOKEN")
configured_external_token = require_env("EXTERNAL_AUTH_TOKEN")
external_token_generation = 1
external_token_updated_at: str | None = None

CHECKOUT_REQUESTS = Counter(
    "opsai_checkout_requests_total",
    "Customer-facing checkout requests.",
    ["status"],
)
CHECKOUT_DURATION = Histogram(
    "opsai_checkout_duration_seconds",
    "End-to-end checkout duration.",
    buckets=(0.05, 0.1, 0.15, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5, 8),
)
EXTERNAL_CALLS = Counter(
    "opsai_external_service_calls_total",
    "Checkout calls to external services by result.",
    ["service", "status"],
)
EXTERNAL_AUTH_FAILURES = Counter(
    "opsai_external_service_auth_failures_total",
    "Bearer-token authentication failures observed by checkout.",
    ["service", "reason"],
)
EXTERNAL_LAST_SUCCESS = Gauge(
    "opsai_external_service_last_success_timestamp_seconds",
    "Timestamp of the latest successful call to an external service.",
    ["service"],
)
EXTERNAL_TOKEN_GENERATION = Gauge(
    "opsai_checkout_external_token_generation",
    "Token generation currently configured in checkout.",
    ["service"],
)

app = FastAPI(title="PulseGuard Checkout Service", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())


class CartItem(BaseModel):
    productId: str = Field(min_length=1, max_length=100)
    quantity: int = Field(ge=1, le=20)
    unitPrice: float = Field(gt=0)


class CheckoutRequest(BaseModel):
    customerId: str = Field(min_length=1, max_length=100)
    items: list[CartItem] = Field(min_length=1, max_length=25)
    currency: str = Field(default="USD", min_length=3, max_length=3)


class ExternalTokenUpdate(BaseModel):
    token: str = Field(min_length=8, max_length=500)
    tokenGeneration: int = Field(default=1, ge=1)
    reason: str = Field(default="governed_refresh", min_length=2, max_length=100)


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
                "service": "checkout-service",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def require_admin(token: str | None) -> None:
    if token != AUTOMATION_API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid internal automation token.")


async def call_external_risk_service(
    customer_id: str,
    order_id: str,
    amount: float,
) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=4) as client:
            response = await client.post(
                f"{EXTERNAL_AUTH_SERVICE_URL}/risk-check",
                headers={"Authorization": f"Bearer {configured_external_token}"},
                json={
                    "customerId": customer_id,
                    "orderId": order_id,
                    "amount": amount,
                },
            )
            if response.status_code == 401:
                EXTERNAL_CALLS.labels(
                    service=EXTERNAL_AUTH_SERVICE_NAME,
                    status="authentication_failed",
                ).inc()
                EXTERNAL_AUTH_FAILURES.labels(
                    service=EXTERNAL_AUTH_SERVICE_NAME,
                    reason="invalid_bearer_token",
                ).inc()
                emit_log(
                    "ERROR",
                    "external_service_authentication_failed",
                    externalService=EXTERNAL_AUTH_SERVICE_NAME,
                    orderId=order_id,
                    configuredTokenFingerprint=fingerprint(configured_external_token),
                    configuredTokenGeneration=external_token_generation,
                )
                raise HTTPException(
                    status_code=502,
                    detail="External service authentication failed.",
                )
            response.raise_for_status()
            payload = response.json()
    except HTTPException:
        raise
    except httpx.HTTPError as exc:
        EXTERNAL_CALLS.labels(
            service=EXTERNAL_AUTH_SERVICE_NAME,
            status="unavailable",
        ).inc()
        emit_log(
            "ERROR",
            "external_service_unavailable",
            externalService=EXTERNAL_AUTH_SERVICE_NAME,
            orderId=order_id,
            error=str(exc),
        )
        raise HTTPException(
            status_code=503,
            detail="External risk service is unavailable.",
        ) from exc

    EXTERNAL_CALLS.labels(
        service=EXTERNAL_AUTH_SERVICE_NAME,
        status="success",
    ).inc()
    EXTERNAL_LAST_SUCCESS.labels(service=EXTERNAL_AUTH_SERVICE_NAME).set(time.time())
    return payload if isinstance(payload, dict) else {}


@app.on_event("startup")
async def startup_event() -> None:
    EXTERNAL_TOKEN_GENERATION.labels(
        service=EXTERNAL_AUTH_SERVICE_NAME,
    ).set(external_token_generation)
    emit_log(
        "INFO",
        "service_started",
        paymentRouterUrl=PAYMENT_ROUTER_URL,
        externalAuthService=EXTERNAL_AUTH_SERVICE_NAME,
        externalTokenFingerprint=fingerprint(configured_external_token),
    )


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "checkout-service", "version": SERVICE_VERSION}


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": "checkout-service",
        "version": SERVICE_VERSION,
        "externalAuthService": EXTERNAL_AUTH_SERVICE_NAME,
        "externalTokenFingerprint": fingerprint(configured_external_token),
        "externalTokenGeneration": external_token_generation,
        "externalTokenUpdatedAt": external_token_updated_at,
    }


@app.get("/admin/external-auth/diagnostics")
async def external_auth_diagnostics(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    return {
        "service": EXTERNAL_AUTH_SERVICE_NAME,
        "serviceUrl": EXTERNAL_AUTH_SERVICE_URL,
        "configuredTokenFingerprint": fingerprint(configured_external_token),
        "configuredTokenGeneration": external_token_generation,
        "updatedAt": external_token_updated_at,
    }


@app.post("/admin/external-auth/token")
async def update_external_auth_token(
    request: ExternalTokenUpdate,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    global configured_external_token, external_token_generation, external_token_updated_at
    previous = fingerprint(configured_external_token)
    configured_external_token = request.token
    external_token_generation = request.tokenGeneration
    external_token_updated_at = utc_now()
    EXTERNAL_TOKEN_GENERATION.labels(
        service=EXTERNAL_AUTH_SERVICE_NAME,
    ).set(external_token_generation)
    emit_log(
        "INFO",
        "external_service_credentials_refreshed",
        externalService=EXTERNAL_AUTH_SERVICE_NAME,
        previousTokenFingerprint=previous,
        configuredTokenFingerprint=fingerprint(configured_external_token),
        configuredTokenGeneration=external_token_generation,
        reason=request.reason,
    )
    return await external_auth_diagnostics(x_opsai_automation_token)


@app.post("/admin/external-auth/verify")
async def verify_external_auth(
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    require_admin(x_opsai_automation_token)
    probe_order = f"AUTH-PROBE-{uuid.uuid4().hex[:10].upper()}"
    payload = await call_external_risk_service(
        "opsai-verification",
        probe_order,
        1.0,
    )
    return {
        "verified": True,
        "externalService": EXTERNAL_AUTH_SERVICE_NAME,
        "tokenFingerprint": fingerprint(configured_external_token),
        "tokenGeneration": external_token_generation,
        "response": {
            "status": payload.get("status"),
            "riskBand": payload.get("riskBand"),
            "tokenGeneration": payload.get("tokenGeneration"),
        },
        "verifiedAt": utc_now(),
    }


@app.post("/checkout")
async def checkout(request: CheckoutRequest) -> dict[str, object]:
    started = time.perf_counter()
    order_id = f"ORD-{uuid.uuid4().hex[:12].upper()}"
    amount = round(sum(item.quantity * item.unitPrice for item in request.items), 2)

    try:
        external_risk = await call_external_risk_service(
            request.customerId,
            order_id,
            amount,
        )
    except HTTPException as exc:
        elapsed_seconds = time.perf_counter() - started
        CHECKOUT_REQUESTS.labels(status="failed").inc()
        CHECKOUT_DURATION.observe(elapsed_seconds)
        emit_log(
            "ERROR",
            "checkout_external_dependency_failed",
            customerId=request.customerId,
            orderId=order_id,
            amount=amount,
            durationMs=round(elapsed_seconds * 1000),
            statusCode=exc.status_code,
            error=exc.detail,
        )
        raise

    payment_payload = {"orderId": order_id, "amount": amount, "currency": request.currency}
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
            response = await client.post(f"{PAYMENT_ROUTER_URL}/payments", json=payment_payload)
            response.raise_for_status()
            payment = response.json()
    except httpx.HTTPStatusError as exc:
        elapsed_seconds = time.perf_counter() - started
        CHECKOUT_REQUESTS.labels(status="failed").inc()
        CHECKOUT_DURATION.observe(elapsed_seconds)
        emit_log(
            "ERROR",
            "checkout_payment_rejected",
            customerId=request.customerId,
            orderId=order_id,
            amount=amount,
            durationMs=round(elapsed_seconds * 1000),
            statusCode=exc.response.status_code,
            error=exc.response.text[:1000],
        )
        raise HTTPException(status_code=502, detail="Payment processing failed.") from exc
    except httpx.HTTPError as exc:
        elapsed_seconds = time.perf_counter() - started
        CHECKOUT_REQUESTS.labels(status="failed").inc()
        CHECKOUT_DURATION.observe(elapsed_seconds)
        emit_log(
            "ERROR",
            "checkout_payment_unavailable",
            customerId=request.customerId,
            orderId=order_id,
            amount=amount,
            durationMs=round(elapsed_seconds * 1000),
            error=str(exc),
        )
        raise HTTPException(status_code=503, detail="Payment service is unavailable.") from exc

    elapsed_seconds = time.perf_counter() - started
    elapsed_ms = round(elapsed_seconds * 1000)
    CHECKOUT_REQUESTS.labels(status="success").inc()
    CHECKOUT_DURATION.observe(elapsed_seconds)
    emit_log(
        "INFO",
        "checkout_completed",
        customerId=request.customerId,
        orderId=order_id,
        amount=amount,
        itemCount=sum(item.quantity for item in request.items),
        processedBy=payment.get("processedBy"),
        routeAttempt=payment.get("routeAttempt"),
        externalRiskBand=external_risk.get("riskBand"),
        durationMs=elapsed_ms,
    )

    return {
        "status": "completed",
        "orderId": order_id,
        "customerId": request.customerId,
        "amount": amount,
        "currency": request.currency,
        "checkoutDurationMs": elapsed_ms,
        "externalRisk": {
            "service": EXTERNAL_AUTH_SERVICE_NAME,
            "riskBand": external_risk.get("riskBand"),
        },
        "payment": payment,
    }
