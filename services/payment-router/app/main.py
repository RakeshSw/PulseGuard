from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge, make_asgi_app

SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.4.0")
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "3"))

ROUTED_REQUESTS = Counter(
    "opsai_router_requests_total",
    "Requests routed by selected node and result.",
    ["node", "status"],
)
ROUTER_RETRIES = Counter(
    "opsai_router_retries_total",
    "Retry attempts after a selected node failed.",
    ["failed_node"],
)
ROUTER_FAILURES = Counter(
    "opsai_router_failures_total",
    "Observed router failures classified by node and transport/application kind.",
    ["failed_node", "error_kind"],
)
NODE_DURATION = Histogram(
    "opsai_router_node_duration_seconds",
    "Time spent calling a selected payment node.",
    ["node"],
    buckets=(0.05, 0.1, 0.15, 0.25, 0.5, 0.75, 1, 1.5, 2, 3, 5),
)
NODE_ACTIVE = Gauge(
    "opsai_router_node_active",
    "Whether a payment node is eligible for new traffic.",
    ["node"],
)


@dataclass
class Node:
    node_id: str
    url: str
    status: str = "active"


class PaymentRequest(BaseModel):
    orderId: str = Field(min_length=1, max_length=100)
    amount: float = Field(gt=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)


def parse_nodes(raw_value: str) -> list[Node]:
    nodes: list[Node] = []
    for item in raw_value.replace("\n", "").split(","):
        item = item.strip()
        if not item:
            continue
        if "=" not in item:
            raise RuntimeError(f"Invalid PAYMENT_NODES item: {item!r}")
        node_id, url = item.split("=", 1)
        nodes.append(Node(node_id=node_id.strip(), url=url.strip().rstrip("/")))
    if not nodes:
        raise RuntimeError("PAYMENT_NODES did not define any Payment nodes.")
    return nodes


NODES = parse_nodes(
    os.getenv(
        "PAYMENT_NODES",
        "payment-node-1=http://payment-node-1:8000,"
        "payment-node-2=http://payment-node-2:8000,"
        "payment-node-3=http://toxiproxy:8667",
    )
)
NODE_BY_ID = {node.node_id: node for node in NODES}

app = FastAPI(title="PulseGuard Payment Router", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())
selection_lock = asyncio.Lock()
next_index = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_log(level: str, event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": utc_now(),
                "level": level,
                "service": "payment-router",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def classify_failure(exc: Exception) -> str:
    """Classify only what the router can observe; never read scenario-controller state."""
    if isinstance(exc, httpx.TimeoutException):
        return "timeout"
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        body = exc.response.text.lower()
        if status == 503 and "shared payment-authorisation dependency" in body:
            return "shared_dependency"
        if status == 503 and "unavailable because" in body:
            return "node_unavailable"
        return f"http_{status}"
    if isinstance(exc, (httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError)):
        return "connection_reset"
    if isinstance(exc, httpx.TransportError):
        return "transport_error"
    if isinstance(exc, ValueError):
        return "invalid_response"
    return "unknown"


async def ordered_active_nodes() -> list[Node]:
    global next_index
    async with selection_lock:
        active_nodes = [node for node in NODES if node.status == "active"]
        if not active_nodes:
            return []
        start = next_index % len(active_nodes)
        next_index = (next_index + 1) % len(active_nodes)
    return active_nodes[start:] + active_nodes[:start]


@app.on_event("startup")
async def startup_event() -> None:
    for node in NODES:
        NODE_ACTIVE.labels(node=node.node_id).set(1)
    emit_log("INFO", "service_started", configuredNodes=[node.node_id for node in NODES])


@app.get("/")
async def root() -> dict[str, object]:
    return {
        "service": "payment-router",
        "version": SERVICE_VERSION,
        "configuredNodes": [node.node_id for node in NODES],
    }


@app.get("/health")
async def health() -> dict[str, object]:
    active_count = sum(1 for node in NODES if node.status == "active")
    return {
        "status": "healthy" if active_count > 0 else "degraded",
        "service": "payment-router",
        "version": SERVICE_VERSION,
        "activeNodes": active_count,
        "totalNodes": len(NODES),
    }


@app.get("/nodes")
async def list_nodes() -> dict[str, list[dict[str, str]]]:
    return {
        "nodes": [
            {"nodeId": node.node_id, "url": node.url, "status": node.status}
            for node in NODES
        ]
    }


@app.post("/nodes/{node_id}/drain")
async def drain_node(node_id: str) -> dict[str, str]:
    node = NODE_BY_ID.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Unknown Payment node: {node_id}")
    active_count = sum(1 for item in NODES if item.status == "active")
    if node.status == "active" and active_count <= 1:
        raise HTTPException(status_code=409, detail="Cannot drain the final active Payment node.")
    node.status = "drained"
    NODE_ACTIVE.labels(node=node_id).set(0)
    emit_log("WARNING", "node_drained", targetNode=node_id)
    return {"nodeId": node_id, "status": node.status}


@app.post("/nodes/{node_id}/restore")
async def restore_node(node_id: str) -> dict[str, str]:
    node = NODE_BY_ID.get(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Unknown Payment node: {node_id}")
    node.status = "active"
    NODE_ACTIVE.labels(node=node_id).set(1)
    emit_log("INFO", "node_restored", targetNode=node_id)
    return {"nodeId": node_id, "status": node.status}


@app.post("/payments")
async def route_payment(request: PaymentRequest) -> dict[str, object]:
    candidates = await ordered_active_nodes()
    if not candidates:
        emit_log("ERROR", "no_active_nodes", orderId=request.orderId)
        raise HTTPException(status_code=503, detail="No active Payment nodes available.")

    errors: list[dict[str, str]] = []
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS) as client:
        for attempt, node in enumerate(candidates, start=1):
            started = time.perf_counter()
            try:
                response = await client.post(f"{node.url}/payments", json=request.model_dump())
                response.raise_for_status()
                payload = response.json()
                duration_seconds = time.perf_counter() - started
                duration_ms = round(duration_seconds * 1000)
                ROUTED_REQUESTS.labels(node=node.node_id, status="success").inc()
                NODE_DURATION.labels(node=node.node_id).observe(duration_seconds)
                emit_log(
                    "INFO",
                    "payment_routed",
                    orderId=request.orderId,
                    selectedNode=node.node_id,
                    attempt=attempt,
                    durationMs=duration_ms,
                )
                return {
                    **payload,
                    "routedBy": "payment-router",
                    "routeAttempt": attempt,
                    "routerDurationMs": duration_ms,
                }
            except (httpx.HTTPError, ValueError) as exc:
                duration_seconds = time.perf_counter() - started
                duration_ms = round(duration_seconds * 1000)
                ROUTED_REQUESTS.labels(node=node.node_id, status="failed").inc()
                NODE_DURATION.labels(node=node.node_id).observe(duration_seconds)
                error_kind = classify_failure(exc)
                ROUTER_RETRIES.labels(failed_node=node.node_id).inc()
                ROUTER_FAILURES.labels(
                    failed_node=node.node_id,
                    error_kind=error_kind,
                ).inc()
                errors.append({
                    "nodeId": node.node_id,
                    "errorKind": error_kind,
                    "error": str(exc),
                })
                emit_log(
                    "ERROR",
                    "payment_route_failed",
                    orderId=request.orderId,
                    selectedNode=node.node_id,
                    attempt=attempt,
                    durationMs=duration_ms,
                    errorKind=error_kind,
                    error=str(exc),
                )

    raise HTTPException(
        status_code=502,
        detail={"message": "Every active Payment node failed.", "attempts": errors},
    )
