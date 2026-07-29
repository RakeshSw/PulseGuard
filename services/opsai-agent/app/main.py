from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Gauge, make_asgi_app
from psycopg.rows import dict_row
from pydantic import BaseModel, Field


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
OPSAI_CORE_URL = os.getenv("OPSAI_CORE_URL", "http://opsai-core:8000").rstrip("/")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090").rstrip("/")
DATABASE_URL = require_env("DATABASE_URL")
KNOWLEDGE_DIR = Path(os.getenv("KNOWLEDGE_DIR", "/app/knowledge"))
POLL_INTERVAL_SECONDS = float(os.getenv("AGENT_POLL_INTERVAL_SECONDS", "5"))
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "mock").strip().lower()
LLM_TIMEOUT_SECONDS = float(os.getenv("LLM_TIMEOUT_SECONDS", "45"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")
TRAFFIC_PROFILE_URL = os.getenv("TRAFFIC_PROFILE_URL", "http://corruption-adapter:8000/profile")
PAYMENT_ROUTER_URL = os.getenv("PAYMENT_ROUTER_URL", "http://payment-router:8000").rstrip("/")
OPSAI_AUTOMATION_URL = os.getenv("OPSAI_AUTOMATION_URL", "http://opsai-automation:8000").rstrip("/")
AUTOMATION_API_TOKEN = require_env("AUTOMATION_API_TOKEN")
CHECKOUT_SERVICE_URL = os.getenv("CHECKOUT_SERVICE_URL", "http://checkout-service:8000").rstrip("/")
EXTERNAL_AUTH_SERVICE_URL = os.getenv(
    "EXTERNAL_AUTH_SERVICE_URL",
    "http://external-auth-service:8000",
).rstrip("/")
PAYMENT_NODE_URLS_RAW = os.getenv(
    "PAYMENT_NODE_URLS",
    "payment-node-1=http://payment-node-1:8000,payment-node-2=http://payment-node-2:8000,payment-node-3=http://payment-node-3:8000",
)

ALLOWED_ACTIONS = {
    "collect_diagnostics",
    "collect_dependency_diagnostics",
    "drain_payment_node",
    "restart_payment_node",
    "rollback_payment_node",
    "scale_payment_capacity",
    "apply_traffic_control",
    "cleanup_disk_space",
    "renew_certificate",
    "bind_backup_certificate",
    "refresh_external_service_credentials",
    "no_action",
}

INVESTIGATIONS = Counter(
    "opsai_agent_investigations_total",
    "Agent investigations by outcome and mode.",
    ["outcome", "mode"],
)
LAST_RUN = Gauge(
    "opsai_agent_last_run_timestamp_seconds",
    "Latest investigation completion timestamp.",
)
DB_READY = Gauge("opsai_agent_database_ready", "Whether agent persistence is ready.")
PROVIDER_READY = Gauge(
    "opsai_agent_provider_ready",
    "Whether a configured real LLM provider is ready.",
    ["provider"],
)

app = FastAPI(title="PulseGuard Investigation", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())
stop_event = asyncio.Event()
knowledge_documents: list[dict[str, Any]] = []


class PredictionExplainRequest(BaseModel):
    prediction: dict[str, Any]
    evidence: dict[str, Any] = Field(default_factory=dict)


def parse_named_urls(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in raw.split(","):
        if "=" not in item:
            continue
        name, url = item.split("=", 1)
        if name.strip() and url.strip():
            result[name.strip()] = url.strip().rstrip("/")
    return result


PAYMENT_NODE_URLS = parse_named_urls(PAYMENT_NODE_URLS_RAW)

def real_ai_ready() -> bool:
    """Return whether the configured real LLM provider has all required settings."""
    if LLM_PROVIDER == "openai":
        return bool(OPENAI_API_KEY)
    if LLM_PROVIDER == "azure_openai":
        return bool(
            AZURE_OPENAI_ENDPOINT
            and AZURE_OPENAI_API_KEY
            and AZURE_OPENAI_DEPLOYMENT
        )
    return False


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def emit_log(level: str, event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": iso_now(),
                "level": level,
                "service": "opsai-agent",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
            default=str,
        ),
        flush=True,
    )


def db_connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)


def initialise_database_sync() -> None:
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with db_connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_investigations (
                        id UUID PRIMARY KEY,
                        incident_id UUID NOT NULL UNIQUE REFERENCES incidents(id) ON DELETE CASCADE,
                        status TEXT NOT NULL,
                        analysis_mode TEXT NOT NULL,
                        provider TEXT NOT NULL,
                        model TEXT NOT NULL DEFAULT '',
                        summary TEXT NOT NULL DEFAULT '',
                        root_cause TEXT NOT NULL DEFAULT '',
                        confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
                        customer_impact TEXT NOT NULL DEFAULT '',
                        recommended_action TEXT NOT NULL DEFAULT '',
                        action_name TEXT NOT NULL DEFAULT '',
                        action_parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
                        policy_decision TEXT NOT NULL DEFAULT '',
                        policy_reason TEXT NOT NULL DEFAULT '',
                        hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
                        evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
                        retrieved_knowledge JSONB NOT NULL DEFAULT '[]'::jsonb,
                        provider_endpoint_host TEXT NOT NULL DEFAULT '',
                        prompt_preview TEXT NOT NULL DEFAULT '',
                        request_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        response_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
                        token_usage JSONB NOT NULL DEFAULT '{}'::jsonb,
                        request_sent_at TIMESTAMPTZ,
                        response_received_at TIMESTAMPTZ,
                        llm_duration_ms INTEGER,
                        action_execution_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED',
                        action_executed BOOLEAN NOT NULL DEFAULT FALSE,
                        action_executor TEXT NOT NULL DEFAULT '',
                        action_execution_result JSONB NOT NULL DEFAULT '{}'::jsonb,
                        action_started_at TIMESTAMPTZ,
                        action_completed_at TIMESTAMPTZ,
                        error TEXT,
                        started_at TIMESTAMPTZ NOT NULL,
                        completed_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS agent_events (
                        id BIGSERIAL PRIMARY KEY,
                        investigation_id UUID NOT NULL REFERENCES agent_investigations(id) ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        message TEXT NOT NULL,
                        details JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS ix_agent_events_investigation "
                    "ON agent_events(investigation_id, created_at)"
                )
                for migration in (
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS provider_endpoint_host TEXT NOT NULL DEFAULT ''",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS prompt_preview TEXT NOT NULL DEFAULT ''",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS request_payload JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS response_payload JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS token_usage JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS request_sent_at TIMESTAMPTZ",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS response_received_at TIMESTAMPTZ",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS llm_duration_ms INTEGER",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_execution_status TEXT NOT NULL DEFAULT 'NOT_EVALUATED'",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_executed BOOLEAN NOT NULL DEFAULT FALSE",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_executor TEXT NOT NULL DEFAULT ''",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_execution_result JSONB NOT NULL DEFAULT '{}'::jsonb",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_started_at TIMESTAMPTZ",
                    "ALTER TABLE agent_investigations ADD COLUMN IF NOT EXISTS action_completed_at TIMESTAMPTZ",
                ):
                    conn.execute(migration)
            DB_READY.set(1)
            return
        except Exception as exc:
            last_error = exc
            DB_READY.set(0)
            time.sleep(3)
    raise RuntimeError(f"Agent database did not become ready: {last_error}")


def load_knowledge() -> None:
    global knowledge_documents
    payload = json.loads(
        (KNOWLEDGE_DIR / "catalog.json").read_text(encoding="utf-8")
    )
    knowledge_documents = payload.get("documents", [])
    emit_log("INFO", "knowledge_loaded", documentCount=len(knowledge_documents))


def tokenize(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_-]+", value.lower())
        if len(token) > 2
    }


def retrieve_knowledge(
    incident: dict[str, Any],
    evidence: dict[str, Any],
    limit: int = 4,
) -> list[dict[str, Any]]:
    query_text = " ".join(
        [
            str(incident.get("incident_type", "")),
            str(incident.get("title", "")),
            str(incident.get("node", "")),
            str(incident.get("summary", "")),
            json.dumps(evidence, default=str),
        ]
    )
    query_tokens = tokenize(query_text)
    ranked: list[tuple[float, dict[str, Any]]] = []
    for doc in knowledge_documents:
        doc_tokens = tokenize(
            " ".join(
                [
                    doc.get("title", ""),
                    " ".join(doc.get("keywords", [])),
                    doc.get("content", ""),
                ]
            )
        )
        score = float(len(query_tokens & doc_tokens))
        if incident.get("incident_type") in doc.get("incidentTypes", []):
            score += 12
        if doc.get("kind") == "runbook":
            score += 2
        if incident.get("node") and incident.get("node") in doc.get("content", ""):
            score += 1
        ranked.append((score, doc))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [
        {**doc, "retrievalScore": score}
        for score, doc in ranked[:limit]
        if score > 0
    ]


async def prometheus_query(query: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": query},
        )
        response.raise_for_status()
        payload = response.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return payload.get("data", {}).get("result", [])


def vector_map(rows: list[dict[str, Any]], label: str) -> dict[str, float]:
    values: dict[str, float] = {}
    for row in rows:
        try:
            values[row.get("metric", {}).get(label, "global")] = float(
                row.get("value", [0, 0])[1]
            )
        except (TypeError, ValueError):
            continue
    return values


async def safe_json_get(url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}


async def collect_evidence(incident: dict[str, Any]) -> dict[str, Any]:
    (
        latency_rows,
        retry_rows,
        checkout_p95_rows,
        failure_rows,
        throughput_rows,
        active_rows,
        failure_kind_rows,
        traffic_context,
        automation_context,
        external_auth_failure_rows,
        external_call_rows,
        external_service_health,
        checkout_health,
    ) = await asyncio.gather(
        prometheus_query(
            "histogram_quantile(0.95, sum by (le,node) "
            "(rate(opsai_router_node_duration_seconds_bucket[1m])))"
        ),
        prometheus_query(
            "sum(rate(opsai_router_retries_total[1m])) by (failed_node)"
        ),
        prometheus_query(
            "histogram_quantile(0.95, sum by (le) "
            "(rate(opsai_checkout_duration_seconds_bucket[1m])))"
        ),
        prometheus_query(
            '100 * sum(rate(opsai_checkout_requests_total{status="failed"}[1m])) '
            "/ clamp_min(sum(rate(opsai_checkout_requests_total[1m])), 0.001)"
        ),
        prometheus_query("sum(rate(opsai_checkout_requests_total[1m]))"),
        prometheus_query("opsai_router_node_active"),
        prometheus_query("sum(rate(opsai_router_failures_total[1m])) by (failed_node,error_kind)"),
        safe_json_get(TRAFFIC_PROFILE_URL),
        safe_json_get(f"{OPSAI_AUTOMATION_URL}/state"),
        prometheus_query(
            "sum(rate(opsai_external_service_auth_failures_total[30s])) by (service)"
        ),
        prometheus_query(
            "sum(rate(opsai_external_service_calls_total[30s])) by (service,status)"
        ),
        safe_json_get(f"{EXTERNAL_AUTH_SERVICE_URL}/health"),
        safe_json_get(f"{CHECKOUT_SERVICE_URL}/health"),
    )
    latencies = vector_map(latency_rows, "node")
    retries = vector_map(retry_rows, "failed_node")
    active = vector_map(active_rows, "node")
    failure_kinds: dict[str, dict[str, float]] = {}
    for row in failure_kind_rows:
        metric = row.get("metric", {})
        node_name = metric.get("failed_node", "unknown")
        error_kind = metric.get("error_kind", "unknown")
        try:
            value = float(row.get("value", [0, 0])[1])
        except (TypeError, ValueError, IndexError):
            continue
        failure_kinds.setdefault(node_name, {})[error_kind] = value
    checkout_p95 = next(
        iter(vector_map(checkout_p95_rows, "none").values()), 0.0
    )
    failure_pct = next(iter(vector_map(failure_rows, "none").values()), 0.0)
    throughput = next(iter(vector_map(throughput_rows, "none").values()), 0.0)
    node = incident.get("node") or ""
    node_latency = latencies.get(node, 0.0)
    peer_values = [
        value for name, value in latencies.items() if name != node and value > 0
    ]
    peer_average = sum(peer_values) / len(peer_values) if peer_values else 0.0
    isolation_ratio = node_latency / peer_average if peer_average > 0 else 0.0
    external_auth_failures = vector_map(external_auth_failure_rows, "service")
    external_calls: dict[str, dict[str, float]] = {}
    for row in external_call_rows:
        metric = row.get("metric", {})
        service_name = str(metric.get("service") or "unknown")
        status_name = str(metric.get("status") or "unknown")
        try:
            value = float(row.get("value", [0, 0])[1])
        except (TypeError, ValueError, IndexError):
            continue
        external_calls.setdefault(service_name, {})[status_name] = value
    aggregate_traffic = {
        key: traffic_context.get(key)
        for key in (
            "sourceMode", "streamConnected", "profile", "targetUsers",
            "baseTargetUsers", "spawnRate", "currentEventsPerMinute",
            "baselineEventsPerMinute", "activityRatio", "overrideActive",
            "trafficOverride", "connectionStatus", "fallbackReason",
        )
        if key in traffic_context
    }
    return {
        "collectedAt": iso_now(),
        "incidentEvidence": incident.get("evidence", {}),
        "nodeP95LatencySeconds": latencies,
        "retryRateByFailedNode": retries,
        "failureKindsByNode": failure_kinds,
        "routerNodeActive": active,
        "checkoutP95Seconds": checkout_p95,
        "checkoutFailurePercent": failure_pct,
        "checkoutRequestsPerSecond": throughput,
        "trafficContext": aggregate_traffic,
        "automationContext": {
            "disk": automation_context.get("disk", {}),
            "certificate": automation_context.get("certificate", {}),
        },
        "externalAuthentication": {
            "failureRateByService": external_auth_failures,
            "callRatesByServiceAndStatus": external_calls,
            "externalServiceHealth": external_service_health,
            "checkoutHealth": checkout_health,
            "secretValuesIncluded": False,
        },
        "affectedNode": node,
        "affectedNodeP95Seconds": node_latency,
        "peerAverageP95Seconds": peer_average,
        "affectedToPeerLatencyRatio": isolation_ratio,
        "notes": [
            "Evidence is collected from Prometheus and aggregate traffic context.",
            "The investigation agent has no connection to scenario-controller ground truth.",
            "Wikimedia event titles and contents are not sent to the AI provider; only aggregate traffic indicators may be included.",
            "The automation context contains bounded demo storage and certificate metadata, not scenario-controller ground truth.",
        ],
    }


async def collect_operational_diagnostics(incident: dict[str, Any]) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=8) as client:
        async def node_snapshot(node: str, base_url: str) -> dict[str, Any]:
            try:
                response = await client.get(f"{base_url}/admin/diagnostics")
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                return {"nodeId": node, "status": "unreachable", "error": str(exc)}
        node_rows = await asyncio.gather(*[
            node_snapshot(node, url) for node, url in PAYMENT_NODE_URLS.items()
        ])
        try:
            router_response = await client.get(f"{PAYMENT_ROUTER_URL}/nodes")
            router_response.raise_for_status()
            router_nodes = router_response.json()
        except Exception as exc:
            router_nodes = {"status": "unavailable", "error": str(exc)}
    return {
        "collectedAt": iso_now(),
        "incidentId": incident.get("id"),
        "scope": "payment-fleet" if not incident.get("node") else incident.get("node"),
        "paymentNodes": node_rows,
        "router": router_nodes,
    }


def mock_analysis(
    incident: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    incident_type = incident.get("incident_type", "")
    node = incident.get("node") or "the payment tier"
    ratio = evidence.get("affectedToPeerLatencyRatio", 0.0)
    if incident_type == "NODE_DISK_PRESSURE":
        disk = evidence.get("automationContext", {}).get("disk", {}) or incident.get("evidence", {}).get("storage", {})
        return {
            "summary": f"Bounded application storage is at {disk.get('usagePercent', 'unknown')}% and has crossed the controlled cleanup threshold.",
            "root_cause": "Synthetic temporary, cache and archived-log data is consuming the allowlisted demo storage volume.",
            "confidence": 0.96,
            "customer_impact": "New writes may fail and application stability can degrade if free space is not restored.",
            "hypotheses": [
                {"name": "Temporary/cache growth", "likelihood": "high", "support": "The storage breakdown identifies reclaimable allowlisted paths."},
                {"name": "Protected active-log growth", "likelihood": "medium", "support": "If controlled cleanup is insufficient, current logs require Infrastructure Support review."},
            ],
            "recommended_action": "Archive a cleanup manifest, remove only allowlisted synthetic temporary/cache/archived-log files, and verify usage below the recovery threshold.",
            "action_name": "cleanup_disk_space",
            "action_parameters": {"scope": "bounded-demo-storage", "archive_before_clear": True},
        }
    if incident_type == "TLS_CERTIFICATE_EXPIRING":
        certificate = evidence.get("automationContext", {}).get("certificate", {}) or incident.get("evidence", {}).get("certificate", {})
        return {
            "summary": f"The active TLS certificate for {certificate.get('hostname', incident.get('node') or 'the endpoint')} is near expiry.",
            "root_cause": "The certificate lifecycle threshold was crossed; renewal is required before expiry.",
            "confidence": 0.97,
            "customer_impact": "An expired certificate would cause TLS trust failures and prevent clients from reaching the endpoint.",
            "hypotheses": [
                {"name": "Normal certificate lifecycle", "likelihood": "high", "support": "The certificate metadata shows a valid hostname and a near-term expiry."},
                {"name": "Renewal path failure", "likelihood": "low", "support": "This becomes likely only if the allowlisted internal CA renewal attempt fails."},
            ],
            "recommended_action": "Renew through the allowlisted demo internal CA, validate hostname/SAN/issuer, atomically replace the binding and verify the extended expiry.",
            "action_name": "renew_certificate",
            "action_parameters": {"hostname": certificate.get("hostname", incident.get("node") or "checkout.pulseguard.local")},
        }
    if incident_type == "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE":
        service = incident.get("node") or incident.get("evidence", {}).get("externalService") or "partner-risk-service"
        return {
            "summary": f"Checkout calls to {service} are being rejected with bearer-token authentication failures.",
            "root_cause": "The checkout client token generation no longer matches the active token generation accepted by the allowlisted external partner service.",
            "confidence": 0.98,
            "customer_impact": "Checkout requests fail before payment processing, causing a direct customer-facing outage while the external service itself remains reachable.",
            "hypotheses": [
                {"name": "Credential rotation not propagated", "likelihood": "high", "support": "The external endpoint returns authentication failures while network reachability remains healthy."},
                {"name": "Incorrect service identity or scope", "likelihood": "medium", "support": "A malformed audience or token scope can produce the same 401 pattern."},
            ],
            "recommended_action": "Refresh the allowlisted partner credential, update checkout through its internal administration endpoint, and verify a probe call without exposing the token in logs.",
            "action_name": "refresh_external_service_credentials",
            "action_parameters": {"service": str(service), "client": "checkout-service"},
        }
    if incident_type == "PAYMENT_NODE_HUNG":
        return {
            "summary": f"{node} remains reachable but its payment worker is stuck and requests exceed the router deadline.",
            "root_cause": "The controlled worker state is hung; the process health endpoint remains reachable, so this is not a container outage.",
            "confidence": 0.96,
            "customer_impact": "Requests routed to the node time out and retry through peers, increasing latency and reducing redundancy.",
            "hypotheses": [
                {"name": "Application worker deadlock", "likelihood": "high", "support": "The node reports an explicit hung worker state while the service process remains healthy."},
                {"name": "Downstream call never returns", "likelihood": "medium", "support": "A blocked dependency can produce the same timeout pattern."},
            ],
            "recommended_action": "Request approval for a bounded application restart of the affected node, then verify fault state, restart generation, payment success and peer stability.",
            "action_name": "restart_payment_node",
            "action_parameters": {"node": node, "restartMode": "bounded_application_restart"},
        }
    if incident_type == "PAYMENT_NODE_LATENCY":
        return {
            "summary": (
                f"{node} is an isolated latency outlier. Its p95 is materially "
                "above peer nodes and customer checkout p95 has increased."
            ),
            "root_cause": (
                "Most likely degradation in the node-local processing or network "
                "path. The available evidence does not prove which of those two "
                "domains is responsible."
            ),
            "confidence": 0.88 if ratio >= 3 else 0.72,
            "customer_impact": (
                "Customers whose payments are routed to the slow node wait longer. "
                "Round-robin continues assigning roughly one-third of requests to "
                "it while it remains active."
            ),
            "hypotheses": [
                {
                    "name": "Isolated network-path degradation",
                    "likelihood": "high",
                    "support": (
                        "Affected node latency is far above peers while the rest of "
                        "the tier remains healthy."
                    ),
                },
                {
                    "name": "Node-local application saturation",
                    "likelihood": "medium",
                    "support": (
                        "The signal is isolated to one service instance, but CPU "
                        "and process diagnostics are not yet collected."
                    ),
                },
                {
                    "name": "Demand surge",
                    "likelihood": "low",
                    "support": (
                        "A demand surge would normally affect peer nodes as well."
                    ),
                },
            ],
            "recommended_action": (
                "Collect diagnostics, then request approval to drain the affected "
                "node while two healthy nodes carry traffic."
            ),
            "action_name": "drain_payment_node",
            "action_parameters": {"node": node},
        }
    if incident_type == "PAYMENT_NODE_TIMEOUT":
        return {
            "summary": (
                f"Requests routed to {node} are timing out and being retried "
                "through healthy nodes."
            ),
            "root_cause": (
                "Likely loss of reachability or severe dependency latency on the "
                "affected node path."
            ),
            "confidence": 0.9,
            "customer_impact": (
                "Failover protects availability, but affected requests experience "
                "timeout-and-retry delay and healthy nodes carry additional load."
            ),
            "hypotheses": [
                {
                    "name": "Node path unavailable",
                    "likelihood": "high",
                    "support": "Retries identify a single failed node.",
                },
                {
                    "name": "Shared provider outage",
                    "likelihood": "low",
                    "support": "Other nodes continue processing payments.",
                },
            ],
            "recommended_action": (
                "Collect diagnostics and request approval to drain the repeatedly "
                "failing node."
            ),
            "action_name": "drain_payment_node",
            "action_parameters": {"node": node},
        }
    if incident_type == "PAYMENT_NODE_UNAVAILABLE":
        return {
            "summary": f"{node} is unavailable to the router while peer nodes continue serving traffic.",
            "root_cause": "The observed path or node is not accepting successful payment calls.",
            "confidence": 0.9,
            "customer_impact": "Failover protects availability, but retries add latency and reduce redundancy.",
            "hypotheses": [{"name": "Node or path unavailable", "likelihood": "high", "support": "The affected node has a near-total failure ratio while peers remain successful."}],
            "recommended_action": "Keep the unavailable node out of new traffic and collect diagnostics before restart or restore.",
            "action_name": "drain_payment_node",
            "action_parameters": {"node": node},
        }
    if incident_type == "PAYMENT_NODE_NETWORK_INSTABILITY":
        return {
            "summary": f"{node} has intermittent network failures mixed with successful calls.",
            "root_cause": "The observed pattern is consistent with an unstable transport path rather than complete node loss.",
            "confidence": 0.82,
            "customer_impact": "A subset of payments incur retry delay while successful calls continue.",
            "hypotheses": [{"name": "Intermittent connection resets", "likelihood": "high", "support": "Both failures and successes are present for the same node."}],
            "recommended_action": "Collect diagnostics and drain the node only if instability persists.",
            "action_name": "collect_diagnostics",
            "action_parameters": {"node": node},
        }
    if incident_type == "PAYMENT_NODE_FLAPPING":
        return {
            "summary": f"{node} repeatedly changes between available and unavailable states.",
            "root_cause": "The node or one of its dependencies is unstable and has not remained healthy for the verification window.",
            "confidence": 0.88,
            "customer_impact": "Repeated state changes cause retries and make routing capacity unpredictable.",
            "hypotheses": [{"name": "Unstable node lifecycle or dependency", "likelihood": "high", "support": "Multiple availability transitions were observed in a short window."}],
            "recommended_action": "Keep the node isolated and collect diagnostics until it remains stable.",
            "action_name": "collect_diagnostics",
            "action_parameters": {"node": node},
        }
    if incident_type == "PAYMENT_SHARED_DEPENDENCY_OUTAGE":
        return {
            "summary": "The payment fleet is failing with the same downstream dependency signature.",
            "root_cause": "A shared payment-authorisation dependency is unavailable across multiple nodes.",
            "confidence": 0.95,
            "customer_impact": "Payments fail across the fleet; draining individual nodes would not restore service.",
            "hypotheses": [{"name": "Shared dependency outage", "likelihood": "very high", "support": "The same dependency failure is observed across multiple payment nodes."}],
            "recommended_action": "Collect dependency diagnostics and activate the dependency recovery path; do not drain individual nodes.",
            "action_name": "collect_dependency_diagnostics",
            "action_parameters": {},
        }
    if incident_type == "PAYMENT_FLEET_CAPACITY_DEGRADATION":
        return {
            "summary": "Multiple payment nodes are slow while aggregate demand is elevated.",
            "root_cause": "The pattern is consistent with fleet-wide capacity pressure rather than an isolated node defect.",
            "confidence": 0.85,
            "customer_impact": "Checkout latency rises broadly across the payment tier.",
            "hypotheses": [{"name": "Demand-driven capacity saturation", "likelihood": "high", "support": "Multiple nodes degrade together during elevated traffic."}],
            "recommended_action": "Increase bounded worker capacity on payment-node-1 and payment-node-2 while node 3 is unavailable, then verify fleet latency and checkout failures recover.",
            "action_name": "scale_payment_capacity",
            "action_parameters": {
                "scope": "payment-fleet",
                "targetNodes": ["payment-node-1", "payment-node-2"],
                "capacityUnits": 2,
                "pressureMs": 1200,
                "simulatedInfrastructure": True,
            },
        }
    return {
        "summary": (
            "Customer checkout failures are elevated and require broader diagnostics."
        ),
        "root_cause": "The current evidence does not isolate one payment node.",
        "confidence": 0.55,
        "customer_impact": "Some customer checkouts are failing.",
        "hypotheses": [
            {
                "name": "Payment-tier degradation",
                "likelihood": "medium",
                "support": "Checkout failures are elevated.",
            },
            {
                "name": "Router or shared dependency issue",
                "likelihood": "medium",
                "support": "No single node is proven by current evidence.",
            },
        ],
        "recommended_action": "Collect diagnostics before isolating any node.",
        "action_name": "collect_diagnostics",
        "action_parameters": {},
    }


def extract_output_text(payload: dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str):
        return payload["output_text"]
    chunks: list[str] = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"} and isinstance(
                content.get("text"), str
            ):
                chunks.append(content["text"])
    return "\n".join(chunks)


def parse_json_text(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return json.loads(cleaned)


async def real_llm_analysis(
    incident: dict[str, Any],
    evidence: dict[str, Any],
    knowledge: list[dict[str, Any]],
) -> tuple[dict[str, Any], str, str, dict[str, Any]]:
    instructions = (
        "You are the investigation specialist inside PulseGuard. Use only "
        "the supplied incident, Prometheus evidence and retrieved knowledge. "
        "Never claim you read scenario-controller ground truth, application logs, "
        "traces, CPU or memory unless supplied. Clearly separate observed facts "
        "from inference. Recommend exactly one action_name from: "
        "collect_diagnostics, collect_dependency_diagnostics, drain_payment_node, "
        "restart_payment_node, rollback_payment_node, scale_payment_capacity, "
        "apply_traffic_control, cleanup_disk_space, renew_certificate, "
        "bind_backup_certificate, refresh_external_service_credentials, no_action. "
        "Do not authorize or execute actions. "
        "Return JSON only with keys summary, root_cause, confidence (0 to 1), "
        "customer_impact, hypotheses (array of objects with name, likelihood, "
        "support), recommended_action, action_name, action_parameters."
    )
    context = {
        "incident": incident,
        "prometheusEvidence": evidence,
        "retrievedKnowledge": knowledge,
    }
    body: dict[str, Any] = {
        "instructions": instructions,
        "input": json.dumps(context, default=str),
        "store": False,
        "max_output_tokens": 1400,
    }
    if LLM_PROVIDER == "openai":
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        url = f"{OPENAI_BASE_URL}/responses"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        body["model"] = OPENAI_MODEL
        model = OPENAI_MODEL
    elif LLM_PROVIDER == "azure_openai":
        if not (
            AZURE_OPENAI_ENDPOINT
            and AZURE_OPENAI_API_KEY
            and AZURE_OPENAI_DEPLOYMENT
        ):
            raise RuntimeError(
                "AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY and "
                "AZURE_OPENAI_DEPLOYMENT are required"
            )
        endpoint = AZURE_OPENAI_ENDPOINT
        if endpoint.endswith("/openai/v1"):
            url = f"{endpoint}/responses"
        else:
            url = f"{endpoint}/openai/v1/responses"
        headers = {
            "api-key": AZURE_OPENAI_API_KEY,
            "Content-Type": "application/json",
        }
        body["model"] = AZURE_OPENAI_DEPLOYMENT
        model = AZURE_OPENAI_DEPLOYMENT
    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")

    request_sent_at = utc_now()
    started = time.perf_counter()
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT_SECONDS) as client:
        response = await client.post(url, headers=headers, json=body)
        response.raise_for_status()
        payload = response.json()
    response_received_at = utc_now()
    duration_ms = round((time.perf_counter() - started) * 1000)
    text = extract_output_text(payload)
    if not text:
        raise RuntimeError("LLM response did not contain output text")
    parsed = parse_json_text(text)
    endpoint_host = urlparse(url).netloc
    usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
    audit = {
        "providerEndpointHost": endpoint_host,
        "promptPreview": instructions + "\n\nINPUT CONTEXT\n" + json.dumps(context, indent=2, default=str),
        "requestPayload": {
            "provider": LLM_PROVIDER,
            "model": model,
            "endpointHost": endpoint_host,
            "instructions": instructions,
            "inputContext": context,
            "store": False,
            "maxOutputTokens": 1400,
        },
        "responsePayload": {
            "responseId": payload.get("id", "") if isinstance(payload, dict) else "",
            "status": payload.get("status", "completed") if isinstance(payload, dict) else "completed",
            "analysis": parsed,
        },
        "tokenUsage": usage if isinstance(usage, dict) else {},
        "requestSentAt": request_sent_at,
        "responseReceivedAt": response_received_at,
        "llmDurationMs": duration_ms,
    }
    return parsed, LLM_PROVIDER, model, audit

def validate_analysis(
    result: dict[str, Any],
    incident: dict[str, Any],
) -> dict[str, Any]:
    action = str(result.get("action_name", "collect_diagnostics"))
    if action not in ALLOWED_ACTIONS:
        action = "collect_diagnostics"
    incident_type = str(incident.get("incident_type") or "")
    # The LLM proposes a response, but runbook/action compatibility is enforced
    # deterministically before policy evaluation. This keeps real-AI analysis
    # while making the demo repair path repeatable and evidence-bound.
    if incident_type == "NODE_DISK_PRESSURE":
        action = "cleanup_disk_space"
        result["recommended_action"] = (
            "Archive old allowlisted logs and remove bounded synthetic temporary/cache files, "
            "then verify disk usage falls below the recovery threshold."
        )
    elif incident_type == "TLS_CERTIFICATE_EXPIRING":
        action = "renew_certificate"
        result["recommended_action"] = (
            "Renew the allowlisted demo certificate, validate hostname/SAN/issuer, atomically "
            "replace the binding, and verify the new expiry."
        )
    elif incident_type == "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE":
        action = "refresh_external_service_credentials"
        service = (
            incident.get("node")
            or (incident.get("evidence") or {}).get("externalService")
            or "partner-risk-service"
        )
        result["recommended_action"] = (
            "Refresh the allowlisted external-service credential, update checkout through "
            "the internal administration endpoint, and verify a probe call without logging "
            "or returning the secret."
        )
        result["action_parameters"] = {
            "service": str(service),
            "client": "checkout-service",
            "secretHandling": "redacted",
        }
    elif incident_type == "PAYMENT_NODE_HUNG":
        action = "restart_payment_node"
        result["recommended_action"] = (
            "Request approval for a bounded application restart of the affected payment "
            "worker, then verify the restart generation and successful payment processing."
        )
    elif incident_type == "PAYMENT_FLEET_CAPACITY_DEGRADATION":
        evidence = incident.get("evidence") or {}
        unavailable_nodes = set(str(item) for item in evidence.get("unavailableNodes", []))
        affected_nodes = set(str(item) for item in evidence.get("affectedNodes", []))
        if (
            "payment-node-3" in unavailable_nodes
            and {"payment-node-1", "payment-node-2"}.issubset(affected_nodes)
        ):
            action = "scale_payment_capacity"
            result["recommended_action"] = (
                "Increase bounded application worker capacity on payment-node-1 and "
                "payment-node-2 while node 3 is unavailable, then verify fleet p95 latency "
                "and checkout failures recover."
            )
    try:
        confidence = min(1.0, max(0.0, float(result.get("confidence", 0.5))))
    except (TypeError, ValueError):
        confidence = 0.5
    hypotheses = result.get("hypotheses", [])
    if not isinstance(hypotheses, list):
        hypotheses = []
    parameters = result.get("action_parameters", {})
    if not isinstance(parameters, dict):
        parameters = {}
    if action in {
        "drain_payment_node",
        "restart_payment_node",
        "rollback_payment_node",
    }:
        parameters["node"] = incident.get("node") or parameters.get("node", "")
    if action == "refresh_external_service_credentials":
        parameters = {
            "service": "partner-risk-service",
            "client": "checkout-service",
            "secretHandling": "redacted",
        }
    if action == "scale_payment_capacity":
        # Never accept arbitrary model-provided infrastructure targets. This demo
        # scales only the two allowlisted healthy peers and only within a bounded
        # application worker-capacity profile.
        parameters = {
            "scope": "payment-fleet",
            "targetNodes": ["payment-node-1", "payment-node-2"],
            "capacityUnits": 2,
            "pressureMs": 1000,
            "simulatedInfrastructure": True,
        }
    return {
        "summary": str(result.get("summary", "Investigation completed.")),
        "root_cause": str(
            result.get("root_cause", "Insufficient evidence to isolate a cause.")
        ),
        "confidence": confidence,
        "customer_impact": str(
            result.get("customer_impact", "Impact is not yet quantified.")
        ),
        "hypotheses": hypotheses[:5],
        "recommended_action": str(
            result.get("recommended_action", "Collect diagnostics.")
        ),
        "action_name": action,
        "action_parameters": parameters,
    }



def correlated_capacity_evidence(
    evidence: dict[str, Any],
    incident: dict[str, Any],
) -> tuple[set[str], set[str], dict[str, Any]]:
    """Build one policy view from incident evidence and collected telemetry.

    The collector stores the original detector payload under
    prometheusEvidence.incidentEvidence, while the policy previously looked
    only at prometheusEvidence top-level keys. This helper intentionally uses
    operational evidence only; it does not read Scenario Controller state.
    """
    merged: dict[str, Any] = {}

    nested = evidence.get("incidentEvidence", {}) if isinstance(evidence, dict) else {}
    if isinstance(nested, dict):
        merged.update(nested)

    incident_evidence = (
        incident.get("evidence", {}) if isinstance(incident, dict) else {}
    )
    if isinstance(incident_evidence, dict):
        merged.update(incident_evidence)

    unavailable_nodes: set[str] = set()
    affected_nodes: set[str] = set()

    for source in (evidence, nested, incident_evidence, merged):
        if not isinstance(source, dict):
            continue
        unavailable_nodes.update(
            str(item)
            for item in source.get("unavailableNodes", [])
            if str(item)
        )
        affected_nodes.update(
            str(item)
            for item in source.get("affectedNodes", [])
            if str(item)
        )

    node_diagnostics = merged.get("nodeDiagnostics")
    if not isinstance(node_diagnostics, dict) and isinstance(evidence, dict):
        node_diagnostics = evidence.get("nodeDiagnostics")
    if not isinstance(node_diagnostics, dict):
        node_diagnostics = {}

    for node_name, diagnostic in node_diagnostics.items():
        if not isinstance(diagnostic, dict):
            continue
        fault_mode = str(diagnostic.get("faultMode") or "").lower()
        accepting = diagnostic.get("acceptingPayments")
        process_status = str(diagnostic.get("processStatus") or "").lower()
        if (
            fault_mode == "unavailable"
            or accepting is False
            or process_status in {"stopped", "failed", "unavailable"}
        ):
            unavailable_nodes.add(str(node_name))

    return unavailable_nodes, affected_nodes, node_diagnostics

def policy_preview(
    action: str,
    parameters: dict[str, Any],
    evidence: dict[str, Any],
    incident: dict[str, Any],
) -> tuple[str, str]:
    incident_type = str(incident.get("incident_type") or "")
    retrying_nodes = [
        name for name, value in evidence.get("retryRateByFailedNode", {}).items()
        if float(value or 0) > 0.01
    ]
    if action in {"collect_diagnostics", "collect_dependency_diagnostics"}:
        return "AUTO_ALLOWED", "Read-only diagnostic collection is allowlisted."
    if action == "cleanup_disk_space":
        if incident_type != "NODE_DISK_PRESSURE":
            return "BLOCKED", "Controlled storage cleanup is only valid for NODE_DISK_PRESSURE incidents."
        return "AUTO_ALLOWED", "Cleanup is restricted to the bounded demo volume, uses archive-before-clear, and cannot accept arbitrary paths from the model."
    if action == "renew_certificate":
        if incident_type != "TLS_CERTIFICATE_EXPIRING":
            return "BLOCKED", "Automatic renewal is only valid for the allowlisted demo certificate lifecycle incident."
        return "AUTO_ALLOWED", "The hostname and internal demo CA are allowlisted; replacement binding and expiry are verified after renewal."
    if action == "refresh_external_service_credentials":
        if incident_type != "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE":
            return "BLOCKED", "Credential refresh is only valid for an external-service authentication incident."
        service = str(parameters.get("service") or "")
        client = str(parameters.get("client") or "")
        if service != "partner-risk-service" or client != "checkout-service":
            return "BLOCKED", "Credential refresh is restricted to the allowlisted partner-risk-service and checkout-service client."
        return (
            "AUTO_ALLOWED",
            "The action rotates one bounded synthetic partner credential, updates only checkout-service through an internal authenticated endpoint, redacts the secret, and verifies a probe call.",
        )
    if action == "bind_backup_certificate":
        return "APPROVAL_REQUIRED", "Binding a backup certificate is a temporary traffic-facing workaround and requires operator approval."
    if action == "drain_payment_node":
        if incident_type == "PAYMENT_SHARED_DEPENDENCY_OUTAGE" or len(retrying_nodes) >= 2:
            return "BLOCKED", "Multiple payment nodes are implicated; draining an individual node could remove fleet capacity without addressing the shared cause."
        node = parameters.get("node", "")
        healthy_peers = [
            name
            for name, active in evidence.get("routerNodeActive", {}).items()
            if name != node and active >= 1
        ]
        if len(healthy_peers) >= 2:
            return (
                "APPROVAL_REQUIRED",
                "Two active peer nodes remain, but draining changes live routing and requires operator approval.",
            )
        return "BLOCKED", "Fewer than two active peer nodes remain."
    if action in {"restart_payment_node", "rollback_payment_node"}:
        return (
            "APPROVAL_REQUIRED",
            "Restart and rollback are controlled changes requiring explicit approval.",
        )
    if action == "scale_payment_capacity":
        if incident_type != "PAYMENT_FLEET_CAPACITY_DEGRADATION":
            return "BLOCKED", "Bounded capacity scaling is only valid for PAYMENT_FLEET_CAPACITY_DEGRADATION incidents."
        unavailable_nodes, affected_nodes, node_diagnostics = correlated_capacity_evidence(
            evidence,
            incident,
        )
        if "payment-node-3" not in unavailable_nodes:
            return (
                "BLOCKED",
                "Automatic peer scaling requires operational evidence that payment-node-3 is unavailable. "
                f"Observed unavailable nodes: {sorted(unavailable_nodes)}.",
            )
        if not {"payment-node-1", "payment-node-2"}.issubset(affected_nodes):
            return (
                "BLOCKED",
                "Both allowlisted peer nodes must show correlated capacity pressure before automatic scaling. "
                f"Observed affected nodes: {sorted(affected_nodes)}.",
            )
        target_nodes = sorted(set(str(item) for item in parameters.get("targetNodes", [])))
        if target_nodes != ["payment-node-1", "payment-node-2"]:
            return "BLOCKED", "Capacity scaling targets must be exactly the two allowlisted healthy peer nodes."
        units = int(parameters.get("capacityUnits", 0) or 0)
        if units != 2:
            return "BLOCKED", "The automatic demo policy permits exactly two bounded capacity units."
        node3 = node_diagnostics.get("payment-node-3", {})
        return (
            "AUTO_ALLOWED",
            "Node-3 unavailability is confirmed by detector evidence and direct node diagnostics "
            f"(faultMode={node3.get('faultMode')}, acceptingPayments={node3.get('acceptingPayments')}). "
            "The action changes only bounded application worker-capacity settings on payment-node-1 and "
            "payment-node-2; it does not use the Docker socket or modify host CPU or memory.",
        )
    if action == "apply_traffic_control":
        return (
            "APPROVAL_REQUIRED",
            "Traffic-control changes require operator approval in this portfolio environment.",
        )
    if action == "no_action":
        return "NO_ACTION", "No operational action was recommended."
    return "BLOCKED", "Action is not allowlisted."


async def execute_policy_action(
    investigation_id: str,
    incident: dict[str, Any],
    action: str,
    decision: str,
) -> dict[str, Any]:
    started = utc_now()
    if decision == "AUTO_ALLOWED" and action in {"cleanup_disk_space", "renew_certificate", "scale_payment_capacity", "refresh_external_service_credentials"}:
        endpoint_by_action = {
            "cleanup_disk_space": "/actions/cleanup-disk",
            "renew_certificate": "/actions/renew-certificate",
            "scale_payment_capacity": "/actions/scale-payment-capacity",
            "refresh_external_service_credentials": "/actions/refresh-external-credentials",
        }
        endpoint = endpoint_by_action[action]
        add_agent_event_sync(
            investigation_id,
            "ACTION_STARTED",
            f"Allowlisted remediation {action} started",
            {"action": action, "executor": "opsai-automation"},
            started,
        )
        try:
            async with httpx.AsyncClient(timeout=45) as client:
                response = await client.post(
                    f"{OPSAI_AUTOMATION_URL}{endpoint}",
                    headers={"X-OpsAI-Automation-Token": AUTOMATION_API_TOKEN},
                    json={
                        "incidentId": str(incident.get("id")),
                        "requestedBy": "opsai-agent",
                        "parameters": incident.get("_validated_action_parameters", {}),
                    },
                )
                payload = response.json() if response.content else {}
                response.raise_for_status()
            completed = utc_now()
            add_agent_event_sync(
                investigation_id,
                "ACTION_EXECUTED",
                f"Automatic remediation {action} completed",
                {"action": action, "result": payload},
                completed,
            )
            return {
                "status": "SUCCEEDED",
                "executed": True,
                "executor": "opsai-automation:governed-remediation",
                "result": payload,
                "startedAt": started,
                "completedAt": completed,
            }
        except Exception as exc:
            completed = utc_now()
            result = {"error": str(exc)}
            if isinstance(exc, httpx.HTTPStatusError):
                try:
                    result["response"] = exc.response.json()
                except Exception:
                    result["response"] = exc.response.text[:1000]
            add_agent_event_sync(
                investigation_id,
                "ACTION_FAILED",
                f"Automatic remediation {action} failed",
                {"action": action, "result": result},
                completed,
            )
            return {
                "status": "FAILED",
                "executed": True,
                "executor": "opsai-automation:governed-remediation",
                "result": result,
                "startedAt": started,
                "completedAt": completed,
            }
    if decision == "AUTO_ALLOWED" and action in {"collect_diagnostics", "collect_dependency_diagnostics"}:
        add_agent_event_sync(
            investigation_id,
            "ACTION_STARTED",
            "Allowlisted diagnostic collection started",
            {"action": action, "executor": "opsai-agent"},
            started,
        )
        try:
            result = await collect_operational_diagnostics(incident)
            completed = utc_now()
            add_agent_event_sync(
                investigation_id,
                "ACTION_EXECUTED",
                "Read-only diagnostics collected",
                {"action": action, "result": result},
                completed,
            )
            return {
                "status": "SUCCEEDED",
                "executed": True,
                "executor": "opsai-agent:diagnostic-collector",
                "result": result,
                "startedAt": started,
                "completedAt": completed,
            }
        except Exception as exc:
            completed = utc_now()
            add_agent_event_sync(
                investigation_id,
                "ACTION_FAILED",
                "Diagnostic collection failed",
                {"action": action, "error": str(exc)},
                completed,
            )
            return {
                "status": "FAILED",
                "executed": True,
                "executor": "opsai-agent:diagnostic-collector",
                "result": {"error": str(exc)},
                "startedAt": started,
                "completedAt": completed,
            }
    status_by_decision = {
        "APPROVAL_REQUIRED": "NOT_EXECUTED_APPROVAL_REQUIRED",
        "BLOCKED": "NOT_EXECUTED_BLOCKED",
        "NO_ACTION": "NOT_APPLICABLE",
    }
    status = status_by_decision.get(decision, "NOT_EXECUTED")
    add_agent_event_sync(
        investigation_id,
        "ACTION_NOT_EXECUTED",
        "Recommended action was not executed",
        {"action": action, "policyDecision": decision, "executionStatus": status},
        started,
    )
    return {
        "status": status,
        "executed": False,
        "executor": "deterministic-policy-gate",
        "result": {"reason": "Execution was prevented by the deterministic policy decision."},
        "startedAt": None,
        "completedAt": started,
    }


def mock_prediction_analysis(
    prediction: dict[str, Any],
    evidence: dict[str, Any],
) -> dict[str, Any]:
    prediction_type = str(prediction.get("predictionType") or "")
    if prediction_type == "FREQUENT_ISSUE_PATTERN":
        return {
            "summary": (
                "The same operational issue has crossed the configured recurrence "
                "threshold, which suggests a persistent or repeatedly triggered cause."
            ),
            "likely_impact": (
                "Repeated incidents increase support effort and may develop into a "
                "larger reliability problem if the shared cause is not removed."
            ),
            "contributing_factors": [
                "Repeated incident type and scope inside the rolling lookback window",
                "Short interval between matching occurrences",
                "Previous resolutions may have treated symptoms rather than the root cause",
            ],
            "confidence": prediction.get("confidence", 0.78),
            "recommended_preventive_action": (
                "Compare evidence and resolution history across occurrences, identify "
                "the common trigger, and create a problem-management investigation "
                "before the issue becomes more frequent."
            ),
            "action_name": "collect_diagnostics",
        }

    if prediction_type == "PREDICTED_DISK_PRESSURE":
        return {
            "summary": "Application storage is growing steadily and is forecast to cross the critical threshold within the prediction horizon.",
            "likely_impact": "New writes and log persistence may fail if the trend continues.",
            "contributing_factors": [
                "Sustained positive disk-usage slope",
                "Forecasted threshold crossing inside the configured horizon",
                "Allowlisted temporary and archived-log paths contribute to growth",
            ],
            "confidence": prediction.get("confidence", 0.8),
            "recommended_preventive_action": "Collect storage diagnostics now and prepare archive-before-cleanup of allowlisted paths before the incident threshold is reached.",
            "action_name": "collect_diagnostics",
        }
    if prediction_type == "PREDICTED_PAYMENT_NODE_DEGRADATION":
        return {
            "summary": "One payment node is trending toward the latency incident threshold while peer nodes remain comparatively stable.",
            "likely_impact": "Customers routed to the degrading node are likely to experience slower payment processing before a hard threshold alert fires.",
            "contributing_factors": [
                "Positive processing-latency slope",
                "Time-to-threshold inside the prediction horizon",
                "Peer comparison indicates an isolated trend",
            ],
            "confidence": prediction.get("confidence", 0.78),
            "recommended_preventive_action": "Collect node diagnostics and prepare an approval-based drain or restart only if the trend continues and peer capacity remains healthy.",
            "action_name": "collect_diagnostics",
        }
    if prediction_type == "PREDICTED_CAPACITY_SATURATION":
        return {
            "summary": "The remaining payment capacity is likely to become insufficient under the current demand and node-availability trend.",
            "likely_impact": "Checkout latency may breach the service threshold and retries may increase.",
            "contributing_factors": [
                "Reduced available node count",
                "Increasing payment-processing p95",
                "Rising traffic-to-capacity ratio",
            ],
            "confidence": prediction.get("confidence", 0.8),
            "recommended_preventive_action": "Prepare bounded peer capacity scaling and require deterministic governance to validate node availability, target nodes and maximum units.",
            "action_name": "no_action",
        }
    return {
        "summary": "A monitored operational signal is trending toward a configured risk threshold.",
        "likely_impact": "Service reliability may degrade if the trend continues.",
        "contributing_factors": ["Deterministic forecast and recent metric trend"],
        "confidence": prediction.get("confidence", 0.65),
        "recommended_preventive_action": "Collect diagnostics and continue observation.",
        "action_name": "collect_diagnostics",
    }


async def real_llm_prediction_analysis(
    prediction: dict[str, Any],
    evidence: dict[str, Any],
) -> tuple[dict[str, Any], str, str]:
    instructions = (
        "You are the predictive operations analyst inside PulseGuard. "
        "The numerical forecast, probability, slope and time-to-threshold were "
        "calculated deterministically; do not recalculate or invent them. Use only "
        "the supplied prediction and evidence. Explain why the risk matters, clearly "
        "separate observed trend from inference, and recommend one preventive action "
        "from collect_diagnostics, no_action, cleanup_disk_space, renew_certificate, "
        "scale_payment_capacity. Do not authorize or execute anything. Return JSON "
        "only with keys summary, likely_impact, contributing_factors (array), "
        "confidence, recommended_preventive_action, action_name."
    )
    context = {"prediction": prediction, "evidence": evidence}
    body: dict[str, Any] = {
        "instructions": instructions,
        "input": json.dumps(context, default=str),
        "store": False,
        "max_output_tokens": 900,
    }
    if LLM_PROVIDER == "openai":
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY is not configured")
        url = f"{OPENAI_BASE_URL}/responses"
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        }
        body["model"] = OPENAI_MODEL
        model = OPENAI_MODEL
    elif LLM_PROVIDER == "azure_openai":
        if not (AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY and AZURE_OPENAI_DEPLOYMENT):
            raise RuntimeError("Azure OpenAI configuration is incomplete")
        endpoint = AZURE_OPENAI_ENDPOINT
        url = (
            f"{endpoint}/responses"
            if endpoint.endswith("/openai/v1")
            else f"{endpoint}/openai/v1/responses"
        )
        headers = {"api-key": AZURE_OPENAI_API_KEY, "Content-Type": "application/json"}
        body["model"] = AZURE_OPENAI_DEPLOYMENT
        model = AZURE_OPENAI_DEPLOYMENT
    else:
        raise RuntimeError(f"Unsupported LLM_PROVIDER: {LLM_PROVIDER}")

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT_SECONDS) as client:
        response = await client.post(url, headers=headers, json=body)
        response.raise_for_status()
        payload = response.json()
    output = extract_output_text(payload)
    if not output:
        raise RuntimeError("Prediction explanation response did not contain output text")
    parsed = parse_json_text(output)
    return parsed, LLM_PROVIDER, model


@app.post("/api/predictions/explain")
async def explain_prediction(request: PredictionExplainRequest) -> dict[str, Any]:
    prediction = request.prediction if isinstance(request.prediction, dict) else {}
    evidence = request.evidence if isinstance(request.evidence, dict) else {}
    mode = "DETERMINISTIC_EXPLANATION"
    provider = "mock"
    model = "built-in"
    try:
        if real_ai_ready():
            analysis, provider, model = await real_llm_prediction_analysis(prediction, evidence)
            mode = "REAL_AI"
        else:
            analysis = mock_prediction_analysis(prediction, evidence)
    except Exception as exc:
        emit_log("WARNING", "prediction_explanation_fallback", error=str(exc))
        analysis = mock_prediction_analysis(prediction, evidence)
        analysis["fallbackReason"] = str(exc)

    try:
        confidence = min(1.0, max(0.0, float(analysis.get("confidence", prediction.get("confidence", 0.5)))))
    except (TypeError, ValueError):
        confidence = float(prediction.get("confidence", 0.5) or 0.5)
    factors = analysis.get("contributing_factors", [])
    if not isinstance(factors, list):
        factors = [str(factors)]
    action = str(analysis.get("action_name") or "collect_diagnostics")
    if action not in {"collect_diagnostics", "no_action", "cleanup_disk_space", "renew_certificate", "scale_payment_capacity"}:
        action = "collect_diagnostics"
    return {
        "analysisMode": mode,
        "provider": provider,
        "model": model,
        "summary": str(analysis.get("summary") or "Prediction explained."),
        "likelyImpact": str(analysis.get("likely_impact") or "Operational risk may increase."),
        "contributingFactors": [str(item) for item in factors[:6]],
        "confidence": confidence,
        "recommendedPreventiveAction": str(
            analysis.get("recommended_preventive_action")
            or "Continue observation and collect diagnostics."
        ),
        "actionName": action,
        "authorised": False,
        "executed": False,
        "generatedAt": iso_now(),
    }


def investigation_exists_sync(incident_id: str) -> bool:
    with db_connect() as conn:
        row = conn.execute(
            "SELECT 1 FROM agent_investigations "
            "WHERE incident_id=%s AND status='COMPLETED'",
            (incident_id,),
        ).fetchone()
    return bool(row)


def add_agent_event_sync(
    investigation_id: str,
    event_type: str,
    message: str,
    details: dict[str, Any] | None = None,
    created_at: datetime | None = None,
) -> None:
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO agent_events(
                investigation_id,event_type,message,details,created_at
            ) VALUES(%s,%s,%s,%s,%s)
            """,
            (
                investigation_id,
                event_type,
                message,
                json.dumps(details or {}, default=str),
                created_at or utc_now(),
            ),
        )


def start_investigation_sync(incident_id: str) -> str:
    investigation_id = str(uuid.uuid4())
    now = utc_now()
    with db_connect() as conn:
        conn.execute(
            "DELETE FROM agent_investigations WHERE incident_id=%s",
            (incident_id,),
        )
        conn.execute(
            """
            INSERT INTO agent_investigations(
                id,incident_id,status,analysis_mode,provider,started_at,updated_at
            ) VALUES(%s,%s,'RUNNING','pending',%s,%s,%s)
            """,
            (investigation_id, incident_id, LLM_PROVIDER, now, now),
        )
    add_agent_event_sync(
        investigation_id,
        "STARTED",
        "Investigation started",
        {"incidentId": incident_id, "investigator": "opsai-agent"},
        now,
    )
    return investigation_id


def complete_investigation_sync(
    investigation_id: str,
    mode: str,
    provider: str,
    model: str,
    analysis: dict[str, Any],
    evidence: dict[str, Any],
    knowledge: list[dict[str, Any]],
    decision: str,
    reason: str,
    execution: dict[str, Any],
    audit: dict[str, Any],
) -> None:
    now = utc_now()
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE agent_investigations SET
                status='COMPLETED', analysis_mode=%s, provider=%s, model=%s,
                summary=%s, root_cause=%s, confidence=%s, customer_impact=%s,
                recommended_action=%s, action_name=%s, action_parameters=%s,
                policy_decision=%s, policy_reason=%s, hypotheses=%s,
                evidence=%s, retrieved_knowledge=%s,
                provider_endpoint_host=%s, prompt_preview=%s,
                request_payload=%s, response_payload=%s, token_usage=%s,
                request_sent_at=%s, response_received_at=%s, llm_duration_ms=%s,
                action_execution_status=%s, action_executed=%s, action_executor=%s,
                action_execution_result=%s, action_started_at=%s, action_completed_at=%s,
                completed_at=%s, updated_at=%s
            WHERE id=%s
            """,
            (
                mode,
                provider,
                model,
                analysis["summary"],
                analysis["root_cause"],
                analysis["confidence"],
                analysis["customer_impact"],
                analysis["recommended_action"],
                analysis["action_name"],
                json.dumps(analysis["action_parameters"]),
                decision,
                reason,
                json.dumps(analysis["hypotheses"]),
                json.dumps(evidence),
                json.dumps(knowledge),
                audit.get("providerEndpointHost", ""),
                audit.get("promptPreview", ""),
                json.dumps(audit.get("requestPayload", {}), default=str),
                json.dumps(audit.get("responsePayload", {}), default=str),
                json.dumps(audit.get("tokenUsage", {}), default=str),
                audit.get("requestSentAt"),
                audit.get("responseReceivedAt"),
                audit.get("llmDurationMs"),
                execution.get("status", "NOT_EVALUATED"),
                bool(execution.get("executed")),
                execution.get("executor", ""),
                json.dumps(execution.get("result", {}), default=str),
                execution.get("startedAt"),
                execution.get("completedAt"),
                now,
                now,
                investigation_id,
            ),
        )
    add_agent_event_sync(
        investigation_id,
        "POLICY_EVALUATED",
        "Governance evaluated the PulseGuard recommendation",
        {
            "action": analysis["action_name"],
            "decision": decision,
            "reason": reason,
        },
        now,
    )
    add_agent_event_sync(
        investigation_id,
        "COMPLETED",
        "Investigation completed",
        {
            "mode": mode,
            "provider": provider,
            "model": model,
            "action": analysis["action_name"],
            "policyDecision": decision,
            "actionExecutionStatus": execution.get("status"),
            "actionExecuted": execution.get("executed"),
        },
        now,
    )

def fail_investigation_sync(investigation_id: str, error: str) -> None:
    now = utc_now()
    with db_connect() as conn:
        conn.execute(
            "UPDATE agent_investigations SET status='FAILED',error=%s,"
            "completed_at=%s,updated_at=%s WHERE id=%s",
            (error, now, now, investigation_id),
        )


async def investigate(incident: dict[str, Any], force: bool = False) -> None:
    incident_id = incident["id"]
    if not force and await asyncio.to_thread(
        investigation_exists_sync, incident_id
    ):
        return
    investigation_id = await asyncio.to_thread(
        start_investigation_sync, incident_id
    )
    emit_log(
        "INFO",
        "investigation_started",
        incidentId=incident_id,
        investigationId=investigation_id,
    )
    try:
        evidence = await collect_evidence(incident)
        await asyncio.to_thread(
            add_agent_event_sync,
            investigation_id,
            "EVIDENCE_COLLECTED",
            "Prometheus evidence collected",
            {
                "affectedNode": evidence.get("affectedNode"),
                "affectedNodeP95Seconds": evidence.get("affectedNodeP95Seconds"),
                "peerAverageP95Seconds": evidence.get("peerAverageP95Seconds"),
                "checkoutP95Seconds": evidence.get("checkoutP95Seconds"),
                "checkoutFailurePercent": evidence.get("checkoutFailurePercent"),
                "checkoutRequestsPerSecond": evidence.get("checkoutRequestsPerSecond"),
                "retryRateByFailedNode": evidence.get("retryRateByFailedNode", {}),
            },
        )
        knowledge = retrieve_knowledge(incident, evidence)
        await asyncio.to_thread(
            add_agent_event_sync,
            investigation_id,
            "KNOWLEDGE_RETRIEVED",
            "Relevant runbooks and operational knowledge retrieved",
            {
                "documents": [
                    {
                        "id": item.get("id"),
                        "title": item.get("title"),
                        "kind": item.get("kind"),
                        "retrievalScore": item.get("retrievalScore"),
                    }
                    for item in knowledge
                ]
            },
        )
        if LLM_PROVIDER in {"openai", "azure_openai"}:
            raw, provider, model, audit = await real_llm_analysis(
                incident, evidence, knowledge
            )
            mode = "REAL_AI"
            await asyncio.to_thread(
                add_agent_event_sync,
                investigation_id,
                "LLM_REQUEST_SENT",
                "Investigation request sent to the configured AI provider",
                {
                    "provider": provider,
                    "model": model,
                    "endpointHost": audit.get("providerEndpointHost", ""),
                    "requestSentAt": audit.get("requestSentAt"),
                },
                audit.get("requestSentAt"),
            )
            await asyncio.to_thread(
                add_agent_event_sync,
                investigation_id,
                "LLM_RESPONSE_RECEIVED",
                "AI investigation response received",
                {
                    "provider": provider,
                    "model": model,
                    "durationMs": audit.get("llmDurationMs"),
                    "tokenUsage": audit.get("tokenUsage", {}),
                },
                audit.get("responseReceivedAt"),
            )
        else:
            raw = mock_analysis(incident, evidence)
            provider = "mock"
            model = "deterministic-fallback"
            mode = "DETERMINISTIC_FALLBACK"
            now = utc_now()
            context = {
                "incident": incident,
                "prometheusEvidence": evidence,
                "retrievedKnowledge": knowledge,
            }
            audit = {
                "providerEndpointHost": "local-deterministic",
                "promptPreview": (
                    "Deterministic fallback investigation. No external AI request "
                    "was sent.\n\nINPUT CONTEXT\n"
                    + json.dumps(context, indent=2, default=str)
                ),
                "requestPayload": {
                    "provider": "mock",
                    "model": model,
                    "endpointHost": "local-deterministic",
                    "inputContext": context,
                },
                "responsePayload": {"analysis": raw},
                "tokenUsage": {},
                "requestSentAt": now,
                "responseReceivedAt": now,
                "llmDurationMs": 0,
            }
            await asyncio.to_thread(
                add_agent_event_sync,
                investigation_id,
                "FALLBACK_ANALYSIS",
                "Deterministic fallback analysis completed locally",
                {"provider": provider, "model": model},
                now,
            )
        analysis = validate_analysis(raw, incident)
        decision, reason = policy_preview(
            analysis["action_name"],
            analysis["action_parameters"],
            evidence,
            incident,
        )
        incident_for_execution = dict(incident)
        incident_for_execution["_validated_action_parameters"] = analysis["action_parameters"]
        execution = await execute_policy_action(
            investigation_id,
            incident_for_execution,
            analysis["action_name"],
            decision,
        )
        await asyncio.to_thread(
            complete_investigation_sync,
            investigation_id,
            mode,
            provider,
            model,
            analysis,
            evidence,
            knowledge,
            decision,
            reason,
            execution,
            audit,
        )
        INVESTIGATIONS.labels(outcome="completed", mode=mode).inc()
        LAST_RUN.set(time.time())
        emit_log(
            "INFO",
            "investigation_completed",
            incidentId=incident_id,
            investigationId=investigation_id,
            mode=mode,
            provider=provider,
            action=analysis["action_name"],
            policyDecision=decision,
            actionExecutionStatus=execution.get("status"),
            actionExecuted=execution.get("executed"),
        )
    except Exception as exc:
        await asyncio.to_thread(
            fail_investigation_sync, investigation_id, str(exc)
        )
        mode = "REAL_AI" if LLM_PROVIDER != "mock" else "DETERMINISTIC_FALLBACK"
        INVESTIGATIONS.labels(outcome="failed", mode=mode).inc()
        emit_log(
            "ERROR",
            "investigation_failed",
            incidentId=incident_id,
            investigationId=investigation_id,
            error=str(exc),
        )

async def fetch_active_incidents() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"{OPSAI_CORE_URL}/incidents",
            params={"status": "active", "limit": 100},
        )
        response.raise_for_status()
        return response.json().get("incidents", [])


async def worker_loop() -> None:
    while not stop_event.is_set():
        try:
            for incident in await fetch_active_incidents():
                await investigate(incident)
        except Exception as exc:
            emit_log("ERROR", "worker_cycle_failed", error=str(exc))
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=POLL_INTERVAL_SECONDS,
            )
        except asyncio.TimeoutError:
            pass


def list_investigations_sync() -> list[dict[str, Any]]:
    with db_connect() as conn:
        rows = conn.execute(
            """
            SELECT ai.*, i.title AS incident_title, i.incident_type,
                   i.severity, i.status AS incident_status, i.node,
                   i.opened_at, i.resolved_at
            FROM agent_investigations ai
            JOIN incidents i ON i.id=ai.incident_id
            ORDER BY ai.started_at DESC LIMIT 50
            """
        ).fetchall()
    return rows


@app.on_event("startup")
async def startup() -> None:
    await asyncio.to_thread(initialise_database_sync)
    load_knowledge()
    ready = (
        LLM_PROVIDER == "openai" and bool(OPENAI_API_KEY)
    ) or (
        LLM_PROVIDER == "azure_openai"
        and bool(
            AZURE_OPENAI_ENDPOINT
            and AZURE_OPENAI_API_KEY
            and AZURE_OPENAI_DEPLOYMENT
        )
    )
    PROVIDER_READY.labels(provider=LLM_PROVIDER).set(1 if ready else 0)
    app.state.worker = asyncio.create_task(worker_loop())
    emit_log(
        "INFO",
        "service_started",
        provider=LLM_PROVIDER,
        realAiReady=ready,
        opsaiCoreUrl=OPSAI_CORE_URL,
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    stop_event.set()
    task = getattr(app.state, "worker", None)
    if task:
        await task


@app.get("/health")
async def health() -> dict[str, Any]:
    real_ready = (
        LLM_PROVIDER == "openai" and bool(OPENAI_API_KEY)
    ) or (
        LLM_PROVIDER == "azure_openai"
        and bool(
            AZURE_OPENAI_ENDPOINT
            and AZURE_OPENAI_API_KEY
            and AZURE_OPENAI_DEPLOYMENT
        )
    )
    return {
        "status": "healthy",
        "service": "opsai-agent",
        "version": SERVICE_VERSION,
        "provider": LLM_PROVIDER,
        "realAiReady": real_ready,
        "analysisMode": "REAL_AI" if real_ready else "DETERMINISTIC_FALLBACK",
    }


@app.get("/api/investigations")
async def api_investigations() -> dict[str, Any]:
    return {
        "provider": LLM_PROVIDER,
        "investigations": await asyncio.to_thread(list_investigations_sync),
    }


@app.post("/api/investigations/{incident_id}/rerun")
async def rerun(incident_id: str) -> dict[str, str]:
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"{OPSAI_CORE_URL}/incidents/{incident_id}"
        )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Incident not found")
    response.raise_for_status()
    payload = response.json()
    incident = payload.get("incident", payload)
    asyncio.create_task(investigate(incident, force=True))
    return {"status": "accepted", "incidentId": incident_id}


@app.get("/", response_class=HTMLResponse)
async def console() -> str:
    return """<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PulseGuard Investigation</title>
<style>
body{font-family:Inter,Segoe UI,Arial;background:#08111f;color:#e5edf7;margin:0}
.wrap{max-width:1250px;margin:auto;padding:28px}
.top{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}
.muted{color:#94a3b8}.badge{display:inline-block;padding:4px 9px;border-radius:999px;background:#172554;color:#bfdbfe;font-size:12px}
.warn{background:#422006;color:#fde68a}.good{background:#052e16;color:#bbf7d0}
.grid{display:grid;gap:16px;margin-top:20px}.card{border:1px solid #24344b;background:#0d1929;border-radius:14px;padding:18px}
.head{display:flex;justify-content:space-between;gap:12px}.cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.section{margin-top:14px}.label{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:#7dd3fc}
.value{margin-top:5px;line-height:1.45}.hyp{padding:9px 0;border-bottom:1px solid #223047}
.action{background:#111f33;padding:12px;border-radius:10px;border-left:4px solid #38bdf8}
button{background:#0284c7;color:white;border:0;border-radius:8px;padding:8px 12px;cursor:pointer}
@media(max-width:800px){.cols{grid-template-columns:1fr}.top{display:block}}
</style>
</head>
<body>
<div class="wrap">
  <div class="top">
    <div>
      <h1>PulseGuard Investigation</h1>
      <div class="muted">Evidence-bounded PulseGuard investigation using telemetry, topology, bounded automation context and a transparent local knowledge base. No scenario-controller ground truth access.</div>
    </div>
    <div id="provider"></div>
  </div>
  <div id="content" class="grid"><div class="card">Loading investigations...</div></div>
</div>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function rerun(id){await fetch('/api/investigations/'+id+'/rerun',{method:'POST'});setTimeout(load,1000)}
function render(i){
  const real=i.analysis_mode==='REAL_AI';
  const hy=(i.hypotheses||[]).map(h=>`<div class="hyp"><b>${esc(h.name)}</b> <span class="badge">${esc(h.likelihood)}</span><div class="muted">${esc(h.support)}</div></div>`).join('');
  const docs=(i.retrieved_knowledge||[]).map(d=>`<li>${esc(d.title)} <span class="muted">(${esc(d.kind)}, score ${Number(d.retrievalScore||0).toFixed(0)})</span></li>`).join('');
  const actionOutcome=i.action_executed?(i.action_execution_status==='SUCCEEDED'?'AUTO_ACTION_COMPLETED':'ACTION_FAILED'):(i.policy_decision==='APPROVAL_REQUIRED'?'WAITING_FOR_APPROVAL':(i.policy_decision==='BLOCKED'?'BLOCKED_BY_GOVERNANCE':'RECOMMENDED_ONLY'));
  return `<div class="card"><div class="head"><div><h2>${esc(i.incident_title)}</h2><div class="muted">${esc(i.incident_type)} - ${esc(i.node||'global')} - incident ${esc(i.incident_status)}</div></div><div><span class="badge ${real?'good':'warn'}">${esc(i.analysis_mode)}</span> <span class="badge">${esc(i.status)}</span></div></div><div class="cols"><div><div class="section"><div class="label">PulseGuard assessment</div><div class="value">${esc(i.summary)}</div></div><div class="section"><div class="label">Likely root cause</div><div class="value">${esc(i.root_cause)}</div></div><div class="section"><div class="label">Customer impact</div><div class="value">${esc(i.customer_impact)}</div></div><div class="section"><div class="label">Hypotheses</div>${hy||'<div class="muted">No hypotheses recorded.</div>'}</div></div><div><div class="section"><div class="label">Confidence</div><div class="value">${Math.round(Number(i.confidence||0)*100)}%</div></div><div class="section action"><div class="label">PulseGuard recommendation</div><div class="value"><b>${esc(i.action_name)}</b><br>${esc(i.recommended_action)}</div></div><div class="section"><div class="label">Governance decision</div><div class="value"><b>${esc(i.policy_decision)}</b><br>${esc(i.policy_reason)}</div></div><div class="section"><div class="label">Action taken</div><div class="value"><b>${esc(i.action_execution_status||'NOT_EVALUATED')}</b><br>${i.action_executed?'Executed by '+esc(i.action_executor||'PulseGuard'):'No operational action executed'}</div><div class="muted">Outcome: ${esc(actionOutcome)}</div><details><summary>Execution result</summary><pre>${esc(JSON.stringify(i.action_execution_result||{},null,2))}</pre></details></div><div class="section"><div class="label">Retrieved knowledge</div><ul>${docs}</ul></div><div class="section"><div class="label">Provider</div><div class="value">${esc(i.provider)} / ${esc(i.model)}</div></div><div class="section"><button data-id="${esc(i.incident_id)}">Re-run investigation</button></div></div></div></div>`;
}
async function load(){
  const r=await fetch('/api/investigations');
  const d=await r.json();
  document.getElementById('provider').innerHTML=`<span class="badge">Provider: ${esc(d.provider)}</span>`;
  document.getElementById('content').innerHTML=d.investigations.length?d.investigations.map(render).join(''):'<div class="card">No investigations yet. Open an incident to trigger one automatically.</div>';
  document.querySelectorAll('button[data-id]').forEach(b=>b.addEventListener('click',()=>rerun(b.dataset.id)));
}
load();setInterval(load,5000);
</script>
<script src="http://localhost:8097/widget.js"></script>
</body>
</html>"""
