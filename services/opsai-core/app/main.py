from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx
import psycopg
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic import BaseModel, Field
from psycopg.rows import dict_row


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090").rstrip("/")
OPSAI_AGENT_URL = os.getenv("OPSAI_AGENT_URL", "http://opsai-agent:8000").rstrip("/")
WIKIMEDIA_PROFILE_URL = os.getenv("WIKIMEDIA_PROFILE_URL", "http://wikimedia-adapter:8000/profile")
TRAFFIC_PROFILE_URL = os.getenv("TRAFFIC_PROFILE_URL", "http://corruption-adapter:8000/profile")
OPSAI_AUTOMATION_URL = os.getenv("OPSAI_AUTOMATION_URL", "http://opsai-automation:8000").rstrip("/")
EXTERNAL_AUTH_SERVICE_URL = os.getenv(
    "EXTERNAL_AUTH_SERVICE_URL",
    "http://external-auth-service:8000",
).rstrip("/")
AUTOMATION_API_TOKEN = require_env("AUTOMATION_API_TOKEN")
PAYMENT_NODE_URLS_RAW = os.getenv(
    "PAYMENT_NODE_URLS",
    "payment-node-1=http://payment-node-1:8000,payment-node-2=http://payment-node-2:8000,payment-node-3=http://payment-node-3:8000",
)
DATABASE_URL = require_env("DATABASE_URL")
EVALUATION_INTERVAL_SECONDS = float(os.getenv("EVALUATION_INTERVAL_SECONDS", "5"))

LATENCY_OPEN_SECONDS = float(os.getenv("LATENCY_OPEN_SECONDS", "0.8"))
LATENCY_RECOVERY_SECONDS = float(os.getenv("LATENCY_RECOVERY_SECONDS", "0.45"))
LATENCY_OPEN_EVALUATIONS = int(os.getenv("LATENCY_OPEN_EVALUATIONS", "3"))
LATENCY_CLOSE_EVALUATIONS = int(os.getenv("LATENCY_CLOSE_EVALUATIONS", "4"))

RETRY_OPEN_RATE = float(os.getenv("RETRY_OPEN_RATE", "0.05"))
RETRY_RECOVERY_RATE = float(os.getenv("RETRY_RECOVERY_RATE", "0.01"))
RETRY_OPEN_EVALUATIONS = int(os.getenv("RETRY_OPEN_EVALUATIONS", "2"))
RETRY_CLOSE_EVALUATIONS = int(os.getenv("RETRY_CLOSE_EVALUATIONS", "4"))

CHECKOUT_FAILURE_OPEN_PERCENT = float(os.getenv("CHECKOUT_FAILURE_OPEN_PERCENT", "5"))
CHECKOUT_FAILURE_RECOVERY_PERCENT = float(os.getenv("CHECKOUT_FAILURE_RECOVERY_PERCENT", "1"))
CHECKOUT_FAILURE_OPEN_EVALUATIONS = int(os.getenv("CHECKOUT_FAILURE_OPEN_EVALUATIONS", "2"))
CHECKOUT_FAILURE_CLOSE_EVALUATIONS = int(os.getenv("CHECKOUT_FAILURE_CLOSE_EVALUATIONS", "4"))
UNAVAILABLE_FAILURE_RATIO = float(os.getenv("UNAVAILABLE_FAILURE_RATIO", "0.85"))
NETWORK_INSTABILITY_RATIO = float(os.getenv("NETWORK_INSTABILITY_RATIO", "0.08"))
FLAPPING_TRANSITIONS = int(os.getenv("FLAPPING_TRANSITIONS", "3"))
FLAPPING_WINDOW_SECONDS = int(os.getenv("FLAPPING_WINDOW_SECONDS", "50"))
FLAPPING_HOLD_DOWN_SECONDS = int(os.getenv("FLAPPING_HOLD_DOWN_SECONDS", "18"))
FLEET_NODE_COUNT_THRESHOLD = float(os.getenv("FLEET_NODE_COUNT_THRESHOLD", "1.5"))
CAPACITY_PROCESSING_RECOVERY_SECONDS = float(
    os.getenv("CAPACITY_PROCESSING_RECOVERY_SECONDS", "0.8")
)
CAPACITY_TRANSITION_HOLD_DOWN_SECONDS = int(
    os.getenv("CAPACITY_TRANSITION_HOLD_DOWN_SECONDS", "90")
)
CAPACITY_TARGET_NODES = ("payment-node-1", "payment-node-2")
EXTERNAL_AUTH_FAILURE_OPEN_RATE = float(
    os.getenv("EXTERNAL_AUTH_FAILURE_OPEN_RATE", "0.10")
)
EXTERNAL_AUTH_FAILURE_RECOVERY_RATE = float(
    os.getenv("EXTERNAL_AUTH_FAILURE_RECOVERY_RATE", "0.01")
)
POST_SHARED_DEPENDENCY_COOLDOWN_SECONDS = int(
    os.getenv("POST_SHARED_DEPENDENCY_COOLDOWN_SECONDS", "75")
)
STRONG_LATENCY_OUTLIER_RATIO = float(
    os.getenv("STRONG_LATENCY_OUTLIER_RATIO", "2.5")
)

INCIDENTS_OPENED = Counter(
    "opsai_incidents_opened_total",
    "Incidents opened by the deterministic detector.",
    ["type", "severity", "node"],
)
INCIDENTS_RESOLVED = Counter(
    "opsai_incidents_resolved_total",
    "Incidents automatically resolved after verified recovery.",
    ["type", "severity", "node"],
)
INCIDENT_ACTIVE = Gauge(
    "opsai_incident_active",
    "Whether an incident fingerprint is currently active.",
    ["type", "severity", "node", "fingerprint"],
)
RULE_SIGNAL = Gauge(
    "opsai_detector_rule_signal",
    "Most recent numeric signal used by a detection rule.",
    ["rule", "node"],
)
RULE_CONDITION = Gauge(
    "opsai_detector_rule_condition",
    "Whether the most recent rule condition evaluated true.",
    ["rule", "node"],
)
EVALUATIONS = Counter(
    "opsai_detector_evaluations_total",
    "Detector evaluation cycles.",
    ["outcome"],
)
LAST_EVALUATION = Gauge(
    "opsai_detector_last_evaluation_timestamp_seconds",
    "Unix timestamp of the latest successful evaluation.",
)
DB_READY = Gauge("opsai_detector_database_ready", "Whether incident persistence is available.")

PROBLEM_STATUSES = {
    "CANDIDATE",
    "UNDER_REVIEW",
    "CONFIRMED",
    "INVESTIGATING",
    "CORRECTIVE_ACTION_PLANNED",
    "MONITORING",
    "CLOSED",
    "REJECTED",
}
PROBLEM_TRANSITIONS = {
    "CANDIDATE": {"UNDER_REVIEW", "REJECTED"},
    "UNDER_REVIEW": {"CONFIRMED", "REJECTED"},
    "CONFIRMED": {"INVESTIGATING", "REJECTED"},
    "INVESTIGATING": {"CORRECTIVE_ACTION_PLANNED", "REJECTED"},
    "CORRECTIVE_ACTION_PLANNED": {"MONITORING", "INVESTIGATING"},
    "MONITORING": {"CLOSED", "INVESTIGATING"},
    "CLOSED": {"MONITORING"},
    "REJECTED": {"UNDER_REVIEW"},
}
PROBLEM_RECORD_CLASSES = {
    "DEMO_CANDIDATE",
    "REVIEW_REQUIRED",
    "OPERATIONAL_CANDIDATE",
}


@dataclass
class RuleMemory:
    consecutive_bad: int = 0
    consecutive_good: int = 0
    last_value: float | None = None
    active_incident_id: str | None = None
    last_condition: bool = False


@dataclass(frozen=True)
class DetectionRule:
    rule_id: str
    incident_type: str
    title_template: str
    severity: str
    unit: str
    open_threshold: float
    recovery_threshold: float
    open_evaluations: int
    close_evaluations: int
    direction: str = "above"
    runbook_hint: str = ""


RULES = {
    "payment_node_timeout": DetectionRule(
        rule_id="payment_node_timeout",
        incident_type="PAYMENT_NODE_TIMEOUT",
        title_template="Payment node timeout and failover: {node}",
        severity="critical",
        unit="retries/second",
        open_threshold=RETRY_OPEN_RATE,
        recovery_threshold=RETRY_RECOVERY_RATE,
        open_evaluations=RETRY_OPEN_EVALUATIONS,
        close_evaluations=RETRY_CLOSE_EVALUATIONS,
        runbook_hint="Validate dependency reachability, drain the node if retries persist, then verify healthy-node capacity.",
    ),
    "payment_node_latency": DetectionRule(
        rule_id="payment_node_latency",
        incident_type="PAYMENT_NODE_LATENCY",
        title_template="Payment node latency degradation: {node}",
        severity="high",
        unit="seconds p95",
        open_threshold=LATENCY_OPEN_SECONDS,
        recovery_threshold=LATENCY_RECOVERY_SECONDS,
        open_evaluations=LATENCY_OPEN_EVALUATIONS,
        close_evaluations=LATENCY_CLOSE_EVALUATIONS,
        runbook_hint="Compare node latency, retries and checkout impact; drain only after confirming the node is the outlier.",
    ),
    "checkout_failure_rate": DetectionRule(
        rule_id="checkout_failure_rate",
        incident_type="CHECKOUT_FAILURE_RATE",
        title_template="Customer checkout failure rate elevated",
        severity="critical",
        unit="percent",
        open_threshold=CHECKOUT_FAILURE_OPEN_PERCENT,
        recovery_threshold=CHECKOUT_FAILURE_RECOVERY_PERCENT,
        open_evaluations=CHECKOUT_FAILURE_OPEN_EVALUATIONS,
        close_evaluations=CHECKOUT_FAILURE_CLOSE_EVALUATIONS,
        runbook_hint="Inspect router failures, dependency health and available payment-node capacity.",
    ),
    "payment_node_unavailable": DetectionRule(
        rule_id="payment_node_unavailable",
        incident_type="PAYMENT_NODE_UNAVAILABLE",
        title_template="Payment node unavailable: {node}",
        severity="critical",
        unit="failure ratio",
        open_threshold=UNAVAILABLE_FAILURE_RATIO,
        recovery_threshold=0.10,
        open_evaluations=2,
        close_evaluations=4,
        runbook_hint="Keep the unavailable node out of new traffic, verify two healthy peers, and collect diagnostics before restart or restore.",
    ),
    "payment_node_network_instability": DetectionRule(
        rule_id="payment_node_network_instability",
        incident_type="PAYMENT_NODE_NETWORK_INSTABILITY",
        title_template="Payment node network instability: {node}",
        severity="high",
        unit="failure ratio",
        open_threshold=NETWORK_INSTABILITY_RATIO,
        recovery_threshold=0.03,
        open_evaluations=2,
        close_evaluations=4,
        runbook_hint="Correlate intermittent connection resets with successful requests, collect network diagnostics, and drain only if instability persists.",
    ),
    "payment_node_flapping": DetectionRule(
        rule_id="payment_node_flapping",
        incident_type="PAYMENT_NODE_FLAPPING",
        title_template="Payment node repeatedly changes availability: {node}",
        severity="critical",
        unit="availability transitions",
        open_threshold=float(FLAPPING_TRANSITIONS - 1),
        recovery_threshold=0.5,
        open_evaluations=1,
        close_evaluations=8,
        runbook_hint="Keep the flapping node isolated until it remains stable for the verification window; collect process and dependency diagnostics.",
    ),
    "payment_shared_dependency_outage": DetectionRule(
        rule_id="payment_shared_dependency_outage",
        incident_type="PAYMENT_SHARED_DEPENDENCY_OUTAGE",
        title_template="Shared payment dependency outage",
        severity="critical",
        unit="affected nodes",
        open_threshold=FLEET_NODE_COUNT_THRESHOLD,
        recovery_threshold=0.5,
        open_evaluations=2,
        close_evaluations=4,
        runbook_hint="Treat correlated failures across the payment fleet as a shared dependency problem. Do not drain individual healthy nodes.",
    ),
    "payment_fleet_capacity_degradation": DetectionRule(
        rule_id="payment_fleet_capacity_degradation",
        incident_type="PAYMENT_FLEET_CAPACITY_DEGRADATION",
        title_template="Payment fleet capacity degradation",
        severity="high",
        unit="slow nodes",
        open_threshold=FLEET_NODE_COUNT_THRESHOLD,
        recovery_threshold=0.5,
        open_evaluations=3,
        close_evaluations=4,
        runbook_hint="Correlate fleet-wide latency with live demand and capacity. Do not drain an individual node unless it is an outlier.",
    ),
    "payment_node_hung": DetectionRule(
        rule_id="payment_node_hung",
        incident_type="PAYMENT_NODE_HUNG",
        title_template="Payment worker appears hung: {node}",
        severity="critical",
        unit="hung state",
        open_threshold=0.5,
        recovery_threshold=0.5,
        open_evaluations=2,
        close_evaluations=4,
        runbook_hint="Confirm the worker is stuck while the service process remains reachable. Restart only after approval and verify the node returns to normal processing.",
    ),
    "external_service_authentication_failure": DetectionRule(
        rule_id="external_service_authentication_failure",
        incident_type="EXTERNAL_SERVICE_AUTHENTICATION_FAILURE",
        title_template="External service authentication failure: {node}",
        severity="critical",
        unit="failures/second",
        open_threshold=EXTERNAL_AUTH_FAILURE_OPEN_RATE,
        recovery_threshold=EXTERNAL_AUTH_FAILURE_RECOVERY_RATE,
        open_evaluations=2,
        close_evaluations=4,
        runbook_hint="Validate the credential generation and service identity. Refresh only the allowlisted partner credential, verify a probe call, and never expose the token in logs or incident evidence.",
    ),
}

rule_memory: dict[str, RuleMemory] = {}
db_ready = False
last_evaluation_at: datetime | None = None
last_evaluation_summary: dict[str, Any] = {}
stop_event = asyncio.Event()
shared_dependency_last_active_at: float | None = None

app = FastAPI(title="PulseGuard Core - Detection and Incidents", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())


class AcknowledgeRequest(BaseModel):
    actor: str = Field(default="operator", min_length=1, max_length=100)
    note: str = Field(default="", max_length=1000)


class ExternalIncidentRequest(BaseModel):
    incidentType: str = Field(min_length=3, max_length=100)
    title: str = Field(min_length=3, max_length=300)
    severity: str = Field(default="high", pattern="^(low|medium|high|critical)$")
    service: str = Field(default="external-platform", min_length=1, max_length=100)
    node: str = Field(default="", max_length=150)
    summary: str = Field(min_length=3, max_length=2000)
    runbookHint: str = Field(default="", max_length=2000)
    evidence: dict[str, Any] = Field(default_factory=dict)
    source: str = Field(default="opsai-automation", max_length=100)


class ExternalResolutionRequest(BaseModel):
    reason: str = Field(min_length=3, max_length=3000)
    repairOutcome: str = Field(default="RECOVERED_WITHOUT_ACTION", max_length=100)
    evidence: dict[str, Any] = Field(default_factory=dict)
    actor: str = Field(default="opsai-automation", max_length=100)


class ExternalEventRequest(BaseModel):
    eventType: str = Field(min_length=2, max_length=100)
    actor: str = Field(default="opsai-automation", max_length=100)
    message: str = Field(min_length=2, max_length=2000)
    details: dict[str, Any] = Field(default_factory=dict)


class ProblemCandidateRequest(BaseModel):
    problemKey: str = Field(min_length=3, max_length=200)
    title: str = Field(min_length=3, max_length=300)
    category: str = Field(min_length=3, max_length=200)
    recordClass: str = Field(
        default="REVIEW_REQUIRED",
        pattern="^(DEMO_CANDIDATE|REVIEW_REQUIRED|OPERATIONAL_CANDIDATE)$",
    )
    riskScore: float = Field(ge=0, le=100)
    riskLevel: str = Field(default="MEDIUM", pattern="^(LOW|MEDIUM|HIGH)$")
    summary: str = Field(min_length=3, max_length=3000)
    hypothesis: str = Field(default="", max_length=5000)
    occurrenceCount: int = Field(ge=0)
    averageIntervalSeconds: float | None = Field(default=None, ge=0)
    firstOccurrenceAt: datetime | None = None
    latestOccurrenceAt: datetime | None = None
    incidentTypes: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)
    linkedIncidentIds: list[str] = Field(default_factory=list)
    originBreakdown: dict[str, int] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)


class ProblemTransitionRequest(BaseModel):
    status: str = Field(min_length=3, max_length=50)
    actor: str = Field(default="operator", min_length=1, max_length=100)
    note: str = Field(default="", max_length=3000)
    confirmedRootCause: str = Field(default="", max_length=5000)
    correctiveAction: str = Field(default="", max_length=5000)
    monitoringNotes: str = Field(default="", max_length=5000)
    rejectionReason: str = Field(default="", max_length=3000)


class ProblemAssignmentRequest(BaseModel):
    ownerQueue: str = Field(default="Problem Management", min_length=1, max_length=200)
    ownerName: str = Field(default="", max_length=200)
    actor: str = Field(default="operator", min_length=1, max_length=100)
    note: str = Field(default="", max_length=3000)


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
node_availability_history: dict[str, deque[tuple[float, bool]]] = {}
last_node_accepting: dict[str, bool] = {}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def emit_log(level: str, event: str, **fields: object) -> None:
    print(json.dumps({
        "timestamp": iso_now(),
        "level": level,
        "service": "opsai-core",
        "version": SERVICE_VERSION,
        "event": event,
        **fields,
    }, separators=(",", ":"), default=str), flush=True)


def db_connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=True)


def initialise_database_sync() -> None:
    global db_ready
    deadline = time.monotonic() + 120
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with db_connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS incidents (
                        id UUID PRIMARY KEY,
                        fingerprint TEXT NOT NULL,
                        incident_type TEXT NOT NULL,
                        title TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        status TEXT NOT NULL,
                        service TEXT NOT NULL,
                        node TEXT NOT NULL DEFAULT '',
                        summary TEXT NOT NULL,
                        runbook_hint TEXT NOT NULL DEFAULT '',
                        evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
                        first_detected_at TIMESTAMPTZ NOT NULL,
                        last_detected_at TIMESTAMPTZ NOT NULL,
                        opened_at TIMESTAMPTZ NOT NULL,
                        acknowledged_at TIMESTAMPTZ,
                        acknowledged_by TEXT,
                        resolved_at TIMESTAMPTZ,
                        resolution_reason TEXT,
                        updated_at TIMESTAMPTZ NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_incidents_active_fingerprint
                    ON incidents (fingerprint)
                    WHERE status IN ('OPEN', 'ACKNOWLEDGED')
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS incident_events (
                        id BIGSERIAL PRIMARY KEY,
                        incident_id UUID NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        message TEXT NOT NULL,
                        details JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                """)
                conn.execute("CREATE INDEX IF NOT EXISTS ix_incident_events_incident_id ON incident_events(incident_id, created_at)")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS problems (
                        id UUID PRIMARY KEY,
                        problem_key TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL,
                        category TEXT NOT NULL,
                        record_class TEXT NOT NULL,
                        status TEXT NOT NULL,
                        risk_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                        risk_level TEXT NOT NULL DEFAULT 'LOW',
                        owner_queue TEXT NOT NULL DEFAULT 'Problem Management',
                        owner_name TEXT NOT NULL DEFAULT '',
                        summary TEXT NOT NULL,
                        hypothesis TEXT NOT NULL DEFAULT '',
                        confirmed_root_cause TEXT NOT NULL DEFAULT '',
                        corrective_action TEXT NOT NULL DEFAULT '',
                        monitoring_notes TEXT NOT NULL DEFAULT '',
                        rejection_reason TEXT NOT NULL DEFAULT '',
                        occurrence_count INTEGER NOT NULL DEFAULT 0,
                        recurrence_after_action INTEGER NOT NULL DEFAULT 0,
                        average_interval_seconds DOUBLE PRECISION,
                        first_occurrence_at TIMESTAMPTZ,
                        latest_occurrence_at TIMESTAMPTZ,
                        incident_types JSONB NOT NULL DEFAULT '[]'::jsonb,
                        scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
                        linked_incident_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                        origin_breakdown JSONB NOT NULL DEFAULT '{}'::jsonb,
                        candidate_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        confirmed_at TIMESTAMPTZ,
                        closed_at TIMESTAMPTZ,
                        rejected_at TIMESTAMPTZ
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS ix_problems_status_risk
                    ON problems(status, risk_score DESC, updated_at DESC)
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS problem_events (
                        id BIGSERIAL PRIMARY KEY,
                        problem_id UUID NOT NULL REFERENCES problems(id) ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        message TEXT NOT NULL,
                        details JSONB NOT NULL DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS ix_problem_events_problem_id
                    ON problem_events(problem_id, created_at)
                """)
            db_ready = True
            DB_READY.set(1)
            emit_log("INFO", "database_ready")
            return
        except Exception as exc:
            last_error = exc
            DB_READY.set(0)
            time.sleep(3)
    raise RuntimeError(f"Database did not become ready: {last_error}")


def hydrate_active_incidents_sync() -> None:
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT id::text, fingerprint, incident_type, severity, node
            FROM incidents
            WHERE status IN ('OPEN', 'ACKNOWLEDGED')
        """).fetchall()
    for row in rows:
        memory = rule_memory.setdefault(row["fingerprint"], RuleMemory())
        memory.active_incident_id = row["id"]
        INCIDENT_ACTIVE.labels(
            type=row["incident_type"], severity=row["severity"],
            node=row["node"] or "none", fingerprint=row["fingerprint"],
        ).set(1)
    emit_log("INFO", "active_incidents_hydrated", count=len(rows))


async def prometheus_query(query: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
        response.raise_for_status()
        payload = response.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return payload.get("data", {}).get("result", [])


def vector_to_map(rows: list[dict[str, Any]], label: str, default_key: str = "global") -> dict[str, float]:
    result: dict[str, float] = {}
    for row in rows:
        key = row.get("metric", {}).get(label, default_key)
        try:
            result[key] = float(row.get("value", [0, "nan"])[1])
        except (TypeError, ValueError, IndexError):
            continue
    return result


def is_bad(rule: DetectionRule, value: float) -> bool:
    return value > rule.open_threshold if rule.direction == "above" else value < rule.open_threshold


def is_recovered(rule: DetectionRule, value: float) -> bool:
    return value < rule.recovery_threshold if rule.direction == "above" else value > rule.recovery_threshold


def incident_fingerprint(rule: DetectionRule, node: str) -> str:
    return f"{rule.rule_id}:{node or 'global'}"


def open_incident_sync(rule: DetectionRule, node: str, value: float, evidence: dict[str, Any]) -> str:
    incident_id = str(uuid.uuid4())
    fingerprint = incident_fingerprint(rule, node)
    now = utc_now()
    title = rule.title_template.format(node=node)
    summary = (
        f"{rule.rule_id} remained above its opening threshold for "
        f"{rule.open_evaluations} consecutive evaluations. Observed {value:.3f} {rule.unit}; "
        f"threshold {rule.open_threshold:.3f}."
    )
    with db_connect() as conn:
        existing = conn.execute("""
            SELECT id::text FROM incidents
            WHERE fingerprint=%s AND status IN ('OPEN','ACKNOWLEDGED')
        """, (fingerprint,)).fetchone()
        if existing:
            return existing["id"]
        conn.execute("""
            INSERT INTO incidents (
                id, fingerprint, incident_type, title, severity, status,
                service, node, summary, runbook_hint, evidence,
                first_detected_at, last_detected_at, opened_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,'OPEN','payment-platform',%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            incident_id, fingerprint, rule.incident_type, title, rule.severity,
            node, summary, rule.runbook_hint, json.dumps(evidence),
            now, now, now, now,
        ))
        conn.execute("""
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'OPENED','detector',%s,%s,%s)
        """, (incident_id, summary, json.dumps(evidence), now))
    INCIDENTS_OPENED.labels(type=rule.incident_type, severity=rule.severity, node=node or "none").inc()
    INCIDENT_ACTIVE.labels(type=rule.incident_type, severity=rule.severity, node=node or "none", fingerprint=fingerprint).set(1)
    emit_log("ERROR" if rule.severity == "critical" else "WARNING", "incident_opened",
             incidentId=incident_id, fingerprint=fingerprint, incidentType=rule.incident_type,
             severity=rule.severity, node=node, observed=value)
    return incident_id


def refresh_incident_sync(incident_id: str, value: float, evidence: dict[str, Any]) -> None:
    now = utc_now()
    with db_connect() as conn:
        conn.execute("""
            UPDATE incidents SET last_detected_at=%s, updated_at=%s, evidence=%s
            WHERE id=%s AND status IN ('OPEN','ACKNOWLEDGED')
        """, (now, now, json.dumps(evidence), incident_id))


def resolve_incident_sync(rule: DetectionRule, node: str, incident_id: str, value: float, evidence: dict[str, Any]) -> None:
    now = utc_now()
    if rule.incident_type == "PAYMENT_FLEET_CAPACITY_DEGRADATION":
        recovery_source = str(evidence.get("capacityRecoverySource") or "")
        if recovery_source == "VERIFIED_AUTO_SCALE":
            reason = (
                f"Bounded capacity recovery passed for {rule.close_evaluations} consecutive evaluations. "
                f"Both peer nodes remained at two capacity units, payment-processing p95 stayed at or below "
                f"{CAPACITY_PROCESSING_RECOVERY_SECONDS:.3f}s, checkout failures remained stable, and node 3 "
                "remained unavailable while the traffic surge was active."
            )
        elif recovery_source == "TRAFFIC_NORMALIZED":
            reason = (
                "Demand returned to normal before bounded capacity recovery was independently verified. "
                "This recovery is not attributed to the automatic scale action."
            )
        else:
            reason = (
                f"Fleet recovery condition passed for {rule.close_evaluations} consecutive evaluations. "
                f"Observed {value:.3f} {rule.unit}."
            )
    else:
        reason = (
            f"Automatic recovery verification passed for {rule.close_evaluations} consecutive evaluations. "
            f"Observed {value:.3f} {rule.unit}; recovery threshold {rule.recovery_threshold:.3f}."
        )
    with db_connect() as conn:
        conn.execute("""
            UPDATE incidents SET status='RESOLVED', resolved_at=%s, resolution_reason=%s,
                evidence=%s, updated_at=%s
            WHERE id=%s AND status IN ('OPEN','ACKNOWLEDGED')
        """, (now, reason, json.dumps(evidence), now, incident_id))
        conn.execute("""
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'RESOLVED','detector',%s,%s,%s)
        """, (incident_id, reason, json.dumps(evidence), now))
    fingerprint = incident_fingerprint(rule, node)
    INCIDENTS_RESOLVED.labels(type=rule.incident_type, severity=rule.severity, node=node or "none").inc()
    INCIDENT_ACTIVE.labels(type=rule.incident_type, severity=rule.severity, node=node or "none", fingerprint=fingerprint).set(0)
    emit_log("INFO", "incident_resolved", incidentId=incident_id, fingerprint=fingerprint,
             incidentType=rule.incident_type, node=node, observed=value)


def active_rule_incident_id(rule: DetectionRule, node: str) -> str | None:
    return rule_memory.get(incident_fingerprint(rule, node), RuleMemory()).active_incident_id


def supersede_incident_sync(
    rule: DetectionRule,
    node: str,
    incident_id: str,
    dominant_rule: DetectionRule,
    dominant_incident_id: str,
) -> None:
    now = utc_now()
    reason = (
        f"Superseded by dominant incident {dominant_rule.incident_type} "
        f"({dominant_incident_id}). The lower-level symptom remains available in the evidence trail."
    )
    evidence_patch = {
        "repairOutcome": "SUPERSEDED",
        "supersededByIncidentId": dominant_incident_id,
        "supersededByIncidentType": dominant_rule.incident_type,
        "supersededAt": now.isoformat(),
    }
    with db_connect() as conn:
        row = conn.execute(
            "SELECT evidence FROM incidents WHERE id=%s AND status IN ('OPEN','ACKNOWLEDGED')",
            (incident_id,),
        ).fetchone()
        if not row:
            return
        evidence = dict(row.get("evidence") or {})
        evidence.update(evidence_patch)
        conn.execute(
            """
            UPDATE incidents SET status='RESOLVED', resolved_at=%s,
                resolution_reason=%s, evidence=%s, updated_at=%s
            WHERE id=%s AND status IN ('OPEN','ACKNOWLEDGED')
            """,
            (now, reason, json.dumps(evidence), now, incident_id),
        )
        conn.execute(
            """
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'SUPERSEDED','detector',%s,%s,%s)
            """,
            (incident_id, reason, json.dumps(evidence_patch), now),
        )
    fingerprint = incident_fingerprint(rule, node)
    INCIDENT_ACTIVE.labels(
        type=rule.incident_type,
        severity=rule.severity,
        node=node or "none",
        fingerprint=fingerprint,
    ).set(0)
    memory = rule_memory.setdefault(fingerprint, RuleMemory())
    memory.active_incident_id = None
    memory.consecutive_bad = 0
    memory.consecutive_good = 0
    emit_log(
        "INFO",
        "incident_superseded",
        incidentId=incident_id,
        incidentType=rule.incident_type,
        dominantIncidentId=dominant_incident_id,
        dominantIncidentType=dominant_rule.incident_type,
        node=node,
    )


def external_fingerprint(incident_type: str, node: str) -> str:
    return f"external:{incident_type.lower()}:{node or 'global'}"


def open_external_incident_sync(request: ExternalIncidentRequest) -> tuple[str, bool]:
    now = utc_now()
    fingerprint = external_fingerprint(request.incidentType, request.node)
    with db_connect() as conn:
        existing = conn.execute(
            """
            SELECT id::text FROM incidents
            WHERE fingerprint=%s AND status IN ('OPEN','ACKNOWLEDGED')
            """,
            (fingerprint,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE incidents SET last_detected_at=%s, updated_at=%s,
                    evidence=%s, summary=%s, runbook_hint=%s
                WHERE id=%s
                """,
                (
                    now,
                    now,
                    json.dumps(request.evidence),
                    request.summary,
                    request.runbookHint,
                    existing["id"],
                ),
            )
            return existing["id"], False
        incident_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO incidents (
                id, fingerprint, incident_type, title, severity, status,
                service, node, summary, runbook_hint, evidence,
                first_detected_at, last_detected_at, opened_at, updated_at
            ) VALUES (%s,%s,%s,%s,%s,'OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                incident_id,
                fingerprint,
                request.incidentType,
                request.title,
                request.severity,
                request.service,
                request.node,
                request.summary,
                request.runbookHint,
                json.dumps(request.evidence),
                now,
                now,
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'OPENED',%s,%s,%s,%s)
            """,
            (
                incident_id,
                request.source,
                request.summary,
                json.dumps(request.evidence),
                now,
            ),
        )
    INCIDENTS_OPENED.labels(
        type=request.incidentType,
        severity=request.severity,
        node=request.node or "none",
    ).inc()
    INCIDENT_ACTIVE.labels(
        type=request.incidentType,
        severity=request.severity,
        node=request.node or "none",
        fingerprint=fingerprint,
    ).set(1)
    emit_log(
        "WARNING" if request.severity != "critical" else "ERROR",
        "external_incident_opened",
        incidentId=incident_id,
        incidentType=request.incidentType,
        source=request.source,
        node=request.node,
    )
    return incident_id, True


def resolve_external_incident_sync(incident_id: str, request: ExternalResolutionRequest) -> dict[str, Any]:
    now = utc_now()
    with db_connect() as conn:
        row = conn.execute(
            """
            SELECT fingerprint, incident_type, severity, node, evidence
            FROM incidents WHERE id=%s AND status IN ('OPEN','ACKNOWLEDGED')
            """,
            (incident_id,),
        ).fetchone()
        if not row:
            raise LookupError("Incident is not active or does not exist")
        evidence = dict(row.get("evidence") or {})
        evidence.update(request.evidence)
        evidence["repairOutcome"] = request.repairOutcome
        evidence["verifiedRecoveryAt"] = now.isoformat()
        conn.execute(
            """
            UPDATE incidents SET status='RESOLVED', resolved_at=%s,
                resolution_reason=%s, evidence=%s, updated_at=%s
            WHERE id=%s
            """,
            (now, request.reason, json.dumps(evidence), now, incident_id),
        )
        conn.execute(
            """
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'RESOLVED',%s,%s,%s,%s)
            """,
            (
                incident_id,
                request.actor,
                request.reason,
                json.dumps({"repairOutcome": request.repairOutcome, **request.evidence}),
                now,
            ),
        )
    INCIDENTS_RESOLVED.labels(
        type=row["incident_type"],
        severity=row["severity"],
        node=row["node"] or "none",
    ).inc()
    INCIDENT_ACTIVE.labels(
        type=row["incident_type"],
        severity=row["severity"],
        node=row["node"] or "none",
        fingerprint=row["fingerprint"],
    ).set(0)
    emit_log(
        "INFO",
        "external_incident_resolved",
        incidentId=incident_id,
        incidentType=row["incident_type"],
        repairOutcome=request.repairOutcome,
    )
    return {
        "incidentId": incident_id,
        "status": "RESOLVED",
        "repairOutcome": request.repairOutcome,
    }


async def evaluate_rule(rule: DetectionRule, node: str, value: float, evidence: dict[str, Any], suppressed: bool = False) -> None:
    fingerprint = incident_fingerprint(rule, node)
    memory = rule_memory.setdefault(fingerprint, RuleMemory())
    memory.last_value = value
    RULE_SIGNAL.labels(rule=rule.rule_id, node=node or "global").set(value)

    # Suppression freezes detector memory. It prevents duplicate incident creation,
    # but it is never treated as recovery and can never close an active incident.
    if suppressed:
        memory.last_condition = False
        RULE_CONDITION.labels(rule=rule.rule_id, node=node or "global").set(0)
        evidence["suppressed"] = True
        evidence["suppressionSemantics"] = "STATE_FROZEN_NOT_RECOVERY"
        return

    bad = is_bad(rule, value)
    recovered = is_recovered(rule, value)
    memory.last_condition = bad
    RULE_CONDITION.labels(rule=rule.rule_id, node=node or "global").set(1 if bad else 0)

    if bad:
        memory.consecutive_bad += 1
        evidence["consecutiveEvaluations"] = memory.consecutive_bad
        memory.consecutive_good = 0
        if memory.active_incident_id:
            await asyncio.to_thread(refresh_incident_sync, memory.active_incident_id, value, evidence)
        elif memory.consecutive_bad >= rule.open_evaluations:
            memory.active_incident_id = await asyncio.to_thread(open_incident_sync, rule, node, value, evidence)
    elif recovered:
        memory.consecutive_bad = 0
        memory.consecutive_good += 1
        if memory.active_incident_id and memory.consecutive_good >= rule.close_evaluations:
            await asyncio.to_thread(resolve_incident_sync, rule, node, memory.active_incident_id, value, evidence)
            memory.active_incident_id = None
            memory.consecutive_good = 0
    else:
        memory.consecutive_bad = 0
        memory.consecutive_good = 0


async def fetch_json(url: str, timeout: float = 4.0) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        emit_log("WARNING", "context_fetch_failed", url=url, error=str(exc))
        return {}


async def fetch_node_health() -> dict[str, dict[str, Any]]:
    async with httpx.AsyncClient(timeout=3) as client:
        async def one(node: str, base_url: str) -> tuple[str, dict[str, Any]]:
            try:
                response = await client.get(f"{base_url}/admin/diagnostics")
                response.raise_for_status()
                return node, response.json()
            except Exception as exc:
                return node, {"nodeId": node, "processStatus": "unreachable", "acceptingPayments": False, "error": str(exc)}
        rows = await asyncio.gather(*[one(node, url) for node, url in PAYMENT_NODE_URLS.items()])
    return dict(rows)


def failure_kind_map(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    for row in rows:
        metric = row.get("metric", {})
        node = metric.get("failed_node", "unknown")
        kind = metric.get("error_kind", "unknown")
        try:
            value = float(row.get("value", [0, 0])[1])
        except (TypeError, ValueError, IndexError):
            continue
        result.setdefault(node, {})[kind] = value
    return result


def update_availability_transitions(node: str, accepting: bool) -> int:
    now = time.monotonic()
    history = node_availability_history.setdefault(node, deque())
    previous = last_node_accepting.get(node)
    if previous is not None and previous != accepting:
        history.append((now, accepting))
    last_node_accepting[node] = accepting
    while history and now - history[0][0] > FLAPPING_WINDOW_SECONDS:
        history.popleft()
    return len(history)


def availability_transition_age_seconds(node: str) -> float | None:
    history = node_availability_history.get(node)
    if not history:
        return None
    return max(0.0, time.monotonic() - history[-1][0])


def seconds_since_iso(value: Any) -> float | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return max(0.0, (utc_now() - timestamp).total_seconds())
    except (TypeError, ValueError):
        return None


def capacity_transition_hold_down(diagnostics: dict[str, Any]) -> tuple[bool, float | None]:
    reason = str(diagnostics.get("capacityChangeReason") or "")
    age = seconds_since_iso(diagnostics.get("capacityChangedAt"))
    active = bool(
        reason in {"scenario_reset", "automatic_scale"}
        and age is not None
        and age < CAPACITY_TRANSITION_HOLD_DOWN_SECONDS
    )
    return active, age


def aggregate_failure_ratio(node: str, success: dict[str, float], failed: dict[str, float]) -> float:
    good = max(0.0, success.get(node, 0.0))
    bad = max(0.0, failed.get(node, 0.0))
    return bad / max(0.001, good + bad)


async def evaluation_cycle() -> dict[str, Any]:
    (
        retry_rows,
        latency_rows,
        checkout_failure_rows,
        success_rows,
        failed_rows,
        failure_kind_rows,
        checkout_p95_rows,
        throughput_rows,
        payment_processing_p95_rows,
        capacity_units_rows,
        external_auth_failure_rows,
        external_call_rows,
    ) = await asyncio.gather(
        prometheus_query('sum(rate(opsai_router_retries_total[1m])) by (failed_node)'),
        prometheus_query('histogram_quantile(0.95, sum by (le,node) (rate(opsai_router_node_duration_seconds_bucket[1m])))'),
        prometheus_query('100 * sum(rate(opsai_checkout_requests_total{status="failed"}[1m])) / clamp_min(sum(rate(opsai_checkout_requests_total[1m])), 0.001)'),
        prometheus_query('sum(rate(opsai_router_requests_total{status="success"}[30s])) by (node)'),
        prometheus_query('sum(rate(opsai_router_requests_total{status="failed"}[30s])) by (node)'),
        prometheus_query('sum(rate(opsai_router_failures_total[30s])) by (failed_node,error_kind)'),
        prometheus_query('histogram_quantile(0.95, sum by (le) (rate(opsai_checkout_duration_seconds_bucket[1m])))'),
        prometheus_query('sum(rate(opsai_checkout_requests_total[1m]))'),
        prometheus_query('histogram_quantile(0.95, sum by (le,node) (rate(opsai_payment_processing_duration_seconds_bucket[30s])))'),
        prometheus_query('opsai_payment_capacity_units'),
        prometheus_query('sum(rate(opsai_external_service_auth_failures_total[30s])) by (service)'),
        prometheus_query('sum(rate(opsai_external_service_calls_total[30s])) by (service,status)'),
    )
    traffic_profile, node_health = await asyncio.gather(
        fetch_json(TRAFFIC_PROFILE_URL),
        fetch_node_health(),
    )
    retry_by_node = vector_to_map(retry_rows, "failed_node")
    latency_by_node = vector_to_map(latency_rows, "node")
    success_by_node = vector_to_map(success_rows, "node")
    failed_by_node = vector_to_map(failed_rows, "node")
    failures_by_kind = failure_kind_map(failure_kind_rows)
    checkout_failure = next(iter(vector_to_map(checkout_failure_rows, "none").values()), 0.0)
    checkout_p95 = next(iter(vector_to_map(checkout_p95_rows, "none").values()), 0.0)
    throughput = next(iter(vector_to_map(throughput_rows, "none").values()), 0.0)
    payment_processing_p95_by_node = vector_to_map(payment_processing_p95_rows, "node")
    capacity_units_by_node = vector_to_map(capacity_units_rows, "node")
    external_auth_failures_by_service = vector_to_map(external_auth_failure_rows, "service")
    external_call_rates: dict[str, dict[str, float]] = {}
    for row in external_call_rows:
        metric = row.get("metric", {})
        service = str(metric.get("service") or "unknown")
        status = str(metric.get("status") or "unknown")
        try:
            rate = float(row.get("value", [0, 0])[1])
        except (TypeError, ValueError, IndexError):
            continue
        external_call_rates.setdefault(service, {})[status] = rate

    all_nodes = sorted(set(latency_by_node) | set(retry_by_node) | set(PAYMENT_NODE_URLS) | {"payment-node-1", "payment-node-2", "payment-node-3"})
    failure_ratio_by_node = {
        node: aggregate_failure_ratio(node, success_by_node, failed_by_node)
        for node in all_nodes
    }
    transitions_by_node = {
        node: update_availability_transitions(
            node,
            bool(node_health.get(node, {}).get("acceptingPayments", True)),
        )
        for node in all_nodes
    }
    timeout_rule = RULES["payment_node_timeout"]
    latency_rule = RULES["payment_node_latency"]
    unavailable_rule = RULES["payment_node_unavailable"]
    network_rule = RULES["payment_node_network_instability"]
    flapping_rule = RULES["payment_node_flapping"]
    shared_rule = RULES["payment_shared_dependency_outage"]
    fleet_rule = RULES["payment_fleet_capacity_degradation"]
    hung_rule = RULES["payment_node_hung"]
    external_auth_rule = RULES["external_service_authentication_failure"]

    shared_nodes = [
        node for node in all_nodes
        if (
            failures_by_kind.get(node, {}).get("shared_dependency", 0.0) > RETRY_RECOVERY_RATE
            or str(node_health.get(node, {}).get("faultMode") or "") == "shared_dependency"
        )
    ]
    unavailable_nodes = [
        node for node in all_nodes
        if str(node_health.get(node, {}).get("faultMode") or "") == "unavailable"
    ]
    slow_nodes = [
        node for node in all_nodes
        if latency_by_node.get(node, 0.0) > LATENCY_OPEN_SECONDS
    ]
    target_users = float(traffic_profile.get("targetUsers") or 0)
    base_users = float(traffic_profile.get("baseTargetUsers") or target_users or 0)
    traffic_ratio = target_users / base_users if base_users > 0 else float(traffic_profile.get("activityRatio") or 0)
    traffic_surge = bool(
        traffic_profile.get("trafficOverride", {}).get("active")
        or traffic_profile.get("overrideActive")
        or traffic_ratio >= 1.5
        or str(traffic_profile.get("profile", "")).lower() == "surge"
    )

    shared_evidence = {
        "metric": "correlated_shared_dependency_failures",
        "observed": float(len(shared_nodes)),
        "unit": shared_rule.unit,
        "threshold": shared_rule.open_threshold,
        "affectedNodes": shared_nodes,
        "failureKindsByNode": failures_by_kind,
        "nodeDiagnostics": node_health,
        "checkoutFailurePercent": checkout_failure,
        "checkoutP95Seconds": checkout_p95,
        "trafficContext": traffic_profile,
        "evaluatedAt": iso_now(),
    }
    await evaluate_rule(shared_rule, "", float(len(shared_nodes)), shared_evidence)
    shared_incident_id = active_rule_incident_id(shared_rule, "")
    shared_active = bool(shared_incident_id) or len(shared_nodes) >= 2
    global shared_dependency_last_active_at
    if shared_active:
        shared_dependency_last_active_at = time.monotonic()
    post_shared_cooldown_active = bool(
        not shared_active
        and shared_dependency_last_active_at is not None
        and time.monotonic() - shared_dependency_last_active_at < POST_SHARED_DEPENDENCY_COOLDOWN_SECONDS
    )

    existing_fleet_incident_id = active_rule_incident_id(fleet_rule, "")
    target_capacities_ready = all(
        int(capacity_units_by_node.get(node, 0)) == 2
        and int(node_health.get(node, {}).get("capacityUnits") or 0) == 2
        for node in CAPACITY_TARGET_NODES
    )
    processing_samples_ready = all(
        float(payment_processing_p95_by_node.get(node, 0)) > 0
        for node in CAPACITY_TARGET_NODES
    )
    processing_recovered = processing_samples_ready and all(
        float(payment_processing_p95_by_node.get(node, 999)) <= CAPACITY_PROCESSING_RECOVERY_SECONDS
        for node in CAPACITY_TARGET_NODES
    )
    node3_unavailable = "payment-node-3" in unavailable_nodes
    checkout_stable = checkout_failure <= CHECKOUT_FAILURE_RECOVERY_PERCENT
    capacity_recovery_qualified = bool(
        existing_fleet_incident_id
        and traffic_surge
        and node3_unavailable
        and target_capacities_ready
        and processing_recovered
        and checkout_stable
    )
    if capacity_recovery_qualified:
        fleet_signal = 0.0
        capacity_recovery_source = "VERIFIED_AUTO_SCALE"
    elif existing_fleet_incident_id and not traffic_surge:
        # Allow the detector to close, but explicitly record that demand expiry -- not
        # the scaling action -- caused the observed recovery. Automation must not count it.
        fleet_signal = 0.0
        capacity_recovery_source = "TRAFFIC_NORMALIZED"
    elif existing_fleet_incident_id:
        # Prevent a transient dip in router-window latency from closing the fleet
        # incident before independent processing-capacity verification succeeds.
        fleet_signal = max(1.0, float(len(slow_nodes)))
        capacity_recovery_source = "PENDING_CAPACITY_VERIFICATION"
    else:
        fleet_signal = float(len(slow_nodes)) if traffic_surge else 0.0
        capacity_recovery_source = "NOT_APPLICABLE"

    fleet_evidence = {
        "metric": "fleet_slow_node_count",
        "observed": fleet_signal,
        "unit": fleet_rule.unit,
        "threshold": fleet_rule.open_threshold,
        "affectedNodes": slow_nodes,
        "unavailableNodes": unavailable_nodes,
        "nodeP95LatencySeconds": latency_by_node,
        "paymentProcessingP95SecondsByNode": payment_processing_p95_by_node,
        "capacityUnitsByNode": capacity_units_by_node,
        "nodeDiagnostics": node_health,
        "checkoutP95Seconds": checkout_p95,
        "checkoutFailurePercent": checkout_failure,
        "checkoutRequestsPerSecond": throughput,
        "trafficContext": traffic_profile,
        "trafficSurgeActive": traffic_surge,
        "node3Unavailable": node3_unavailable,
        "targetCapacitiesReady": target_capacities_ready,
        "processingSamplesReady": processing_samples_ready,
        "processingRecoveryThresholdSeconds": CAPACITY_PROCESSING_RECOVERY_SECONDS,
        "processingRecovered": processing_recovered,
        "checkoutStable": checkout_stable,
        "capacityRecoveryQualified": capacity_recovery_qualified,
        "capacityRecoverySource": capacity_recovery_source,
        "evaluatedAt": iso_now(),
    }
    await evaluate_rule(fleet_rule, "", fleet_signal, fleet_evidence, suppressed=shared_active)
    fleet_incident_id = active_rule_incident_id(fleet_rule, "")
    fleet_active = bool(fleet_incident_id) or (traffic_surge and len(slow_nodes) >= 2)

    # Once an aggregate fleet incident exists, lower-level node symptoms are evidence,
    # not independent incidents requiring duplicate investigations.
    if shared_incident_id:
        for node in all_nodes:
            for lower_rule in (hung_rule, flapping_rule, timeout_rule, unavailable_rule, network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync,
                        lower_rule,
                        node,
                        lower_id,
                        shared_rule,
                        shared_incident_id,
                    )
    if fleet_incident_id:
        for node in all_nodes:
            for lower_rule in (hung_rule, unavailable_rule, network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync,
                        lower_rule,
                        node,
                        lower_id,
                        fleet_rule,
                        fleet_incident_id,
                    )

    classifications: dict[str, str] = {}
    for node in all_nodes:
        retry_value = retry_by_node.get(node, 0.0)
        latency_value = latency_by_node.get(node, 0.0)
        failure_ratio = failure_ratio_by_node.get(node, 0.0)
        success_rate = success_by_node.get(node, 0.0)
        failure_rate = failed_by_node.get(node, 0.0)
        kinds = failures_by_kind.get(node, {})
        transitions = transitions_by_node.get(node, 0)
        transition_age = availability_transition_age_seconds(node)
        diagnostic = node_health.get(node, {})
        accepting = bool(diagnostic.get("acceptingPayments", True))
        fault_mode = str(diagnostic.get("faultMode") or "none")
        explicit_hung = fault_mode == "hung"
        explicit_timeout = (
            not explicit_hung
            and (
                fault_mode == "timeout"
                or kinds.get("timeout", 0.0) > RETRY_RECOVERY_RATE
            )
        )
        recent_availability_transition = bool(
            transitions > 0
            and transition_age is not None
            and transition_age <= FLAPPING_HOLD_DOWN_SECONDS
        )
        explicit_unavailable = (
            fault_mode == "unavailable"
            or kinds.get("node_unavailable", 0.0) > RETRY_RECOVERY_RATE
        )
        reset_failure_ratio = float(
            kinds.get("connection_reset", 0.0)
            / max(0.001, success_rate + failure_rate)
        )
        dominant_connection_reset = bool(
            kinds.get("connection_reset", 0.0) > RETRY_RECOVERY_RATE
            and failure_ratio >= UNAVAILABLE_FAILURE_RATIO
        )
        continuous_connection_loss = bool(
            kinds.get("connection_reset", 0.0) > RETRY_RECOVERY_RATE
            and failure_rate > RETRY_RECOVERY_RATE
            and success_rate <= RETRY_RECOVERY_RATE
        )
        unavailable_signal = 1.0 if (
            not explicit_hung
            and not explicit_timeout
            and not recent_availability_transition
            and (explicit_unavailable or continuous_connection_loss or dominant_connection_reset)
        ) else 0.0
        flapping_signal = float(transitions)
        timeout_signal = max(retry_value, timeout_rule.open_threshold * 1.1) if explicit_timeout else 0.0
        network_signal = failure_ratio if (
            kinds.get("connection_reset", 0.0) > RETRY_RECOVERY_RATE
            and failure_rate > RETRY_RECOVERY_RATE
            and success_rate > RETRY_RECOVERY_RATE
            and NETWORK_INSTABILITY_RATIO < failure_ratio < UNAVAILABLE_FAILURE_RATIO
            and not recent_availability_transition
        ) else 0.0
        base_evidence = {
            "node": node,
            "retryRate": retry_value,
            "successRate": success_rate,
            "failureRate": failure_rate,
            "failureRatio": failure_ratio,
            "connectionResetDominanceRatio": reset_failure_ratio,
            "failureKinds": kinds,
            "nodeDiagnostics": diagnostic,
            "availabilityTransitions": transitions,
            "lastTransitionAgeSeconds": transition_age,
            "flappingHoldDownSeconds": FLAPPING_HOLD_DOWN_SECONDS,
            "p95LatencySeconds": latency_value,
            "peerLatencies": {name: value for name, value in latency_by_node.items() if name != node},
            "checkoutFailurePercent": checkout_failure,
            "checkoutP95Seconds": checkout_p95,
            "trafficContext": traffic_profile,
            "evaluatedAt": iso_now(),
        }
        hung_signal = 1.0 if explicit_hung else 0.0
        await evaluate_rule(
            hung_rule,
            node,
            hung_signal,
            {
                **base_evidence,
                "metric": "payment_worker_hung_state",
                "observed": hung_signal,
                "unit": hung_rule.unit,
                "threshold": hung_rule.open_threshold,
                "restartGeneration": diagnostic.get("restartGeneration"),
                "lastRestartAt": diagnostic.get("lastRestartAt"),
            },
            suppressed=shared_active or fleet_active,
        )
        hung_incident_id = active_rule_incident_id(hung_rule, node)
        hung_active = bool(hung_incident_id) or explicit_hung

        await evaluate_rule(
            flapping_rule,
            node,
            flapping_signal,
            {**base_evidence, "metric": "node_availability_transitions", "observed": flapping_signal, "unit": flapping_rule.unit, "threshold": flapping_rule.open_threshold},
            suppressed=shared_active or fleet_active or hung_active,
        )
        flapping_incident_id = active_rule_incident_id(flapping_rule, node)
        flapping_active = bool(flapping_incident_id) or transitions >= FLAPPING_TRANSITIONS

        # Explicit timeout evidence dominates the node health flag. The deliberate
        # timeout mode keeps acceptingPayments false, but it is not an availability loss.
        await evaluate_rule(
            timeout_rule,
            node,
            timeout_signal,
            {**base_evidence, "metric": "router_timeout_retry_rate", "observed": timeout_signal, "unit": timeout_rule.unit, "threshold": timeout_rule.open_threshold},
            suppressed=shared_active or fleet_active or hung_active or flapping_active,
        )
        timeout_incident_id = active_rule_incident_id(timeout_rule, node)
        timeout_active = bool(timeout_incident_id) or explicit_timeout

        await evaluate_rule(
            unavailable_rule,
            node,
            unavailable_signal,
            {**base_evidence, "metric": "router_node_failure_ratio", "observed": unavailable_signal, "unit": unavailable_rule.unit, "threshold": unavailable_rule.open_threshold},
            suppressed=shared_active or fleet_active or hung_active or flapping_active or timeout_active,
        )
        unavailable_incident_id = active_rule_incident_id(unavailable_rule, node)
        unavailable_active = bool(unavailable_incident_id) or unavailable_signal > UNAVAILABLE_FAILURE_RATIO

        await evaluate_rule(
            network_rule,
            node,
            network_signal,
            {**base_evidence, "metric": "router_node_failure_ratio", "observed": network_signal, "unit": network_rule.unit, "threshold": network_rule.open_threshold},
            suppressed=shared_active or fleet_active or hung_active or flapping_active or timeout_active or unavailable_active,
        )
        network_incident_id = active_rule_incident_id(network_rule, node)
        network_active = bool(network_incident_id) or network_signal > NETWORK_INSTABILITY_RATIO

        if hung_incident_id:
            for lower_rule in (flapping_rule, timeout_rule, unavailable_rule, network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync, lower_rule, node, lower_id, hung_rule, hung_incident_id
                    )
        elif flapping_incident_id:
            for lower_rule in (timeout_rule, unavailable_rule, network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync, lower_rule, node, lower_id, flapping_rule, flapping_incident_id
                    )
        elif timeout_incident_id:
            for lower_rule in (unavailable_rule, network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync, lower_rule, node, lower_id, timeout_rule, timeout_incident_id
                    )
        elif unavailable_incident_id:
            for lower_rule in (network_rule, latency_rule):
                lower_id = active_rule_incident_id(lower_rule, node)
                if lower_id:
                    await asyncio.to_thread(
                        supersede_incident_sync, lower_rule, node, lower_id, unavailable_rule, unavailable_incident_id
                    )

        retry_classification_active = hung_active or flapping_active or timeout_active or unavailable_active or network_active
        transition_hold_down, transition_age_seconds = capacity_transition_hold_down(
            node_health.get(node, {})
        )
        peer_values = [
            float(value)
            for name, value in latency_by_node.items()
            if name != node and float(value) > 0
        ]
        peer_average = sum(peer_values) / len(peer_values) if peer_values else 0.0
        strong_isolated_outlier = bool(
            latency_value >= LATENCY_OPEN_SECONDS * 1.25
            and peer_average > 0
            and latency_value / max(peer_average, 0.001) >= STRONG_LATENCY_OUTLIER_RATIO
            and not retry_classification_active
            and not fleet_active
            and not shared_active
        )
        latency_suppressed = (
            retry_classification_active
            or fleet_active
            or shared_active
            or (transition_hold_down and not strong_isolated_outlier)
        )
        latency_evidence = {
            **base_evidence,
            "metric": "router_node_p95_latency",
            "observed": latency_value,
            "unit": latency_rule.unit,
            "threshold": latency_rule.open_threshold,
            "suppressedByCorrelatedSignal": latency_suppressed,
            "capacityTransitionHoldDown": transition_hold_down,
            "capacityTransitionAgeSeconds": transition_age_seconds,
            "capacityTransitionHoldDownSeconds": CAPACITY_TRANSITION_HOLD_DOWN_SECONDS,
            "strongIsolatedOutlierBypass": strong_isolated_outlier,
            "peerAverageP95Seconds": peer_average,
        }
        await evaluate_rule(
            latency_rule,
            node,
            latency_value,
            latency_evidence,
            suppressed=latency_suppressed,
        )
        if shared_active:
            classifications[node] = "shared_dependency"
        elif hung_active:
            classifications[node] = "hung"
        elif fleet_active and (node in slow_nodes or node in unavailable_nodes):
            classifications[node] = "fleet_capacity"
        elif flapping_active:
            classifications[node] = "flapping"
        elif timeout_active:
            classifications[node] = "timeout"
        elif unavailable_active:
            classifications[node] = "unavailable"
        elif network_active:
            classifications[node] = "network_instability"
        elif transition_hold_down and latency_value > LATENCY_OPEN_SECONDS:
            classifications[node] = "capacity_transition_hold_down"
        elif latency_value > LATENCY_OPEN_SECONDS:
            classifications[node] = "isolated_latency"
        else:
            classifications[node] = "healthy"

    external_auth_active = False
    for service_name, failure_rate in external_auth_failures_by_service.items():
        auth_evidence = {
            "metric": "external_service_auth_failure_rate",
            "observed": failure_rate,
            "unit": external_auth_rule.unit,
            "threshold": external_auth_rule.open_threshold,
            "externalService": service_name,
            "callRatesByStatus": external_call_rates.get(service_name, {}),
            "evaluatedAt": iso_now(),
        }
        await evaluate_rule(
            external_auth_rule,
            service_name,
            failure_rate,
            auth_evidence,
        )
        if active_rule_incident_id(external_auth_rule, service_name) or failure_rate >= external_auth_rule.open_threshold:
            external_auth_active = True

    checkout_rule = RULES["checkout_failure_rate"]
    checkout_evidence = {
        "metric": "checkout_failure_percentage",
        "observed": checkout_failure,
        "unit": checkout_rule.unit,
        "threshold": checkout_rule.open_threshold,
        "failureKindsByNode": failures_by_kind,
        "trafficContext": traffic_profile,
        "evaluatedAt": iso_now(),
    }
    checkout_evidence["postSharedDependencyCooldownActive"] = post_shared_cooldown_active
    checkout_evidence["postSharedDependencyCooldownSeconds"] = POST_SHARED_DEPENDENCY_COOLDOWN_SECONDS
    checkout_evidence["externalAuthenticationFailureActive"] = external_auth_active
    await evaluate_rule(
        checkout_rule,
        "",
        checkout_failure,
        checkout_evidence,
        suppressed=shared_active or fleet_active or post_shared_cooldown_active or external_auth_active,
    )

    return {
        "evaluatedAt": iso_now(),
        "retryRateByNode": retry_by_node,
        "p95LatencySecondsByNode": latency_by_node,
        "paymentProcessingP95SecondsByNode": payment_processing_p95_by_node,
        "capacityUnitsByNode": capacity_units_by_node,
        "externalServiceAuthenticationFailuresByService": external_auth_failures_by_service,
        "externalServiceCallRates": external_call_rates,
        "postSharedDependencyCooldownActive": post_shared_cooldown_active,
        "capacityRecovery": {
            "qualified": capacity_recovery_qualified,
            "source": capacity_recovery_source,
            "processingThresholdSeconds": CAPACITY_PROCESSING_RECOVERY_SECONDS,
            "trafficSurgeActive": traffic_surge,
            "node3Unavailable": node3_unavailable,
            "targetCapacitiesReady": target_capacities_ready,
            "processingRecovered": processing_recovered,
            "checkoutStable": checkout_stable,
        },
        "failureRateByNode": failed_by_node,
        "successRateByNode": success_by_node,
        "failureRatioByNode": failure_ratio_by_node,
        "failureKindsByNode": failures_by_kind,
        "nodeDiagnostics": node_health,
        "availabilityTransitionsByNode": transitions_by_node,
        "checkoutFailurePercent": checkout_failure,
        "checkoutP95Seconds": checkout_p95,
        "checkoutRequestsPerSecond": throughput,
        "trafficContext": traffic_profile,
        "classifications": classifications,
        "sharedDependencyActive": shared_active,
        "fleetCapacityActive": fleet_active,
    }


async def detector_loop() -> None:
    global last_evaluation_at, last_evaluation_summary
    while not stop_event.is_set():
        try:
            last_evaluation_summary = await evaluation_cycle()
            last_evaluation_at = utc_now()
            LAST_EVALUATION.set(last_evaluation_at.timestamp())
            EVALUATIONS.labels(outcome="success").inc()
        except Exception as exc:
            EVALUATIONS.labels(outcome="failed").inc()
            emit_log("ERROR", "evaluation_failed", error=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=EVALUATION_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


@app.on_event("startup")
async def startup_event() -> None:
    await asyncio.to_thread(initialise_database_sync)
    await asyncio.to_thread(hydrate_active_incidents_sync)
    app.state.detector_task = asyncio.create_task(detector_loop())
    emit_log("INFO", "service_started", prometheusUrl=PROMETHEUS_URL)


@app.on_event("shutdown")
async def shutdown_event() -> None:
    stop_event.set()
    task = getattr(app.state, "detector_task", None)
    if task:
        await task


@app.get("/health")
async def health() -> dict[str, Any]:
    evaluation_age = None
    if last_evaluation_at:
        evaluation_age = (utc_now() - last_evaluation_at).total_seconds()
    healthy = db_ready and evaluation_age is not None and evaluation_age < max(30, EVALUATION_INTERVAL_SECONDS * 4)
    return {
        "status": "healthy" if healthy else "starting",
        "service": "opsai-core",
        "version": SERVICE_VERSION,
        "databaseReady": db_ready,
        "lastEvaluationAgeSeconds": evaluation_age,
    }


@app.get("/evaluation")
async def evaluation() -> dict[str, Any]:
    return {
        "lastEvaluation": last_evaluation_summary,
        "rules": {
            key: {
                "openThreshold": rule.open_threshold,
                "recoveryThreshold": rule.recovery_threshold,
                "openEvaluations": rule.open_evaluations,
                "closeEvaluations": rule.close_evaluations,
                "unit": rule.unit,
            } for key, rule in RULES.items()
        },
        "memory": {
            key: {
                "consecutiveBad": value.consecutive_bad,
                "consecutiveGood": value.consecutive_good,
                "lastValue": value.last_value,
                "activeIncidentId": value.active_incident_id,
                "lastCondition": value.last_condition,
            } for key, value in rule_memory.items()
        },
    }


def list_incidents_sync(status: str, limit: int) -> list[dict[str, Any]]:
    where = ""
    params: list[Any] = []
    if status == "active":
        where = "WHERE status IN ('OPEN','ACKNOWLEDGED')"
    elif status != "all":
        where = "WHERE status=%s"
        params.append(status.upper())
    params.append(limit)
    with db_connect() as conn:
        rows = conn.execute(f"""
            SELECT id::text, fingerprint, incident_type, title, severity, status,
                   service, node, summary, runbook_hint, evidence,
                   first_detected_at, last_detected_at, opened_at,
                   acknowledged_at, acknowledged_by, resolved_at,
                   resolution_reason, updated_at
            FROM incidents {where}
            ORDER BY opened_at DESC LIMIT %s
        """, params).fetchall()
    return rows



def serialise_problem_row(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    if "id" in result:
        result["id"] = str(result["id"])
    return result


def list_problems_sync(status: str, limit: int) -> list[dict[str, Any]]:
    requested = status.upper()
    if requested != "ALL" and requested not in PROBLEM_STATUSES:
        raise ValueError(f"Unsupported problem status: {status}")
    where = "" if requested == "ALL" else "WHERE status=%s"
    params: tuple[Any, ...] = (limit,) if requested == "ALL" else (requested, limit)
    with db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM problems
            {where}
            ORDER BY
                CASE status
                    WHEN 'CANDIDATE' THEN 1
                    WHEN 'UNDER_REVIEW' THEN 2
                    WHEN 'CONFIRMED' THEN 3
                    WHEN 'INVESTIGATING' THEN 4
                    WHEN 'CORRECTIVE_ACTION_PLANNED' THEN 5
                    WHEN 'MONITORING' THEN 6
                    WHEN 'CLOSED' THEN 7
                    WHEN 'REJECTED' THEN 8
                    ELSE 9
                END,
                risk_score DESC,
                updated_at DESC
            LIMIT %s
            """,
            params,
        ).fetchall()
    return [serialise_problem_row(row) for row in rows]


def problem_detail_sync(problem_id: str) -> dict[str, Any]:
    with db_connect() as conn:
        problem = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (problem_id,),
        ).fetchone()
        if not problem:
            raise LookupError("Problem not found")
        events = conn.execute(
            """
            SELECT event_type,actor,message,details,created_at
            FROM problem_events
            WHERE problem_id=%s
            ORDER BY created_at
            """,
            (problem_id,),
        ).fetchall()
    return {
        "problem": serialise_problem_row(problem),
        "events": events,
    }


def upsert_problem_candidate_sync(
    request: ProblemCandidateRequest,
) -> tuple[dict[str, Any], bool, bool]:
    now = utc_now()
    with db_connect() as conn:
        existing = conn.execute(
            "SELECT * FROM problems WHERE problem_key=%s",
            (request.problemKey,),
        ).fetchone()
        if not existing:
            problem_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO problems (
                    id,problem_key,title,category,record_class,status,
                    risk_score,risk_level,owner_queue,owner_name,summary,hypothesis,
                    occurrence_count,recurrence_after_action,average_interval_seconds,
                    first_occurrence_at,latest_occurrence_at,incident_types,scopes,
                    linked_incident_ids,origin_breakdown,candidate_evidence,
                    created_at,updated_at
                )
                VALUES (
                    %s,%s,%s,%s,%s,'CANDIDATE',
                    %s,%s,'Problem Management','',%s,%s,
                    %s,0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                """,
                (
                    problem_id,
                    request.problemKey,
                    request.title,
                    request.category,
                    request.recordClass,
                    request.riskScore,
                    request.riskLevel,
                    request.summary,
                    request.hypothesis,
                    request.occurrenceCount,
                    request.averageIntervalSeconds,
                    request.firstOccurrenceAt,
                    request.latestOccurrenceAt,
                    json.dumps(request.incidentTypes),
                    json.dumps(request.scopes),
                    json.dumps(request.linkedIncidentIds),
                    json.dumps(request.originBreakdown),
                    json.dumps(request.evidence),
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO problem_events (
                    problem_id,event_type,actor,message,details,created_at
                )
                VALUES (%s,'CANDIDATE_CREATED','opsai-predictor',%s,%s,%s)
                """,
                (
                    problem_id,
                    (
                        f"Recurring incident patterns created candidate "
                        f"{request.title}."
                    ),
                    json.dumps(
                        {
                            "recordClass": request.recordClass,
                            "riskScore": request.riskScore,
                            "occurrenceCount": request.occurrenceCount,
                        }
                    ),
                    now,
                ),
            )
            problem = conn.execute(
                "SELECT * FROM problems WHERE id=%s",
                (problem_id,),
            ).fetchone()
            return serialise_problem_row(problem), True, True

        changed = any(
            [
                int(existing.get("occurrence_count") or 0) != request.occurrenceCount,
                float(existing.get("risk_score") or 0) != float(request.riskScore),
                existing.get("latest_occurrence_at") != request.latestOccurrenceAt,
                existing.get("record_class") != request.recordClass,
                existing.get("origin_breakdown") != request.originBreakdown,
            ]
        )
        recurrence_after_action = int(existing.get("recurrence_after_action") or 0)
        if existing.get("status") in {"MONITORING", "CLOSED"}:
            increase = max(
                0,
                request.occurrenceCount - int(existing.get("occurrence_count") or 0),
            )
            recurrence_after_action += increase

        conn.execute(
            """
            UPDATE problems
            SET title=%s,category=%s,record_class=%s,risk_score=%s,risk_level=%s,
                summary=%s,hypothesis=%s,occurrence_count=%s,
                recurrence_after_action=%s,average_interval_seconds=%s,
                first_occurrence_at=%s,latest_occurrence_at=%s,
                incident_types=%s,scopes=%s,linked_incident_ids=%s,
                origin_breakdown=%s,candidate_evidence=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                request.title,
                request.category,
                request.recordClass,
                request.riskScore,
                request.riskLevel,
                request.summary,
                request.hypothesis,
                request.occurrenceCount,
                recurrence_after_action,
                request.averageIntervalSeconds,
                request.firstOccurrenceAt,
                request.latestOccurrenceAt,
                json.dumps(request.incidentTypes),
                json.dumps(request.scopes),
                json.dumps(request.linkedIncidentIds),
                json.dumps(request.originBreakdown),
                json.dumps(request.evidence),
                now,
                existing["id"],
            ),
        )
        if changed:
            conn.execute(
                """
                INSERT INTO problem_events (
                    problem_id,event_type,actor,message,details,created_at
                )
                VALUES (%s,'CANDIDATE_REFRESHED','opsai-predictor',%s,%s,%s)
                """,
                (
                    existing["id"],
                    (
                        f"Recurring evidence refreshed for "
                        f"{request.title}."
                    ),
                    json.dumps(
                        {
                            "recordClass": request.recordClass,
                            "riskScore": request.riskScore,
                            "occurrenceCount": request.occurrenceCount,
                            "recurrenceAfterAction": recurrence_after_action,
                        }
                    ),
                    now,
                ),
            )
        problem = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (existing["id"],),
        ).fetchone()
        return serialise_problem_row(problem), False, changed


def transition_problem_sync(
    problem_id: str,
    request: ProblemTransitionRequest,
) -> dict[str, Any]:
    target = request.status.upper()
    if target not in PROBLEM_STATUSES:
        raise ValueError(f"Unsupported problem status: {request.status}")
    now = utc_now()
    with db_connect() as conn:
        current = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (problem_id,),
        ).fetchone()
        if not current:
            raise LookupError("Problem not found")
        current_status = str(current.get("status") or "")
        allowed = PROBLEM_TRANSITIONS.get(current_status, set())
        if target != current_status and target not in allowed:
            raise RuntimeError(
                f"Transition {current_status} -> {target} is not allowed"
            )

        confirmed_root_cause = (
            request.confirmedRootCause
            or str(current.get("confirmed_root_cause") or "")
        )
        corrective_action = (
            request.correctiveAction
            or str(current.get("corrective_action") or "")
        )
        monitoring_notes = (
            request.monitoringNotes
            or str(current.get("monitoring_notes") or "")
        )
        rejection_reason = (
            request.rejectionReason
            or str(current.get("rejection_reason") or "")
        )
        confirmed_at = current.get("confirmed_at")
        closed_at = current.get("closed_at")
        rejected_at = current.get("rejected_at")
        if target == "CONFIRMED" and confirmed_at is None:
            confirmed_at = now
        if target == "CLOSED":
            closed_at = now
        if target == "REJECTED":
            rejected_at = now

        conn.execute(
            """
            UPDATE problems
            SET status=%s,confirmed_root_cause=%s,corrective_action=%s,
                monitoring_notes=%s,rejection_reason=%s,confirmed_at=%s,
                closed_at=%s,rejected_at=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                target,
                confirmed_root_cause,
                corrective_action,
                monitoring_notes,
                rejection_reason,
                confirmed_at,
                closed_at,
                rejected_at,
                now,
                problem_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO problem_events (
                problem_id,event_type,actor,message,details,created_at
            )
            VALUES (%s,'STATUS_CHANGED',%s,%s,%s,%s)
            """,
            (
                problem_id,
                request.actor,
                request.note
                or f"Problem status changed from {current_status} to {target}.",
                json.dumps(
                    {
                        "from": current_status,
                        "to": target,
                        "confirmedRootCause": confirmed_root_cause,
                        "correctiveAction": corrective_action,
                        "monitoringNotes": monitoring_notes,
                        "rejectionReason": rejection_reason,
                    }
                ),
                now,
            ),
        )
        problem = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (problem_id,),
        ).fetchone()
    return serialise_problem_row(problem)


def assign_problem_sync(
    problem_id: str,
    request: ProblemAssignmentRequest,
) -> dict[str, Any]:
    now = utc_now()
    with db_connect() as conn:
        current = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (problem_id,),
        ).fetchone()
        if not current:
            raise LookupError("Problem not found")
        conn.execute(
            """
            UPDATE problems
            SET owner_queue=%s,owner_name=%s,updated_at=%s
            WHERE id=%s
            """,
            (
                request.ownerQueue,
                request.ownerName,
                now,
                problem_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO problem_events (
                problem_id,event_type,actor,message,details,created_at
            )
            VALUES (%s,'ASSIGNED',%s,%s,%s,%s)
            """,
            (
                problem_id,
                request.actor,
                request.note
                or (
                    f"Problem assigned to {request.ownerQueue}"
                    + (
                        f" / {request.ownerName}"
                        if request.ownerName
                        else ""
                    )
                    + "."
                ),
                json.dumps(
                    {
                        "ownerQueue": request.ownerQueue,
                        "ownerName": request.ownerName,
                    }
                ),
                now,
            ),
        )
        problem = conn.execute(
            "SELECT * FROM problems WHERE id=%s",
            (problem_id,),
        ).fetchone()
    return serialise_problem_row(problem)


async def require_automation_token(token: str | None) -> None:
    if token != AUTOMATION_API_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid automation token")


@app.post("/api/external/incidents")
async def create_external_incident(
    request: ExternalIncidentRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await require_automation_token(x_opsai_automation_token)
    incident_id, created = await asyncio.to_thread(open_external_incident_sync, request)
    return {"incidentId": incident_id, "created": created, "status": "OPEN"}


@app.post("/api/external/incidents/{incident_id}/resolve")
async def resolve_external_incident(
    incident_id: str,
    request: ExternalResolutionRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await require_automation_token(x_opsai_automation_token)
    try:
        return await asyncio.to_thread(resolve_external_incident_sync, incident_id, request)
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/external/incidents/{incident_id}/events")
async def add_external_incident_event(
    incident_id: str,
    request: ExternalEventRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await require_automation_token(x_opsai_automation_token)
    now = utc_now()
    with db_connect() as conn:
        exists = conn.execute("SELECT 1 FROM incidents WHERE id=%s", (incident_id,)).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="Incident not found")
        conn.execute(
            """
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,%s,%s,%s,%s,%s)
            """,
            (
                incident_id,
                request.eventType,
                request.actor,
                request.message,
                json.dumps(request.details),
                now,
            ),
        )
    return {"status": "recorded", "incidentId": incident_id}



@app.post("/api/problems/candidates/upsert")
async def upsert_problem_candidate(
    request: ProblemCandidateRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> dict[str, Any]:
    await require_automation_token(x_opsai_automation_token)
    problem, created, changed = await asyncio.to_thread(
        upsert_problem_candidate_sync,
        request,
    )
    emit_log(
        "INFO",
        "problem_candidate_upserted",
        problemId=problem.get("id"),
        problemKey=request.problemKey,
        created=created,
        changed=changed,
        occurrenceCount=request.occurrenceCount,
        recordClass=request.recordClass,
    )
    return {
        "problem": problem,
        "created": created,
        "changed": changed,
    }


@app.get("/problems")
async def problems(
    status: str = Query(default="all"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        rows = await asyncio.to_thread(list_problems_sync, status, limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"count": len(rows), "problems": rows}


@app.get("/problems/summary")
async def problems_summary() -> dict[str, Any]:
    rows = await asyncio.to_thread(list_problems_sync, "all", 500)
    by_status = {
        status: sum(row.get("status") == status for row in rows)
        for status in PROBLEM_STATUSES
    }
    return {
        "total": len(rows),
        "open": sum(
            row.get("status") not in {"CLOSED", "REJECTED"}
            for row in rows
        ),
        "byStatus": by_status,
        "highRisk": sum(
            row.get("risk_level") == "HIGH"
            and row.get("status") not in {"CLOSED", "REJECTED"}
            for row in rows
        ),
        "demoCandidates": sum(
            row.get("record_class") == "DEMO_CANDIDATE"
            for row in rows
        ),
        "operationalCandidates": sum(
            row.get("record_class") == "OPERATIONAL_CANDIDATE"
            for row in rows
        ),
    }


@app.get("/problems/{problem_id}")
async def problem_detail(problem_id: str) -> dict[str, Any]:
    try:
        uuid.UUID(problem_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid problem ID") from exc
    try:
        return await asyncio.to_thread(problem_detail_sync, problem_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/problems/{problem_id}/transition")
async def transition_problem(
    problem_id: str,
    request: ProblemTransitionRequest,
) -> dict[str, Any]:
    try:
        uuid.UUID(problem_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid problem ID") from exc
    try:
        problem = await asyncio.to_thread(
            transition_problem_sync,
            problem_id,
            request,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    emit_log(
        "INFO",
        "problem_status_changed",
        problemId=problem_id,
        status=problem.get("status"),
        actor=request.actor,
    )
    return {"problem": problem}


@app.post("/problems/{problem_id}/assign")
async def assign_problem(
    problem_id: str,
    request: ProblemAssignmentRequest,
) -> dict[str, Any]:
    try:
        uuid.UUID(problem_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid problem ID") from exc
    try:
        problem = await asyncio.to_thread(
            assign_problem_sync,
            problem_id,
            request,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    emit_log(
        "INFO",
        "problem_assigned",
        problemId=problem_id,
        ownerQueue=request.ownerQueue,
        ownerName=request.ownerName,
        actor=request.actor,
    )
    return {"problem": problem}


@app.get("/incidents")
async def incidents(
    status: str = Query(default="all", pattern="^(all|active|open|acknowledged|resolved)$"),
    limit: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    rows = await asyncio.to_thread(list_incidents_sync, status, limit)
    # Enrich the incident summary with the latest governed recommendation and
    # execution state. The table can therefore show what happened before closure
    # without forcing the operator to open every investigation panel.
    investigations_by_incident: dict[str, dict[str, Any]] = {}
    remediations_by_incident: dict[str, dict[str, Any]] = {}
    tickets_by_incident: dict[str, dict[str, Any]] = {}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{OPSAI_AGENT_URL}/api/investigations")
            response.raise_for_status()
            payload = response.json()
        for item in payload.get("investigations", []):
            incident_id = str(item.get("incident_id") or "")
            if not incident_id or incident_id in investigations_by_incident:
                continue
            investigations_by_incident[incident_id] = item
    except Exception as exc:
        emit_log("WARNING", "incident_summary_investigation_enrichment_failed", error=str(exc))
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{OPSAI_AUTOMATION_URL}/state")
            response.raise_for_status()
            operations_payload = response.json()
        remediations_by_incident = {
            str(key): value
            for key, value in (operations_payload.get("remediations") or {}).items()
            if isinstance(value, dict)
        }
        tickets_by_incident = {
            str(item.get("incidentId")): item
            for item in (operations_payload.get("tickets") or [])
            if isinstance(item, dict) and item.get("incidentId")
        }
    except Exception as exc:
        emit_log("WARNING", "incident_summary_automation_enrichment_failed", error=str(exc))
    for row in rows:
        incident_id = str(row.get("id"))
        investigation = investigations_by_incident.get(incident_id) or {}
        remediation = remediations_by_incident.get(incident_id) or {}
        ticket = tickets_by_incident.get(incident_id) or {}
        evidence = row.get("evidence") or {}
        row["resolution_action"] = investigation.get("action_name") or remediation.get("action")
        row["resolution_policy"] = investigation.get("policy_decision")
        row["resolution_execution_status"] = (
            investigation.get("action_execution_status")
            or ("SUCCEEDED" if remediation.get("actionSucceeded") or remediation.get("verificationPassed") else None)
        )
        row["resolution_action_executed"] = bool(
            investigation.get("action_executed") or remediation.get("executed")
        )
        row["resolution_execution_result"] = (
            investigation.get("action_execution_result") or remediation or {}
        )
        row["repair_outcome"] = (
            remediation.get("repairOutcome")
            or evidence.get("repairOutcome")
            or ("MANUAL_INTERVENTION_REQUIRED" if ticket and ticket.get("status") != "RESOLVED" else None)
        )
    return {"count": len(rows), "incidents": rows}


@app.get("/incidents/{incident_id}")
async def incident_detail(incident_id: str) -> dict[str, Any]:
    try:
        uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident ID") from exc
    investigation: dict[str, Any] | None = None
    investigation_events: list[dict[str, Any]] = []
    with db_connect() as conn:
        incident = conn.execute(
            "SELECT *, id::text AS id_text FROM incidents WHERE id=%s",
            (incident_id,),
        ).fetchone()
        if not incident:
            raise HTTPException(status_code=404, detail="Incident not found")
        events = conn.execute(
            """
            SELECT event_type, actor, message, details, created_at
            FROM incident_events WHERE incident_id=%s ORDER BY created_at
            """,
            (incident_id,),
        ).fetchall()
        try:
            investigation = conn.execute(
                """
                SELECT *, id::text AS id_text, incident_id::text AS incident_id_text
                FROM agent_investigations
                WHERE incident_id=%s
                ORDER BY started_at DESC LIMIT 1
                """,
                (incident_id,),
            ).fetchone()
            if investigation:
                investigation_id = investigation.pop("id_text")
                investigation["id"] = investigation_id
                investigation["incident_id"] = investigation.pop("incident_id_text")
                investigation_events = conn.execute(
                    """
                    SELECT event_type, message, details, created_at
                    FROM agent_events
                    WHERE investigation_id=%s
                    ORDER BY created_at
                    """,
                    (investigation_id,),
                ).fetchall()
        except psycopg.Error:
            # The agent creates these tables during startup. Incident detection must
            # remain available if the agent is disabled or still starting.
            investigation = None
            investigation_events = []
    incident["id"] = incident.pop("id_text")
    operations: dict[str, Any] = {}
    try:
        async with httpx.AsyncClient(timeout=4) as client:
            response = await client.get(f"{OPSAI_AUTOMATION_URL}/api/incidents/{incident_id}/operations")
            if response.status_code < 400:
                operations = response.json()
    except Exception as exc:
        operations = {"status": "unavailable", "error": str(exc)}
    return {
        "incident": incident,
        "events": events,
        "investigation": investigation,
        "investigationEvents": investigation_events,
        "operations": operations,
    }

@app.post("/incidents/{incident_id}/acknowledge")
async def acknowledge_incident(incident_id: str, request: AcknowledgeRequest) -> dict[str, Any]:
    now = utc_now()
    with db_connect() as conn:
        row = conn.execute("""
            UPDATE incidents SET status='ACKNOWLEDGED', acknowledged_at=%s,
                acknowledged_by=%s, updated_at=%s
            WHERE id=%s AND status='OPEN'
            RETURNING id::text, status
        """, (now, request.actor, now, incident_id)).fetchone()
        if not row:
            raise HTTPException(status_code=409, detail="Incident is not open or does not exist")
        conn.execute("""
            INSERT INTO incident_events (incident_id,event_type,actor,message,details,created_at)
            VALUES (%s,'ACKNOWLEDGED',%s,%s,%s,%s)
        """, (incident_id, request.actor, request.note or "Incident acknowledged", json.dumps({}), now))
    emit_log("INFO", "incident_acknowledged", incidentId=incident_id, actor=request.actor)
    return row


@app.post("/incidents/{incident_id}/investigate")
async def rerun_incident_investigation(incident_id: str) -> dict[str, Any]:
    try:
        uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident ID") from exc
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            f"{OPSAI_AGENT_URL}/api/investigations/{incident_id}/rerun"
        )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail="Incident not found")
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"PulseGuard Agent rejected investigation request: {response.text[:500]}",
        )
    return response.json()


@app.get("/traffic/profile")
async def traffic_profile() -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=4) as client:
            response = await client.get(WIKIMEDIA_PROFILE_URL)
        response.raise_for_status()
        payload = response.json()
        payload["proxyStatus"] = "ok"
        return payload
    except Exception as exc:
        return {
            "proxyStatus": "unavailable",
            "sourceMode": "unavailable",
            "streamConnected": False,
            "connectionStatus": "unavailable",
            "profile": "unavailable",
            "targetUsers": None,
            "spawnRate": None,
            "currentEventsPerMinute": None,
            "baselineEventsPerMinute": None,
            "activityRatio": None,
            "lastEventAgeSeconds": None,
            "latestEvent": None,
            "connectionError": f"Could not reach Wikimedia adapter: {str(exc)[:300]}",
            "fallbackReason": "adapter_unavailable",
            "tlsMode": "unknown",
            "tlsVerificationEnabled": None,
            "streamUrl": None,
        }


@app.get("/", response_class=HTMLResponse)
async def incident_console() -> str:
    return r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PulseGuard Incident Console</title>
<style>
:root{color-scheme:dark;--bg:#08111f;--panel:#101b2e;--panel2:#15233a;--border:#2b3c57;--text:#e8eff8;--muted:#9eb0c6;--accent:#4b91ff;--good:#1e6d4f;--warn:#70501d;--bad:#74333d}
*{box-sizing:border-box}body{font-family:Segoe UI,Arial,sans-serif;background:var(--bg);color:var(--text);margin:0;padding:24px}main{max-width:1440px;margin:auto}
h1{margin:0 0 6px}.sub{color:var(--muted);margin-bottom:20px}.cards{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}
.card,.panel,.detail{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:16px}.value{font-size:30px;font-weight:700;margin-top:6px}
.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;min-width:1380px}th,td{text-align:left;padding:10px;border-bottom:1px solid var(--border);vertical-align:top}th{color:var(--muted);font-weight:600}.incident-row.expanded{background:#12233a}.incident-row.expanded>td{border-bottom:0}.investigation-row>td{padding:0 10px 14px;border-bottom:1px solid var(--border)}.investigation-shell{background:#0b1525;border:1px solid #35547d;border-radius:10px;padding:12px;max-height:72vh;overflow:auto}.investigation-shell .detail{margin:0;background:var(--panel);box-shadow:inset 4px 0 0 var(--accent)}.investigation-toggle[aria-expanded="true"]{background:#315f9f}.inline-panel-note{display:flex;justify-content:space-between;gap:10px;align-items:center;margin-bottom:8px;color:var(--muted);font-size:12px}
.badge{display:inline-block;padding:3px 8px;border-radius:999px;background:#26364f;font-size:12px}.critical{background:var(--bad)}.high{background:var(--warn)}.good{background:var(--good)}.resolved-row{opacity:.7}.resolved-row.expanded{opacity:1}
button{background:var(--accent);color:white;border:0;border-radius:7px;padding:7px 10px;cursor:pointer;margin:2px}button.secondary{background:#2a3a54}button:disabled{opacity:.5;cursor:default}.empty{padding:30px;text-align:center;color:var(--muted)}
small,.muted{color:var(--muted)}.detail{margin-top:12px}.detail-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.detail h2,.detail h3{margin:0 0 10px}.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px}.section{background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:13px}.section.full{grid-column:1/-1}.label{text-transform:uppercase;letter-spacing:.06em;color:var(--muted);font-size:12px;margin-bottom:7px}.big{font-size:22px;font-weight:650}.kv{display:grid;grid-template-columns:minmax(130px,190px) 1fr;gap:7px 12px}.kv div:nth-child(odd){color:var(--muted)}
pre{white-space:pre-wrap;word-break:break-word;background:#07101d;border:1px solid var(--border);border-radius:8px;padding:12px;max-height:390px;overflow:auto;color:#dce8f7}details{margin-top:9px}summary{cursor:pointer;color:#b9d4ff}.hyp{padding:9px 0;border-bottom:1px solid var(--border)}.hyp:last-child{border-bottom:0}.action{border-left:4px solid var(--accent)}
.timeline{list-style:none;padding:0;margin:0}.timeline li{position:relative;padding:0 0 14px 24px}.timeline li:before{content:'';position:absolute;left:4px;top:6px;width:9px;height:9px;border-radius:50%;background:var(--accent)}.timeline li:after{content:'';position:absolute;left:8px;top:17px;bottom:0;width:1px;background:var(--border)}.timeline li:last-child:after{display:none}.timeline time{display:block;color:var(--muted);font-size:12px}.route{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.route-node{background:#0b1525;border:1px solid var(--border);padding:8px 10px;border-radius:8px}.arrow{color:var(--muted)}.traffic-panel{margin-bottom:18px}.traffic-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}.traffic-grid{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:10px;margin-top:12px}.traffic-metric{background:#0b1525;border:1px solid var(--border);border-radius:9px;padding:10px}.traffic-metric .metric-value{font-size:20px;font-weight:650;margin-top:4px}.traffic-note{margin-top:10px;padding:10px;border-radius:8px;background:#0b1525;border-left:4px solid var(--warn)}.traffic-note.live{border-left-color:var(--good)}.traffic-error{margin-top:8px;color:#ffc7ce;white-space:pre-wrap;word-break:break-word}
@media(max-width:1100px){.traffic-grid{grid-template-columns:repeat(3,1fr)}}
@media(max-width:800px){body{padding:14px}.cards,.grid,.traffic-grid{grid-template-columns:1fr}.section.full{grid-column:auto}.detail-head{display:block}.kv{grid-template-columns:1fr}.route{display:block}.route-node,.arrow{display:block;margin:5px 0}}
</style>
</head>
<body><main>
<h1>PulseGuard Incident Console</h1>
<div class="sub">PulseGuard detects operational risk, correlates evidence, recommends a governed response, executes only allowlisted actions, verifies recovery and prepares support handoffs when human intervention is required.</div>
<div class="cards">
  <div class="card"><div>Active incidents</div><div class="value" id="active">-</div></div>
  <div class="card"><div>Auto-repaired</div><div class="value" id="autoRepaired">-</div></div>
  <div class="card"><div>Awaiting approval</div><div class="value" id="awaitingApproval">-</div></div>
  <div class="card"><div>Assigned to support</div><div class="value" id="assignedSupport">-</div></div>
  <div class="card"><div>Recent resolved</div><div class="value" id="resolved">-</div></div>
  <div class="card"><div>Detector</div><div class="value" id="health">...</div></div>
  <div class="card"><div>External traffic signal</div><div class="value" id="trafficModeCard">...</div></div>
</div>
<div class="panel traffic-panel">
  <div class="traffic-head"><div><h2 style="margin:0 0 5px">Live traffic signal</h2><div class="muted">Wikimedia EventStreams controls the intensity of local synthetic checkout traffic. Raw event content is not sent to the checkout application or Azure OpenAI; only aggregate traffic indicators may be included in an investigation.</div></div><div><span class="badge" id="trafficStatusBadge">LOADING</span><button class="secondary" id="refreshTraffic">Refresh signal</button></div></div>
  <div class="traffic-grid">
    <div class="traffic-metric"><div class="label">Connection</div><div class="metric-value" id="trafficConnection">-</div></div>
    <div class="traffic-metric"><div class="label">Traffic profile</div><div class="metric-value" id="trafficProfile">-</div></div>
    <div class="traffic-metric"><div class="label">Locust users</div><div class="metric-value" id="trafficUsers">-</div></div>
    <div class="traffic-metric"><div class="label">Spawn rate</div><div class="metric-value" id="trafficSpawn">-</div></div>
    <div class="traffic-metric"><div class="label">Events / minute</div><div class="metric-value" id="trafficEvents">-</div></div>
    <div class="traffic-metric"><div class="label">Activity ratio</div><div class="metric-value" id="trafficRatio">-</div></div>
  </div>
  <div class="traffic-note" id="trafficNote"><b id="trafficHeadline">Checking Wikimedia stream...</b><div class="muted" id="trafficExplanation"></div><div class="traffic-error" id="trafficError"></div></div>
  <div class="kv" style="margin-top:10px"><div>TLS mode</div><div id="trafficTls">-</div><div>Last event age</div><div id="trafficAge">-</div><div>Latest public event</div><div id="trafficLatest">-</div><div>Stream endpoint</div><div id="trafficEndpoint">-</div></div>
  <details><summary>Raw traffic profile</summary><pre id="trafficRaw">{}</pre></details>
</div>
<div class="panel">
  <div class="table-wrap"><table><thead><tr><th>Status</th><th>Severity</th><th>Incident</th><th>Node</th><th>Opened</th><th>Detection evidence</th><th>Assigned To</th><th>Resolution</th><th>Actions</th></tr></thead><tbody id="rows"></tbody></table></div>
  <div id="empty" class="empty" hidden>No incidents yet. Inject latency or timeout and allow the detector to confirm it.</div>
</div>
</main>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtTime=v=>v?new Date(v).toLocaleString():'-';
const pretty=v=>JSON.stringify(v??{},null,2);
let openIncidentId=null;
let incidentListSignature='';
let refreshInProgress=false;
const safeDomId=value=>String(value??'').replace(/[^A-Za-z0-9_-]/g,'_');
function setText(id,value){const el=document.getElementById(id);if(el)el.textContent=value==null||value===''?'-':String(value);}
function incidentRowFor(id){return Array.from(document.querySelectorAll('tr.incident-row')).find(row=>String(row.dataset.incidentId)===String(id))||null;}
function updateInvestigationButtons(){document.querySelectorAll('.investigation-toggle').forEach(button=>{const expanded=String(button.dataset.id)===String(openIncidentId)&&Boolean(document.getElementById(`investigation-row-${safeDomId(button.dataset.id)}`));button.setAttribute('aria-expanded',expanded?'true':'false');button.textContent=expanded?'Collapse investigation':'View investigation';});}
function removeInlineInvestigation(){document.querySelectorAll('tr.investigation-row').forEach(row=>row.remove());document.querySelectorAll('tr.incident-row.expanded').forEach(row=>row.classList.remove('expanded'));updateInvestigationButtons();}
function ensureInlineInvestigation(id){const incidentRow=incidentRowFor(id);if(!incidentRow)return null;const rowId=`investigation-row-${safeDomId(id)}`;let detailRow=document.getElementById(rowId);if(detailRow)return detailRow.querySelector('.investigation-shell');removeInlineInvestigation();detailRow=document.createElement('tr');detailRow.id=rowId;detailRow.className='investigation-row';detailRow.dataset.incidentId=String(id);const cell=document.createElement('td');cell.colSpan=9;const shell=document.createElement('div');shell.className='investigation-shell';shell.id=`investigation-host-${safeDomId(id)}`;cell.appendChild(shell);detailRow.appendChild(cell);incidentRow.after(detailRow);incidentRow.classList.add('expanded');updateInvestigationButtons();return shell;}
function formatNumber(value,digits=2){const n=Number(value);return Number.isFinite(n)?n.toFixed(digits):'-';}
function resolutionDetail(x){
 const action=x.resolution_action||'';const r=x.resolution_execution_result||{};
 if(action==='scale_payment_capacity'){const nodes=(r.targetNodes||[]).join(', ');return `Scaled ${nodes||'peer nodes'} to ${r.capacityUnits??2} bounded capacity units`;}
 if(action==='cleanup_disk_space'){const bytes=Number(r.reclaimedBytes||0);return bytes?`Reclaimed ${(bytes/1048576).toFixed(1)} MiB from allowlisted storage`:'Controlled storage cleanup completed';}
 if(action==='renew_certificate'){const expiry=(r.after||{}).notAfter||r.notAfter;return expiry?`Certificate renewed through ${fmtTime(expiry)}`:'Certificate renewed and binding verified';}
 if(action==='collect_diagnostics')return 'Node and router diagnostics collected';
 if(action==='collect_dependency_diagnostics')return 'Fleet dependency diagnostics collected';
 if(action==='drain_payment_node')return `Traffic drain ${x.resolution_action_executed?'executed':'not executed'}`;
 return '';
}
function assignmentHtml(ticket){
 if(!ticket)return '<span class="muted">Unassigned</span>';
 const queue=ticket.primaryQueue||'Unassigned';
 const owner=ticket.acknowledgedBy||'';
 const status=String(ticket.status||'ASSIGNED').replaceAll('_',' ');
 return `<b>${esc(queue)}</b>${owner?`<br><small>Owner: ${esc(owner)}</small>`:''}<br><small>${esc(status)}</small>`;
}function renderTraffic(p){
 const live=p&&p.sourceMode==='live'&&p.streamConnected===true;const fallback=p&&p.sourceMode==='fallback';
 const mode=live?'LIVE':(fallback?'FALLBACK':'UNAVAILABLE');
 setText('trafficModeCard',mode);setText('trafficStatusBadge',mode);
 const badge=document.getElementById('trafficStatusBadge');if(badge)badge.className=`badge ${live?'good':fallback?'high':'critical'}`;
 setText('trafficConnection',p?.connectionStatus||(p?.streamConnected?'connected':'disconnected'));
 setText('trafficProfile',p?.profile);setText('trafficUsers',p?.targetUsers);setText('trafficSpawn',p?.spawnRate==null?'-':`${p.spawnRate}/s`);
 setText('trafficEvents',formatNumber(p?.currentEventsPerMinute));setText('trafficRatio',p?.activityRatio==null?'-':`${formatNumber(p.activityRatio)}x`);
 setText('trafficTls',p?.tlsMode||'-');setText('trafficAge',p?.lastEventAgeSeconds==null?'No live event':`${formatNumber(p.lastEventAgeSeconds,1)} s`);
 const latest=p?.latestEvent;setText('trafficLatest',latest?`${latest.wiki||'wiki'} | ${latest.type||'event'} | ${latest.title||'-'}`:'No live event received');
 setText('trafficEndpoint',p?.streamUrl||'-');setText('trafficRaw',pretty(p||{}));
 const note=document.getElementById('trafficNote');const headline=document.getElementById('trafficHeadline');const explanation=document.getElementById('trafficExplanation');const error=document.getElementById('trafficError');
 if(note)note.className=`traffic-note ${live?'live':''}`;
 if(live){headline.textContent='Real Wikimedia activity is driving the synthetic load profile.';explanation.textContent=`Locust is currently targeting ${p.targetUsers} users at ${p.spawnRate} users per second. Current public activity is ${formatNumber(p.currentEventsPerMinute)} events/minute.`;error.textContent='';}
 else if(fallback){headline.textContent='Safe fallback traffic is active; current Locust traffic is not being driven by Wikimedia.';explanation.textContent=`The load generator is using the configured fallback of ${p.targetUsers??'-'} users until the external stream becomes available.`;error.textContent=p.connectionError||'The stream is disconnected or no recent event has been received.';}
 else{headline.textContent='Traffic adapter status is unavailable.';explanation.textContent='The Incident Console could not retrieve the Wikimedia traffic profile.';error.textContent=p?.connectionError||'';}
}
async function refreshTraffic(){try{const response=await fetch('/traffic/profile');renderTraffic(await response.json());}catch(error){renderTraffic({sourceMode:'unavailable',connectionError:String(error)});}}
async function acknowledge(id){await fetch(`/incidents/${id}/acknowledge`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({actor:'demo-operator',note:'Acknowledged from incident console'})});await refresh();if(openIncidentId===id)await showDetail(id,true);}
function knowledgeList(items){return (items||[]).map(d=>`<li><b>${esc(d.title||d.id)}</b> <span class="muted">${esc(d.kind||'')} | retrieval score ${Number(d.retrievalScore||0).toFixed(0)}</span></li>`).join('')||'<li class="muted">No knowledge record was stored.</li>';}
function hypothesisList(items){return (items||[]).map(h=>`<div class="hyp"><b>${esc(h.name)}</b> <span class="badge">${esc(h.likelihood)}</span><div class="muted">${esc(h.support)}</div></div>`).join('')||'<div class="muted">No hypotheses recorded.</div>';}
function timelineHtml(incidentEvents,agentEvents){
 const all=[];
 for(const e of incidentEvents||[])all.push({source:'Incident',type:e.event_type,message:e.message,details:e.details,created_at:e.created_at,actor:e.actor});
 for(const e of agentEvents||[])all.push({source:'Agent',type:e.event_type,message:e.message,details:e.details,created_at:e.created_at,actor:'opsai-agent'});
 all.sort((a,b)=>new Date(a.created_at)-new Date(b.created_at));
 return all.map(e=>`<li><time>${fmtTime(e.created_at)}</time><b>${esc(e.source)} | ${esc(e.type)}</b> <span class="muted">${esc(e.actor||'')}</span><div>${esc(e.message)}</div>${Object.keys(e.details||{}).length?`<details><summary>Event details</summary><pre>${esc(pretty(e.details))}</pre></details>`:''}</li>`).join('')||'<li>No events recorded.</li>';
}
function supportHandoffHtml(operations){
 const handoff=operations.supportHandoff;
 if(!handoff)return '<div class="muted">No support handoff is currently required.</div>';
 const h=handoff.handoff||{};
 return `<div class="kv"><div>Primary queue</div><div><b>${esc(handoff.primaryQueue||'-')}</b></div><div>Secondary queue</div><div>${esc(handoff.secondaryQueue||'-')}</div><div>Assignment</div><div>${esc(handoff.assignmentMode||'-')}</div><div>Triage confidence</div><div>${Math.round(Number(handoff.assignmentConfidence||0)*100)}%</div><div>Status</div><div>${esc(handoff.status||'-')}</div><div>Ticket</div><div>${esc(handoff.ticketId||'-')}</div></div><p>${esc(handoff.routingReason||'')}</p><div style="display:flex;gap:8px;flex-wrap:wrap;margin:10px 0"><button class="support-ack" data-incident="${esc(handoff.incidentId||'')}">Accept assignment</button>${h.governanceDecision==='APPROVAL_REQUIRED'?`<button class="support-approve" data-incident="${esc(handoff.incidentId||'')}">Approve recommended action</button><button class="secondary support-reject" data-incident="${esc(handoff.incidentId||'')}">Reject</button>`:''}<button class="secondary support-reassign" data-incident="${esc(handoff.incidentId||'')}" data-queue="${esc(handoff.primaryQueue||'Operations Triage')}">Reassign</button><button class="secondary support-escalate" data-incident="${esc(handoff.incidentId||'')}">Escalate to L3</button></div><details open><summary>Detailed operational handoff</summary><pre>${esc(pretty(h))}</pre></details>`;
}
async function supportTicketAction(id,action,queue){
 const body={actor:'demo-operator',note:`${action} from Incident Console`};if(queue)body.queue=queue;
 const response=await fetch(`http://localhost:8097/tickets/${id}/${action}`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
 if(!response.ok)alert('Support action failed: '+await response.text());else await showDetail(id,true);
}
async function showDetail(id,preservePosition=false){
 const pageScrollY=window.scrollY;
 if(String(openIncidentId)!==String(id))removeInlineInvestigation();
 openIncidentId=String(id);const host=ensureInlineInvestigation(id);
 if(!host)return;
 if(!preservePosition)host.innerHTML='<div class="inline-panel-note"><span>Investigation details for the selected incident</span><span>Use Collapse investigation to close this panel.</span></div><div class="detail">Loading incident and AI investigation...</div>';
 const d=await fetch(`/incidents/${id}`).then(r=>r.json());const x=d.incident||{};const i=d.investigation;const ev=x.evidence||{};
 if(!i){host.innerHTML=`<div class="inline-panel-note"><span>Inline investigation panel</span><span>Incident list refresh continues independently.</span></div><div class="detail"><div class="detail-head"><div><h2>${esc(x.title)}</h2><div class="muted">Incident ${esc(x.status)} | ${esc(x.node||'global')}</div></div><div><button class="secondary" id="refreshInvestigation">Refresh investigation</button><button class="secondary" id="closeDetail">Collapse</button></div></div><div class="section" style="margin-top:12px"><div class="label">AI investigation</div><div class="big">Waiting for PulseGuard Investigation</div><p class="muted">The deterministic incident exists, but no agent investigation record is available yet. This panel stays directly below the incident row and refreshes only when requested.</p></div></div></div>`;document.getElementById('closeDetail').onclick=closeDetail;document.getElementById('refreshInvestigation').onclick=()=>showDetail(id,true);if(preservePosition)requestAnimationFrame(()=>window.scrollTo(0,pageScrollY));else requestAnimationFrame(()=>host.scrollIntoView({behavior:'smooth',block:'nearest'}));return;}
 const response=(i.response_payload||{}).analysis||{};const request=i.request_payload||{};const duration=i.llm_duration_ms==null?'-':`${i.llm_duration_ms} ms`;const real=i.analysis_mode==='REAL_AI';
 host.innerHTML=`<div class="inline-panel-note"><span>Inline investigation panel</span><span>Opened directly below this incident row.</span></div><div class="detail">
 <div class="detail-head"><div><h2>${esc(x.title)}</h2><div class="muted">${esc(x.incident_type)} | ${esc(x.node||'global')} | incident ${esc(x.status)}</div></div><div><span class="badge ${real?'good':''}">${esc(i.analysis_mode)}</span> <span class="badge">Investigation ${esc(i.status)}</span> <button class="secondary" id="refreshInvestigation">Refresh investigation</button> <button id="rerunInvestigation">Re-run AI investigation</button> <button class="secondary" id="closeDetail">Collapse</button></div></div>
 <div class="muted" style="margin-top:8px">The incident list continues to update every 5 seconds. This panel remains attached to the selected row while you review the investigation.</div>
 <div class="grid">
  <div class="section full"><div class="label">Where the investigation was sent</div><div class="route"><div class="route-node">Incident detector<br><small>opsai-core</small></div><div class="arrow">-></div><div class="route-node">Investigation specialist<br><small>opsai-agent</small></div><div class="arrow">-></div><div class="route-node">${esc(i.provider)}<br><small>${esc(i.provider_endpoint_host||'local')}</small></div><div class="arrow">-></div><div class="route-node">${esc(i.model||'-')}</div></div><div class="kv" style="margin-top:12px"><div>Request sent</div><div>${fmtTime(i.request_sent_at)}</div><div>Response received</div><div>${fmtTime(i.response_received_at)}</div><div>Model duration</div><div>${duration}</div><div>Provider mode</div><div>${esc(i.analysis_mode)}</div></div></div>
  <div class="section"><div class="label">Detection</div><div class="kv"><div>Metric</div><div>${esc(ev.metric||'-')}</div><div>Observed</div><div>${esc(ev.observed)}</div><div>Opening threshold</div><div>${esc(ev.threshold)}</div><div>Consecutive checks</div><div>${esc(ev.consecutiveEvaluations||ev.consecutive_evaluations||'-')}</div></div></div>
  <div class="section"><div class="label">PulseGuard assessment</div><div class="big">${esc(i.summary)}</div><p>${esc(i.root_cause)}</p><div><span class="badge">Confidence ${Math.round(Number(i.confidence||0)*100)}%</span></div></div>
  <div class="section"><div class="label">Evidence sent to AI</div><div class="kv"><div>Affected node p95</div><div>${esc((i.evidence||{}).affectedNodeP95Seconds)} s</div><div>Peer average p95</div><div>${esc((i.evidence||{}).peerAverageP95Seconds)} s</div><div>Checkout p95</div><div>${esc((i.evidence||{}).checkoutP95Seconds)} s</div><div>Failure rate</div><div>${esc((i.evidence||{}).checkoutFailurePercent)}%</div><div>Throughput</div><div>${esc((i.evidence||{}).checkoutRequestsPerSecond)} req/s</div><div>Retry evidence</div><div>${esc(pretty((i.evidence||{}).retryRateByFailedNode))}</div></div><details><summary>Raw evidence JSON</summary><pre>${esc(pretty(i.evidence))}</pre></details></div>
  <div class="section"><div class="label">Retrieved knowledge sent to AI</div><ul>${knowledgeList(i.retrieved_knowledge)}</ul><details><summary>Raw retrieved knowledge</summary><pre>${esc(pretty(i.retrieved_knowledge))}</pre></details></div>
  <div class="section full"><div class="label">PulseGuard response</div><div class="grid"><div><h3>Likely cause</h3><p>${esc(i.root_cause)}</p><h3>Customer impact</h3><p>${esc(i.customer_impact)}</p></div><div><h3>Hypotheses</h3>${hypothesisList(i.hypotheses)}</div></div></div>
  <div class="section action"><div class="label">PulseGuard recommendation</div><div class="big">${esc(i.action_name)}</div><p>${esc(i.recommended_action)}</p><pre>${esc(pretty(i.action_parameters))}</pre></div>
  <div class="section"><div class="label">Governance decision</div><div class="big">${esc(i.policy_decision)}</div><p>${esc(i.policy_reason)}</p><div class="muted">PulseGuard analyses the available evidence and recommends an operational response. Its deterministic governance layer decides whether the action may run automatically, requires operator approval, or must be blocked.</div></div>
  <div class="section full"><div class="label">Action taken</div><div class="kv"><div>Execution status</div><div><b>${esc(i.action_execution_status||'NOT_EVALUATED')}</b></div><div>Operational action executed</div><div>${i.action_executed?'Yes':'No'}</div><div>Executor</div><div>${esc(i.action_executor||'-')}</div><div>Started</div><div>${fmtTime(i.action_started_at)}</div><div>Completed</div><div>${fmtTime(i.action_completed_at)}</div><div>Repair outcome</div><div><b>${esc((d.operations||{}).repairOutcome||(ev||{}).repairOutcome||(i.action_executed?'AUTO_ACTION_COMPLETED':(i.policy_decision==='APPROVAL_REQUIRED'?'WAITING_FOR_APPROVAL':'RECOMMENDED_ONLY')))}</b></div></div><p>${i.action_executed?'The allowlisted action was executed and its result is shown below. Recovery is counted as auto-repaired only after independent verification.':'No operational action was executed. The recommendation remains governed by the decision above.'}</p><details open><summary>Execution result</summary><pre>${esc(pretty(i.action_execution_result||{}))}</pre></details></div>
  <div class="section full"><div class="label">Support handoff</div>${supportHandoffHtml(d.operations||{})}</div>
  <div class="section full"><div class="label">Prompt and request transparency</div><details open><summary>Readable prompt preview</summary><pre>${esc(i.prompt_preview||'No prompt stored.')}</pre></details><details><summary>Sanitized request JSON</summary><pre>${esc(pretty(request))}</pre></details><details><summary>Parsed AI response JSON</summary><pre>${esc(pretty(i.response_payload||response))}</pre></details><details><summary>Token usage</summary><pre>${esc(pretty(i.token_usage))}</pre></details></div>
  <div class="section full"><div class="label">Incident and investigation timeline</div><ul class="timeline">${timelineHtml(d.events,d.investigationEvents)}</ul></div>
 </div></div>`;
 document.getElementById('closeDetail').onclick=closeDetail;const refreshButton=document.getElementById('refreshInvestigation');if(refreshButton)refreshButton.onclick=()=>showDetail(id,true);const rerunButton=document.getElementById('rerunInvestigation');if(rerunButton)rerunButton.onclick=()=>rerunInvestigation(id);document.querySelectorAll('.support-ack').forEach(b=>b.onclick=()=>supportTicketAction(b.dataset.incident,'acknowledge'));document.querySelectorAll('.support-approve').forEach(b=>b.onclick=()=>{if(confirm('Approve and execute the governed operational action?'))supportTicketAction(b.dataset.incident,'approve')});document.querySelectorAll('.support-reject').forEach(b=>b.onclick=()=>supportTicketAction(b.dataset.incident,'reject'));document.querySelectorAll('.support-escalate').forEach(b=>b.onclick=()=>supportTicketAction(b.dataset.incident,'escalate'));document.querySelectorAll('.support-reassign').forEach(b=>b.onclick=()=>{const q=prompt('Reassign to queue',b.dataset.queue||'Operations Triage');if(q)supportTicketAction(b.dataset.incident,'reassign',q)});updateInvestigationButtons();if(preservePosition)requestAnimationFrame(()=>window.scrollTo(0,pageScrollY));else requestAnimationFrame(()=>host.scrollIntoView({behavior:'smooth',block:'nearest'}));
}
async function rerunInvestigation(id){
 const button=document.getElementById('rerunInvestigation');if(button){button.disabled=true;button.textContent='Investigation requested...';}
 const response=await fetch(`/incidents/${id}/investigate`,{method:'POST'});
 if(!response.ok){const text=await response.text();alert('Could not request investigation: '+text);if(button){button.disabled=false;button.textContent='Re-run AI investigation';}return;}
 setTimeout(()=>showDetail(id,true),1500);
}
function closeDetail(){openIncidentId=null;removeInlineInvestigation();}
async function toggleInvestigation(id){if(String(openIncidentId)===String(id)&&document.getElementById(`investigation-row-${safeDomId(id)}`)){closeDetail();return;}await showDetail(id,false);}
async function refresh(){
 if(refreshInProgress)return;refreshInProgress=true;
 try{
  const [all,health,traffic,automation,ticketPayload]=await Promise.all([fetch('/incidents?status=all&limit=50').then(r=>r.json()),fetch('/health').then(r=>r.json()),fetch('/traffic/profile').then(r=>r.json()),fetch('http://localhost:8097/summary').then(r=>r.json()).catch(()=>({})),fetch('http://localhost:8097/tickets').then(r=>r.json()).catch(()=>({tickets:[]}))]);
  const active=all.incidents.filter(x=>x.status==='OPEN'||x.status==='ACKNOWLEDGED');const resolved=all.incidents.filter(x=>x.status==='RESOLVED');const ticketMap=Object.fromEntries((ticketPayload.tickets||[]).map(t=>[String(t.incidentId),t]));
  document.getElementById('active').textContent=active.length;document.getElementById('autoRepaired').textContent=automation.autoRepaired??0;document.getElementById('awaitingApproval').textContent=automation.awaitingApproval??0;document.getElementById('assignedSupport').textContent=automation.assignedToSupport??0;document.getElementById('resolved').textContent=resolved.length;document.getElementById('health').textContent=health.status;renderTraffic(traffic);
  const nextSignature=JSON.stringify(all.incidents.map(x=>{const t=ticketMap[String(x.id)]||{};return [x.id,x.status,x.updated_at,(x.evidence||{}).observed,(x.evidence||{}).threshold,x.resolution_action,x.resolution_execution_status,x.repair_outcome,x.resolution_reason,t.primaryQueue,t.status,t.acknowledgedBy,t.updatedAt];}));
  document.getElementById('empty').hidden=all.incidents.length>0;
  if(nextSignature!==incidentListSignature){
   const body=document.getElementById('rows');body.innerHTML='';
   for(const x of all.incidents){const tr=document.createElement('tr');tr.className='incident-row';tr.dataset.incidentId=String(x.id);if(x.status==='RESOLVED')tr.classList.add('resolved-row');const ev=x.evidence||{};
   const action=x.resolution_action||((ev.remediation||{}).action)||'-';
   const execution=x.resolution_execution_status||'NOT_EVALUATED';
   const outcome=x.repair_outcome||(x.status==='RESOLVED'?'RECOVERY_VERIFIED':(x.resolution_policy==='APPROVAL_REQUIRED'?'WAITING_FOR_APPROVAL':'IN_PROGRESS'));
   const actionLine=action==='-'?'No operational action recorded':`${action} | ${execution}`;
   const reason=x.resolution_reason||((x.resolution_execution_result||{}).reason)||'';
   const detail=resolutionDetail(x);
   const resolution=`<b>${esc(outcome)}</b><br><small>${esc(actionLine)}</small>${detail?`<br><small>${esc(detail)}</small>`:''}${reason?`<br><small title="${esc(reason)}">${esc(String(reason).slice(0,140))}${String(reason).length>140?'...':''}</small>`:''}`;
   const expanded=String(openIncidentId)===String(x.id);tr.innerHTML=`<td><span class="badge">${esc(x.status)}</span></td><td><span class="badge ${esc(x.severity)}">${esc(x.severity)}</span></td><td><b>${esc(x.title)}</b><br><small>${esc(x.summary)}</small></td><td>${esc(x.node||'-')}</td><td>${fmtTime(x.opened_at)}</td><td>${esc(ev.metric||'')}<br><small>${esc(ev.observed)} / threshold ${esc(ev.threshold)}</small></td><td>${assignmentHtml(ticketMap[String(x.id)]||null)}</td><td>${resolution}</td><td>${x.status==='OPEN'?`<button class="ack" data-id="${esc(x.id)}">Acknowledge</button>`:''}<button class="secondary view investigation-toggle" data-id="${esc(x.id)}" aria-expanded="${expanded?'true':'false'}" aria-controls="investigation-row-${safeDomId(x.id)}">${expanded?'Collapse investigation':'View investigation'}</button></td>`;body.appendChild(tr);}
   body.querySelectorAll('.ack').forEach(b=>b.addEventListener('click',()=>acknowledge(b.dataset.id)));body.querySelectorAll('.view').forEach(b=>b.addEventListener('click',()=>toggleInvestigation(b.dataset.id)));
   incidentListSignature=nextSignature;
   if(openIncidentId&&incidentRowFor(openIncidentId)){showDetail(openIncidentId,true);}else if(openIncidentId){closeDetail();}
  }
 }finally{refreshInProgress=false;}
}
document.getElementById('refreshTraffic').onclick=refreshTraffic;
refresh();setInterval(refresh,5000);
</script>
<script src="http://localhost:8097/widget.js"></script>
</body></html>"""
