from __future__ import annotations

import asyncio
import json
import os
import shutil
import ssl
import time
import threading
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, StreamingResponse
from prometheus_client import Counter, Gauge, make_asgi_app
from pydantic import BaseModel, Field


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
OPSAI_CORE_URL = os.getenv("OPSAI_CORE_URL", "http://opsai-core:8000").rstrip("/")
OPSAI_AGENT_URL = os.getenv("OPSAI_AGENT_URL", "http://opsai-agent:8000").rstrip("/")
SCENARIO_CONTROLLER_URL = os.getenv("SCENARIO_CONTROLLER_URL", "http://scenario-controller:8000").rstrip("/")
PAYMENT_ROUTER_URL = os.getenv("PAYMENT_ROUTER_URL", "http://payment-router:8000").rstrip("/")
CHECKOUT_SERVICE_URL = os.getenv("CHECKOUT_SERVICE_URL", "http://checkout-service:8000").rstrip("/")
EXTERNAL_AUTH_SERVICE_URL = os.getenv(
    "EXTERNAL_AUTH_SERVICE_URL",
    "http://external-auth-service:8000",
).rstrip("/")
PAYMENT_NODE_URLS_RAW = os.getenv(
    "PAYMENT_NODE_URLS",
    "payment-node-1=http://payment-node-1:8000,payment-node-2=http://payment-node-2:8000,payment-node-3=http://payment-node-3:8000",
)
INTERNAL_TOKEN = require_env("AUTOMATION_API_TOKEN")
DATA_ROOT = Path(os.getenv("AUTOMATION_DATA_ROOT", "/data"))
STATE_FILE = DATA_ROOT / "automation-state.json"
STORAGE_ROOT = DATA_ROOT / "storage"
CERT_ROOT = DATA_ROOT / "certificates"
VIRTUAL_DISK_CAPACITY_BYTES = int(os.getenv("VIRTUAL_DISK_CAPACITY_BYTES", str(160 * 1024 * 1024)))
DISK_OPEN_PERCENT = float(os.getenv("DISK_OPEN_PERCENT", "85"))
DISK_RECOVERY_PERCENT = float(os.getenv("DISK_RECOVERY_PERCENT", "70"))
CERT_OPEN_SECONDS = int(os.getenv("CERT_OPEN_SECONDS", "900"))
MONITOR_INTERVAL_SECONDS = float(os.getenv("AUTOMATION_MONITOR_SECONDS", "3"))
DEMO_CERT_HOSTNAME = os.getenv("DEMO_CERT_HOSTNAME", "checkout.pulseguard.local")
MAX_ACTIVITY = int(os.getenv("MAX_ACTIVITY_EVENTS", "1500"))
SLA_RISK_SECONDS = int(os.getenv("SLA_RISK_SECONDS", "300"))
SUPPORT_HANDOFF_DELAY_SECONDS = int(os.getenv("SUPPORT_HANDOFF_DELAY_SECONDS", "180"))

ACTIVITY_COUNTER = Counter(
    "opsai_activity_events_total",
    "PulseGuard Live Activity events by stage and severity.",
    ["stage", "severity"],
)
TICKET_COUNTER = Counter(
    "opsai_support_assignments_total",
    "Support assignments by queue.",
    ["queue"],
)
REMEDIATION_COUNTER = Counter(
    "opsai_automatic_remediations_total",
    "Automatic remediation executions by action and result.",
    ["action", "result"],
)
DISK_USAGE = Gauge(
    "opsai_demo_disk_usage_percent",
    "Usage percentage of the bounded synthetic PulseGuard storage volume.",
)
DISK_USED = Gauge(
    "opsai_demo_disk_used_bytes",
    "Synthetic bytes stored in the bounded PulseGuard storage volume.",
)
CERT_EXPIRY = Gauge(
    "opsai_demo_certificate_expiry_seconds",
    "Seconds until the active demo certificate expires.",
    ["hostname"],
)
AUTO_REPAIRED = Gauge(
    "opsai_auto_repaired_incidents",
    "Number of incidents verified as automatically repaired.",
)

app = FastAPI(title="PulseGuard Automation, Activity and Support", version=SERVICE_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8090",
        "http://localhost:8095",
        "http://localhost:8096",
    ],
    allow_origin_regex=r"https://.*\.app\.github\.dev",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())

stop_event = asyncio.Event()
activity_condition = asyncio.Condition()
state_lock = asyncio.Lock()
state_file_lock = threading.RLock()
predictive_disk_task: asyncio.Task[None] | None = None


def parse_named_urls(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in raw.split(","):
        if "=" not in item:
            continue
        name, url = item.split("=", 1)
        name = name.strip()
        url = url.strip().rstrip("/")
        if name and url:
            result[name] = url
    return result


PAYMENT_NODE_URLS = parse_named_urls(PAYMENT_NODE_URLS_RAW)

state: dict[str, Any] = {
    "activities": [],
    "tickets": {},
    "remediations": {},
    "seenIncidents": {},
    "seenInvestigations": {},
    "diskIncidentId": None,
    "certificateIncidentId": None,
    "diskRecoveryChecks": 0,
    "certificateRecoveryChecks": 0,
    "certificateRenewalFailure": False,
    "lastScenarioResetAt": None,
    "diskIncidentOpenedAt": None,
    "certificateIncidentOpenedAt": None,
    "lastPersistedAt": None,
}


class ActionRequest(BaseModel):
    incidentId: str = Field(min_length=1, max_length=100)
    requestedBy: str = Field(default="opsai-agent", min_length=1, max_length=100)
    parameters: dict[str, Any] = Field(default_factory=dict)


class TicketUpdateRequest(BaseModel):
    actor: str = Field(default="operator", min_length=1, max_length=100)
    queue: str | None = Field(default=None, max_length=100)
    note: str = Field(default="", max_length=1000)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def emit_log(level: str, event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": iso_now(),
                "level": level,
                "service": "opsai-automation",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
            default=str,
        ),
        flush=True,
    )


def require_token(value: str | None) -> None:
    if value != INTERNAL_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid internal automation token")


def ensure_directories() -> None:
    for path in (
        DATA_ROOT,
        STORAGE_ROOT / "temp",
        STORAGE_ROOT / "cache",
        STORAGE_ROOT / "logs" / "archive",
        STORAGE_ROOT / "logs" / "current",
        CERT_ROOT,
    ):
        path.mkdir(parents=True, exist_ok=True)


def normalise_state_shape() -> dict[str, Any]:
    """Migrate persisted v0.5 state defensively without deleting valid history."""
    repaired: dict[str, Any] = {}

    activities = state.get("activities")
    if not isinstance(activities, list):
        repaired["activities"] = type(activities).__name__
        activities = []
    state["activities"] = [item for item in activities if isinstance(item, dict)][-MAX_ACTIVITY:]

    for key in ("tickets", "remediations", "seenIncidents", "seenInvestigations"):
        value = state.get(key)
        if not isinstance(value, dict):
            repaired[key] = type(value).__name__
            value = {}
        state[key] = {
            str(item_key): item_value
            for item_key, item_value in value.items()
            if isinstance(item_value, dict)
        }

    for key in ("diskRecoveryChecks", "certificateRecoveryChecks"):
        try:
            state[key] = int(state.get(key) or 0)
        except (TypeError, ValueError):
            repaired[key] = type(state.get(key)).__name__
            state[key] = 0

    state["certificateRenewalFailure"] = bool(state.get("certificateRenewalFailure", False))
    for key in ("lastScenarioResetAt", "diskIncidentOpenedAt", "certificateIncidentOpenedAt"):
        value = state.get(key)
        if value is not None and not isinstance(value, str):
            repaired[key] = type(value).__name__
            state[key] = None
    return repaired


def load_state() -> None:
    global state
    ensure_directories()
    if not STATE_FILE.exists():
        normalise_state_shape()
        return
    try:
        payload = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            for key in state:
                if key in payload:
                    state[key] = payload[key]
        repaired = normalise_state_shape()
        if repaired:
            emit_log("WARNING", "state_shape_migrated", repaired=repaired)
            persist_state_sync()
    except Exception as exc:
        emit_log("WARNING", "state_load_failed", error=str(exc))
        normalise_state_shape()


def persist_state_sync() -> None:
    # Multiple monitor/action threads can persist concurrently. Serialise writes and
    # use a unique temporary file so one writer can never replace another writer's file.
    with state_file_lock:
        state["lastPersistedAt"] = iso_now()
        payload = json.dumps(state, indent=2, default=str)
        temporary = STATE_FILE.with_name(f"{STATE_FILE.name}.{uuid.uuid4().hex}.tmp")
        try:
            temporary.write_text(payload, encoding="utf-8")
            os.replace(temporary, STATE_FILE)
        finally:
            temporary.unlink(missing_ok=True)


async def persist_state() -> None:
    await asyncio.to_thread(persist_state_sync)


def append_activity_sync(
    stage: str,
    severity: str,
    title: str,
    message: str,
    incident_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "eventId": str(uuid.uuid4()),
        "timestamp": iso_now(),
        "incidentId": incident_id,
        "stage": stage,
        "severity": severity,
        "title": title,
        "message": message,
        "details": details or {},
    }
    activities = state.setdefault("activities", [])
    activities.append(event)
    del activities[:-MAX_ACTIVITY]
    ACTIVITY_COUNTER.labels(stage=stage, severity=severity).inc()
    persist_state_sync()
    emit_log("INFO", "activity_event", **event)
    return event


async def append_activity(
    stage: str,
    severity: str,
    title: str,
    message: str,
    incident_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with state_lock:
        event = await asyncio.to_thread(
            append_activity_sync,
            stage,
            severity,
            title,
            message,
            incident_id,
            details,
        )
    async with activity_condition:
        activity_condition.notify_all()
    return event


def iter_storage_files() -> list[Path]:
    ensure_directories()
    return [path for path in STORAGE_ROOT.rglob("*") if path.is_file()]


def storage_used_bytes() -> int:
    return sum(path.stat().st_size for path in iter_storage_files())


def disk_snapshot() -> dict[str, Any]:
    used = storage_used_bytes()
    percent = round(100 * used / max(1, VIRTUAL_DISK_CAPACITY_BYTES), 2)
    by_area: dict[str, int] = {"temp": 0, "cache": 0, "logsArchive": 0, "logsCurrent": 0}
    for path in iter_storage_files():
        relative = path.relative_to(STORAGE_ROOT).as_posix()
        size = path.stat().st_size
        if relative.startswith("temp/"):
            by_area["temp"] += size
        elif relative.startswith("cache/"):
            by_area["cache"] += size
        elif relative.startswith("logs/archive/"):
            by_area["logsArchive"] += size
        elif relative.startswith("logs/current/"):
            by_area["logsCurrent"] += size
    DISK_USAGE.set(percent)
    DISK_USED.set(used)
    return {
        "capacityBytes": VIRTUAL_DISK_CAPACITY_BYTES,
        "usedBytes": used,
        "freeBytes": max(0, VIRTUAL_DISK_CAPACITY_BYTES - used),
        "usagePercent": percent,
        "breakdownBytes": by_area,
        "openThresholdPercent": DISK_OPEN_PERCENT,
        "recoveryThresholdPercent": DISK_RECOVERY_PERCENT,
        "allowlistedCleanupPaths": [
            "/data/storage/temp",
            "/data/storage/cache",
            "/data/storage/logs/archive",
        ],
    }


def write_sparse_file(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.truncate(max(0, size))


def reset_storage_sync() -> dict[str, Any]:
    ensure_directories()
    for folder in (
        STORAGE_ROOT / "temp",
        STORAGE_ROOT / "cache",
        STORAGE_ROOT / "logs" / "archive",
    ):
        for path in folder.glob("*"):
            if path.is_file() or path.is_symlink():
                path.unlink(missing_ok=True)
            elif path.is_dir():
                shutil.rmtree(path)

    # The deliberately protected file belongs only to the
    # cleanup-insufficient scenario. Remove it during scenario reset so it
    # cannot contaminate the next normal disk-repair test.
    current_folder = STORAGE_ROOT / "logs" / "current"
    for path in current_folder.glob("*"):
        if path.name == "application-current.log":
            continue
        if path.is_file() or path.is_symlink():
            path.unlink(missing_ok=True)
        elif path.is_dir():
            shutil.rmtree(path)

    current = current_folder / "application-current.log"
    write_sparse_file(current, 2 * 1024 * 1024)
    return disk_snapshot()


def fill_storage_sync(target_percent: float, cleanup_insufficient: bool = False) -> dict[str, Any]:
    reset_storage_sync()
    target_percent = max(5.0, min(95.0, target_percent))
    target_bytes = int(VIRTUAL_DISK_CAPACITY_BYTES * target_percent / 100)
    baseline = storage_used_bytes()
    required = max(0, target_bytes - baseline)
    if cleanup_insufficient:
        # Put most bytes in current logs, which governance deliberately excludes from deletion.
        protected = int(required * 0.78)
        write_sparse_file(STORAGE_ROOT / "logs" / "current" / "protected-active.log", protected)
        required -= protected
    allocations = [
        (STORAGE_ROOT / "temp" / "synthetic-session.tmp", int(required * 0.45)),
        (STORAGE_ROOT / "cache" / "synthetic-cache.bin", int(required * 0.35)),
        (STORAGE_ROOT / "logs" / "archive" / "old-application-logs.synthetic", required - int(required * 0.45) - int(required * 0.35)),
    ]
    for path, size in allocations:
        write_sparse_file(path, size)
    snapshot = disk_snapshot()
    snapshot["cleanupInsufficientScenario"] = cleanup_insufficient
    return snapshot


async def run_predictive_disk_growth(
    start_percent: float,
    end_percent: float,
    duration_seconds: int,
) -> None:
    global predictive_disk_task
    try:
        await asyncio.to_thread(reset_storage_sync)
        steps = max(8, int(duration_seconds / max(2.0, MONITOR_INTERVAL_SECONDS)))
        for index in range(1, steps + 1):
            target = start_percent + (end_percent - start_percent) * index / steps
            await asyncio.to_thread(fill_storage_sync, target, False)
            await append_activity(
                "PREDICTIVE_SIGNAL_UPDATED",
                "info",
                "Synthetic disk growth progressed",
                f"Bounded storage reached {target:.1f}% during the predictive trend scenario.",
                details={
                    "predictionScenario": "disk_growth",
                    "step": index,
                    "steps": steps,
                    "targetPercent": round(target, 2),
                },
            )
            await asyncio.sleep(duration_seconds / steps)
    except asyncio.CancelledError:
        raise
    finally:
        if predictive_disk_task is asyncio.current_task():
            predictive_disk_task = None


def cleanup_storage_sync(incident_id: str) -> dict[str, Any]:
    before = disk_snapshot()
    candidates: list[dict[str, Any]] = []
    allowlisted_roots = [
        STORAGE_ROOT / "temp",
        STORAGE_ROOT / "cache",
        STORAGE_ROOT / "logs" / "archive",
    ]
    for root in allowlisted_roots:
        for path in root.rglob("*"):
            if path.is_file():
                candidates.append(
                    {
                        "path": path.relative_to(STORAGE_ROOT).as_posix(),
                        "sizeBytes": path.stat().st_size,
                    }
                )
    archive_path = STORAGE_ROOT / "logs" / "archive" / f"cleanup-manifest-{int(time.time())}.zip"
    manifest = {
        "incidentId": incident_id,
        "createdAt": iso_now(),
        "files": candidates,
        "archiveBeforeClear": True,
    }
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr("manifest.json", json.dumps(manifest, indent=2))
    deleted: list[dict[str, Any]] = []
    for item in candidates:
        path = STORAGE_ROOT / item["path"]
        if path.resolve() == archive_path.resolve():
            continue
        path.unlink(missing_ok=True)
        deleted.append(item)
    after = disk_snapshot()
    reclaimed = max(0, before["usedBytes"] - after["usedBytes"])
    verified = after["usagePercent"] < DISK_RECOVERY_PERCENT
    return {
        "action": "cleanup_disk_space",
        "incidentId": incident_id,
        "executor": "opsai-automation:allowlisted-storage-cleaner",
        "archiveBeforeClear": True,
        "archivePath": archive_path.relative_to(STORAGE_ROOT).as_posix(),
        "candidateCount": len(candidates),
        "deletedFiles": deleted,
        "reclaimedBytes": reclaimed,
        "before": before,
        "after": after,
        "verificationPassed": verified,
        "protectedPathsUntouched": [
            "/data/storage/logs/current",
            "application binaries",
            "configuration",
            "certificates",
            "database files",
        ],
    }


def load_or_create_ca() -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key_path = CERT_ROOT / "demo-ca-key.pem"
    cert_path = CERT_ROOT / "demo-ca-cert.pem"
    if key_path.exists() and cert_path.exists():
        key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
        cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
        return key, cert
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "PulseGuard Demo Internal CA")])
    now = utc_now()
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    return key, cert


def issue_certificate_sync(valid_seconds: int, purpose: str) -> dict[str, Any]:
    ensure_directories()
    ca_key, ca_cert = load_or_create_ca()
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = utc_now()
    valid_seconds = max(60, valid_seconds)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, DEMO_CERT_HOSTNAME)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(seconds=5))
        .not_valid_after(now + timedelta(seconds=valid_seconds))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(DEMO_CERT_HOSTNAME)]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    suffix = f"{purpose}-{int(time.time())}"
    key_temp = CERT_ROOT / f"{suffix}.key.pem"
    cert_temp = CERT_ROOT / f"{suffix}.cert.pem"
    key_temp.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    cert_temp.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    active_key = CERT_ROOT / "active.key.pem"
    active_cert = CERT_ROOT / "active.cert.pem"
    os.replace(key_temp, active_key)
    os.replace(cert_temp, active_cert)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(active_cert), str(active_key))
    return certificate_snapshot()


def certificate_snapshot() -> dict[str, Any]:
    ensure_directories()
    cert_path = CERT_ROOT / "active.cert.pem"
    if not cert_path.exists():
        issue_certificate_sync(90 * 86400, "initial")
    cert = x509.load_pem_x509_certificate(cert_path.read_bytes())
    now = utc_now()
    expires_at = cert.not_valid_after_utc
    seconds = max(0, int((expires_at - now).total_seconds()))
    sans: list[str] = []
    try:
        extension = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
        sans = extension.value.get_values_for_type(x509.DNSName)
    except x509.ExtensionNotFound:
        pass
    CERT_EXPIRY.labels(hostname=DEMO_CERT_HOSTNAME).set(seconds)
    return {
        "hostname": DEMO_CERT_HOSTNAME,
        "issuer": cert.issuer.rfc4514_string(),
        "subject": cert.subject.rfc4514_string(),
        "serialNumber": str(cert.serial_number),
        "notBefore": cert.not_valid_before_utc.isoformat(),
        "notAfter": expires_at.isoformat(),
        "expiresInSeconds": seconds,
        "subjectAlternativeNames": sans,
        "binding": "active.cert.pem + active.key.pem",
        "bindingValidated": True,
        "renewalFailureScenario": bool(state.get("certificateRenewalFailure")),
        "openThresholdSeconds": CERT_OPEN_SECONDS,
    }


def reset_certificate_sync() -> dict[str, Any]:
    state["certificateRenewalFailure"] = False
    return issue_certificate_sync(90 * 86400, "reset")


def renew_certificate_sync(incident_id: str) -> dict[str, Any]:
    before = certificate_snapshot()
    if state.get("certificateRenewalFailure"):
        return {
            "action": "renew_certificate",
            "incidentId": incident_id,
            "executor": "opsai-automation:certificate-manager",
            "before": before,
            "verificationPassed": False,
            "error": "The controlled certificate authority renewal failure scenario is active.",
            "fallbackRecommendation": "bind_backup_certificate",
        }
    after = issue_certificate_sync(90 * 86400, "renewed")
    hostname_valid = DEMO_CERT_HOSTNAME in after.get("subjectAlternativeNames", [])
    chain_valid = "PulseGuard Demo Internal CA" in after.get("issuer", "")
    verified = bool(hostname_valid and chain_valid and after["expiresInSeconds"] > 30 * 86400)
    return {
        "action": "renew_certificate",
        "incidentId": incident_id,
        "executor": "opsai-automation:certificate-manager",
        "before": before,
        "after": after,
        "validation": {
            "hostnameValid": hostname_valid,
            "trustedDemoIssuer": chain_valid,
            "bindingLoadedBySslContext": after.get("bindingValidated", False),
        },
        "verificationPassed": verified,
    }


async def core_request(method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"X-OpsAI-Automation-Token": INTERNAL_TOKEN}
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.request(method, f"{OPSAI_CORE_URL}{path}", json=payload, headers=headers)
        response.raise_for_status()
        body = response.json()
        return body if isinstance(body, dict) else {}


async def fetch_json(url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        emit_log("WARNING", "fetch_failed", url=url, error=str(exc))
        return {}


async def open_external_incident(
    incident_type: str,
    title: str,
    severity: str,
    service: str,
    node: str,
    summary: str,
    runbook_hint: str,
    evidence: dict[str, Any],
) -> str | None:
    try:
        result = await core_request(
            "POST",
            "/api/external/incidents",
            {
                "incidentType": incident_type,
                "title": title,
                "severity": severity,
                "service": service,
                "node": node,
                "summary": summary,
                "runbookHint": runbook_hint,
                "evidence": evidence,
                "source": "opsai-automation",
            },
        )
        return str(result.get("incidentId") or "") or None
    except Exception as exc:
        emit_log("ERROR", "external_incident_open_failed", incidentType=incident_type, error=str(exc))
        return None


async def resolve_external_incident(
    incident_id: str,
    reason: str,
    repair_outcome: str,
    evidence: dict[str, Any],
) -> None:
    try:
        await core_request(
            "POST",
            f"/api/external/incidents/{incident_id}/resolve",
            {
                "reason": reason,
                "repairOutcome": repair_outcome,
                "evidence": evidence,
                "actor": "opsai-automation",
            },
        )
    except Exception as exc:
        emit_log("ERROR", "external_incident_resolve_failed", incidentId=incident_id, error=str(exc))


def route_queue(incident: dict[str, Any], investigation: dict[str, Any] | None) -> tuple[str, str, float, str]:
    incident_type = str(incident.get("incident_type") or "")
    mapping = {
        "NODE_DISK_PRESSURE": ("Infrastructure Support", "Application Engineering L3"),
        "TLS_CERTIFICATE_EXPIRING": ("Network & Platform Support", "Infrastructure Support"),
        "TLS_CERTIFICATE_RENEWAL_FAILED": ("Network & Platform Support", "Infrastructure Support"),
        "PAYMENT_NODE_UNAVAILABLE": ("Infrastructure Support", "Application Support L2"),
        "PAYMENT_NODE_FLAPPING": ("Infrastructure Support", "Application Engineering L3"),
        "PAYMENT_NODE_NETWORK_INSTABILITY": ("Network & Platform Support", "Infrastructure Support"),
        "PAYMENT_NODE_TIMEOUT": ("Network & Platform Support", "Infrastructure Support"),
        "PAYMENT_NODE_LATENCY": ("Infrastructure Support", "Application Engineering L3"),
        "PAYMENT_SHARED_DEPENDENCY_OUTAGE": ("Integration Support", "Application Engineering L3"),
        "PAYMENT_FLEET_CAPACITY_DEGRADATION": ("Infrastructure Support", "Application Engineering L3"),
        "PAYMENT_NODE_HUNG": ("Application Support L2", "Application Engineering L3"),
        "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE": ("Integration Support", "Application Support L2"),
        "CHECKOUT_FAILURE_RATE": ("Application Support L2", "Operations Triage"),
    }
    primary, secondary = mapping.get(incident_type, ("Operations Triage", "Application Support L2"))
    confidence = float((investigation or {}).get("confidence") or 0.7)
    confidence = max(0.55, min(0.98, confidence))
    if confidence < 0.60:
        primary, secondary = "Operations Triage", primary
    routing = "AUTO_ASSIGNED" if confidence >= 0.80 else "REVIEW_RECOMMENDED"
    reason = f"{incident_type or 'Unclassified incident'} maps to {primary} under the deterministic ownership matrix."
    return primary, secondary, confidence, routing + ": " + reason


def verification_criteria(incident_type: str) -> list[str]:
    if incident_type == "NODE_DISK_PRESSURE":
        return [
            f"Disk usage remains below {DISK_RECOVERY_PERCENT:.0f}% for three consecutive checks.",
            "Application health remains healthy after cleanup.",
            "Only allowlisted temporary, cache and archived-log paths were changed.",
        ]
    if incident_type.startswith("TLS_CERTIFICATE"):
        return [
            "Certificate hostname and SAN match checkout.pulseguard.local.",
            "The certificate chain is issued by the allowlisted demo internal CA.",
            "The active binding loads successfully and expiry is extended.",
        ]
    if incident_type == "PAYMENT_FLEET_CAPACITY_DEGRADATION":
        return [
            "The remaining payment nodes report the requested bounded capacity units.",
            "Fleet p95 latency remains below the recovery threshold for consecutive checks.",
            "Checkout failures remain below the recovery threshold while node 3 is unavailable.",
        ]
    if incident_type == "EXTERNAL_SERVICE_AUTHENTICATION_FAILURE":
        return [
            "The external partner service accepts the refreshed checkout credential.",
            "No credential value or bearer token appears in logs, activities or support handoffs.",
            "External authentication failures remain below the recovery threshold for consecutive checks.",
        ]
    if incident_type == "PAYMENT_NODE_HUNG":
        return [
            "The bounded application restart generation increases for the affected node.",
            "The node reports faultMode=none and acceptingPayments=true.",
            "Payment calls succeed and timeout/retry telemetry remains below recovery thresholds.",
        ]
    return [
        "The triggering metric remains below its recovery threshold.",
        "Customer failure and latency indicators return to acceptable levels.",
        "The affected component remains stable during the verification window.",
    ]


async def ensure_ticket(incident: dict[str, Any], investigation: dict[str, Any] | None, trigger: str) -> dict[str, Any]:
    incident_id = str(incident.get("id"))
    async with state_lock:
        existing = state.setdefault("tickets", {}).get(incident_id)
        if existing:
            return existing
        primary, secondary, confidence, routing_reason = route_queue(incident, investigation)
        evidence = incident.get("evidence") or {}
        ticket = {
            "ticketId": f"OPS-{str(uuid.uuid4())[:8].upper()}",
            "incidentId": incident_id,
            "incidentType": incident.get("incident_type"),
            "title": incident.get("title"),
            "severity": incident.get("severity"),
            "primaryQueue": primary,
            "secondaryQueue": secondary,
            "assignmentConfidence": round(confidence, 3),
            "assignmentMode": "AUTO_ASSIGNED" if confidence >= 0.80 else "REVIEW_RECOMMENDED",
            "routingReason": routing_reason,
            "status": "ASSIGNED",
            "acknowledgedBy": None,
            "acknowledgedAt": None,
            "createdAt": iso_now(),
            "updatedAt": iso_now(),
            "trigger": trigger,
            "handoff": {
                "incidentSummary": incident.get("summary"),
                "customerImpact": (investigation or {}).get("customer_impact") or "Operational impact is being assessed.",
                "technicalEvidence": evidence,
                "likelyCause": (investigation or {}).get("root_cause") or "Pending support validation.",
                "alternativeHypotheses": (investigation or {}).get("hypotheses") or [],
                "opsAICommanderRecommendation": (investigation or {}).get("recommended_action") or "Review the evidence and execute the applicable runbook.",
                "actionName": (investigation or {}).get("action_name"),
                "governanceDecision": (investigation or {}).get("policy_decision"),
                "governanceReason": (investigation or {}).get("policy_reason"),
                "actionExecutionStatus": (investigation or {}).get("action_execution_status"),
                "actionExecutionResult": (investigation or {}).get("action_execution_result") or {},
                "verificationCriteria": verification_criteria(str(incident.get("incident_type") or "")),
            },
        }
        state["tickets"][incident_id] = ticket
        TICKET_COUNTER.labels(queue=primary).inc()
        await persist_state()
    await append_activity(
        "SUPPORT_ASSIGNED",
        "warning" if incident.get("severity") != "critical" else "critical",
        f"Incident assigned to {primary}",
        f"PulseGuard prepared a detailed support handoff. Assignment confidence: {round(confidence * 100)}%.",
        incident_id,
        {"ticket": ticket},
    )
    return ticket


def reset_happened_after_incident(
    incident_opened_at: Any,
    remediation: dict[str, Any] | None,
) -> bool:
    reset_at = parse_time(state.get("lastScenarioResetAt"))
    opened_at = parse_time(incident_opened_at)
    if not reset_at or not opened_at or reset_at <= opened_at:
        return False
    completed_at = parse_time((remediation or {}).get("completedAt"))
    return completed_at is None or reset_at >= completed_at


def automatic_action_contributed_to_recovery(
    remediation: dict[str, Any] | None,
    expected_action: str,
) -> bool:
    if not remediation:
        return False
    if remediation.get("action") != expected_action or not remediation.get("executed"):
        return False

    completed_at = parse_time(remediation.get("completedAt"))
    reset_at = parse_time(state.get("lastScenarioResetAt"))
    if completed_at and reset_at and completed_at <= reset_at:
        return False

    if expected_action == "cleanup_disk_space":
        return int(remediation.get("reclaimedBytes") or 0) > 0
    if expected_action == "renew_certificate":
        return bool(
            remediation.get("verificationPassed")
            or remediation.get("bindingLoaded")
            or (remediation.get("verification") or {}).get("passed")
        )
    if expected_action == "scale_payment_capacity":
        verification = remediation.get("recoveryVerification") or {}
        return bool(
            remediation.get("actionSucceeded")
            and remediation.get("recoveryVerified")
            and verification.get("capacityRecoveryQualified") is True
        )
    if expected_action == "refresh_external_service_credentials":
        return bool(
            remediation.get("actionSucceeded")
            and remediation.get("verificationPassed")
            and remediation.get("secretRedacted") is True
        )
    return False


def mark_verified_repair(
    remediation: dict[str, Any],
    outcome: str,
    verification: dict[str, Any],
) -> None:
    remediation["verificationPassed"] = True
    remediation["recoveryVerified"] = True
    remediation["verifiedAt"] = iso_now()
    remediation["repairOutcome"] = outcome
    remediation["recoveryVerification"] = verification



async def monitor_disk_and_certificate() -> None:
    disk = await asyncio.to_thread(disk_snapshot)
    cert = await asyncio.to_thread(certificate_snapshot)

    disk_incident_id = state.get("diskIncidentId")
    if disk["usagePercent"] >= DISK_OPEN_PERCENT:
        state["diskRecoveryChecks"] = 0
        if not disk_incident_id:
            incident_id = await open_external_incident(
                "NODE_DISK_PRESSURE",
                "Bounded application storage is critically full",
                "high",
                "storage-platform",
                "opsai-demo-storage",
                f"Synthetic application storage reached {disk['usagePercent']:.1f}% usage.",
                "Archive old logs, remove allowlisted temporary/cache files, verify free space, and route to Infrastructure Support when controlled cleanup is insufficient.",
                {
                    "metric": "opsai_demo_disk_usage_percent",
                    "observed": disk["usagePercent"],
                    "threshold": DISK_OPEN_PERCENT,
                    "unit": "percent",
                    "storage": disk,
                    "evaluatedAt": iso_now(),
                },
            )
            if incident_id:
                state["diskIncidentId"] = incident_id
                state["diskIncidentOpenedAt"] = iso_now()
                await persist_state()
    elif disk_incident_id:
        state["diskRecoveryChecks"] = int(state.get("diskRecoveryChecks") or 0) + 1
        if state["diskRecoveryChecks"] >= 3:
            remediation = state.get("remediations", {}).get(disk_incident_id, {})
            verification = {
                "storage": disk,
                "checks": state["diskRecoveryChecks"],
                "threshold": DISK_RECOVERY_PERCENT,
                "verifiedAt": iso_now(),
            }

            if automatic_action_contributed_to_recovery(
                remediation,
                "cleanup_disk_space",
            ):
                outcome = "AUTO_REPAIRED"
                mark_verified_repair(remediation, outcome, verification)
            elif reset_happened_after_incident(
                state.get("diskIncidentOpenedAt"),
                remediation,
            ):
                outcome = "RECOVERED_AFTER_TEST_CLEANUP"
                if remediation:
                    remediation["repairOutcome"] = outcome
                    remediation["recoveryVerification"] = verification
            else:
                outcome = "RECOVERED_WITHOUT_ACTION"
                if remediation:
                    remediation["repairOutcome"] = outcome
                    remediation["recoveryVerification"] = verification

            await persist_state()
            await resolve_external_incident(
                disk_incident_id,
                f"Storage usage remained below {DISK_RECOVERY_PERCENT:.0f}% for three consecutive checks.",
                outcome,
                {
                    "storage": disk,
                    "remediation": remediation,
                    "verifiedAt": iso_now(),
                    "recoverySource": outcome,
                },
            )
            state["diskIncidentId"] = None
            state["diskIncidentOpenedAt"] = None
            state["diskRecoveryChecks"] = 0
            await persist_state()

    cert_incident_id = state.get("certificateIncidentId")
    if cert["expiresInSeconds"] <= CERT_OPEN_SECONDS:
        state["certificateRecoveryChecks"] = 0
        if not cert_incident_id:
            incident_id = await open_external_incident(
                "TLS_CERTIFICATE_EXPIRING",
                f"TLS certificate is expiring for {DEMO_CERT_HOSTNAME}",
                "high",
                "edge-platform",
                DEMO_CERT_HOSTNAME,
                f"The active certificate expires in {cert['expiresInSeconds']} seconds.",
                "Validate hostname, SAN, issuer and binding; renew through the allowlisted internal CA. Route failures to Network & Platform Support.",
                {
                    "metric": "opsai_demo_certificate_expiry_seconds",
                    "observed": cert["expiresInSeconds"],
                    "threshold": CERT_OPEN_SECONDS,
                    "unit": "seconds",
                    "certificate": cert,
                    "evaluatedAt": iso_now(),
                },
            )
            if incident_id:
                state["certificateIncidentId"] = incident_id
                state["certificateIncidentOpenedAt"] = iso_now()
                await persist_state()
    elif cert_incident_id:
        state["certificateRecoveryChecks"] = int(state.get("certificateRecoveryChecks") or 0) + 1
        if state["certificateRecoveryChecks"] >= 3:
            remediation = state.get("remediations", {}).get(cert_incident_id, {})
            verification = {
                "certificate": cert,
                "checks": state["certificateRecoveryChecks"],
                "threshold": CERT_OPEN_SECONDS,
                "verifiedAt": iso_now(),
            }

            if automatic_action_contributed_to_recovery(
                remediation,
                "renew_certificate",
            ):
                outcome = "AUTO_REPAIRED"
                mark_verified_repair(remediation, outcome, verification)
            elif reset_happened_after_incident(
                state.get("certificateIncidentOpenedAt"),
                remediation,
            ):
                outcome = "RECOVERED_AFTER_TEST_CLEANUP"
                if remediation:
                    remediation["repairOutcome"] = outcome
                    remediation["recoveryVerification"] = verification
            else:
                outcome = "RECOVERED_WITHOUT_ACTION"
                if remediation:
                    remediation["repairOutcome"] = outcome
                    remediation["recoveryVerification"] = verification

            await persist_state()
            await resolve_external_incident(
                cert_incident_id,
                "The replacement certificate passed hostname, issuer, binding and expiry verification for three consecutive checks.",
                outcome,
                {
                    "certificate": cert,
                    "remediation": remediation,
                    "verifiedAt": iso_now(),
                    "recoverySource": outcome,
                },
            )
            state["certificateIncidentId"] = None
            state["certificateIncidentOpenedAt"] = None
            state["certificateRecoveryChecks"] = 0
            await persist_state()


async def monitor_incident_activity() -> None:
    incidents_payload, investigations_payload = await asyncio.gather(
        fetch_json(f"{OPSAI_CORE_URL}/incidents?status=all&limit=300"),
        fetch_json(f"{OPSAI_AGENT_URL}/api/investigations"),
    )
    incidents = incidents_payload.get("incidents", []) if incidents_payload else []
    investigations = investigations_payload.get("investigations", []) if investigations_payload else []
    investigations_by_incident = {str(item.get("incident_id")): item for item in investigations}

    # Reconcile resolved incidents with persisted remediation evidence. This also
    # corrects older v0.5.1 rows that were labelled RECOVERED_WITHOUT_ACTION even
    # though a governed remediation executed and the incident later passed its
    # independent recovery checks.
    reconciled = False
    for incident in incidents:
        if incident.get("status") != "RESOLVED":
            continue
        incident_id = str(incident.get("id"))
        remediation = state.setdefault("remediations", {}).get(incident_id)
        if not remediation or not remediation.get("executed"):
            continue
        current_outcome = str(remediation.get("repairOutcome") or "")
        action = str(remediation.get("action") or "")
        evidence_outcome = str((incident.get("evidence") or {}).get("repairOutcome") or "")
        resolution_reason = str(incident.get("resolution_reason") or "")
        incident_evidence = incident.get("evidence") or {}
        capacity_source = str(incident_evidence.get("capacityRecoverySource") or "")
        capacity_qualified = incident_evidence.get("capacityRecoveryQualified") is True
        existing_verification = remediation.get("recoveryVerification") or {}
        historical_capacity_claim_needs_review = bool(
            action == "scale_payment_capacity"
            and current_outcome in {"AUTO_REPAIRED", "OPERATOR_REPAIRED"}
            and existing_verification.get("capacityRecoveryQualified") is not True
        )
        if (
            current_outcome in {"AUTO_REPAIRED", "OPERATOR_REPAIRED", "RECOVERED_AFTER_TEST_CLEANUP"}
            and not historical_capacity_claim_needs_review
        ):
            continue
        if action == "scale_payment_capacity" and not capacity_source:
            traffic_context = incident_evidence.get("trafficContext") or {}
            override = traffic_context.get("trafficOverride") or {}
            if override.get("active") is False:
                capacity_source = "TRAFFIC_NORMALIZED"
        verified_by_resolution = (
            (action == "cleanup_disk_space" and "Storage usage remained below" in resolution_reason)
            or (action == "renew_certificate" and "replacement certificate passed" in resolution_reason)
        )
        if evidence_outcome == "RECOVERED_AFTER_TEST_CLEANUP":
            remediation["repairOutcome"] = evidence_outcome
            reconciled = True
        elif action == "scale_payment_capacity":
            if capacity_qualified and remediation.get("actionSucceeded"):
                previous_outcome = remediation.get("repairOutcome") or evidence_outcome or "UNSET"
                remediation["outcomeCorrectedFrom"] = previous_outcome
                mark_verified_repair(
                    remediation,
                    "OPERATOR_REPAIRED" if remediation.get("approvedBy") else "AUTO_REPAIRED",
                    {
                        "source": "capacity-processing-verification",
                        "capacityRecoveryQualified": True,
                        "capacityRecoverySource": capacity_source,
                        "processingP95SecondsByNode": incident_evidence.get("paymentProcessingP95SecondsByNode") or {},
                        "capacityUnitsByNode": incident_evidence.get("capacityUnitsByNode") or {},
                        "checkoutFailurePercent": incident_evidence.get("checkoutFailurePercent"),
                        "trafficSurgeActive": incident_evidence.get("trafficSurgeActive"),
                        "resolutionReason": resolution_reason,
                        "verifiedAt": iso_now(),
                    },
                )
            elif capacity_source == "TRAFFIC_NORMALIZED":
                remediation["outcomeCorrectedFrom"] = current_outcome or evidence_outcome or "UNSET"
                remediation["verificationPassed"] = False
                remediation["recoveryVerified"] = False
                remediation["repairOutcome"] = "RECOVERED_AFTER_TRAFFIC_NORMALIZED"
                remediation["recoveryVerification"] = {
                    "source": "traffic-normalized-before-capacity-verification",
                    "capacityRecoveryQualified": False,
                    "capacityRecoverySource": capacity_source,
                    "resolutionReason": resolution_reason,
                    "verifiedAt": iso_now(),
                }
            elif reset_happened_after_incident(incident.get("opened_at"), remediation):
                remediation["verificationPassed"] = False
                remediation["recoveryVerified"] = False
                remediation["repairOutcome"] = "RECOVERED_AFTER_TEST_CLEANUP"
            else:
                remediation["repairOutcome"] = remediation.get("repairOutcome") or "AUTO_ACTION_COMPLETED"
            reconciled = True
        elif verified_by_resolution and automatic_action_contributed_to_recovery(remediation, action):
            previous_outcome = remediation.get("repairOutcome") or evidence_outcome or "UNSET"
            remediation["outcomeCorrectedFrom"] = previous_outcome
            mark_verified_repair(
                remediation,
                "OPERATOR_REPAIRED" if remediation.get("approvedBy") else "AUTO_REPAIRED",
                {
                    "source": "resolved-incident-reconciliation",
                    "resolutionReason": resolution_reason,
                    "verifiedAt": iso_now(),
                },
            )
            reconciled = True
    if reconciled:
        await persist_state()

    for incident in incidents:
        incident_id = str(incident.get("id"))
        previous = state.setdefault("seenIncidents", {}).get(incident_id)
        current_signature = {
            "status": incident.get("status"),
            "updatedAt": str(incident.get("updated_at")),
            "repairOutcome": (incident.get("evidence") or {}).get("repairOutcome"),
        }
        if not previous:
            await append_activity(
                "INCIDENT_RAISED",
                "critical" if incident.get("severity") == "critical" else "warning",
                "PulseGuard detected an incident",
                str(incident.get("title") or incident.get("incident_type")),
                incident_id,
                {"incident": incident},
            )
        elif previous.get("status") != current_signature["status"]:
            if current_signature["status"] == "RESOLVED":
                remediation = state.setdefault("remediations", {}).get(incident_id)
                if remediation and remediation.get("executed"):
                    action_succeeded = bool(
                        remediation.get("actionSucceeded", remediation.get("verificationPassed", False))
                    )
                    incident_evidence = incident.get("evidence") or {}
                    action = str(remediation.get("action") or "")
                    capacity_source = str(incident_evidence.get("capacityRecoverySource") or "")
                    capacity_qualified = incident_evidence.get("capacityRecoveryQualified") is True
                    if action == "scale_payment_capacity":
                        if action_succeeded and capacity_qualified:
                            mark_verified_repair(
                                remediation,
                                "OPERATOR_REPAIRED" if remediation.get("approvedBy") else "AUTO_REPAIRED",
                                {
                                    "source": "capacity-processing-verification",
                                    "capacityRecoveryQualified": True,
                                    "capacityRecoverySource": capacity_source,
                                    "processingP95SecondsByNode": incident_evidence.get("paymentProcessingP95SecondsByNode") or {},
                                    "capacityUnitsByNode": incident_evidence.get("capacityUnitsByNode") or {},
                                    "checkoutFailurePercent": incident_evidence.get("checkoutFailurePercent"),
                                    "trafficSurgeActive": incident_evidence.get("trafficSurgeActive"),
                                    "verifiedAt": iso_now(),
                                },
                            )
                        elif capacity_source == "TRAFFIC_NORMALIZED":
                            remediation["verificationPassed"] = False
                            remediation["recoveryVerified"] = False
                            remediation["repairOutcome"] = "RECOVERED_AFTER_TRAFFIC_NORMALIZED"
                        elif reset_happened_after_incident(incident.get("opened_at"), remediation):
                            remediation["verificationPassed"] = False
                            remediation["recoveryVerified"] = False
                            remediation["repairOutcome"] = "RECOVERED_AFTER_TEST_CLEANUP"
                        else:
                            remediation["repairOutcome"] = remediation.get("repairOutcome") or "AUTO_ACTION_COMPLETED"
                    elif remediation.get("approvedBy") and action_succeeded:
                        remediation["verificationPassed"] = True
                        remediation["recoveryVerified"] = True
                        remediation["verifiedAt"] = iso_now()
                        remediation["repairOutcome"] = "OPERATOR_REPAIRED"
                    elif (
                        not remediation.get("approvedBy")
                        and action_succeeded
                        and action in {"cleanup_disk_space", "renew_certificate", "refresh_external_service_credentials"}
                    ):
                        remediation["verificationPassed"] = True
                        remediation["recoveryVerified"] = True
                        remediation["verifiedAt"] = iso_now()
                        remediation["repairOutcome"] = "AUTO_REPAIRED"
                    await persist_state()
                outcome = (remediation or {}).get("repairOutcome") or current_signature.get("repairOutcome") or "RECOVERED_WITHOUT_ACTION"
                await append_activity(
                    "INCIDENT_RESOLVED",
                    "success",
                    "Recovery verified",
                    f"Incident resolved with outcome {outcome}.",
                    incident_id,
                    {"incident": incident, "repairOutcome": outcome},
                )
                ticket = state.setdefault("tickets", {}).get(incident_id)
                if ticket:
                    ticket["status"] = "RESOLVED"
                    ticket["updatedAt"] = iso_now()
                    await persist_state()
            elif current_signature["status"] == "ACKNOWLEDGED":
                await append_activity(
                    "INCIDENT_ACKNOWLEDGED",
                    "info",
                    "Incident acknowledged",
                    "An operator acknowledged the incident in the Incident Console.",
                    incident_id,
                )
        state["seenIncidents"][incident_id] = current_signature

    for investigation in investigations:
        incident_id = str(investigation.get("incident_id"))
        key = str(investigation.get("id") or incident_id)
        previous = state.setdefault("seenInvestigations", {}).get(key)
        signature = {
            "status": investigation.get("status"),
            "action": investigation.get("action_name"),
            "policy": investigation.get("policy_decision"),
            "execution": investigation.get("action_execution_status"),
        }
        if not previous:
            await append_activity(
                "INVESTIGATION_STARTED" if investigation.get("status") != "COMPLETED" else "INVESTIGATION_COMPLETED",
                "info",
                "PulseGuard is investigating",
                "Operational telemetry, topology and runbook evidence are being correlated.",
                incident_id,
                {"investigationId": investigation.get("id")},
            )
        if investigation.get("status") == "COMPLETED" and previous != signature:
            action = str(investigation.get("action_name") or "no_action")
            policy = str(investigation.get("policy_decision") or "NOT_EVALUATED")
            execution_status = str(investigation.get("action_execution_status") or "NOT_EVALUATED")
            await append_activity(
                "OPSAI_RECOMMENDATION",
                "info",
                "PulseGuard recommendation",
                f"Recommended {action} with {round(float(investigation.get('confidence') or 0) * 100)}% confidence.",
                incident_id,
                {"investigation": investigation},
            )
            await append_activity(
                "GOVERNANCE_DECISION",
                "warning" if policy in {"APPROVAL_REQUIRED", "BLOCKED"} else "info",
                "Governance decision",
                f"{policy}: {investigation.get('policy_reason') or ''}",
                incident_id,
                {"policyDecision": policy, "action": action},
            )
            if investigation.get("action_executed"):
                await append_activity(
                    "ACTION_EXECUTED",
                    "success" if execution_status == "SUCCEEDED" else "critical",
                    "Operational action completed" if execution_status == "SUCCEEDED" else "Operational action failed",
                    f"{action} execution status: {execution_status}.",
                    incident_id,
                    {
                        "action": action,
                        "executionStatus": execution_status,
                        "executor": investigation.get("action_executor"),
                        "result": investigation.get("action_execution_result"),
                    },
                )
            else:
                await append_activity(
                    "ACTION_NOT_EXECUTED",
                    "warning" if policy == "APPROVAL_REQUIRED" else "info",
                    "No operational action executed",
                    f"Recommended action {action} was not executed. Governance: {policy}.",
                    incident_id,
                    {"action": action, "policyDecision": policy, "executionStatus": execution_status},
                )
            incident = next((item for item in incidents if str(item.get("id")) == incident_id), None)
            if incident and incident.get("status") != "RESOLVED":
                # Create an immediate handoff only when a human decision is required,
                # governance blocked the action, or execution genuinely failed. Successful
                # diagnostic collection is not itself a reason to create a support ticket.
                needs_support = (
                    policy in {"APPROVAL_REQUIRED", "BLOCKED"}
                    or execution_status == "FAILED"
                )
                if needs_support:
                    await ensure_ticket(incident, investigation, f"{policy}/{execution_status}")
        state["seenInvestigations"][key] = signature

    # Diagnostic-only incidents get time to recover before a manual handoff is created.
    # This avoids assigning routine transient incidents to support immediately.
    incident_by_id = {str(item.get("id")): item for item in incidents}
    for investigation in investigations:
        if investigation.get("status") != "COMPLETED":
            continue
        action = str(investigation.get("action_name") or "no_action")
        execution_status = str(investigation.get("action_execution_status") or "NOT_EVALUATED")
        if action not in {"collect_diagnostics", "collect_dependency_diagnostics", "no_action"}:
            continue
        if action != "no_action" and execution_status != "SUCCEEDED":
            continue
        incident_id = str(investigation.get("incident_id"))
        incident = incident_by_id.get(incident_id)
        if not incident or incident.get("status") == "RESOLVED" or state.get("tickets", {}).get(incident_id):
            continue
        opened_at = parse_time(incident.get("opened_at"))
        age_seconds = (utc_now() - opened_at).total_seconds() if opened_at else 0
        if age_seconds >= SUPPORT_HANDOFF_DELAY_SECONDS:
            await ensure_ticket(
                incident,
                investigation,
                f"UNRESOLVED_AFTER_DIAGNOSTICS/{round(age_seconds)}s",
            )

    await persist_state()


async def monitor_loop() -> None:
    while not stop_event.is_set():
        try:
            await monitor_disk_and_certificate()
            await monitor_incident_activity()
        except Exception as exc:
            emit_log("ERROR", "monitor_cycle_failed", error=str(exc))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=MONITOR_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


def operations_context(incident_id: str) -> dict[str, Any]:
    activities = [event for event in state.get("activities", []) if event.get("incidentId") == incident_id]
    remediation = state.get("remediations", {}).get(incident_id)
    ticket = state.get("tickets", {}).get(incident_id)
    repair_outcome = None
    if remediation:
        if remediation.get("repairOutcome"):
            repair_outcome = remediation.get("repairOutcome")
        elif remediation.get("recoveryVerified"):
            repair_outcome = "OPERATOR_REPAIRED" if remediation.get("approvedBy") else "AUTO_REPAIRED"
        elif remediation.get("verificationPassed"):
            repair_outcome = "OPERATOR_REPAIRED" if remediation.get("approvedBy") else "AUTO_REPAIRED_PENDING_CORE_VERIFICATION"
        elif remediation.get("executed"):
            repair_outcome = "OPERATOR_ACTION_COMPLETED" if remediation.get("approvedBy") else "AUTO_ACTION_COMPLETED"
    if ticket and ticket.get("status") != "RESOLVED":
        repair_outcome = repair_outcome or "MANUAL_INTERVENTION_REQUIRED"
    return {
        "incidentId": incident_id,
        "remediation": remediation,
        "supportHandoff": ticket,
        "repairOutcome": repair_outcome,
        "activities": activities,
    }


def summary_snapshot() -> dict[str, Any]:
    # Old v0.5 state is intentionally preserved across upgrades. Take filtered,
    # immutable snapshots so one malformed legacy record can never break the KPI API.
    normalise_state_shape()
    incident_rows = [
        item for item in list(state.get("seenIncidents", {}).values())
        if isinstance(item, dict)
    ]
    ticket_rows = [
        item for item in list(state.get("tickets", {}).values())
        if isinstance(item, dict)
    ]
    remediation_rows = [
        item for item in list(state.get("remediations", {}).values())
        if isinstance(item, dict)
    ]
    investigation_rows = [
        item for item in list(state.get("seenInvestigations", {}).values())
        if isinstance(item, dict)
    ]
    activity_rows = [
        item for item in list(state.get("activities", []))
        if isinstance(item, dict)
    ]

    active_count = sum(
        item.get("status") in {"OPEN", "ACKNOWLEDGED"}
        for item in incident_rows
    )
    auto_repaired = sum(
        item.get("repairOutcome") == "AUTO_REPAIRED"
        or (
            bool(item.get("recoveryVerified"))
            and item.get("action") in {
                "cleanup_disk_space",
                "renew_certificate",
                "scale_payment_capacity",
            }
        )
        for item in remediation_rows
    )
    AUTO_REPAIRED.set(auto_repaired)

    # Count only unresolved, actionable approval handoffs. Historical
    # APPROVAL_REQUIRED investigations must not remain in the current KPI.
    awaiting_approval = sum(
        ticket.get("status") in {
            "ASSIGNED",
            "ACKNOWLEDGED",
            "IN_PROGRESS",
            "ESCALATED_TO_L3",
        }
        and (ticket.get("handoff") or {}).get("governanceDecision") == "APPROVAL_REQUIRED"
        and not ticket.get("approvedBy")
        and not ticket.get("rejectedBy")
        for ticket in ticket_rows
    )
    policy_blocked = sum(
        item.get("policy") == "BLOCKED"
        for item in investigation_rows
    )

    now = utc_now()
    unacknowledged = sum(
        ticket.get("status") == "ASSIGNED"
        for ticket in ticket_rows
    )
    in_progress = sum(
        ticket.get("status") in {
            "ACKNOWLEDGED",
            "IN_PROGRESS",
            "ESCALATED_TO_L3",
        }
        for ticket in ticket_rows
    )
    sla_at_risk = 0
    for ticket in ticket_rows:
        if ticket.get("status") == "RESOLVED":
            continue
        created = parse_time(ticket.get("createdAt"))
        if created and created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created and (now - created).total_seconds() >= SLA_RISK_SECONDS:
            sla_at_risk += 1

    return {
        "activeIncidents": active_count,
        "investigated": sum(
            item.get("status") == "COMPLETED"
            for item in investigation_rows
        ),
        "autoRepaired": auto_repaired,
        "awaitingApproval": awaiting_approval,
        "policyBlocked": policy_blocked,
        "assignedToSupport": sum(
            ticket.get("status") != "RESOLVED"
            for ticket in ticket_rows
        ),
        "unacknowledged": unacknowledged,
        "inProgress": in_progress,
        "slaAtRisk": sla_at_risk,
        "resolved": sum(
            item.get("status") == "RESOLVED"
            for item in incident_rows
        ),
        "resiliencePasses": 0,
        "disk": disk_snapshot(),
        "certificate": certificate_snapshot(),
        "queues": queue_summary(ticket_rows),
        "latestActivity": activity_rows[-1] if activity_rows else None,
        "stateCompatibility": {
            "version": SERVICE_VERSION,
            "normalised": True,
        },
    }


def queue_summary(tickets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queues: dict[str, dict[str, Any]] = {}
    for ticket in tickets:
        if not isinstance(ticket, dict):
            continue
        queue = str(ticket.get("primaryQueue") or "Operations Triage")
        row = queues.setdefault(queue, {"queue": queue, "assigned": 0, "waiting": 0, "inProgress": 0, "resolved": 0})
        row["assigned"] += 1
        status = ticket.get("status")
        if status == "ASSIGNED":
            row["waiting"] += 1
        elif status == "RESOLVED":
            row["resolved"] += 1
        else:
            row["inProgress"] += 1
    return sorted(queues.values(), key=lambda item: item["queue"])


@app.on_event("startup")
async def startup() -> None:
    ensure_directories()
    load_state()
    await asyncio.to_thread(reset_storage_sync)
    if not (CERT_ROOT / "active.cert.pem").exists():
        await asyncio.to_thread(reset_certificate_sync)
    app.state.monitor = asyncio.create_task(monitor_loop(), name="opsai-automation-monitor")
    await append_activity(
        "SERVICE_READY",
        "info",
        "PulseGuard automation services ready",
        "Live activity, support triage, bounded disk remediation and certificate lifecycle monitoring are active.",
    )


@app.on_event("shutdown")
async def shutdown() -> None:
    stop_event.set()
    task = getattr(app.state, "monitor", None)
    if task:
        await task


@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": "opsai-automation",
        "version": SERVICE_VERSION,
        "disk": disk_snapshot(),
        "certificate": certificate_snapshot(),
    }


@app.get("/state")
async def get_state() -> dict[str, Any]:
    return {
        "disk": disk_snapshot(),
        "certificate": certificate_snapshot(),
        "summary": summary_snapshot(),
        "tickets": list(state.get("tickets", {}).values()),
        "remediations": state.get("remediations", {}),
    }


@app.get("/summary")
async def get_summary() -> dict[str, Any]:
    return summary_snapshot()


@app.get("/api/incidents/{incident_id}/operations")
async def get_incident_operations(incident_id: str) -> dict[str, Any]:
    return operations_context(incident_id)


@app.get("/activity")
async def get_activity(
    limit: int = Query(default=100, ge=1, le=500),
    incident_id: str | None = Query(default=None),
) -> dict[str, Any]:
    events = state.get("activities", [])
    if incident_id:
        events = [event for event in events if event.get("incidentId") == incident_id]
    return {"activities": events[-limit:]}


@app.get("/activity/stream")
async def activity_stream(last_event_id: str | None = Header(default=None, alias="Last-Event-ID")) -> StreamingResponse:
    async def event_generator():
        sent: set[str] = set()
        if last_event_id:
            sent.add(last_event_id)
        while not stop_event.is_set():
            current = list(state.get("activities", []))
            start_index = 0
            if last_event_id:
                for index, event in enumerate(current):
                    if event.get("eventId") == last_event_id:
                        start_index = index + 1
                        break
            for event in current[start_index:]:
                event_id = str(event.get("eventId"))
                if event_id in sent:
                    continue
                sent.add(event_id)
                yield f"id: {event_id}\nevent: opsai-activity\ndata: {json.dumps(event, default=str)}\n\n"
            try:
                async with activity_condition:
                    await asyncio.wait_for(activity_condition.wait(), timeout=15)
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.post("/scenarios/reset")
async def reset_scenarios() -> dict[str, Any]:
    global predictive_disk_task
    if predictive_disk_task and not predictive_disk_task.done():
        predictive_disk_task.cancel()
        await asyncio.gather(predictive_disk_task, return_exceptions=True)
    predictive_disk_task = None
    state["lastScenarioResetAt"] = iso_now()
    disk = await asyncio.to_thread(reset_storage_sync)
    certificate = await asyncio.to_thread(reset_certificate_sync)
    await persist_state()
    await append_activity(
        "SCENARIO_RESET",
        "info",
        "Automation scenarios reset",
        "Synthetic storage and certificate conditions were returned to a healthy baseline. Test cleanup is recorded separately and is not counted as an automatic repair.",
        details={"resetAt": state["lastScenarioResetAt"]},
    )
    return {
        "status": "reset",
        "resetAt": state["lastScenarioResetAt"],
        "disk": disk,
        "certificate": certificate,
    }


@app.post("/scenarios/disk-pressure")
async def disk_pressure(
    target_percent: float = Query(default=92, ge=86, le=95),
    cleanup_insufficient: bool = Query(default=False),
) -> dict[str, Any]:
    result = await asyncio.to_thread(fill_storage_sync, target_percent, cleanup_insufficient)
    await append_activity(
        "DISTURBANCE_INJECTED",
        "warning",
        "Controlled disk pressure injected",
        f"Bounded synthetic storage increased to {result['usagePercent']:.1f}%.",
        details={"disk": result, "cleanupInsufficient": cleanup_insufficient},
    )
    return result


@app.post("/scenarios/disk-growth")
async def predictive_disk_growth(
    start_percent: float = Query(default=35, ge=5, le=70),
    end_percent: float = Query(default=79, ge=50, le=84),
    duration_seconds: int = Query(default=120, ge=45, le=600),
) -> dict[str, Any]:
    global predictive_disk_task
    if end_percent <= start_percent:
        raise HTTPException(status_code=400, detail="end_percent must be greater than start_percent.")
    if predictive_disk_task and not predictive_disk_task.done():
        raise HTTPException(status_code=409, detail="A predictive disk-growth scenario is already running.")
    predictive_disk_task = asyncio.create_task(
        run_predictive_disk_growth(start_percent, end_percent, duration_seconds),
        name="opsai-predictive-disk-growth",
    )
    await append_activity(
        "PREDICTIVE_SCENARIO_STARTED",
        "warning",
        "Predictive disk-growth scenario started",
        f"Storage will grow from {start_percent:.1f}% to {end_percent:.1f}% over {duration_seconds} seconds without crossing the reactive threshold.",
        details={
            "startPercent": start_percent,
            "endPercent": end_percent,
            "durationSeconds": duration_seconds,
            "reactiveThresholdPercent": DISK_OPEN_PERCENT,
        },
    )
    return {
        "status": "running",
        "scenario": "predictive_disk_growth",
        "startPercent": start_percent,
        "endPercent": end_percent,
        "durationSeconds": duration_seconds,
        "reactiveThresholdPercent": DISK_OPEN_PERCENT,
        "observationOnly": True,
    }


@app.post("/scenarios/certificate-expiring")
async def certificate_expiring(seconds: int = Query(default=300, ge=60, le=900)) -> dict[str, Any]:
    state["certificateRenewalFailure"] = False
    result = await asyncio.to_thread(issue_certificate_sync, seconds, "expiring")
    await persist_state()
    await append_activity(
        "DISTURBANCE_INJECTED",
        "warning",
        "Expiring certificate injected",
        f"The active certificate for {DEMO_CERT_HOSTNAME} now expires in approximately {seconds} seconds.",
        details={"certificate": result},
    )
    return result


@app.post("/scenarios/certificate-renewal-failure")
async def certificate_renewal_failure(seconds: int = Query(default=300, ge=60, le=900)) -> dict[str, Any]:
    state["certificateRenewalFailure"] = True
    result = await asyncio.to_thread(issue_certificate_sync, seconds, "renewal-failure")
    state["certificateRenewalFailure"] = True
    await persist_state()
    await append_activity(
        "DISTURBANCE_INJECTED",
        "critical",
        "Certificate renewal failure injected",
        "The certificate is near expiry and the controlled internal CA renewal path will fail, requiring support intervention.",
        details={"certificate": result},
    )
    return result


@app.post("/actions/cleanup-disk")
async def cleanup_disk(request: ActionRequest, x_opsai_automation_token: str | None = Header(default=None)) -> dict[str, Any]:
    require_token(x_opsai_automation_token)
    await append_activity(
        "ACTION_STARTED",
        "info",
        "Controlled disk cleanup started",
        "PulseGuard is archiving the cleanup manifest and deleting only allowlisted temporary, cache and archived-log files.",
        request.incidentId,
    )
    result = await asyncio.to_thread(cleanup_storage_sync, request.incidentId)
    result["executed"] = True
    result["actionSucceeded"] = int(result.get("reclaimedBytes") or 0) > 0
    result["completedAt"] = iso_now()
    result["repairOutcome"] = (
        "AUTO_REPAIRED_PENDING_CORE_VERIFICATION"
        if result.get("verificationPassed")
        else "AUTO_ACTION_COMPLETED"
    )
    state.setdefault("remediations", {})[request.incidentId] = result
    await persist_state()
    REMEDIATION_COUNTER.labels(action="cleanup_disk_space", result="succeeded" if result["verificationPassed"] else "insufficient").inc()
    await append_activity(
        "ACTION_EXECUTED",
        "success" if result["verificationPassed"] else "warning",
        "Disk cleanup completed",
        f"Reclaimed {result['reclaimedBytes']} bytes. Usage is now {result['after']['usagePercent']:.1f}%.",
        request.incidentId,
        {"result": result},
    )
    return result


@app.post("/actions/renew-certificate")
async def renew_certificate(request: ActionRequest, x_opsai_automation_token: str | None = Header(default=None)) -> JSONResponse:
    require_token(x_opsai_automation_token)
    await append_activity(
        "ACTION_STARTED",
        "info",
        "Certificate renewal started",
        "PulseGuard is validating the hostname, SAN, issuer and replacement binding.",
        request.incidentId,
    )
    result = await asyncio.to_thread(renew_certificate_sync, request.incidentId)
    result["executed"] = True
    result["completedAt"] = iso_now()
    state.setdefault("remediations", {})[request.incidentId] = result
    await persist_state()
    REMEDIATION_COUNTER.labels(action="renew_certificate", result="succeeded" if result.get("verificationPassed") else "failed").inc()
    await append_activity(
        "ACTION_EXECUTED",
        "success" if result.get("verificationPassed") else "critical",
        "Certificate renewal completed" if result.get("verificationPassed") else "Certificate renewal failed",
        "The replacement binding passed validation." if result.get("verificationPassed") else str(result.get("error")),
        request.incidentId,
        {"result": result},
    )
    return JSONResponse(result, status_code=200 if result.get("verificationPassed") else 503)


@app.post("/actions/scale-payment-capacity")
async def scale_payment_capacity(
    request: ActionRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> JSONResponse:
    require_token(x_opsai_automation_token)
    parameters = request.parameters or {}
    target_nodes = [
        str(item) for item in parameters.get("targetNodes", ["payment-node-1", "payment-node-2"])
        if str(item) in PAYMENT_NODE_URLS and str(item) != "payment-node-3"
    ]
    target_nodes = sorted(set(target_nodes))
    units = max(2, min(3, int(parameters.get("capacityUnits", 2))))
    if not target_nodes:
        raise HTTPException(status_code=409, detail="No allowlisted healthy peer nodes were selected for bounded capacity scaling")

    await append_activity(
        "ACTION_STARTED",
        "info",
        "Bounded payment capacity scaling started",
        f"PulseGuard is increasing simulated worker capacity on {', '.join(target_nodes)} to {units} units.",
        request.incidentId,
        {"targetNodes": target_nodes, "capacityUnits": units, "simulatedInfrastructure": True},
    )
    results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=10) as client:
        for node in target_nodes:
            try:
                response = await client.post(
                    f"{PAYMENT_NODE_URLS[node]}/admin/capacity",
                    json={
                        "units": units,
                        "pressureMs": int(parameters.get("pressureMs", 1000)),
                        "reason": "automatic_scale",
                    },
                )
                payload = response.json() if response.content else {}
                response.raise_for_status()
                results.append(payload)
            except Exception as exc:
                failures.append({"node": node, "error": str(exc)})

    succeeded = not failures and len(results) == len(target_nodes)
    result = {
        "action": "scale_payment_capacity",
        "incidentId": request.incidentId,
        "executor": "opsai-automation:bounded-capacity-controller",
        "executed": True,
        "actionSucceeded": succeeded,
        "verificationPassed": False,
        "recoveryVerified": False,
        "targetNodes": target_nodes,
        "capacityUnits": units,
        "simulatedInfrastructure": True,
        "scope": "application worker capacity only; no Docker socket or host resource mutation",
        "nodeResults": results,
        "failures": failures,
        "completedAt": iso_now(),
    }
    state.setdefault("remediations", {})[request.incidentId] = result
    await persist_state()
    REMEDIATION_COUNTER.labels(
        action="scale_payment_capacity",
        result="applied" if succeeded else "failed",
    ).inc()
    await append_activity(
        "ACTION_EXECUTED",
        "success" if succeeded else "critical",
        "Payment capacity scaling applied" if succeeded else "Payment capacity scaling failed",
        (
            f"Capacity increased to {units} units on {', '.join(target_nodes)}; PulseGuard Core is verifying latency recovery."
            if succeeded
            else f"Capacity scaling failed on {len(failures)} node(s)."
        ),
        request.incidentId,
        {"result": result},
    )
    return JSONResponse(result, status_code=200 if succeeded else 503)


@app.post("/actions/refresh-external-credentials")
async def refresh_external_credentials(
    request: ActionRequest,
    x_opsai_automation_token: str | None = Header(default=None),
) -> JSONResponse:
    require_token(x_opsai_automation_token)
    parameters = request.parameters if isinstance(request.parameters, dict) else {}
    if parameters.get("service") not in {None, "", "partner-risk-service"}:
        raise HTTPException(status_code=409, detail="Only partner-risk-service is allowlisted.")
    if parameters.get("client") not in {None, "", "checkout-service"}:
        raise HTTPException(status_code=409, detail="Only checkout-service may receive the refreshed credential.")

    started = iso_now()
    await append_activity(
        "ACTION_STARTED",
        "warning",
        "External credential refresh started",
        "PulseGuard is rotating the bounded partner credential and updating checkout without exposing the secret.",
        request.incidentId,
        {"service": "partner-risk-service", "client": "checkout-service"},
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            rotate_response = await client.post(
                f"{EXTERNAL_AUTH_SERVICE_URL}/admin/rotate",
                headers={"X-OpsAI-Automation-Token": INTERNAL_TOKEN},
                json={"reason": "governed_incident_repair"},
            )
            rotate_response.raise_for_status()
            rotated = rotate_response.json()
            token = str(rotated.get("token") or "")
            generation = int(rotated.get("tokenGeneration") or 0)
            fingerprint = str(rotated.get("tokenFingerprint") or "")
            if not token or generation < 1:
                raise RuntimeError("External service did not return a valid rotated credential.")

            update_response = await client.post(
                f"{CHECKOUT_SERVICE_URL}/admin/external-auth/token",
                headers={"X-OpsAI-Automation-Token": INTERNAL_TOKEN},
                json={
                    "token": token,
                    "tokenGeneration": generation,
                    "reason": "opsai_governed_refresh",
                },
            )
            update_response.raise_for_status()

            verify_response = await client.post(
                f"{CHECKOUT_SERVICE_URL}/admin/external-auth/verify",
                headers={"X-OpsAI-Automation-Token": INTERNAL_TOKEN},
            )
            verify_response.raise_for_status()
            verified = verify_response.json()

        result = {
            "action": "refresh_external_service_credentials",
            "incidentId": request.incidentId,
            "executed": True,
            "actionSucceeded": bool(verified.get("verified")),
            "verificationPassed": bool(verified.get("verified")),
            "secretRedacted": True,
            "externalService": "partner-risk-service",
            "client": "checkout-service",
            "tokenFingerprint": fingerprint,
            "tokenGeneration": generation,
            "startedAt": started,
            "completedAt": iso_now(),
            "executor": "opsai-automation:bounded-credential-rotation",
            "repairOutcome": (
                "AUTO_REPAIRED_PENDING_CORE_VERIFICATION"
                if verified.get("verified")
                else "AUTO_ACTION_COMPLETED"
            ),
            "verification": {
                "verified": bool(verified.get("verified")),
                "response": verified.get("response"),
                "verifiedAt": verified.get("verifiedAt"),
            },
        }
        state.setdefault("remediations", {})[request.incidentId] = result
        REMEDIATION_COUNTER.labels(
            action="refresh_external_service_credentials",
            result="success" if result["actionSucceeded"] else "failed",
        ).inc()
        await persist_state()
        await append_activity(
            "ACTION_EXECUTED",
            "success" if result["actionSucceeded"] else "critical",
            "External credential refresh completed" if result["actionSucceeded"] else "External credential refresh failed",
            (
                "The credential was rotated, checkout was updated, and an authenticated probe succeeded."
                if result["actionSucceeded"]
                else "The bounded credential workflow did not pass verification."
            ),
            request.incidentId,
            {
                "result": {
                    key: value
                    for key, value in result.items()
                    if key not in {"token"}
                }
            },
        )
        return JSONResponse(result, status_code=200 if result["actionSucceeded"] else 503)
    except Exception as exc:
        failure = {
            "action": "refresh_external_service_credentials",
            "incidentId": request.incidentId,
            "executed": True,
            "actionSucceeded": False,
            "verificationPassed": False,
            "secretRedacted": True,
            "startedAt": started,
            "completedAt": iso_now(),
            "executor": "opsai-automation:bounded-credential-rotation",
            "repairOutcome": "MANUAL_INTERVENTION_REQUIRED",
            "error": str(exc),
        }
        state.setdefault("remediations", {})[request.incidentId] = failure
        REMEDIATION_COUNTER.labels(
            action="refresh_external_service_credentials",
            result="failed",
        ).inc()
        await persist_state()
        await append_activity(
            "ACTION_FAILED",
            "critical",
            "External credential refresh failed",
            "The bounded credential workflow failed. Integration Support handoff is required.",
            request.incidentId,
            {"result": failure},
        )
        return JSONResponse(failure, status_code=503)


@app.get("/tickets")
async def list_tickets() -> dict[str, Any]:
    return {"tickets": list(state.get("tickets", {}).values())}


@app.post("/tickets/{incident_id}/approve")
async def approve_ticket_action(incident_id: str, request: TicketUpdateRequest) -> dict[str, Any]:
    ticket = state.setdefault("tickets", {}).get(incident_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Support handoff not found")
    investigations_payload = await fetch_json(f"{OPSAI_AGENT_URL}/api/investigations")
    investigations = investigations_payload.get("investigations", []) if investigations_payload else []
    investigation = next(
        (item for item in investigations if str(item.get("incident_id")) == incident_id and item.get("status") == "COMPLETED"),
        None,
    )
    if not investigation:
        raise HTTPException(status_code=409, detail="Completed PulseGuard investigation not found")
    if investigation.get("policy_decision") != "APPROVAL_REQUIRED":
        raise HTTPException(status_code=409, detail="The recommendation is not awaiting approval")
    action = str(investigation.get("action_name") or "")
    if action not in {"drain_payment_node", "restart_payment_node"}:
        raise HTTPException(status_code=409, detail=f"Approval execution is not implemented for {action}")
    parameters = investigation.get("action_parameters") or {}
    node = str(parameters.get("node") or "")
    if not node:
        raise HTTPException(status_code=409, detail="The recommendation does not identify a target node")
    router = await fetch_json(f"{PAYMENT_ROUTER_URL}/nodes")
    nodes = router.get("nodes", [])
    target = next((item for item in nodes if item.get("nodeId") == node), None)
    healthy_peers = [item for item in nodes if item.get("nodeId") != node and item.get("status") == "active"]
    if not target:
        raise HTTPException(status_code=404, detail=f"Router node {node} was not found")
    if len(healthy_peers) < 2:
        raise HTTPException(status_code=409, detail="Approval blocked: fewer than two active peer nodes remain")

    if action == "drain_payment_node":
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(f"{PAYMENT_ROUTER_URL}/nodes/{node}/drain")
            response.raise_for_status()
            result = response.json()
        executor = "opsai-automation:operator-approved-router-action"
    else:
        node_url = PAYMENT_NODE_URLS.get(node)
        if not node_url:
            raise HTTPException(status_code=404, detail=f"Payment node URL is not configured: {node}")
        diagnostics_before = await fetch_json(f"{node_url}/admin/diagnostics")
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"{node_url}/admin/restart",
                json={"reason": "operator_approved_restart"},
            )
            response.raise_for_status()
            result = response.json()
        diagnostics_after = await fetch_json(f"{node_url}/admin/diagnostics")
        result = {
            "restart": result,
            "before": diagnostics_before,
            "after": diagnostics_after,
            "verification": {
                "faultCleared": diagnostics_after.get("faultMode") == "none",
                "acceptingPayments": diagnostics_after.get("acceptingPayments") is True,
                "restartGenerationIncreased": int(diagnostics_after.get("restartGeneration") or 0) > int(diagnostics_before.get("restartGeneration") or 0),
            },
        }
        executor = "opsai-automation:operator-approved-bounded-application-restart"
    remediation = {
        "action": action,
        "incidentId": incident_id,
        "target": node,
        "executed": True,
        "approvedBy": request.actor,
        "approvedAt": iso_now(),
        "executor": executor,
        "executionResult": result,
        "actionSucceeded": bool(
            action == "drain_payment_node"
            or (
                action == "restart_payment_node"
                and (result.get("verification") or {}).get("faultCleared")
                and (result.get("verification") or {}).get("acceptingPayments")
                and (result.get("verification") or {}).get("restartGenerationIncreased")
            )
        ),
        "verificationPassed": False,
        "repairOutcome": "OPERATOR_ACTION_COMPLETED",
    }
    state.setdefault("remediations", {})[incident_id] = remediation
    ticket["status"] = "ACTION_COMPLETED"
    ticket["updatedAt"] = iso_now()
    ticket["approvedBy"] = request.actor
    ticket["approvedAt"] = iso_now()
    await persist_state()
    action_description = (
        f"{node} was removed from new routing"
        if action == "drain_payment_node"
        else f"{node} completed a bounded application-worker restart"
    )
    await append_activity(
        "OPERATOR_ACTION_EXECUTED",
        "warning",
        f"{action} executed after operator approval",
        f"{action_description} after approval by {request.actor}. PulseGuard is verifying recovery.",
        incident_id,
        {"ticket": ticket, "remediation": remediation},
    )
    try:
        await core_request("POST", f"/api/external/incidents/{incident_id}/events", {
            "eventType": "OPERATOR_ACTION_EXECUTED",
            "actor": request.actor,
            "message": f"Approved and executed {action} for {node}.",
            "details": remediation,
        })
    except Exception:
        pass
    return {"status": "ACTION_COMPLETED", "ticket": ticket, "remediation": remediation}


@app.post("/tickets/{incident_id}/reject")
async def reject_ticket_action(incident_id: str, request: TicketUpdateRequest) -> dict[str, Any]:
    ticket = state.setdefault("tickets", {}).get(incident_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Support handoff not found")
    ticket["status"] = "REJECTED"
    ticket["updatedAt"] = iso_now()
    ticket["rejectedBy"] = request.actor
    ticket["rejectionNote"] = request.note
    await persist_state()
    await append_activity(
        "OPERATOR_ACTION_REJECTED",
        "warning",
        "Recommended action rejected",
        f"The operator rejected the recommended action. Incident remains assigned to {ticket.get('primaryQueue')}.",
        incident_id,
        {"ticket": ticket},
    )
    return ticket


@app.post("/tickets/{incident_id}/acknowledge")
async def acknowledge_ticket(incident_id: str, request: TicketUpdateRequest) -> dict[str, Any]:
    ticket = state.get("tickets", {}).get(incident_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Support handoff not found")
    ticket["status"] = "ACKNOWLEDGED"
    ticket["acknowledgedBy"] = request.actor
    ticket["acknowledgedAt"] = iso_now()
    ticket["updatedAt"] = iso_now()
    ticket["note"] = request.note
    await persist_state()
    await append_activity(
        "SUPPORT_ACKNOWLEDGED",
        "info",
        f"{ticket['primaryQueue']} acknowledged the incident",
        f"Acknowledged by {request.actor}.",
        incident_id,
        {"ticket": ticket},
    )
    return ticket


@app.post("/tickets/{incident_id}/reassign")
async def reassign_ticket(incident_id: str, request: TicketUpdateRequest) -> dict[str, Any]:
    ticket = state.get("tickets", {}).get(incident_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Support handoff not found")
    if not request.queue:
        raise HTTPException(status_code=400, detail="queue is required")
    previous = ticket["primaryQueue"]
    ticket["primaryQueue"] = request.queue
    ticket["status"] = "ASSIGNED"
    ticket["updatedAt"] = iso_now()
    ticket["reassignedBy"] = request.actor
    ticket["note"] = request.note
    await persist_state()
    await append_activity(
        "SUPPORT_REASSIGNED",
        "warning",
        "Support assignment changed",
        f"Reassigned from {previous} to {request.queue} by {request.actor}.",
        incident_id,
        {"ticket": ticket},
    )
    return ticket


@app.post("/tickets/{incident_id}/escalate")
async def escalate_ticket(incident_id: str, request: TicketUpdateRequest) -> dict[str, Any]:
    ticket = state.get("tickets", {}).get(incident_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Support handoff not found")
    ticket["primaryQueue"] = "Application Engineering L3"
    ticket["status"] = "ESCALATED_TO_L3"
    ticket["updatedAt"] = iso_now()
    ticket["escalatedBy"] = request.actor
    ticket["note"] = request.note
    await persist_state()
    await append_activity(
        "SUPPORT_ESCALATED",
        "critical",
        "Incident escalated to Application Engineering L3",
        f"Escalated by {request.actor}.",
        incident_id,
        {"ticket": ticket},
    )
    return ticket


WIDGET_JS = r'''
(() => {
  if (window.__opsaiLiveActivityLoaded) return;
  window.__opsaiLiveActivityLoaded = true;
  const BASE = new URL(document.currentScript.src, window.location.href).origin;
  const esc = value => String(value ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
  const style = document.createElement('style');
  style.textContent = `
  #opsai-live-launcher{position:fixed;right:18px;bottom:18px;z-index:2147483000;width:390px;max-width:calc(100vw - 24px);background:#0b1728;color:#e5edf7;border:1px solid #36516f;border-radius:14px;box-shadow:0 18px 55px rgba(0,0,0,.45);font-family:Inter,Segoe UI,Arial,sans-serif;overflow:hidden}
  #opsai-live-launcher *{box-sizing:border-box}#opsai-live-head{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:#10233a;cursor:pointer}.opsai-live-title{font-weight:700}.opsai-live-dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#22c55e;margin-right:8px;box-shadow:0 0 0 4px rgba(34,197,94,.15)}
  #opsai-live-count{font-size:12px;background:#1e3a5f;padding:4px 8px;border-radius:999px}#opsai-live-body{display:none;max-height:72vh;overflow:hidden}#opsai-live-launcher.open #opsai-live-body{display:block}.opsai-live-kpis{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:10px;background:#0a1422}.opsai-live-kpi{background:#132238;border:1px solid #263a54;border-radius:9px;padding:7px}.opsai-live-kpi b{display:block;font-size:18px}.opsai-live-kpi span{font-size:10px;color:#9fb1c7;text-transform:uppercase}
  .opsai-live-tabs{display:flex;gap:5px;padding:8px 10px;border-top:1px solid #20334b;border-bottom:1px solid #20334b;overflow:auto}.opsai-live-tabs button{border:0;border-radius:999px;background:#1a2b43;color:#c7d5e7;font-size:11px;padding:5px 9px;white-space:nowrap;cursor:pointer}.opsai-live-tabs button.active{background:#0369a1;color:white}
  #opsai-live-feed{max-height:42vh;overflow:auto;padding:8px 10px}.opsai-live-event{border-left:3px solid #38bdf8;background:#101e31;border-radius:8px;margin:7px 0;padding:9px}.opsai-live-event.critical{border-left-color:#ef4444}.opsai-live-event.warning{border-left-color:#f59e0b}.opsai-live-event.success{border-left-color:#22c55e}.opsai-live-event h4{margin:0 0 4px;font-size:13px}.opsai-live-event p{margin:0;color:#c3d0e1;font-size:12px;line-height:1.35}.opsai-live-meta{margin-top:5px;color:#7f95ad;font-size:10px}.opsai-live-actions{display:flex;gap:6px;margin-top:7px}.opsai-live-actions button{border:0;border-radius:6px;background:#0369a1;color:white;font-size:10px;padding:5px 7px;cursor:pointer}
  .opsai-live-footer{padding:8px 10px;color:#8499b1;font-size:10px;border-top:1px solid #20334b}.opsai-live-latest{padding:0 14px 11px;color:#b7c7da;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}@media(max-width:500px){#opsai-live-launcher{right:8px;bottom:8px;width:calc(100vw - 16px)}}`;
  document.head.appendChild(style);
  const host = document.createElement('div');
  host.id = 'opsai-live-launcher';
  host.innerHTML = `<div id="opsai-live-head"><div><span class="opsai-live-dot"></span><span class="opsai-live-title">PulseGuard Live Activity</span></div><span id="opsai-live-count">Connecting</span></div><div class="opsai-live-latest" id="opsai-live-latest">Waiting for operational events...</div><div id="opsai-live-body"><div class="opsai-live-kpis" id="opsai-live-kpis"></div><div class="opsai-live-tabs" id="opsai-live-tabs"></div><div id="opsai-live-feed"></div><div class="opsai-live-footer">PulseGuard separates recommendation, governance, action execution, support handoff and verified repair outcome.</div></div>`;
  document.body.appendChild(host);
  let events = [], summary = {}, filter = 'all', userReading = false;
  const filters = [['all','All'],['active','Active'],['approval','Needs approval'],['support','Assigned'],['repair','Auto-repaired'],['resolved','Resolved']];
  document.getElementById('opsai-live-head').onclick = () => host.classList.toggle('open');
  const tabs = document.getElementById('opsai-live-tabs');
  tabs.innerHTML = filters.map(([id,label])=>`<button data-filter="${id}">${label}</button>`).join('');
  tabs.querySelectorAll('button').forEach(button => button.onclick = () => {filter=button.dataset.filter;render();});
  const feed = document.getElementById('opsai-live-feed');
  feed.addEventListener('scroll',()=>{userReading = feed.scrollTop + feed.clientHeight < feed.scrollHeight - 30;});
  function matches(event){
    const stage=String(event.stage||'');
    if(filter==='all')return true;
    if(filter==='active')return !stage.includes('RESOLVED');
    if(filter==='approval')return stage==='ACTION_NOT_EXECUTED' && String(event.message||'').includes('APPROVAL_REQUIRED');
    if(filter==='support')return stage.startsWith('SUPPORT_');
    if(filter==='repair')return stage==='INCIDENT_RESOLVED' && String(event.message||'').includes('AUTO_REPAIRED');
    if(filter==='resolved')return stage==='INCIDENT_RESOLVED';
    return true;
  }
  function render(){
    document.querySelectorAll('#opsai-live-tabs button').forEach(b=>b.classList.toggle('active',b.dataset.filter===filter));
    document.getElementById('opsai-live-count').textContent=`${Number(summary.activeIncidents||0)} active | ${Number(summary.assignedToSupport||0)} assigned`;
    document.getElementById('opsai-live-latest').textContent=(summary.latestActivity&&summary.latestActivity.title)||'No new operational activity.';
    const kpis=[['Active',summary.activeIncidents],['Auto-repaired',summary.autoRepaired],['Awaiting approval',summary.awaitingApproval],['Assigned',summary.assignedToSupport],['Unacknowledged',summary.unacknowledged],['Resolved',summary.resolved]];
    document.getElementById('opsai-live-kpis').innerHTML=kpis.map(([label,value])=>`<div class="opsai-live-kpi"><b>${Number(value||0)}</b><span>${esc(label)}</span></div>`).join('');
    const visible=events.filter(matches).slice(-100).reverse();
    feed.innerHTML=visible.length?visible.map(event=>{
      const details=event.details||{};const ticket=details.ticket;const incident=event.incidentId;
      let actions='';
      if(ticket&&['ASSIGNED','ACKNOWLEDGED','IN_PROGRESS'].includes(ticket.status)){const approval=ticket.handoff&&ticket.handoff.governanceDecision==='APPROVAL_REQUIRED'?`<button data-approve="${esc(incident)}">Approve action</button><button data-reject="${esc(incident)}">Reject</button>`:'';actions=`<div class="opsai-live-actions"><button data-ack="${esc(incident)}">Acknowledge ${esc(ticket.primaryQueue)}</button>${approval}<button data-escalate="${esc(incident)}">Escalate L3</button></div>`;}
      return `<div class="opsai-live-event ${esc(event.severity||'info')}"><h4>${esc(event.title)}</h4><p>${esc(event.message)}</p><div class="opsai-live-meta">${new Date(event.timestamp).toLocaleTimeString()} | ${esc(event.stage)}${incident?' | '+esc(incident.slice(0,8)):''}</div>${actions}</div>`;
    }).join(''):'<div class="opsai-live-event"><p>No events match this filter.</p></div>';
    feed.querySelectorAll('[data-ack]').forEach(button=>button.onclick=()=>ticketAction(button.dataset.ack,'acknowledge'));
    feed.querySelectorAll('[data-approve]').forEach(button=>button.onclick=()=>{if(confirm('Approve and execute the governed operational action?'))ticketAction(button.dataset.approve,'approve')});
    feed.querySelectorAll('[data-reject]').forEach(button=>button.onclick=()=>ticketAction(button.dataset.reject,'reject'));
    feed.querySelectorAll('[data-escalate]').forEach(button=>button.onclick=()=>ticketAction(button.dataset.escalate,'escalate'));
    if(!userReading) feed.scrollTop=0;
  }
  async function ticketAction(id, action){
    await fetch(`${BASE}/tickets/${id}/${action}`,{method:'POST',credentials:'include',headers:{'Content-Type':'application/json'},body:JSON.stringify({actor:'demo-operator',note:'Actioned from PulseGuard Live Activity'})});
    await refresh();
  }
  async function refresh(){
    try{
      const [s,a]=await Promise.all([fetch(`${BASE}/summary`,{credentials:'include'}).then(r=>r.json()),fetch(`${BASE}/activity?limit=250`,{credentials:'include'}).then(r=>r.json())]);
      summary=s;events=a.activities||[];render();
      window.dispatchEvent(new CustomEvent('opsai-summary-updated',{detail:summary}));
    }catch(error){document.getElementById('opsai-live-count').textContent='Activity unavailable';}
  }
  try{
    const source=new EventSource(`${BASE}/activity/stream`,{withCredentials:true});
    source.addEventListener('opsai-activity',event=>{try{events.push(JSON.parse(event.data));events=events.slice(-300);refresh();}catch(_){}});
  }catch(_){ }
  refresh();setInterval(refresh,5000);
})();
'''


@app.get("/widget.js")
async def widget_js() -> PlainTextResponse:
    return PlainTextResponse(WIDGET_JS, media_type="application/javascript", headers={"Cache-Control": "no-store"})


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>PulseGuard Automation</title><style>body{font-family:Segoe UI,Arial;background:#08111f;color:#e5edf7;margin:0}.wrap{max-width:1100px;margin:auto;padding:28px}.card{background:#0e1b2d;border:1px solid #263c58;border-radius:14px;padding:18px;margin:14px 0}button{background:#0369a1;color:white;border:0;border-radius:8px;padding:9px 12px;margin:4px;cursor:pointer}pre{white-space:pre-wrap;background:#07101c;padding:12px;border-radius:8px}</style></head><body><div class='wrap'><h1>PulseGuard Automation, Activity and Support</h1><p>Bounded auto-repair scenarios, support queue handoffs and the shared live activity stream.</p><div class='card'><h2>Automatic repair scenarios</h2><button onclick="post('/scenarios/disk-pressure?target_percent=92')">Fill disk to 92%</button><button onclick="post('/scenarios/disk-pressure?target_percent=92&cleanup_insufficient=true')">Disk cleanup insufficient</button><button onclick="post('/scenarios/certificate-expiring?seconds=300')">Certificate expires in 5 minutes</button><button onclick="post('/scenarios/certificate-renewal-failure?seconds=300')">Certificate renewal failure</button><button onclick="post('/scenarios/reset')">Reset</button></div><div class='card'><h2>Current state</h2><pre id='state'>Loading...</pre></div></div><script>async function post(path){await fetch(path,{method:'POST'});load()}async function load(){document.getElementById('state').textContent=JSON.stringify(await fetch('/state').then(r=>r.json()),null,2)}load();setInterval(load,5000)</script><script src='/widget.js'></script></body></html>"""
