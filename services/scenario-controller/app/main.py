from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from prometheus_client import Counter, Gauge, make_asgi_app


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.upper().startswith("CHANGE_ME_"):
        raise RuntimeError(f"Required environment variable {name} is not configured.")
    return value


SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.6.0")
TOXIPROXY_API_URL = os.getenv("TOXIPROXY_API_URL", "http://toxiproxy:8474").rstrip("/")
CORRUPTION_ADAPTER_URL = os.getenv("CORRUPTION_ADAPTER_URL", "http://corruption-adapter:8000").rstrip("/")
TRAFFIC_PROXY_NAME = os.getenv("TRAFFIC_PROXY_NAME", "traffic_profile")
TRAFFIC_PROXY_LISTEN = os.getenv("TRAFFIC_PROXY_LISTEN", "0.0.0.0:8666")
TRAFFIC_PROXY_UPSTREAM = os.getenv("TRAFFIC_PROXY_UPSTREAM", "corruption-adapter:8000")
PAYMENT_PROXY_NAME = os.getenv("PAYMENT_PROXY_NAME", "payment_node_3")
PAYMENT_PROXY_LISTEN = os.getenv("PAYMENT_PROXY_LISTEN", "0.0.0.0:8667")
PAYMENT_PROXY_UPSTREAM = os.getenv("PAYMENT_PROXY_UPSTREAM", "payment-node-3:8000")
PAYMENT_NODE_URLS_RAW = os.getenv(
    "PAYMENT_NODE_URLS",
    "payment-node-1=http://payment-node-1:8000,payment-node-2=http://payment-node-2:8000,payment-node-3=http://payment-node-3:8000",
)


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
OPSAI_CORE_URL = os.getenv("OPSAI_CORE_URL", "http://opsai-core:8000").rstrip("/")
OPSAI_AGENT_URL = os.getenv("OPSAI_AGENT_URL", "http://opsai-agent:8000").rstrip("/")
PAYMENT_ROUTER_URL = os.getenv("PAYMENT_ROUTER_URL", "http://payment-router:8000").rstrip("/")
OPSAI_AUTOMATION_URL = os.getenv("OPSAI_AUTOMATION_URL", "http://opsai-automation:8000").rstrip("/")
AUTOMATION_API_TOKEN = require_env("AUTOMATION_API_TOKEN")
CHECKOUT_SERVICE_URL = os.getenv("CHECKOUT_SERVICE_URL", "http://checkout-service:8000").rstrip("/")
EXTERNAL_AUTH_SERVICE_URL = os.getenv(
    "EXTERNAL_AUTH_SERVICE_URL",
    "http://external-auth-service:8000",
).rstrip("/")
OPSAI_PREDICTOR_URL = os.getenv(
    "OPSAI_PREDICTOR_URL",
    "http://opsai-predictor:8000",
).rstrip("/")
TEST_RUN_DATA_FILE = Path(os.getenv("TEST_RUN_DATA_FILE", "/data/test-runs.json"))
TEST_HISTORY_LIMIT = int(os.getenv("TEST_HISTORY_LIMIT", "20"))
TEST_PRECONDITION_TIMEOUT_SECONDS = int(os.getenv("TEST_PRECONDITION_TIMEOUT_SECONDS", "120"))
TEST_INVESTIGATION_TIMEOUT_SECONDS = int(os.getenv("TEST_INVESTIGATION_TIMEOUT_SECONDS", "120"))
TEST_RECOVERY_TIMEOUT_SECONDS = int(os.getenv("TEST_RECOVERY_TIMEOUT_SECONDS", "120"))
TEST_POLL_SECONDS = float(os.getenv("TEST_POLL_SECONDS", "3"))
TEST_INCIDENT_SETTLE_SECONDS = int(os.getenv("TEST_INCIDENT_SETTLE_SECONDS", "10"))
TEST_BASELINE_STABLE_EVALUATIONS = int(os.getenv("TEST_BASELINE_STABLE_EVALUATIONS", "4"))
TEST_BASELINE_CHECKOUT_FAILURE_PERCENT = float(os.getenv("TEST_BASELINE_CHECKOUT_FAILURE_PERCENT", "1"))
TEST_BASELINE_RETRY_RATE = float(os.getenv("TEST_BASELINE_RETRY_RATE", "0.01"))

SCENARIO_ACTIVATIONS = Counter(
    "opsai_scenario_activations_total",
    "Scenario activations by scenario name.",
    ["scenario"],
)
ACTIVE_SCENARIO = Gauge(
    "opsai_active_scenario_info",
    "One for the active scenario label and zero otherwise.",
    ["scenario"],
)
TEST_RUNS = Counter(
    "opsai_scenario_test_runs_total",
    "Random disturbance test runs by final outcome.",
    ["outcome"],
)
TEST_STEPS = Counter(
    "opsai_scenario_test_steps_total",
    "Random disturbance test steps by disturbance and outcome.",
    ["disturbance", "outcome"],
)
ACTIVE_TEST_RUN = Gauge(
    "opsai_scenario_test_run_active",
    "One while a random disturbance test run is active.",
)

app = FastAPI(title="PulseGuard Scenario Controller", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())
ready = False
background_scenario_task: asyncio.Task[None] | None = None
test_run_task: asyncio.Task[None] | None = None
test_run_stop_requested = False
test_runs: list[dict[str, Any]] = []
current_state: dict[str, Any] = {
    "scenario": "starting",
    "activatedAt": None,
    "details": {},
}
known_scenarios = [
    "none",
    "traffic_spike",
    "capacity_failover_scale",
    "payment_latency",
    "payment_timeout",
    "payment_reset",
    "node_offline",
    "intermittent_network",
    "node_flapping",
    "shared_dependency_outage",
    "external_latency",
    "external_timeout",
    "payload_corruption",
    "disk_pressure",
    "certificate_expiring",
    "certificate_renewal_failure",
    "external_auth_failure",
    "payment_node_hung",
    "predictive_disk_growth",
    "predictive_node_degradation",
    "predictive_capacity_risk",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_log(level: str, event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": utc_now(),
                "level": level,
                "service": "scenario-controller",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def update_state(scenario: str, **details: object) -> None:
    current_state.clear()
    current_state.update({"scenario": scenario, "activatedAt": utc_now(), "details": details})
    for item in known_scenarios:
        ACTIVE_SCENARIO.labels(scenario=item).set(1 if item == scenario else 0)
    SCENARIO_ACTIVATIONS.labels(scenario=scenario).inc()
    emit_log("WARNING" if scenario != "none" else "INFO", "scenario_changed", scenario=scenario, **details)


def update_state_details(**details: object) -> None:
    current_state.setdefault("details", {}).update(details)


async def wait_for_toxiproxy() -> None:
    delay = 1.0
    for _ in range(60):
        try:
            async with httpx.AsyncClient(timeout=3) as client:
                response = await client.get(f"{TOXIPROXY_API_URL}/version")
                response.raise_for_status()
                return
        except httpx.HTTPError:
            await asyncio.sleep(delay)
            delay = min(5.0, delay * 1.2)
    raise RuntimeError("Toxiproxy did not become ready.")


async def populate_proxies() -> None:
    proxies = [
        {
            "name": TRAFFIC_PROXY_NAME,
            "listen": TRAFFIC_PROXY_LISTEN,
            "upstream": TRAFFIC_PROXY_UPSTREAM,
            "enabled": True,
        },
        {
            "name": PAYMENT_PROXY_NAME,
            "listen": PAYMENT_PROXY_LISTEN,
            "upstream": PAYMENT_PROXY_UPSTREAM,
            "enabled": True,
        },
    ]
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(f"{TOXIPROXY_API_URL}/populate", json=proxies)
        response.raise_for_status()


async def reset_toxiproxy() -> None:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(f"{TOXIPROXY_API_URL}/reset")
        response.raise_for_status()
    await populate_proxies()


async def set_corruption_mode(mode: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(f"{CORRUPTION_ADAPTER_URL}/mode", json={"mode": mode})
        response.raise_for_status()
        return response.json()


async def set_traffic_override(multiplier: float, duration_seconds: int) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(
            f"{CORRUPTION_ADAPTER_URL}/traffic-override",
            json={"multiplier": multiplier, "durationSeconds": duration_seconds},
        )
        response.raise_for_status()
        return response.json()


async def reset_traffic_override() -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(f"{CORRUPTION_ADAPTER_URL}/traffic-override/reset")
        response.raise_for_status()
        return response.json()


async def get_traffic_override() -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.get(f"{CORRUPTION_ADAPTER_URL}/traffic-override")
        response.raise_for_status()
        return response.json()


async def set_node_fault(node_id: str, mode: str) -> dict[str, Any]:
    node_url = PAYMENT_NODE_URLS.get(node_id)
    if node_url is None:
        raise HTTPException(status_code=404, detail=f"Unknown payment node: {node_id}")
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(f"{node_url}/admin/fault", json={"mode": mode})
        response.raise_for_status()
        return response.json()


async def reset_all_node_faults() -> None:
    async with httpx.AsyncClient(timeout=5) as client:
        responses = await asyncio.gather(
            *[client.post(f"{url}/admin/fault/reset") for url in PAYMENT_NODE_URLS.values()],
            return_exceptions=True,
        )
    failures = [str(item) for item in responses if isinstance(item, Exception)]
    http_failures = [
        f"HTTP {item.status_code}: {item.text[:200]}"
        for item in responses
        if isinstance(item, httpx.Response) and item.is_error
    ]
    if failures or http_failures:
        raise RuntimeError(f"Failed to reset node faults: {failures + http_failures}")


async def set_node_capacity(
    node_id: str,
    units: int,
    pressure_ms: int = 0,
    reason: str = "scenario_pressure",
) -> dict[str, Any]:
    node_url = PAYMENT_NODE_URLS.get(node_id)
    if node_url is None:
        raise HTTPException(status_code=404, detail=f"Unknown payment node: {node_id}")
    async with httpx.AsyncClient(timeout=8) as client:
        response = await client.post(
            f"{node_url}/admin/capacity",
            json={"units": units, "pressureMs": pressure_ms, "reason": reason},
        )
        response.raise_for_status()
        return response.json()


async def reset_all_node_capacity() -> None:
    async with httpx.AsyncClient(timeout=8) as client:
        responses = await asyncio.gather(
            *[client.post(f"{url}/admin/capacity/reset") for url in PAYMENT_NODE_URLS.values()],
            return_exceptions=True,
        )
    failures = [str(item) for item in responses if isinstance(item, Exception)]
    http_failures = [
        f"HTTP {item.status_code}: {item.text[:200]}"
        for item in responses
        if isinstance(item, httpx.Response) and item.is_error
    ]
    if failures or http_failures:
        raise RuntimeError(f"Failed to reset node capacity profiles: {failures + http_failures}")


async def restore_all_router_nodes() -> None:
    async with httpx.AsyncClient(timeout=5) as client:
        responses = await asyncio.gather(
            *[client.post(f"{PAYMENT_ROUTER_URL}/nodes/{node_id}/restore") for node_id in PAYMENT_NODE_URLS],
            return_exceptions=True,
        )
    failures = [str(item) for item in responses if isinstance(item, Exception)]
    http_failures = [
        f"HTTP {item.status_code}: {item.text[:200]}"
        for item in responses
        if isinstance(item, httpx.Response) and item.is_error
    ]
    if failures or http_failures:
        emit_log("WARNING", "router_restore_failed", errors=failures + http_failures)


async def get_node_diagnostics() -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=5) as client:
        async def fetch(node_id: str, url: str) -> dict[str, Any]:
            try:
                response = await client.get(f"{url}/admin/diagnostics")
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as exc:
                return {"nodeId": node_id, "processStatus": "unreachable", "error": str(exc)}

        return await asyncio.gather(*[fetch(node_id, url) for node_id, url in PAYMENT_NODE_URLS.items()])


async def add_toxic(
    proxy_name: str,
    toxic_name: str,
    toxic_type: str,
    stream: str,
    attributes: dict[str, object],
    toxicity: float = 1.0,
) -> None:
    payload = {
        "name": toxic_name,
        "type": toxic_type,
        "stream": stream,
        "toxicity": toxicity,
        "attributes": attributes,
    }
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(
            f"{TOXIPROXY_API_URL}/proxies/{proxy_name}/toxics", json=payload
        )
        response.raise_for_status()


async def cancel_background_scenario() -> None:
    global background_scenario_task
    task = background_scenario_task
    if task is None:
        return
    background_scenario_task = None
    if task is asyncio.current_task():
        return
    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def automation_post(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"X-OpsAI-Automation-Token": AUTOMATION_API_TOKEN}
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{OPSAI_AUTOMATION_URL}{path}", params=params, headers=headers)
        response.raise_for_status()
        return response.json()


async def automation_get(path: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(f"{OPSAI_AUTOMATION_URL}{path}")
        response.raise_for_status()
        return response.json()


async def synchronise_external_auth_token() -> dict[str, Any]:
    headers = {"X-OpsAI-Automation-Token": AUTOMATION_API_TOKEN}
    async with httpx.AsyncClient(timeout=15) as client:
        token_response = await client.post(
            f"{EXTERNAL_AUTH_SERVICE_URL}/admin/current-token",
            headers=headers,
        )
        token_response.raise_for_status()
        token_payload = token_response.json()
        checkout_response = await client.post(
            f"{CHECKOUT_SERVICE_URL}/admin/external-auth/token",
            headers=headers,
            json={
                "token": token_payload.get("token"),
                "tokenGeneration": token_payload.get("tokenGeneration", 1),
                "reason": "scenario_reset",
            },
        )
        checkout_response.raise_for_status()
        checkout_payload = checkout_response.json()
    return {
        "externalService": token_payload.get("service"),
        "tokenFingerprint": token_payload.get("tokenFingerprint"),
        "tokenGeneration": token_payload.get("tokenGeneration"),
        "checkout": checkout_payload,
    }


async def invalidate_external_auth_token() -> dict[str, Any]:
    headers = {"X-OpsAI-Automation-Token": AUTOMATION_API_TOKEN}
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(
            f"{EXTERNAL_AUTH_SERVICE_URL}/admin/invalidate-client-token",
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
    return {
        "service": payload.get("service"),
        "activeTokenFingerprint": payload.get("tokenFingerprint"),
        "activeTokenGeneration": payload.get("tokenGeneration"),
        "secretReturnedToScenarioController": False,
    }


async def predictor_reset() -> None:
    headers = {"X-OpsAI-Automation-Token": AUTOMATION_API_TOKEN}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"{OPSAI_PREDICTOR_URL}/admin/reset",
                headers=headers,
            )
            response.raise_for_status()
    except Exception as exc:
        emit_log("WARNING", "predictor_reset_failed", error=str(exc))


async def run_predictive_node_degradation(
    node_id: str,
    start_pressure_ms: int,
    end_pressure_ms: int,
    duration_seconds: int,
    capacity_risk: bool = False,
) -> None:
    global background_scenario_task
    steps = max(10, int(duration_seconds / 6))
    try:
        if capacity_risk:
            await set_node_fault("payment-node-3", "unavailable")
            await set_traffic_override(3.0, duration_seconds + 60)
        for index in range(steps + 1):
            pressure = round(
                start_pressure_ms
                + (end_pressure_ms - start_pressure_ms) * index / steps
            )
            if capacity_risk:
                await asyncio.gather(
                    set_node_capacity(
                        "payment-node-1",
                        1,
                        pressure,
                        "predictive_capacity_ramp",
                    ),
                    set_node_capacity(
                        "payment-node-2",
                        1,
                        pressure,
                        "predictive_capacity_ramp",
                    ),
                )
                update_state_details(
                    phase="ramping",
                    step=index,
                    steps=steps,
                    pressureMs=pressure,
                    nodes=["payment-node-1", "payment-node-2"],
                )
            else:
                await set_node_capacity(
                    node_id,
                    1,
                    pressure,
                    "predictive_degradation_ramp",
                )
                update_state_details(
                    phase="ramping",
                    step=index,
                    steps=steps,
                    pressureMs=pressure,
                    target=node_id,
                )
            await asyncio.sleep(duration_seconds / steps)
        update_state_details(phase="holding", completedAt=utc_now())
    except asyncio.CancelledError:
        raise
    finally:
        if background_scenario_task is asyncio.current_task():
            background_scenario_task = None


async def clean_before_scenario() -> None:
    await cancel_background_scenario()
    await reset_toxiproxy()
    await set_corruption_mode("none")
    await reset_traffic_override()
    await reset_all_node_faults()
    await reset_all_node_capacity()
    await restore_all_router_nodes()
    try:
        await automation_post("/scenarios/reset")
    except Exception as exc:
        emit_log("WARNING", "automation_reset_failed", error=str(exc))
    try:
        await synchronise_external_auth_token()
    except Exception as exc:
        emit_log("WARNING", "external_auth_reset_failed", error=str(exc))
    await predictor_reset()


async def run_node_flapping(node_id: str, duration_seconds: int, interval_seconds: int) -> None:
    global background_scenario_task
    deadline = time.monotonic() + duration_seconds
    cycle = 0
    try:
        while time.monotonic() < deadline:
            cycle += 1
            await set_node_fault(node_id, "unavailable")
            update_state_details(phase="offline", cycle=cycle, lastTransitionAt=utc_now())
            await asyncio.sleep(min(interval_seconds, max(0.1, deadline - time.monotonic())))
            if time.monotonic() >= deadline:
                break
            await set_node_fault(node_id, "none")
            update_state_details(phase="online", cycle=cycle, lastTransitionAt=utc_now())
            await asyncio.sleep(min(interval_seconds, max(0.1, deadline - time.monotonic())))
    except asyncio.CancelledError:
        raise
    finally:
        try:
            await set_node_fault(node_id, "none")
        except Exception as exc:  # best-effort cleanup; Reset all faults remains available.
            emit_log("ERROR", "flapping_cleanup_failed", node=node_id, error=str(exc))
        if background_scenario_task is asyncio.current_task():
            background_scenario_task = None
        if current_state.get("scenario") == "node_flapping":
            update_state("none", completedScenario="node_flapping", target=node_id)


async def refresh_expiring_state(traffic_override: dict[str, Any]) -> None:
    if current_state.get("scenario") == "traffic_spike" and not traffic_override.get("active"):
        update_state("none", completedScenario="traffic_spike")


TEST_SCENARIOS: dict[str, dict[str, Any]] = {
    "payment_latency": {
        "displayName": "Isolated node-3 latency",
        "description": "Adds 1.2 seconds of latency and jitter to payment-node-3 while peers remain healthy.",
        "desiredIncidentTypes": ["PAYMENT_NODE_LATENCY"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_LATENCY"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 150,
    },
    "payment_timeout": {
        "displayName": "Node-3 hard timeout",
        "description": "Makes node 3 hold calls beyond the router deadline so a true timeout is recorded.",
        "desiredIncidentTypes": ["PAYMENT_NODE_TIMEOUT"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_TIMEOUT"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 90,
    },
    "payment_reset": {
        "displayName": "Node-3 connection reset",
        "description": "Resets every new connection to node 3.",
        "desiredIncidentTypes": ["PAYMENT_NODE_UNAVAILABLE"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_NETWORK_INSTABILITY"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 100,
    },
    "intermittent_network": {
        "displayName": "Intermittent node-3 network",
        "description": "Resets approximately 40 percent of node-3 connections.",
        "desiredIncidentTypes": ["PAYMENT_NODE_NETWORK_INSTABILITY"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_NETWORK_INSTABILITY"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 100,
    },
    "node_offline": {
        "displayName": "Node 3 unavailable",
        "description": "Keeps the process running but makes node 3 reject payment traffic.",
        "desiredIncidentTypes": ["PAYMENT_NODE_UNAVAILABLE"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_UNAVAILABLE"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 100,
    },
    "node_flapping": {
        "displayName": "Node 3 flapping",
        "description": "Alternates node 3 between available and unavailable every four seconds.",
        "desiredIncidentTypes": ["PAYMENT_NODE_FLAPPING"],
        "currentAcceptableIncidentTypes": ["PAYMENT_NODE_FLAPPING"],
        "expectedNode": "payment-node-3",
        "detectionTimeoutSeconds": 110,
    },
    "shared_dependency_outage": {
        "displayName": "Shared payment dependency outage",
        "description": "Makes all payment nodes report the same downstream authorisation failure.",
        "desiredIncidentTypes": ["PAYMENT_SHARED_DEPENDENCY_OUTAGE"],
        "currentAcceptableIncidentTypes": ["PAYMENT_SHARED_DEPENDENCY_OUTAGE"],
        "expectedNode": None,
        "detectionTimeoutSeconds": 100,
    },
    "traffic_spike": {
        "displayName": "Four-times demand spike",
        "description": "Amplifies the current Wikimedia-derived synthetic load by four, capped at the configured safe maximum.",
        "desiredIncidentTypes": ["PAYMENT_FLEET_CAPACITY_DEGRADATION"],
        "currentAcceptableIncidentTypes": [],
        "expectedNode": None,
        "detectionTimeoutSeconds": 80,
        "noIncidentCanPass": True,
    },
    "capacity_failover_scale": {
        "displayName": "Node-3 loss with bounded capacity scale-up",
        "description": "Takes node 3 unavailable, adds a four-times demand surge and controlled capacity pressure on nodes 1 and 2; PulseGuard should scale the two remaining nodes automatically.",
        "desiredIncidentTypes": ["PAYMENT_FLEET_CAPACITY_DEGRADATION"],
        "currentAcceptableIncidentTypes": ["PAYMENT_FLEET_CAPACITY_DEGRADATION"],
        "expectedNode": None,
        "detectionTimeoutSeconds": 150,
        "autoRepairExpected": True,
        "expectedAction": "scale_payment_capacity",
    },
    "disk_pressure": {
        "displayName": "Bounded disk-space pressure",
        "description": "Fills a dedicated synthetic volume to 92 percent; PulseGuard should archive and clean allowlisted content automatically.",
        "desiredIncidentTypes": ["NODE_DISK_PRESSURE"],
        "currentAcceptableIncidentTypes": ["NODE_DISK_PRESSURE"],
        "expectedNode": "opsai-demo-storage",
        "detectionTimeoutSeconds": 90,
        "autoRepairExpected": True,
        "expectedAction": "cleanup_disk_space",
    },
    "certificate_expiring": {
        "displayName": "Certificate expiry predicted",
        "description": "Issues an allowlisted demo certificate expiring in five minutes; PulseGuard should renew and verify it automatically.",
        "desiredIncidentTypes": ["TLS_CERTIFICATE_EXPIRING"],
        "currentAcceptableIncidentTypes": ["TLS_CERTIFICATE_EXPIRING"],
        "expectedNode": "checkout.pulseguard.local",
        "detectionTimeoutSeconds": 90,
        "autoRepairExpected": True,
        "expectedAction": "renew_certificate",
    },
    "certificate_renewal_failure": {
        "displayName": "Certificate renewal failure",
        "description": "Makes renewal fail after an expiring-certificate alert; PulseGuard should create a detailed Network & Platform Support handoff.",
        "desiredIncidentTypes": ["TLS_CERTIFICATE_EXPIRING"],
        "currentAcceptableIncidentTypes": ["TLS_CERTIFICATE_EXPIRING"],
        "expectedNode": "checkout.pulseguard.local",
        "detectionTimeoutSeconds": 90,
        "manualInterventionExpected": True,
        "expectedAction": "renew_certificate",
        "expectedQueue": "Network & Platform Support",
    },
    "external_auth_failure": {
        "displayName": "External partner authentication failure",
        "description": "Rotates the partner service token without updating checkout; PulseGuard should identify repeated 401 failures, refresh the bounded credential and verify recovery.",
        "desiredIncidentTypes": ["EXTERNAL_SERVICE_AUTHENTICATION_FAILURE"],
        "currentAcceptableIncidentTypes": ["EXTERNAL_SERVICE_AUTHENTICATION_FAILURE"],
        "expectedNode": "partner-risk-service",
        "detectionTimeoutSeconds": 100,
        "autoRepairExpected": True,
        "expectedAction": "refresh_external_service_credentials",
    },
}



def load_test_runs() -> None:
    global test_runs
    try:
        if not TEST_RUN_DATA_FILE.exists():
            test_runs = []
            return
        payload = json.loads(TEST_RUN_DATA_FILE.read_text(encoding="utf-8"))
        loaded = payload if isinstance(payload, list) else payload.get("runs", [])
        test_runs = [item for item in loaded if isinstance(item, dict)][-TEST_HISTORY_LIMIT:]
        for run in test_runs:
            if run.get("status") in {"QUEUED", "RUNNING", "STOPPING"}:
                run["status"] = "INTERRUPTED"
                run["completedAt"] = utc_now()
                run["message"] = "Scenario Controller restarted before this run completed."
    except Exception as exc:
        test_runs = []
        emit_log("ERROR", "test_history_load_failed", error=str(exc))


def save_test_runs() -> None:
    try:
        TEST_RUN_DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary = TEST_RUN_DATA_FILE.with_suffix(".tmp")
        temporary.write_text(json.dumps(test_runs[-TEST_HISTORY_LIMIT:], indent=2), encoding="utf-8")
        temporary.replace(TEST_RUN_DATA_FILE)
    except Exception as exc:
        emit_log("ERROR", "test_history_save_failed", error=str(exc))


def find_test_run(run_id: str) -> dict[str, Any]:
    run = next((item for item in test_runs if item.get("id") == run_id), None)
    if run is None:
        raise HTTPException(status_code=404, detail="Test run not found")
    return run


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def seconds_between(start: Any, end: Any) -> float | None:
    start_dt = parse_timestamp(start)
    end_dt = parse_timestamp(end)
    if start_dt is None or end_dt is None:
        return None
    return round((end_dt - start_dt).total_seconds(), 3)


def recalculate_run_summary(run: dict[str, Any]) -> None:
    steps = run.get("steps", [])
    completed = [step for step in steps if step.get("status") in {"COMPLETED", "FAILED", "STOPPED"}]
    run["summary"] = {
        "planned": len(steps),
        "completed": len(completed),
        "injected": sum(bool(step.get("injectionSucceeded")) for step in steps),
        "detected": sum(bool(step.get("detected")) for step in steps),
        "investigated": sum(bool(step.get("investigated")) for step in steps),
        "realAi": sum(bool(step.get("realAi")) for step in steps),
        "correctlyClassified": sum(step.get("classificationStatus") == "CORRECT" for step in steps),
        "functionalFallback": sum(step.get("classificationStatus") in {"FUNCTIONAL_FALLBACK", "CORRECT_WITH_DUPLICATE_NOISE"} for step in steps),
        "resolved": sum(bool(step.get("resolved")) for step in steps if step.get("detected")),
        "passed": sum(step.get("outcome") == "PASSED" for step in steps),
        "partial": sum(step.get("outcome") == "PARTIAL" for step in steps),
        "gaps": sum(step.get("outcome") == "GAP" for step in steps),
        "failed": sum(step.get("outcome") == "FAILED" for step in steps),
        "stopped": sum(step.get("outcome") == "STOPPED" for step in steps),
        "incidentsRaised": sum(int(step.get("incidentCount") or 0) for step in steps),
        "investigationsCompleted": sum(int(step.get("completedInvestigationCount") or 0) for step in steps),
        "aiDecisions": sum(len(step.get("investigations", [])) for step in steps),
        "actionsExecuted": sum(int(step.get("executedActionCount") or 0) for step in steps),
        "actionsSucceeded": sum(int(step.get("succeededActionCount") or 0) for step in steps),
        "approvalRequired": sum(int(step.get("approvalRequiredCount") or 0) for step in steps),
        "actionsBlocked": sum(int(step.get("blockedActionCount") or 0) for step in steps),
        "resiliencePasses": sum(step.get("classificationStatus") == "RESILIENCE_PASS" for step in steps),
        "autoRepaired": sum(step.get("repairOutcome") == "AUTO_REPAIRED" for step in steps),
        "recoveredAfterTestCleanup": sum(step.get("repairOutcome") == "RECOVERED_AFTER_TEST_CLEANUP" for step in steps),
        "assignedToSupport": sum(bool(step.get("assignedSupportQueue")) for step in steps),
        "correctQueueAssignments": sum(step.get("queueRoutingStatus") == "CORRECT" for step in steps),
        "manualInterventionRequired": sum(step.get("repairOutcome") == "MANUAL_INTERVENTION_REQUIRED" for step in steps),
    }


def persist_run(run: dict[str, Any]) -> None:
    run["updatedAt"] = utc_now()
    recalculate_run_summary(run)
    save_test_runs()


async def core_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(f"{OPSAI_CORE_URL}{path}", params=params)
        response.raise_for_status()
        return response.json()


async def fetch_incidents(status: str = "all", limit: int = 300) -> list[dict[str, Any]]:
    payload = await core_get("/incidents", {"status": status, "limit": limit})
    return payload.get("incidents", [])


async def fetch_incident_detail(incident_id: str) -> dict[str, Any]:
    return await core_get(f"/incidents/{incident_id}")


async def fetch_operations(incident_id: str) -> dict[str, Any]:
    try:
        return await automation_get(f"/api/incidents/{incident_id}/operations")
    except Exception as exc:
        emit_log("WARNING", "operations_context_fetch_failed", incidentId=incident_id, error=str(exc))
        return {}


async def stop_aware_sleep(seconds: float, run: dict[str, Any]) -> bool:
    deadline = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return False
        await asyncio.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
    return True


async def wait_for_active_incidents_to_clear(run: dict[str, Any], timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return False
        try:
            active = await fetch_incidents("active", 200)
            if not active:
                return True
        except Exception as exc:
            run["message"] = f"Waiting for PulseGuard Core before test: {str(exc)[:220]}"
            persist_run(run)
        await asyncio.sleep(TEST_POLL_SECONDS)
    return False


async def wait_for_metric_stability(run: dict[str, Any], timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    consecutive = 0
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return False
        try:
            active, evaluation = await asyncio.gather(
                fetch_incidents("active", 200),
                core_get("/evaluation"),
            )
            latest = evaluation.get("lastEvaluation", {})
            retry_values = [float(value or 0) for value in (latest.get("retryRateByNode") or {}).values()]
            checkout_failure = float(latest.get("checkoutFailurePercent") or 0)
            stable = (
                not active
                and max(retry_values or [0.0]) <= TEST_BASELINE_RETRY_RATE
                and checkout_failure <= TEST_BASELINE_CHECKOUT_FAILURE_PERCENT
                and not bool(latest.get("sharedDependencyActive"))
                and not bool(latest.get("fleetCapacityActive"))
            )
            consecutive = consecutive + 1 if stable else 0
            run["message"] = (
                f"Waiting for a clean metric baseline: {consecutive}/{TEST_BASELINE_STABLE_EVALUATIONS} stable checks; "
                f"checkout failures {checkout_failure:.2f}%, max retry {max(retry_values or [0.0]):.3f}/s."
            )
            persist_run(run)
            if consecutive >= TEST_BASELINE_STABLE_EVALUATIONS:
                return True
        except Exception as exc:
            consecutive = 0
            run["message"] = f"Waiting for stable PulseGuard metrics before test: {str(exc)[:220]}"
            persist_run(run)
        await asyncio.sleep(TEST_POLL_SECONDS)
    return False


async def activate_test_disturbance(name: str) -> dict[str, Any]:
    global background_scenario_task
    await clean_before_scenario()
    if name == "payment_latency":
        await add_toxic(PAYMENT_PROXY_NAME, "payment_latency_downstream", "latency", "downstream", {"latency": 1200, "jitter": 150})
        update_state("payment_latency", latencyMs=1200, jitterMs=150, target="payment-node-3", testRun=True)
    elif name == "payment_timeout":
        diagnostics = await set_node_fault("payment-node-3", "timeout")
        update_state("payment_timeout", target="payment-node-3", implementation="application call exceeds router timeout", diagnostics=diagnostics, testRun=True)
    elif name == "payment_reset":
        await add_toxic(PAYMENT_PROXY_NAME, "payment_reset_downstream", "reset_peer", "downstream", {"timeout": 100})
        update_state("payment_reset", timeoutMs=100, target="payment-node-3", testRun=True)
    elif name == "intermittent_network":
        await add_toxic(PAYMENT_PROXY_NAME, "payment_intermittent_reset_downstream", "reset_peer", "downstream", {"timeout": 100}, toxicity=0.4)
        update_state("intermittent_network", target="payment-node-3", toxicity=0.4, approximateFailurePercent=40, testRun=True)
    elif name == "node_offline":
        diagnostics = await set_node_fault("payment-node-3", "unavailable")
        update_state("node_offline", target="payment-node-3", implementation="controlled application fault; process remains running", diagnostics=diagnostics, testRun=True)
    elif name == "node_flapping":
        update_state("node_flapping", target="payment-node-3", durationSeconds=60, intervalSeconds=4, phase="starting", cycle=0, testRun=True)
        background_scenario_task = asyncio.create_task(run_node_flapping("payment-node-3", 60, 4), name="opsai-random-test-flap-payment-node-3")
    elif name == "shared_dependency_outage":
        results = await asyncio.gather(*[set_node_fault(node_id, "shared_dependency") for node_id in PAYMENT_NODE_URLS])
        update_state("shared_dependency_outage", target="payment-authorisation-provider", affectedNodes=sorted(PAYMENT_NODE_URLS), diagnostics=results, testRun=True)
    elif name == "traffic_spike":
        override = await set_traffic_override(4.0, 100)
        update_state("traffic_spike", multiplier=4.0, durationSeconds=100, expiresAt=override.get("expiresAt"), maxTargetUsers=override.get("maxTargetUsers"), source="live Wikimedia traffic profile", testRun=True)
    elif name == "capacity_failover_scale":
        node3 = await set_node_fault("payment-node-3", "unavailable")
        capacity = await asyncio.gather(
            set_node_capacity("payment-node-1", 1, 1000),
            set_node_capacity("payment-node-2", 1, 1000),
        )
        override = await set_traffic_override(4.0, 210)
        update_state(
            "capacity_failover_scale",
            unavailableNode="payment-node-3",
            remainingNodes=["payment-node-1", "payment-node-2"],
            initialCapacityUnits=1,
            capacityPressureMs=1000,
            trafficMultiplier=4.0,
            expiresAt=override.get("expiresAt"),
            node3Diagnostics=node3,
            peerDiagnostics=capacity,
            simulatedInfrastructure=True,
            testRun=True,
        )
    elif name == "disk_pressure":
        result = await automation_post("/scenarios/disk-pressure", {"target_percent": 92, "cleanup_insufficient": "false"})
        update_state("disk_pressure", target="opsai-demo-storage", automation=result, testRun=True)
    elif name == "certificate_expiring":
        result = await automation_post("/scenarios/certificate-expiring", {"seconds": 300})
        update_state("certificate_expiring", target="checkout.pulseguard.local", automation=result, testRun=True)
    elif name == "certificate_renewal_failure":
        result = await automation_post("/scenarios/certificate-renewal-failure", {"seconds": 300})
        update_state("certificate_renewal_failure", target="checkout.pulseguard.local", automation=result, testRun=True)
    elif name == "external_auth_failure":
        result = await invalidate_external_auth_token()
        update_state(
            "external_auth_failure",
            target="partner-risk-service",
            implementation="external token rotated without checkout update",
            secretExposed=False,
            externalService=result,
            testRun=True,
        )
    else:
        raise RuntimeError(f"Unknown test disturbance: {name}")
    return dict(current_state)


async def wait_for_new_incidents(
    baseline_ids: set[str],
    run: dict[str, Any],
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str | None]:
    deadline = time.monotonic() + timeout_seconds
    first_detected_at: str | None = None
    found: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return found, first_detected_at
        incidents = await fetch_incidents("all", 300)
        found = [item for item in incidents if str(item.get("id")) not in baseline_ids]
        if found:
            first_detected_at = utc_now()
            await stop_aware_sleep(TEST_INCIDENT_SETTLE_SECONDS, run)
            incidents = await fetch_incidents("all", 300)
            found = [item for item in incidents if str(item.get("id")) not in baseline_ids]
            return found, first_detected_at
        await asyncio.sleep(TEST_POLL_SECONDS)
    return [], None


async def wait_for_investigations(
    incident_ids: list[str],
    run: dict[str, Any],
    timeout_seconds: int,
) -> tuple[list[dict[str, Any]], str | None]:
    if not incident_ids:
        return [], None
    deadline = time.monotonic() + timeout_seconds
    latest: list[dict[str, Any]] = []
    first_completed_at: str | None = None
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return latest, first_completed_at
        latest = []
        for incident_id in incident_ids:
            try:
                detail = await fetch_incident_detail(incident_id)
                investigation = detail.get("investigation")
                if investigation:
                    latest.append(investigation)
            except Exception as exc:
                emit_log("WARNING", "test_investigation_poll_failed", incidentId=incident_id, error=str(exc))
        completed = [item for item in latest if item.get("status") in {"COMPLETED", "FAILED"}]
        if completed and first_completed_at is None:
            first_completed_at = utc_now()
        if len(completed) >= len(incident_ids):
            return latest, first_completed_at
        await asyncio.sleep(TEST_POLL_SECONDS)
    return latest, first_completed_at


async def wait_for_incident_resolution(
    incident_ids: list[str],
    run: dict[str, Any],
    timeout_seconds: int,
) -> bool:
    if not incident_ids:
        return False
    deadline = time.monotonic() + timeout_seconds
    wanted = set(incident_ids)
    while time.monotonic() < deadline:
        if test_run_stop_requested or run.get("status") == "STOPPING":
            return False
        incidents = await fetch_incidents("all", 300)
        state_by_id = {str(item.get("id")): item.get("status") for item in incidents}
        if all(state_by_id.get(item) == "RESOLVED" for item in wanted):
            return True
        await asyncio.sleep(TEST_POLL_SECONDS)
    return False


def assess_test_step(step: dict[str, Any], definition: dict[str, Any]) -> None:
    actual_types = set(step.get("incidentTypes", []))
    desired_types = set(definition.get("desiredIncidentTypes", []))
    acceptable_types = set(definition.get("currentAcceptableIncidentTypes", []))
    if not step.get("injectionSucceeded"):
        step["classificationStatus"] = "NOT_ASSESSED"
        step["outcome"] = "FAILED"
        return
    if not step.get("detected"):
        if definition.get("noIncidentCanPass") and step.get("noImpactObserved"):
            step["classificationStatus"] = "RESILIENCE_PASS"
            step["repairOutcome"] = "RESILIENCE_PASS"
            step["outcome"] = "PASSED"
            step["resolved"] = True
            step["notes"].append("The platform absorbed the traffic surge without incident-worthy latency or failures.")
            return
        step["classificationStatus"] = "NOT_DETECTED"
        step["outcome"] = "GAP"
        step["notes"].append("The disturbance was injected, but no new PulseGuard incident was raised within the test timeout.")
        return
    extras = actual_types - desired_types
    if desired_types.issubset(actual_types) and not extras and int(step.get("incidentCount") or 0) == len(desired_types):
        step["classificationStatus"] = "CORRECT"
    elif actual_types.intersection(desired_types):
        step["classificationStatus"] = "CORRECT_WITH_DUPLICATE_NOISE"
        step["notes"].append(f"The desired type was present, but unexpected standalone incident types were also raised: {sorted(extras)}.")
    elif actual_types.intersection(acceptable_types):
        step["classificationStatus"] = "FUNCTIONAL_FALLBACK"
        step["notes"].append("Detection worked through a generic fallback classification.")
    else:
        step["classificationStatus"] = "INCORRECT"
        step["notes"].append("The raised incident type did not match the expected classification.")

    expected_action = definition.get("expectedAction")
    if expected_action and expected_action not in set(step.get("recommendations", [])):
        step["notes"].append(f"Expected PulseGuard action {expected_action} was not observed.")
    expected_queue = definition.get("expectedQueue")
    assigned_queue = step.get("assignedSupportQueue")
    if expected_queue:
        if assigned_queue:
            step["queueRoutingStatus"] = "CORRECT" if assigned_queue == expected_queue else "MISMATCH"
        else:
            step["queueRoutingStatus"] = "MISSING"
    else:
        step["queueRoutingStatus"] = "NOT_EXPECTED" if not assigned_queue else "UNEXPECTED_ASSIGNMENT"

    if definition.get("autoRepairExpected") and step.get("repairOutcome") != "AUTO_REPAIRED":
        step["outcome"] = "PARTIAL"
        step["notes"].append("The scenario required a verified automatic repair, but AUTO_REPAIRED was not recorded.")
    elif definition.get("manualInterventionExpected"):
        manual_pass = (
            step.get("classificationStatus") == "CORRECT"
            and step.get("investigated")
            and step.get("realAi")
            and step.get("repairOutcome") == "MANUAL_INTERVENTION_REQUIRED"
            and step.get("queueRoutingStatus") == "CORRECT"
            and not step.get("resolved")
        )
        if manual_pass:
            step["outcome"] = "PASSED"
            step["notes"].append("The automatic repair failed safely, the expected support queue received a detailed handoff, and the incident intentionally remains open for an operator.")
            return
        step["outcome"] = "PARTIAL"
        step["notes"].append("The failed automatic repair did not produce the complete expected support handoff.")
    elif not step.get("investigated"):
        step["outcome"] = "PARTIAL"
        step["notes"].append("Detection succeeded, but no completed investigation was observed.")
    elif not step.get("realAi"):
        step["outcome"] = "PARTIAL"
        step["notes"].append("Investigation completed without REAL_AI mode for every incident.")
    elif step.get("classificationStatus") == "CORRECT" and step.get("resolved"):
        step["outcome"] = "PASSED"
    else:
        step["outcome"] = "PARTIAL"


async def execute_test_step(run: dict[str, Any], step: dict[str, Any]) -> None:
    definition = TEST_SCENARIOS[step["disturbance"]]
    step.update({"status": "PREPARING", "startedAt": utc_now(), "notes": [], "repairOutcome": None, "assignedSupportQueue": None, "queueRoutingStatus": None})
    persist_run(run)
    try:
        await clean_before_scenario()
        step["precondition"] = "WAITING_FOR_CLEAN_BASELINE"
        persist_run(run)
        if not await wait_for_active_incidents_to_clear(run, TEST_PRECONDITION_TIMEOUT_SECONDS):
            if test_run_stop_requested or run.get("status") == "STOPPING":
                step.update({"status": "STOPPED", "outcome": "STOPPED"})
                return
            step.update({"status": "FAILED", "outcome": "FAILED", "error": "Existing active incidents did not clear before the test timeout."})
            return
        step["precondition"] = "WAITING_FOR_STABLE_METRICS"
        persist_run(run)
        if not await wait_for_metric_stability(run, TEST_PRECONDITION_TIMEOUT_SECONDS):
            step.update({"status": "FAILED", "outcome": "FAILED", "error": "Prometheus metrics did not return to a stable baseline before the test timeout."})
            return
        step["precondition"] = "CLEAN_BASELINE_CONFIRMED"
        baseline = await fetch_incidents("all", 300)
        baseline_ids = {str(item.get("id")) for item in baseline}
        step.update({"baselineIncidentCount": len(baseline_ids), "status": "INJECTING", "injectedAt": utc_now()})
        persist_run(run)
        step["scenarioState"] = await activate_test_disturbance(step["disturbance"])
        step.update({"injectionSucceeded": True, "status": "WAITING_FOR_DETECTION"})
        persist_run(run)
        incidents, detected_at = await wait_for_new_incidents(baseline_ids, run, int(definition["detectionTimeoutSeconds"]))
        superseded = [item for item in incidents if (item.get("evidence") or {}).get("repairOutcome") == "SUPERSEDED"]
        primary_incidents = [item for item in incidents if item not in superseded]
        if primary_incidents:
            incidents = primary_incidents
        step["supersededIncidentIds"] = [str(item.get("id")) for item in superseded]
        step["detectedAt"] = detected_at
        step["detectionSeconds"] = seconds_between(step.get("injectedAt"), detected_at)
        step["detected"] = bool(incidents)
        step["incidentCount"] = len(incidents)
        step["incidentIds"] = [str(item.get("id")) for item in incidents]
        step["incidentTypes"] = sorted({str(item.get("incident_type")) for item in incidents})
        step["incidentNodes"] = sorted({str(item.get("node")) for item in incidents if item.get("node")})
        if not incidents and definition.get("noIncidentCanPass"):
            evaluation = await core_get("/evaluation")
            latest = evaluation.get("lastEvaluation", {})
            rules = evaluation.get("rules", {})
            latency_threshold = float(rules.get("payment_node_latency", {}).get("openThreshold", 0.8))
            failure_threshold = float(rules.get("checkout_failure_rate", {}).get("openThreshold", 5.0))
            node_latencies = latest.get("p95LatencySecondsByNode", {}) or {}
            max_node_latency = max([float(value or 0) for value in node_latencies.values()] or [0.0])
            checkout_failure = float(latest.get("checkoutFailurePercent") or 0)
            step["impactSnapshot"] = {"maxNodeP95Seconds": max_node_latency, "checkoutP95Seconds": latest.get("checkoutP95Seconds"), "checkoutFailurePercent": checkout_failure, "checkoutRequestsPerSecond": latest.get("checkoutRequestsPerSecond"), "trafficContext": latest.get("trafficContext", {}), "latencyThresholdSeconds": latency_threshold, "failureThresholdPercent": failure_threshold}
            step["noImpactObserved"] = max_node_latency < latency_threshold and checkout_failure < failure_threshold
        persist_run(run)
        investigations = []
        if incidents:
            step["status"] = "WAITING_FOR_INVESTIGATION"
            persist_run(run)
            investigations, investigated_at = await wait_for_investigations(step["incidentIds"], run, TEST_INVESTIGATION_TIMEOUT_SECONDS)
            completed = [item for item in investigations if item.get("status") == "COMPLETED"]
            step["investigatedAt"] = investigated_at
            step["investigationSeconds"] = seconds_between(detected_at, investigated_at)
            step["completedInvestigationCount"] = len(completed)
            step["failedInvestigationCount"] = sum(item.get("status") == "FAILED" for item in investigations)
            step["investigated"] = bool(completed)
            step["investigationCoveragePercent"] = round(100 * len(completed) / max(1, len(incidents)), 1)
            step["realAi"] = bool(completed) and all(item.get("analysis_mode") == "REAL_AI" for item in completed)
            step["analysisModes"] = sorted({str(item.get("analysis_mode")) for item in completed})
            step["providers"] = sorted({str(item.get("provider")) for item in completed})
            step["recommendations"] = sorted({str(item.get("action_name")) for item in completed if item.get("action_name")})
            step["policyDecisions"] = sorted({str(item.get("policy_decision")) for item in completed if item.get("policy_decision")})
            step["actionExecutionStatuses"] = sorted({str(item.get("action_execution_status")) for item in completed if item.get("action_execution_status")})
            step["executedActionCount"] = sum(bool(item.get("action_executed")) for item in completed)
            step["succeededActionCount"] = sum(item.get("action_execution_status") == "SUCCEEDED" for item in completed)
            step["approvalRequiredCount"] = sum(item.get("policy_decision") == "APPROVAL_REQUIRED" for item in completed)
            step["blockedActionCount"] = sum(item.get("policy_decision") == "BLOCKED" for item in completed)
            step["averageConfidence"] = round(sum(float(item.get("confidence") or 0) for item in completed) / len(completed), 3) if completed else None
            enriched=[]
            for item in investigations:
                incident_id=str(item.get("incident_id"))
                operations=await fetch_operations(incident_id)
                handoff=operations.get("supportHandoff") or {}
                if handoff.get("primaryQueue") and not step.get("assignedSupportQueue"):
                    step["assignedSupportQueue"] = handoff.get("primaryQueue")
                if operations.get("repairOutcome"):
                    step["repairOutcome"] = operations.get("repairOutcome")
                enriched.append({"incidentId": incident_id, "status": item.get("status"), "analysisMode": item.get("analysis_mode"), "provider": item.get("provider"), "model": item.get("model"), "actionName": item.get("action_name"), "recommendedAction": item.get("recommended_action"), "policyDecision": item.get("policy_decision"), "policyReason": item.get("policy_reason"), "confidence": item.get("confidence"), "actionExecutionStatus": item.get("action_execution_status"), "actionExecuted": item.get("action_executed"), "actionExecutor": item.get("action_executor"), "actionExecutionResult": item.get("action_execution_result"), "repairOutcome": operations.get("repairOutcome"), "supportHandoff": handoff, "error": item.get("error")})
            step["investigations"] = enriched
            persist_run(run)

        # Automatic repair scenarios are allowed to resolve themselves before cleanup.
        if definition.get("autoRepairExpected") and step.get("incidentIds"):
            step["status"] = "VERIFYING_AUTOMATIC_REPAIR"
            persist_run(run)
            step["resolved"] = await wait_for_incident_resolution(step["incidentIds"], run, TEST_RECOVERY_TIMEOUT_SECONDS)
            step["resolvedAt"] = utc_now() if step["resolved"] else None
            for incident_id in step["incidentIds"]:
                operations = await fetch_operations(incident_id)
                if operations.get("repairOutcome"):
                    step["repairOutcome"] = "AUTO_REPAIRED" if str(operations.get("repairOutcome")).startswith("AUTO_REPAIRED") else operations.get("repairOutcome")
            step["recoverySeconds"] = seconds_between(step.get("injectedAt"), step.get("resolvedAt"))
        elif definition.get("manualInterventionExpected") and step.get("incidentIds"):
            deadline=time.monotonic()+45
            while time.monotonic()<deadline and not step.get("assignedSupportQueue"):
                for incident_id in step["incidentIds"]:
                    operations=await fetch_operations(incident_id)
                    handoff=operations.get("supportHandoff") or {}
                    if handoff.get("primaryQueue"):
                        step["assignedSupportQueue"]=handoff.get("primaryQueue")
                        step["supportHandoff"]=handoff
                        step["repairOutcome"]="MANUAL_INTERVENTION_REQUIRED"
                        break
                if not step.get("assignedSupportQueue"):
                    await asyncio.sleep(TEST_POLL_SECONDS)
            step["resolved"] = False
        else:
            step["status"] = "RESETTING"
            persist_run(run)
            await clean_before_scenario()
            step["resetAt"] = utc_now()
            if step.get("incidentIds"):
                step["status"] = "VERIFYING_RECOVERY"
                persist_run(run)
                step["resolved"] = await wait_for_incident_resolution(step["incidentIds"], run, TEST_RECOVERY_TIMEOUT_SECONDS)
                step["resolvedAt"] = utc_now() if step["resolved"] else None
                step["recoverySeconds"] = seconds_between(step.get("resetAt"), step.get("resolvedAt"))
                if step["resolved"]:
                    step["repairOutcome"] = "RECOVERED_AFTER_TEST_CLEANUP"
            else:
                step["resolved"] = False
                step["resolvedAt"] = None
                step["recoverySeconds"] = None
        assess_test_step(step, definition)
        step["status"] = "COMPLETED"
    except Exception as exc:
        step.update({"status": "FAILED", "outcome": "FAILED", "error": str(exc)})
        step.setdefault("notes", []).append("The test runner encountered an execution error.")
        emit_log("ERROR", "random_test_step_failed", runId=run.get("id"), disturbance=step.get("disturbance"), error=str(exc))
    finally:
        try:
            await clean_before_scenario()
        except Exception as cleanup_exc:
            step.setdefault("notes", []).append(f"Cleanup warning: {str(cleanup_exc)[:250]}")
        step["completedAt"] = utc_now()
        if test_run_stop_requested or run.get("status") == "STOPPING":
            if step.get("status") not in {"COMPLETED", "FAILED"}:
                step.update({"status": "STOPPED", "outcome": "STOPPED"})
        persist_run(run)
        TEST_STEPS.labels(disturbance=step["disturbance"], outcome=str(step.get("outcome") or "UNKNOWN").lower()).inc()


async def execute_test_run(run_id: str) -> None:
    global test_run_task, test_run_stop_requested
    run = find_test_run(run_id)
    ACTIVE_TEST_RUN.set(1)
    run["status"] = "RUNNING"
    run["startedAt"] = utc_now()
    run["message"] = "Preparing the first disturbance. Existing faults will be reset before every step."
    persist_run(run)
    try:
        for step in run.get("steps", []):
            if test_run_stop_requested or run.get("status") == "STOPPING":
                break
            run["currentStep"] = step["index"]
            run["message"] = f"Running {step['displayName']} ({step['index']} of {len(run['steps'])})."
            persist_run(run)
            await execute_test_step(run, step)
        if test_run_stop_requested or run.get("status") == "STOPPING":
            for step in run.get("steps", []):
                if step.get("status") == "PENDING":
                    step["status"] = "STOPPED"
                    step["outcome"] = "STOPPED"
            run["status"] = "STOPPED"
            run["message"] = "Test stopped by the operator. All controlled faults were reset."
        elif any(step.get("outcome") == "FAILED" for step in run.get("steps", [])):
            run["status"] = "COMPLETED_WITH_ERRORS"
            run["message"] = "The suite completed, but one or more injections failed to execute."
        elif any(step.get("outcome") in {"GAP", "PARTIAL"} for step in run.get("steps", [])):
            run["status"] = "COMPLETED_WITH_GAPS"
            run["message"] = "The suite completed and recorded detection, investigation or classification gaps."
        else:
            run["status"] = "COMPLETED"
            run["message"] = "Every selected disturbance was either correctly detected and investigated or recorded as a verified resilience pass; policy and action execution results are included."
    except Exception as exc:
        run["status"] = "FAILED"
        run["message"] = f"Test run failed: {str(exc)[:500]}"
        run["error"] = str(exc)
        emit_log("ERROR", "random_test_run_failed", runId=run_id, error=str(exc))
    finally:
        try:
            await clean_before_scenario()
        except Exception as exc:
            run["cleanupError"] = str(exc)
        run["completedAt"] = utc_now()
        run["currentStep"] = None
        persist_run(run)
        outcome = str(run.get("status") or "UNKNOWN").lower()
        TEST_RUNS.labels(outcome=outcome).inc()
        ACTIVE_TEST_RUN.set(0)
        test_run_stop_requested = False
        test_run_task = None


def create_test_run(mode: str, count: int | None, seed: int | None) -> dict[str, Any]:
    global test_run_task, test_run_stop_requested, test_runs
    if test_run_task is not None and not test_run_task.done():
        raise HTTPException(status_code=409, detail="A random disturbance test is already running")
    scenario_names = list(TEST_SCENARIOS)
    generated_seed = seed if seed is not None else random.SystemRandom().randint(1, 2_147_483_647)
    rng = random.Random(generated_seed)
    if mode == "full":
        selected = scenario_names[:]
        rng.shuffle(selected)
    else:
        selected_count = max(1, min(int(count or 5), len(scenario_names)))
        selected = rng.sample(scenario_names, selected_count)
    run_id = str(uuid.uuid4())
    now = utc_now()
    run = {
        "id": run_id,
        "mode": mode,
        "seed": generated_seed,
        "status": "QUEUED",
        "createdAt": now,
        "startedAt": None,
        "completedAt": None,
        "updatedAt": now,
        "currentStep": None,
        "message": "Test queued.",
        "selectedDisturbances": selected,
        "steps": [
            {
                "index": index,
                "disturbance": name,
                "displayName": TEST_SCENARIOS[name]["displayName"],
                "description": TEST_SCENARIOS[name]["description"],
                "desiredIncidentTypes": TEST_SCENARIOS[name]["desiredIncidentTypes"],
                "currentAcceptableIncidentTypes": TEST_SCENARIOS[name]["currentAcceptableIncidentTypes"],
                "status": "PENDING",
                "outcome": None,
                "injectionSucceeded": False,
                "detected": False,
                "investigated": False,
                "realAi": False,
                "resolved": False,
                "incidentCount": 0,
                "executedActionCount": 0,
                "succeededActionCount": 0,
                "approvalRequiredCount": 0,
                "blockedActionCount": 0,
                "completedInvestigationCount": 0,
                "repairOutcome": None,
                "assignedSupportQueue": None,
                "queueRoutingStatus": None,
                "notes": [],
            }
            for index, name in enumerate(selected, start=1)
        ],
        "summary": {},
    }
    test_runs.append(run)
    test_runs = test_runs[-TEST_HISTORY_LIMIT:]
    test_run_stop_requested = False
    persist_run(run)
    test_run_task = asyncio.create_task(execute_test_run(run_id), name=f"opsai-random-test-{run_id}")
    return run


@app.on_event("startup")
async def startup_event() -> None:
    global ready
    load_test_runs()
    await wait_for_toxiproxy()
    await clean_before_scenario()
    update_state("none")
    ready = True


@app.get("/", response_class=HTMLResponse)
async def control_page() -> str:
    return r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PulseGuard Scenario Controller</title>
  <style>
    :root { color-scheme: dark; }
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; background: #0b1120; color: #f8fafc; }
    main { max-width: 1380px; margin: auto; padding: 2rem; }
    h1 { margin: 0 0 .25rem; } h2 { margin: 0 0 .35rem; font-size: 1.05rem; }
    .sub,.muted { color: #94a3b8; } .sub { margin-top: 0; }
    .notice { padding: .85rem 1rem; border: 1px solid #334155; border-radius: .7rem; background: #111827; margin: 1rem 0; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(285px,1fr)); gap: 1rem; }
    .card { background: #111827; padding: 1rem; border-radius: .8rem; border: 1px solid #334155; }
    .card p { color: #cbd5e1; font-size: .91rem; }
    .scenario-card p { min-height: 2.7rem; }
    button { padding: .65rem .8rem; border: 0; border-radius: .45rem; margin: .22rem .12rem; cursor: pointer; font-weight: 650; }
    button:disabled { opacity: .55; cursor: wait; }
    .danger { background: #dc2626; color: white; } .warn { background: #f59e0b; color: #111827; }
    .safe { background: #10b981; color: #052e16; } .info { background: #3b82f6; color: white; }
    .purple { background: #8b5cf6; color: white; } .neutral { background:#334155;color:white; }
    .status-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(160px,1fr)); gap:.65rem; margin:1rem 0; }
    .status { background:#111827; border:1px solid #334155; border-radius:.65rem; padding:.75rem; }
    .status b { display:block; font-size:1.05rem; margin-top:.2rem; }
    .good { color:#34d399; } .bad { color:#f87171; } .amber { color:#fbbf24; } .blue { color:#60a5fa; }
    pre { background: #020617; padding: 1rem; border-radius: .55rem; overflow: auto; max-height: 460px; border:1px solid #1e293b; }
    details { margin-top:1rem; } summary { cursor:pointer; color:#cbd5e1; }
    #message,#testMessage { min-height:1.5rem; margin:.75rem 0; font-weight:600; }
    .tabs { display:flex; gap:.45rem; margin:1rem 0; border-bottom:1px solid #334155; padding-bottom:.65rem; }
    .tab-button { background:#1e293b; color:#cbd5e1; }
    .tab-button.active { background:#2563eb; color:white; }
    .tab-panel { display:none; } .tab-panel.active { display:block; }
    .toolbar { display:flex; gap:.45rem; align-items:center; flex-wrap:wrap; margin:.7rem 0 1rem; }
    .progress-shell { height:14px; background:#020617; border:1px solid #334155; border-radius:999px; overflow:hidden; }
    .progress-bar { height:100%; width:0; background:#10b981; transition:width .25s ease; }
    table { width:100%; border-collapse:collapse; font-size:.88rem; }
    th,td { text-align:left; padding:.65rem .55rem; border-bottom:1px solid #263244; vertical-align:top; }
    th { color:#cbd5e1; position:sticky; top:0; background:#111827; }
    .table-wrap { overflow:auto; max-height:620px; border:1px solid #334155; border-radius:.65rem; }
    .pill { display:inline-block; padding:.18rem .45rem; border-radius:999px; background:#263244; margin:.08rem; font-size:.78rem; white-space:nowrap; }
    .outcome-PASSED { color:#34d399; } .outcome-PARTIAL,.outcome-GAP { color:#fbbf24; }
    .outcome-FAILED,.outcome-STOPPED { color:#f87171; }
    .run-list { display:flex; flex-wrap:wrap; gap:.45rem; margin:.65rem 0; }
    .run-chip { background:#1e293b;color:#cbd5e1;border:1px solid #334155; }
    .run-chip.active { border-color:#60a5fa;color:white; }
    .explain { font-size:.86rem;color:#94a3b8; }
  </style>
</head>
<body><main>
  <h1>PulseGuard Scenario Controller</h1>
  <p class="sub">Inject controlled turbulence and auto-repair scenarios, then validate detection, PulseGuard investigation, governance, action execution, support routing and recovery.</p>
  <div class="notice"><b>Safety boundary:</b> scenarios use application admin endpoints and Toxiproxy. PulseGuard is not given Docker socket access or arbitrary shell execution. Automatic cleanup is restricted to a bounded demo volume, and certificate renewal is restricted to the allowlisted demo CA and hostname.</div>

  <div class="status-grid">
    <div class="status">Active scenario<b id="activeScenario">Loading...</b></div>
    <div class="status">Traffic override<b id="trafficOverride">Loading...</b></div>
    <div class="status">Node 3 fault<b id="node3Fault">Loading...</b></div>
    <div class="status">Controller<b id="controllerStatus">Loading...</b></div>
    <div class="status">Random test<b id="testRunStatus">Loading...</b></div>
  </div>

  <div class="tabs">
    <button class="tab-button active" data-tab="manual" onclick="showTab('manual')">Manual scenarios</button>
    <button class="tab-button" data-tab="tests" onclick="showTab('tests')">Random test summary</button>
  </div>

  <section id="manualTab" class="tab-panel active">
    <div id="message"></div>
    <div class="grid">
      <section class="card scenario-card"><h2>Sudden demand spike</h2>
        <p>Amplifies the live Wikimedia-derived Locust target temporarily, capped by TRAFFIC_MAX_USERS. All checkout traffic remains synthetic and local.</p>
        <button class="info manual-action" onclick="runScenario('/scenarios/traffic-spike?multiplier=2&duration_seconds=60')">2x for 60s</button>
        <button class="purple manual-action" onclick="runScenario('/scenarios/traffic-spike?multiplier=4&duration_seconds=90')">4x for 90s</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/traffic-spike?multiplier=6&duration_seconds=120', 'Inject a 6x demand spike? The target remains capped, but this is the heaviest local load test.')">6x for 120s</button>
      </section>

      <section class="card scenario-card"><h2>Node 3 availability</h2>
        <p>Simulates a running process that cannot accept payments. The router should retry healthy peers; this is not a container restart.</p>
        <button class="danger manual-action" onclick="runScenario('/scenarios/node-offline?node_id=payment-node-3', 'Take payment-node-3 out of service?')">Take node 3 offline</button>
        <button class="warn manual-action" onclick="runScenario('/scenarios/node-flapping?node_id=payment-node-3&duration_seconds=60&interval_seconds=5')">Flap node 3 for 60s</button>
      </section>

      <section class="card scenario-card"><h2>Failover capacity scale-up</h2>
        <p>Takes node 3 unavailable, raises demand and applies controlled capacity pressure to nodes 1 and 2. PulseGuard may automatically increase their bounded worker-capacity units. This simulates infrastructure scaling without Docker-socket or host-resource access.</p>
        <button class="danger manual-action" onclick="runScenario('/scenarios/capacity-failover-scale', 'Take node 3 unavailable and run the bounded capacity auto-scaling scenario?')">Run capacity failover scale-up</button>
      </section>

      <section class="card scenario-card"><h2>Intermittent node network</h2>
        <p>Resets approximately 30% of new node-3 connections. This is packet-loss-like turbulence, implemented as probabilistic connection resets.</p>
        <button class="warn manual-action" onclick="runScenario('/scenarios/intermittent-network?toxicity=0.3&timeout_ms=100')">30% connection resets</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/payment-reset?timeout_ms=100')">100% connection resets</button>
      </section>

      <section class="card scenario-card"><h2>Shared dependency outage</h2>
        <p>Makes all payment nodes report the same downstream authorisation dependency failure. The summary tab will expose whether PulseGuard uses one fleet incident or generic per-node fallbacks.</p>
        <button class="danger manual-action" onclick="runScenario('/scenarios/shared-dependency-outage', 'Fail the shared payment dependency for all three nodes? Checkout requests will fail until reset.')">Fail shared dependency</button>
      </section>

      <section class="card scenario-card"><h2>Node 3 latency and timeout</h2>
        <p>Isolated-node experiments for sustained latency and timeout/failover behaviour.</p>
        <button class="warn manual-action" onclick="runScenario('/scenarios/payment-latency?latency_ms=1200&jitter_ms=150')">Add 1.2s latency</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/payment-timeout')">Force timeout</button>
      </section>

      <section class="card scenario-card"><h2>Wikimedia signal path</h2>
        <p>Disrupts only the validated traffic-profile path. Locust should move to its safe fallback; Wikimedia content never becomes checkout data.</p>
        <button class="warn manual-action" onclick="runScenario('/scenarios/external-latency?latency_ms=2500&jitter_ms=300')">Delay profile</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/external-timeout?timeout_ms=500')">Timeout profile</button>
      </section>

      <section class="card scenario-card"><h2>Traffic-profile data quality</h2>
        <p>Produces malformed or unsafe traffic-control data so the load generator can demonstrate validation and fallback.</p>
        <button class="info manual-action" onclick="runScenario('/scenarios/payload-corruption/wrong_type')">Wrong type</button>
        <button class="info manual-action" onclick="runScenario('/scenarios/payload-corruption/stale_timestamp')">Stale timestamp</button>
        <button class="info manual-action" onclick="runScenario('/scenarios/payload-corruption/outlier')">Extreme outlier</button>
        <button class="info manual-action" onclick="runScenario('/scenarios/payload-corruption/malformed_json')">Malformed JSON</button>
      </section>

      <section class="card scenario-card"><h2>Bounded disk pressure</h2>
        <p>Fills only the dedicated demo volume. PulseGuard archives old logs, cleans allowlisted content and verifies recovered capacity.</p>
        <button class="warn manual-action" onclick="runScenario('/scenarios/disk-pressure')">Fill demo disk to 92%</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/disk-pressure?cleanup_insufficient=true')">Make cleanup insufficient</button>
      </section>

      <section class="card scenario-card"><h2>Certificate lifecycle</h2>
        <p>Creates a real locally signed demo certificate near expiry and validates automated renewal or a support handoff.</p>
        <button class="warn manual-action" onclick="runScenario('/scenarios/certificate-expiring')">Expire in 5 minutes</button>
        <button class="danger manual-action" onclick="runScenario('/scenarios/certificate-renewal-failure')">Simulate renewal failure</button>
      </section>

      <section class="card scenario-card"><h2>External authentication failure</h2>
        <p>Rotates the synthetic partner token while checkout still holds the previous generation. PulseGuard detects repeated 401 responses, refreshes the allowlisted credential, redacts the secret and verifies a probe call.</p>
        <button class="danger manual-action" onclick="runScenario('/scenarios/external-auth-failure', 'Rotate the partner credential and deliberately break checkout authentication?')">Break partner authentication</button>
      </section>

      <section class="card scenario-card"><h2>Hung payment worker and restart</h2>
        <p>Makes a payment worker stop completing requests while its process and health endpoint remain reachable. PulseGuard recommends a bounded application restart, but deterministic governance requires explicit operator approval.</p>
        <button class="danger manual-action" onclick="runScenario('/scenarios/payment-node-hung?node_id=payment-node-3', 'Simulate a hung payment worker on node 3? Restart will still require operator approval.')">Hang payment node 3</button>
      </section>

      <section class="card scenario-card"><h2>Predictive disk growth</h2>
        <p>Gradually increases the bounded demo volume without crossing the reactive threshold. The predictor forecasts time to threshold and asks AI to explain the operational risk in observation-only mode.</p>
        <button class="purple manual-action" onclick="runScenario('/scenarios/predictive-disk-growth?start_percent=35&end_percent=79&duration_seconds=120')">Run disk forecast</button>
      </section>

      <section class="card scenario-card"><h2>Predictive node degradation</h2>
        <p>Gradually increases node-local processing pressure while peers remain stable. The predictor should identify the isolated trend before the reactive latency incident opens.</p>
        <button class="purple manual-action" onclick="runScenario('/scenarios/predictive-node-degradation?node_id=payment-node-2&start_pressure_ms=100&end_pressure_ms=650&duration_seconds=120')">Forecast node degradation</button>
      </section>

      <section class="card scenario-card"><h2>Predictive capacity risk</h2>
        <p>Takes node 3 unavailable and gradually raises pressure on the two remaining nodes. PulseGuard forecasts capacity saturation and explains a preventive response without executing it in Day 5 observation mode.</p>
        <button class="purple manual-action" onclick="runScenario('/scenarios/predictive-capacity-risk?start_pressure_ms=100&end_pressure_ms=650&duration_seconds=150')">Forecast capacity saturation</button>
      </section>

      <section class="card scenario-card"><h2>Recovery</h2>
        <p>Cancels timed scenarios, restores node fault modes, removes Toxiproxy toxics, clears traffic amplification, resets the bounded storage and restores the normal demo certificate.</p>
        <button class="safe manual-action" onclick="runScenario('/scenarios/reset')">Reset all faults</button>
      </section>
    </div>
    <details><summary>Raw controller state</summary><pre id="state">Loading...</pre></details>
  </section>

  <section id="testsTab" class="tab-panel">
    <div class="card">
      <h2>Random end-to-end turbulence test</h2>
      <p>Each disturbance is applied from a clean baseline. The runner records detection, PulseGuard recommendation, governance, action actually taken, repair outcome, support routing and verified recovery.</p>
      <div class="toolbar">
        <button class="info test-start" onclick="startRandomTest(3)">Run 3 random</button>
        <button class="purple test-start" onclick="startRandomTest(5)">Run 5 random</button>
        <button class="danger test-start" onclick="startFullTest()">Run full suite</button>
        <button id="stopTestButton" class="warn" onclick="stopTest()" disabled>Stop and reset</button>
        <button class="neutral" onclick="refreshTestRuns(true)">Refresh summary</button>
      </div>
      <p class="explain">A full run includes operational turbulence, resilience, external authentication repair, bounded capacity scale-up after node loss, disk cleanup, certificate renewal and failed-renewal support handoff. Auto-repaired is counted only after an automatic remediation succeeds and recovery is independently verified.</p>
      <div id="testMessage"></div>
      <div class="progress-shell"><div id="testProgress" class="progress-bar"></div></div>
      <p id="testProgressText" class="muted">No test selected.</p>
    </div>

    <div id="runHistory" class="run-list"></div>

    <div class="status-grid">
      <div class="status">Planned<b id="sumPlanned">0</b></div>
      <div class="status">Injected<b id="sumInjected">0</b></div>
      <div class="status">Detected<b id="sumDetected">0</b></div>
      <div class="status">Investigated disturbances<b id="sumInvestigated">0</b></div>
      <div class="status">Incidents raised<b id="sumIncidents">0</b></div>
      <div class="status">Investigations completed<b id="sumInvestigations">0</b></div>
      <div class="status">Real AI<b id="sumRealAi">0</b></div>
      <div class="status">Correct classification<b id="sumClassified">0</b></div>
      <div class="status">Resilience passes<b id="sumResilience">0</b></div>
      <div class="status">PulseGuard decisions<b id="sumAiDecisions">0</b></div>
      <div class="status">Actions executed<b id="sumActionsExecuted">0</b></div>
      <div class="status">Actions succeeded<b id="sumActionsSucceeded">0</b></div>
      <div class="status">Auto-repaired<b id="sumAutoRepaired">0</b></div>
      <div class="status">Assigned to support<b id="sumAssignedSupport">0</b></div>
      <div class="status">Manual intervention<b id="sumManualIntervention">0</b></div>
      <div class="status">Approval required<b id="sumApprovalRequired">0</b></div>
      <div class="status">Blocked by policy<b id="sumBlocked">0</b></div>
      <div class="status">Resolved<b id="sumResolved">0</b></div>
      <div class="status">Gaps / partial / failed<b id="sumGaps">0</b></div>
    </div>

    <section class="card">
      <h2 id="selectedRunTitle">Test steps</h2>
      <p id="selectedRunMeta" class="muted">Start a random test to populate the summary.</p>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>#</th><th>Disturbance</th><th>Injection</th><th>Detection</th><th>PulseGuard investigation</th><th>Classification</th><th>PulseGuard decision</th><th>Governance</th><th>Action taken</th><th>Recovery</th><th>Repair outcome</th><th>Support queue</th><th>Outcome</th>
          </tr></thead>
          <tbody id="testStepRows"><tr><td colspan="13" class="muted">No test data.</td></tr></tbody>
        </table>
      </div>
      <details><summary>Selected run JSON</summary><pre id="testRunJson">No test selected.</pre></details>
    </section>
  </section>

<script>
function pulseGuardServiceUrl(port,path='') {
  const host=window.location.hostname;
  if(host.endsWith('.app.github.dev')) {
    const parts=host.split('.');
    parts[0]=parts[0].replace(/-\\d+$/,`-${port}`);
    return `${window.location.protocol}//${parts.join('.')}${path}`;
  }
  return `http://localhost:${port}${path}`;
}
function loadPulseGuardWidget() {
  const script=document.createElement('script');
  script.src=pulseGuardServiceUrl(8097,'/widget.js');
  script.crossOrigin='use-credentials';
  document.body.appendChild(script);
}
let selectedRunId=null;
let latestRuns=[];
let activeRun=null;
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const isRunning=s=>['QUEUED','RUNNING','STOPPING'].includes(s);
function showTab(name){
  document.querySelectorAll('.tab-button').forEach(b=>b.classList.toggle('active',b.dataset.tab===name));
  document.getElementById('manualTab').classList.toggle('active',name==='manual');
  document.getElementById('testsTab').classList.toggle('active',name==='tests');
  if(name==='tests') refreshTestRuns(true);
}
function setManualDisabled(value){document.querySelectorAll('.manual-action').forEach(b=>b.disabled=value);}
function renderState(body){
  const scenario=body.scenario||'none';
  const override=body.trafficOverride||{};
  const node3=(body.paymentNodes||[]).find(n=>n.nodeId==='payment-node-3')||{};
  document.getElementById('activeScenario').textContent=scenario;
  document.getElementById('activeScenario').className=scenario==='none'?'good':'amber';
  document.getElementById('trafficOverride').textContent=override.active?`${override.multiplier}× | ${override.remainingSeconds}s left`:'inactive';
  document.getElementById('trafficOverride').className=override.active?'amber':'good';
  document.getElementById('node3Fault').textContent=node3.faultMode||node3.processStatus||'unknown';
  document.getElementById('node3Fault').className=(node3.faultMode&&node3.faultMode!=='none')||node3.processStatus==='unreachable'?'bad':'good';
  document.getElementById('controllerStatus').textContent=body.ready?'healthy':'starting';
  document.getElementById('controllerStatus').className=body.ready?'good':'amber';
  document.getElementById('state').textContent=JSON.stringify(body,null,2);
}
async function refreshState(){
  try { const r=await fetch('/state'); const body=await r.json(); if(!r.ok) throw new Error(JSON.stringify(body)); renderState(body); }
  catch(e){ document.getElementById('message').textContent=`State refresh failed: ${e}`; }
}
async function runScenario(path,confirmText){
  if(activeRun&&isRunning(activeRun.status)){alert('A random test is running. Stop it before applying a manual scenario.');return;}
  if(confirmText && !confirm(confirmText)) return;
  setManualDisabled(true); document.getElementById('message').textContent='Applying scenario...';
  try {
    const r=await fetch(path,{method:'POST'}); const body=await r.json();
    if(!r.ok) throw new Error(JSON.stringify(body));
    document.getElementById('message').textContent=`Scenario applied: ${body.scenario||'updated'}`;
  } catch(e){ document.getElementById('message').textContent=`Scenario failed: ${e}`; }
  finally { setManualDisabled(false); await refreshState(); }
}
async function startRandomTest(count){
  if(!confirm(`Run ${count} randomly selected operational disturbances? Existing faults will be reset. The run may take several minutes.`)) return;
  await startTest(`/test-runs/random?count=${count}`);
}
async function startFullTest(){
  if(!confirm('Run the complete operational, resilience and auto-repair suite in random order? This can take 20 minutes or longer. Existing faults will be reset.')) return;
  await startTest('/test-runs/full');
}
async function startTest(path){
  document.getElementById('testMessage').textContent='Starting test...';
  document.querySelectorAll('.test-start').forEach(b=>b.disabled=true);
  try{
    const r=await fetch(path,{method:'POST'}); const body=await r.json();
    if(!r.ok) throw new Error(JSON.stringify(body));
    selectedRunId=body.id; showTab('tests'); await refreshTestRuns(true);
  }catch(e){document.getElementById('testMessage').textContent=`Could not start test: ${e}`;}
}
async function stopTest(){
  if(!activeRun||!isRunning(activeRun.status)) return;
  if(!confirm('Stop the test and reset all controlled faults?')) return;
  const r=await fetch(`/test-runs/${activeRun.id}/stop`,{method:'POST'}); const body=await r.json();
  if(!r.ok) document.getElementById('testMessage').textContent=`Stop failed: ${JSON.stringify(body)}`;
  await refreshTestRuns(true);
}
function pills(values){return (values||[]).map(v=>`<span class="pill">${esc(v)}</span>`).join('')||'—';}
function yesNo(value){return value?'<span class="good">Yes</span>':'<span class="bad">No</span>';}
function renderRuns(runs){
  latestRuns=runs||[];
  activeRun=latestRuns.find(r=>isRunning(r.status))||null;
  document.getElementById('testRunStatus').textContent=activeRun?activeRun.status:'idle';
  document.getElementById('testRunStatus').className=activeRun?'amber':'good';
  setManualDisabled(Boolean(activeRun));
  document.querySelectorAll('.test-start').forEach(b=>b.disabled=Boolean(activeRun));
  document.getElementById('stopTestButton').disabled=!activeRun;
  if(!selectedRunId&&latestRuns.length) selectedRunId=activeRun?.id||latestRuns[0].id;
  document.getElementById('runHistory').innerHTML=latestRuns.map(r=>`<button class="run-chip ${r.id===selectedRunId?'active':''}" onclick="selectRun('${r.id}')">${esc(r.status)} | ${new Date(r.createdAt).toLocaleString()} | ${esc(r.mode)}</button>`).join('');
}
async function selectRun(id){selectedRunId=id;await refreshTestRuns(true);}
function renderSelectedRun(run){
  if(!run) return;
  const s=run.summary||{};
  ['Planned','Injected','Detected','Investigated','RealAi','Classified','Resolved'].forEach(k=>{});
  document.getElementById('sumPlanned').textContent=s.planned??0;
  document.getElementById('sumInjected').textContent=s.injected??0;
  document.getElementById('sumDetected').textContent=s.detected??0;
  document.getElementById('sumInvestigated').textContent=s.investigated??0;
  document.getElementById('sumIncidents').textContent=s.incidentsRaised??0;
  document.getElementById('sumInvestigations').textContent=s.investigationsCompleted??0;
  document.getElementById('sumRealAi').textContent=s.realAi??0;
  document.getElementById('sumClassified').textContent=s.correctlyClassified??0;
  document.getElementById('sumResilience').textContent=s.resiliencePasses??0;
  document.getElementById('sumAiDecisions').textContent=s.aiDecisions??0;
  document.getElementById('sumActionsExecuted').textContent=s.actionsExecuted??0;
  document.getElementById('sumActionsSucceeded').textContent=s.actionsSucceeded??0;
  document.getElementById('sumAutoRepaired').textContent=s.autoRepaired??0;
  document.getElementById('sumAssignedSupport').textContent=s.assignedToSupport??0;
  document.getElementById('sumManualIntervention').textContent=s.manualInterventionRequired??0;
  document.getElementById('sumApprovalRequired').textContent=s.approvalRequired??0;
  document.getElementById('sumBlocked').textContent=s.actionsBlocked??0;
  document.getElementById('sumResolved').textContent=s.resolved??0;
  document.getElementById('sumGaps').textContent=(s.gaps??0)+(s.partial??0)+(s.failed??0);
  const done=s.completed??0, planned=s.planned??0, pct=planned?Math.round(100*done/planned):0;
  document.getElementById('testProgress').style.width=`${pct}%`;
  document.getElementById('testProgressText').textContent=`${run.status}: ${done}/${planned} steps completed. ${run.message||''}`;
  document.getElementById('testMessage').textContent=run.message||'';
  document.getElementById('selectedRunTitle').textContent=`Test run ${run.id.slice(0,8)} | ${run.status}`;
  document.getElementById('selectedRunMeta').textContent=`Mode ${run.mode}; seed ${run.seed}; created ${new Date(run.createdAt).toLocaleString()}.`;
  document.getElementById('testStepRows').innerHTML=(run.steps||[]).map(step=>{
    const detection=step.detected?`${yesNo(true)}<br>${pills(step.incidentTypes)}<br><span class="muted">${step.incidentCount||0} incident(s); ${step.detectionSeconds??'—'}s</span>`:step.status==='PENDING'?'Pending':`${yesNo(false)}<br><span class="muted">No incident in timeout</span>`;
    const investigation=step.investigated?`${yesNo(true)}<br>${pills(step.analysisModes)}<br><span class="muted">${step.completedInvestigationCount||0}/${step.incidentCount||0}; ${step.investigationSeconds??'—'}s</span>`:step.detected?`${yesNo(false)}<br><span class="muted">Coverage ${step.investigationCoveragePercent??0}%</span>`:'—';
    const classification=`<b>${esc(step.classificationStatus||'—')}</b><br><span class="muted">Desired: ${esc((step.desiredIncidentTypes||[]).join(', '))}</span>`;
    const aiDecision=`${pills(step.recommendations)}<br><span class="muted">Confidence ${step.averageConfidence==null?'—':Math.round(Number(step.averageConfidence)*100)+'%'}</span>`;
    const policy=`${pills(step.policyDecisions)}`;
    const actionTaken=`${pills(step.actionExecutionStatuses)}<br><span class="muted">Executed ${step.executedActionCount||0}; succeeded ${step.succeededActionCount||0}</span>`;
    const recovery=step.detected?`${yesNo(step.resolved)}<br><span class="muted">${step.recoverySeconds??'—'}s</span>`:step.classificationStatus==='RESILIENCE_PASS'?'<span class="good">No recovery needed</span>':'—';
    const repairOutcome=`<b>${esc(step.repairOutcome||'RECOMMENDED_ONLY')}</b>`;
    const supportQueue=step.assignedSupportQueue?`<b>${esc(step.assignedSupportQueue)}</b><br><span class="muted">${esc(step.queueRoutingStatus||'ASSIGNED')}</span>`:'—';
    const notes=(step.notes||[]).map(n=>`<div class="muted">${esc(n)}</div>`).join('');
    const decisionDetails=(step.investigations||[]).map(i=>{const h=i.supportHandoff||{};return `<details><summary>${esc(i.actionName||'PulseGuard recommendation')} | ${esc(i.policyDecision||'')}</summary><div class="muted">Incident ${esc(i.incidentId||'')}</div><p><b>PulseGuard recommendation:</b> ${esc(i.actionName||'')} (${Math.round(Number(i.confidence||0)*100)}%)</p><p>${esc(i.recommendedAction||'')}</p><p><b>Governance decision:</b> ${esc(i.policyDecision||'')} — ${esc(i.policyReason||'')}</p><p><b>Action taken:</b> ${i.actionExecuted?'Yes':'No'} | ${esc(i.actionExecutionStatus||'NOT_EVALUATED')} | ${esc(i.actionExecutor||'-')}</p><p><b>Repair outcome:</b> ${esc(i.repairOutcome||step.repairOutcome||'RECOMMENDED_ONLY')}</p>${h.primaryQueue?`<p><b>Support handoff:</b> ${esc(h.primaryQueue)} | ${esc(h.status||'ASSIGNED')} | confidence ${Math.round(Number(h.assignmentConfidence||0)*100)}%</p><p>${esc(h.routingReason||h.assignmentReason||'')}</p>`:''}<pre>${esc(JSON.stringify(i.actionExecutionResult||{},null,2))}</pre></details>`}).join('');
    return `<tr><td>${step.index}</td><td><b>${esc(step.displayName)}</b><br><span class="muted">${esc(step.disturbance)}</span>${decisionDetails}</td><td>${step.injectionSucceeded?yesNo(true):step.status==='PENDING'?'Pending':yesNo(false)}</td><td>${detection}</td><td>${investigation}</td><td>${classification}</td><td>${aiDecision}</td><td>${policy}</td><td>${actionTaken}</td><td>${recovery}</td><td>${repairOutcome}</td><td>${supportQueue}</td><td><b class="outcome-${esc(step.outcome||'')}">${esc(step.outcome||step.status)}</b>${notes}${step.error?`<div class="bad">${esc(step.error)}</div>`:''}</td></tr>`;
  }).join('')||'<tr><td colspan="13" class="muted">No steps.</td></tr>';
  document.getElementById('testRunJson').textContent=JSON.stringify(run,null,2);
}
async function refreshTestRuns(forceDetail=false){
  try{
    const r=await fetch('/test-runs?limit=20'); const body=await r.json(); if(!r.ok) throw new Error(JSON.stringify(body));
    renderRuns(body.runs||[]);
    if(selectedRunId){
      const d=await fetch(`/test-runs/${selectedRunId}`); const run=await d.json(); if(!d.ok) throw new Error(JSON.stringify(run)); renderSelectedRun(run);
    }
  }catch(e){document.getElementById('testMessage').textContent=`Test summary refresh failed: ${e}`;}
}
setInterval(refreshState,2000);
setInterval(()=>refreshTestRuns(false),2500);
refreshState(); refreshTestRuns(true);
</script>
<script>loadPulseGuardWidget();</script>
</main></body></html>
"""


@app.get("/health")
async def health() -> dict[str, object]:
    if not ready:
        raise HTTPException(status_code=503, detail="Scenario controller is initializing.")
    return {"status": "healthy", "service": "scenario-controller", "state": current_state}


@app.get("/state")
async def state() -> dict[str, object]:
    async with httpx.AsyncClient(timeout=5) as client:
        proxies_response = await client.get(f"{TOXIPROXY_API_URL}/proxies")
        proxies_response.raise_for_status()
        corruption_response = await client.get(f"{CORRUPTION_ADAPTER_URL}/mode")
        corruption_response.raise_for_status()
    traffic_override = await get_traffic_override()
    await refresh_expiring_state(traffic_override)
    return {
        **current_state,
        "ready": ready,
        "corruption": corruption_response.json(),
        "trafficOverride": traffic_override,
        "paymentNodes": await get_node_diagnostics(),
        "proxies": proxies_response.json(),
    }


@app.get("/scenarios")
async def scenarios() -> dict[str, object]:
    return {
        "scenarios": [
            "traffic-spike",
            "node-offline",
            "node-flapping",
            "intermittent-network",
            "shared-dependency-outage",
            "payment-latency",
            "payment-timeout",
            "payment-reset",
            "external-latency",
            "external-timeout",
            "payload-corruption/{mode}",
            "disk-pressure",
            "certificate-expiring",
            "certificate-renewal-failure",
            "reset",
        ],
        "paymentNodes": sorted(PAYMENT_NODE_URLS),
        "payloadModes": [
            "missing_target",
            "wrong_type",
            "stale_timestamp",
            "outlier",
            "malformed_json",
            "http_503",
        ],
    }


@app.get("/test-runs/catalog")
async def test_run_catalog() -> dict[str, Any]:
    return {
        "disturbances": TEST_SCENARIOS,
        "notes": [
            "Only operational disturbances expected to affect payment telemetry are included.",
            "Signal-path and payload-quality tests remain available on the Manual scenarios tab.",
            "Detected and correctly classified are separate measures.",
        ],
    }


@app.get("/test-runs")
async def list_test_runs(limit: int = Query(default=20, ge=1, le=100)) -> dict[str, Any]:
    runs = list(reversed(test_runs[-limit:]))
    return {
        "count": len(runs),
        "activeRunId": next((item.get("id") for item in runs if item.get("status") in {"QUEUED", "RUNNING", "STOPPING"}), None),
        "runs": [
            {
                "id": item.get("id"),
                "mode": item.get("mode"),
                "seed": item.get("seed"),
                "status": item.get("status"),
                "createdAt": item.get("createdAt"),
                "startedAt": item.get("startedAt"),
                "completedAt": item.get("completedAt"),
                "updatedAt": item.get("updatedAt"),
                "currentStep": item.get("currentStep"),
                "message": item.get("message"),
                "summary": item.get("summary", {}),
            }
            for item in runs
        ],
    }


@app.get("/test-runs/{run_id}")
async def get_test_run(run_id: str) -> dict[str, Any]:
    return find_test_run(run_id)


@app.post("/test-runs/random")
async def start_random_test(
    count: int = Query(default=5, ge=1, le=len(TEST_SCENARIOS)),
    seed: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    return create_test_run("random", count, seed)


@app.post("/test-runs/full")
async def start_full_test(seed: int | None = Query(default=None, ge=1)) -> dict[str, Any]:
    return create_test_run("full", None, seed)


@app.post("/test-runs/{run_id}/stop")
async def stop_test_run(run_id: str) -> dict[str, Any]:
    global test_run_stop_requested
    run = find_test_run(run_id)
    if run.get("status") not in {"QUEUED", "RUNNING", "STOPPING"}:
        raise HTTPException(status_code=409, detail="Test run is not active")
    test_run_stop_requested = True
    run["status"] = "STOPPING"
    run["message"] = "Stop requested. The runner is resetting all faults."
    persist_run(run)
    return run


@app.post("/scenarios/reset")
async def reset() -> dict[str, object]:
    await clean_before_scenario()
    update_state("none")
    return current_state


@app.post("/scenarios/traffic-spike")
async def traffic_spike(
    multiplier: float = Query(default=4.0, ge=1.1, le=10.0),
    duration_seconds: int = Query(default=90, ge=10, le=600),
) -> dict[str, object]:
    await clean_before_scenario()
    override = await set_traffic_override(multiplier, duration_seconds)
    update_state(
        "traffic_spike",
        multiplier=multiplier,
        durationSeconds=duration_seconds,
        expiresAt=override.get("expiresAt"),
        maxTargetUsers=override.get("maxTargetUsers"),
        source="live Wikimedia traffic profile",
    )
    return current_state


@app.post("/scenarios/capacity-failover-scale")
async def capacity_failover_scale(
    multiplier: float = Query(default=4.0, ge=2.0, le=6.0),
    duration_seconds: int = Query(default=210, ge=90, le=360),
    pressure_ms: int = Query(default=1000, ge=700, le=1800),
) -> dict[str, object]:
    await clean_before_scenario()
    node3 = await set_node_fault("payment-node-3", "unavailable")
    peer_capacity = await asyncio.gather(
        set_node_capacity("payment-node-1", 1, pressure_ms, "scenario_pressure"),
        set_node_capacity("payment-node-2", 1, pressure_ms, "scenario_pressure"),
    )
    override = await set_traffic_override(multiplier, duration_seconds)
    update_state(
        "capacity_failover_scale",
        unavailableNode="payment-node-3",
        remainingNodes=["payment-node-1", "payment-node-2"],
        initialCapacityUnits=1,
        capacityPressureMs=pressure_ms,
        trafficMultiplier=multiplier,
        durationSeconds=duration_seconds,
        expiresAt=override.get("expiresAt"),
        node3Diagnostics=node3,
        peerDiagnostics=peer_capacity,
        simulatedInfrastructure=True,
        scope="bounded application worker capacity; no Docker socket or host CPU/memory mutation",
    )
    return current_state


@app.post("/scenarios/node-offline")
async def node_offline(node_id: str = Query(default="payment-node-3")) -> dict[str, object]:
    await clean_before_scenario()
    diagnostics = await set_node_fault(node_id, "unavailable")
    update_state(
        "node_offline",
        target=node_id,
        implementation="controlled application fault; process remains running",
        diagnostics=diagnostics,
    )
    return current_state


@app.post("/scenarios/node-flapping")
async def node_flapping(
    node_id: str = Query(default="payment-node-3"),
    duration_seconds: int = Query(default=60, ge=20, le=300),
    interval_seconds: int = Query(default=5, ge=2, le=30),
) -> dict[str, object]:
    global background_scenario_task
    if node_id not in PAYMENT_NODE_URLS:
        raise HTTPException(status_code=404, detail=f"Unknown payment node: {node_id}")
    await clean_before_scenario()
    update_state(
        "node_flapping",
        target=node_id,
        durationSeconds=duration_seconds,
        intervalSeconds=interval_seconds,
        phase="starting",
        cycle=0,
    )
    background_scenario_task = asyncio.create_task(
        run_node_flapping(node_id, duration_seconds, interval_seconds),
        name=f"opsai-flap-{node_id}",
    )
    return current_state


@app.post("/scenarios/intermittent-network")
async def intermittent_network(
    toxicity: float = Query(default=0.3, ge=0.05, le=0.95),
    timeout_ms: int = Query(default=100, ge=0, le=10000),
) -> dict[str, object]:
    await clean_before_scenario()
    await add_toxic(
        PAYMENT_PROXY_NAME,
        "payment_intermittent_reset_downstream",
        "reset_peer",
        "downstream",
        {"timeout": timeout_ms},
        toxicity=toxicity,
    )
    update_state(
        "intermittent_network",
        target="payment-node-3",
        toxicity=toxicity,
        approximateFailurePercent=round(toxicity * 100),
        implementation="probabilistic connection resets; packet-loss-like, not raw packet loss",
    )
    return current_state


@app.post("/scenarios/shared-dependency-outage")
async def shared_dependency_outage() -> dict[str, object]:
    await clean_before_scenario()
    results = await asyncio.gather(
        *[set_node_fault(node_id, "shared_dependency") for node_id in PAYMENT_NODE_URLS]
    )
    update_state(
        "shared_dependency_outage",
        target="payment-authorisation-provider",
        affectedNodes=sorted(PAYMENT_NODE_URLS),
        implementation="all payment nodes return the same controlled dependency failure",
        diagnostics=results,
    )
    return current_state


@app.post("/scenarios/payment-latency")
async def payment_latency(
    latency_ms: int = Query(default=1200, ge=100, le=10000),
    jitter_ms: int = Query(default=150, ge=0, le=5000),
) -> dict[str, object]:
    await clean_before_scenario()
    await add_toxic(
        PAYMENT_PROXY_NAME,
        "payment_latency_downstream",
        "latency",
        "downstream",
        {"latency": latency_ms, "jitter": jitter_ms},
    )
    update_state("payment_latency", latencyMs=latency_ms, jitterMs=jitter_ms, target="payment-node-3")
    return current_state


@app.post("/scenarios/payment-timeout")
async def payment_timeout() -> dict[str, object]:
    await clean_before_scenario()
    diagnostics = await set_node_fault("payment-node-3", "timeout")
    update_state("payment_timeout", target="payment-node-3", implementation="application call exceeds the router timeout", diagnostics=diagnostics)
    return current_state


@app.post("/scenarios/payment-reset")
async def payment_reset(timeout_ms: int = Query(default=100, ge=0, le=10000)) -> dict[str, object]:
    await clean_before_scenario()
    await add_toxic(
        PAYMENT_PROXY_NAME,
        "payment_reset_downstream",
        "reset_peer",
        "downstream",
        {"timeout": timeout_ms},
    )
    update_state("payment_reset", timeoutMs=timeout_ms, target="payment-node-3")
    return current_state


@app.post("/scenarios/disk-pressure")
async def disk_pressure(cleanup_insufficient: bool = Query(default=False)) -> dict[str, object]:
    await clean_before_scenario()
    result = await automation_post("/scenarios/disk-pressure", {"target_percent": 92, "cleanup_insufficient": str(cleanup_insufficient).lower()})
    update_state("disk_pressure", target="opsai-demo-storage", automation=result)
    return current_state


@app.post("/scenarios/certificate-expiring")
async def certificate_expiring() -> dict[str, object]:
    await clean_before_scenario()
    result = await automation_post("/scenarios/certificate-expiring", {"seconds": 300})
    update_state("certificate_expiring", target="checkout.pulseguard.local", automation=result)
    return current_state


@app.post("/scenarios/certificate-renewal-failure")
async def certificate_renewal_failure() -> dict[str, object]:
    await clean_before_scenario()
    result = await automation_post("/scenarios/certificate-renewal-failure", {"seconds": 300})
    update_state("certificate_renewal_failure", target="checkout.pulseguard.local", automation=result)
    return current_state


@app.post("/scenarios/external-auth-failure")
async def external_auth_failure() -> dict[str, object]:
    await clean_before_scenario()
    result = await invalidate_external_auth_token()
    update_state(
        "external_auth_failure",
        target="partner-risk-service",
        implementation="The external service rotates its bearer token while checkout retains the previous generation.",
        secretExposed=False,
        externalService=result,
    )
    return current_state


@app.post("/scenarios/payment-node-hung")
async def payment_node_hung(
    node_id: str = Query(default="payment-node-3"),
) -> dict[str, object]:
    await clean_before_scenario()
    diagnostics = await set_node_fault(node_id, "hung")
    update_state(
        "payment_node_hung",
        target=node_id,
        implementation="Bounded application worker hang; the container and health endpoint remain reachable.",
        expectedAction="restart_payment_node",
        governance="APPROVAL_REQUIRED",
        diagnostics=diagnostics,
    )
    return current_state


@app.post("/scenarios/predictive-disk-growth")
async def predictive_disk_growth(
    start_percent: float = Query(default=35, ge=5, le=70),
    end_percent: float = Query(default=79, ge=50, le=84),
    duration_seconds: int = Query(default=120, ge=45, le=600),
) -> dict[str, object]:
    await clean_before_scenario()
    result = await automation_post(
        "/scenarios/disk-growth",
        {
            "start_percent": start_percent,
            "end_percent": end_percent,
            "duration_seconds": duration_seconds,
        },
    )
    update_state(
        "predictive_disk_growth",
        target="opsai-demo-storage",
        observationOnly=True,
        reactiveThresholdPercent=85,
        predictorUrl="http://localhost:8098",
        automation=result,
    )
    return current_state


@app.post("/scenarios/predictive-node-degradation")
async def predictive_node_degradation(
    node_id: str = Query(default="payment-node-2"),
    start_pressure_ms: int = Query(default=100, ge=0, le=500),
    end_pressure_ms: int = Query(default=650, ge=400, le=760),
    duration_seconds: int = Query(default=120, ge=60, le=600),
) -> dict[str, object]:
    global background_scenario_task
    await clean_before_scenario()
    update_state(
        "predictive_node_degradation",
        target=node_id,
        startPressureMs=start_pressure_ms,
        endPressureMs=end_pressure_ms,
        durationSeconds=duration_seconds,
        observationOnly=True,
        reactiveLatencyThresholdSeconds=0.8,
        predictorUrl="http://localhost:8098",
    )
    background_scenario_task = asyncio.create_task(
        run_predictive_node_degradation(
            node_id,
            start_pressure_ms,
            end_pressure_ms,
            duration_seconds,
            False,
        ),
        name=f"opsai-predictive-node-{node_id}",
    )
    return current_state


@app.post("/scenarios/predictive-capacity-risk")
async def predictive_capacity_risk(
    start_pressure_ms: int = Query(default=100, ge=0, le=500),
    end_pressure_ms: int = Query(default=650, ge=400, le=760),
    duration_seconds: int = Query(default=150, ge=90, le=600),
) -> dict[str, object]:
    global background_scenario_task
    await clean_before_scenario()
    update_state(
        "predictive_capacity_risk",
        unavailableNode="payment-node-3",
        remainingNodes=["payment-node-1", "payment-node-2"],
        startPressureMs=start_pressure_ms,
        endPressureMs=end_pressure_ms,
        durationSeconds=duration_seconds,
        trafficMultiplier=3.0,
        observationOnly=True,
        predictorUrl="http://localhost:8098",
    )
    background_scenario_task = asyncio.create_task(
        run_predictive_node_degradation(
            "payment-fleet",
            start_pressure_ms,
            end_pressure_ms,
            duration_seconds,
            True,
        ),
        name="opsai-predictive-capacity-risk",
    )
    return current_state


@app.post("/scenarios/external-latency")
async def external_latency(
    latency_ms: int = Query(default=2500, ge=100, le=15000),
    jitter_ms: int = Query(default=300, ge=0, le=5000),
) -> dict[str, object]:
    await clean_before_scenario()
    await add_toxic(
        TRAFFIC_PROXY_NAME,
        "external_latency_downstream",
        "latency",
        "downstream",
        {"latency": latency_ms, "jitter": jitter_ms},
    )
    update_state("external_latency", latencyMs=latency_ms, jitterMs=jitter_ms, target="traffic-profile")
    return current_state


@app.post("/scenarios/external-timeout")
async def external_timeout(timeout_ms: int = Query(default=500, ge=0, le=10000)) -> dict[str, object]:
    await clean_before_scenario()
    await add_toxic(
        TRAFFIC_PROXY_NAME,
        "external_timeout_downstream",
        "timeout",
        "downstream",
        {"timeout": timeout_ms},
    )
    update_state("external_timeout", timeoutMs=timeout_ms, target="traffic-profile")
    return current_state


@app.post("/scenarios/payload-corruption/{mode}")
async def payload_corruption(mode: str) -> dict[str, object]:
    allowed = {
        "missing_target",
        "wrong_type",
        "stale_timestamp",
        "outlier",
        "malformed_json",
        "http_503",
    }
    if mode not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported corruption mode: {mode}")
    await clean_before_scenario()
    await set_corruption_mode(mode)
    update_state("payload_corruption", mode=mode, target="traffic-profile")
    return current_state
