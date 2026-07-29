from __future__ import annotations

import asyncio
import json
import math
import os
import ssl
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI
from prometheus_client import Counter, Gauge, make_asgi_app

SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.4.0")
STREAM_URL = os.getenv("WIKIMEDIA_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
USER_AGENT = os.getenv("WIKIMEDIA_USER_AGENT", "PulseGuard/0.4.0")
WINDOW_SECONDS = max(10, int(os.getenv("WINDOW_SECONDS", "30")))
STALE_AFTER_SECONDS = max(10, int(os.getenv("STALE_AFTER_SECONDS", "30")))
MIN_USERS = max(1, int(os.getenv("MIN_USERS", "10")))
BASE_USERS = max(MIN_USERS, int(os.getenv("BASE_USERS", "25")))
MAX_USERS = max(BASE_USERS, int(os.getenv("MAX_USERS", "80")))
FALLBACK_USERS = min(MAX_USERS, max(MIN_USERS, int(os.getenv("FALLBACK_USERS", "15"))))
TLS_VERIFY_SETTING = os.getenv("WIKIMEDIA_TLS_VERIFY", "true").strip().lower()
TLS_VERIFY_ENABLED = TLS_VERIFY_SETTING not in {"0", "false", "no", "off"}
CA_BUNDLE = os.getenv("WIKIMEDIA_CA_BUNDLE", "").strip()
HTTPX_VERIFY: bool | ssl.SSLContext = (
    ssl.create_default_context(cafile=CA_BUNDLE) if CA_BUNDLE else TLS_VERIFY_ENABLED
)
TLS_MODE = "custom-ca" if CA_BUNDLE else ("system-ca" if TLS_VERIFY_ENABLED else "disabled")

EVENTS_TOTAL = Counter("opsai_wikimedia_events_total", "Valid Wikimedia events received.")
CANARY_TOTAL = Counter("opsai_wikimedia_canary_events_total", "Canary events discarded.")
INVALID_TOTAL = Counter("opsai_wikimedia_invalid_events_total", "Events that could not be parsed.")
DUPLICATE_TOTAL = Counter("opsai_wikimedia_duplicate_events_total", "Duplicate events discarded.")
RECONNECT_TOTAL = Counter("opsai_wikimedia_reconnects_total", "Wikimedia SSE reconnect attempts.")
EVENTS_PER_MINUTE = Gauge("opsai_wikimedia_events_per_minute", "Current rolling event rate.")
BASELINE_EVENTS_PER_MINUTE = Gauge("opsai_wikimedia_baseline_events_per_minute", "Slow moving event-rate baseline.")
TARGET_USERS = Gauge("opsai_wikimedia_target_users", "Locust users requested by the live signal.")
LAST_EVENT_AGE = Gauge("opsai_wikimedia_last_event_age_seconds", "Age of the latest valid live event.")
STREAM_CONNECTED = Gauge("opsai_wikimedia_stream_connected", "Whether the SSE connection is currently open.")

app = FastAPI(title="PulseGuard Wikimedia Traffic Adapter", version=SERVICE_VERSION)
app.mount("/metrics", make_asgi_app())

state_lock = asyncio.Lock()
event_times: deque[float] = deque()
seen_ids: set[str] = set()
seen_order: deque[str] = deque()
last_event_monotonic: float | None = None
last_event_at: str | None = None
last_sse_id: str | None = None
latest_event: dict[str, Any] | None = None
stream_connected = False
baseline_epm: float | None = None
smoothed_target = float(BASE_USERS)
current_target_users = FALLBACK_USERS
current_profile_name = "fallback"
current_source_mode = "fallback"
background_tasks: list[asyncio.Task[Any]] = []
last_connection_error: str | None = None
last_connection_attempt_at: str | None = None
last_connection_success_at: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_log(level: str, event: str, **fields: object) -> None:
    print(
        json.dumps(
            {
                "timestamp": utc_now(),
                "level": level,
                "service": "wikimedia-adapter",
                "version": SERVICE_VERSION,
                "event": event,
                **fields,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )


def remember_id(event_id: str) -> bool:
    if event_id in seen_ids:
        return False
    seen_ids.add(event_id)
    seen_order.append(event_id)
    while len(seen_order) > 5000:
        old_id = seen_order.popleft()
        seen_ids.discard(old_id)
    return True


async def process_event(raw_data: str, sse_id: str | None) -> None:
    global last_event_monotonic, last_event_at, latest_event, last_sse_id
    try:
        payload = json.loads(raw_data)
    except json.JSONDecodeError:
        INVALID_TOTAL.inc()
        return

    if payload.get("meta", {}).get("domain") == "canary":
        CANARY_TOTAL.inc()
        return

    event_id = str(sse_id or payload.get("meta", {}).get("id") or payload.get("id") or "")
    if event_id and not remember_id(event_id):
        DUPLICATE_TOTAL.inc()
        return

    now_mono = time.monotonic()
    now_iso = utc_now()
    summary = {
        "id": event_id or None,
        "wiki": payload.get("wiki"),
        "type": payload.get("type"),
        "title": str(payload.get("title") or "")[:180],
        "serverName": payload.get("server_name"),
        "bot": bool(payload.get("bot", False)),
        "observedAt": now_iso,
    }

    async with state_lock:
        event_times.append(now_mono)
        last_event_monotonic = now_mono
        last_event_at = now_iso
        latest_event = summary
        if sse_id:
            last_sse_id = sse_id
    EVENTS_TOTAL.inc()


async def consume_stream() -> None:
    global stream_connected, last_connection_error, last_connection_attempt_at, last_connection_success_at
    backoff_seconds = 1.0
    timeout = httpx.Timeout(connect=10, read=None, write=10, pool=10)

    while True:
        headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
        async with state_lock:
            if last_sse_id:
                headers["Last-Event-ID"] = last_sse_id
        try:
            last_connection_attempt_at = utc_now()
            async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, verify=HTTPX_VERIFY) as client:
                async with client.stream("GET", STREAM_URL, headers=headers) as response:
                    response.raise_for_status()
                    stream_connected = True
                    last_connection_error = None
                    last_connection_success_at = utc_now()
                    STREAM_CONNECTED.set(1)
                    emit_log(
                        "INFO",
                        "wikimedia_stream_connected",
                        url=STREAM_URL,
                        tlsMode=TLS_MODE,
                    )
                    backoff_seconds = 1.0
                    current_id: str | None = None
                    data_lines: list[str] = []
                    async for line in response.aiter_lines():
                        if line == "":
                            if data_lines:
                                await process_event("\n".join(data_lines), current_id)
                            current_id = None
                            data_lines = []
                            continue
                        if line.startswith(":"):
                            continue
                        if line.startswith("id:"):
                            current_id = line[3:].strip()
                        elif line.startswith("data:"):
                            data_lines.append(line[5:].lstrip())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            last_connection_error = str(exc)[:500]
            emit_log(
                "WARNING",
                "wikimedia_stream_disconnected",
                error=last_connection_error,
                retryInSeconds=backoff_seconds,
                tlsMode=TLS_MODE,
            )
        finally:
            stream_connected = False
            STREAM_CONNECTED.set(0)

        RECONNECT_TOTAL.inc()
        await asyncio.sleep(backoff_seconds)
        backoff_seconds = min(30.0, backoff_seconds * 2)


async def profile_monitor() -> None:
    global baseline_epm, smoothed_target, current_target_users, current_profile_name, current_source_mode
    while True:
        now_mono = time.monotonic()
        async with state_lock:
            while event_times and event_times[0] < now_mono - WINDOW_SECONDS:
                event_times.popleft()
            current_epm = len(event_times) * 60.0 / WINDOW_SECONDS
            age = None if last_event_monotonic is None else max(0.0, now_mono - last_event_monotonic)

        live = stream_connected and age is not None and age <= STALE_AFTER_SECONDS and current_epm > 0
        if live:
            if baseline_epm is None:
                baseline_epm = current_epm
            else:
                baseline_epm = 0.97 * baseline_epm + 0.03 * current_epm
            ratio = current_epm / max(1.0, baseline_epm)
            bounded_ratio = min(MAX_USERS / BASE_USERS, max(MIN_USERS / BASE_USERS, ratio))
            requested = BASE_USERS * bounded_ratio
            smoothed_target = 0.70 * smoothed_target + 0.30 * requested
            target = min(MAX_USERS, max(MIN_USERS, int(round(smoothed_target))))
        else:
            target = FALLBACK_USERS

        if live and baseline_epm:
            current_ratio = current_epm / max(1.0, baseline_epm)
            if current_ratio < 0.75:
                current_profile_name = "quiet"
            elif current_ratio < 1.25:
                current_profile_name = "normal"
            elif current_ratio < 1.75:
                current_profile_name = "busy"
            else:
                current_profile_name = "surge"
            current_source_mode = "live"
        else:
            current_profile_name = "fallback"
            current_source_mode = "fallback"
        current_target_users = target

        EVENTS_PER_MINUTE.set(current_epm)
        BASELINE_EVENTS_PER_MINUTE.set(baseline_epm or 0)
        TARGET_USERS.set(target)
        LAST_EVENT_AGE.set(age if age is not None else STALE_AFTER_SECONDS * 10)
        await asyncio.sleep(2)


async def build_profile() -> dict[str, Any]:
    now_mono = time.monotonic()
    async with state_lock:
        while event_times and event_times[0] < now_mono - WINDOW_SECONDS:
            event_times.popleft()
        current_epm = len(event_times) * 60.0 / WINDOW_SECONDS
        age = None if last_event_monotonic is None else max(0.0, now_mono - last_event_monotonic)
        event_snapshot = latest_event.copy() if latest_event else None
        connected = stream_connected
        observed_at = last_event_at

    live = connected and age is not None and age <= STALE_AFTER_SECONDS and current_epm > 0
    ratio = current_epm / max(1.0, baseline_epm or current_epm or 1.0) if live else 0.0
    target = current_target_users
    profile_name = current_profile_name
    source_mode = current_source_mode

    return {
        "generatedAt": utc_now(),
        "sourceMode": source_mode,
        "streamConnected": connected,
        "profile": profile_name,
        "targetUsers": target,
        "spawnRate": max(1, min(20, math.ceil(target / 5))),
        "currentEventsPerMinute": round(current_epm, 2),
        "baselineEventsPerMinute": round(baseline_epm or 0.0, 2),
        "activityRatio": round(ratio, 3),
        "lastEventAt": observed_at,
        "lastEventAgeSeconds": None if age is None else round(age, 2),
        "windowSeconds": WINDOW_SECONDS,
        "latestEvent": event_snapshot,
        "connectionStatus": "connected" if connected else "disconnected",
        "connectionError": last_connection_error,
        "lastConnectionAttemptAt": last_connection_attempt_at,
        "lastConnectionSuccessAt": last_connection_success_at,
        "tlsVerificationEnabled": TLS_VERIFY_ENABLED,
        "tlsMode": TLS_MODE,
        "caBundleConfigured": bool(CA_BUNDLE),
        "streamUrl": STREAM_URL,
        "fallbackReason": (
            None
            if live
            else ("stream_connection_error" if last_connection_error else "stream_not_live")
        ),
    }


@app.on_event("startup")
async def startup_event() -> None:
    background_tasks.extend(
        [asyncio.create_task(consume_stream()), asyncio.create_task(profile_monitor())]
    )
    emit_log(
        "INFO",
        "service_started",
        streamUrl=STREAM_URL,
        tlsMode=TLS_MODE,
        tlsVerificationEnabled=TLS_VERIFY_ENABLED,
        caBundleConfigured=bool(CA_BUNDLE),
    )


@app.on_event("shutdown")
async def shutdown_event() -> None:
    for task in background_tasks:
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "wikimedia-adapter", "version": SERVICE_VERSION}


@app.get("/health")
async def health() -> dict[str, Any]:
    profile = await build_profile()
    return {
        "status": "healthy",
        "service": "wikimedia-adapter",
        "version": SERVICE_VERSION,
        "sourceMode": profile["sourceMode"],
        "streamConnected": profile["streamConnected"],
        "connectionError": profile["connectionError"],
        "tlsMode": profile["tlsMode"],
    }


@app.get("/profile")
async def profile() -> dict[str, Any]:
    return await build_profile()


@app.get("/events/latest")
async def latest() -> dict[str, Any]:
    profile = await build_profile()
    return {"sourceMode": profile["sourceMode"], "latestEvent": profile["latestEvent"]}
