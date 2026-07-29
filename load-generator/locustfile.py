from __future__ import annotations

import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from locust import HttpUser, LoadTestShape, between, task

logger = logging.getLogger("opsai.traffic-profile")

MIN_WAIT = float(os.getenv("LOCUST_MIN_WAIT_SECONDS", "1"))
MAX_WAIT = float(os.getenv("LOCUST_MAX_WAIT_SECONDS", "3"))
TRAFFIC_MODE = os.getenv("TRAFFIC_MODE", "wikimedia").lower()
PROFILE_URL = os.getenv("TRAFFIC_PROFILE_URL", "http://toxiproxy:8666/profile")
PROFILE_POLL_SECONDS = max(2.0, float(os.getenv("TRAFFIC_PROFILE_POLL_SECONDS", "5")))
FALLBACK_USERS = max(1, int(os.getenv("TRAFFIC_FALLBACK_USERS", "15")))
MAX_USERS = max(FALLBACK_USERS, int(os.getenv("TRAFFIC_MAX_USERS", "80")))
DEFAULT_SPAWN_RATE = max(1.0, float(os.getenv("TRAFFIC_SPAWN_RATE", "10")))

PRODUCTS = [
    ("PROD-101", 19.99),
    ("PROD-102", 29.50),
    ("PROD-103", 49.99),
    ("PROD-104", 79.00),
    ("PROD-105", 149.99),
]


class CheckoutUser(HttpUser):
    wait_time = between(MIN_WAIT, MAX_WAIT)

    @task
    def submit_checkout(self) -> None:
        selected_products = random.sample(PRODUCTS, k=random.randint(1, min(3, len(PRODUCTS))))
        items = [
            {
                "productId": product_id,
                "quantity": random.randint(1, 3),
                "unitPrice": unit_price,
            }
            for product_id, unit_price in selected_products
        ]
        payload = {
            "customerId": f"CUS-{uuid.uuid4().hex[:8].upper()}",
            "currency": "USD",
            "items": items,
        }
        with self.client.post("/checkout", json=payload, name="/checkout", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(
                    f"Checkout failed with HTTP {response.status_code}: {response.text[:300]}"
                )
                return
            try:
                body = response.json()
            except ValueError:
                response.failure("Checkout returned invalid JSON.")
                return
            if body.get("status") != "completed":
                response.failure(f"Unexpected checkout status: {body.get('status')}")
            else:
                response.success()


class WikimediaTrafficShape(LoadTestShape):
    """Poll a validated live traffic profile and safely fall back on any fault."""

    abstract = False

    def __init__(self) -> None:
        super().__init__()
        self.target_users = FALLBACK_USERS
        self.spawn_rate = DEFAULT_SPAWN_RATE
        self.last_poll = 0.0
        self.last_profile_mode = "fallback"

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        if not isinstance(value, str):
            raise ValueError("generatedAt must be an ISO timestamp string")
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _poll_profile(self) -> None:
        try:
            response = requests.get(PROFILE_URL, timeout=2.0)
            response.raise_for_status()
            profile = response.json()

            target = profile.get("targetUsers")
            if isinstance(target, bool) or not isinstance(target, int):
                raise ValueError("targetUsers must be an integer")
            if not 1 <= target <= MAX_USERS:
                raise ValueError(f"targetUsers outside safe range: {target}")

            generated_at = self._parse_timestamp(profile.get("generatedAt"))
            now = datetime.now(timezone.utc)
            age_seconds = (now - generated_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds < -5 or age_seconds > 30:
                raise ValueError(f"profile is stale or future-dated: {age_seconds:.1f}s")

            spawn_rate = profile.get("spawnRate", DEFAULT_SPAWN_RATE)
            if isinstance(spawn_rate, bool) or not isinstance(spawn_rate, (int, float)):
                raise ValueError("spawnRate must be numeric")
            spawn_rate = min(20.0, max(1.0, float(spawn_rate)))

            self.target_users = target
            self.spawn_rate = spawn_rate
            self.last_profile_mode = str(profile.get("sourceMode", "unknown"))
            logger.info(
                "Accepted traffic profile mode=%s profile=%s users=%s spawn_rate=%s epm=%s",
                self.last_profile_mode,
                profile.get("profile"),
                self.target_users,
                self.spawn_rate,
                profile.get("currentEventsPerMinute"),
            )
        except Exception as exc:
            self.target_users = FALLBACK_USERS
            self.spawn_rate = DEFAULT_SPAWN_RATE
            self.last_profile_mode = "safety-fallback"
            logger.warning(
                "Rejected/unavailable traffic profile; using %s users: %s",
                FALLBACK_USERS,
                exc,
            )

    def tick(self) -> tuple[int, float]:
        now = time.monotonic()
        if TRAFFIC_MODE == "wikimedia" and now - self.last_poll >= PROFILE_POLL_SECONDS:
            self._poll_profile()
            self.last_poll = now
        return self.target_users, self.spawn_rate
