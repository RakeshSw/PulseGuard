# Random disturbance test runner

The Scenario Controller now has two tabs:

- **Manual scenarios** keeps the existing one-click turbulence controls.
- **Random test summary** runs a random selection of operational disturbances and records the actual end-to-end result.

For every selected disturbance, the runner:

1. Resets existing controlled faults.
2. Waits for a clean incident baseline.
3. Injects one disturbance.
4. Waits for a newly opened PulseGuard incident.
5. Waits for the related AI investigation.
6. Records whether the investigation used `REAL_AI`.
7. Compares the actual incident type with the desired classification.
8. Resets the disturbance.
9. Waits for the incident to resolve.

The summary deliberately separates **detected** from **correctly classified**. For example, a shared dependency outage may currently produce generic checkout-failure or per-node timeout incidents. That counts as detection but is marked as a functional fallback until `PAYMENT_SHARED_DEPENDENCY_OUTAGE` exists.

Available automated operational disturbances:

- isolated payment-node-3 latency
- node-3 hard timeout
- node-3 connection reset
- intermittent node-3 network resets
- node 3 unavailable
- node 3 flapping
- shared payment dependency outage
- four-times Wikimedia-derived demand spike

Signal-path delay/timeout and malformed traffic-profile scenarios remain available under **Manual scenarios** because they validate safe fallback behavior rather than the payment-incident AI workflow.

The test history is stored in the Docker volume `scenario-test-data` and survives ordinary container recreation. The active application `.env` and Azure OpenAI key are not copied into the history.
