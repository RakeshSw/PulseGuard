# Correlation, AI decisions and action audit

This patch adds deterministic correlation before incidents are opened:

- PAYMENT_NODE_UNAVAILABLE
- PAYMENT_NODE_NETWORK_INSTABILITY
- PAYMENT_NODE_FLAPPING
- PAYMENT_SHARED_DEPENDENCY_OUTAGE
- PAYMENT_FLEET_CAPACITY_DEGRADATION

It also records three separate stages for every completed investigation:

1. AI recommendation and confidence
2. Deterministic policy decision and reason
3. Action execution status and result

Read-only diagnostic collection is the only action executed automatically in this patch. Drain, restart, rollback, scaling and traffic-control recommendations remain approval-required or blocked. Shared multi-node failures block individual-node drains.

The Random Test Summary tab shows decision, policy and execution separately. A traffic spike that produces no threshold breach is reported as RESILIENCE_PASS rather than a detection gap.