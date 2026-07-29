# Day 4 architecture

```mermaid
flowchart LR
    W[Wikimedia EventStreams] --> WA[Wikimedia Adapter]
    WA --> C[Payload Corruption Adapter]
    C --> TP1[Toxiproxy: traffic profile]
    TP1 --> L[Locust dynamic load shape]
    L --> CO[Checkout Service]
    CO --> R[Payment Router]
    R --> P1[Payment Node 1]
    R --> P2[Payment Node 2]
    R --> TP2[Toxiproxy: node 3 path]
    TP2 --> P3[Payment Node 3]

    S[Scenario Controller] --> C
    S --> TP1
    S --> TP2

    CO --> M[Prometheus]
    R --> M
    P1 --> M
    P2 --> M
    P3 --> M
    WA --> M
    C --> M
    S --> M
    TP1 --> M
    M --> G[Grafana]
```

## Realism principles

- Only one process consumes the public Wikimedia stream.
- Public events are not sent directly to the retail application.
- The rolling activity rate controls the number of synthetic Locust users.
- Corruption is injected locally, so no public system is harmed.
- Toxiproxy changes real TCP behavior; metrics measure the actual consequences.
- Invalid, stale, or unavailable traffic profiles are rejected and Locust falls back safely.
- Payment node 3 sits behind a separate Toxiproxy path, allowing real latency, timeout, and connection-reset scenarios.
