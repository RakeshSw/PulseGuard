# PulseGuard v0.6.1 — Range Forecasting and Recurring Issue Intelligence

## Prediction data flow

Services expose metrics continuously. Prometheus stores time-series history. PulseGuard Predictor queries bounded historical windows using `/api/v1/query_range`.

The predictor calculates trend slope, R-squared, current value, threshold distance and estimated time to threshold. The real AI agent is contacted only when a new deterministic trigger is satisfied. Metrics are never streamed continuously to the LLM.

## Default calculation window

- Range window: 15 minutes
- Query step: 15 seconds
- Minimum valid samples: 12
- Evaluation interval: 15 seconds

## Prediction triggers

### Disk pressure

Linear regression over `opsai_demo_disk_usage_percent`.

Prediction requires:

- current usage below 85%
- positive growth of at least 0.15 percentage points per minute
- forecast crossing within 30 minutes

### Isolated payment-node degradation

Linear regression per node over payment-processing p95.

Prediction requires:

- current p95 below 0.8 seconds
- positive slope of at least 0.025 seconds per minute
- estimated threshold crossing within 15 minutes
- node-to-peer latency ratio of at least 1.35

### Capacity saturation

Uses the payment-processing range forecast plus current node availability and capacity units.

Prediction requires:

- payment-node-3 unavailable
- payment-node-1 and payment-node-2 at one unit
- both peers forecast to reach 0.8 seconds within 15 minutes

### Certificate expiry

Uses certificate expiry history and countdown calculation. The AI is contacted once when the certificate enters the renewal horizon.

## Recurring issue intelligence

Every minute, PulseGuard Predictor reads incident history from PulseGuard Core.

Incidents are grouped by:

- incident type
- node when present, otherwise service

A potential recurring problem is highlighted when the same group occurs at least twice in the last 24 hours. The dashboard shows count, average interval, incident IDs, severity and an AI-generated problem-management recommendation.

This does not declare a root cause automatically. It identifies a pattern that merits common-cause investigation.

## Predictive Console

Open `http://localhost:8098`.

The console displays:

- calculation catalogue and thresholds
- exact Prometheus query/window/step/minimum samples
- active and historical predictions
- current value, threshold, slope, R², ETA and sample count
- sparkline with threshold
- the exact rule that caused the real AI call
- latest AI contact
- frequent issue patterns and recurrence calculations
