# Day 5 — Predictive Analysis and New Governed Scenarios

PulseGuard v0.6 adds a prediction layer beside the existing reactive detector.

## Responsibility split

- **PulseGuard Core** detects conditions that are already happening using deterministic rules.
- **PulseGuard Predictor** calculates trends, risk, confidence, and time to threshold from Prometheus windows.
- **PulseGuard Agent** explains deterministic forecasts and recommends preventive options; it does not invent probabilities or execute actions.
- **Governance** remains responsible for action authorization.
- **PulseGuard Automation** performs only bounded, allowlisted actions.

## Prediction lifecycle

`OBSERVING → RISK_INCREASING → PREDICTED → RISK_REDUCED`

Day 5 is observation-only. Prediction explanations contain `authorised=false` and `executed=false`.

## Initial prediction types

- `PREDICTED_DISK_PRESSURE`
- `PREDICTED_PAYMENT_NODE_DEGRADATION`
- `PREDICTED_CAPACITY_SATURATION`
- `PREDICTED_CERTIFICATE_EXPIRY`

## External authentication repair

The synthetic `partner-risk-service` requires a bearer token. A scenario rotates the partner token without updating checkout. Repeated 401s create `EXTERNAL_SERVICE_AUTHENTICATION_FAILURE`. PulseGuard may automatically rotate the allowlisted credential, update only checkout, redact the secret, and verify an authenticated probe.

## Service restart scenario

`PAYMENT_NODE_HUNG` simulates an application worker that remains reachable but no longer completes requests. PulseGuard recommends `restart_payment_node`. Governance requires operator approval. The executor performs a bounded **application-level restart simulation** that clears the fault and increments a restart generation; it does not restart Docker, access the Docker socket, run shell commands, or mutate host resources.
