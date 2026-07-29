# PulseGuard Problem Register

## Purpose

The Problem Register converts repeated incident patterns into persistent, auditable
problem-management candidates.

A repeated incident is not automatically treated as a confirmed root cause.

```text
Incident history
→ deterministic recurrence calculation
→ potential problem candidate
→ persistent CANDIDATE record
→ operator review and confirmation
→ investigation and corrective action
→ recurrence monitoring
```

## Deterministic ownership

PulseGuard Predictor owns:

- incident grouping
- occurrence count
- rolling lookback
- average recurrence interval
- affected services and nodes
- linked incident identifiers
- synthetic, organic and unclassified origin counts
- deterministic problem-risk score

REAL_AI may explain why the pattern matters, but it cannot change those facts.

## Record classes

- `DEMO_CANDIDATE`: all linked incidents came from synthetic test scenarios
- `REVIEW_REQUIRED`: at least one incident origin is unclassified
- `OPERATIONAL_CANDIDATE`: at least one linked incident is classified as organic

Synthetic-only evidence is persisted for the portfolio demonstration but must not
be represented as production instability.

## Lifecycle

```text
CANDIDATE
→ UNDER_REVIEW
→ CONFIRMED
→ INVESTIGATING
→ CORRECTIVE_ACTION_PLANNED
→ MONITORING
→ CLOSED
```

Alternative paths:

```text
CANDIDATE / UNDER_REVIEW / INVESTIGATING
→ REJECTED
```

A closed problem may return to `MONITORING` if the issue recurs.

## Stored fields

- Problem ID and stable problem key
- Title and category
- Status and record class
- Deterministic risk score and level
- Owner queue and named owner
- Summary and AI-supported hypothesis
- Confirmed root cause
- Corrective action and monitoring notes
- First and latest occurrence
- Occurrence count and average interval
- Recurrence after corrective action
- Related incident types and scopes
- Linked incident IDs
- Origin breakdown
- Lifecycle event history

## Safety boundary

The predictor can create or refresh only a `CANDIDATE`.

It cannot:

- confirm a problem
- assign an owner
- declare a root cause
- approve corrective action
- close a problem

Those lifecycle changes require an explicit operator action from the Problem
Register UI or API.

## API

PulseGuard Core:

```text
GET  /problems
GET  /problems/summary
GET  /problems/{problem_id}
POST /problems/{problem_id}/assign
POST /problems/{problem_id}/transition
```

Internal candidate ingestion:

```text
POST /api/problems/candidates/upsert
```

Predictive-console proxies:

```text
GET  /problem-candidates
GET  /problem-register
GET  /problem-register/summary
GET  /problem-register/{problem_id}
POST /problem-register/{problem_id}/assign
POST /problem-register/{problem_id}/transition
```

## Validation

```powershell
.\scripts\test-opsai-problem-register-v1.ps1
```

Collect a review bundle:

```powershell
.\scripts\collect-opsai-predictive-analysis-v1.ps1 `
    -LookbackMinutes 120
```
