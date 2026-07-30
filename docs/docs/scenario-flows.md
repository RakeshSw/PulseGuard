# PulseGuard Scenario and Event Flows

PulseGuard demonstrates a governed agentic reliability workflow:

> **Telemetry detects the problem. AI investigates and recommends. Deterministic policy governs. Automation executes bounded actions. Telemetry verifies recovery.**

---

## Contents

1. [Master incident lifecycle](#1-master-incident-lifecycle)
2. [Complete platform block trace](#2-complete-platform-block-trace)
3. [End-to-end incident sequence](#3-end-to-end-incident-sequence)
4. [Payment-node latency](#4-payment-node-latency)
5. [Payment-node unavailability](#5-payment-node-unavailability)
6. [Shared dependency failure](#6-shared-dependency-failure)
7. [External authentication credential mismatch](#7-external-authentication-credential-mismatch)
8. [Disk capacity breach](#8-disk-capacity-breach)
9. [Certificate expiry](#9-certificate-expiry)
10. [Traffic-profile corruption](#10-traffic-profile-corruption)
11. [Application capacity degradation](#11-application-capacity-degradation)
12. [Predictive future breach](#12-predictive-future-breach)
13. [Governance decision flow](#13-governance-decision-flow)
14. [Recovery verification flow](#14-recovery-verification-flow)
15. [Component responsibilities](#15-component-responsibilities)

---

# 1. Master incident lifecycle

This diagram shows the common lifecycle followed by PulseGuard scenarios.

```mermaid
flowchart TD
    A["Scenario trigger<br/>Controlled fault, natural degradation, or predictive trend"] --> B["Application or dependency impact"]
    B --> C["Services publish operational metrics"]
    C --> D["Prometheus collects telemetry"]
    D --> E["PulseGuard Core<br/>Deterministic rule evaluation"]
    E --> F{"Incident threshold breached?"}

    F -- "No" --> G["Continue monitoring"]
    G --> D

    F -- "Yes" --> H["Create incident"]
    H --> I["Store incident in PostgreSQL"]
    I --> J["PulseGuard Agent discovers active incident"]
    J --> K["Collect bounded operational evidence"]
    K --> L["Send bounded context to Azure OpenAI"]
    L --> M["Receive structured recommendation"]
    M --> N["Agent policy and governance evaluation"]
    N --> O{"Governance decision"}

    O -- "AUTO_ALLOWED" --> P["PulseGuard Automation executes bounded action"]
    O -- "APPROVAL_REQUIRED" --> Q["Wait for human approval"]
    O -- "HUMAN_ONLY" --> R["Create support handoff"]
    O -- "DENIED" --> R

    Q --> S{"Approved?"}
    S -- "Yes" --> P
    S -- "No" --> R

    P --> T["Record execution result"]
    T --> U["Monitor recovery telemetry"]
    U --> V{"Recovery thresholds satisfied?"}

    V -- "No" --> W["Continue verification or escalate"]
    W --> U

    V -- "Yes" --> X["Count consecutive healthy evaluations"]
    X --> Y{"Enough healthy evaluations?"}

    Y -- "No" --> U
    Y -- "Yes" --> Z["Resolve incident"]

    D --> AA["Grafana dashboards"]
```

---

# 2. Complete platform block trace

This diagram shows how all major PulseGuard containers participate in the platform.

```mermaid
flowchart LR
    subgraph External["External activity source"]
        EXT1["Wikimedia Recent Changes<br/>Server-Sent Events"]
    end

    subgraph Traffic["Traffic generation layer"]
        TRA1["Wikimedia Adapter"]
        TRA2["Corruption Adapter"]
        TRA3["Toxiproxy<br/>Traffic-profile proxy"]
        TRA4["Locust Load Generator"]
    end

    subgraph Business["Synthetic business application"]
        BUS1["Checkout Service"]
        BUS2["External Auth Service"]
        BUS3["Payment Router"]
        BUS4["Payment Node 1"]
        BUS5["Payment Node 2"]
        BUS6["Toxiproxy<br/>Payment-node-3 proxy"]
        BUS7["Payment Node 3"]
    end

    subgraph Control["Controlled scenario layer"]
        CTL1["Scenario Controller"]
    end

    subgraph Observability["Observability and storage"]
        OBS1["Prometheus"]
        OBS2["Grafana"]
        OBS3["PostgreSQL"]
    end

    subgraph Intelligence["PulseGuard intelligence layer"]
        PG1["PulseGuard Core"]
        PG2["PulseGuard Agent"]
        PG3["Azure OpenAI"]
        PG4["PulseGuard Automation"]
        PG5["PulseGuard Predictor"]
    end

    EXT1 --> TRA1
    TRA1 --> TRA2
    TRA2 --> TRA3
    TRA3 --> TRA4

    TRA4 --> BUS1
    BUS1 --> BUS2
    BUS1 --> BUS3

    BUS3 --> BUS4
    BUS3 --> BUS5
    BUS3 --> BUS6
    BUS6 --> BUS7

    CTL1 --> TRA2
    CTL1 --> TRA3
    CTL1 --> BUS2
    CTL1 --> BUS4
    CTL1 --> BUS5
    CTL1 --> BUS6
    CTL1 --> BUS7

    TRA1 --> OBS1
    TRA2 --> OBS1
    TRA4 --> OBS1

    BUS1 --> OBS1
    BUS2 --> OBS1
    BUS3 --> OBS1
    BUS4 --> OBS1
    BUS5 --> OBS1
    BUS7 --> OBS1

    PG1 --> OBS1
    PG2 --> OBS1
    PG4 --> OBS1
    PG5 --> OBS1

    OBS1 --> OBS2
    OBS1 --> PG1
    OBS1 --> PG2
    OBS1 --> PG5

    PG1 --> OBS3
    PG2 --> OBS3

    PG1 --> PG2
    PG2 --> PG3
    PG3 --> PG2

    PG2 --> PG4
    PG4 --> BUS2
    PG4 --> BUS3
    PG4 --> BUS4
    PG4 --> BUS5
    PG4 --> BUS7

    PG4 --> OBS1
    OBS1 --> PG1
```

---

# 3. End-to-end incident sequence

This sequence diagram traces a complete incident from customer traffic to resolution.

```mermaid
sequenceDiagram
    autonumber

    participant SC as Scenario Controller
    participant TP as Toxiproxy
    participant L as Locust
    participant CO as Checkout Service
    participant PR as Payment Router
    participant PN as Payment Node
    participant PM as Prometheus
    participant CORE as PulseGuard Core
    participant DB as PostgreSQL
    participant AG as PulseGuard Agent
    participant AI as Azure OpenAI
    participant AU as PulseGuard Automation
    participant OP as Human Operator

    SC->>TP: Inject controlled fault
    L->>CO: Send checkout request
    CO->>PR: Request payment authorization
    PR->>TP: Route request to affected path
    TP->>PN: Apply latency, timeout, or connection fault
    PN-->>PR: Slow response or failure
    PR-->>CO: Retry, degraded response, or failure
    CO-->>L: Checkout result

    PM->>CO: Scrape checkout metrics
    PM->>PR: Scrape routing and retry metrics
    PM->>PN: Scrape node metrics

    CORE->>PM: Query deterministic incident signals
    PM-->>CORE: Return latency, failures, and availability
    CORE->>CORE: Evaluate thresholds and consecutive breaches
    CORE->>DB: Create incident

    AG->>CORE: Poll active incidents
    CORE-->>AG: Return new active incident
    AG->>PM: Collect bounded telemetry
    AG->>PR: Collect routing state
    AG->>PN: Collect node diagnostics
    AG->>AI: Send bounded evidence and knowledge
    AI-->>AG: Return structured recommendation

    AG->>AG: Validate action and parameters
    AG->>AG: Apply deterministic governance policy

    alt Action is automatically allowed
        AG->>AU: Submit governed action
    else Human approval is required
        AG-->>OP: Present recommendation and evidence
        OP->>AU: Approve governed action
    else Action is denied or human-only
        AG->>DB: Record support handoff
    end

    AU->>PR: Execute allowlisted remediation
    PR-->>AU: Return execution result

    PM->>PR: Continue metric collection
    CORE->>PM: Evaluate recovery signals
    PM-->>CORE: Return healthy telemetry
    CORE->>CORE: Count consecutive healthy evaluations
    CORE->>DB: Resolve incident
```

---

# 4. Payment-node latency

A controlled network delay affects one payment node.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Select payment-node latency scenario"] --> B["Toxiproxy adds latency to payment-node-3 path"]
    B --> C["Payment Router selects payment-node-3"]
    C --> D["Request passes through Toxiproxy"]
    D --> E["Payment response is delayed"]
    E --> F["Router observes high duration or timeout"]
    F --> G["Router may retry through another healthy node"]
    G --> H["Checkout latency and retry count increase"]

    H --> I["Prometheus collects:<br/>node latency<br/>router retries<br/>checkout duration<br/>failure classifications"]
    I --> J["PulseGuard Core evaluates latency rule"]
    J --> K{"Threshold exceeded for required evaluations?"}

    K -- "No" --> L["Continue monitoring"]
    L --> I

    K -- "Yes" --> M["Create PAYMENT_NODE_LATENCY incident"]
    M --> N["PulseGuard Agent collects evidence"]
    N --> O["Compare affected node with healthy peer nodes"]
    O --> P["Azure OpenAI recommends drain_payment_node"]
    P --> Q["Agent governance evaluation"]

    Q --> R{"Is node drain safe?"}
    R -- "No" --> S["Deny action and create support handoff"]
    R -- "Yes" --> T["Decision: APPROVAL_REQUIRED"]

    T --> U["Human reviews evidence and recommendation"]
    U --> V{"Approved?"}

    V -- "No" --> S
    V -- "Yes" --> W["Automation drains payment-node-3"]

    W --> X["Payment Router stops sending new traffic to node 3"]
    X --> Y["Nodes 1 and 2 handle payment traffic"]
    Y --> Z["Prometheus records falling latency and retries"]
    Z --> AA["Core verifies consecutive healthy evaluations"]
    AA --> AB["Incident resolved"]
```

---

# 5. Payment-node unavailability

A payment node becomes unavailable or its network path is disabled.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Disable payment-node path"] --> B["Toxiproxy disables or resets connection"]
    B --> C["Payment Router sends request to affected node"]
    C --> D["Connection fails or times out"]
    D --> E["Router classifies node failure"]
    E --> F["Router retries through a healthy node"]
    F --> G["Retry rate and node failure metrics increase"]

    G --> H["Prometheus collects availability and retry telemetry"]
    H --> I["PulseGuard Core evaluates unavailability rule"]
    I --> J{"Required failure threshold reached?"}

    J -- "No" --> K["Continue monitoring"]
    K --> H

    J -- "Yes" --> L["Create PAYMENT_NODE_UNAVAILABLE incident"]
    L --> M["PulseGuard Agent collects node and peer evidence"]
    M --> N["AI recommends drain, restore, or bounded restart"]
    N --> O["Agent governance evaluates recommendation"]

    O --> P{"Governance decision"}

    P -- "AUTO_ALLOWED" --> Q["Automation performs bounded recovery action"]
    P -- "APPROVAL_REQUIRED" --> R["Human approves action"]
    P -- "DENIED" --> S["Support handoff"]
    P -- "HUMAN_ONLY" --> S

    R --> Q

    Q --> T["Router health and node availability are rechecked"]
    T --> U["Prometheus observes restored request success"]
    U --> V["Core verifies recovery"]
    V --> W["Incident resolved"]
```

---

# 6. Shared dependency failure

A dependency used by multiple payment nodes fails, causing correlated failures.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Inject shared dependency failure"] --> B["Multiple payment nodes report dependency errors"]
    B --> C["Payment Router observes failures across several nodes"]
    C --> D["Checkout failures increase"]
    D --> E["Prometheus records correlated failures"]

    E --> F["PulseGuard Core detects multi-node service degradation"]
    F --> G["Create SHARED_DEPENDENCY_FAILURE incident"]
    G --> H["PulseGuard Agent collects evidence from all nodes"]
    H --> I["Compare node symptoms and common dependency signals"]
    I --> J["AI identifies likely shared dependency problem"]
    J --> K["AI recommends dependency recovery or support escalation"]
    K --> L["Agent governance evaluation"]

    L --> M{"Is a bounded dependency action available?"}

    M -- "No" --> N["Decision: HUMAN_ONLY"]
    N --> O["Create support handoff with evidence"]

    M -- "Yes" --> P{"Does action require approval?"}
    P -- "Yes" --> Q["Decision: APPROVAL_REQUIRED"]
    P -- "No" --> R["Decision: AUTO_ALLOWED"]

    Q --> S["Human approves action"]
    S --> T["Automation executes bounded dependency recovery"]
    R --> T

    T --> U["Dependency health improves"]
    U --> V["Payment nodes recover"]
    V --> W["Checkout success rate improves"]
    W --> X["Core verifies recovery"]
    X --> Y["Incident resolved"]
```

---

# 7. External authentication credential mismatch

The checkout credential no longer matches the external partner service.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Rotate or invalidate partner credential"] --> B["Checkout Service calls External Auth Service"]
    B --> C["External service rejects bearer token"]
    C --> D["HTTP 401 or authentication failure"]
    D --> E["Checkout requests fail before payment processing"]

    E --> F["Prometheus records:<br/>authentication failures<br/>dependency errors<br/>checkout failures"]
    F --> G["PulseGuard Core detects authentication incident"]
    G --> H["Create EXTERNAL_SERVICE_AUTHENTICATION_FAILURE incident"]

    H --> I["PulseGuard Agent collects bounded evidence"]
    I --> J["Check external service health and checkout credential state"]
    J --> K["Azure OpenAI recommends refresh_external_service_credentials"]
    K --> L["Agent governance evaluation"]

    L --> M{"Expected service and client?"}
    M -- "No" --> N["Decision: DENIED"]
    N --> O["Create support handoff"]

    M -- "Yes" --> P{"Credential operation allowlisted?"}
    P -- "No" --> N
    P -- "Yes" --> Q["Decision: AUTO_ALLOWED"]

    Q --> R["Automation generates or retrieves bounded synthetic credential"]
    R --> S["Update Checkout Service credential"]
    S --> T["Run validation request against External Auth Service"]

    T --> U{"Authentication successful?"}
    U -- "No" --> V["Mark remediation unsuccessful and escalate"]
    U -- "Yes" --> W["Prometheus records successful dependency calls"]

    W --> X["Core verifies checkout recovery"]
    X --> Y["Incident resolved"]
```

---

# 8. Disk capacity breach

A service reports high disk use, capacity pressure, or an approaching disk limit.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Simulate disk pressure"] --> B["Target service reports increasing disk usage"]
    B --> C["Prometheus collects disk utilization and growth metrics"]
    C --> D["PulseGuard Core evaluates disk threshold"]

    D --> E{"Disk threshold breached?"}
    E -- "No" --> F["Continue monitoring"]
    F --> C

    E -- "Yes" --> G["Create DISK_CAPACITY_BREACH incident"]
    G --> H["PulseGuard Agent collects disk evidence"]
    H --> I["Evidence includes:<br/>current utilization<br/>growth trend<br/>target location<br/>service impact"]
    I --> J["Azure OpenAI recommends cleanup_allowlisted_disk_location"]
    J --> K["Agent governance evaluation"]

    K --> L{"Target path allowlisted?"}
    L -- "No" --> M["Decision: DENIED"]
    M --> N["Create support handoff"]

    L -- "Yes" --> O{"Cleanup within safe limit?"}
    O -- "No" --> P["Decision: APPROVAL_REQUIRED"]
    O -- "Yes" --> Q["Decision: AUTO_ALLOWED"]

    P --> R["Human reviews cleanup scope"]
    R --> S{"Approved?"}
    S -- "No" --> N
    S -- "Yes" --> T["Automation cleans allowlisted location"]

    Q --> T

    T --> U["Record deleted files, reclaimed space, and action result"]
    U --> V["Prometheus collects updated disk utilization"]
    V --> W{"Disk usage below recovery threshold?"}

    W -- "No" --> X["Escalate for manual capacity expansion"]
    W -- "Yes" --> Y["Core counts consecutive healthy evaluations"]
    Y --> Z["Incident resolved"]
```

---

# 9. Certificate expiry

A bounded demo certificate is expired or approaching its expiry threshold.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Simulate certificate expiry"] --> B["Target service reports certificate warning or failure"]
    B --> C["Prometheus or health endpoint reports certificate state"]
    C --> D["PulseGuard Core evaluates certificate rule"]

    D --> E{"Expiry or failure threshold reached?"}
    E -- "No" --> F["Continue monitoring"]
    F --> C

    E -- "Yes" --> G["Create CERTIFICATE_RISK incident"]
    G --> H["PulseGuard Agent collects certificate evidence"]
    H --> I["Evidence includes:<br/>expiry date<br/>remaining validity<br/>affected service<br/>service health"]
    I --> J["Azure OpenAI recommends renew_demo_certificate"]
    J --> K["Agent governance evaluation"]

    K --> L{"Certificate is bounded demo certificate?"}
    L -- "No" --> M["Decision: HUMAN_ONLY"]
    M --> N["Create support handoff"]

    L -- "Yes" --> O{"Renewal action allowlisted?"}
    O -- "No" --> P["Decision: DENIED"]
    P --> N

    O -- "Yes" --> Q["Decision: AUTO_ALLOWED"]
    Q --> R["Automation generates renewed certificate"]
    R --> S["Apply certificate to bounded target"]
    S --> T["Reload or refresh service state"]
    T --> U["Validate certificate and service health"]

    U --> V{"Validation successful?"}
    V -- "No" --> W["Rollback or create support handoff"]
    V -- "Yes" --> X["Prometheus records healthy certificate state"]

    X --> Y["Core verifies recovery"]
    Y --> Z["Incident resolved"]
```

---

# 10. Traffic-profile corruption

The traffic profile is modified, amplified, malformed, or made unsafe.

```mermaid
flowchart TD
    A["Wikimedia Adapter calculates valid traffic profile"] --> B["Corruption Adapter receives profile"]
    B --> C["Scenario Controller enables profile corruption"]
    C --> D["Corruption Adapter changes target users or profile values"]
    D --> E["Toxiproxy forwards corrupted profile"]
    E --> F["Locust reads traffic profile"]

    F --> G{"Profile validation passes?"}

    G -- "No" --> H["Locust applies safe fallback profile"]
    H --> I["Record rejected or stale profile metric"]

    G -- "Yes" --> J["Locust increases synthetic traffic"]
    J --> K["Checkout demand rises"]
    K --> L["Latency, failures, or saturation may increase"]

    I --> M["Prometheus collects traffic-profile anomaly metrics"]
    L --> M

    M --> N["PulseGuard Core evaluates corruption or load anomaly rule"]
    N --> O["Create TRAFFIC_PROFILE_CORRUPTION incident"]
    O --> P["PulseGuard Agent collects profile and application evidence"]
    P --> Q["Compare Wikimedia source, corruption output, and Locust load"]
    Q --> R["AI recommends reset corruption or apply safe fallback"]
    R --> S["Agent governance evaluation"]

    S --> T{"Bounded reset action allowlisted?"}
    T -- "No" --> U["Decision: HUMAN_ONLY"]
    U --> V["Create support handoff"]

    T -- "Yes" --> W["Decision: AUTO_ALLOWED"]
    W --> X["Automation resets corruption state"]
    X --> Y["Valid or fallback traffic profile restored"]
    Y --> Z["Locust returns to safe user count"]
    Z --> AA["Prometheus verifies application recovery"]
    AA --> AB["Incident resolved"]
```

---

# 11. Application capacity degradation

Available payment-processing capacity falls below a safe level.

```mermaid
flowchart TD
    A["Scenario Controller<br/>Reduce node capacity or disable node"] --> B["Available payment capacity decreases"]
    B --> C["Payment Router has fewer healthy processing paths"]
    C --> D["Queueing, retries, latency, or failures increase"]
    D --> E["Prometheus records degraded capacity"]

    E --> F["PulseGuard Core evaluates capacity rule"]
    F --> G{"Capacity below safe threshold?"}

    G -- "No" --> H["Continue monitoring"]
    H --> E

    G -- "Yes" --> I["Create CAPACITY_DEGRADATION incident"]
    I --> J["PulseGuard Agent collects fleet evidence"]
    J --> K["Evidence includes:<br/>active nodes<br/>drained nodes<br/>node health<br/>request rate<br/>retry rate"]
    K --> L["Azure OpenAI recommends scale capacity, restore node, or isolate unhealthy node"]
    L --> M["Agent governance evaluation"]

    M --> N{"Requested action"}

    N -- "Bounded scale" --> O{"Safe scale limit?"}
    N -- "Restore known node" --> P{"Node state supports restore?"}
    N -- "Drain unhealthy node" --> Q{"Enough healthy peer capacity remains?"}

    O -- "Yes" --> R["AUTO_ALLOWED or APPROVAL_REQUIRED"]
    O -- "No" --> S["DENIED"]

    P -- "Yes" --> R
    P -- "No" --> S

    Q -- "Yes" --> T["APPROVAL_REQUIRED"]
    Q -- "No" --> S

    R --> U["Automation performs bounded capacity action"]
    T --> V["Human approves routing change"]
    V --> U

    S --> W["Create support handoff"]

    U --> X["Healthy processing capacity increases"]
    X --> Y["Latency and failures decrease"]
    Y --> Z["Prometheus verifies capacity recovery"]
    Z --> AA["Core resolves incident"]
```

---

# 12. Predictive future breach

The predictor identifies a likely future threshold breach before a reactive incident is created.

```mermaid
flowchart TD
    A["Prometheus historical metrics"] --> B["PulseGuard Predictor executes query_range"]
    B --> C["Validate sample count and data quality"]
    C --> D{"Enough valid samples?"}

    D -- "No" --> E["Prediction unavailable<br/>continue monitoring"]
    E --> A

    D -- "Yes" --> F["Calculate deterministic trend or slope"]
    F --> G["Project time to configured threshold"]
    G --> H{"Threshold likely within forecast horizon?"}

    H -- "No" --> I["Record healthy or stable prediction state"]
    I --> A

    H -- "Yes" --> J["Create deterministic prediction event"]
    J --> K["Collect bounded supporting evidence"]
    K --> L["Send calculated forecast to Azure OpenAI"]
    L --> M["AI explains risk and suggests preventive action"]
    M --> N["Predictive Analysis Console displays result"]

    N --> O{"Preventive action requested?"}
    O -- "No" --> P["Continue monitoring"]
    O -- "Yes" --> Q["Send recommendation through governance policy"]

    Q --> R{"Governance decision"}
    R -- "AUTO_ALLOWED" --> S["Automation executes bounded preventive action"]
    R -- "APPROVAL_REQUIRED" --> T["Human approves preventive action"]
    R -- "DENIED" --> U["No automation<br/>record recommendation"]
    R -- "HUMAN_ONLY" --> U

    T --> S
    S --> V["Prometheus verifies whether risk trend improves"]
    V --> W["Prediction state updated"]
```

---

# 13. Governance decision flow

The LLM recommendation is treated as untrusted input. PulseGuard Agent applies deterministic policy before any action reaches Automation.

```mermaid
flowchart TD
    A["Structured LLM recommendation"] --> B["Validate response schema"]
    B --> C{"Schema valid?"}

    C -- "No" --> D["Reject recommendation"]
    D --> E["Decision: DENIED"]

    C -- "Yes" --> F["Extract action name and parameters"]
    F --> G{"Action name allowlisted?"}

    G -- "No" --> E
    G -- "Yes" --> H{"Action valid for incident type?"}

    H -- "No" --> E
    H -- "Yes" --> I{"Parameters valid and bounded?"}

    I -- "No" --> E
    I -- "Yes" --> J{"Evidence supports target and action?"}

    J -- "No" --> E
    J -- "Yes" --> K{"Would action violate safety constraints?"}

    K -- "Yes" --> E
    K -- "No" --> L{"Execution category"}

    L -- "Low-risk predefined operation" --> M["Decision: AUTO_ALLOWED"]
    L -- "Valid but operationally sensitive" --> N["Decision: APPROVAL_REQUIRED"]
    L -- "Valid but no automated implementation" --> O["Decision: HUMAN_ONLY"]

    M --> P["Submit governed action to Automation"]
    N --> Q["Present evidence and recommendation to operator"]
    O --> R["Create support handoff"]
    E --> R

    Q --> S{"Operator approves?"}
    S -- "No" --> R
    S -- "Yes" --> P

    P --> T["Automation validates token and action contract"]
    T --> U{"Automation endpoint implemented?"}

    U -- "No" --> R
    U -- "Yes" --> V["Execute predefined bounded operation"]
```

---

# 14. Recovery verification flow

PulseGuard does not resolve an incident only because an automation request returned successfully.

```mermaid
flowchart TD
    A["Governed remediation executed"] --> B["Automation records action response"]
    B --> C{"Execution call successful?"}

    C -- "No" --> D["Mark remediation failed"]
    D --> E["Retry within policy or create support handoff"]

    C -- "Yes" --> F["Begin telemetry-based recovery verification"]
    F --> G["Prometheus continues metric collection"]
    G --> H["PulseGuard Core evaluates recovery threshold"]
    H --> I{"Recovery threshold satisfied?"}

    I -- "No" --> J["Increment unhealthy or pending verification state"]
    J --> K{"Verification timeout reached?"}

    K -- "No" --> G
    K -- "Yes" --> L["Recovery not verified"]
    L --> M["Create escalation or support handoff"]

    I -- "Yes" --> N["Increment consecutive healthy evaluation count"]
    N --> O{"Required healthy count reached?"}

    O -- "No" --> G
    O -- "Yes" --> P["Mark recovery verified"]
    P --> Q["Resolve incident"]
    Q --> R["Store complete audit trail in PostgreSQL"]
```

---

# 15. Component responsibilities

```mermaid
flowchart LR
    A["Scenario Controller"] --> A1["Creates controlled demo faults"]
    B["Prometheus"] --> B1["Collects operational telemetry"]
    C["PulseGuard Core"] --> C1["Deterministically detects and resolves incidents"]
    D["PulseGuard Agent"] --> D1["Collects evidence and calls the LLM"]
    D --> D2["Evaluates governance policy"]
    E["Azure OpenAI"] --> E1["Explains evidence and recommends an action"]
    F["PulseGuard Automation"] --> F1["Executes predefined bounded actions"]
    F --> F2["Records execution and verification activity"]
    G["PulseGuard Predictor"] --> G1["Calculates deterministic future-risk forecasts"]
    H["PostgreSQL"] --> H1["Stores incidents, investigations, and audit records"]
    I["Grafana"] --> I1["Visualizes operational telemetry"]
```

| Component | Primary responsibility |
|---|---|
| Scenario Controller | Introduces controlled faults for repeatable demonstrations |
| Toxiproxy | Injects network latency, timeouts, and connection failures |
| Locust | Generates synthetic checkout traffic |
| Checkout Service | Represents the customer-facing business transaction |
| External Auth Service | Represents a protected external dependency |
| Payment Router | Routes requests and handles payment-node retries |
| Payment Nodes | Represent replicated payment-processing services |
| Prometheus | Collects and stores operational metrics |
| Grafana | Visualizes metrics and service behaviour |
| PulseGuard Core | Opens and resolves incidents using deterministic thresholds |
| PulseGuard Agent | Collects evidence, calls the LLM, and evaluates governance policy |
| Azure OpenAI | Produces bounded explanations and structured recommendations |
| PulseGuard Automation | Executes only predefined and governed actions |
| PulseGuard Predictor | Calculates deterministic future-risk forecasts |
| PostgreSQL | Stores incidents, investigations, recommendations, and audit history |

---

# Design principles

## Deterministic detection

The LLM does not decide whether an incident exists.

```text
Prometheus telemetry
        ↓
PulseGuard Core rules
        ↓
Incident created
```

## Bounded AI investigation

The LLM receives a limited evidence package rather than unrestricted access to the environment.

```text
Incident
   ↓
Bounded telemetry and diagnostics
   ↓
Azure OpenAI
   ↓
Structured recommendation
```

## Deterministic governance

The LLM recommendation is not automatically trusted.

```text
LLM recommendation
        ↓
Allowlist and parameter validation
        ↓
Incident-action compatibility checks
        ↓
Safety and capacity checks
        ↓
AUTO_ALLOWED / APPROVAL_REQUIRED / HUMAN_ONLY / DENIED
```

## Controlled execution

Automation exposes predefined operations rather than arbitrary command execution.

```text
Governed action
      ↓
Allowlisted automation endpoint
      ↓
Bounded service API
```

## Telemetry-based recovery

An incident is not resolved merely because an action returned HTTP 200.

```text
Remediation executed
        ↓
Prometheus recovery metrics
        ↓
Consecutive healthy evaluations
        ↓
Incident resolved
```

---

# Summary

The complete PulseGuard lifecycle is:

```text
Generate traffic
    ↓
Inject or observe degradation
    ↓
Collect telemetry
    ↓
Detect incident deterministically
    ↓
Collect bounded evidence
    ↓
Use AI to explain and recommend
    ↓
Evaluate recommendation through deterministic governance
    ↓
Execute only allowed actions
    ↓
Verify recovery using telemetry
    ↓
Resolve or escalate
```

> **The LLM recommends. The Agent governs. Automation executes. Core verifies.**
