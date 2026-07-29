# Controlled turbulence scenarios

The Scenario Controller supports one active scenario at a time. Starting a new scenario first clears Toxiproxy toxics, traffic overrides, payload corruption and payment-node fault modes.

## Demand

- **2x for 60 seconds**, **4x for 90 seconds**, **6x for 120 seconds**: multiplies the live Wikimedia-derived Locust target, capped by `TRAFFIC_MAX_USERS`.
- Wikimedia event content is not converted into checkout data. Only the aggregate traffic intensity is amplified.

## Node and network

- **Take node 3 offline**: the payment process remains running but rejects payment requests with a controlled `503`. The router can retry the healthy peers.
- **Flap node 3**: alternates the same controlled unavailable/available mode every five seconds.
- **30% connection resets**: uses Toxiproxy `reset_peer` with toxicity `0.3`. This is packet-loss-like connection turbulence, not literal IP packet loss.
- **100% connection resets**, **latency**, and **timeout** remain available for node 3.

## Shared dependency

- **Fail shared dependency**: all three payment nodes return the same controlled payment-authorisation dependency failure. This is intentionally fleet-wide and should not lead to draining a single node.

## Safety

- No Docker socket is mounted.
- No arbitrary shell command is accepted.
- Faults are named and allowlisted.
- **Reset all faults** restores every node, traffic profile and proxy.

## Validation

```powershell
.\scripts\test-turbulence.ps1
```

The script validates demand amplification, node-down failover, shared-dependency failure and guaranteed cleanup.
