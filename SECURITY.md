# Security Policy

## Project scope

PulseGuard is a local proof of concept for demonstrating governed, agent-assisted incident response. It must not be treated as a production security, monitoring, or remediation control.

## Supported reporting

Please report suspected vulnerabilities privately to the repository owner before opening a public issue. Include:

- affected component
- reproduction steps
- expected and actual behavior
- potential impact
- suggested mitigation, when known

Do not include live credentials, access tokens, personal data, or proprietary logs in an issue.

## Secret handling

Never commit:

- `.env` or environment-specific variants
- Azure OpenAI keys
- bearer tokens
- database exports
- private certificates
- diagnostic ZIP files
- production logs or customer data

The supplied `.gitignore` excludes common secret and runtime files, but it is not a substitute for review.

Before publishing, run:

```powershell
.\scripts\verify-public-release.ps1 -ProjectRoot $PWD
```

## AI boundary

The investigation agent should receive only evidence required for the current incident:

- telemetry summaries
- incident facts
- service topology
- bounded automation context
- transparent local knowledge entries

Raw Wikimedia event content is not required for investigation and must not be sent to the checkout application or the model. Only aggregate traffic indicators may be included.

The agent must not receive:

- scenario-controller ground truth
- credentials or environment dumps
- unrestricted host access
- unrelated logs
- personal or customer data

## Automation boundary

Automated actions must be explicitly allowlisted. Policy should classify each proposed action as:

- automatic
- approval required
- human only
- denied

Command completion is not proof of recovery. PulseGuard must verify recovery using telemetry before resolving an incident.

## Dependency and image hygiene

- Pin container and package versions where practical.
- Review third-party licenses before redistribution.
- Rebuild images after dependency updates.
- Do not publish local images containing embedded secrets.
- Treat public traffic adapters as untrusted input boundaries.

## Disclaimer

Running fault-injection or load-generation tools can consume significant CPU and memory. Use an isolated local environment and do not target systems you do not own or have permission to test.

## Fail-closed local configuration

Required database and internal automation credentials have no application-code fallback values. Services stop at startup when these variables are absent or still contain a `CHANGE_ME_` placeholder. `scripts/start.ps1` creates unique local values in the ignored `.env` file.
