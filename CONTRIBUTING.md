# Contributing to PulseGuard

Thank you for helping improve PulseGuard.

## Good contributions

- clearer incident evidence
- safer policy decisions
- stronger verification logic
- deterministic tests
- improved observability
- documentation and accessibility fixes
- new controlled demo scenarios
- removal of hidden coupling or ground-truth leakage

## Development principles

1. Keep the public product name **PulseGuard**.
2. Preserve internal `opsai-*` identifiers unless a coordinated migration is planned.
3. Do not hardcode user names, local paths, API keys, incident IDs, or scenario-specific answers.
4. Prefer generic evidence applicability, policy, validation, and recovery logic.
5. Keep AI context bounded and explainable.
6. Make destructive scripts project-scoped and require an explicit confirmation switch.
7. Add or update tests with every behavior change.

## Local workflow

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d --build
docker compose ps
```

Run the relevant validation scripts before opening a pull request.

## Pull requests

A pull request should include:

- problem statement
- implementation summary
- screenshots or logs for visible behavior
- tests performed
- security or data-handling impact
- rollback considerations

Do not attach secrets, full environment files, or private diagnostic archives.
