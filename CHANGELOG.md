# Changelog

All notable public changes to PulseGuard should be recorded here.

## [Unreleased]

### Added

- GitHub-ready documentation and static project site
- Public release verification script
- Project-scoped destructive teardown script
- Security, contribution, roadmap, and demo documentation

### Changed

- Public product branding standardized as PulseGuard
- Browser-facing activity labels standardized
- Documentation clarifies the boundary between public branding and internal `opsai-*` identifiers

## [1.0.1-poc] - 2026-07-29

### Security

- Removed shared fallback automation and partner tokens from all services.
- Removed the fallback PostgreSQL connection string from application code.
- Required secrets now fail closed when absent or left as placeholders.
- Startup generates unique local secrets when required `.env` values are blank.
- Public test scripts read the generated token from `.env` instead of embedding it.

### Fixed

- Removed an accidental leading character from `scripts/test-day3.ps1`.
- Added a clean-room Docker end-to-end release validator.

## [1.0.0-poc] - 2026-07-29

### Added

- Live Wikimedia-driven synthetic traffic profile
- Multi-node checkout and payment path
- Incident console and investigation UI
- Evidence-bounded Azure OpenAI investigation path
- Policy-gated automation and approval flow
- Recovery verification and support handoff
- Prometheus and Grafana observability
- Controlled latency, timeout, corruption, and dependency-failure components
