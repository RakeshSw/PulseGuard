#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker compose down --remove-orphans
printf '\nPulseGuard containers stopped. Docker volumes are preserved.\n'
