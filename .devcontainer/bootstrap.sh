#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

chmod +x .devcontainer/*.sh

printf '\nPulseGuard Codespaces environment is ready.\n'
printf 'Start the full Docker demo with:\n\n'
printf '  bash .devcontainer/demo-start.sh\n\n'
printf 'The first build can take several minutes because all services are built from source.\n'
