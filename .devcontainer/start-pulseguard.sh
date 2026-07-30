#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker CLI is unavailable in this Codespace." >&2
  exit 1
fi

if ! command -v pwsh >/dev/null 2>&1; then
  echo "PowerShell is unavailable in this Codespace." >&2
  exit 1
fi

for attempt in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "The Docker-in-Docker daemon did not become ready." >&2
    exit 1
  fi
  sleep 2
done

pwsh -NoProfile -File ./scripts/start.ps1
bash .devcontainer/show-demo-urls.sh
