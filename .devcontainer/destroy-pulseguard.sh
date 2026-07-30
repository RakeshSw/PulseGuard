#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

docker compose down --volumes --remove-orphans --rmi local
rm -f .env
printf '\nPulseGuard containers, local images, volumes, database, metrics, and generated .env were removed.\n'
