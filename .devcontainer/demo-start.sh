#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"
CLEAN_DEMO=false

cd "$ROOT"

usage() {
  cat <<'HELP'
Usage:
  bash .devcontainer/demo-start.sh
  bash .devcontainer/demo-start.sh --clean

Options:
  --clean   Remove containers and persisted demo volumes before startup.
            This clears previous incidents, metrics, and Grafana runtime data.
HELP
}

for arg in "$@"; do
  case "$arg" in
    --clean)
      CLEAN_DEMO=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      usage >&2
      exit 1
      ;;
  esac
done

for command_name in docker pwsh; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command is unavailable: $command_name" >&2
    exit 1
  fi
done

if [ ! -f "$ENV_FILE" ]; then
  cp "$ROOT/.env.example" "$ENV_FILE"
  echo "Created local .env from .env.example"
fi

get_env() {
  local key="$1"
  local line=""

  line="$(grep -E "^${key}=" "$ENV_FILE" 2>/dev/null | tail -n 1 || true)"
  printf '%s' "${line#*=}"
}

set_env() {
  local key="$1"
  local value="$2"
  local temporary_file
  local replaced=0
  local line

  temporary_file="$(mktemp)"

  while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" == "${key}="* ]]; then
      printf '%s=%s\n' "$key" "$value" >> "$temporary_file"
      replaced=1
    else
      printf '%s\n' "$line" >> "$temporary_file"
    fi
  done < "$ENV_FILE"

  if [ "$replaced" -eq 0 ]; then
    printf '%s=%s\n' "$key" "$value" >> "$temporary_file"
  fi

  mv "$temporary_file" "$ENV_FILE"
}

new_local_secret() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
    return
  fi

  pwsh -NoProfile -Command '
    $bytes = [byte[]]::new(24)
    [System.Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    [Convert]::ToHexString($bytes).ToLowerInvariant()
  '
}

echo
echo "PulseGuard real-AI demo configuration"
echo "====================================="

endpoint="${AZURE_OPENAI_ENDPOINT:-$(get_env AZURE_OPENAI_ENDPOINT)}"
deployment="${AZURE_OPENAI_DEPLOYMENT:-$(get_env AZURE_OPENAI_DEPLOYMENT)}"
configured_key="$(get_env AZURE_OPENAI_API_KEY)"
api_key="${AZURE_OPENAI_API_KEY:-}"

if [ -z "$endpoint" ]; then
  read -r -p "Azure OpenAI endpoint: " endpoint
fi

if [ -z "$deployment" ]; then
  read -r -p "Azure OpenAI deployment name: " deployment
fi

if [ -z "$api_key" ]; then
  if [ -n "$configured_key" ]; then
    read -r -s \
      -p "Paste a new Azure OpenAI API key, or press Enter to reuse the current key: " \
      entered_key
    echo

    if [ -n "$entered_key" ]; then
      api_key="$entered_key"
    else
      api_key="$configured_key"
    fi
  else
    read -r -s -p "Azure OpenAI API key: " api_key
    echo
  fi
else
  echo "Using AZURE_OPENAI_API_KEY supplied by the Codespace environment."
fi

if [ -z "$endpoint" ] || [ -z "$deployment" ] || [ -z "$api_key" ]; then
  echo "Azure OpenAI endpoint, deployment, and API key are required." >&2
  exit 1
fi

set_env "LLM_PROVIDER" "azure_openai"
set_env "LLM_TIMEOUT_SECONDS" "90"
set_env "AZURE_OPENAI_ENDPOINT" "$endpoint"
set_env "AZURE_OPENAI_API_KEY" "$api_key"
set_env "AZURE_OPENAI_DEPLOYMENT" "$deployment"

required_local_secrets=(
  GRAFANA_ADMIN_PASSWORD
  POSTGRES_PASSWORD
  AUTOMATION_API_TOKEN
  EXTERNAL_AUTH_INITIAL_TOKEN
)

for secret_name in "${required_local_secrets[@]}"; do
  secret_value="$(get_env "$secret_name")"

  if [ -z "$secret_value" ] || [[ "$secret_value" == CHANGE_ME_* ]]; then
    set_env "$secret_name" "$(new_local_secret)"
  fi
done

if [ "${CODESPACES:-false}" = "true" ] &&
   [ "${PULSEGUARD_KEEP_TRAFFIC:-false}" != "true" ]; then

  cpu_count="$(nproc 2>/dev/null || printf '2')"

  if [ "$cpu_count" -le 2 ]; then
    set_env "TRAFFIC_MIN_USERS" "2"
    set_env "TRAFFIC_BASE_USERS" "4"
    set_env "TRAFFIC_MAX_USERS" "8"
    set_env "TRAFFIC_FALLBACK_USERS" "4"
    set_env "TRAFFIC_SPAWN_RATE" "1"
    traffic_profile="low — 2-core Codespace"
  elif [ "$cpu_count" -le 4 ]; then
    set_env "TRAFFIC_MIN_USERS" "4"
    set_env "TRAFFIC_BASE_USERS" "8"
    set_env "TRAFFIC_MAX_USERS" "20"
    set_env "TRAFFIC_FALLBACK_USERS" "6"
    set_env "TRAFFIC_SPAWN_RATE" "3"
    traffic_profile="medium — ${cpu_count}-core Codespace"
  else
    traffic_profile="existing configuration — ${cpu_count} cores"
  fi

  echo "Traffic profile: $traffic_profile"
fi

chmod 600 "$ENV_FILE"

echo "Azure endpoint  : $endpoint"
echo "Azure deployment: $deployment"
echo "Azure API key   : configured but not displayed"

if [ "$CLEAN_DEMO" = "true" ]; then
  echo
  echo "Removing previous PulseGuard containers and demo volumes..."
  docker compose down -v --remove-orphans
fi

echo
echo "Starting PulseGuard..."
bash "$ROOT/.devcontainer/start-pulseguard.sh"

echo
echo "Checking real-AI readiness..."

pwsh -NoProfile -Command '
$health = Invoke-RestMethod "http://localhost:8096/health"

[pscustomobject]@{
    Status       = $health.status
    Provider     = $health.provider
    RealAiReady  = $health.realAiReady
    AnalysisMode = $health.analysisMode
} | Format-List

if ($health.provider -ne "azure_openai") {
    throw "PulseGuard agent is not using Azure OpenAI."
}

if ($health.realAiReady -ne $true) {
    throw "Azure OpenAI configuration is incomplete."
}

if ($health.analysisMode -ne "REAL_AI") {
    throw "PulseGuard agent is not running in REAL_AI mode."
}
'

echo
echo "[PASS] PulseGuard real-AI demo is ready."
