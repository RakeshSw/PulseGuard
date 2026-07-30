#!/usr/bin/env bash
set -euo pipefail

if [ "${CODESPACES:-false}" != "true" ]; then
  printf '\nPulseGuard local URLs:\n'
  printf '  Incident Console : http://localhost:8095/\n'
  printf '  Investigation    : http://localhost:8096/\n'
  printf '  Scenario Control : http://localhost:8090/\n'
  printf '  Predictive       : http://localhost:8098/\n'
  printf '  Locust           : http://localhost:8089/\n'
  printf '  Grafana          : http://localhost:3000/\n'
  exit 0
fi

DOMAIN="${GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN:-app.github.dev}"
BASE="${CODESPACE_NAME}"

url() {
  printf 'https://%s-%s.%s/' "$BASE" "$1" "$DOMAIN"
}

printf '\nPulseGuard Codespaces URLs (private by default):\n'
printf '  Incident Console : %s\n' "$(url 8095)"
printf '  Investigation    : %s\n' "$(url 8096)"
printf '  Scenario Control : %s\n' "$(url 8090)"
printf '  Live Activity    : %s\n' "$(url 8097)"
printf '  Predictive       : %s\n' "$(url 8098)"
printf '  Locust           : %s\n' "$(url 8089)"
printf '  Grafana          : %s\n' "$(url 3000)"
printf '  Prometheus       : %s\n' "$(url 9090)"
printf '\nOpen the Ports tab in Codespaces to launch or change visibility.\n'
