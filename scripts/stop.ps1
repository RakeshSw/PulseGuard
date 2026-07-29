[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
docker compose down
if ($LASTEXITCODE -ne 0) { throw "docker compose down failed." }
Write-Host "PulseGuard stopped. Prometheus and Grafana volumes were preserved." -ForegroundColor Green
