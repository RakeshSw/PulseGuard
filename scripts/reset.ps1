[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
Write-Host "Removing containers, networks, telemetry volumes, and old container logs..." -ForegroundColor Yellow
docker compose down -v --remove-orphans
if ($LASTEXITCODE -ne 0) { throw "docker compose down failed." }
& "$PSScriptRoot\start.ps1"
