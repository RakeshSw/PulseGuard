[CmdletBinding()]
param([string]$Service)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if ([string]::IsNullOrWhiteSpace($Service)) {
    docker compose logs --follow --tail 100
} else {
    docker compose logs --follow --tail 100 $Service
}
