[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
docker compose ps
Write-Host ""
Write-Host "Scenario state:" -ForegroundColor Cyan
try { Invoke-RestMethod -Uri "http://localhost:8090/state" | ConvertTo-Json -Depth 10 }
catch { Write-Warning "Scenario Controller is unavailable." }
Write-Host ""
Write-Host "Live traffic profile:" -ForegroundColor Cyan
try { Invoke-RestMethod -Uri "http://localhost:8093/profile" | ConvertTo-Json -Depth 10 }
catch { Write-Warning "Wikimedia Adapter is unavailable." }

Write-Host ""
Write-Host "Active incidents:" -ForegroundColor Cyan
try {
    $Incidents = Invoke-RestMethod -Method Get -Uri "http://localhost:8095/incidents?status=active"
    if ($Incidents.count -eq 0) { Write-Host "None" -ForegroundColor Green }
    else { $Incidents.incidents | Select-Object status, severity, incident_type, node, opened_at | Format-Table -AutoSize }
}
catch { Write-Warning "PulseGuard Core is not reachable at http://localhost:8095." }

Write-Host "Agent provider:" -ForegroundColor Cyan
try { Invoke-RestMethod http://localhost:8096/health | ConvertTo-Json -Depth 5 }
catch { Write-Warning "PulseGuard Agent is not reachable." }
