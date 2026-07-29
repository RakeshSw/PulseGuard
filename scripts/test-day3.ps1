[CmdletBinding()]
param()
$ErrorActionPreference="Stop"

function Wait-Incident {
    param([string]$Type,[string]$Node,[int]$TimeoutSeconds=90)
    $Deadline=(Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $Response=Invoke-RestMethod -Method Get -Uri "http://localhost:8095/incidents?status=active&limit=100"
        $Match=$Response.incidents | Where-Object { $_.incident_type -eq $Type -and ($Node -eq "" -or $_.node -eq $Node) } | Select-Object -First 1
        if ($Match) { return $Match }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $Deadline)
    throw "Timed out waiting for active incident $Type on $Node"
}

function Wait-Resolved {
    param([string]$IncidentId,[int]$TimeoutSeconds=120)
    $Deadline=(Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $Response=Invoke-RestMethod -Method Get -Uri "http://localhost:8095/incidents/$IncidentId"
        if ($Response.incident.status -eq "RESOLVED") { return $Response.incident }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $Deadline)
    throw "Timed out waiting for incident $IncidentId to resolve"
}

Write-Host "Resetting faults and waiting for baseline..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null
Start-Sleep -Seconds 35

Write-Host "Injecting 1.2-second latency..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/payment-latency?latency_ms=1200&jitter_ms=100" | Out-Null
$Latency=Wait-Incident -Type "PAYMENT_NODE_LATENCY" -Node "payment-node-3"
Write-Host "Latency incident opened: $($Latency.id)" -ForegroundColor Yellow

Write-Host "Resetting latency and verifying automatic recovery..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null
$ResolvedLatency=Wait-Resolved -IncidentId $Latency.id
Write-Host "Latency incident resolved automatically." -ForegroundColor Green

Start-Sleep -Seconds 20
Write-Host "Injecting timeout/failover..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/payment-timeout" | Out-Null
$Timeout=Wait-Incident -Type "PAYMENT_NODE_TIMEOUT" -Node "payment-node-3"
Write-Host "Timeout incident opened: $($Timeout.id)" -ForegroundColor Yellow

Write-Host "Resetting timeout and verifying automatic recovery..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null
$ResolvedTimeout=Wait-Resolved -IncidentId $Timeout.id
Write-Host "Timeout incident resolved automatically." -ForegroundColor Green

Write-Host ""
Write-Host "Day 4 validation passed: detection, persistence and recovery verification are working." -ForegroundColor Green
