[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = 'Stop'
Set-Location $ProjectRoot

function Get-Health([string]$Name, [string]$Uri) {
    try {
        $value = Invoke-RestMethod -Uri $Uri -TimeoutSec 15
        $Status = if ($value.status) { $value.status } else { 'healthy' }
        Write-Host ("[PASS] {0}: {1}" -f $Name, $Status) -ForegroundColor Green
        return $value
    }
    catch {
        Write-Host ("[FAIL] {0}: {1}" -f $Name, $_.Exception.Message) -ForegroundColor Red
        throw
    }
}

Write-Host "PulseGuard v0.5.1 validation" -ForegroundColor Cyan
Write-Host "==================================="

docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'docker compose config failed' }
Write-Host '[PASS] Docker Compose configuration' -ForegroundColor Green

$Scenario = Get-Health 'Scenario Controller' 'http://localhost:8090/health'
$Core = Get-Health 'PulseGuard Core' 'http://localhost:8095/health'
$Agent = Get-Health 'PulseGuard Agent' 'http://localhost:8096/health'
$Automation = Get-Health 'Automation/Activity/Support' 'http://localhost:8097/health'

if (-not $Agent.realAiReady) {
    Write-Warning 'The agent is healthy, but realAiReady is false. Check the existing .env Azure/OpenAI configuration.'
} else {
    Write-Host ("[PASS] Real AI ready: {0} / {1}" -f $Agent.provider, $Agent.analysisMode) -ForegroundColor Green
}

$Summary = Invoke-RestMethod -Uri 'http://localhost:8097/summary' -TimeoutSec 15
$State = Invoke-RestMethod -Uri 'http://localhost:8097/state' -TimeoutSec 15
if ($null -eq $Summary.autoRepaired) { throw 'Auto-repaired KPI is missing.' }
if ($null -eq $State.disk -or $null -eq $State.certificate) { throw 'Automation state is incomplete.' }
Write-Host '[PASS] Live activity, auto-repair KPI, storage and certificate state' -ForegroundColor Green

$IncidentPage = (Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:8095/' -TimeoutSec 15).Content
if ($IncidentPage -notmatch '<th>Resolution</th>') { throw 'Incident summary Resolution column is missing.' }
Write-Host '[PASS] Incident summary includes the Resolution column' -ForegroundColor Green

$Catalog = Invoke-RestMethod -Uri 'http://localhost:8090/test-runs/catalog' -TimeoutSec 15
$CapacityScenario = $Catalog.disturbances.capacity_failover_scale
if (-not $CapacityScenario) { throw 'Capacity failover scale-up scenario is missing from the test catalog.' }
Write-Host '[PASS] Full suite includes bounded failover capacity scale-up' -ForegroundColor Green

$PromTargets = Invoke-RestMethod -Uri 'http://localhost:9090/api/v1/targets' -TimeoutSec 15
$AutomationTarget = @($PromTargets.data.activeTargets) | Where-Object { $_.labels.job -eq 'opsai-automation' }
if (-not $AutomationTarget) { throw 'Prometheus target opsai-automation was not found.' }
Write-Host '[PASS] Prometheus scrapes opsai-automation' -ForegroundColor Green

Write-Host ''
Write-Host 'Open:' -ForegroundColor Cyan
Write-Host '  Scenario Controller : http://localhost:8090'
Write-Host '  Incident Console    : http://localhost:8095'
Write-Host '  Agent Console       : http://localhost:8096'
Write-Host '  Automation Console  : http://localhost:8097'
Write-Host ''
Write-Host 'Focused real auto-repair test:' -ForegroundColor Cyan
Write-Host '  .\scripts\test-opsai-auto-repair.ps1 -Scenario all'
