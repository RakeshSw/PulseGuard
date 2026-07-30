[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [switch]$RequireRealAI
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $ProjectRoot

function Get-Health {
    param([string]$Name, [string]$Uri)
    try {
        $Value = Invoke-RestMethod -Uri $Uri -TimeoutSec 20
        $Status = if ($Value.status) { [string]$Value.status } else { 'healthy' }
        Write-Host ("[PASS] {0}: {1}" -f $Name, $Status) -ForegroundColor Green
        return $Value
    }
    catch {
        Write-Host ("[FAIL] {0}: {1}" -f $Name, $_.Exception.Message) -ForegroundColor Red
        throw
    }
}

Write-Host 'PulseGuard v0.6 validation' -ForegroundColor Cyan
Write-Host '================================'

try {
    docker info *> $null
}
catch {
    throw 'Docker Desktop is not running or Docker is unavailable.'
}

if ($LASTEXITCODE -ne 0) {
    throw 'Docker Desktop Linux engine is unavailable.'
}

docker compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'docker compose config failed.' }
Write-Host '[PASS] Docker Compose configuration' -ForegroundColor Green

$ComposeText = docker compose config | Out-String
if ($ComposeText -match '/var/run/docker\.sock') {
    throw 'Safety validation failed: Docker socket is mounted into a service.'
}
Write-Host '[PASS] No Docker socket is exposed to PulseGuard services' -ForegroundColor Green

$Scenario = Get-Health 'Scenario Controller' 'http://localhost:8090/health'
$Core = Get-Health 'PulseGuard Core' 'http://localhost:8095/health'
$Agent = Get-Health 'PulseGuard Agent' 'http://localhost:8096/health'
$Automation = Get-Health 'Automation/Activity/Support' 'http://localhost:8097/health'
$Predictor = Get-Health 'Predictive Analysis' 'http://localhost:8098/health'
$External = Get-Health 'External Partner Authentication Service' 'http://localhost:8099/health'
$Checkout = Get-Health 'Checkout Service' 'http://localhost:8080/health'

if (-not $Agent.realAiReady) {
    if ($RequireRealAI) {
        throw 'The agent is healthy, but realAiReady is false. Check the existing .env provider configuration.'
    }
    Write-Warning 'The agent is healthy, but realAiReady is false. Predictive explanations may use deterministic fallback.'
}
else {
    Write-Host ("[PASS] Real AI ready: {0} / {1}" -f $Agent.provider, $Agent.analysisMode) -ForegroundColor Green
}

$ExpectedPredictorMode = 'PROMETHEUS_RANGE_FORECAST_WITH_EVENT_DRIVEN_AI'
if ($Predictor.mode -ne $ExpectedPredictorMode) {
    throw "Unexpected predictor mode: $($Predictor.mode). Expected: $ExpectedPredictorMode"
}
if ($Predictor.dataSourceMode -ne 'PROMETHEUS_RANGE_QUERIES') {
    throw "Unexpected predictor data source mode: $($Predictor.dataSourceMode)"
}
if ($Predictor.continuousMetricsToAi -ne $false) {
    throw 'Predictor must not send continuous metrics to AI.'
}
if ($Predictor.aiContactMode -ne 'NEW_DETERMINISTIC_TRIGGER_ONLY') {
    throw "Unexpected predictor AI contact mode: $($Predictor.aiContactMode)"
}
Write-Host '[PASS] Predictor uses deterministic Prometheus range forecasts and event-driven AI explanations only' -ForegroundColor Green

$PredictorSummary = Invoke-RestMethod -Uri 'http://localhost:8098/summary' -TimeoutSec 20
if ($null -eq $PredictorSummary.activePredictions -or $null -eq $PredictorSummary.totalPredictions) {
    throw 'Predictor summary is incomplete.'
}
Write-Host '[PASS] Predictor summary and lifecycle APIs' -ForegroundColor Green

$AutomationSummary = Invoke-RestMethod -Uri 'http://localhost:8097/summary' -TimeoutSec 20
if ($null -eq $AutomationSummary.autoRepaired) { throw 'Auto-repaired KPI is missing.' }
Write-Host '[PASS] Governed automation summary' -ForegroundColor Green

$IncidentPage = (Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:8095/' -TimeoutSec 20).Content
if ($IncidentPage -notmatch '<th>Resolution</th>') { throw 'Incident summary Resolution column is missing.' }
Write-Host '[PASS] Incident summary includes Resolution' -ForegroundColor Green

$Catalog = Invoke-RestMethod -Uri 'http://localhost:8090/test-runs/catalog' -TimeoutSec 20
if (-not $Catalog.disturbances.external_auth_failure) {
    throw 'External authentication failure scenario is missing from the full-suite catalog.'
}
if (-not $Catalog.disturbances.capacity_failover_scale) {
    throw 'Capacity failover scenario is missing from the full-suite catalog.'
}
Write-Host '[PASS] Full suite includes external authentication repair and capacity scaling' -ForegroundColor Green

$ScenarioPage = (Invoke-WebRequest -UseBasicParsing -Uri 'http://localhost:8090/' -TimeoutSec 20).Content
foreach ($Marker in @(
    'External authentication failure',
    'Hung payment worker and restart',
    'Predictive disk growth',
    'Predictive node degradation',
    'Predictive capacity risk'
)) {
    if ($ScenarioPage -notmatch [regex]::Escape($Marker)) {
        throw "Scenario Controller UI marker is missing: $Marker"
    }
}
Write-Host '[PASS] Predictive, authentication and restart scenarios are visible' -ForegroundColor Green

$Targets = Invoke-RestMethod -Uri 'http://localhost:9090/api/v1/targets' -TimeoutSec 20
$ActiveTargets = @($Targets.data.activeTargets)
foreach ($Job in @('opsai-core','opsai-agent','opsai-automation','opsai-predictor','external-auth-service','checkout-service')) {
    $Matches = @($ActiveTargets | Where-Object { $_.labels.job -eq $Job })
    if (-not $Matches) { throw "Prometheus target was not found: $Job" }
    if (@($Matches | Where-Object { $_.health -ne 'up' }).Count -gt 0) {
        throw "Prometheus target is not healthy: $Job"
    }
}
Write-Host '[PASS] Prometheus scrapes detection, agent, automation, predictor and external-service telemetry' -ForegroundColor Green

if ([string]::IsNullOrWhiteSpace([string]$External.tokenFingerprint)) {
    throw 'External service health does not expose a safe token fingerprint.'
}
if ($External.PSObject.Properties.Name -contains 'token') {
    throw 'External service health must not expose the bearer token.'
}
Write-Host '[PASS] External credential health is fingerprint-only' -ForegroundColor Green

Write-Host ''
Write-Host 'Open:' -ForegroundColor Cyan
Write-Host '  Scenario Controller : http://localhost:8090'
Write-Host '  Incident Console    : http://localhost:8095'
Write-Host '  Agent Console       : http://localhost:8096'
Write-Host '  Automation Console  : http://localhost:8097'
Write-Host '  Predictive Console  : http://localhost:8098'
Write-Host '  External Auth Demo  : http://localhost:8099'
Write-Host ''
Write-Host 'Next validations:' -ForegroundColor Cyan
Write-Host '  .\scripts\test-opsai-predictive.ps1 -Scenario all'
Write-Host '  .\scripts\test-opsai-auth-restart.ps1 -Scenario all'
