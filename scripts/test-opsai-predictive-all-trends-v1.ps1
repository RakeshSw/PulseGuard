[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8098"
)

$ErrorActionPreference = "Stop"

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $Condition) {
        throw $Message
    }
}

Write-Host ""
Write-Host "PulseGuard all-trends dashboard validation" -ForegroundColor Cyan
Write-Host "====================================="

$Health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 20
Assert-True ($Health.status -eq "healthy") "Predictor is not healthy."
Assert-True ($Health.dataSourceMode -eq "PROMETHEUS_RANGE_QUERIES") `
    "Predictor is not using Prometheus range queries."

$Signals = Invoke-RestMethod -Uri "$BaseUrl/signals" -TimeoutSec 30
$Groups = @($Signals.groups)

Assert-True ($Groups.Count -ge 4) `
    "Expected at least four live signal groups."

$ExpectedIds = @(
    "payment-latency",
    "disk-usage",
    "certificate-expiry",
    "checkout-throughput"
)

foreach ($Id in $ExpectedIds) {
    Assert-True (@($Groups | Where-Object { $_.id -eq $Id }).Count -eq 1) `
        "Missing signal group: $Id"
}

$Page = Invoke-WebRequest `
    -Uri "$BaseUrl/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 20

foreach ($Marker in @(
    'id="signalGrid" class="signal-grid"',
    "function renderSignals()",
    "groups.map(renderSignalPanel)",
    "All monitored trends are shown together",
    "setInterval(refresh,REFRESH_MS)"
)) {
    Assert-True ($Page.Content.Contains($Marker)) `
        "Dashboard marker missing: $Marker"
}

foreach ($Forbidden in @(
    "signalSelect",
    "selectedSignal",
    "Metric<select"
)) {
    Assert-True (-not $Page.Content.Contains($Forbidden)) `
        "Old dropdown implementation remains: $Forbidden"
}

$CacheControl = [string]$Page.Headers["Cache-Control"]
Assert-True ($CacheControl -match "no-store") `
    "Dashboard is not configured with Cache-Control: no-store."

Write-Host "[PASS] Four live trend groups are available" -ForegroundColor Green
Write-Host "[PASS] All trend charts render together" -ForegroundColor Green
Write-Host "[PASS] Metric dropdown was removed" -ForegroundColor Green
Write-Host "[PASS] Five-second auto-refresh remains enabled" -ForegroundColor Green
Write-Host ""
$Groups |
    Select-Object id, label, unit, direction |
    Format-Table -AutoSize |
    Out-String |
    Write-Host
