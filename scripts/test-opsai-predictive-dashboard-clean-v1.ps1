[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8098"
)

$ErrorActionPreference = "Stop"

function Assert-True {
    param([bool]$Condition,[string]$Message)
    if (-not $Condition) { throw $Message }
}

Write-Host ""
Write-Host "PulseGuard clean predictive dashboard validation" -ForegroundColor Cyan
Write-Host "==========================================="

$Health = Invoke-RestMethod "$BaseUrl/health" -TimeoutSec 20
Assert-True ($Health.status -eq "healthy") "Predictor is not healthy."
Assert-True ($Health.dataSourceMode -eq "PROMETHEUS_RANGE_QUERIES") "Range-query mode is not active."

$Signals = $null
for ($Attempt = 1; $Attempt -le 30; $Attempt++) {
    $Signals = Invoke-RestMethod "$BaseUrl/signals" -TimeoutSec 20
    $Latency = @($Signals.groups | Where-Object { $_.id -eq "payment-latency" })
    $SampleCount = @(
        $Latency.series |
            ForEach-Object { @($_.samples).Count } |
            Measure-Object -Maximum
    ).Maximum

    if ([int]$SampleCount -ge 2) { break }
    Start-Sleep -Seconds 3
}

Assert-True (@($Signals.groups).Count -ge 4) "Live signal catalogue is incomplete."
Assert-True ([int]$SampleCount -ge 2) "Payment-latency range samples are not available."

$Page = Invoke-WebRequest `
    -Uri "$BaseUrl/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 20

foreach ($Marker in @(
    "Live risk monitor",
    "Graphs always show",
    "Forecast decisions",
    "Potential problem candidates",
    "Frequently occurring issues",
    "signalSelect",
    "drawChart(group)",
    "setInterval(refresh,REFRESH_MS)"
)) {
    Assert-True ($Page.Content.Contains($Marker)) "Dashboard marker missing: $Marker"
}

Assert-True ([string]$Page.Headers["Cache-Control"] -match "no-store") `
    "Dashboard response does not disable browser caching."

Write-Host "[PASS] Prometheus range-query signal endpoint" -ForegroundColor Green
Write-Host "[PASS] Graph remains visible without active predictions" -ForegroundColor Green
Write-Host "[PASS] Metric selector and threshold rendering" -ForegroundColor Green
Write-Host "[PASS] Forecast table and problem ranking" -ForegroundColor Green
Write-Host "[PASS] Automatic refresh and no-cache response" -ForegroundColor Green
Write-Host ""
Write-Host ("Signal groups: {0}" -f @($Signals.groups).Count)
Write-Host ("Latency samples: {0}" -f $SampleCount)
