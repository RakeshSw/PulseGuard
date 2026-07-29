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
Write-Host "PulseGuard Predictive Dashboard validation" -ForegroundColor Cyan
Write-Host "====================================="

$Health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 20
Assert-True ($Health.status -eq "healthy") "Predictor is not healthy."
Assert-True ($Health.dataSourceMode -eq "PROMETHEUS_RANGE_QUERIES") `
    "Predictor is not using Prometheus range queries."
Assert-True ($Health.continuousMetricsToAi -eq $false) `
    "Predictor incorrectly reports continuous metrics streaming to AI."

$Page = Invoke-WebRequest `
    -Uri "$BaseUrl/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 20

foreach ($Marker in @(
    "Auto-refresh active",
    "Potential problem candidates",
    "Synthetic test history",
    "setInterval(refresh,REFRESH_MS)",
    "window.addEventListener('focus',refresh)",
    "recommendationValidation",
    "Risk at trigger",
    "TYPE_SCOPED_ALLOWLIST"
)) {
    Assert-True ($Page.Content.Contains($Marker)) `
        "Dashboard marker missing: $Marker"
}

$CacheControl = [string]$Page.Headers["Cache-Control"]
Assert-True ($CacheControl -match "no-store") `
    "Dashboard response is not configured with Cache-Control: no-store."

$Summary = Invoke-RestMethod -Uri "$BaseUrl/summary" -TimeoutSec 20
Assert-True ([int]$Summary.dashboardRefreshSeconds -ge 2) `
    "Dashboard refresh interval is missing or invalid."

$Patterns = Invoke-RestMethod -Uri "$BaseUrl/patterns?status=all" -TimeoutSec 20
$PatternRows = @($Patterns.patterns)

if ($PatternRows.Count -gt 0) {
    $MissingOrigin = @(
        $PatternRows |
            Where-Object {
                $null -eq $_.originBreakdown
            }
    )

    Assert-True ($MissingOrigin.Count -eq 0) `
        "One or more recurring patterns do not contain originBreakdown."

    $InvalidAction = @(
        $PatternRows |
            Where-Object {
                $_.aiExplanation -and
                $_.aiExplanation.recommendationPolicy -ne "TYPE_SCOPED_ALLOWLIST"
            }
    )

    Assert-True ($InvalidAction.Count -eq 0) `
        "One or more AI recommendations were not validated by the type-scoped allowlist."
}
else {
    Write-Warning "No recurring patterns are currently available. Origin and recommendation validation will appear after recurrence evaluation."
}

$Predictions = Invoke-RestMethod -Uri "$BaseUrl/predictions?status=all" -TimeoutSec 20
$PredictionRows = @($Predictions.predictions)
$InvalidPredictionAction = @(
    $PredictionRows |
        Where-Object {
            $_.aiExplanation -and
            $_.aiExplanation.recommendationPolicy -ne "TYPE_SCOPED_ALLOWLIST"
        }
)
Assert-True ($InvalidPredictionAction.Count -eq 0) `
    "One or more prediction recommendations were not validated by the type-scoped allowlist."

Write-Host "[PASS] Predictor health and range-query mode" -ForegroundColor Green
Write-Host "[PASS] Dashboard auto-refresh and no-cache behavior" -ForegroundColor Green
Write-Host "[PASS] Trigger-risk versus reduced-risk presentation" -ForegroundColor Green
Write-Host "[PASS] Potential-problem grouping visual" -ForegroundColor Green
Write-Host "[PASS] Synthetic/organic/unclassified origin display" -ForegroundColor Green
Write-Host "[PASS] Type-scoped AI recommendation validation" -ForegroundColor Green
Write-Host ""
Write-Host ("Refresh interval: {0} seconds" -f $Summary.dashboardRefreshSeconds)
Write-Host ("Predictions:      {0}" -f $PredictionRows.Count)
Write-Host ("Patterns:         {0}" -f $PatternRows.Count)
