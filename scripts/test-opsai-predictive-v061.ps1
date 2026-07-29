[CmdletBinding()]
param(
    [ValidateSet('disk','node','capacity','recurrence','all')]
    [string]$Scenario = 'all',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$TimeoutSeconds = 240
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $ProjectRoot

$ScenarioController = 'http://localhost:8090'
$Predictor = 'http://localhost:8098'
$Core = 'http://localhost:8095'

function Reset-Environment {
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 45 | Out-Null
    Start-Sleep -Seconds 12
}

function Wait-Prediction {
    param([string]$PredictionType,[datetime]$StartedAt)

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Rows = @((Invoke-RestMethod -Uri "$Predictor/predictions?status=all" -TimeoutSec 20).predictions)
        $Prediction = $Rows |
            Where-Object {
                $_.predictionType -eq $PredictionType -and
                ([datetime]$_.firstPredictedAt) -ge $StartedAt.AddSeconds(-10)
            } |
            Sort-Object { [datetime]$_.updatedAt } -Descending |
            Select-Object -First 1

        if ($Prediction -and $Prediction.aiExplanation) {
            return $Prediction
        }
        Start-Sleep -Seconds 5
    }

    $DiagnosticPath = Join-Path $HOME ("Downloads\opsai-v061-predictive-timeout-{0}-{1}.json" -f $PredictionType,(Get-Date -Format 'yyyyMMdd_HHmmss'))
    [pscustomobject]@{
        predictionType = $PredictionType
        health = Invoke-RestMethod -Uri "$Predictor/health" -TimeoutSec 15
        summary = Invoke-RestMethod -Uri "$Predictor/summary" -TimeoutSec 15
        calculations = Invoke-RestMethod -Uri "$Predictor/calculations" -TimeoutSec 15
        predictions = (Invoke-RestMethod -Uri "$Predictor/predictions?status=all" -TimeoutSec 15).predictions
    } | ConvertTo-Json -Depth 80 | Out-File -LiteralPath $DiagnosticPath -Encoding utf8 -Width 6000
    throw "Timed out waiting for $PredictionType. Diagnostic saved to $DiagnosticPath"
}

function Assert-RangePrediction {
    param($Prediction)

    if ($Prediction.dataSource.type -ne 'PROMETHEUS_RANGE_QUERY') {
        throw "Prediction did not use a Prometheus range query."
    }
    if (-not $Prediction.calculation) {
        throw "Prediction has no calculation detail."
    }
    if ($Prediction.aiExplanation.analysisMode -ne 'REAL_AI') {
        throw "Prediction was not explained by REAL_AI."
    }
    if ($Prediction.aiExplanation.authorised -eq $true -or $Prediction.aiExplanation.executed -eq $true) {
        throw "Observation mode must not authorize or execute preventive action."
    }

    Write-Host ("[PASS] {0}" -f $Prediction.predictionType) -ForegroundColor Green
    Write-Host ("  Scope        : {0}" -f $Prediction.scope)
    Write-Host ("  Data source  : {0}" -f $Prediction.dataSource.type)
    Write-Host ("  Range window : {0} seconds" -f $Prediction.dataSource.windowSeconds)
    Write-Host ("  Range step   : {0} seconds" -f $Prediction.dataSource.stepSeconds)
    Write-Host ("  Samples      : {0}" -f $Prediction.calculation.sampleCount)
    Write-Host ("  Current      : {0}" -f $Prediction.currentValue)
    Write-Host ("  Threshold    : {0}" -f $Prediction.threshold)
    Write-Host ("  Formula      : {0}" -f $Prediction.calculation.formula)
    Write-Host ("  Lead time    : {0} seconds" -f $Prediction.timeToThresholdSeconds)
    Write-Host ("  AI contacted : {0}" -f $Prediction.aiContact.triggered)
    Write-Host ("  AI mode      : {0}" -f $Prediction.aiExplanation.analysisMode)
    Write-Host ("  Suggestion   : {0}" -f $Prediction.aiExplanation.recommendedPreventiveAction)
}

function Run-Disk {
    Reset-Environment
    $Started = Get-Date
    Write-Host 'Starting disk-growth prediction scenario...' -ForegroundColor Yellow
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-disk-growth?start_percent=35&end_percent=79&duration_seconds=120" -TimeoutSec 30 | Out-Null
    Assert-RangePrediction (Wait-Prediction -PredictionType 'PREDICTED_DISK_PRESSURE' -StartedAt $Started)
}

function Run-Node {
    Reset-Environment
    $Started = Get-Date
    Write-Host 'Starting isolated-node degradation prediction scenario...' -ForegroundColor Yellow
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-node-degradation?node_id=payment-node-2&start_pressure_ms=100&end_pressure_ms=650&duration_seconds=150" -TimeoutSec 30 | Out-Null
    Assert-RangePrediction (Wait-Prediction -PredictionType 'PREDICTED_PAYMENT_NODE_DEGRADATION' -StartedAt $Started)
}

function Run-Capacity {
    Reset-Environment
    $Started = Get-Date
    Write-Host 'Starting capacity-risk prediction scenario...' -ForegroundColor Yellow
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-capacity-risk?start_pressure_ms=100&end_pressure_ms=650&duration_seconds=180" -TimeoutSec 30 | Out-Null
    Assert-RangePrediction (Wait-Prediction -PredictionType 'PREDICTED_CAPACITY_SATURATION' -StartedAt $Started)
}

function Show-Recurrence {
    Write-Host 'Evaluating frequent operational issue patterns...' -ForegroundColor Yellow
    Start-Sleep -Seconds 65
    $Patterns = @((Invoke-RestMethod -Uri "$Predictor/patterns?status=all" -TimeoutSec 20).patterns)
    $Rows = @($Patterns | Where-Object { $_.occurrences -ge $_.threshold })
    if ($Rows.Count -eq 0) {
        Write-Warning 'No incident pattern currently crosses the recurrence threshold. Run repeated latency scenarios or retain incident history, then check the Predictive Console.'
        return
    }

    foreach ($Pattern in $Rows) {
        Write-Host ("[PASS] Frequent pattern: {0} / {1}" -f $Pattern.incidentType,$Pattern.scope) -ForegroundColor Green
        Write-Host ("  Occurrences : {0} in {1} hours" -f $Pattern.occurrences,$Pattern.lookbackHours)
        Write-Host ("  Threshold   : {0}" -f $Pattern.threshold)
        Write-Host ("  AI mode     : {0}" -f $Pattern.aiExplanation.analysisMode)
        Write-Host ("  Suggestion  : {0}" -f $Pattern.aiExplanation.recommendedPreventiveAction)
    }
}

$Health = Invoke-RestMethod -Uri "$Predictor/health" -TimeoutSec 20
if ($Health.dataSourceMode -ne 'PROMETHEUS_RANGE_QUERIES') {
    throw "Predictor is not in Prometheus range-query mode."
}
if ($Health.continuousMetricsToAi -ne $false) {
    throw "Predictor incorrectly reports continuous metric streaming to AI."
}
Write-Host ("[PASS] Predictor mode: {0}" -f $Health.mode) -ForegroundColor Green

try {
    if ($Scenario -in @('disk','all')) { Run-Disk }
    if ($Scenario -in @('node','all')) { Run-Node }
    if ($Scenario -in @('capacity','all')) { Run-Capacity }
    if ($Scenario -in @('recurrence','all')) { Show-Recurrence }
}
finally {
    try { Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 45 | Out-Null } catch { Write-Warning $_ }
}
