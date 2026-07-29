[CmdletBinding()]
param(
    [ValidateSet('disk','node','capacity','all')]
    [string]$Scenario = 'all',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$TimeoutSeconds = 210
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

function Get-NewReactiveIncidents {
    param([datetime]$StartedAt, [string[]]$Types)
    $Rows = @((Invoke-RestMethod -Uri "$Core/incidents?status=all&limit=500" -TimeoutSec 20).incidents)
    return @($Rows | Where-Object {
        $Opened = $null
        try { $Opened = [datetime]$_.opened_at } catch { return $false }
        $Opened -ge $StartedAt.AddSeconds(-5) -and $Types -contains [string]$_.incident_type
    })
}

function Wait-Prediction {
    param(
        [string]$PredictionType,
        [datetime]$StartedAt,
        [string[]]$ReactiveTypes
    )

    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Rows = @((Invoke-RestMethod -Uri "$Predictor/predictions?status=all" -TimeoutSec 20).predictions)
        $Prediction = $Rows |
            Where-Object {
                $_.predictionType -eq $PredictionType -and
                ([datetime]$_.firstPredictedAt) -ge $StartedAt.AddSeconds(-5)
            } |
            Sort-Object { [datetime]$_.updatedAt } -Descending |
            Select-Object -First 1

        if ($Prediction -and $Prediction.aiExplanation) {
            $Reactive = Get-NewReactiveIncidents -StartedAt $StartedAt -Types $ReactiveTypes
            if ($Reactive.Count -gt 0) {
                $Types = (@($Reactive | ForEach-Object { $_.incident_type }) -join ', ')
                throw "A reactive incident opened before the predictive event was demonstrated: $Types"
            }
            return $Prediction
        }
        Start-Sleep -Seconds 4
    }

    $DiagnosticPath = Join-Path $HOME ("Downloads\opsai-predictive-timeout-{0}-{1}.json" -f $PredictionType,(Get-Date -Format 'yyyyMMdd_HHmmss'))
    [pscustomobject]@{
        predictionType = $PredictionType
        predictorHealth = Invoke-RestMethod -Uri "$Predictor/health" -TimeoutSec 15
        predictorSummary = Invoke-RestMethod -Uri "$Predictor/summary" -TimeoutSec 15
        predictions = (Invoke-RestMethod -Uri "$Predictor/predictions?status=all" -TimeoutSec 15).predictions
        coreEvaluation = Invoke-RestMethod -Uri "$Core/evaluation" -TimeoutSec 15
    } | ConvertTo-Json -Depth 60 | Out-File -LiteralPath $DiagnosticPath -Encoding utf8 -Width 5000
    throw "Timed out waiting for $PredictionType. Diagnostic saved to $DiagnosticPath"
}

function Show-Prediction {
    param($Prediction)
    $AI = $Prediction.aiExplanation
    Write-Host ("[PASS] {0}" -f $Prediction.predictionType) -ForegroundColor Green
    Write-Host ("  Scope       : {0}" -f $Prediction.scope)
    Write-Host ("  Risk        : {0}%" -f [math]::Round(100 * [double]$Prediction.riskScore))
    Write-Host ("  Confidence  : {0}%" -f [math]::Round(100 * [double]$Prediction.confidence))
    Write-Host ("  Lead time   : {0} seconds" -f $Prediction.timeToThresholdSeconds)
    Write-Host ("  AI mode     : {0}" -f $AI.analysisMode)
    Write-Host ("  Explanation : {0}" -f $AI.summary)
    Write-Host ("  Recommendation: {0}" -f $AI.recommendedPreventiveAction)
    Write-Host ("  Authorised  : {0}; Executed: {1}" -f $AI.authorised,$AI.executed)
    if ($AI.authorised -eq $true -or $AI.executed -eq $true) {
        throw 'Day 5 observation mode must not authorize or execute preventive actions.'
    }
}

function Run-DiskPrediction {
    Reset-Environment
    Write-Host 'Starting gradual bounded disk growth...' -ForegroundColor Yellow
    $Started = Get-Date
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-disk-growth?start_percent=35&end_percent=79&duration_seconds=120" -TimeoutSec 30 | Out-Null
    $Prediction = Wait-Prediction -PredictionType 'PREDICTED_DISK_PRESSURE' -StartedAt $Started -ReactiveTypes @('NODE_DISK_PRESSURE')
    Show-Prediction $Prediction
}

function Run-NodePrediction {
    Reset-Environment
    Write-Host 'Starting gradual isolated node degradation...' -ForegroundColor Yellow
    $Started = Get-Date
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-node-degradation?node_id=payment-node-2&start_pressure_ms=100&end_pressure_ms=650&duration_seconds=120" -TimeoutSec 30 | Out-Null
    $Prediction = Wait-Prediction -PredictionType 'PREDICTED_PAYMENT_NODE_DEGRADATION' -StartedAt $Started -ReactiveTypes @('PAYMENT_NODE_LATENCY')
    Show-Prediction $Prediction
}

function Run-CapacityPrediction {
    Reset-Environment
    Write-Host 'Starting reduced-redundancy capacity trend...' -ForegroundColor Yellow
    $Started = Get-Date
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/predictive-capacity-risk?start_pressure_ms=100&end_pressure_ms=650&duration_seconds=150" -TimeoutSec 30 | Out-Null
    $Prediction = Wait-Prediction -PredictionType 'PREDICTED_CAPACITY_SATURATION' -StartedAt $Started -ReactiveTypes @('PAYMENT_FLEET_CAPACITY_DEGRADATION')
    Show-Prediction $Prediction
}

try {
    if ($Scenario -in @('disk','all')) { Run-DiskPrediction }
    if ($Scenario -in @('node','all')) { Run-NodePrediction }
    if ($Scenario -in @('capacity','all')) { Run-CapacityPrediction }

    $Summary = Invoke-RestMethod -Uri "$Predictor/summary" -TimeoutSec 20
    Write-Host ''
    Write-Host ("Predictor totals: {0} prediction(s), {1} REAL_AI explanation(s)." -f $Summary.totalPredictions,$Summary.realAiExplanations) -ForegroundColor Cyan
}
finally {
    try { Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 45 | Out-Null } catch { Write-Warning $_ }
}
