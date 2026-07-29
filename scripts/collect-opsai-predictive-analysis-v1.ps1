[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$LookbackMinutes = 120,
    [int]$LogTailLines = 30000
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ProjectRoot)) {
    throw "Project root not found: $ProjectRoot"
}

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OutRoot = Join-Path $HOME "Downloads\opsai-predictive-analysis-$Stamp"
$ApiRoot = Join-Path $OutRoot "api"
$PromRoot = Join-Path $OutRoot "prometheus"
$LogRoot = Join-Path $OutRoot "logs"
$SourceRoot = Join-Path $OutRoot "source-snapshot"

New-Item -ItemType Directory -Path $ApiRoot,$PromRoot,$LogRoot,$SourceRoot -Force | Out-Null

function Save-JsonEndpoint {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][string]$Destination
    )
    try {
        Invoke-RestMethod -Uri $Uri -TimeoutSec 30 |
            ConvertTo-Json -Depth 100 |
            Out-File -LiteralPath $Destination -Encoding utf8 -Width 12000
    }
    catch {
        [pscustomobject]@{
            uri = $Uri
            error = $_.Exception.Message
            capturedAt = (Get-Date).ToString("o")
        } |
            ConvertTo-Json -Depth 10 |
            Out-File -LiteralPath $Destination -Encoding utf8
    }
}

function Save-PrometheusRange {
    param(
        [Parameter(Mandatory = $true)][string]$Query,
        [Parameter(Mandatory = $true)][string]$Destination,
        [int]$StepSeconds = 15
    )
    $End = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
    $Start = $End - ($LookbackMinutes * 60)
    $Encoded = [uri]::EscapeDataString($Query)
    $Uri = "http://localhost:9090/api/v1/query_range?query=$Encoded&start=$Start&end=$End&step=$StepSeconds"
    Save-JsonEndpoint -Uri $Uri -Destination $Destination
}

Write-Host ""
Write-Host "Collecting PulseGuard predictive-analysis evidence..." -ForegroundColor Cyan
Write-Host "Output: $OutRoot"

Save-JsonEndpoint "http://localhost:8098/health" "$ApiRoot\predictor-health.json"
Save-JsonEndpoint "http://localhost:8098/summary" "$ApiRoot\predictor-summary.json"
Save-JsonEndpoint "http://localhost:8098/calculations" "$ApiRoot\predictor-calculations.json"
Save-JsonEndpoint "http://localhost:8098/signals" "$ApiRoot\predictor-signals.json"
Save-JsonEndpoint "http://localhost:8098/predictions?status=all" "$ApiRoot\predictor-predictions.json"
Save-JsonEndpoint "http://localhost:8098/patterns?status=all" "$ApiRoot\predictor-patterns.json"
Save-JsonEndpoint "http://localhost:8098/problem-candidates" "$ApiRoot\problem-candidates.json"
Save-JsonEndpoint "http://localhost:8098/problem-register" "$ApiRoot\problem-register.json"
Save-JsonEndpoint "http://localhost:8098/problem-register/summary" "$ApiRoot\problem-register-summary.json"
Save-JsonEndpoint "http://localhost:8098/events?limit=500" "$ApiRoot\predictor-events.json"
Save-JsonEndpoint "http://localhost:8096/health" "$ApiRoot\agent-health.json"
Save-JsonEndpoint "http://localhost:8095/health" "$ApiRoot\core-health.json"
Save-JsonEndpoint "http://localhost:8095/incidents?status=all&limit=500" "$ApiRoot\incidents-all.json"
Save-JsonEndpoint "http://localhost:8095/problems?status=all&limit=500" "$ApiRoot\core-problems-all.json"
Save-JsonEndpoint "http://localhost:8095/problems/summary" "$ApiRoot\core-problems-summary.json"
Save-JsonEndpoint "http://localhost:9090/api/v1/targets" "$ApiRoot\prometheus-targets.json"

Save-PrometheusRange `
    'histogram_quantile(0.95, sum by (le,node) (rate(opsai_payment_processing_duration_seconds_bucket[1m])))' `
    "$PromRoot\payment-node-p95.json"

Save-PrometheusRange `
    'opsai_demo_disk_usage_percent' `
    "$PromRoot\disk-usage-percent.json"

Save-PrometheusRange `
    'opsai_demo_certificate_expiry_seconds' `
    "$PromRoot\certificate-expiry-seconds.json"

Save-PrometheusRange `
    'sum(rate(opsai_checkout_requests_total[1m]))' `
    "$PromRoot\checkout-throughput.json"

Save-PrometheusRange `
    'opsai_payment_capacity_units' `
    "$PromRoot\payment-capacity-units.json"

Save-PrometheusRange `
    'opsai_payment_fault_mode_info{mode="unavailable"}' `
    "$PromRoot\payment-unavailable-state.json"

Save-PrometheusRange `
    'opsai_prediction_risk_score' `
    "$PromRoot\prediction-risk-score.json"

Save-PrometheusRange `
    'opsai_prediction_time_to_threshold_seconds' `
    "$PromRoot\prediction-eta.json"

Set-Location -LiteralPath $ProjectRoot

docker compose logs `
    --no-color `
    --timestamps `
    --tail $LogTailLines `
    opsai-predictor |
    Out-File "$LogRoot\opsai-predictor.log" -Encoding utf8 -Width 12000

docker compose logs `
    --no-color `
    --timestamps `
    --tail $LogTailLines `
    opsai-agent |
    Out-File "$LogRoot\opsai-agent.log" -Encoding utf8 -Width 12000

docker compose logs `
    --no-color `
    --timestamps `
    --tail $LogTailLines `
    opsai-core |
    Out-File "$LogRoot\opsai-core.log" -Encoding utf8 -Width 12000

docker compose ps |
    Out-File "$OutRoot\docker-compose-ps.txt" -Encoding utf8 -Width 12000

Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "services\opsai-predictor\app\main.py") `
    -Destination (Join-Path $SourceRoot "opsai-predictor-main.py") `
    -Force

Copy-Item `
    -LiteralPath (Join-Path $ProjectRoot "services\opsai-core\app\main.py") `
    -Destination (Join-Path $SourceRoot "opsai-core-main.py") `
    -Force

$DashboardResponse = Invoke-WebRequest `
    -Uri "http://localhost:8098/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 30

$DashboardResponse.Content |
    Out-File "$OutRoot\predictive-dashboard.html" -Encoding utf8 -Width 12000

@"
PulseGuard predictive-analysis evidence bundle
Captured: $(Get-Date -Format o)
Lookback: $LookbackMinutes minutes

Included:
- Predictor health, summary, calculations, live signals, predictions, patterns, problem candidates and events
- Agent and Core health
- Incident history used for recurrence analysis
- Persistent Problem Register, lifecycle status, ownership and linked incidents
- Prometheus range-query history for latency, disk, certificate, throughput, capacity and availability
- Prediction risk and ETA metrics
- Predictor, Agent and Core logs
- Current Predictor and PulseGuard Core sources
- Rendered dashboard HTML
- Docker service status

Safety:
- .env is not copied
- API keys, bearer tokens and passwords are not collected
- Docker Compose resolved environment is not collected
"@ | Out-File "$OutRoot\README.txt" -Encoding utf8

$Zip = "$OutRoot.zip"
Remove-Item -LiteralPath $Zip -Force -ErrorAction SilentlyContinue
Compress-Archive -Path "$OutRoot\*" -DestinationPath $Zip -Force

Write-Host ""
Write-Host "Predictive-analysis bundle created:" -ForegroundColor Green
Write-Host "  $Zip"
