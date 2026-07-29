[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$LookbackMinutes = 180,
    [int]$LogTailLines = 30000
)

$ErrorActionPreference = 'Continue'
if (-not (Test-Path -LiteralPath $ProjectRoot)) { throw "Project root not found: $ProjectRoot" }
Set-Location -LiteralPath $ProjectRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Desktop Linux engine is not running.' }

$Stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$BundleRoot = Join-Path $HOME "Downloads\opsai-v060-analysis-$Stamp"
$ZipPath = "$BundleRoot.zip"
foreach ($Folder in @(
    $BundleRoot,
    "$BundleRoot\api",
    "$BundleRoot\logs",
    "$BundleRoot\prometheus",
    "$BundleRoot\docker",
    "$BundleRoot\source-snapshot"
)) {
    New-Item -ItemType Directory -Path $Folder -Force | Out-Null
}

function Save-Json {
    param([string]$Uri,[string]$Path)
    try {
        Invoke-RestMethod -Uri $Uri -TimeoutSec 45 |
            ConvertTo-Json -Depth 100 |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 6000
    }
    catch {
        "URI: $Uri`n$($_ | Out-String)" |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 6000
    }
}

function Save-Cmd {
    param([string]$Path,[scriptblock]$Command)
    try { & $Command 2>&1 | Out-File -LiteralPath $Path -Encoding utf8 -Width 6000 }
    catch { $_ | Out-String | Out-File -LiteralPath $Path -Encoding utf8 -Width 6000 }
}

function Save-Range {
    param([string]$Name,[string]$Query,[long]$Start,[long]$End)
    $Encoded = [uri]::EscapeDataString($Query)
    Save-Json `
        -Uri "http://localhost:9090/api/v1/query_range?query=$Encoded&start=$Start&end=$End&step=15" `
        -Path "$BundleRoot\prometheus\$Name"
}

$End = [DateTimeOffset]::UtcNow
$Start = $End.AddMinutes(-$LookbackMinutes)
$Since = "${LookbackMinutes}m"

$Endpoints = [ordered]@{
    'scenario-health.json' = 'http://localhost:8090/health'
    'scenario-state.json' = 'http://localhost:8090/state'
    'test-runs.json' = 'http://localhost:8090/test-runs?limit=20'
    'test-catalog.json' = 'http://localhost:8090/test-runs/catalog'
    'core-health.json' = 'http://localhost:8095/health'
    'incidents-all.json' = 'http://localhost:8095/incidents?status=all&limit=500'
    'core-evaluation.json' = 'http://localhost:8095/evaluation'
    'agent-health.json' = 'http://localhost:8096/health'
    'investigations-all.json' = 'http://localhost:8096/api/investigations'
    'automation-health.json' = 'http://localhost:8097/health'
    'automation-summary.json' = 'http://localhost:8097/summary'
    'automation-state.json' = 'http://localhost:8097/state'
    'automation-activity.json' = 'http://localhost:8097/activity?limit=500'
    'automation-tickets.json' = 'http://localhost:8097/tickets'
    'predictor-health.json' = 'http://localhost:8098/health'
    'predictor-summary.json' = 'http://localhost:8098/summary'
    'predictions.json' = 'http://localhost:8098/predictions?status=all'
    'prediction-events.json' = 'http://localhost:8098/events?limit=500'
    'external-auth-health.json' = 'http://localhost:8099/health'
    'checkout-health.json' = 'http://localhost:8080/health'
    'prometheus-targets.json' = 'http://localhost:9090/api/v1/targets'
}
foreach ($Item in $Endpoints.GetEnumerator()) {
    Save-Json -Uri $Item.Value -Path "$BundleRoot\api\$($Item.Key)"
}

# Save latest test-run detail when available.
try {
    $RunList = Invoke-RestMethod -Uri 'http://localhost:8090/test-runs?limit=20' -TimeoutSec 30
    $LatestRun = @($RunList.runs) |
        Sort-Object {
            $Value = $_.completedAt
            if (-not $Value) { $Value = $_.updatedAt }
            if (-not $Value) { $Value = $_.startedAt }
            try { [datetime]$Value } catch { [datetime]::MinValue }
        } -Descending |
        Select-Object -First 1
    if ($LatestRun -and $LatestRun.id) {
        Save-Json -Uri "http://localhost:8090/test-runs/$($LatestRun.id)" -Path "$BundleRoot\api\latest-test-run.json"
    }
}
catch {
    $_ | Out-String | Out-File -LiteralPath "$BundleRoot\api\latest-test-run-error.txt" -Encoding utf8
}

# Save recent incident detail and operations context.
try {
    $IncidentRows = @((Invoke-RestMethod -Uri 'http://localhost:8095/incidents?status=all&limit=500' -TimeoutSec 30).incidents)
    foreach ($Incident in $IncidentRows) {
        $Opened = $null
        try { $Opened = [DateTimeOffset]$Incident.opened_at } catch { continue }
        if ($Opened -lt $Start) { continue }
        $SafeId = ([string]$Incident.id) -replace '[^A-Za-z0-9_-]','_'
        Save-Json -Uri "http://localhost:8095/incidents/$($Incident.id)" -Path "$BundleRoot\api\incident-$SafeId.json"
        Save-Json -Uri "http://localhost:8097/api/incidents/$($Incident.id)/operations" -Path "$BundleRoot\api\operations-$SafeId.json"
    }
}
catch {
    $_ | Out-String | Out-File -LiteralPath "$BundleRoot\api\incident-detail-error.txt" -Encoding utf8
}

Save-Cmd "$BundleRoot\docker\compose-ps.txt" { docker compose ps --all }
Save-Cmd "$BundleRoot\docker\compose-images.txt" { docker compose images }
Save-Cmd "$BundleRoot\docker\compose-config.txt" { docker compose config }
Save-Cmd "$BundleRoot\docker\container-inspect.json" {
    $Ids = @(docker compose ps -aq)
    if ($Ids.Count) { docker inspect $Ids }
}

Save-Cmd "$BundleRoot\logs\00-all-services.log" {
    docker compose logs --no-color --timestamps --since $Since --tail $LogTailLines
}

$Services = @(
    'scenario-controller','opsai-core','opsai-agent','opsai-automation','opsai-predictor',
    'external-auth-service','checkout-service','payment-router','payment-node-1',
    'payment-node-2','payment-node-3','load-generator','wikimedia-adapter',
    'corruption-adapter','toxiproxy','prometheus','postgres','grafana'
)
$Index = 1
foreach ($Service in $Services) {
    $Prefix = '{0:D2}' -f $Index
    Save-Cmd "$BundleRoot\logs\$Prefix-$Service.log" {
        docker compose logs --no-color --timestamps --since $Since --tail $LogTailLines $Service
    }
    $Index++
}

$Queries = [ordered]@{
    'predictions-active.json' = 'opsai_prediction_active'
    'prediction-risk.json' = 'opsai_prediction_risk_score'
    'prediction-eta.json' = 'opsai_prediction_time_to_threshold_seconds'
    'predictions-raised.json' = 'opsai_predictions_raised_total'
    'payment-processing-p95.json' = 'histogram_quantile(0.95, sum by (le,node) (rate(opsai_payment_processing_duration_seconds_bucket[30s])))'
    'payment-capacity-units.json' = 'opsai_payment_capacity_units'
    'payment-restart-generation.json' = 'opsai_payment_restart_generation'
    'payment-restarts.json' = 'opsai_payment_service_restarts_total'
    'router-node-p95.json' = 'histogram_quantile(0.95, sum by (le,node) (rate(opsai_router_node_duration_seconds_bucket[1m])))'
    'checkout-p95.json' = 'histogram_quantile(0.95, sum by (le) (rate(opsai_checkout_duration_seconds_bucket[1m])))'
    'checkout-failure-percent.json' = '100 * sum(rate(opsai_checkout_requests_total{status="failed"}[1m])) / clamp_min(sum(rate(opsai_checkout_requests_total[1m])), 0.001)'
    'external-auth-failures.json' = 'sum(rate(opsai_external_service_auth_failures_total[30s])) by (service,reason)'
    'external-call-status.json' = 'sum(rate(opsai_external_service_calls_total[30s])) by (service,status)'
    'external-token-generations.json' = 'opsai_external_auth_token_generation or opsai_checkout_external_token_generation'
    'disk-usage.json' = 'opsai_demo_disk_usage_percent'
    'certificate-expiry.json' = 'opsai_demo_certificate_expiry_seconds'
    'automatic-remediations.json' = 'opsai_automatic_remediations_total'
    'auto-repaired-kpi.json' = 'opsai_auto_repaired_incidents'
}
foreach ($Item in $Queries.GetEnumerator()) {
    Save-Range -Name $Item.Key -Query $Item.Value -Start $Start.ToUnixTimeSeconds() -End $End.ToUnixTimeSeconds()
}

$FilesToCopy = @(
    'compose.yaml','.env.example','observability\prometheus\prometheus.yml',
    'scripts\test-opsai-v06.ps1','scripts\test-opsai-predictive.ps1',
    'scripts\test-opsai-auth-restart.ps1','scripts\test-opsai-auto-repair.ps1',
    'services\opsai-predictor\app\main.py','services\opsai-core\app\main.py',
    'services\opsai-agent\app\main.py','services\opsai-automation\app\main.py',
    'services\scenario-controller\app\main.py','services\external-auth-service\app\main.py',
    'services\checkout-service\app\main.py','services\payment-service\app\main.py',
    'services\opsai-agent\knowledge\catalog.json','docs\day5-predictive.md'
)
foreach ($Relative in $FilesToCopy) {
    $Source = Join-Path $ProjectRoot $Relative
    if (Test-Path -LiteralPath $Source) {
        $Destination = Join-Path "$BundleRoot\source-snapshot" $Relative
        New-Item -ItemType Directory -Path (Split-Path -Parent $Destination) -Force | Out-Null
        Copy-Item -LiteralPath $Source -Destination $Destination -Force
    }
}

@"
PulseGuard v0.6 analysis bundle
Captured: $(Get-Date -Format o)
Lookback: $LookbackMinutes minutes

Coverage:
- Reactive incidents and test-run records
- Deterministic predictive events, risk scores and lead times
- REAL_AI prediction explanations
- External authentication failures and governed credential refresh
- Approval-based bounded application restart
- Automation actions, repair outcomes, tickets and live activity
- Prometheus evidence and Docker logs

Safety:
- .env was not copied.
- /admin/current-token was never queried by this collector.
- Certificate/private-key files were not copied.
- Bearer tokens, API keys, passwords and private-key blocks are redacted.
- No scenario was injected, reset or cleared.
"@ | Out-File -LiteralPath "$BundleRoot\README.txt" -Encoding utf8

# Redact common secret forms from text artifacts.
Get-ChildItem -LiteralPath $BundleRoot -Recurse -File | ForEach-Object {
    try {
        $Content = Get-Content -LiteralPath $_.FullName -Raw -ErrorAction Stop
        $Content = $Content -replace '(?i)(AZURE_OPENAI_API_KEY\s*[:=]\s*)[^\s"'';,]+','${1}<REDACTED>'
        $Content = $Content -replace '(?i)(OPENAI_API_KEY\s*[:=]\s*)[^\s"'';,]+','${1}<REDACTED>'
        $Content = $Content -replace '(?i)(Authorization\s*[:=]\s*["'']?Bearer\s+)[A-Za-z0-9._~+/\-=]+','${1}<REDACTED>'
        $Content = $Content -replace '(?i)("?token"?\s*:\s*")[^"]+("),','${1}<REDACTED>${2},'
        $Content = $Content -replace '(?i)(password["'']?\s*[:=]\s*["'']?)[^"''\s,;]+','${1}<REDACTED>'
        $Content = $Content -replace '-----BEGIN (RSA |EC |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?-----END (RSA |EC |ENCRYPTED )?PRIVATE KEY-----','<PRIVATE KEY REDACTED>'
        [IO.File]::WriteAllText($_.FullName,$Content,[Text.UTF8Encoding]::new($false))
    }
    catch { }
}

Compress-Archive -Path "$BundleRoot\*" -DestinationPath $ZipPath -Force
$Hash = Get-FileHash -Algorithm SHA256 -LiteralPath $ZipPath
Write-Host ''
Write-Host "Analysis ZIP created: $ZipPath" -ForegroundColor Green
Write-Host "SHA-256: $($Hash.Hash)" -ForegroundColor DarkGray
Write-Host 'Upload this ZIP in the chat.' -ForegroundColor Cyan
