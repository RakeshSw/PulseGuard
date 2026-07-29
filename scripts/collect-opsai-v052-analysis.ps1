[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$LookbackMinutes = 120,
    [int]$LogTailLines = 20000
)

$ErrorActionPreference = "Continue"
if (-not (Test-Path -LiteralPath $ProjectRoot)) { throw "Project root not found: $ProjectRoot" }
Set-Location -LiteralPath $ProjectRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw "Docker Desktop Linux engine is not running." }

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BundleRoot = Join-Path $HOME "Downloads\opsai-v052-analysis-$Stamp"
$ZipPath = "$BundleRoot.zip"
foreach ($Folder in @($BundleRoot,"$BundleRoot\api","$BundleRoot\logs","$BundleRoot\prometheus","$BundleRoot\docker")) {
    New-Item -ItemType Directory -Path $Folder -Force | Out-Null
}

function Save-Json([string]$Uri,[string]$Path) {
    try { Invoke-RestMethod -Uri $Uri -TimeoutSec 30 | ConvertTo-Json -Depth 100 | Out-File $Path -Encoding utf8 -Width 5000 }
    catch { "URI: $Uri`n$($_ | Out-String)" | Out-File $Path -Encoding utf8 -Width 5000 }
}
function Save-Cmd([string]$Path,[scriptblock]$Command) {
    try { & $Command 2>&1 | Out-File $Path -Encoding utf8 -Width 5000 }
    catch { $_ | Out-String | Out-File $Path -Encoding utf8 -Width 5000 }
}
function Save-Range([string]$Name,[string]$Query,[long]$Start,[long]$End) {
    $Encoded=[uri]::EscapeDataString($Query)
    Save-Json "http://localhost:9090/api/v1/query_range?query=$Encoded&start=$Start&end=$End&step=15" "$BundleRoot\prometheus\$Name"
}

$End=[DateTimeOffset]::UtcNow
$Start=$End.AddMinutes(-$LookbackMinutes)
$Since="${LookbackMinutes}m"

$Endpoints=[ordered]@{
    'incidents-all.json'='http://localhost:8095/incidents?status=all&limit=500'
    'core-evaluation.json'='http://localhost:8095/evaluation'
    'investigations-all.json'='http://localhost:8096/api/investigations'
    'automation-summary.json'='http://localhost:8097/summary'
    'automation-state.json'='http://localhost:8097/state'
    'automation-activity.json'='http://localhost:8097/activity?limit=500'
    'automation-tickets.json'='http://localhost:8097/tickets'
    'prometheus-targets.json'='http://localhost:9090/api/v1/targets'
}
foreach($e in $Endpoints.GetEnumerator()){Save-Json $e.Value "$BundleRoot\api\$($e.Key)"}

Save-Cmd "$BundleRoot\docker\compose-ps.txt" { docker compose ps --all }
Save-Cmd "$BundleRoot\docker\container-inspect.json" { $ids=@(docker compose ps -aq); if($ids.Count){docker inspect $ids} }
Save-Cmd "$BundleRoot\logs\all-services.log" { docker compose logs --no-color --timestamps --since $Since --tail $LogTailLines }
foreach($svc in @('scenario-controller','opsai-core','opsai-agent','opsai-automation','payment-router','payment-node-1','payment-node-2','payment-node-3','checkout-service','wikimedia-adapter','prometheus','postgres')){
    Save-Cmd "$BundleRoot\logs\$svc.log" { docker compose logs --no-color --timestamps --since $Since --tail $LogTailLines $svc }
}

$Queries=[ordered]@{
    'payment-processing-p95.json'='histogram_quantile(0.95, sum by (le,node) (rate(opsai_payment_processing_duration_seconds_bucket[30s])))'
    'payment-capacity-units.json'='opsai_payment_capacity_units'
    'router-node-p95.json'='histogram_quantile(0.95, sum by (le,node) (rate(opsai_router_node_duration_seconds_bucket[1m])))'
    'checkout-p95.json'='histogram_quantile(0.95, sum by (le) (rate(opsai_checkout_duration_seconds_bucket[1m])))'
    'checkout-failure-percent.json'='100 * sum(rate(opsai_checkout_requests_total{status="failed"}[1m])) / clamp_min(sum(rate(opsai_checkout_requests_total[1m])), 0.001)'
    'disk-usage.json'='opsai_demo_disk_usage_percent'
    'certificate-expiry.json'='opsai_demo_certificate_expiry_seconds'
    'automatic-remediations.json'='opsai_automatic_remediations_total'
    'auto-repaired-kpi.json'='opsai_auto_repaired_incidents'
}
foreach($q in $Queries.GetEnumerator()){Save-Range $q.Key $q.Value $Start.ToUnixTimeSeconds() $End.ToUnixTimeSeconds()}

# Redact text secrets without copying .env or certificate material.
Get-ChildItem $BundleRoot -Recurse -File | ForEach-Object {
    try {
        $c=Get-Content $_.FullName -Raw -ErrorAction Stop
        $c=$c -replace '(?i)(AZURE_OPENAI_API_KEY\s*[:=]\s*)[^\s"'';,]+','${1}<REDACTED>'
        $c=$c -replace '(?i)(Authorization\s*[:=]\s*["'']?Bearer\s+)[A-Za-z0-9._~+/\-=]+','${1}<REDACTED>'
        [IO.File]::WriteAllText($_.FullName,$c,[Text.UTF8Encoding]::new($false))
    } catch {}
}
Compress-Archive -Path "$BundleRoot\*" -DestinationPath $ZipPath -Force
Write-Host "Analysis ZIP created: $ZipPath" -ForegroundColor Green
