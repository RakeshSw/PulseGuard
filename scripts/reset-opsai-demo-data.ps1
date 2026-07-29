[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ProjectRoot)) {
    throw "Project root not found: $ProjectRoot"
}

Set-Location -LiteralPath $ProjectRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop Linux engine is not running."
}

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Default
    )

    $EnvFile = Join-Path $ProjectRoot ".env"
    if (-not (Test-Path -LiteralPath $EnvFile)) {
        return $Default
    }

    $Line = Get-Content -LiteralPath $EnvFile |
        Where-Object {
            $_ -match "^\s*$([regex]::Escape($Name))\s*="
        } |
        Select-Object -Last 1

    if (-not $Line) {
        return $Default
    }

    return (($Line -split "=", 2)[1]).Trim().Trim('"').Trim("'")
}

$DbUser = Get-DotEnvValue -Name "POSTGRES_USER" -Default "opsai"
$DbName = Get-DotEnvValue -Name "POSTGRES_DB" -Default "opsai"

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupRoot = Join-Path $HOME "Downloads\opsai-clean-reset-backup-$Stamp"
New-Item -ItemType Directory -Path $BackupRoot -Force | Out-Null

Write-Host ""
Write-Host "PulseGuard clean-data reset" -ForegroundColor Cyan
Write-Host "================================"
Write-Host "Project: $ProjectRoot"
Write-Host "Backup:  $BackupRoot"
Write-Host ""
Write-Host "This clears application history but preserves source, .env, credentials," -ForegroundColor DarkGray
Write-Host "Docker volumes, Grafana configuration and Prometheus metric history." -ForegroundColor DarkGray

try {
    Invoke-RestMethod `
        -Method Post `
        -Uri "http://localhost:8090/scenarios/reset" `
        -TimeoutSec 20 |
        Out-Null

    Write-Host "[PASS] Controlled scenarios reset" -ForegroundColor Green
}
catch {
    Write-Host "Scenario reset skipped because the controller was unavailable." -ForegroundColor DarkGray
}

Write-Host "Stopping services that can write operational state..." -ForegroundColor Cyan
docker compose stop `
    scenario-controller `
    opsai-predictor `
    opsai-agent `
    opsai-core `
    opsai-automation

if ($LASTEXITCODE -ne 0) {
    throw "Could not stop state-writing services."
}

docker compose up -d postgres
if ($LASTEXITCODE -ne 0) {
    throw "Could not start PostgreSQL."
}

$Ready = $false
for ($Attempt = 1; $Attempt -le 40; $Attempt++) {
    docker compose exec -T postgres `
        pg_isready `
        -U $DbUser `
        -d $DbName *> $null

    if ($LASTEXITCODE -eq 0) {
        $Ready = $true
        break
    }

    Start-Sleep -Seconds 2
}

if (-not $Ready) {
    throw "PostgreSQL did not become ready."
}

Write-Host "Archiving current state before clearing..." -ForegroundColor Cyan

docker compose exec -T postgres `
    sh -lc "pg_dump -Fc -U '$DbUser' -d '$DbName' -f /tmp/opsai-before-reset.dump"

if ($LASTEXITCODE -eq 0) {
    docker cp `
        "opsai-postgres:/tmp/opsai-before-reset.dump" `
        (Join-Path $BackupRoot "postgres-before-reset.dump") *> $null
}

try {
    docker cp `
        "opsai-automation:/data/automation-state.json" `
        (Join-Path $BackupRoot "automation-state.json") *> $null
}
catch {
}

try {
    docker cp `
        "opsai-scenario-controller:/data/test-runs.json" `
        (Join-Path $BackupRoot "test-runs.json") *> $null
}
catch {
}

$Sql = @'
DO $$
DECLARE
    current_table RECORD;
BEGIN
    FOR current_table IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    LOOP
        EXECUTE format(
            'TRUNCATE TABLE public.%I RESTART IDENTITY CASCADE',
            current_table.tablename
        );
    END LOOP;
END
$$;
'@

$Sql |
    docker compose exec -T postgres `
        psql `
        -U $DbUser `
        -d $DbName `
        -v ON_ERROR_STOP=1

if ($LASTEXITCODE -ne 0) {
    throw "PostgreSQL cleanup failed. Backup: $BackupRoot"
}

Write-Host "[PASS] PostgreSQL incident, investigation and problem data cleared" -ForegroundColor Green

$AutomationCleanup = @'
from pathlib import Path
import shutil

root = Path("/data")
(root / "automation-state.json").unlink(missing_ok=True)

for folder_name in ("storage", "certificates"):
    folder = root / folder_name
    if folder.exists():
        shutil.rmtree(folder)
'@

$AutomationCleanup |
    docker compose run `
        --rm `
        --no-deps `
        -T `
        --entrypoint python `
        opsai-automation `
        -

if ($LASTEXITCODE -ne 0) {
    throw "Automation-state cleanup failed. Backup: $BackupRoot"
}

Write-Host "[PASS] Automation KPI, remediation, ticket and activity history cleared" -ForegroundColor Green

$ScenarioCleanup = @'
from pathlib import Path

root = Path("/data")
for name in ("test-runs.json", "test-runs.json.tmp"):
    (root / name).unlink(missing_ok=True)
'@

$ScenarioCleanup |
    docker compose run `
        --rm `
        --no-deps `
        -T `
        --entrypoint python `
        scenario-controller `
        -

if ($LASTEXITCODE -ne 0) {
    throw "Scenario test-history cleanup failed. Backup: $BackupRoot"
}

Write-Host "[PASS] Scenario test-run history cleared" -ForegroundColor Green

Write-Host "Starting the complete platform..." -ForegroundColor Cyan
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    throw "Platform startup failed. Backup: $BackupRoot"
}

$HealthChecks = @(
    [pscustomobject]@{ Name = "Scenario Controller"; Uri = "http://localhost:8090/health" },
    [pscustomobject]@{ Name = "PulseGuard Core"; Uri = "http://localhost:8095/health" },
    [pscustomobject]@{ Name = "PulseGuard Agent"; Uri = "http://localhost:8096/health" },
    [pscustomobject]@{ Name = "PulseGuard Automation"; Uri = "http://localhost:8097/health" },
    [pscustomobject]@{ Name = "PulseGuard Predictor"; Uri = "http://localhost:8098/health" }
)

foreach ($Check in $HealthChecks) {
    $Healthy = $false

    for ($Attempt = 1; $Attempt -le 80; $Attempt++) {
        try {
            $Response = Invoke-RestMethod `
                -Uri $Check.Uri `
                -TimeoutSec 5

            if ($Response.status -eq "healthy") {
                $Healthy = $true
                break
            }
        }
        catch {
        }

        Start-Sleep -Seconds 3
    }

    if (-not $Healthy) {
        docker compose logs `
            --no-color `
            --timestamps `
            --tail 300 `
            opsai-core `
            opsai-agent `
            opsai-automation `
            opsai-predictor `
            scenario-controller

        throw "$($Check.Name) did not become healthy. Backup: $BackupRoot"
    }

    Write-Host "[PASS] $($Check.Name)" -ForegroundColor Green
}

Start-Sleep -Seconds 8

$Incidents = Invoke-RestMethod `
    -Uri "http://localhost:8095/incidents?status=all&limit=10" `
    -TimeoutSec 20

$AutomationSummary = Invoke-RestMethod `
    -Uri "http://localhost:8097/summary" `
    -TimeoutSec 20

$IncidentCount = @($Incidents.incidents).Count

if ($IncidentCount -ne 0) {
    throw "Expected zero incidents after cleanup; found $IncidentCount."
}

foreach ($Check in @(
    [pscustomobject]@{ Name = "autoRepaired"; Value = [int]$AutomationSummary.autoRepaired },
    [pscustomobject]@{ Name = "awaitingApproval"; Value = [int]$AutomationSummary.awaitingApproval },
    [pscustomobject]@{ Name = "assignedToSupport"; Value = [int]$AutomationSummary.assignedToSupport },
    [pscustomobject]@{ Name = "resolved"; Value = [int]$AutomationSummary.resolved }
)) {
    if ($Check.Value -ne 0) {
        throw "Expected $($Check.Name)=0 after cleanup; found $($Check.Value)."
    }
}

Write-Host ""
Write-Host "[PASS] Incident Console has zero incidents" -ForegroundColor Green
Write-Host "[PASS] Auto-repaired KPI reset to zero" -ForegroundColor Green
Write-Host "[PASS] Awaiting-approval KPI reset to zero" -ForegroundColor Green
Write-Host "[PASS] Support-assignment and resolved KPIs reset to zero" -ForegroundColor Green

Write-Host ""
Write-Host "PulseGuard is now in a clean application-data state." -ForegroundColor Green
Write-Host ""
Write-Host "Hard-refresh:"
Write-Host "  http://localhost:8095"
Write-Host "  http://localhost:8098"
Write-Host ""
Write-Host "Use Ctrl+F5 once."
Write-Host ""
Write-Host "Prometheus history was intentionally preserved, so graphs may show"
Write-Host "the previous 15-minute metric window until it ages out."
Write-Host ""
Write-Host "Backup:"
Write-Host "  $BackupRoot"
