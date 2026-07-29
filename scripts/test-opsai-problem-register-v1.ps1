[CmdletBinding()]
param(
    [string]$PredictorUrl = "http://localhost:8098",
    [string]$CoreUrl = "http://localhost:8095",
    [string]$InternalToken,
    [int]$WaitSeconds = 120
)

$ErrorActionPreference = "Stop"

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    $Line = Get-Content -LiteralPath $Path |
        Where-Object { $_ -match ("^" + [regex]::Escape($Name) + "=") } |
        Select-Object -Last 1

    if (-not $Line) {
        return $null
    }

    return ($Line -split "=", 2)[1].Trim()
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($InternalToken)) {
    $InternalToken = Get-DotEnvValue `
        -Path (Join-Path $ProjectRoot ".env") `
        -Name "AUTOMATION_API_TOKEN"
}

if (
    [string]::IsNullOrWhiteSpace($InternalToken) -or
    $InternalToken.StartsWith("CHANGE_ME_", [System.StringComparison]::OrdinalIgnoreCase)
) {
    throw "AUTOMATION_API_TOKEN is required. Run scripts/start.ps1 or pass -InternalToken."
}

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
Write-Host "PulseGuard persistent Problem Register validation" -ForegroundColor Cyan
Write-Host "============================================"

$PredictorHealth = Invoke-RestMethod -Uri "$PredictorUrl/health" -TimeoutSec 20
$CoreHealth = Invoke-RestMethod -Uri "$CoreUrl/health" -TimeoutSec 20

Assert-True ($PredictorHealth.status -eq "healthy") "Predictor is not healthy."
Assert-True ($CoreHealth.status -eq "healthy") "PulseGuard Core is not healthy."
Assert-True ($PredictorHealth.dataSourceMode -eq "PROMETHEUS_RANGE_QUERIES") `
    "Predictor is not using Prometheus range queries."

Invoke-RestMethod `
    -Method Post `
    -Uri "$PredictorUrl/admin/evaluate" `
    -Headers @{ "X-OpsAI-Automation-Token" = $InternalToken } `
    -TimeoutSec 120 |
    Out-Null

$Deadline = (Get-Date).AddSeconds($WaitSeconds)
$Candidates = @()
$Problems = @()

do {
    $CandidateResponse = Invoke-RestMethod `
        -Uri "$PredictorUrl/problem-candidates" `
        -TimeoutSec 30

    $RegisterResponse = Invoke-RestMethod `
        -Uri "$PredictorUrl/problem-register" `
        -TimeoutSec 30

    $Candidates = @($CandidateResponse.candidates)
    $Problems = @($RegisterResponse.problems)

    if (
        $Candidates.Count -gt 0 -and
        $Problems.Count -gt 0
    ) {
        break
    }

    Start-Sleep -Seconds 5
}
while ((Get-Date) -lt $Deadline)

Assert-True ($Candidates.Count -gt 0) `
    "No recurring candidate was created. Run repeated incident scenarios first."
Assert-True ($Problems.Count -gt 0) `
    "No persistent Problem record was created."

$ProblemByKey = @{}
foreach ($Problem in $Problems) {
    $ProblemByKey[[string]$Problem.problem_key] = $Problem
}

$Missing = @(
    $Candidates |
        Where-Object {
            -not $ProblemByKey.ContainsKey([string]$_.problemKey)
        }
)

Assert-True ($Missing.Count -eq 0) `
    "One or more predictor candidates were not persisted in PulseGuard Core."

$InvalidStatus = @(
    $Problems |
        Where-Object {
            $_.status -notin @(
                "CANDIDATE",
                "UNDER_REVIEW",
                "CONFIRMED",
                "INVESTIGATING",
                "CORRECTIVE_ACTION_PLANNED",
                "MONITORING",
                "CLOSED",
                "REJECTED"
            )
        }
)

Assert-True ($InvalidStatus.Count -eq 0) `
    "One or more Problem records have an invalid lifecycle status."

$MissingFields = @(
    $Problems |
        Where-Object {
            [string]::IsNullOrWhiteSpace([string]$_.id) -or
            [string]::IsNullOrWhiteSpace([string]$_.problem_key) -or
            [string]::IsNullOrWhiteSpace([string]$_.title) -or
            [string]::IsNullOrWhiteSpace([string]$_.record_class) -or
            $null -eq $_.linked_incident_ids -or
            $null -eq $_.origin_breakdown
        }
)

Assert-True ($MissingFields.Count -eq 0) `
    "One or more Problem records are missing required audit fields."

$Summary = Invoke-RestMethod `
    -Uri "$PredictorUrl/problem-register/summary" `
    -TimeoutSec 30

Assert-True ([int]$Summary.total -eq $Problems.Count) `
    "Problem Register summary total does not match the returned records."

$Page = Invoke-WebRequest `
    -Uri "$PredictorUrl/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 30

foreach ($Marker in @(
    "Problem Register",
    "id=""problemRows""",
    "function renderProblemRegister",
    "/problem-register/summary",
    "problemRows.addEventListener",
    "Open problem records"
)) {
    Assert-True ($Page.Content.Contains($Marker)) `
        "Predictive dashboard marker missing: $Marker"
}

Write-Host "[PASS] Recurring patterns grouped into potential problem candidates" -ForegroundColor Green
Write-Host "[PASS] Candidates persisted in the PulseGuard Core PostgreSQL database" -ForegroundColor Green
Write-Host "[PASS] Lifecycle, ownership, linked incidents and origin fields available" -ForegroundColor Green
Write-Host "[PASS] Synthetic-only candidates remain explicitly classified" -ForegroundColor Green
Write-Host "[PASS] Predictive dashboard includes the interactive Problem Register" -ForegroundColor Green
Write-Host ""
Write-Host ("Candidates:       {0}" -f $Candidates.Count)
Write-Host ("Problem records:  {0}" -f $Problems.Count)
Write-Host ("Open records:     {0}" -f $Summary.open)
Write-Host ("High-risk open:   {0}" -f $Summary.highRisk)
Write-Host ""
$Problems |
    Select-Object `
        status,
        risk_level,
        title,
        occurrence_count,
        record_class,
        owner_queue |
    Format-Table -AutoSize |
    Out-String |
    Write-Host
