[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$RunId = "",
    [int]$WaitTimeoutMinutes = 90,
    [int]$PollSeconds = 15,
    [int]$DefaultLookbackHours = 8,
    [int]$LogTailLines = 20000,
    [switch]$NoWait
)

$ErrorActionPreference = "Continue"

if (-not (Test-Path -LiteralPath $ProjectRoot)) {
    throw "Project folder not found: $ProjectRoot"
}

Set-Location -LiteralPath $ProjectRoot

docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop is not running or the Linux engine is unavailable."
}

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BundleName = "opsai-v051-full-suite-analysis-$Stamp"
$BundleRoot = Join-Path $HOME "Downloads\$BundleName"
$ZipPath = "$BundleRoot.zip"

$Folders = @(
    $BundleRoot,
    (Join-Path $BundleRoot "test-run"),
    (Join-Path $BundleRoot "incidents"),
    (Join-Path $BundleRoot "investigations"),
    (Join-Path $BundleRoot "automation"),
    (Join-Path $BundleRoot "support-triage"),
    (Join-Path $BundleRoot "activity"),
    (Join-Path $BundleRoot "api"),
    (Join-Path $BundleRoot "logs"),
    (Join-Path $BundleRoot "docker"),
    (Join-Path $BundleRoot "prometheus"),
    (Join-Path $BundleRoot "source-snapshot")
)

foreach ($Folder in $Folders) {
    New-Item -ItemType Directory -Path $Folder -Force | Out-Null
}

function Save-Json {
    param(
        [Parameter(Mandatory = $true)]$Value,
        [Parameter(Mandatory = $true)][string]$Path
    )

    try {
        $Value |
            ConvertTo-Json -Depth 100 |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 4000
    }
    catch {
        $_ | Out-String |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 4000
    }
}

function Get-JsonEndpoint {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][string]$Path,
        [int]$TimeoutSec = 30
    )

    try {
        $Response = Invoke-RestMethod -Method Get -Uri $Uri -TimeoutSec $TimeoutSec
        Save-Json -Value $Response -Path $Path
        return $Response
    }
    catch {
        @(
            "URI: $Uri"
            "ERROR:"
            ($_ | Out-String)
        ) |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 4000
        return $null
    }
}

function Save-Command {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][scriptblock]$Command
    )

    try {
        & $Command 2>&1 |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 3000
    }
    catch {
        $_ | Out-String |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 3000
    }
}

function Get-PropertyValue {
    param(
        $Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return $null
    }

    foreach ($Name in $Names) {
        $Property = $Object.PSObject.Properties[$Name]
        if ($null -ne $Property) {
            return $Property.Value
        }
    }

    return $null
}

function Get-TestRuns {
    try {
        return Invoke-RestMethod `
            -Method Get `
            -Uri "http://localhost:8090/test-runs?limit=100" `
            -TimeoutSec 20
    }
    catch {
        return $null
    }
}

function Select-Run {
    param(
        $RunList,
        [string]$RequestedRunId
    )

    if ($null -eq $RunList -or $null -eq $RunList.runs) {
        return $null
    }

    $Runs = @($RunList.runs)

    if (-not [string]::IsNullOrWhiteSpace($RequestedRunId)) {
        return $Runs |
            Where-Object { [string]$_.id -eq $RequestedRunId } |
            Select-Object -First 1
    }

    $Active = $Runs |
        Where-Object {
            ([string]$_.mode -eq "full") -and
            ([string]$_.status -match "RUNNING|STARTED|IN_PROGRESS|PENDING")
        } |
        Sort-Object {
            try { [datetimeoffset](Get-PropertyValue $_ @("startedAt", "createdAt")) }
            catch { [datetimeoffset]::MinValue }
        } -Descending |
        Select-Object -First 1

    if ($null -ne $Active) {
        return $Active
    }

    $LatestFull = $Runs |
        Where-Object { [string]$_.mode -eq "full" } |
        Sort-Object {
            try { [datetimeoffset](Get-PropertyValue $_ @("completedAt", "updatedAt", "startedAt", "createdAt")) }
            catch { [datetimeoffset]::MinValue }
        } -Descending |
        Select-Object -First 1

    if ($null -ne $LatestFull) {
        return $LatestFull
    }

    return $Runs |
        Sort-Object {
            try { [datetimeoffset](Get-PropertyValue $_ @("completedAt", "updatedAt", "startedAt", "createdAt")) }
            catch { [datetimeoffset]::MinValue }
        } -Descending |
        Select-Object -First 1
}

function Save-PrometheusRange {
    param(
        [Parameter(Mandatory = $true)][string]$FileName,
        [Parameter(Mandatory = $true)][string]$Query,
        [Parameter(Mandatory = $true)][long]$StartUnix,
        [Parameter(Mandatory = $true)][long]$EndUnix,
        [int]$StepSeconds = 15
    )

    $Encoded = [uri]::EscapeDataString($Query)
    $Uri = "http://localhost:9090/api/v1/query_range?query=$Encoded&start=$StartUnix&end=$EndUnix&step=$StepSeconds"
    $Path = Join-Path (Join-Path $BundleRoot "prometheus") $FileName

    try {
        $Response = Invoke-RestMethod -Method Get -Uri $Uri -TimeoutSec 120
        Save-Json -Value ([PSCustomObject]@{
            query = $Query
            startUnix = $StartUnix
            endUnix = $EndUnix
            stepSeconds = $StepSeconds
            response = $Response
        }) -Path $Path
    }
    catch {
        @(
            "QUERY: $Query"
            "URI: $Uri"
            "ERROR:"
            ($_ | Out-String)
        ) |
            Out-File -LiteralPath $Path -Encoding utf8 -Width 4000
    }
}

function Redact-TextFiles {
    param([string]$Root)

    $TextExtensions = @(
        ".txt", ".log", ".json", ".csv", ".yaml", ".yml",
        ".py", ".ps1", ".md", ".html", ".js"
    )

    Get-ChildItem -LiteralPath $Root -Recurse -File |
        Where-Object { $TextExtensions -contains $_.Extension.ToLowerInvariant() } |
        ForEach-Object {
            try {
                $Content = Get-Content -LiteralPath $_.FullName -Raw -ErrorAction Stop

                $Content = $Content -replace `
                    '(?i)(AZURE_OPENAI_API_KEY\s*[:=]\s*)[^\s"'';,]+', `
                    '${1}<REDACTED>'

                $Content = $Content -replace `
                    '(?i)(Authorization\s*[:=]\s*["'']?Bearer\s+)[A-Za-z0-9._~+/\-=]+', `
                    '${1}<REDACTED>'

                $Content = $Content -replace `
                    '(?i)(api-key["'']?\s*[:=]\s*["'']?)[A-Za-z0-9._~+/\-=]+', `
                    '${1}<REDACTED>'

                $Content = $Content -replace `
                    '(?i)(password["'']?\s*[:=]\s*["'']?)[^"''\s,;]+', `
                    '${1}<REDACTED>'

                $Content = $Content -replace `
                    '-----BEGIN (RSA |EC |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?-----END (RSA |EC |ENCRYPTED )?PRIVATE KEY-----', `
                    '<PRIVATE KEY REDACTED>'

                [System.IO.File]::WriteAllText(
                    $_.FullName,
                    $Content,
                    [System.Text.UTF8Encoding]::new($false)
                )
            }
            catch {
                # Continue when a file is binary or temporarily locked.
            }
        }
}

Write-Host ""
Write-Host "PulseGuard v0.5.1 full-suite evidence collector" -ForegroundColor Cyan
Write-Host "=================================================="
Write-Host "Project: $ProjectRoot"
Write-Host "Output:  $ZipPath"
Write-Host ""
Write-Host "This collector does not inject faults, reset scenarios, clear logs, or copy .env." -ForegroundColor DarkGray

# ---------------------------------------------------------------------------
# 1. Identify the active or most recent full-suite run
# ---------------------------------------------------------------------------
$RunList = Get-TestRuns
$SelectedSummary = Select-Run -RunList $RunList -RequestedRunId $RunId

if ($null -eq $SelectedSummary) {
    throw "No test run was returned by Scenario Controller."
}

$SelectedRunId = [string]$SelectedSummary.id
$InitialStatus = [string]$SelectedSummary.status

Write-Host ""
Write-Host "Selected run: $SelectedRunId" -ForegroundColor Green
Write-Host "Mode: $($SelectedSummary.mode) | Status: $InitialStatus | Seed: $($SelectedSummary.seed)"

# ---------------------------------------------------------------------------
# 2. Optionally wait for the selected run to finish
# ---------------------------------------------------------------------------
if (-not $NoWait -and $InitialStatus -match "RUNNING|STARTED|IN_PROGRESS|PENDING") {
    Write-Host ""
    Write-Host "The full suite is still running. Waiting for completion..." -ForegroundColor Yellow
    Write-Host "Timeout: $WaitTimeoutMinutes minutes | Poll interval: $PollSeconds seconds"

    $Deadline = (Get-Date).AddMinutes($WaitTimeoutMinutes)

    while ((Get-Date) -lt $Deadline) {
        Start-Sleep -Seconds $PollSeconds

        try {
            $Current = Invoke-RestMethod `
                -Method Get `
                -Uri "http://localhost:8090/test-runs/$SelectedRunId" `
                -TimeoutSec 20

            $CurrentStatus = [string]$Current.status
            $Completed = Get-PropertyValue -Object $Current -Names @("completed", "summary.completed")
            $Planned = Get-PropertyValue -Object $Current -Names @("planned", "summary.planned")

            Write-Host ("[{0}] Status={1} Completed={2}/{3}" -f `
                (Get-Date -Format "HH:mm:ss"), `
                $CurrentStatus, `
                $Completed, `
                $Planned)

            if ($CurrentStatus -notmatch "RUNNING|STARTED|IN_PROGRESS|PENDING") {
                Write-Host "Run completed with status: $CurrentStatus" -ForegroundColor Green
                break
            }
        }
        catch {
            Write-Host "Waiting for Scenario Controller response..." -ForegroundColor DarkYellow
        }
    }
}

# ---------------------------------------------------------------------------
# 3. Capture final test-run data
# ---------------------------------------------------------------------------
$RunList = Get-JsonEndpoint `
    -Uri "http://localhost:8090/test-runs?limit=100" `
    -Path (Join-Path (Join-Path $BundleRoot "test-run") "test-runs-list.json")

Get-JsonEndpoint `
    -Uri "http://localhost:8090/test-runs/catalog" `
    -Path (Join-Path (Join-Path $BundleRoot "test-run") "test-run-catalog.json") |
    Out-Null

$SelectedRun = Get-JsonEndpoint `
    -Uri "http://localhost:8090/test-runs/$SelectedRunId" `
    -Path (Join-Path (Join-Path $BundleRoot "test-run") "selected-run.json")

if ($null -eq $SelectedRun) {
    throw "Could not load test run '$SelectedRunId'."
}

# Persisted raw test-run history, when available.
Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "test-run") "persisted-test-history.txt") `
    -Command {
        docker exec opsai-scenario-controller sh -c '
            for f in /data/test-runs.json /data/test_runs.json /data/runs.json /data/*.json; do
              if [ -f "$f" ]; then
                echo "===== $f ====="
                cat "$f"
                echo
              fi
            done
        '
    }

# ---------------------------------------------------------------------------
# 4. Create a readable test-step summary
# ---------------------------------------------------------------------------
$StepRows = @()
$IncidentIds = New-Object System.Collections.Generic.HashSet[string]

foreach ($Step in @($SelectedRun.steps)) {
    foreach ($IncidentId in @($Step.incidentIds)) {
        if (-not [string]::IsNullOrWhiteSpace([string]$IncidentId)) {
            [void]$IncidentIds.Add([string]$IncidentId)
        }
    }

    $AiDecision = Get-PropertyValue -Object $Step -Names @(
        "opsaiCommanderDecision",
        "aiDecision",
        "recommendation",
        "recommendedAction",
        "actionName"
    )

    if ($null -eq $AiDecision) {
        $AiDecision = @($Step.recommendations) -join ";"
    }

    $GovernanceDecision = Get-PropertyValue -Object $Step -Names @(
        "governanceDecision",
        "policyDecision",
        "policy"
    )

    if ($null -eq $GovernanceDecision) {
        $GovernanceDecision = @($Step.policyDecisions) -join ";"
    }

    $StepRows += [PSCustomObject]@{
        Index = $Step.index
        Disturbance = $Step.disturbance
        DisplayName = $Step.displayName
        StepStatus = $Step.status
        FinalResult = Get-PropertyValue $Step @("finalResult", "outcome")
        InjectionSucceeded = $Step.injectionSucceeded
        Detected = $Step.detected
        IncidentCount = $Step.incidentCount
        IncidentTypes = (@($Step.incidentTypes) -join ";")
        DesiredIncidentTypes = (@($Step.desiredIncidentTypes) -join ";")
        ClassificationStatus = $Step.classificationStatus
        Investigated = $Step.investigated
        RealAI = $Step.realAi
        AnalysisModes = (@($Step.analysisModes) -join ";")
        PulseGuardCommanderDecision = $AiDecision
        Confidence = Get-PropertyValue $Step @("averageConfidence", "confidence")
        GovernanceDecision = $GovernanceDecision
        GovernanceReason = Get-PropertyValue $Step @("governanceReason", "policyReason")
        ActionTaken = Get-PropertyValue $Step @("actionTaken", "actionExecuted", "executed")
        ExecutedAction = Get-PropertyValue $Step @("executedAction", "actionName")
        ExecutionStatus = Get-PropertyValue $Step @("executionStatus", "actionExecutionStatus")
        ExecutionResult = Get-PropertyValue $Step @("executionResult", "actionExecutionResult")
        RecoveryVerified = Get-PropertyValue $Step @("recoveryVerified", "verified")
        RepairOutcome = Get-PropertyValue $Step @("repairOutcome", "remediationOutcome", "resolutionOutcome")
        AutoRepaired = Get-PropertyValue $Step @("autoRepaired", "isAutoRepaired")
        ExpectedQueue = Get-PropertyValue $Step @("expectedQueue", "desiredQueue")
        AssignedQueue = Get-PropertyValue $Step @("assignedQueue", "supportQueue", "triageQueue")
        QueueConfidence = Get-PropertyValue $Step @("queueConfidence", "triageConfidence", "assignmentConfidence")
        AssignmentStatus = Get-PropertyValue $Step @("assignmentStatus", "supportStatus")
        DetectionSeconds = $Step.detectionSeconds
        InvestigationSeconds = $Step.investigationSeconds
        RecoverySeconds = $Step.recoverySeconds
        Notes = (@($Step.notes) -join " | ")
        Error = $Step.error
    }
}

$StepRows |
    Export-Csv `
        -LiteralPath (Join-Path (Join-Path $BundleRoot "test-run") "step-summary.csv") `
        -NoTypeInformation `
        -Encoding utf8

# ---------------------------------------------------------------------------
# 5. Capture v0.5 service/API state
# ---------------------------------------------------------------------------
$ApiRequests = [ordered]@{
    "scenario-health.json" = "http://localhost:8090/health"
    "scenario-state.json" = "http://localhost:8090/state"
    "scenario-summary.json" = "http://localhost:8090/test-runs/$SelectedRunId"
    "wikimedia-profile.json" = "http://localhost:8093/profile"
    "opsai-core-health.json" = "http://localhost:8095/health"
    "opsai-core-evaluation.json" = "http://localhost:8095/evaluation"
    "incidents-all.json" = "http://localhost:8095/incidents?status=all&limit=500"
    "incidents-active.json" = "http://localhost:8095/incidents?status=active&limit=500"
    "opsai-agent-health.json" = "http://localhost:8096/health"
    "investigations-all.json" = "http://localhost:8096/api/investigations"
    "automation-health.json" = "http://localhost:8097/health"
    "automation-summary.json" = "http://localhost:8097/summary"
    "automation-state.json" = "http://localhost:8097/state"
    "support-tickets.json" = "http://localhost:8097/tickets"
    "activity-events-automation.json" = "http://localhost:8097/activity?limit=1500"
    "prometheus-targets.json" = "http://localhost:9090/api/v1/targets"
}

foreach ($Entry in $ApiRequests.GetEnumerator()) {
    $TargetFolder = "api"

    if ($Entry.Key -like "automation-*") {
        $TargetFolder = "automation"
    }
    elseif ($Entry.Key -like "support-*") {
        $TargetFolder = "support-triage"
    }
    elseif ($Entry.Key -like "activity-*") {
        $TargetFolder = "activity"
    }

    Get-JsonEndpoint `
        -Uri $Entry.Value `
        -Path (Join-Path (Join-Path $BundleRoot $TargetFolder) $Entry.Key) |
        Out-Null
}

# Save the Automation data directory structure without copying keys/certificates.
Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "automation") "automation-data-file-list.txt") `
    -Command {
        docker exec opsai-automation sh -c '
            echo "Automation /data file inventory"
            echo "Private keys and certificate contents are not collected."
            find /data -maxdepth 4 -type f \
              ! -name "*.key" \
              ! -name "*.pem" \
              ! -name "*.pfx" \
              ! -name "*.p12" \
              ! -name "*.crt" \
              ! -name "*.cer" \
              -printf "%p | %s bytes | %TY-%Tm-%TdT%TH:%TM:%TS\n" 2>/dev/null \
              | sort
        '
    }

# ---------------------------------------------------------------------------
# 6. Linked incident and investigation details
# ---------------------------------------------------------------------------
foreach ($IncidentId in $IncidentIds) {
    $SafeId = $IncidentId -replace '[^a-zA-Z0-9_-]', '_'

    Get-JsonEndpoint `
        -Uri "http://localhost:8095/incidents/$IncidentId" `
        -Path (Join-Path (Join-Path $BundleRoot "incidents") "incident-$SafeId.json") |
        Out-Null

    $InvestigationRoutes = @(
        "http://localhost:8096/api/investigations/$IncidentId",
        "http://localhost:8096/api/investigations?incident_id=$IncidentId",
        "http://localhost:8096/api/incidents/$IncidentId/investigation"
    )

    $RouteIndex = 1
    foreach ($Uri in $InvestigationRoutes) {
        Get-JsonEndpoint `
            -Uri $Uri `
            -Path (Join-Path (Join-Path $BundleRoot "investigations") "incident-$SafeId-route-$RouteIndex.json") |
            Out-Null
        $RouteIndex++
    }

    Get-JsonEndpoint `
        -Uri "http://localhost:8097/api/incidents/$IncidentId/operations" `
        -Path (Join-Path (Join-Path $BundleRoot "support-triage") "incident-$SafeId-operations.json") |
        Out-Null
}

# ---------------------------------------------------------------------------
# 7. Determine the exact test interval
# ---------------------------------------------------------------------------
$RunStart = $null
$RunEnd = $null

foreach ($Candidate in @($SelectedRun.startedAt, $SelectedRun.createdAt)) {
    if ($null -eq $RunStart -and -not [string]::IsNullOrWhiteSpace([string]$Candidate)) {
        try { $RunStart = [datetimeoffset]$Candidate } catch {}
    }
}

foreach ($Candidate in @($SelectedRun.completedAt, $SelectedRun.updatedAt)) {
    if ($null -eq $RunEnd -and -not [string]::IsNullOrWhiteSpace([string]$Candidate)) {
        try { $RunEnd = [datetimeoffset]$Candidate } catch {}
    }
}

if ($null -eq $RunStart) {
    $RunStart = [datetimeoffset](Get-Date).AddHours(-$DefaultLookbackHours)
}

if ($null -eq $RunEnd) {
    $RunEnd = [datetimeoffset](Get-Date)
}

$CaptureStart = $RunStart.AddMinutes(-10)
$CaptureEnd = $RunEnd.AddMinutes(20)

$SinceMinutes = [math]::Ceiling(
    ((Get-Date).ToUniversalTime() - $CaptureStart.UtcDateTime).TotalMinutes
)
$SinceMinutes = [math]::Max(45, [math]::Min(4320, $SinceMinutes))
$SinceArg = "${SinceMinutes}m"

$StartUnix = $CaptureStart.ToUnixTimeSeconds()
$EndUnix = $CaptureEnd.ToUnixTimeSeconds()

@"
Selected run ID: $SelectedRunId
Mode: $($SelectedRun.mode)
Status: $($SelectedRun.status)
Seed: $($SelectedRun.seed)
Started: $($RunStart.ToString("o"))
Completed/last update: $($RunEnd.ToString("o"))
Log lookback: $SinceMinutes minutes
Prometheus capture: $($CaptureStart.ToString("o")) through $($CaptureEnd.ToString("o"))
Collector waited for completion: $(-not $NoWait)
"@ |
    Out-File `
        -LiteralPath (Join-Path (Join-Path $BundleRoot "test-run") "selected-run-metadata.txt") `
        -Encoding utf8

# ---------------------------------------------------------------------------
# 8. Docker status, health and logs
# ---------------------------------------------------------------------------
Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "docker") "docker-compose-ps.txt") `
    -Command { docker compose ps --all }

Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "docker") "docker-compose-images.txt") `
    -Command { docker compose images }

Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "docker") "docker-compose-config.txt") `
    -Command { docker compose config }

Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "docker") "container-health.json") `
    -Command {
        $ContainerIds = @(docker compose ps -aq)
        if ($ContainerIds.Count -gt 0) {
            docker inspect $ContainerIds
        }
    }

Save-Command `
    -Path (Join-Path (Join-Path $BundleRoot "logs") "00-all-services.log") `
    -Command {
        docker compose logs `
            --no-color `
            --timestamps `
            --since $SinceArg `
            --tail $LogTailLines
    }

$Services = @(
    "scenario-controller",
    "opsai-core",
    "opsai-agent",
    "opsai-automation",
    "payment-router",
    "payment-node-1",
    "payment-node-2",
    "payment-node-3",
    "checkout-service",
    "wikimedia-adapter",
    "corruption-adapter",
    "toxiproxy",
    "load-generator",
    "prometheus",
    "postgres",
    "grafana"
)

$Number = 1
foreach ($Service in $Services) {
    $Prefix = "{0:D2}" -f $Number

    Save-Command `
        -Path (Join-Path (Join-Path $BundleRoot "logs") "$Prefix-$Service.log") `
        -Command {
            docker compose logs `
                --no-color `
                --timestamps `
                --since $SinceArg `
                --tail $LogTailLines `
                $Service
        }

    $Number++
}

# ---------------------------------------------------------------------------
# 9. Prometheus evidence, including auto-repair telemetry
# ---------------------------------------------------------------------------
$Queries = [ordered]@{
    "node-p95-latency.json" = 'histogram_quantile(0.95, sum by (le,node) (rate(opsai_router_node_duration_seconds_bucket[1m])))'
    "retry-rate-by-node.json" = 'sum(rate(opsai_router_retries_total[1m])) by (failed_node)'
    "router-failures-by-kind.json" = 'sum(rate(opsai_router_failures_total[1m])) by (node,failure_kind)'
    "checkout-p95.json" = 'histogram_quantile(0.95, sum by (le) (rate(opsai_checkout_duration_seconds_bucket[1m])))'
    "checkout-failure-percentage.json" = '100 * sum(rate(opsai_checkout_requests_total{status="failed"}[1m])) / clamp_min(sum(rate(opsai_checkout_requests_total[1m])), 0.001)'
    "checkout-throughput.json" = 'sum(rate(opsai_checkout_requests_total[1m]))'
    "router-node-active.json" = 'opsai_router_node_active'
    "payment-capacity-units.json" = 'opsai_payment_capacity_units'
    "payment-capacity-pressure-ms.json" = 'opsai_payment_capacity_pressure_milliseconds'
    "disk-usage-percent.json" = 'opsai_demo_disk_usage_percent'
    "disk-free-bytes.json" = '167772160 - opsai_demo_disk_used_bytes'
    "temp-file-bytes.json" = 'opsai_demo_disk_used_bytes'
    "log-archive-bytes.json" = 'opsai_demo_disk_used_bytes'
    "certificate-expiry-seconds.json" = 'opsai_demo_certificate_expiry_seconds'
    "automation-actions-total.json" = 'opsai_automatic_remediations_total'
    "automation-action-failures.json" = 'opsai_automatic_remediations_total{result="failed"}'
    "auto-repairs-total.json" = 'opsai_auto_repaired_incidents'
    "support-assignments-total.json" = 'opsai_support_assignments_total'
}

foreach ($Entry in $Queries.GetEnumerator()) {
    Save-PrometheusRange `
        -FileName $Entry.Key `
        -Query $Entry.Value `
        -StartUnix $StartUnix `
        -EndUnix $EndUnix `
        -StepSeconds 15
}

# ---------------------------------------------------------------------------
# 10. Relevant source snapshot, excluding .env and private certificates
# ---------------------------------------------------------------------------
$FilesToCopy = @(
    "compose.yaml",
    ".env.example",
    "observability\prometheus\prometheus.yml",
    "scripts\test-opsai-v05.ps1",
    "scripts\test-opsai-auto-repair.ps1",
    "scripts\collect-opsai-v051-analysis.ps1",
    "scripts\test-correlation-ai-actions.ps1",
    "services\scenario-controller\app\main.py",
    "services\opsai-core\app\main.py",
    "services\opsai-agent\app\main.py",
    "services\opsai-agent\knowledge\catalog.json",
    "services\opsai-automation\Dockerfile",
    "services\opsai-automation\requirements.txt",
    "services\opsai-automation\app\main.py",
    "services\payment-router\app\main.py",
    "services\payment-service\app\main.py",
    "services\wikimedia-adapter\app\main.py"
)

foreach ($RelativePath in $FilesToCopy) {
    $Source = Join-Path $ProjectRoot $RelativePath

    if (Test-Path -LiteralPath $Source) {
        $Destination = Join-Path (Join-Path $BundleRoot "source-snapshot") $RelativePath
        $DestinationFolder = Split-Path -Parent $Destination
        New-Item -ItemType Directory -Path $DestinationFolder -Force | Out-Null
        Copy-Item -LiteralPath $Source -Destination $Destination -Force
    }
}

# ---------------------------------------------------------------------------
# 11. Redact secrets, create README and ZIP
# ---------------------------------------------------------------------------
Redact-TextFiles -Root $BundleRoot

@"
PulseGuard v0.5.1 full-suite analysis bundle
Captured: $(Get-Date -Format o)
Project: $ProjectRoot
Selected run: $SelectedRunId

Start with:
1. test-run/selected-run.json
2. test-run/step-summary.csv
3. api/incidents-all.json
4. api/investigations-all.json
5. automation/*
6. support-triage/*
7. activity/*
8. logs/01-scenario-controller.log
9. logs/02-opsai-core.log
10. logs/03-opsai-agent.log
11. logs/04-opsai-automation.log
12. prometheus/*

The bundle is designed to analyse:
- Exact disturbance injection and classification
- PulseGuard recommendations and confidence
- Governance decisions
- Actions actually executed
- Disk-cleanup and certificate-renewal results
- Recovery verification and strict AUTO_REPAIRED outcomes
- Support queue assignment and handoff details
- Live activity events
- Docker health and service logs
- Prometheus evidence for the exact run period

Safety:
- .env was not copied.
- Certificate private keys and certificate files were not copied.
- Common API-key, bearer-token, password and private-key patterns were redacted.
- No scenario was injected, reset or cleared.
- Logs and test history were not deleted.
"@ |
    Out-File `
        -LiteralPath (Join-Path $BundleRoot "README.txt") `
        -Encoding utf8

Compress-Archive `
    -Path (Join-Path $BundleRoot "*") `
    -DestinationPath $ZipPath `
    -Force

$Hash = Get-FileHash -Algorithm SHA256 -LiteralPath $ZipPath

Write-Host ""
Write-Host "Analysis bundle created:" -ForegroundColor Green
Write-Host $ZipPath -ForegroundColor White
Write-Host "SHA-256: $($Hash.Hash)" -ForegroundColor DarkGray
Write-Host ""
Write-Host "Upload this ZIP in the chat." -ForegroundColor Cyan
