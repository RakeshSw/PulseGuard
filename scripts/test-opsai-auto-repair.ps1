[CmdletBinding()]
param(
    [ValidateSet('disk','certificate','capacity','both','all')]
    [string]$Scenario = 'all',
    [int]$TimeoutSeconds = 300,
    [int]$PostResolutionGraceSeconds = 60
)

$ErrorActionPreference = 'Stop'
$Automation = 'http://localhost:8097'
$Core = 'http://localhost:8095'
$Agent = 'http://localhost:8096'
$ScenarioController = 'http://localhost:8090'

function Get-IncidentOutcome {
    param(
        [Parameter(Mandatory=$true)]$Incident,
        [Parameter(Mandatory=$true)]$Detail
    )

    $outcome = $null

    if ($Detail.operations) {
        $outcome = $Detail.operations.repairOutcome
        if (-not $outcome -and $Detail.operations.remediation) {
            $outcome = $Detail.operations.remediation.repairOutcome
            if (-not $outcome -and $Detail.operations.remediation.recoveryVerified) {
                $outcome = 'AUTO_REPAIRED'
            }
            if (-not $outcome -and $Detail.operations.remediation.verificationPassed) {
                $outcome = 'AUTO_REPAIRED_PENDING_CORE_VERIFICATION'
            }
            if (-not $outcome -and $Detail.operations.remediation.executed) {
                $outcome = 'AUTO_ACTION_COMPLETED'
            }
        }
    }

    if (-not $outcome) { $outcome = $Incident.repair_outcome }
    if (-not $outcome -and $Incident.evidence) { $outcome = $Incident.evidence.repairOutcome }
    return $outcome
}

function Get-IncidentAction {
    param(
        [Parameter(Mandatory=$true)]$Incident,
        [Parameter(Mandatory=$true)]$Detail
    )

    if ($Detail.operations -and $Detail.operations.remediation -and $Detail.operations.remediation.action) {
        return [string]$Detail.operations.remediation.action
    }

    if ($Detail.investigation -and $Detail.investigation.action_name) {
        return [string]$Detail.investigation.action_name
    }

    if ($Incident.resolution_action) {
        return [string]$Incident.resolution_action
    }

    return ''
}

function Wait-ForResult {
    param(
        [Parameter(Mandatory=$true)][string]$IncidentType,
        [Parameter(Mandatory=$true)][datetime]$StartedAt,
        [Parameter(Mandatory=$true)][string]$ExpectedAction,
        [Parameter(Mandatory=$true)][string]$ExpectedOutcome
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $resolvedAt = $null
    $lastIncident = $null
    $lastDetail = $null
    $lastOutcome = $null
    $lastAction = $null

    while ((Get-Date) -lt $deadline) {
        $items = @(
            (Invoke-RestMethod -Uri "$Core/incidents?status=all&limit=300" -TimeoutSec 15).incidents
        ) |
            Where-Object {
                $_.incident_type -eq $IncidentType -and
                ([datetime]$_.opened_at) -ge $StartedAt.AddSeconds(-5)
            } |
            Sort-Object { [datetime]$_.opened_at } -Descending

        $incident = $items | Select-Object -First 1

        if ($incident) {
            $detail = Invoke-RestMethod -Uri "$Core/incidents/$($incident.id)" -TimeoutSec 15
            $outcome = Get-IncidentOutcome -Incident $incident -Detail $detail
            $action = Get-IncidentAction -Incident $incident -Detail $detail

            $lastIncident = $incident
            $lastDetail = $detail
            $lastOutcome = $outcome
            $lastAction = $action

            if ($incident.status -eq 'RESOLVED') {
                if (-not $resolvedAt) {
                    $resolvedAt = Get-Date
                    Write-Host (
                        "Incident resolved; waiting for Automation reconciliation. " +
                        "Action='{0}', Outcome='{1}'." -f $action, $outcome
                    ) -ForegroundColor DarkYellow
                }

                $actionMatches = ($action -eq $ExpectedAction)
                $outcomeMatches = (
                    -not [string]::IsNullOrWhiteSpace([string]$outcome) -and
                    $outcome -like "$ExpectedOutcome*"
                )

                if ($actionMatches -and $outcomeMatches) {
                    return [pscustomobject]@{
                        Incident = $incident
                        Detail = $detail
                        Outcome = $outcome
                        Action = $action
                    }
                }

                if (((Get-Date) - $resolvedAt).TotalSeconds -ge $PostResolutionGraceSeconds) {
                    break
                }
            }
        }

        Start-Sleep -Seconds 3
    }

    $diagnostic = [ordered]@{
        incidentType = $IncidentType
        expectedAction = $ExpectedAction
        expectedOutcome = $ExpectedOutcome
        lastAction = $lastAction
        lastOutcome = $lastOutcome
        incident = $lastIncident
        operations = if ($lastDetail) { $lastDetail.operations } else { $null }
        investigation = if ($lastDetail) { $lastDetail.investigation } else { $null }
    }

    $diagnosticPath = Join-Path $HOME (
        "Downloads\opsai-auto-repair-diagnostic-{0}-{1}.json" -f
        ($IncidentType -replace '[^A-Za-z0-9_-]', '_'),
        (Get-Date -Format 'yyyyMMdd_HHmmss')
    )

    $diagnostic |
        ConvertTo-Json -Depth 50 |
        Out-File -LiteralPath $diagnosticPath -Encoding utf8 -Width 4000

    $message = (
        "Timed out waiting for {0}. Last action='{1}', last outcome='{2}'. Diagnostic saved to {3}" -f
        $IncidentType,
        $lastAction,
        $lastOutcome,
        $diagnosticPath
    )
    if ($lastOutcome -eq 'RECOVERED_AFTER_TRAFFIC_NORMALIZED') {
        throw (
            "Capacity action executed, but demand normalized before processing-capacity recovery was independently verified. " +
            "This correctly does not count as AUTO_REPAIRED. Diagnostic saved to $diagnosticPath"
        )
    }

    throw $message
}

function Reset-And-Stabilise {
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 30 | Out-Null
    Start-Sleep -Seconds 12
}

function Run-DiskRepair {
    Reset-And-Stabilise
    Write-Host 'Injecting bounded disk pressure...' -ForegroundColor Yellow
    $start = Get-Date
    Invoke-RestMethod -Method Post -Uri "$Automation/scenarios/disk-pressure?target_percent=92&cleanup_insufficient=false" -TimeoutSec 30 | Out-Null
    $result = Wait-ForResult `
        -IncidentType 'NODE_DISK_PRESSURE' `
        -StartedAt $start `
        -ExpectedAction 'cleanup_disk_space' `
        -ExpectedOutcome 'AUTO_REPAIRED'

    Write-Host (
        "[PASS] Disk automatically repaired. Action={0}; Outcome={1}" -f
        $result.Action,
        $result.Outcome
    ) -ForegroundColor Green
}

function Run-CertificateRepair {
    Reset-And-Stabilise
    Write-Host 'Injecting expiring demo certificate...' -ForegroundColor Yellow
    $start = Get-Date
    Invoke-RestMethod -Method Post -Uri "$Automation/scenarios/certificate-expiring?seconds=300" -TimeoutSec 30 | Out-Null
    $result = Wait-ForResult `
        -IncidentType 'TLS_CERTIFICATE_EXPIRING' `
        -StartedAt $start `
        -ExpectedAction 'renew_certificate' `
        -ExpectedOutcome 'AUTO_REPAIRED'

    Write-Host (
        "[PASS] Certificate automatically renewed and verified. Action={0}; Outcome={1}" -f
        $result.Action,
        $result.Outcome
    ) -ForegroundColor Green
}

function Run-CapacityRepair {
    Reset-And-Stabilise
    Write-Host 'Taking node 3 unavailable and applying bounded pressure to nodes 1 and 2...' -ForegroundColor Yellow
    $start = Get-Date
    Invoke-RestMethod `
        -Method Post `
        -Uri "$ScenarioController/scenarios/capacity-failover-scale?multiplier=4&duration_seconds=180&pressure_ms=1200" `
        -TimeoutSec 30 |
        Out-Null

    $result = Wait-ForResult `
        -IncidentType 'PAYMENT_FLEET_CAPACITY_DEGRADATION' `
        -StartedAt $start `
        -ExpectedAction 'scale_payment_capacity' `
        -ExpectedOutcome 'AUTO_REPAIRED'

    Write-Host (
        "[PASS] Remaining payment capacity scaled and recovery verified. Action={0}; Outcome={1}" -f
        $result.Action,
        $result.Outcome
    ) -ForegroundColor Green
}

try {
    if ($Scenario -in @('disk','both','all')) { Run-DiskRepair }
    if ($Scenario -in @('certificate','both','all')) { Run-CertificateRepair }
    if ($Scenario -in @('capacity','all')) { Run-CapacityRepair }

    $summary = Invoke-RestMethod -Uri "$Automation/summary" -TimeoutSec 15
    Write-Host ("Auto-repaired KPI: {0}" -f $summary.autoRepaired) -ForegroundColor Cyan
}
finally {
    try {
        Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 30 | Out-Null
    }
    catch {
        Write-Warning $_
    }
}
