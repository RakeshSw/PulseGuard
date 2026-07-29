[CmdletBinding()]
param(
    [ValidateSet('auth','restart','all')]
    [string]$Scenario = 'all',
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [int]$TimeoutSeconds = 240
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $ProjectRoot

$ScenarioController = 'http://localhost:8090'
$Core = 'http://localhost:8095'
$Agent = 'http://localhost:8096'
$Automation = 'http://localhost:8097'

function Reset-Environment {
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 45 | Out-Null
    Start-Sleep -Seconds 12
}

function Wait-Incident {
    param([string]$IncidentType, [datetime]$StartedAt)
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Rows = @((Invoke-RestMethod -Uri "$Core/incidents?status=all&limit=500" -TimeoutSec 20).incidents)
        $Incident = $Rows |
            Where-Object {
                $_.incident_type -eq $IncidentType -and
                ([datetime]$_.opened_at) -ge $StartedAt.AddSeconds(-5)
            } |
            Sort-Object { [datetime]$_.opened_at } -Descending |
            Select-Object -First 1
        if ($Incident) { return $Incident }
        Start-Sleep -Seconds 3
    }
    throw "Timed out waiting for incident $IncidentType."
}

function Wait-Investigation {
    param([string]$IncidentId)
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $Deadline) {
        $Rows = @((Invoke-RestMethod -Uri "$Agent/api/investigations" -TimeoutSec 20).investigations)
        $Investigation = $Rows |
            Where-Object { $_.incident_id -eq $IncidentId -and $_.status -eq 'COMPLETED' } |
            Sort-Object { [datetime]$_.completed_at } -Descending |
            Select-Object -First 1
        if ($Investigation) { return $Investigation }
        Start-Sleep -Seconds 3
    }
    throw "Timed out waiting for investigation of incident $IncidentId."
}

function Wait-OperationsOutcome {
    param([string]$IncidentId, [string]$ExpectedOutcome)
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $Last = $null
    while ((Get-Date) -lt $Deadline) {
        $Last = Invoke-RestMethod -Uri "$Automation/api/incidents/$IncidentId/operations" -TimeoutSec 20
        if ([string]$Last.repairOutcome -like "$ExpectedOutcome*") { return $Last }
        Start-Sleep -Seconds 3
    }
    $Path = Join-Path $HOME ("Downloads\opsai-operations-timeout-{0}-{1}.json" -f $IncidentId,(Get-Date -Format 'yyyyMMdd_HHmmss'))
    $Last | ConvertTo-Json -Depth 60 | Out-File -LiteralPath $Path -Encoding utf8 -Width 5000
    throw "Timed out waiting for $ExpectedOutcome. Last operations state saved to $Path"
}

function Wait-Ticket {
    param([string]$IncidentId)
    $Deadline = (Get-Date).AddSeconds(90)
    while ((Get-Date) -lt $Deadline) {
        $Rows = @((Invoke-RestMethod -Uri "$Automation/tickets" -TimeoutSec 20).tickets)
        $Ticket = $Rows | Where-Object { $_.incidentId -eq $IncidentId } | Select-Object -First 1
        if ($Ticket) { return $Ticket }
        Start-Sleep -Seconds 3
    }
    throw "Timed out waiting for the approval ticket for $IncidentId."
}

function Run-AuthenticationRepair {
    Reset-Environment
    Write-Host 'Rotating the external partner credential without updating checkout...' -ForegroundColor Yellow
    $Started = Get-Date
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/external-auth-failure" -TimeoutSec 30 | Out-Null

    $Incident = Wait-Incident -IncidentType 'EXTERNAL_SERVICE_AUTHENTICATION_FAILURE' -StartedAt $Started
    $Investigation = Wait-Investigation -IncidentId $Incident.id

    if ($Investigation.action_name -ne 'refresh_external_service_credentials') {
        throw "Unexpected authentication action: $($Investigation.action_name)"
    }
    if ($Investigation.policy_decision -ne 'AUTO_ALLOWED') {
        throw "Authentication repair was not auto-allowed: $($Investigation.policy_decision)"
    }

    $Operations = Wait-OperationsOutcome -IncidentId $Incident.id -ExpectedOutcome 'AUTO_REPAIRED'
    $Remediation = $Operations.remediation
    if (-not $Remediation.secretRedacted) { throw 'Credential remediation did not confirm secret redaction.' }
    if (-not $Remediation.verificationPassed) { throw 'Credential probe verification did not pass.' }
    if ($Remediation.PSObject.Properties.Name -contains 'token') { throw 'Credential remediation exposed the bearer token.' }

    Write-Host '[PASS] External authentication failure automatically repaired.' -ForegroundColor Green
    Write-Host ("  Incident     : {0}" -f $Incident.id)
    Write-Host ("  AI action    : {0}" -f $Investigation.action_name)
    Write-Host ("  Governance   : {0}" -f $Investigation.policy_decision)
    Write-Host ("  Token gen.   : {0}" -f $Remediation.tokenGeneration)
    Write-Host ("  Fingerprint  : {0}" -f $Remediation.tokenFingerprint)
    Write-Host ("  Outcome      : {0}" -f $Operations.repairOutcome)
}

function Run-ApprovedRestart {
    Reset-Environment
    Write-Host 'Simulating a hung payment worker on payment-node-3...' -ForegroundColor Yellow
    $Started = Get-Date
    Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/payment-node-hung?node_id=payment-node-3" -TimeoutSec 30 | Out-Null

    $Incident = Wait-Incident -IncidentType 'PAYMENT_NODE_HUNG' -StartedAt $Started
    $Investigation = Wait-Investigation -IncidentId $Incident.id
    if ($Investigation.action_name -ne 'restart_payment_node') {
        throw "Unexpected hung-worker action: $($Investigation.action_name)"
    }
    if ($Investigation.policy_decision -ne 'APPROVAL_REQUIRED') {
        throw "Restart should require approval, but governance returned $($Investigation.policy_decision)."
    }
    if ($Investigation.action_executed) {
        throw 'Restart executed before operator approval.'
    }

    $Ticket = Wait-Ticket -IncidentId $Incident.id
    Write-Host ("Approval ticket created: {0} / {1}" -f $Ticket.ticketId,$Ticket.primaryQueue) -ForegroundColor Cyan

    $Approval = Invoke-RestMethod `
        -Method Post `
        -Uri "$Automation/tickets/$($Incident.id)/approve" `
        -ContentType 'application/json' `
        -Body (@{
            actor = 'demo-operator'
            note = 'Approved by the focused v0.6 restart validation.'
        } | ConvertTo-Json) `
        -TimeoutSec 45

    if (-not $Approval.remediation.actionSucceeded) {
        throw 'The bounded application restart did not pass its immediate verification.'
    }
    $Verification = $Approval.remediation.executionResult.verification
    if (-not $Verification.faultCleared -or -not $Verification.acceptingPayments -or -not $Verification.restartGenerationIncreased) {
        throw 'Restart verification is incomplete.'
    }

    $Operations = Wait-OperationsOutcome -IncidentId $Incident.id -ExpectedOutcome 'OPERATOR_REPAIRED'
    $Executor = [string]$Operations.remediation.executor
    if ($Executor -notmatch 'bounded-application-restart') {
        throw "Unexpected restart executor: $Executor"
    }

    Write-Host '[PASS] Hung worker repaired after explicit approval.' -ForegroundColor Green
    Write-Host ("  Incident     : {0}" -f $Incident.id)
    Write-Host ("  AI action    : {0}" -f $Investigation.action_name)
    Write-Host ("  Governance   : {0}" -f $Investigation.policy_decision)
    Write-Host ("  Approved by  : {0}" -f $Approval.remediation.approvedBy)
    Write-Host ("  Executor     : {0}" -f $Executor)
    Write-Host ("  Docker restart: False")
    Write-Host ("  Outcome      : {0}" -f $Operations.repairOutcome)
}

try {
    if ($Scenario -in @('auth','all')) { Run-AuthenticationRepair }
    if ($Scenario -in @('restart','all')) { Run-ApprovedRestart }
}
finally {
    try { Invoke-RestMethod -Method Post -Uri "$ScenarioController/scenarios/reset" -TimeoutSec 45 | Out-Null } catch { Write-Warning $_ }
}
