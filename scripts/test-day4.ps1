[CmdletBinding()]
param(
    [switch]$RequireRealAI,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$script:IncidentId = $null
$script:Investigation = $null
$script:ValidationReachedRecovery = $false

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)]
        [scriptblock]$Condition,
        [Parameter(Mandatory = $true)]
        [string]$Description,
        [int]$Seconds = $TimeoutSeconds
    )

    $Deadline = (Get-Date).AddSeconds($Seconds)
    do {
        try {
            if (& $Condition) {
                Write-Host "$Description confirmed." -ForegroundColor Green
                return
            }
        }
        catch {
            # The dependent service may still be updating. Retry until timeout.
        }
        Start-Sleep -Seconds 5
    } while ((Get-Date) -lt $Deadline)

    throw "Timed out waiting for: $Description"
}

try {
    Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null

    $Health = Invoke-RestMethod "http://localhost:8096/health"
    $Health | ConvertTo-Json -Depth 5

    if ($RequireRealAI -and -not $Health.realAiReady) {
        throw "Real AI is required but the agent is in deterministic fallback mode."
    }

    Invoke-RestMethod `
        -Method Post `
        -Uri "http://localhost:8090/scenarios/payment-latency?latency_ms=1200&jitter_ms=100" |
        Out-Null

    Wait-Until -Description "latency incident" -Condition {
        $Result = Invoke-RestMethod "http://localhost:8095/incidents?status=active&limit=20"
        $Incident = $Result.incidents |
            Where-Object {
                $_.incident_type -eq "PAYMENT_NODE_LATENCY" -and
                $_.node -eq "payment-node-3"
            } |
            Select-Object -First 1

        if ($Incident) {
            $script:IncidentId = $Incident.id
            return $true
        }
        return $false
    }

    Wait-Until -Description "completed investigation" -Condition {
        $Result = Invoke-RestMethod "http://localhost:8096/api/investigations"
        $Investigation = $Result.investigations |
            Where-Object {
                $_.incident_id -eq $script:IncidentId -and
                $_.status -eq "COMPLETED"
            } |
            Select-Object -First 1

        if ($Investigation) {
            $script:Investigation = $Investigation
            return $true
        }
        return $false
    }

    $script:Investigation |
        Select-Object `
            analysis_mode,
            provider,
            model,
            summary,
            root_cause,
            confidence,
            action_name,
            policy_decision,
            provider_endpoint_host,
            llm_duration_ms |
        Format-List

    if ($RequireRealAI -and $script:Investigation.analysis_mode -ne "REAL_AI") {
        throw "Expected REAL_AI investigation mode."
    }

    $ExpectedPolicyByAction = @{
        collect_diagnostics = "AUTO_ALLOWED"
        drain_payment_node  = "APPROVAL_REQUIRED"
    }

    $Action = [string]$script:Investigation.action_name
    $Policy = [string]$script:Investigation.policy_decision

    if (-not $ExpectedPolicyByAction.ContainsKey($Action)) {
        throw "Unexpected recommendation for an isolated latency incident: '$Action'. Expected collect_diagnostics or drain_payment_node."
    }

    $ExpectedPolicy = $ExpectedPolicyByAction[$Action]
    if ($Policy -ne $ExpectedPolicy) {
        throw "Action '$Action' should produce policy '$ExpectedPolicy', but received '$Policy'."
    }

    if ([double]$script:Investigation.confidence -lt 0 -or [double]$script:Investigation.confidence -gt 1) {
        throw "Investigation confidence must be between 0 and 1."
    }

    if ($RequireRealAI) {
        if ([string]::IsNullOrWhiteSpace([string]$script:Investigation.provider_endpoint_host)) {
            throw "AI provider endpoint host was not recorded for audit transparency."
        }
        if (-not $script:Investigation.request_payload) {
            throw "Sanitized AI request payload was not stored."
        }
        if (-not $script:Investigation.response_payload) {
            throw "Parsed AI response payload was not stored."
        }
    }

    Write-Host "Valid AI recommendation confirmed: $Action / $Policy" -ForegroundColor Green
}
finally {
    try {
        Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null
        Write-Host "Faults reset in test cleanup." -ForegroundColor Cyan
    }
    catch {
        Write-Warning "Could not reset scenarios during cleanup: $($_.Exception.Message)"
    }
}

if ($script:IncidentId) {
    Wait-Until -Description "incident resolution" -Condition {
        $Detail = Invoke-RestMethod "http://localhost:8095/incidents/$script:IncidentId"
        return $Detail.incident.status -eq "RESOLVED"
    }
}

Write-Host "Day 4 validation passed." -ForegroundColor Green
