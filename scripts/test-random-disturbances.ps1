[CmdletBinding()]
param(
    [int]$Count = 3,
    [switch]$Full,
    [int]$PollSeconds = 5
)

$ErrorActionPreference = "Stop"
$BaseUrl = "http://localhost:8090"

if ($Full) {
    Write-Host "Starting the full random disturbance suite..." -ForegroundColor Cyan
    $Run = Invoke-RestMethod -Method Post -Uri "$BaseUrl/test-runs/full"
}
else {
    Write-Host "Starting $Count randomly selected disturbances..." -ForegroundColor Cyan
    $Run = Invoke-RestMethod -Method Post -Uri "$BaseUrl/test-runs/random?count=$Count"
}

Write-Host "Run ID: $($Run.id)" -ForegroundColor DarkGray
Write-Host "Seed:   $($Run.seed)" -ForegroundColor DarkGray
Write-Host "Order:  $($Run.selectedDisturbances -join ', ')" -ForegroundColor DarkGray

$TerminalStates = @(
    "COMPLETED",
    "COMPLETED_WITH_GAPS",
    "COMPLETED_WITH_ERRORS",
    "FAILED",
    "STOPPED",
    "INTERRUPTED"
)

while ($true) {
    Start-Sleep -Seconds $PollSeconds
    $Current = Invoke-RestMethod -Method Get -Uri "$BaseUrl/test-runs/$($Run.id)"
    $Summary = $Current.summary

    Write-Host (
        "[{0}] Completed {1}/{2}; injected {3}; detected {4}; investigated {5}; real AI {6}; classified {7}; resolved {8}" -f `
        $Current.status,
        $Summary.completed,
        $Summary.planned,
        $Summary.injected,
        $Summary.detected,
        $Summary.investigated,
        $Summary.realAi,
        $Summary.correctlyClassified,
        $Summary.resolved
    )

    if ($TerminalStates -contains $Current.status) {
        break
    }
}

$Rows = foreach ($Step in $Current.steps) {
    [PSCustomObject]@{
        Step           = $Step.index
        Disturbance    = $Step.displayName
        Injected       = $Step.injectionSucceeded
        Detected       = $Step.detected
        IncidentTypes  = ($Step.incidentTypes -join ", ")
        Investigated   = $Step.investigated
        RealAI         = $Step.realAi
        Classification = $Step.classificationStatus
        Recommendation = ($Step.recommendations -join ", ")
        Policy         = ($Step.policyDecisions -join ", ")
        Resolved       = $Step.resolved
        Outcome        = $Step.outcome
    }
}

$Rows | Format-Table -AutoSize

Write-Host ""
Write-Host "Final status: $($Current.status)" -ForegroundColor Cyan
$Current.summary | Format-List

if ($Current.status -in @("FAILED", "COMPLETED_WITH_ERRORS", "INTERRUPTED")) {
    throw "Random disturbance suite completed with execution errors. Review http://localhost:8090 on the Random test summary tab."
}

if ($Current.status -eq "COMPLETED_WITH_GAPS") {
    Write-Warning "The test executed successfully but exposed detection, investigation, classification, or recovery gaps. Review the summary tab."
}
