[CmdletBinding()]
param(
    [string]$BaseUrl = "http://localhost:8095"
)

$ErrorActionPreference = "Stop"

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
Write-Host "PulseGuard inline investigation-panel validation" -ForegroundColor Cyan
Write-Host "==========================================="

$Health = Invoke-RestMethod -Uri "$BaseUrl/health" -TimeoutSec 20
Assert-True ($Health.status -eq "healthy") "PulseGuard Core is not healthy."

$Page = Invoke-WebRequest `
    -Uri "$BaseUrl/?_=$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())" `
    -UseBasicParsing `
    -Headers @{ "Cache-Control" = "no-cache" } `
    -TimeoutSec 20

foreach ($Marker in @(
    "ensureInlineInvestigation",
    "toggleInvestigation",
    "investigation-row",
    "investigation-shell",
    "Collapse investigation",
    "Opened directly below this incident row.",
    "aria-controls="
)) {
    Assert-True ($Page.Content.Contains($Marker)) `
        "Incident Console marker missing: $Marker"
}

foreach ($Forbidden in @(
    '<div id="detailHost"></div>',
    "document.getElementById('detailHost')"
)) {
    Assert-True (-not $Page.Content.Contains($Forbidden)) `
        "Legacy bottom investigation panel remains: $Forbidden"
}

Assert-True ($Page.Content.Contains("<th>Assigned To</th>")) `
    "Assigned To column was not preserved."

Write-Host "[PASS] Incident Console is healthy" -ForegroundColor Green
Write-Host "[PASS] Investigation opens in an inline row panel" -ForegroundColor Green
Write-Host "[PASS] Panel can be collapsed from the selected incident row" -ForegroundColor Green
Write-Host "[PASS] Legacy bottom-of-page investigation host removed" -ForegroundColor Green
Write-Host "[PASS] Assigned To column preserved" -ForegroundColor Green
