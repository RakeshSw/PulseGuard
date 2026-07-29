[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
)
$ErrorActionPreference = "Stop"
Set-Location $ProjectRoot

Write-Host "Validating correlation, AI-decision and action-audit capabilities..." -ForegroundColor Cyan
$core = Invoke-RestMethod http://localhost:8095/evaluation -TimeoutSec 20
$agent = Invoke-RestMethod http://localhost:8096/health -TimeoutSec 20
$controller = Invoke-RestMethod http://localhost:8090/health -TimeoutSec 20

$requiredRules = @(
    'payment_node_unavailable',
    'payment_node_network_instability',
    'payment_node_flapping',
    'payment_shared_dependency_outage',
    'payment_fleet_capacity_degradation'
)
foreach ($rule in $requiredRules) {
    if (-not $core.rules.PSObject.Properties.Name.Contains($rule)) {
        throw "Missing detector rule: $rule"
    }
}
if ($agent.analysisMode -ne 'REAL_AI') {
    throw "Expected REAL_AI, received $($agent.analysisMode)."
}
if ($controller.status -ne 'healthy') {
    throw "Scenario Controller is not healthy."
}

Write-Host "Detector correlation rules: present" -ForegroundColor Green
Write-Host "Agent mode: $($agent.analysisMode) / $($agent.provider)" -ForegroundColor Green
Write-Host "Scenario Controller: healthy" -ForegroundColor Green
Write-Host "Open http://localhost:8090, select Random Test Summary, and run the full suite." -ForegroundColor Cyan
