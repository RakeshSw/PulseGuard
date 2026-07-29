[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$script:LoadGeneratorWasRunning = $false
$script:LoadGeneratorPausedByTest = $false

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) { throw $Message }
}

function Get-State {
    Invoke-RestMethod -Method Get -Uri "http://localhost:8090/state" -TimeoutSec 10
}

function Reset-All {
    Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" -TimeoutSec 15 | Out-Null
    Start-Sleep -Seconds 2
}

function Pause-BackgroundLoad {
    $RunningServices = @(docker compose ps --status running --services 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to inspect Docker Compose services."
    }

    $script:LoadGeneratorWasRunning = $RunningServices -contains "load-generator"
    if ($script:LoadGeneratorWasRunning) {
        Write-Host "   pausing Locust background load for deterministic failover checks..." -ForegroundColor DarkGray
        docker compose stop load-generator | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to pause the load-generator container."
        }
        $script:LoadGeneratorPausedByTest = $true
        Start-Sleep -Seconds 3
    }
}

function Resume-BackgroundLoad {
    if ($script:LoadGeneratorPausedByTest -and $script:LoadGeneratorWasRunning) {
        Write-Host "   restoring Locust background load..." -ForegroundColor DarkGray
        docker compose up -d load-generator | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to restart the load-generator container."
        }
        $script:LoadGeneratorPausedByTest = $false
    }
}

function Invoke-CheckoutWithRetry {
    param(
        [Parameter(Mandatory = $true)]
        [string]$CustomerId,
        [int]$MaxAttempts = 3
    )

    $Payload = @{
        customerId = $CustomerId
        currency = "USD"
        items = @(@{ productId = "PROD-101"; quantity = 1; unitPrice = 19.99 })
    } | ConvertTo-Json -Depth 6

    $LastFailure = $null
    for ($Attempt = 1; $Attempt -le $MaxAttempts; $Attempt++) {
        try {
            return Invoke-RestMethod `
                -Method Post `
                -Uri "http://localhost:8080/checkout" `
                -ContentType "application/json" `
                -Body $Payload `
                -TimeoutSec 15
        }
        catch {
            $LastFailure = $_
            if ($Attempt -lt $MaxAttempts) {
                Start-Sleep -Milliseconds 750
            }
        }
    }

    throw "Checkout '$CustomerId' failed after $MaxAttempts attempts. $($LastFailure.Exception.Message)"
}

Write-Host "PulseGuard controlled turbulence validation v2" -ForegroundColor Cyan

try {
    $Health = Invoke-RestMethod -Method Get -Uri "http://localhost:8090/health" -TimeoutSec 10
    Assert-True ($Health.status -eq "healthy") "Scenario Controller is not healthy."

    Reset-All

    Write-Host "1. Validating live-demand amplification..." -ForegroundColor Cyan
    Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/traffic-spike?multiplier=2&duration_seconds=20" -TimeoutSec 10 | Out-Null
    Start-Sleep -Seconds 3
    $State = Get-State
    Assert-True ($State.scenario -eq "traffic_spike") "Traffic-spike scenario was not activated."
    Assert-True ([bool]$State.trafficOverride.active) "Traffic override is not active."
    Assert-True ([double]$State.trafficOverride.multiplier -eq 2.0) "Unexpected traffic multiplier."
    $Profile = Invoke-RestMethod -Method Get -Uri "http://localhost:8094/profile" -TimeoutSec 10
    Assert-True ($Profile.profile -eq "forced-surge") "Corruption adapter did not return the forced-surge profile."
    Assert-True ([int]$Profile.targetUsers -ge [int]$Profile.baseTargetUsers) "Amplified target is below its base target."
    Write-Host "   traffic spike confirmed: $($Profile.baseTargetUsers) -> $($Profile.targetUsers) users" -ForegroundColor Green

    Reset-All

    # The Wikimedia-derived baseline can itself be high. Pause background Locust
    # only while testing deterministic router failover and shared dependency behavior.
    # This does not affect the manual turbulence scenarios.
    Pause-BackgroundLoad
    Reset-All

    Write-Host "2. Validating node-3 offline simulation and router failover..." -ForegroundColor Cyan
    Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/node-offline?node_id=payment-node-3" -TimeoutSec 10 | Out-Null
    Start-Sleep -Seconds 2
    $State = Get-State
    $Node3 = $State.paymentNodes | Where-Object { $_.nodeId -eq "payment-node-3" }
    Assert-True ($Node3.faultMode -eq "unavailable") "payment-node-3 is not in unavailable mode."

    $FailoverObserved = $false
    for ($i = 1; $i -le 8; $i++) {
        $Checkout = Invoke-CheckoutWithRetry -CustomerId "TURB-$i"
        Assert-True ($Checkout.status -eq "completed") "Checkout did not fail over successfully."
        Assert-True ($Checkout.payment.processedBy -ne "payment-node-3") "An offline node processed a payment."
        if ([int]$Checkout.payment.routeAttempt -gt 1) {
            $FailoverObserved = $true
        }
    }
    Assert-True $FailoverObserved "The test did not observe a request retry away from payment-node-3."
    Write-Host "   node-down failover confirmed across 8 checkouts" -ForegroundColor Green

    Reset-All

    Write-Host "3. Validating shared dependency outage..." -ForegroundColor Cyan
    Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/shared-dependency-outage" -TimeoutSec 10 | Out-Null
    Start-Sleep -Seconds 2
    $State = Get-State
    $DependencyFaults = @($State.paymentNodes | Where-Object { $_.faultMode -eq "shared_dependency" })
    Assert-True ($DependencyFaults.Count -eq 3) "The shared dependency fault was not applied to all three nodes."

    $FailureObserved = $false
    try {
        $Payload = @{
            customerId = "TURB-SHARED"
            currency = "USD"
            items = @(@{ productId = "PROD-102"; quantity = 1; unitPrice = 29.50 })
        } | ConvertTo-Json -Depth 6
        Invoke-RestMethod -Method Post -Uri "http://localhost:8080/checkout" -ContentType "application/json" -Body $Payload -TimeoutSec 15 | Out-Null
    }
    catch {
        $FailureObserved = $true
    }
    Assert-True $FailureObserved "Checkout unexpectedly succeeded during the shared dependency outage."
    Write-Host "   fleet-wide shared dependency failure confirmed" -ForegroundColor Green

    Write-Host "Controlled turbulence validation passed." -ForegroundColor Green
}
catch {
    Write-Host "" 
    Write-Warning "Turbulence validation failed: $($_.Exception.Message)"
    Write-Host "Recent Payment Router logs:" -ForegroundColor Yellow
    docker compose logs --tail 40 payment-router 2>$null
    Write-Host "Recent payment-node logs:" -ForegroundColor Yellow
    docker compose logs --tail 25 payment-node-1 payment-node-2 payment-node-3 2>$null
    throw
}
finally {
    try {
        Reset-All
        Write-Host "All turbulence reset in test cleanup." -ForegroundColor Yellow
    }
    catch {
        Write-Warning "Automatic scenario cleanup failed. Use Reset all faults. $($_.Exception.Message)"
    }

    try {
        Resume-BackgroundLoad
    }
    catch {
        Write-Warning "The test paused Locust but could not restart it. Run: docker compose up -d load-generator"
    }
}