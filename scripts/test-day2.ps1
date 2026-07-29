[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function New-CheckoutBody {
    return @{
        customerId = "CUS-$((Get-Random -Minimum 1000 -Maximum 9999))"
        items = @(@{
            productId = "PROD-$((Get-Random -Minimum 100 -Maximum 999))"
            quantity = Get-Random -Minimum 1 -Maximum 4
            unitPrice = [Math]::Round((Get-Random -Minimum 10 -Maximum 150) + 0.99, 2)
        })
        currency = "USD"
    } | ConvertTo-Json -Depth 5
}

function Invoke-CheckoutBatch([int]$Count) {
    $Results = @()
    for ($Index = 1; $Index -le $Count; $Index++) {
        $Response = Invoke-RestMethod `
            -Method Post `
            -Uri "http://localhost:8080/checkout" `
            -ContentType "application/json" `
            -Body (New-CheckoutBody) `
            -TimeoutSec 20
        $Results += [PSCustomObject]@{
            Request = $Index
            Node = $Response.payment.processedBy
            Attempt = $Response.payment.routeAttempt
            RouterMs = $Response.payment.routerDurationMs
            CheckoutMs = $Response.checkoutDurationMs
        }
    }
    return $Results
}

Write-Host "Temporarily stopping Locust so routing checks are deterministic..." -ForegroundColor Cyan
docker compose stop load-generator | Out-Null

try {
Write-Host "Resetting all scenarios..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null

Write-Host ""
Write-Host "Test 1: Wikimedia live/fallback profile" -ForegroundColor Cyan
$Profile = Invoke-RestMethod -Uri "http://localhost:8093/profile"
$Profile | Select-Object sourceMode, streamConnected, profile, targetUsers, currentEventsPerMinute, lastEventAgeSeconds | Format-List
if ($Profile.targetUsers -lt 1) { throw "Traffic profile did not return a safe user target." }

Write-Host ""
Write-Host "Test 2: Baseline checkout routing" -ForegroundColor Cyan
$Baseline = Invoke-CheckoutBatch 12
$Baseline | Group-Object Node | Select-Object Name, Count | Format-Table -AutoSize
if (@($Baseline.Node | Sort-Object -Unique).Count -lt 3) {
    throw "Baseline did not reach all three nodes."
}

Write-Host ""
Write-Host "Test 3: Real downstream latency on payment-node-3" -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/payment-latency?latency_ms=900&jitter_ms=0" | Out-Null
$Latency = Invoke-CheckoutBatch 15
$Latency | Group-Object Node | ForEach-Object {
    [PSCustomObject]@{
        Node = $_.Name
        Count = $_.Count
        AverageRouterMs = [Math]::Round(($_.Group | Measure-Object RouterMs -Average).Average, 0)
    }
} | Format-Table -AutoSize
$SlowNode3 = @($Latency | Where-Object { $_.Node -eq "payment-node-3" -and $_.RouterMs -ge 800 }).Count
if ($SlowNode3 -eq 0) { throw "The latency fault was not visible on payment-node-3." }

Write-Host ""
Write-Host "Test 4: Real timeout plus router failover" -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/payment-timeout?timeout_ms=300&toxicity=1" | Out-Null
$Timeout = Invoke-CheckoutBatch 18
$Timeout | Group-Object Attempt | Select-Object @{Name='RouteAttempt';Expression={$_.Name}}, Count | Format-Table -AutoSize
if (@($Timeout | Where-Object { $_.Attempt -gt 1 }).Count -eq 0) {
    throw "No router retry was observed during the timeout scenario."
}
if (@($Timeout | Where-Object { $_.Node -eq "payment-node-3" }).Count -gt 0) {
    throw "payment-node-3 should not complete payments while the timeout is active."
}

Write-Host ""
Write-Host "Test 5: Payload corruption is real and detectable" -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/payload-corruption/wrong_type" | Out-Null
$CorruptProfile = Invoke-RestMethod -Uri "http://localhost:8094/profile"
if ($CorruptProfile.targetUsers -ne "not-a-number") {
    throw "Expected a corrupted targetUsers field."
}
Write-Host "Corruption adapter returned targetUsers='$($CorruptProfile.targetUsers)'. Locust will reject it and use the safety fallback." -ForegroundColor Yellow

Write-Host ""
Write-Host "Resetting all faults..." -ForegroundColor Cyan
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" | Out-Null
Write-Host "Day 4 validation passed." -ForegroundColor Green
}
finally {
    Write-Host "Restarting Locust..." -ForegroundColor Cyan
    docker compose start load-generator | Out-Null
}
