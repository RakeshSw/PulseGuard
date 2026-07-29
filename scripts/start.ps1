#requires -Version 5.1
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host ""
Write-Host "PulseGuard 1.0.1-poc startup" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan

try {
    docker info | Out-Null
}
catch {
    Write-Error "Docker Desktop is not running or Docker is unavailable. Start Docker Desktop and retry."
    exit 1
}

function New-LocalSecret {
    param([int]$Bytes = 24)

    $Buffer = New-Object byte[] $Bytes
    $Generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $Generator.GetBytes($Buffer)
    }
    finally {
        $Generator.Dispose()
    }

    return [Convert]::ToBase64String($Buffer).TrimEnd([char[]]"=").Replace("+", "A").Replace("/", "B")
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created local .env from .env.example" -ForegroundColor Yellow
}

$EnvPath = Join-Path $Root ".env"
$EnvText = [System.IO.File]::ReadAllText($EnvPath)
$RequiredSecrets = @(
    "GRAFANA_ADMIN_PASSWORD",
    "POSTGRES_PASSWORD",
    "AUTOMATION_API_TOKEN",
    "EXTERNAL_AUTH_INITIAL_TOKEN"
)

$SecretsChanged = $false
foreach ($Name in $RequiredSecrets) {
    $Pattern = "(?m)^" + [regex]::Escape($Name) + "\s*=\s*(.*)$"
    $Match = [regex]::Match($EnvText, $Pattern)
    $CurrentValue = if ($Match.Success) { $Match.Groups[1].Value.Trim() } else { "" }

    if (
        -not $Match.Success -or
        [string]::IsNullOrWhiteSpace($CurrentValue) -or
        $CurrentValue.StartsWith("CHANGE_ME_", [System.StringComparison]::OrdinalIgnoreCase)
    ) {
        $GeneratedValue = New-LocalSecret
        if ($Match.Success) {
            $EnvText = [regex]::Replace(
                $EnvText,
                $Pattern,
                ($Name + "=" + $GeneratedValue),
                1
            )
        }
        else {
            $EnvText = $EnvText.TrimEnd() + [Environment]::NewLine + $Name + "=" + $GeneratedValue + [Environment]::NewLine
        }
        $SecretsChanged = $true
    }
}

if ($SecretsChanged) {
    [System.IO.File]::WriteAllText(
        $EnvPath,
        $EnvText,
        (New-Object System.Text.UTF8Encoding($false))
    )
    Write-Host "Generated local random credentials in .env" -ForegroundColor Green
}

Write-Host "Validating Docker Compose configuration..." -ForegroundColor Cyan
docker compose config --quiet
if ($LASTEXITCODE -ne 0) {
    throw "docker compose config validation failed."
}

Write-Host "Building and starting PulseGuard..." -ForegroundColor Cyan
docker compose up --build -d
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed."
}

function Wait-Endpoint {
    param(
        [string]$Name,
        [string]$Uri,
        [int]$Attempts = 60,
        [int]$DelaySeconds = 5
    )

    for ($Attempt = 1; $Attempt -le $Attempts; $Attempt++) {
        try {
            Invoke-RestMethod -Method Get -Uri $Uri -TimeoutSec 4 | Out-Null
            Write-Host "$Name is ready." -ForegroundColor Green
            return
        }
        catch {
            if ($Attempt -eq $Attempts) {
                throw "$Name did not become ready at $Uri."
            }
            Start-Sleep -Seconds $DelaySeconds
        }
    }
}

Wait-Endpoint -Name "Scenario Controller" -Uri "http://localhost:8090/health"
Wait-Endpoint -Name "Payment Router" -Uri "http://localhost:8081/health"
Wait-Endpoint -Name "Checkout Service" -Uri "http://localhost:8080/health"
Wait-Endpoint -Name "Locust" -Uri "http://localhost:8089"
Wait-Endpoint -Name "Prometheus" -Uri "http://localhost:9090/-/ready"
Wait-Endpoint -Name "Grafana" -Uri "http://localhost:3000/api/health"
Wait-Endpoint -Name "PulseGuard Core" -Uri "http://localhost:8095/health"
Wait-Endpoint -Name "PulseGuard Investigation" -Uri "http://localhost:8096/health"
Wait-Endpoint -Name "PulseGuard Automation" -Uri "http://localhost:8097/health"
Wait-Endpoint -Name "PulseGuard Predictor" -Uri "http://localhost:8098/health"
Wait-Endpoint -Name "External Auth Service" -Uri "http://localhost:8099/health"

Write-Host ""
Write-Host "PulseGuard is running." -ForegroundColor Green
Write-Host "Checkout API       : http://localhost:8080"
Write-Host "Payment nodes      : http://localhost:8081/nodes"
Write-Host "Locust             : http://localhost:8089"
Write-Host "Scenario Controller: http://localhost:8090"
Write-Host "Wikimedia profile  : http://localhost:8093/profile"
Write-Host "Corruption adapter : http://localhost:8094/profile"
Write-Host "Prometheus         : http://localhost:9090"
Write-Host "Grafana            : http://localhost:3000  (credentials in .env)"
Write-Host "Incident Console   : http://localhost:8095"
Write-Host "Investigation      : http://localhost:8096"
Write-Host "Automation Console : http://localhost:8097"
Write-Host "Predictive Console : http://localhost:8098"
Write-Host "External Auth Demo : http://localhost:8099"
Write-Host ""
Write-Host "Run .\scripts\test-opsai-v06.ps1 for platform validation." -ForegroundColor Cyan
