#requires -Version 5.1
[CmdletBinding()]
param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),

    [Parameter(Mandatory = $true)]
    [switch]$ConfirmDataLoss,

    [switch]$ReplaceExistingEnv,

    [switch]$StopAfterTest,

    [switch]$CleanAfterTest,

    [int]$StartupTimeoutSeconds = 420
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Section {
    param([Parameter(Mandatory = $true)][string]$Text)

    Write-Host ""
    Write-Host $Text -ForegroundColor Cyan
    Write-Host ("=" * $Text.Length) -ForegroundColor DarkCyan
}

function Invoke-Compose {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose failed: docker compose $($Arguments -join ' ')"
    }
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

function Set-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Value
    )

    $Text = [System.IO.File]::ReadAllText($Path)
    $Pattern = "(?m)^" + [regex]::Escape($Name) + "\\s*=.*$"

    if ([regex]::IsMatch($Text, $Pattern)) {
        $Text = [regex]::Replace($Text, $Pattern, ($Name + "=" + $Value))
    }
    else {
        $Text = $Text.TrimEnd() + [Environment]::NewLine + $Name + "=" + $Value + [Environment]::NewLine
    }

    [System.IO.File]::WriteAllText(
        $Path,
        $Text,
        (New-Object System.Text.UTF8Encoding($false))
    )
}

function Get-DotEnvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Name
    )

    $Line = Get-Content -LiteralPath $Path |
        Where-Object { $_ -match ("^" + [regex]::Escape($Name) + "=") } |
        Select-Object -Last 1

    if (-not $Line) {
        return $null
    }

    return ($Line -split "=", 2)[1].Trim()
}

function Wait-Endpoint {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][datetime]$Deadline
    )

    do {
        try {
            Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 5 | Out-Null
            Write-Host "[PASS] $Name" -ForegroundColor Green
            return
        }
        catch {
            Start-Sleep -Seconds 5
        }
    }
    while ((Get-Date) -lt $Deadline)

    throw "$Name did not become ready at $Uri."
}

function Assert-PageText {
    param(
        [Parameter(Mandatory = $true)][string]$Uri,
        [Parameter(Mandatory = $true)][string[]]$Required,
        [string[]]$Forbidden = @()
    )

    $Content = (Invoke-WebRequest -UseBasicParsing -Uri $Uri -TimeoutSec 20).Content

    foreach ($Marker in $Required) {
        if ($Content -notmatch [regex]::Escape($Marker)) {
            throw "Required UI marker is missing from $Uri`: $Marker"
        }
    }

    foreach ($Marker in $Forbidden) {
        if ($Content -match [regex]::Escape($Marker)) {
            throw "Forbidden UI marker remains in $Uri`: $Marker"
        }
    }
}

function Save-FailureBundle {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$Stamp,
        [Parameter(Mandatory = $true)][string]$FailureDetails
    )

    $Downloads = Join-Path $HOME "Downloads"
    if (-not (Test-Path -LiteralPath $Downloads)) {
        New-Item -ItemType Directory -Path $Downloads -Force | Out-Null
    }
    $Folder = Join-Path $Downloads ("PulseGuard-E2E-failure-" + $Stamp)
    $Zip = $Folder + ".zip"
    New-Item -ItemType Directory -Path $Folder -Force | Out-Null

    $FailureDetails | Out-File (Join-Path $Folder "failure.txt") -Encoding utf8 -Width 5000
    try { docker compose ps -a 2>&1 | Out-File (Join-Path $Folder "compose-ps.txt") -Encoding utf8 } catch {}
    try { docker compose logs --no-color --timestamps 2>&1 | Out-File (Join-Path $Folder "compose-logs.txt") -Encoding utf8 } catch {}

    if (Test-Path -LiteralPath $Zip) {
        Remove-Item -LiteralPath $Zip -Force
    }

    Compress-Archive -Path (Join-Path $Folder "*") -DestinationPath $Zip -CompressionLevel Optimal
    Remove-Item -LiteralPath $Folder -Recurse -Force
    Write-Warning "Failure diagnostics: $Zip"
}

if (-not $ConfirmDataLoss) {
    throw "This clean-room test deletes PulseGuard containers, volumes, images and database data. Re-run with -ConfirmDataLoss."
}

if ($StartupTimeoutSeconds -lt 120) {
    throw "StartupTimeoutSeconds must be at least 120."
}

if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI is not installed."
}

$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path.TrimEnd([char[]]@("\", "/"))
$ComposeFile = Join-Path $ProjectRoot "compose.yaml"
$EnvExample = Join-Path $ProjectRoot ".env.example"
$EnvFile = Join-Path $ProjectRoot ".env"
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$Downloads = Join-Path $HOME "Downloads"
if (-not (Test-Path -LiteralPath $Downloads)) {
    New-Item -ItemType Directory -Path $Downloads -Force | Out-Null
}
$ReportPath = Join-Path $Downloads ("PulseGuard-Docker-E2E-" + $Stamp + ".txt")
$Results = New-Object "System.Collections.Generic.List[string]"

if (-not (Test-Path -LiteralPath $ComposeFile)) {
    throw "compose.yaml was not found under $ProjectRoot."
}

if (-not (Test-Path -LiteralPath $EnvExample)) {
    throw ".env.example was not found under $ProjectRoot."
}

Push-Location $ProjectRoot
try {
    Write-Section "PulseGuard clean-room Docker E2E validation"
    Write-Host "Project: $ProjectRoot"
    Write-Host "Report:  $ReportPath"

    docker info | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop Linux engine is unavailable."
    }

    if (Test-Path -LiteralPath $EnvFile) {
        if (-not $ReplaceExistingEnv) {
            throw ".env already exists. Use -ReplaceExistingEnv only when it is safe to replace the local configuration."
        }
        Remove-Item -LiteralPath $EnvFile -Force
    }

    Copy-Item -LiteralPath $EnvExample -Destination $EnvFile -Force

    $GeneratedSecrets = @{}
    foreach ($Name in @(
        "GRAFANA_ADMIN_PASSWORD",
        "POSTGRES_PASSWORD",
        "AUTOMATION_API_TOKEN",
        "EXTERNAL_AUTH_INITIAL_TOKEN"
    )) {
        $GeneratedSecrets[$Name] = New-LocalSecret
        Set-DotEnvValue -Path $EnvFile -Name $Name -Value $GeneratedSecrets[$Name]
    }

    $UniqueSecrets = @($GeneratedSecrets.Values | Sort-Object -Unique)
    if ($UniqueSecrets.Count -ne $GeneratedSecrets.Count) {
        throw "Generated secrets are not unique."
    }

    foreach ($Name in $GeneratedSecrets.Keys) {
        $Value = Get-DotEnvValue -Path $EnvFile -Name $Name
        if (
            [string]::IsNullOrWhiteSpace($Value) -or
            $Value.StartsWith("CHANGE_ME_", [System.StringComparison]::OrdinalIgnoreCase)
        ) {
            throw "Required secret was not generated: $Name"
        }
    }
    $Results.Add("PASS: unique local secrets generated")

    Invoke-Compose -Arguments @("config", "--quiet")
    $ComposeText = (& docker compose config | Out-String)
    if ($ComposeText -match "/var/run/docker\\.sock") {
        throw "Docker socket exposure was detected."
    }
    $Results.Add("PASS: Compose validation and Docker-socket safety")

    Write-Section "Removing previous runtime"
    & docker compose down --volumes --remove-orphans --rmi local --timeout 30
    # A first-run Compose project may not exist, so down is allowed to return nonzero.

    Write-Section "Building every service with no cache"
    Invoke-Compose -Arguments @("build", "--no-cache", "--pull")
    $Results.Add("PASS: no-cache Docker build")

    Write-Section "Starting clean environment"
    Invoke-Compose -Arguments @(
        "up",
        "-d",
        "--force-recreate",
        "--renew-anon-volumes",
        "--remove-orphans"
    )

    $Deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
    $Endpoints = @(
        @{ Name = "Checkout Service"; Uri = "http://localhost:8080/health" },
        @{ Name = "Payment Router"; Uri = "http://localhost:8081/health" },
        @{ Name = "Locust"; Uri = "http://localhost:8089" },
        @{ Name = "Scenario Controller"; Uri = "http://localhost:8090/health" },
        @{ Name = "Wikimedia Adapter"; Uri = "http://localhost:8093/health" },
        @{ Name = "Corruption Adapter"; Uri = "http://localhost:8094/health" },
        @{ Name = "PulseGuard Core"; Uri = "http://localhost:8095/health" },
        @{ Name = "PulseGuard Investigation"; Uri = "http://localhost:8096/health" },
        @{ Name = "PulseGuard Automation"; Uri = "http://localhost:8097/health" },
        @{ Name = "PulseGuard Predictor"; Uri = "http://localhost:8098/health" },
        @{ Name = "External Auth Service"; Uri = "http://localhost:8099/health" },
        @{ Name = "Prometheus"; Uri = "http://localhost:9090/-/ready" },
        @{ Name = "Grafana"; Uri = "http://localhost:3000/api/health" }
    )

    foreach ($Endpoint in $Endpoints) {
        Wait-Endpoint -Name $Endpoint.Name -Uri $Endpoint.Uri -Deadline $Deadline
    }
    $Results.Add("PASS: all public health and UI endpoints")

    $ExpectedServices = @(
        (& docker compose config --services) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )
    $ContainerIds = @(
        (& docker compose ps -q) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    if ($ContainerIds.Count -ne $ExpectedServices.Count) {
        throw "Expected $($ExpectedServices.Count) containers but found $($ContainerIds.Count)."
    }

    foreach ($ContainerId in $ContainerIds) {
        $Inspect = (& docker inspect $ContainerId | ConvertFrom-Json)[0]
        $StateStatus = [string]$Inspect.State.Status

        if ($StateStatus -ne "running") {
            throw "Container is not running: $($Inspect.Name) / $StateStatus"
        }

        # Some third-party containers do not define a Docker HEALTHCHECK.
        # Under Set-StrictMode, directly accessing a missing State.Health
        # property throws, so inspect the property collection first.
        $HealthProperty = $Inspect.State.PSObject.Properties["Health"]
        if ($null -ne $HealthProperty -and $null -ne $HealthProperty.Value) {
            $HealthStatusProperty = $HealthProperty.Value.PSObject.Properties["Status"]
            $HealthStatus = if ($null -ne $HealthStatusProperty) {
                [string]$HealthStatusProperty.Value
            }
            else {
                "unknown"
            }

            if ($HealthStatus -ne "healthy") {
                throw "Container is not healthy: $($Inspect.Name) / $HealthStatus"
            }
        }
    }
    $Results.Add("PASS: every Compose service running and healthy")

    Write-Section "Validating UI branding and secret exposure"

    # Invoke-WebRequest validates the server-rendered HTML only. The shared
    # activity widget is injected by widget.js at browser runtime, so validate
    # the static pages and the widget asset separately.
    Assert-PageText `
        -Uri "http://localhost:8095/" `
        -Required @(
            "PulseGuard Incident Console",
            "Live traffic signal",
            "http://localhost:8097/widget.js"
        ) `
        -Forbidden @(
            "OpsAI Incident Console",
            "OpsAI Live Activity"
        )

    Assert-PageText `
        -Uri "http://localhost:8096/" `
        -Required @(
            "PulseGuard Investigation",
            "Evidence-bounded PulseGuard investigation",
            "http://localhost:8097/widget.js"
        ) `
        -Forbidden @("OpsAI Live Activity")

    Assert-PageText `
        -Uri "http://localhost:8097/widget.js" `
        -Required @(
            "PulseGuard Live Activity",
            "PulseGuard separates recommendation, governance, action execution"
        ) `
        -Forbidden @("OpsAI Live Activity")

    $ExternalHealth = Invoke-RestMethod -Uri "http://localhost:8099/health" -TimeoutSec 20
    if ($ExternalHealth.PSObject.Properties.Name -contains "token") {
        throw "External-auth health response exposes a token field."
    }
    if ([string]::IsNullOrWhiteSpace([string]$ExternalHealth.tokenFingerprint)) {
        throw "External-auth health response is missing the safe fingerprint."
    }
    $Results.Add("PASS: public branding and secret-safe health output")

    Write-Section "Running platform validation"
    & (Join-Path $ProjectRoot "scripts/test-opsai-v06.ps1") -ProjectRoot $ProjectRoot
    $Results.Add("PASS: platform and observability validation")

    Write-Section "Running detection, investigation and recovery validation"
    & (Join-Path $ProjectRoot "scripts/test-day4.ps1")
    $Results.Add("PASS: detection, investigation, governance and recovery")

    Write-Section "Running credential repair and approval-gated restart"
    & (Join-Path $ProjectRoot "scripts/test-opsai-auth-restart.ps1") `
        -Scenario all `
        -ProjectRoot $ProjectRoot
    $Results.Add("PASS: credential auto-repair and approval-gated restart")

    Write-Section "Running automatic repair validation"
    & (Join-Path $ProjectRoot "scripts/test-opsai-auto-repair.ps1") -Scenario both
    $Results.Add("PASS: disk and certificate automatic repair")

    $Results.Add("PASS: clean-room Docker E2E completed")

    @(
        "PulseGuard Docker E2E validation",
        "Timestamp: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz')",
        "Project: $ProjectRoot",
        "",
        $Results,
        "",
        "docker compose ps:",
        (& docker compose ps -a | Out-String)
    ) | Out-File -LiteralPath $ReportPath -Encoding utf8 -Width 5000

    Write-Section "Docker E2E validation passed"
    $Results | ForEach-Object { Write-Host $_ -ForegroundColor Green }
    Write-Host "Report: $ReportPath" -ForegroundColor Green
}
catch {
    Write-Host ""
    Write-Host "[FAIL] $($_.Exception.Message)" -ForegroundColor Red
    $FailureDetails = ($_ | Format-List * -Force | Out-String -Width 5000)
    Save-FailureBundle `
        -ProjectRoot $ProjectRoot `
        -Stamp $Stamp `
        -FailureDetails $FailureDetails
    throw
}
finally {
    try {
        Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" -TimeoutSec 30 | Out-Null
    }
    catch {}

    if ($CleanAfterTest) {
        try { docker compose down --volumes --remove-orphans --rmi local --timeout 30 | Out-Null } catch {}
        if (Test-Path -LiteralPath $EnvFile) {
            Remove-Item -LiteralPath $EnvFile -Force -ErrorAction SilentlyContinue
        }
    }
    elseif ($StopAfterTest) {
        try { docker compose down --remove-orphans --timeout 30 | Out-Null } catch {}
    }

    Pop-Location
}
