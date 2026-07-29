#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectRoot,

    [Parameter(Mandatory = $true)]
    [switch]$ConfirmDataLoss,

    [switch]$RemoveLocalSecrets,

    [switch]$RemovePatchBackups
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
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [switch]$AllowFailure
    )

    & docker compose @Arguments
    $ExitCode = $LASTEXITCODE

    if (-not $AllowFailure -and $ExitCode -ne 0) {
        throw "Docker Compose failed with exit code $ExitCode`: docker compose $($Arguments -join ' ')"
    }
}

function Get-ComposeProjectName {
    param([Parameter(Mandatory = $true)][string]$FallbackName)

    try {
        $JsonText = (& docker compose config --format json 2>$null | Out-String).Trim()

        if (-not [string]::IsNullOrWhiteSpace($JsonText)) {
            $Config = $JsonText | ConvertFrom-Json

            if (
                $null -ne $Config.name -and
                -not [string]::IsNullOrWhiteSpace([string]$Config.name)
            ) {
                return [string]$Config.name
            }
        }
    }
    catch {
        Write-Warning "Could not resolve the Compose project name from JSON config."
    }

    if (-not [string]::IsNullOrWhiteSpace($env:COMPOSE_PROJECT_NAME)) {
        return $env:COMPOSE_PROJECT_NAME
    }

    return $FallbackName.ToLowerInvariant()
}

function Test-IsUnderProject {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$Candidate
    )

    $Root = [System.IO.Path]::GetFullPath($ProjectRoot).TrimEnd("\", "/")
    $Path = [System.IO.Path]::GetFullPath($Candidate).TrimEnd("\", "/")

    if ($Path.Equals($Root, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $false
    }

    return $Path.StartsWith(
        $Root + [System.IO.Path]::DirectorySeparatorChar,
        [System.StringComparison]::OrdinalIgnoreCase
    )
}

function Remove-ProjectItem {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Reason
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    $Resolved = (Resolve-Path -LiteralPath $Path).Path

    if (-not (Test-IsUnderProject -ProjectRoot $ProjectRoot -Candidate $Resolved)) {
        throw "Safety stop: refusing to delete outside the project root: $Resolved"
    }

    Write-Host "Removing $Resolved ($Reason)" -ForegroundColor Yellow
    Remove-Item -LiteralPath $Resolved -Recurse -Force
}

if (-not $ConfirmDataLoss) {
    throw "This permanently deletes all PulseGuard Docker data. Re-run with -ConfirmDataLoss."
}

if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI was not found."
}

$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path.TrimEnd("\", "/")

$ComposeFile = Join-Path $ProjectRoot "compose.yaml"
if (-not (Test-Path -LiteralPath $ComposeFile)) {
    $ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
}

if (-not (Test-Path -LiteralPath $ComposeFile)) {
    throw "No compose.yaml or docker-compose.yml was found under: $ProjectRoot"
}

Push-Location $ProjectRoot

try {
    Write-Section "PulseGuard final local teardown"
    Write-Host "Project: $ProjectRoot"
    Write-Host ""
    Write-Host "This permanently removes all PulseGuard runtime resources:" -ForegroundColor Yellow
    Write-Host "  - containers and orphan containers"
    Write-Host "  - PostgreSQL, Prometheus, Grafana and automation volumes"
    Write-Host "  - project networks"
    Write-Host "  - locally built PulseGuard images"
    Write-Host "  - logs, caches, test output and generated runtime state"
    Write-Host ""
    Write-Host "Source code and Git metadata are preserved." -ForegroundColor Green

    Invoke-Compose -Arguments @("config", "--quiet")

    $ProjectFolderName = Split-Path -Leaf $ProjectRoot
    $ComposeProjectName = Get-ComposeProjectName -FallbackName $ProjectFolderName

    Write-Host "Compose project: $ComposeProjectName"

    $ContainerIds = @(
        (& docker ps -aq --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $VolumeNames = @(
        (& docker volume ls -q --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $NetworkIds = @(
        (& docker network ls -q --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    Write-Section "Removing Compose resources"

    Invoke-Compose -Arguments @(
        "down",
        "--volumes",
        "--remove-orphans",
        "--rmi",
        "local",
        "--timeout",
        "30"
    ) -AllowFailure

    foreach ($ContainerId in $ContainerIds) {
        & docker container inspect $ContainerId 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            & docker container rm -f $ContainerId | Out-Null
        }
    }

    foreach ($VolumeName in $VolumeNames) {
        & docker volume inspect $VolumeName 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            & docker volume rm -f $VolumeName | Out-Null
        }
    }

    foreach ($NetworkId in $NetworkIds) {
        & docker network inspect $NetworkId 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            & docker network rm $NetworkId 2>$null | Out-Null
        }
    }

    $ProjectImageIds = @(
        (& docker image ls -q --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $OpsAiImageIds = @(
        (& docker image ls --format "{{.Repository}} {{.ID}}") |
            Where-Object { $_ -match '^(opsai-|pulseguard-)' } |
            ForEach-Object { ($_ -split "\s+", 2)[1] } |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $AllProjectImageIds = @(
        $ProjectImageIds + $OpsAiImageIds |
            Sort-Object -Unique
    )

    foreach ($ImageId in $AllProjectImageIds) {
        & docker image inspect $ImageId 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            & docker image rm -f $ImageId 2>$null | Out-Null
        }
    }

    Write-Host "[PASS] PulseGuard Docker resources removed." -ForegroundColor Green
    Write-Host "[PASS] PostgreSQL and all persisted runtime history removed." -ForegroundColor Green

    Write-Section "Removing generated project files"

    $GeneratedDirectories = @(
        "logs",
        "log",
        "runtime",
        ".runtime",
        "state",
        ".state",
        "tmp",
        "temp",
        "test-results",
        "test_output",
        "test-output",
        "coverage",
        "htmlcov",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        "postgres-data",
        "postgres_data",
        "db-data",
        "db_data",
        "prometheus-data",
        "prometheus_data",
        "grafana-data",
        "grafana_data"
    )

    if ($RemovePatchBackups) {
        $GeneratedDirectories += @(
            "patch-backups",
            "release-backups"
        )
    }

    foreach ($RelativePath in $GeneratedDirectories) {
        $Candidate = Join-Path $ProjectRoot $RelativePath
        if (Test-Path -LiteralPath $Candidate) {
            Remove-ProjectItem `
                -ProjectRoot $ProjectRoot `
                -Path $Candidate `
                -Reason "generated runtime content"
        }
    }

    $ExcludedFragments = @(
        "\.git\",
        "\node_modules\",
        "\.venv\",
        "\venv\"
    )

    $CacheDirectories = @(
        Get-ChildItem `
            -LiteralPath $ProjectRoot `
            -Recurse `
            -Directory `
            -Force `
            -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Name -in @(
                    "__pycache__",
                    ".pytest_cache",
                    ".mypy_cache",
                    ".ruff_cache"
                )
            } |
            Where-Object {
                $Normalised = $_.FullName.Replace("/", "\").ToLowerInvariant()
                $Excluded = $false

                foreach ($Fragment in $ExcludedFragments) {
                    if ($Normalised.Contains($Fragment)) {
                        $Excluded = $true
                        break
                    }
                }

                -not $Excluded
            } |
            Sort-Object { $_.FullName.Length } -Descending
    )

    foreach ($Directory in $CacheDirectories) {
        if (Test-Path -LiteralPath $Directory.FullName) {
            Remove-ProjectItem `
                -ProjectRoot $ProjectRoot `
                -Path $Directory.FullName `
                -Reason "cache"
        }
    }

    $LogFiles = @(
        Get-ChildItem `
            -LiteralPath $ProjectRoot `
            -Recurse `
            -File `
            -Force `
            -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Extension.Equals(
                    ".log",
                    [System.StringComparison]::OrdinalIgnoreCase
                )
            } |
            Where-Object {
                $Normalised = $_.FullName.Replace("/", "\").ToLowerInvariant()
                $Excluded = $false

                foreach ($Fragment in $ExcludedFragments) {
                    if ($Normalised.Contains($Fragment)) {
                        $Excluded = $true
                        break
                    }
                }

                -not $Excluded
            }
    )

    foreach ($LogFile in $LogFiles) {
        Write-Host "Removing log file $($LogFile.FullName)" -ForegroundColor Yellow
        Remove-Item -LiteralPath $LogFile.FullName -Force
    }

    if ($RemoveLocalSecrets) {
        Write-Section "Removing local secret files"

        $SecretFiles = @(
            ".env",
            ".env.local",
            ".env.production",
            ".env.development",
            "secrets.json",
            "credentials.json"
        )

        foreach ($SecretName in $SecretFiles) {
            $SecretPath = Join-Path $ProjectRoot $SecretName

            if (Test-Path -LiteralPath $SecretPath) {
                Remove-ProjectItem `
                    -ProjectRoot $ProjectRoot `
                    -Path $SecretPath `
                    -Reason "local secret file"
            }
        }

        Write-Host "[PASS] Requested local secret files removed." -ForegroundColor Green
    }
    else {
        Write-Host ""
        Write-Host "Local .env and credential files were preserved." -ForegroundColor DarkGray
        Write-Host "Use -RemoveLocalSecrets before copying or publishing the repository." -ForegroundColor DarkGray
    }

    Write-Section "Final verification"

    $RemainingContainers = @(
        (& docker ps -aq --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $RemainingVolumes = @(
        (& docker volume ls -q --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    $RemainingNetworks = @(
        (& docker network ls -q --filter ("label=com.docker.compose.project=" + $ComposeProjectName)) |
            Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
    )

    if ($RemainingContainers.Count -gt 0) {
        throw "Cleanup incomplete: project containers still remain."
    }

    if ($RemainingVolumes.Count -gt 0) {
        throw "Cleanup incomplete: project volumes still remain."
    }

    if ($RemainingNetworks.Count -gt 0) {
        throw "Cleanup incomplete: project networks still remain."
    }

    Write-Host "[PASS] No PulseGuard containers remain." -ForegroundColor Green
    Write-Host "[PASS] No PulseGuard volumes remain." -ForegroundColor Green
    Write-Host "[PASS] No PulseGuard networks remain." -ForegroundColor Green
    Write-Host "[PASS] Source tree is ready to copy to the personal laptop." -ForegroundColor Green

    Write-Host ""
    Write-Host "Unrelated Docker projects were not pruned." -ForegroundColor DarkGray
}
finally {
    Pop-Location
}
