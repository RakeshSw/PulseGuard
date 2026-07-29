#requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$ProjectRoot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Write-Section {
    param([Parameter(Mandatory = $true)][string]$Text)

    Write-Host ""
    Write-Host $Text -ForegroundColor Cyan
    Write-Host ("=" * $Text.Length) -ForegroundColor DarkCyan
}

function Test-ExcludedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    $Normalised = $Path.Replace("/", "\").ToLowerInvariant()
    $Excluded = @(
        "\.git\",
        "\node_modules\",
        "\.venv\",
        "\venv\",
        "\__pycache__\",
        "\dist\",
        "\build\"
    )

    foreach ($Fragment in $Excluded) {
        if ($Normalised.Contains($Fragment)) {
            return $true
        }
    }

    return $false
}

$ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path.TrimEnd("\", "/")
$Failures = New-Object "System.Collections.Generic.List[string]"
$Warnings = New-Object "System.Collections.Generic.List[string]"

Write-Section "PulseGuard public release verification"
Write-Host "Project: $ProjectRoot"

$RequiredFiles = @(
    "README.md",
    "LICENSE",
    "SECURITY.md",
    "CONTRIBUTING.md",
    ".gitignore",
    ".env.example",
    "docs\index.html",
    "docs\architecture.html",
    "docs\demo.html",
    "docs\security.html"
)

foreach ($RelativePath in $RequiredFiles) {
    $Path = Join-Path $ProjectRoot $RelativePath
    if (-not (Test-Path -LiteralPath $Path)) {
        $Failures.Add("Required file missing: $RelativePath")
    }
}

$ForbiddenFiles = @(
    ".env",
    ".env.local",
    ".env.production",
    ".env.development",
    "credentials.json",
    "secrets.json"
)

foreach ($RelativePath in $ForbiddenFiles) {
    $Path = Join-Path $ProjectRoot $RelativePath
    if (Test-Path -LiteralPath $Path) {
        $Failures.Add("Local secret file exists: $RelativePath")
    }
}

$ForbiddenDirectories = @(
    "patch-backups",
    "release-backups",
    "logs",
    "runtime",
    "postgres-data",
    "prometheus-data",
    "grafana-data"
)

foreach ($RelativePath in $ForbiddenDirectories) {
    $Path = Join-Path $ProjectRoot $RelativePath
    if (Test-Path -LiteralPath $Path) {
        $Failures.Add("Generated or private directory exists: $RelativePath")
    }
}

$TextExtensions = @(
    ".py", ".ps1", ".md", ".yaml", ".yml", ".json",
    ".js", ".jsx", ".ts", ".tsx", ".css", ".scss",
    ".html", ".htm", ".txt", ".toml", ".ini", ".cfg",
    ".properties", ".example"
)

$VerifierPath = $MyInvocation.MyCommand.Path

$TextFiles = @(
    Get-ChildItem -LiteralPath $ProjectRoot -Recurse -File -Force -ErrorAction SilentlyContinue |
        Where-Object {
            -not (Test-ExcludedPath -Path $_.FullName) -and
            -not $_.FullName.Equals(
                $VerifierPath,
                [System.StringComparison]::OrdinalIgnoreCase
            ) -and
            (
                $TextExtensions -contains $_.Extension.ToLowerInvariant() -or
                $_.Name -in @("Dockerfile", ".gitignore", ".env.example", "LICENSE")
            )
        }
)

$RejectedBrandPattern = (("Ops" + "AI Commander") + "|" + ("Command" + "[\s-]?" + "Weave"))
$MojibakePattern = (([string][char]0x00C3) + "|" + ([string][char]0x00C2))

$Checks = @(
    [pscustomobject]@{
        Name = "Rejected public branding"
        Pattern = $RejectedBrandPattern
        Severity = "failure"
    },
    [pscustomobject]@{
        Name = "Hard-coded local Windows user path"
        Pattern = "C:\\Users\\[^\\]+\\"
        Severity = "failure"
    },
    [pscustomobject]@{
        Name = "Bearer token"
        Pattern = "(?i)authorization\s*[:=]\s*bearer\s+[A-Za-z0-9._~+/=-]{12,}"
        Severity = "failure"
    },
    [pscustomobject]@{
        Name = "Likely API key or secret"
        Pattern = "(?i)(api[_-]?key|client[_-]?secret|password|passwd|access[_-]?token)\s*[:=]\s*['`"]?[A-Za-z0-9._~+/=-]{12,}"
        Severity = "failure"
    },
    [pscustomobject]@{
        Name = "Mojibake"
        Pattern = $MojibakePattern
        Severity = "failure"
    },
    [pscustomobject]@{
        Name = "Unresolved placeholder"
        Pattern = "<YOUR_GITHUB_USERNAME>|replace-with-a-local-password"
        Severity = "warning"
    }
)

foreach ($File in $TextFiles) {
    foreach ($Check in $Checks) {
        if (
            $File.Name -eq ".env.example" -and
            $Check.Name -eq "Likely API key or secret"
        ) {
            continue
        }

        $Matches = @(
            Select-String `
                -LiteralPath $File.FullName `
                -Pattern $Check.Pattern `
                -AllMatches `
                -ErrorAction SilentlyContinue
        )

        foreach ($Match in $Matches) {
            $Relative = $File.FullName.Substring($ProjectRoot.Length).TrimStart("\", "/")
            $Message = (
                $Check.Name + ": " +
                $Relative + ":" +
                $Match.LineNumber
            )

            if ($Check.Severity -eq "failure") {
                $Failures.Add($Message)
            }
            else {
                $Warnings.Add($Message)
            }
        }
    }
}

$ComposeFile = Join-Path $ProjectRoot "compose.yaml"
if (-not (Test-Path -LiteralPath $ComposeFile)) {
    $ComposeFile = Join-Path $ProjectRoot "docker-compose.yml"
}

if (Test-Path -LiteralPath $ComposeFile) {
    if ($null -eq (Get-Command docker -ErrorAction SilentlyContinue)) {
        $Warnings.Add("Docker CLI not found; Compose validation skipped.")
    }
    else {
        Push-Location $ProjectRoot
        $RequiredComposeSecrets = @(
            "GRAFANA_ADMIN_PASSWORD",
            "POSTGRES_PASSWORD",
            "AUTOMATION_API_TOKEN",
            "EXTERNAL_AUTH_INITIAL_TOKEN"
        )
        $PreviousValues = @{}
        try {
            foreach ($Name in $RequiredComposeSecrets) {
                $PreviousValues[$Name] = [Environment]::GetEnvironmentVariable($Name, "Process")
                $Buffer = New-Object byte[] 24
                $Generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
                try {
                    $Generator.GetBytes($Buffer)
                }
                finally {
                    $Generator.Dispose()
                }
                $Value = [Convert]::ToBase64String($Buffer).TrimEnd([char[]]"=").Replace("+", "A").Replace("/", "B")
                [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
            }

            & docker compose --env-file .env.example config --quiet
            if ($LASTEXITCODE -ne 0) {
                $Failures.Add("docker compose config validation failed.")
            }
        }
        finally {
            foreach ($Name in $RequiredComposeSecrets) {
                [Environment]::SetEnvironmentVariable($Name, $PreviousValues[$Name], "Process")
            }
            Pop-Location
        }
    }
}
else {
    $Failures.Add("Compose file not found.")
}

if ($null -ne (Get-Command git -ErrorAction SilentlyContinue)) {
    Push-Location $ProjectRoot
    try {
        $TrackedSecretFiles = @(
            (& git ls-files) |
                Where-Object {
                    $_ -match '(^|/)\.env($|\.)' -or
                    $_ -match '(?i)(credential|secret|token|\.pem$|\.pfx$|\.key$)'
                }
        )

        foreach ($TrackedFile in $TrackedSecretFiles) {
            if ($TrackedFile -ne ".env.example") {
                $Failures.Add("Potential secret file tracked by Git: $TrackedFile")
            }
        }
    }
    finally {
        Pop-Location
    }
}

Write-Section "Verification result"

if ($Warnings.Count -gt 0) {
    Write-Host "Warnings:" -ForegroundColor Yellow
    $Warnings | Sort-Object -Unique | ForEach-Object {
        Write-Host ("  " + $_) -ForegroundColor Yellow
    }
    Write-Host ""
}

if ($Failures.Count -gt 0) {
    Write-Host "Failures:" -ForegroundColor Red
    $Failures | Sort-Object -Unique | ForEach-Object {
        Write-Host ("  " + $_) -ForegroundColor Red
    }

    throw "PulseGuard is not ready for public publication."
}

Write-Host "[PASS] Required public files are present." -ForegroundColor Green
Write-Host "[PASS] No local secret files or generated runtime directories were found." -ForegroundColor Green
Write-Host "[PASS] No rejected branding, hard-coded user paths, or obvious secrets were found." -ForegroundColor Green
Write-Host "[PASS] Compose configuration is valid." -ForegroundColor Green
Write-Host "[PASS] PulseGuard is ready for final human review and Git staging." -ForegroundColor Green
