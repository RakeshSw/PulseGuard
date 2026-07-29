[CmdletBinding()]
param()
$ErrorActionPreference = "Stop"
Invoke-RestMethod -Method Post -Uri "http://localhost:8090/scenarios/reset" |
    ConvertTo-Json -Depth 8
