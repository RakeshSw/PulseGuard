[CmdletBinding()]
param([ValidateSet("all","active","open","acknowledged","resolved")][string]$Status="all")
$ErrorActionPreference="Stop"
$Response=Invoke-RestMethod -Method Get -Uri "http://localhost:8095/incidents?status=$Status&limit=100"
$Response.incidents | Select-Object status,severity,incident_type,node,title,opened_at,resolved_at | Format-Table -AutoSize
