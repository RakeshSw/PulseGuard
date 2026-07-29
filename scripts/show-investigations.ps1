[CmdletBinding()]
param()
$ErrorActionPreference="Stop"
$r=Invoke-RestMethod http://localhost:8096/api/investigations
$r.investigations | Select-Object incident_title,incident_status,analysis_mode,provider,confidence,action_name,policy_decision,completed_at | Format-Table -AutoSize
