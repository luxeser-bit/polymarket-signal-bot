param(
    [string]$TaskName = "PolySignalMaintenance"
)

$ErrorActionPreference = "Stop"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed Windows maintenance task: $TaskName"
} else {
    Write-Host "Maintenance task not found: $TaskName"
}
