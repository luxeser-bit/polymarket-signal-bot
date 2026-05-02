param(
    [string]$TaskName = "PolySignalMaintenance",
    [string]$At = "03:30",
    [switch]$RunNow
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$CycleScript = Join-Path $Root "scripts\maintenance_cycle.ps1"

if (-not (Test-Path $CycleScript)) {
    throw "Maintenance cycle script not found: $CycleScript"
}

$time = [datetime]::ParseExact($At, "HH:mm", [Globalization.CultureInfo]::InvariantCulture)
$arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$CycleScript`""

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $arguments `
    -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -Daily -At $time
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 6)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Daily SQLite WAL checkpoint/cleanup for Polymarket Signal Bot indexer database." `
    -Force | Out-Null

Write-Host "Installed Windows maintenance task: $TaskName"
Write-Host "Schedule: daily at $At"
Write-Host "Script: $CycleScript"

if ($RunNow) {
    Start-ScheduledTask -TaskName $TaskName
    Write-Host "Started task now: $TaskName"
}
