param(
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$StartScript = Join-Path $Root "scripts\start_polysignal.ps1"
$TaskName = "PolySignalBot"

if (-not (Test-Path $StartScript)) {
    throw "Start script not found: $StartScript"
}

$arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$StartScript`""
if ($NoBrowser) {
    $arguments += " -NoBrowser"
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $arguments `
    -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Days 0)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description "Start Polymarket Signal Bot API, React dashboard, indexer, monitor, and paper runner at Windows logon." `
    -Force | Out-Null

Write-Host "Installed Windows autostart task: $TaskName"
Write-Host "It will run on your next Windows logon."
