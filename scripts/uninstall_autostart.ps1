$ErrorActionPreference = "Stop"
$TaskName = "PolySignalBot"

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed Windows autostart task: $TaskName"
} else {
    Write-Host "Autostart task not found: $TaskName"
}
