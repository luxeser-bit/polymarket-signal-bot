param(
    [switch]$KeepApi
)

$ErrorActionPreference = "Continue"
$ApiUrl = "http://127.0.0.1:8000"

function Stop-ByPattern {
    param(
        [string]$Name,
        [scriptblock]$Filter
    )
    $targets = Get-CimInstance Win32_Process | Where-Object $Filter
    foreach ($target in $targets) {
        Write-Host ("Stopping {0}: PID {1}" -f $Name, $target.ProcessId)
        Stop-Process -Id $target.ProcessId -Force
    }
}

try {
    Invoke-RestMethod -Method Post -Uri "$ApiUrl/system/stop" | Out-Null
    Write-Host "Stopped managed workers through FastAPI."
} catch {
    Write-Host "FastAPI stop endpoint unavailable; stopping known worker processes by pattern."
    Stop-ByPattern "indexer" { $_.Name -like "python*" -and ($_.CommandLine -like "*src.indexer*" -or $_.CommandLine -like "*polymarket_signal_bot.indexer*") }
    Stop-ByPattern "live paper" { $_.Name -like "python*" -and $_.CommandLine -like "*polymarket_signal_bot.live_paper_runner*" }
    Stop-ByPattern "monitor" { $_.Name -like "python*" -and $_.CommandLine -like "*polymarket_signal_bot*" -and $_.CommandLine -like "* monitor *" }
}

Stop-ByPattern "React dashboard" { ($_.Name -eq "node.exe" -and $_.CommandLine -like "*node_modules/vite*") -or ($_.Name -eq "cmd.exe" -and $_.CommandLine -like "*npm run dev*") }

if (-not $KeepApi) {
    Stop-ByPattern "FastAPI" { $_.Name -like "python*" -and $_.CommandLine -like "*server/api.py*" }
}

Write-Host "PolySignal stop command finished."
