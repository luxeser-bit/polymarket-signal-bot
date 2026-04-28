param(
    [switch]$NoBrowser,
    [switch]$SkipWorkers
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$DashboardDir = Join-Path $Root "crypto-dashboard"
$LogDir = Join-Path $Root "data\system_logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$ApiUrl = "http://127.0.0.1:8000"
$UiUrl = "http://127.0.0.1:3000"

function Test-HttpReady {
    param([string]$Url)
    try {
        Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 2 | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Wait-HttpReady {
    param(
        [string]$Url,
        [int]$Seconds = 45
    )
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-HttpReady $Url) {
            return $true
        }
        Start-Sleep -Seconds 1
    }
    return $false
}

function Start-FastApi {
    if (Test-HttpReady "$ApiUrl/system/status") {
        Write-Host "FastAPI already running: $ApiUrl"
        return
    }

    $python = (Get-Command python -ErrorAction Stop).Source
    $stdout = Join-Path $LogDir "fastapi.out.log"
    $stderr = Join-Path $LogDir "fastapi.err.log"
    Write-Host "Starting FastAPI..."
    Start-Process `
        -FilePath $python `
        -ArgumentList @("server/api.py") `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr

    if (-not (Wait-HttpReady "$ApiUrl/system/status" 45)) {
        throw "FastAPI did not become ready. Check $stderr"
    }
    Write-Host "FastAPI ready: $ApiUrl"
}

function Start-ReactDashboard {
    if (Test-HttpReady $UiUrl) {
        Write-Host "React dashboard already running: $UiUrl"
        return
    }
    if (-not (Test-Path $DashboardDir)) {
        throw "React dashboard directory not found: $DashboardDir"
    }

    $stdout = Join-Path $LogDir "react.out.log"
    $stderr = Join-Path $LogDir "react.err.log"
    $command = 'set "PATH=C:\Program Files\nodejs;%PATH%"&& npm run dev'
    Write-Host "Starting React dashboard..."
    Start-Process `
        -FilePath "cmd.exe" `
        -ArgumentList @("/d", "/s", "/c", $command) `
        -WorkingDirectory $DashboardDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr

    if (-not (Wait-HttpReady $UiUrl 60)) {
        throw "React dashboard did not become ready. Check $stderr"
    }
    Write-Host "React dashboard ready: $UiUrl"
}

function Start-SystemWorkers {
    if ($SkipWorkers) {
        Write-Host "Skipping indexer/monitor/live-paper startup."
        return
    }

    Write-Host "Starting indexer, monitor, and live paper..."
    Invoke-RestMethod -Method Post -Uri "$ApiUrl/system/start" | Out-Null
    Start-Sleep -Seconds 3
    $status = Invoke-RestMethod -Uri "$ApiUrl/system/status"
    foreach ($name in @("indexer", "monitor", "live_paper")) {
        $component = $status.components.$name
        Write-Host ("{0}: {1}" -f $component.name, $component.status)
    }
}

Start-FastApi
Start-ReactDashboard
Start-SystemWorkers

if (-not $NoBrowser) {
    Start-Process $UiUrl
}

Write-Host ""
Write-Host "PolySignal is ready."
Write-Host "Dashboard: $UiUrl"
Write-Host "API:       $ApiUrl"
