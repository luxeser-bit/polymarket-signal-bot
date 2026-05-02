param(
    [string]$ApiUrl = "http://127.0.0.1:8000",
    [string]$DbPath = "",
    [Int64]$JournalSizeLimit = 20000000000,
    [int]$TimeoutSeconds = 900,
    [switch]$SkipStart
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LogDir = Join-Path $Root "data\server_logs"
$LogPath = Join-Path $LogDir "maintenance.log"
$MaintenanceScript = Join-Path $Root "scripts\maintenance.py"

if (-not $DbPath) {
    $DbPath = Join-Path $Root "data\indexer.db"
}

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Write-Log {
    param([string]$Message)
    $line = "{0} {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -Path $LogPath -Value $line
    Write-Host $line
}

function Invoke-Api {
    param(
        [string]$Method,
        [string]$Path,
        [int]$Timeout = 20
    )
    try {
        return Invoke-RestMethod -Method $Method -Uri "$ApiUrl$Path" -TimeoutSec $Timeout
    } catch {
        Write-Log ("API {0} {1} failed: {2}" -f $Method, $Path, $_.Exception.Message)
        return $null
    }
}

function Stop-IndexerFallback {
    $targets = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -like "python*" -and (
            $_.CommandLine -like "*src.indexer*" -or
            $_.CommandLine -like "*polymarket_signal_bot.indexer*"
        )
    }
    foreach ($target in $targets) {
        Write-Log ("Fallback stopping indexer PID {0}" -f $target.ProcessId)
        Stop-Process -Id $target.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

function Start-IndexerFallback {
    $python = (Get-Command python -ErrorAction Stop).Source
    $indexerLog = Join-Path $LogDir "indexer.log"
    $indexerErr = Join-Path $LogDir "indexer.err.log"
    $rpcRps = if ($env:RPC_RPS) { $env:RPC_RPS } else { "25" }
    $chunkSize = if ($env:CHUNK_SIZE) { $env:CHUNK_SIZE } else { "100" }
    $maxWorkers = if ($env:MAX_WORKERS) { $env:MAX_WORKERS } else { "5" }
    Write-Log "Fallback starting indexer process"
    Start-Process `
        -FilePath $python `
        -ArgumentList @(
            "-m", "src.indexer",
            "--sync",
            "--db", $DbPath,
            "--chunk-size", $chunkSize,
            "--max-workers", $maxWorkers,
            "--rpc-rps", $rpcRps,
            "--skip-contract-check"
        ) `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $indexerLog `
        -RedirectStandardError $indexerErr
}

function Wait-IndexerState {
    param(
        [bool]$Running,
        [int]$Seconds = 60
    )
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        $status = Invoke-Api "GET" "/system/status" 10
        if ($status -and $status.components -and $status.components.indexer) {
            $isRunning = [bool]$status.components.indexer.running
            if ($isRunning -eq $Running) {
                return $true
            }
        }
        Start-Sleep -Seconds 2
    }
    return $false
}

function Run-Maintenance {
    $python = (Get-Command python -ErrorAction Stop).Source
    if (-not (Test-Path $MaintenanceScript)) {
        throw "Maintenance script not found: $MaintenanceScript"
    }
    Write-Log "Running SQLite maintenance"
    & $python $MaintenanceScript `
        --db $DbPath `
        --journal-size-limit $JournalSizeLimit `
        --timeout-seconds $TimeoutSeconds 2>&1 |
        ForEach-Object { Write-Log "$_" }
    if ($LASTEXITCODE -ne 0) {
        throw "maintenance.py failed with exit code $LASTEXITCODE"
    }
}

Write-Log "maintenance_cycle_start db=$DbPath limit=$JournalSizeLimit skip_start=$SkipStart"

try {
    $stopped = Invoke-Api "POST" "/system/indexer/stop" 30
    if ($stopped) {
        Write-Log "Requested indexer stop through FastAPI"
    } else {
        Stop-IndexerFallback
    }

    if (-not (Wait-IndexerState -Running:$false -Seconds 90)) {
        Write-Log "Indexer did not report stopped through API; applying fallback stop"
        Stop-IndexerFallback
        Start-Sleep -Seconds 5
    }

    Run-Maintenance

    if (-not $SkipStart) {
        $started = Invoke-Api "POST" "/system/indexer/start" 30
        if ($started) {
            Write-Log "Requested indexer start through FastAPI"
        } else {
            Start-IndexerFallback
        }
        Start-Sleep -Seconds 5
        $metrics = Invoke-Api "GET" "/api/metrics" 15
        if ($metrics) {
            Write-Log ("metrics raw_events={0} last_block={1} running={2}" -f $metrics.raw_events, $metrics.last_block, $metrics.running)
        }
    }

    Write-Log "maintenance_cycle_done"
    exit 0
} catch {
    Write-Log ("maintenance_cycle_failed: {0}" -f $_.Exception.Message)
    if (-not $SkipStart) {
        try {
            Write-Log "Attempting to restart indexer after failure"
            $null = Invoke-Api "POST" "/system/indexer/start" 30
        } catch {
            Start-IndexerFallback
        }
    }
    exit 1
}
