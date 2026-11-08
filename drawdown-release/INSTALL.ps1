param(
    [string]$WorkerPath = '',
    [switch]$Apply,
    [switch]$RollingRestart
)
$ErrorActionPreference = 'Stop'
$expectedWorker = 'DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_20261008.py'
if (-not $WorkerPath) {
    $found = @()
    $processes = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'"
    foreach ($process in $processes) {
        foreach ($match in [regex]::Matches([string]$process.CommandLine, '"([^"\r\n]+\.py)"|([A-Za-z]:\\[^\s"]+\.py)')) {
            $candidate = if ($match.Groups[1].Success) { $match.Groups[1].Value } else { $match.Groups[2].Value }
            if ((Split-Path -Leaf $candidate) -eq $expectedWorker -and (Test-Path -LiteralPath $candidate)) {
                $found += (Resolve-Path -LiteralPath $candidate).Path
            }
        }
    }
    $found = @($found | Sort-Object -Unique)
    if ($found.Count -gt 1) { throw 'Multiple DD worker locations are running. No files changed; resolve the duplicate fleet first.' }
    if ($found.Count -eq 1) {
        $WorkerPath = $found[0]
        Write-Host 'Located the worker from the running DD process.'
    } else {
        $known = Join-Path 'C:\Users\Administrator\Documents\nairapips_mt5_rotation_engine (1)' $expectedWorker
        if (-not (Test-Path -LiteralPath $known)) { throw 'Could not confirm the DD worker location. No files changed. Supply -WorkerPath explicitly after checking the VPS.' }
        $WorkerPath = $known
        Write-Host 'Found the worker in the recorded setup folder; no running process location was detected.'
        if ($Apply) { throw 'Automatic installation requires a detected running worker. Run the check first and supply the confirmed -WorkerPath explicitly.' }
    }
}
$target = (Resolve-Path -LiteralPath $WorkerPath).Path
if ([IO.Path]::GetExtension($target) -ne '.py') { throw 'Select the existing DD Python worker file.' }
$current = [IO.File]::ReadAllText($target)
if (-not ($current.Contains('NAIRAPIPS_DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_2026_10_08') -or $current.Contains('NAIRAPIPS_DD_POLICE_STATIC_EQUITY_TRAILING_V1_20261107') -or $current.Contains('NAIRAPIPS_DD_POLICE_STATIC_TRAILING_DAILY_V1_20261108'))) {
    throw 'This installer expects the existing V3.20 DD worker. A different version needs review; no files changed.'
}
$destination = Split-Path -Parent $target
$files = @('equity_drawdown.py','plan_risk_controls.py','withdrawal_controls.py','package_dd_runtime.py','package_account_rules.py','package_contracts.py','DD_BREACH_RELAY_V1_20261003.py','fleet_instance_lock.py')
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'SHA256.json') -Raw | ConvertFrom-Json
foreach ($item in @('DD_POLICE_STATIC_TRAILING_V1_20261107.py') + $files) {
    $source = Join-Path $PSScriptRoot $item
    $expected = $manifest.$item
    if (-not $expected -or (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) {
        throw "Checksum verification failed: $item"
    }
}
if ($RollingRestart) {
    $owned = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and ([string]$_.CommandLine).Contains($target) })
    if ($owned.Count -ne 4) { throw 'Rolling update requires exactly four running workers at this exact path. No files changed.' }
    $parents = @($owned | Select-Object -ExpandProperty ParentProcessId -Unique)
    if ($parents.Count -ne 1) { throw 'Workers do not share one supervisor. No files changed.' }
    $supervisor = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $parents[0])
    $supervisorPath = Join-Path $destination 'NAIRAPIPS_DD_FLEET_SUPERVISOR_V3_20_CLEAN_LIVE_STATE_20261008.py'
    if (-not $supervisor -or -not ([string]$supervisor.CommandLine).Contains($supervisorPath)) { throw 'Exact DD supervisor identity not confirmed. No files changed.' }
    $roll = @()
    foreach ($worker in $owned) {
        $match = [regex]::Match([string]$worker.CommandLine,'--shard\s+([1-4])(?:\s|$)')
        if (-not $match.Success -or ([string]$worker.CommandLine) -notmatch '--total-shards\s+4(?:\s|$)') { throw 'Four-shard arguments not confirmed. No files changed.' }
        $roll += [PSCustomObject]@{Shard=[int]$match.Groups[1].Value; Process=$worker}
    }
    if (@($roll | Select-Object -ExpandProperty Shard -Unique).Count -ne 4) { throw 'Duplicate shard identity. No files changed.' }
}
foreach ($name in @('DD_POLICE_STATIC_TRAILING_V1_20261107.py') + $files) {
    & python -c "import ast,pathlib,sys; ast.parse(pathlib.Path(sys.argv[1]).read_bytes())" (Join-Path $PSScriptRoot $name)
    if ($LASTEXITCODE -ne 0) { throw 'Source syntax check failed. No files changed.' }
}
& python -c "import requests, MetaTrader5 as m; assert callable(getattr(m,'initialize',None)); print('Python and MT5 dependencies verified; no broker session started.')"
if ($LASTEXITCODE -ne 0) { throw 'Dependency check failed. No files changed.' }
Write-Host "Existing worker: $target"
Write-Host 'Proposed: back up worker/modules, install Static + Equity Trailing; preserve all state folders.'
if (-not $Apply) { Write-Host 'CHECK ONLY. No files changed. Add -Apply after reviewing this path.'; exit 0 }
$backup = Join-Path $destination ('backup-static-trailing-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $backup | Out-Null
Copy-Item -LiteralPath $target -Destination $backup
foreach ($item in $files) {
    $old = Join-Path $destination $item
    if (Test-Path -LiteralPath $old) { Copy-Item -LiteralPath $old -Destination $backup }
}
foreach ($item in $files) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $item) -Destination (Join-Path $destination $item) -Force
}
$temp = $target + '.staged'
Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'DD_POLICE_STATIC_TRAILING_V1_20261107.py') -Destination $temp
Move-Item -LiteralPath $temp -Destination $target -Force
Write-Host "Installed. Backup: $backup"
if (-not $RollingRestart) { Write-Host 'Restart the existing DD supervisor, then verify every shard. Do not start a duplicate supervisor.' }
Write-Host 'Purchases remain controlled by the backend verification setting and each package switch.'

if ($RollingRestart) {
    foreach ($item in ($roll | Sort-Object Shard)) {
        $before = $item.Process
        $live = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $before.ProcessId)
        if (-not $live -or $live.CreationDate -ne $before.CreationDate -or $live.CommandLine -ne $before.CommandLine) { throw 'Worker identity changed. Remaining workers were not stopped.' }
        $stamp = [DateTime]::UtcNow
        Write-Host ('Rolling shard ' + $item.Shard + '/4; other shards and Operations remain running.')
        Stop-Process -Id $live.ProcessId -Force -ErrorAction Stop
        $ready = $false
        for ($attempt=0; $attempt -lt 90; $attempt++) {
            Start-Sleep -Seconds 2
            try {
                $health = Get-Content -LiteralPath ('C:\NairaPips_DD_Fleet\state\dd_health_shard_{0:00}.json' -f $item.Shard) -Raw | ConvertFrom-Json
                if ($health.release -eq 'NAIRAPIPS_DD_POLICE_STATIC_TRAILING_DAILY_V1_20261108' -and $health.daily_history_probe_ready -eq $true -and $health.monitor_ready -gt 0 -and ([DateTime]::Parse($health.timestamp).ToUniversalTime() -ge $stamp)) { $ready=$true; break }
            } catch { }
            if ($attempt % 10 -eq 0) { Write-Host ('Waiting for shard ' + $item.Shard + ' broker-history probe...') }
        }
        if (-not $ready) { throw ('Shard ' + $item.Shard + ' has not verified its new health. Rollout paused; inspect this output. Backup: ' + $backup) }
        Write-Host ('Shard ' + $item.Shard + ' verified.')
    }
    $relayPath = Join-Path $destination 'DD_BREACH_RELAY_V1_20261003.py'
    $relays = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and $_.ParentProcessId -eq $supervisor.ProcessId -and ([string]$_.CommandLine).Contains($relayPath) })
    if ($relays.Count -ne 1) { throw 'Exact relay identity is not unique; no relay was stopped.' }
    $relay = $relays[0]
    $live = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $relay.ProcessId)
    if (-not $live -or $live.CreationDate -ne $relay.CreationDate -or $live.CommandLine -ne $relay.CommandLine) { throw 'Relay identity changed; no relay was stopped.' }
    Stop-Process -Id $live.ProcessId -Force -ErrorAction Stop
    Write-Host 'Signed breach relay restarting under its existing supervisor; durable outbox and offsets preserved.'
    $relayReady = $false
    for ($attempt=0; $attempt -lt 30; $attempt++) {
        Start-Sleep -Seconds 2
        $next = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and $_.ParentProcessId -eq $supervisor.ProcessId -and $_.ProcessId -ne $relay.ProcessId -and ([string]$_.CommandLine).Contains($relayPath) })
        if ($next.Count -eq 1) { $relayReady=$true; break }
    }
    if (-not $relayReady) { throw 'Updated relay process has not appeared. Inspect supervisor output.' }
    Write-Host 'ROLLING UPDATE COMPLETE. Four broker-history probes verified. No purchase setting was changed.'
}
