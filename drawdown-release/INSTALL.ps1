param(
    [string]$WorkerPath = '',
    [switch]$Apply
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
if (-not $current.Contains('NAIRAPIPS_DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_2026_10_08')) {
    throw 'This installer expects the existing V3.20 DD worker. A different version needs review; no files changed.'
}
$destination = Split-Path -Parent $target
$files = @('equity_drawdown.py','package_dd_runtime.py','package_account_rules.py','package_contracts.py')
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'SHA256.json') -Raw | ConvertFrom-Json
foreach ($item in @('DD_POLICE_STATIC_TRAILING_V1_20261107.py') + $files) {
    $source = Join-Path $PSScriptRoot $item
    $expected = $manifest.$item
    if (-not $expected -or (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $expected) {
        throw "Checksum verification failed: $item"
    }
}
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
Write-Host 'Restart the existing DD supervisor, then verify every shard. Do not start a duplicate supervisor.'
Write-Host 'Purchases remain controlled by the backend verification setting and each package switch.'
