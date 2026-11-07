param(
    [Parameter(Mandatory=$true)][string]$WorkerPath,
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'
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
