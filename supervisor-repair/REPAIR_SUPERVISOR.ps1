$ErrorActionPreference = 'Stop'
$Engine = 'C:\Users\Administrator\Documents\nairapips_mt5_rotation_engine (1)'
$Supervisor = 'NAIRAPIPS_DD_FLEET_SUPERVISOR_V3_20_CLEAN_LIVE_STATE_20261008.py'
$Helpers = @('DD_ROSTER_WATCHER_V1_20261003.py','DD_BREACH_RELAY_V1_20261003.py')
$Files = @($Supervisor,'fleet_instance_lock.py') + $Helpers
$Engine = (Resolve-Path -LiteralPath $Engine).Path
if (-not (Test-Path -LiteralPath (Join-Path $Engine 'DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_20261008.py'))) {
    throw 'Installed DD worker not found in the confirmed engine folder. No files changed.'
}
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'SHA256.json') -Raw | ConvertFrom-Json
foreach ($name in $Files) {
    $source = Join-Path $PSScriptRoot $name
    if (-not $manifest.$name -or (Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -ne $manifest.$name) {
        throw "Package checksum failed: $name. No files changed."
    }
}
& python -c "import encodings, requests, MetaTrader5; print('Python dependencies verified')"
if ($LASTEXITCODE -ne 0) { throw 'Python dependency check failed. No files changed.' }
& python -c "import ast,pathlib,sys; p=pathlib.Path(sys.argv[1]); t=ast.parse(p.read_text(encoding='utf-8-sig')); assert any(isinstance(n,ast.FunctionDef) and n.name=='main' for n in t.body); assert any(isinstance(n,ast.If) and '__name__' in ast.unparse(n.test) for n in t.body); print('Replacement supervisor entry point verified')" (Join-Path $PSScriptRoot $Supervisor)
if ($LASTEXITCODE -ne 0) { throw 'Supervisor source check failed. No files changed.' }
$PythonProcesses = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' })
$ActiveDD = @($PythonProcesses | Where-Object { $_.CommandLine -match 'DD_FLEET_SUPERVISOR|DD_POLICE_' })
if ($ActiveDD.Count -gt 0) {
    Write-Host 'Existing DD processes found. Repair blocked to prevent a duplicate fleet:'
    $ActiveDD | Select-Object ProcessId,ParentProcessId | Format-Table
    throw 'Stop the existing DD fleet gracefully first. No files changed; no process was stopped.'
}
# Match ONLY these two helper filenames with an absolute path in this engine folder.
# No generic Python termination and no Operations/MT5/terminal process matching.
$OwnedHelpers = @()
foreach ($process in $PythonProcesses) {
    foreach ($match in [regex]::Matches([string]$process.CommandLine, '"([^"\r\n]+\.py)"|([A-Za-z]:\\[^\s"]+\.py)')) {
        $script = if ($match.Groups[1].Success) { $match.Groups[1].Value } else { $match.Groups[2].Value }
        if ($Helpers -contains (Split-Path -Leaf $script)) {
            if ((Split-Path -Parent $script).TrimEnd('\') -ine $Engine.TrimEnd('\')) {
                throw 'A DD helper belongs to a different folder. Manual review needed; no files changed.'
            }
            $OwnedHelpers += $process
        }
    }
}
$backup = Join-Path $Engine ('backup-dd-supervisor-' + (Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $backup | Out-Null
$Existed = @{}
foreach ($name in $Files) {
    $target = Join-Path $Engine $name
    $Existed[$name] = Test-Path -LiteralPath $target
    if ($Existed[$name]) { Copy-Item -LiteralPath $target -Destination (Join-Path $backup $name) }
}
try {
    foreach ($name in $Files) {
        $target = Join-Path $Engine $name
        $staged = $target + '.repair-staged'
        Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination $staged -Force
        Move-Item -LiteralPath $staged -Destination $target -Force
        if ((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() -ne $manifest.$name) {
            throw "Installed checksum mismatch: $name"
        }
    }
} catch {
    foreach ($name in $Files) {
        $target = Join-Path $Engine $name
        if ($Existed[$name]) { Copy-Item -LiteralPath (Join-Path $backup $name) -Destination $target -Force }
        elseif (Test-Path -LiteralPath $target) { Remove-Item -LiteralPath $target }
    }
    throw 'Repair copy failed; original program files restored. No state folders were changed.'
}
# With the DD supervisor/workers confirmed absent, retire only the exact old DD
# helpers so the repaired supervisor can own one relay and one roster watcher.
foreach ($process in $OwnedHelpers) {
    $current = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $process.ProcessId)
    if ($current -and $current.CreationDate -eq $process.CreationDate -and $current.CommandLine -eq $process.CommandLine) {
        Stop-Process -Id $current.ProcessId -Force -ErrorAction Stop
        Write-Host ('Stopped old DD helper PID ' + $current.ProcessId + ' for supervisor handoff.')
    }
}
Write-Host 'REPAIR COMPLETE. Full supervisor, helper programs and duplicate-instance locks installed.'
Write-Host ('Backup: ' + $backup)
Write-Host 'The installed DD worker, trading rules, state folders, Operations Engine and MT5 terminals were not replaced.'
Write-Host 'No new supervisor was started automatically.'
Write-Host 'Return to your existing black command window and run:'
Write-Host ('python "' + (Join-Path $Engine $Supervisor) + '"')
