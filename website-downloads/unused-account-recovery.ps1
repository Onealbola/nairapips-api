$ErrorActionPreference = 'Stop'
$workers = @(Get-CimInstance Win32_Process | Where-Object {$_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match 'DD_POLICE_' -and $_.CommandLine -match '--shard\s+1(\s|$)'})
if ($workers.Count -ne 1) { throw 'Exactly one worker 1 must be running. Nothing changed.' }
$worker = $workers[0]
$supervisor = Get-CimInstance Win32_Process -Filter "ProcessId=$($worker.ParentProcessId)"
if ($supervisor.CommandLine -notmatch 'DD_FLEET_SUPERVISOR') { throw 'Supervisor not confirmed. Nothing changed.' }
$paths = @([regex]::Matches($worker.CommandLine,'"([^"\r\n]+\.py)"|([A-Za-z]:\\[^\s"]+\.py)') | ForEach-Object {if ($_.Groups[1].Success) {$_.Groups[1].Value} else {$_.Groups[2].Value}} | Where-Object {(Split-Path -Leaf $_) -eq 'DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_20261008.py'})
if ($paths.Count -ne 1) { throw 'Worker source path not confirmed. Nothing changed.' }
$target = $paths[0]
$folder = Split-Path -Parent $target
$module = Join-Path $folder 'unused_account_recovery.py'
$tempModule = Join-Path $env:TEMP ('np-unused-' + [guid]::NewGuid() + '.py')
Invoke-WebRequest 'https://nairapips-api.onrender.com/downloads/unused-account-recovery.py' -UseBasicParsing -OutFile $tempModule
if ((Get-FileHash $tempModule -Algorithm SHA256).Hash.ToLowerInvariant() -ne 'ca707a6fcddba37ce2330a98d109c4eff934dda0edaacc92ed008e05c99fa1cc') { throw 'Recovery module checksum failed. Nothing changed.' }
$source = [IO.File]::ReadAllText($target)
$hook = "from unused_account_recovery import install as _np_unused_recovery`n_np_unused_recovery(globals())`n"
if (-not $source.Contains('_np_unused_recovery(globals())')) {
    $pattern = '(?m)^if __name__\s*==\s*["'']__main__["'']\s*:'
    if ([regex]::Matches($source,$pattern).Count -ne 1) { throw 'Worker entry point not unique. Nothing changed.' }
    foreach ($name in @('def verified_live_info(', 'from equity_drawdown import EquityDrawdownJournal')) {
        if (-not $source.Contains($name)) { throw 'Worker risk functions not confirmed. Nothing changed.' }
    }
    $source = [regex]::Replace($source,$pattern,($hook+'$0'))
}
$staged = $target + '.unused-recovery-staged'
[IO.File]::WriteAllText($staged,$source,(New-Object Text.UTF8Encoding($false)))
& $worker.ExecutablePath -E -c 'import ast,pathlib,sys; [ast.parse(pathlib.Path(p).read_bytes()) for p in sys.argv[1:]]; import MetaTrader5,requests; print(True)' $staged $tempModule
if ($LASTEXITCODE -ne 0) { throw 'Source verification failed. Original worker unchanged.' }
$backup = $target + '.before-unused-recovery-' + (Get-Date -Format yyyyMMddHHmmss) + '.bak'
Copy-Item $target $backup
if (Test-Path $module) { Copy-Item $module ($module+'.backup-'+(Get-Date -Format yyyyMMddHHmmss)) }
Copy-Item $tempModule $module -Force
Move-Item $staged $target -Force
$current = Get-CimInstance Win32_Process -Filter "ProcessId=$($worker.ProcessId)"
if (-not $current -or $current.CreationDate -ne $worker.CreationDate -or $current.CommandLine -ne $worker.CommandLine) { throw 'Worker changed during repair. Files installed; no process stopped. Send this result.' }
Stop-Process -Id $worker.ProcessId
Write-Host 'Verified-unused-account recovery installed. Supervisor will restart worker 1 only.'
Write-Host 'No journal, breach latch, trade, rule or credential was edited by this installer.'
Write-Host ('Original worker backup: ' + $backup)
Write-Host 'After one minute, read dd_police_shard_01.log and send the final output.'
