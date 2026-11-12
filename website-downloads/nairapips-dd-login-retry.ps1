param([string]$WorkerPath = '')
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

$owned = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and ([string]$_.CommandLine).Contains($target) })
if ($owned.Count -ne 4) { throw 'Expected exactly four workers at this path. Nothing changed.' }
$parents = @($owned | Select-Object -ExpandProperty ParentProcessId -Unique)
if ($parents.Count -ne 1) { throw 'The four workers must share one supervisor. Nothing changed.' }
$supervisor = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $parents[0])
$supervisorPath = Join-Path (Split-Path -Parent $target) 'NAIRAPIPS_DD_FLEET_SUPERVISOR_V3_20_CLEAN_LIVE_STATE_20261008.py'
if (-not $supervisor -or -not ([string]$supervisor.CommandLine).Contains($supervisorPath)) { throw 'Existing supervisor not confirmed. Nothing changed.' }
$roll = @()
foreach ($worker in $owned) {
    $match = [regex]::Match([string]$worker.CommandLine,'--shard\s+([1-4])(?:\s|$)')
    if (-not $match.Success -or ([string]$worker.CommandLine) -notmatch '--total-shards\s+4(?:\s|$)') { throw 'Worker shard arguments not confirmed. Nothing changed.' }
    $roll += [PSCustomObject]@{Shard=[int]$match.Groups[1].Value; Process=$worker}
}
if (@($roll | Select-Object -ExpandProperty Shard -Unique).Count -ne 4) { throw 'Duplicate shards detected. Nothing changed.' }
$pythonExe = $owned[0].ExecutablePath
if (-not $pythonExe -or -not (Test-Path -LiteralPath $pythonExe)) { throw 'Worker Python executable not confirmed. Nothing changed.' }
$patcher = @'
"""Surgical failed-login starvation repair. Leaves all DD/risk and saved state intact."""
from pathlib import Path
import ast,sys,datetime,os
MARKER='# NP_BOUNDED_LOGIN_RETRY_20261112'
OLD='''            for x in rows:
                if x not in verified or x in invalid_live_capital:
                    continue
                last_good=last_valid_dd_read.get(x)
'''
NEW='''            for x in rows:
                # NP_BOUNDED_LOGIN_RETRY_20261112
                # An initial failed login must not remain behind always-due
                # verified accounts. Existing rescue/risk alternation is retained.
                if x not in verified:
                    if (x in first_attempted and next_due.get(x,0)<=now
                            and auth_quarantine_until.get(x,0)<=now
                            and now-last_attempted_at.get(x,now)>=120.0):
                        coverage_overdue.append(x)
                    continue
                if x in invalid_live_capital:
                    continue
                last_good=last_valid_dd_read.get(x)
'''
OLD_SORT='''key=lambda x: last_valid_dd_read.get(x,0.0)'''
NEW_SORT='''key=lambda x: (last_valid_dd_read.get(x,0.0) if x in verified else last_attempted_at.get(x,0.0))'''
def patched(source):
 if MARKER in source:
  if NEW not in source or NEW_SORT not in source:raise ValueError('Incomplete or changed retry repair; review required')
  return source
 if source.count(OLD)!=1 or source.count(OLD_SORT)!=1:raise ValueError('Unknown scheduler version; no files changed')
 result=source.replace(OLD,NEW).replace(OLD_SORT,NEW_SORT)
 ast.parse(result);return result

def install(path):
 path=Path(path);raw=path.read_bytes();source=raw.decode('utf-8-sig').replace('\r\n','\n');result=patched(source)
 if result==source:print('Retry repair is already installed; source unchanged.');return
 backup=path.with_name(path.name+'.before-login-retry-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.bak')
 with backup.open('xb') as f:f.write(raw)
 newline='\r\n' if b'\r\n' in raw else '\n'
 staged=path.with_name(path.name+'.retry-staged');staged.write_bytes(result.replace('\n',newline).encode('utf-8'))
 if path.read_bytes()!=raw:staged.unlink();raise ValueError('Worker changed during repair; no replacement made')
 os.replace(staged,path);print('Retry repair installed. Backup:',backup)
if __name__=='__main__':install(sys.argv[1])

'@
$patchFile = Join-Path $env:TEMP ('np-retry-patch-' + [Guid]::NewGuid().ToString() + '.py')
try {
    [IO.File]::WriteAllText($patchFile,$patcher,(New-Object Text.UTF8Encoding($false)))
    & $pythonExe $patchFile $target
    if ($LASTEXITCODE -ne 0) { throw 'Patch did not pass exact-source validation. No worker was stopped.' }
} finally {
    Remove-Item -LiteralPath $patchFile -ErrorAction SilentlyContinue
}
# Existing supervisor restarts one Python worker at a time. MT5 terminals,
# Operations, orders, credentials, risk journals and breach latches stay intact.
foreach ($item in ($roll | Sort-Object @{Expression={if ($_.Shard -eq 2) {0} else {$_.Shard}}})) {
    $before = $item.Process
    $live = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $before.ProcessId)
    $parent = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $supervisor.ProcessId)
    if (-not $parent -or $parent.CreationDate -ne $supervisor.CreationDate -or -not $live -or $live.CreationDate -ne $before.CreationDate -or $live.CommandLine -ne $before.CommandLine) { throw 'Process identity changed; rollout paused.' }
    $stamp = [DateTime]::UtcNow
    Write-Host ('Updating retry scheduling on shard ' + $item.Shard + '/4. Other shards continue.')
    Stop-Process -Id $live.ProcessId -Force -ErrorAction Stop
    $ready = $false
    for ($attempt=0; $attempt -lt 90; $attempt++) {
        Start-Sleep -Seconds 2
        $next = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and $_.ParentProcessId -eq $supervisor.ProcessId -and $_.ProcessId -ne $before.ProcessId -and ([string]$_.CommandLine).Contains($target) -and ([string]$_.CommandLine) -match ('--shard\s+' + $item.Shard + '(?:\s|$)') })
        try {
            $health = Get-Content -LiteralPath ('C:\NairaPips_DD_Fleet\state\dd_health_shard_{0:00}.json' -f $item.Shard) -Raw | ConvertFrom-Json
            if ($next.Count -eq 1 -and $health.monitor_ready -gt 0 -and ([DateTime]::Parse($health.timestamp).ToUniversalTime() -ge $stamp)) { $ready=$true; break }
        } catch { }
        if ($attempt % 10 -eq 0) { Write-Host ('Waiting for shard ' + $item.Shard + ' to verify accounts...') }
    }
    if (-not $ready) { throw ('Shard ' + $item.Shard + ' has not verified fresh health. Other shards were not stopped; send this output.') }
    Write-Host ('Shard ' + $item.Shard + ' resumed verified monitoring.')
}
Write-Host 'Retry scheduling repair installed across all four workers. Send this result so live dashboard updates can be verified.'
