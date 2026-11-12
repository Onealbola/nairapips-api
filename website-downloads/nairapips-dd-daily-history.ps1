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
"""Preserve frozen rules and journals; replace only the verified daily-history reader."""
from pathlib import Path
import ast,datetime,os,sys
OLD = 'def broker_daily_realised(mt5, row, observed_at):\n    """Trading net P/L since WAT midnight; deposits and withdrawals are excluded."""\n    observed = parse_time(observed_at)\n    start = observed.astimezone(WAT).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)\n    assigned = row.get(\'account_started_at\') or row.get(\'started_at\') or row.get(\'created_at\')\n    if assigned:\n        start = max(start, parse_time(assigned))\n    if start > observed:\n        raise ValueError(\'Account assignment is in the future\')\n    expected = str(row.get(\'mt5_login\'))\n    info = mt5.account_info()\n    if info is None or str(info.login) != expected:\n        raise ValueError(\'Daily history account identity mismatch\')\n    deals = mt5.history_deals_get(start, observed)\n    if deals is None:\n        raise ValueError(\'Daily broker trading history unavailable\')\n    total = Decimal(0)\n    for deal in deals:\n        if getattr(deal, \'type\', None) not in (0, 1):\n            continue\n        for field in (\'profit\', \'commission\', \'swap\', \'fee\'):\n            value = getattr(deal, field, 0) or 0\n            if isinstance(value, bool):\n                raise ValueError(\'Invalid daily trading evidence\')\n            amount = Decimal(str(value))\n            if not amount.is_finite():\n                raise ValueError(\'Invalid daily trading evidence\')\n            total += amount\n    info = mt5.account_info()\n    if info is None or str(info.login) != expected:\n        raise ValueError(\'Terminal account changed during daily history read\')\n    return total\n'
NEW = 'def broker_daily_realised(mt5, row, observed_at):\n    """Verify the assigned account\'s balance ledger before accepting daily P/L.\n\n    An empty/stale history after account switching is not evidence of zero loss.\n    Reconcile all post-assignment balance entries, then select this WAT day.\n    Transfers affect the balance proof but are excluded from trading P/L.\n    """\n    observed = parse_time(observed_at)\n    day_start = observed.astimezone(WAT).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)\n    assigned = row.get(\'account_started_at\') or row.get(\'started_at\') or row.get(\'created_at\')\n    if not assigned:\n        raise ValueError(\'Exact account assignment time is required for daily history\')\n    start = parse_time(assigned)\n    if start > observed:\n        raise ValueError(\'Account assignment is in the future\')\n    expected = str(row.get(\'mt5_login\'))\n    def money(value):\n        if isinstance(value,bool):raise ValueError(\'Invalid broker ledger amount\')\n        amount=Decimal(str(value))\n        if not amount.is_finite():raise ValueError(\'Invalid broker ledger amount\')\n        return amount\n    capital=money(row.get(\'start_balance\') or row.get(\'account_size\') or 0)\n    if capital<=0:raise ValueError(\'Exact initial capital is required for daily history\')\n    info = mt5.account_info()\n    if info is None or str(info.login) != expected:\n        raise ValueError(\'Daily history account identity mismatch\')\n    before=money(info.balance)\n    deals = mt5.history_deals_get(start, observed)\n    if deals is None:\n        raise ValueError(\'Daily broker trading history unavailable\')\n    ledger=Decimal(0);total=Decimal(0);seen=set()\n    for deal in deals:\n        ticket=getattr(deal,\'ticket\',None)\n        if ticket is None or ticket in seen:raise ValueError(\'Broker deal identity missing or duplicated\')\n        seen.add(ticket)\n        kind=getattr(deal,\'type\',None)\n        if isinstance(kind,bool) or not isinstance(kind,int):raise ValueError(\'Broker deal type missing\')\n        raw_time=getattr(deal,\'time_msc\',0)\n        raw_time=money(raw_time)/1000 if raw_time else money(getattr(deal,\'time\',0))\n        when=datetime.fromtimestamp(float(raw_time),timezone.utc)\n        # MT5 ranges have second precision while assignment/observation timestamps\n        # can include fractions. Boundary deals belong to this same lifecycle.\n        if when < start.replace(microsecond=0) or when > observed:\n            raise ValueError(\'Broker deal lies outside the verified history window\')\n        net=sum((money(getattr(deal,field,0) or 0) for field in (\'profit\',\'commission\',\'swap\',\'fee\')),Decimal(0))\n        if kind!=3:ledger+=net # MT5 CREDIT changes account credit, not balance.\n        if kind in (0,1) and when>=day_start:total+=net\n    info = mt5.account_info()\n    if info is None or str(info.login) != expected:\n        raise ValueError(\'Terminal account changed during daily history read\')\n    after=money(info.balance)\n    if abs(after-before)>Decimal(\'.01\'):\n        raise ValueError(\'Broker balance changed during daily history read; retry required\')\n    if abs(capital+ledger-after)>Decimal(\'.01\'):\n        raise ValueError(\'Broker history does not reconcile to the assigned capital and current balance\')\n    return total'
def patched(source):
    start=source.index('def broker_daily_realised(')
    end=source.index('\n\n_ready_cache',start)
    current=source[start:end]
    if current==NEW:return source
    if current!=OLD:raise ValueError('Unknown daily-history reader; no files changed')
    result=source[:start]+NEW+source[end:];ast.parse(result);return result

def install(worker):
    path=Path(worker).resolve().parent/'plan_risk_controls.py'
    raw=path.read_bytes();source=raw.decode('utf-8-sig').replace('\r\n','\n');result=patched(source)
    if result==source:print('Verified daily-history repair already installed.');return
    backup=path.with_name(path.name+'.before-daily-ledger-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.bak')
    with backup.open('xb') as f:f.write(raw)
    newline='\r\n' if b'\r\n' in raw else '\n'
    staged=path.with_name(path.name+'.ledger-staged');staged.write_bytes(result.replace('\n',newline).encode('utf-8'))
    if path.read_bytes()!=raw:staged.unlink();raise ValueError('Daily helper changed during repair; no replacement made')
    os.replace(staged,path);print('Daily-history reconciliation installed. Backup:',backup)
if __name__=='__main__':install(sys.argv[1])

'@
$patchFile = Join-Path $env:TEMP ('np-daily-history-' + [Guid]::NewGuid().ToString() + '.py')
try {
    [IO.File]::WriteAllText($patchFile,$patcher,(New-Object Text.UTF8Encoding($false)))
    & $pythonExe $patchFile $target
    if ($LASTEXITCODE -ne 0) { throw 'Daily-history reader did not pass exact-source validation. No worker was stopped.' }
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
    Write-Host ('Updating verified daily history on shard ' + $item.Shard + '/4. Other shards continue.')
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
Write-Host 'Daily-history repair loaded across all four workers. Send this result so the daily rule and account evidence can be verified.'
