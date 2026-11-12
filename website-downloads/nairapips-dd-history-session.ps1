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
"""Verify real broker sessions and credential history without changing DD thresholds."""
from pathlib import Path
import ast,datetime,os,sys
PARTS = {'old_auth': 'def authenticate(row, master_only=False):\n    # V3.5: bad/stale credentials are isolated per account. They must never\n    # hard-reset the shard terminal or delay valid traders.\n    login=norm(row.get("mt5_login")); server=norm(row.get("mt5_server"))\n    now=time.monotonic()\n    if not master_only and now < auth_quarantine_until.get(login, 0.0):\n        return None\n    if not login.isdigit() or not server:\n        log(f"AUTH PENDING MT5={login or \'UNKNOWN\'} reason=missing_login_or_server | NO DD DECISION","WARNING")\n        return None\n    if master_only:\n        candidates=[]\n        for source,pwd in (("row-master",row_master(row)),("pool-master",pool_master(login))):\n            if pwd and pwd not in [x[1] for x in candidates]: candidates.append((source,pwd))\n    else:\n        candidates=[]\n        inv=row_investor(row)\n        if inv: candidates.append(("row-investor",inv))\n        pinv=pool_investor(login)\n        if pinv and pinv not in [x[1] for x in candidates]: candidates.append(("pool-investor",pinv))\n        rm=row_master(row)\n        if rm and rm not in [x[1] for x in candidates]: candidates.append(("row-master-fallback",rm))\n        pm=pool_master(login)\n        if pm and pm not in [x[1] for x in candidates]: candidates.append(("pool-master-fallback",pm))\n    first_error=None\n    for source,pwd in candidates:\n        ok=mt5.login(login=int(login),password=pwd,server=server)\n        if not ok:\n            if first_error is None: first_error=mt5.last_error()\n            continue\n        info=verified_live_info(login,server,pwd,row)\n        if info is not None:\n            auth_fail_count.pop(login, None)\n            auth_quarantine_until.pop(login, None)\n            return {"info":info,"password":pwd,"source":source,"server":server}\n    if not master_only:\n        failures=auth_fail_count.get(login,0)+1\n        auth_fail_count[login]=failures\n        cooldown=min(AUTH_QUARANTINE_BASE_SECONDS*(2**min(failures-1,3)), AUTH_QUARANTINE_MAX_SECONDS)\n        auth_quarantine_until[login]=time.monotonic()+cooldown\n        log(f"AUTH ISOLATED MT5={login} failures={failures} retry_in={cooldown:.0f}s | first={first_error} final={mt5.last_error()} | NO DD DECISION","WARNING")\n    else:\n        log(f"AUTH PENDING MT5={login} master_only=1 | first={first_error} final={mt5.last_error()} | NO DD DECISION","WARNING")\n    return None\n', 'new_auth': 'def authenticate(row, master_only=False):\n    login=norm(row.get("mt5_login")); server=norm(row.get("mt5_server"))\n    now=time.monotonic()\n    if not master_only and now < auth_quarantine_until.get(login, 0.0):\n        return None\n    if not login.isdigit() or not server:\n        log(f"AUTH PENDING MT5={login or \'UNKNOWN\'} reason=missing_login_or_server | NO DD DECISION","WARNING")\n        return None\n    if master_only:\n        candidates=[]\n        for source,pwd in (("row-master",row_master(row)),("pool-master",pool_master(login))):\n            if pwd and pwd not in [x[1] for x in candidates]: candidates.append((source,pwd))\n    else:\n        candidates=[]\n        inv=row_investor(row)\n        if inv: candidates.append(("row-investor",inv))\n        pinv=pool_investor(login)\n        if pinv and pinv not in [x[1] for x in candidates]: candidates.append(("pool-investor",pinv))\n        rm=row_master(row)\n        if rm and rm not in [x[1] for x in candidates]: candidates.append(("row-master-fallback",rm))\n        pm=pool_master(login)\n        if pm and pm not in [x[1] for x in candidates]: candidates.append(("pool-master-fallback",pm))\n    # NP_VERIFIED_HISTORY_SESSION_20261112: a cached login is not a live session.\n    from plan_risk_controls import daily_percent, broker_daily_realised\n    require_history=not master_only and daily_percent(row)>0\n    def live_session():\n        terminal=mt5.terminal_info();account=mt5.account_info()\n        if (terminal is None or not getattr(terminal,\'connected\',False)\n                or account is None or norm(getattr(account,\'login\',\'\'))!=login\n                or norm(getattr(account,\'server\',\'\')).lower()!=server.lower()):return None\n        # A cached balance with zero/old equity is not a coherent MT5 snapshot.\n        # Genuine zero equity remains valid when the reported floating loss\n        # reconciles it to balance and credit; breach thresholds are unchanged.\n        import math\n        try:\n            balance,equity,profit,credit=(float(getattr(account,k)) for k in (\'balance\',\'equity\',\'profit\',\'credit\'))\n            if not all(math.isfinite(v) for v in (balance,equity,profit,credit)):return None\n            if abs(equity-(balance+profit+credit))>max(.05,abs(equity)*1e-10):return None\n        except (TypeError,ValueError,AttributeError):return None\n        return account\n    preferred=getattr(authenticate,\'_history_sources\',{})\n    cache_key=(login,server)\n    if require_history:\n        candidates.sort(key=lambda candidate:candidate[0]!=preferred.get(cache_key))\n    first_error=None;history_fallback=None\n    for source,pwd in candidates:\n        ok=mt5.login(login=int(login),password=pwd,server=server)\n        if not ok:\n            if first_error is None: first_error=mt5.last_error()\n            continue\n        info=verified_live_info(login,server,pwd,row)\n        if info is None:continue\n        for settle_attempt in range(11):\n            info=live_session()\n            if info is not None:break\n            if settle_attempt<10:time.sleep(.2)\n        if info is None:\n            log(f"AUTH SESSION UNREADY MT5={login} source={source} | cached account data rejected | NO DD DECISION","WARNING")\n            continue\n        if require_history:\n            history_ok=False\n            for attempt in range(3):\n                try:\n                    realised=broker_daily_realised(mt5,row,now_iso())\n                    history_ok=True;break\n                except Exception as exc:\n                    if attempt<2:time.sleep(.2)\n                    else:log(f"AUTH HISTORY UNREADY MT5={login} source={source}: {type(exc).__name__}: {str(exc)[:160]} | trying next credential","WARNING")\n            info=live_session()\n            if info is None:continue\n            if not history_ok:\n                history_fallback={"info":info,"password":pwd,"source":source,"server":server,"daily_history_unavailable":True}\n                continue\n        if require_history:\n            if preferred.get(cache_key)!=source:\n                log(f"AUTH HISTORY VERIFIED MT5={login} source={source} daily_net={realised}")\n            if len(preferred)>=2048:preferred.clear()\n            preferred[cache_key]=source\n            authenticate._history_sources=preferred\n        auth_fail_count.pop(login, None)\n        auth_quarantine_until.pop(login, None)\n        return {"info":info,"password":pwd,"source":source,"server":server}\n    # Preserve overall-DD policing from a connected exact account even when\n    # daily history is unavailable. inspect() still refuses a daily-safe quote\n    # and does not mark a complete valid DD read until its journal succeeds.\n    if history_fallback:\n        info=live_session()\n        if info is not None:\n            history_fallback[\'info\']=info\n            return history_fallback\n    if not master_only:\n        failures=auth_fail_count.get(login,0)+1\n        auth_fail_count[login]=failures\n        cooldown=min(AUTH_QUARANTINE_BASE_SECONDS*(2**min(failures-1,3)), AUTH_QUARANTINE_MAX_SECONDS)\n        auth_quarantine_until[login]=time.monotonic()+cooldown\n        log(f"AUTH ISOLATED MT5={login} failures={failures} retry_in={cooldown:.0f}s | first={first_error} final={mt5.last_error()} | NO DD DECISION","WARNING")\n    else:\n        log(f"AUTH PENDING MT5={login} master_only=1 | first={first_error} final={mt5.last_error()} | NO DD DECISION","WARNING")\n    return None\n\n', 'old_verified': '    verified.add(login); last_ok[login]=time.monotonic()\n', 'old_coverage': '    # V3.13 true coverage timestamp: only a complete, usable live DD observation\n    # counts as protected coverage.\n    last_valid_dd_read[login]=time.monotonic()\n    last_inspected[login]=last_valid_dd_read[login]\n\n', 'anchor': '    # V3.17: publish the same already-computed valid MT5 observation asynchronously.\n', 'old_daily': '            if daily_enabled:\n                log(f"DAILY HISTORY UNAVAILABLE MT5={login}: {type(exc).__name__}; overall breach protection retained", "ERROR")\n\n    if package_account:\n', 'new_daily': '            if daily_enabled:\n                log(f"DAILY HISTORY UNAVAILABLE MT5={login}: {type(exc).__name__}: {str(exc)[:160]}; overall breach protection retained", "ERROR")\n\n    if daily_enabled:\n        # Do not count a just-closed loss both as old floating loss and as new\n        # realised loss. A changed balance needs another coherent inspection.\n        latest=mt5.account_info();terminal=mt5.terminal_info()\n        if (latest is None or terminal is None or not getattr(terminal,\'connected\',False)\n                or norm(getattr(latest,\'login\',\'\'))!=login\n                or norm(getattr(latest,\'server\',\'\')).lower()!=norm(row.get(\'mt5_server\')).lower()):\n            return AUTH_RETRY_SECONDS\n        import math\n        try:\n            new_balance,new_equity,profit,credit=(float(getattr(latest,k)) for k in (\'balance\',\'equity\',\'profit\',\'credit\'))\n            if not all(math.isfinite(v) for v in (new_balance,new_equity,profit,credit)):return AUTH_RETRY_SECONDS\n            if abs(new_equity-(new_balance+profit+credit))>max(.05,abs(new_equity)*1e-10):return AUTH_RETRY_SECONDS\n        except (TypeError,ValueError,AttributeError):return AUTH_RETRY_SECONDS\n        if daily_realised is not None and new_balance!=bal:\n            log(f"DAILY SNAPSHOT CHANGED MT5={login}; retrying without a DD verdict","WARNING")\n            return AUTH_RETRY_SECONDS\n        bal,eq=new_balance,new_equity\n\n    if package_account:\n'}
MARKER='NP_VERIFIED_HISTORY_SESSION_20261112'
def patched(source):
    p=PARTS
    replacement='    # '+MARKER+': coverage follows usable rule evidence.\n'+p['old_verified']+p['old_coverage']+p['anchor']
    if MARKER in source:
        if p['new_auth'] not in source or replacement not in source or p['new_daily'] not in source:raise ValueError('Partial or unknown session repair; no files changed')
        return source
    for value in (p['old_auth'],p['old_verified'],p['old_coverage'],p['anchor'],p['old_daily']):
        if source.count(value)!=1:raise ValueError('Worker source differs from reviewed version; no files changed')
    result=source.replace(p['old_auth'],p['new_auth']).replace(p['old_verified'],'').replace(p['old_coverage'],'').replace(p['anchor'],replacement).replace(p['old_daily'],p['new_daily'])
    ast.parse(result);return result

def install(worker):
    path=Path(worker).resolve();raw=path.read_bytes();source=raw.decode('utf-8-sig').replace('\r\n','\n');result=patched(source)
    if result==source:print('Verified broker-session repair is already installed.');return
    backup=path.with_name(path.name+'.before-history-session-'+datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')+'.bak')
    with backup.open('xb') as f:f.write(raw)
    newline='\r\n' if b'\r\n' in raw else '\n'
    staged=path.with_name(path.name+'.session-staged');staged.write_bytes(result.replace('\n',newline).encode('utf-8'))
    if path.read_bytes()!=raw:staged.unlink();raise ValueError('Worker changed during repair; no replacement made')
    os.replace(staged,path);print('Broker-session and daily-history verification installed. Backup:',backup)
if __name__=='__main__':install(sys.argv[1])

'@
$patchFile = Join-Path $env:TEMP ('np-history-session-' + [Guid]::NewGuid().ToString() + '.py')
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
    Write-Host ('Updating live-session verification on shard ' + $item.Shard + '/4. Other shards continue.')
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
Write-Host 'Live-session and daily-history verification loaded across all four workers.'

Write-Host 'Recent Shobukola session diagnostics:'
Get-Content -LiteralPath 'C:\NairaPips_DD_Fleet\logs\dd_police_shard_02.log' -Tail 1200 | Where-Object { $_ -match '477723574' -and $_ -match 'AUTH HISTORY|AUTH SESSION|ACCOUNT VERIFIED' } | Select-Object -Last 8
Write-Host 'Send this final output so the actual daily-loss reading can be checked.'
