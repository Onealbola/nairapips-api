$ErrorActionPreference = 'Stop'
$workers = @(Get-CimInstance Win32_Process | Where-Object { $_.Name -match '^python(w)?\.exe$' -and ([string]$_.CommandLine).Contains('DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_20261008.py') -and ([string]$_.CommandLine) -match '--shard\s+2(?:\s|$)' })
if ($workers.Count -ne 1) { throw 'Exact shard 2 Python process is not unique; no diagnostic connection opened.' }
$worker = $workers[0]
$paths = @([regex]::Matches([string]$worker.CommandLine, '"([^"\r\n]+\.py)"|([A-Za-z]:\\[^\s"]+\.py)') | ForEach-Object { if ($_.Groups[1].Success) {$_.Groups[1].Value} else {$_.Groups[2].Value} } | Where-Object { (Split-Path -Leaf $_) -eq 'DD_POLICE_V3_20_CLEAN_LIVE_STATE_TELEMETRY_20261008.py' })
if ($paths.Count -ne 1) { throw 'Worker file path not confirmed.' }
$arg = [regex]::Match([string]$worker.CommandLine,'--terminal-path\s+(?:"([^"]+)"|(\S+))')
if (-not $arg.Success) { throw 'Worker terminal path not confirmed.' }
$terminal = if ($arg.Groups[1].Success) {$arg.Groups[1].Value} else {$arg.Groups[2].Value}
$terminals = @(Get-CimInstance Win32_Process -Filter "Name='terminal64.exe'" | Where-Object { $_.ExecutablePath -eq $terminal })
if ($terminals.Count -ne 1) { throw 'The exact existing terminal is not running; nothing was started or stopped.' }
$code = @'
"""Read-only MT5 daily-history audit; no login, order, rule or journal changes."""
import sys,json,time,importlib.util
from pathlib import Path
from datetime import datetime,timezone,timedelta
from decimal import Decimal

def amount(deal):
 return sum((Decimal(str(getattr(deal,k,0) or 0)) for k in ('profit','commission','swap','fee')),Decimal(0))
def stamp(deal):
 return datetime.fromtimestamp(float(getattr(deal,'time_msc',0) or getattr(deal,'time',0)*1000)/1000,timezone.utc)
def summarise(deals,observed,assigned):
 if deals is None:return {'available':False}
 day=observed.astimezone(timezone(timedelta(hours=1))).replace(hour=0,minute=0,second=0,microsecond=0).astimezone(timezone.utc)
 ledger=sum((amount(d) for d in deals if getattr(d,'type',None)!=3),Decimal(0))
 trading=sum((amount(d) for d in deals if getattr(d,'type',None) in (0,1) and max(day,assigned)<=stamp(d)<=observed),Decimal(0))
 funds=[{'ticket':str(getattr(d,'ticket','')),'at':stamp(d).isoformat(),'type':getattr(d,'type',None),'net':str(amount(d)),'before_assignment':stamp(d)<assigned} for d in deals if getattr(d,'type',None) not in (0,1,3)]
 return {'available':True,'deals':len(deals),'balance_ledger_net':str(ledger),'today_trading_net':str(trading),'funding_sample':funds[:6]}

def main():
 import MetaTrader5 as mt5
 worker=Path(sys.argv[1]);terminal=sys.argv[2];snapshot=Path(sys.argv[3]);login=sys.argv[4];output=Path(sys.argv[5])
 rows=json.loads(snapshot.read_text(encoding='utf-8-sig')).get('accounts',[])
 matches=[r for r in rows if str(r.get('mt5_login'))==login]
 if len(matches)!=1:raise ValueError('Exact account is not unique in the local roster')
 row=matches[0];assigned=datetime.fromisoformat(str(row['account_started_at']).replace('Z','+00:00'))
 if assigned.tzinfo is None:raise ValueError('Assignment timezone is missing')
 sys.path.insert(0,str(worker.parent));spec=importlib.util.spec_from_file_location('daily_audit_controls',worker.parent/'plan_risk_controls.py');controls=importlib.util.module_from_spec(spec);spec.loader.exec_module(controls)
 if not mt5.initialize(path=terminal,timeout=5000):raise ValueError('Could not attach to the already-running terminal')
 report=None
 try:
  deadline=time.monotonic()+90
  print('Reading the existing terminal only; waiting for MT5 '+login+'. No broker login or trading commands are sent.',flush=True)
  while time.monotonic()<deadline:
   before=mt5.account_info()
   if before is None or str(before.login)!=login:time.sleep(.1);continue
   observed=datetime.now(timezone.utc);captured=[]
   class Reader:
    def account_info(self):return mt5.account_info()
    def history_deals_get(self,*args):
     deals=mt5.history_deals_get(*args);captured.append((args,deals));return deals
   try:daily=controls.broker_daily_realised(Reader(),row,observed.isoformat());error=None
   except Exception as exc:daily=None;error=type(exc).__name__+': '+str(exc)
   current=mt5.account_info()
   if current is None or str(current.login)!=login or abs(float(current.balance)-float(before.balance))>.01:time.sleep(.1);continue
   # This independent, read-only full-account query distinguishes missing
   # initial funding from an empty/partially loaded post-assignment cache.
   full=mt5.history_deals_get(datetime(1970,1,1,tzinfo=timezone.utc),observed)
   after=mt5.account_info()
   if after is None or str(after.login)!=login or abs(float(after.balance)-float(before.balance))>.01:time.sleep(.1);continue
   report={'login':login,'server':getattr(after,'server',None),'expected_server':row.get('mt5_server'),'observed_at':observed.isoformat(),'assignment':assigned.isoformat(),'capital':row.get('start_balance') or row.get('account_size'),'balance':after.balance,'equity':after.equity,'daily_percent':str(controls.daily_percent(row)),'daily_reader_result':str(daily) if daily is not None else None,'daily_reader_error':error,'assigned_query':summarise(captured[-1][1],observed,assigned) if captured else None,'full_query':summarise(full,observed,assigned)}
   break
 finally:mt5.shutdown()
 if report is None:report={'login':login,'result':'No stable read while account switching; no changes were made.'}
 output.write_text(json.dumps(report,indent=2),encoding='utf-8');print(json.dumps(report,indent=2),flush=True);print('Report saved:',output,flush=True)
if __name__=='__main__':main()

'@
$probePath = Join-Path $env:TEMP ('np-daily-audit-' + [Guid]::NewGuid().ToString() + '.py')
$output = Join-Path ([Environment]::GetFolderPath('Desktop')) 'Shobukola-Daily-Audit.txt'
try {
    [IO.File]::WriteAllText($probePath,$code,(New-Object Text.UTF8Encoding($false)))
    $pythonExe = $worker.ExecutablePath
    if (-not $pythonExe) { throw 'Worker Python executable unavailable.' }
    & $pythonExe $probePath $paths[0] $terminal 'C:\NairaPips_DD_Fleet\state\monitorable_accounts_snapshot.json' '477723574' $output
    if ($LASTEXITCODE -ne 0) { throw 'Read-only diagnostic did not complete; send the visible error.' }
} finally { Remove-Item -LiteralPath $probePath -ErrorAction SilentlyContinue }
Write-Host 'Read-only audit finished. No worker restart, broker login, trade or rule changes were performed.'
notepad.exe $output
