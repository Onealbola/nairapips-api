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
