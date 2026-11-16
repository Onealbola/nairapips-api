"""Conservative missing-journal recovery for broker-proven unused static accounts.

No trades, logins, rule edits, old journal writes or breach-latch changes.
"""
import json,os
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
RELEASE='NP_UNUSED_STATIC_JOURNAL_20261116'

def money(value):
    if isinstance(value,bool):raise ValueError('Invalid broker amount')
    n=Decimal(str(value))
    if not n.is_finite():raise ValueError('Non-finite broker amount')
    return n

def coherent(info):
    try:
        b,e,p,c=(money(getattr(info,k)) for k in ('balance','equity','profit','credit'))
        return b>=0 and e>=0 and abs(e-b-p-c)<=Decimal('.01')
    except Exception:return False

def prove(mt5,row):
    rules=row['drawdown_rules'];login=str(row['mt5_login']);capital=money(rules['capital'])
    if rules['drawdown_type']!='STATIC' or money(rules.get('daily_drawdown',0))<=0:raise ValueError('Only unused static daily accounts can recover this way')
    if any(row.get(k) for k in ('breached_at','breach_at','reset_consumed_at')) or any(k in str(row.get('account_status') or row.get('status') or '').lower() for k in ('breach','archive','closed','passed','reset','locked')):raise ValueError('Terminal account cannot recover')
    def exact():
        a=mt5.account_info();t=mt5.terminal_info()
        if a is None or t is None or not t.connected or str(a.login)!=login or str(a.server).lower()!=str(row['mt5_server']).lower() or not coherent(a):raise ValueError('Fresh coherent broker identity required')
        if money(a.balance)!=capital or money(a.equity)!=capital or money(a.profit)!=0 or money(a.credit)!=0:raise ValueError('Unused account must have exact funded capital and no profit or credit')
    exact();at=datetime.now(timezone.utc)
    deals=mt5.history_deals_get(datetime(1970,1,1,tzinfo=timezone.utc),at)
    if not deals:raise ValueError('Complete funding history required')
    seen=set();total=Decimal(0)
    for d in deals:
        if d.ticket in seen or d.type not in (2,5):raise ValueError('Trading or unsupported history requires manual review')
        seen.add(d.ticket)
        if datetime.fromtimestamp(float(d.time_msc or d.time*1000)/1000,timezone.utc)>at:raise ValueError('Future broker deal')
        total+=sum((money(getattr(d,k,0)) for k in ('profit','commission','swap','fee')),Decimal(0))
    positions=mt5.positions_get();orders=mt5.orders_get()
    if positions is None or orders is None or positions or orders or total!=capital:raise ValueError('Empty positions/orders and reconciled funding required')
    exact()
    return dict(release=RELEASE,verified_at=at.isoformat(),mt5_login=login,funding_tickets=sorted(str(x) for x in seen),funding_total=str(total),trading_deals=0)

def install(g):
    previous=g['verified_live_info']
    def verified(login,server,password,row):
        info=previous(login,server,password,row)
        if info is not None and not coherent(info):
            g['log'](f'BROKER FINANCIAL SNAPSHOT INCOMPLETE MT5={login}; trying next credential | NO DD DECISION','WARNING')
            return None
        return info
    g['verified_live_info']=verified
    base=g['EquityDrawdownJournal']
    class GuardedJournal(base):
        def assess(self,row,equity,observed_at,*args,**kwargs):
            try:return super().assess(row,equity,observed_at,*args,**kwargs)
            except ValueError as exc:
                if str(exc)!='Existing account watermark must be recovered; it cannot be reset':raise
            from equity_drawdown import initial,account_lock
            rules=row['drawdown_rules'];path=self.directory/(rules['account_id']+'.json')
            if money(equity)!=money(rules['capital']) or money(kwargs.get('balance'))!=money(rules['capital']) or money(kwargs.get('daily_realised'))!=0:
                raise ValueError('Current assessment is not a coherent unused-account observation')
            proof=prove(g['mt5'],row)
            root=Path(os.environ.get('NAIRAPIPS_DD_LATCH_ROOT',r'C:\NairaPips_DD_Fleet\state'))
            if not root.is_dir():raise ValueError('Breach latch directory unavailable')
            for latch in root.glob('breach_latch_shard_*.json'):
                raw=latch.read_text(encoding='utf-8-sig');json.loads(raw)
                if str(row['mt5_login']) in raw or rules['account_id'] in raw:raise ValueError('Existing breach latch prevents recovery')
            with account_lock(path.with_suffix('.lock')):
                if not path.exists():
                    state=dict(initial(rules),unused_account_recovery=proof)
                    with open(path,'x',encoding='utf-8') as stream:
                        json.dump(state,stream,sort_keys=True);stream.flush();os.fsync(stream.fileno())
                    g['log'](f'UNUSED STATIC DAILY JOURNAL RECOVERED MT5={row["mt5_login"]}; full broker history verified; no historical journal replaced','WARNING')
            return super().assess(row,equity,observed_at,*args,**kwargs)
    g['EquityDrawdownJournal']=GuardedJournal
