"""Validate a canonical archived pass against an exact saved broker observation."""
from decimal import Decimal
from datetime import datetime

def verify(account,purchase,live,journal=None):
    try:
        def num(v):
            n=Decimal(str(v))
            if not n.is_finite():raise ValueError('Non-finite evidence')
            return n
        def stamp(v):
            d=datetime.fromisoformat(str(v).replace('Z','+00:00'))
            if d.tzinfo is None:raise ValueError('Missing timezone')
            return d
        stage=account['stage'];aid=str(account['id']);owner=str(account['trader_id']);login=str(account['mt5_login'])
        if stage not in ('phase1','phase2'):return False
        if (account.get('account_status')!='archived_'+stage or account.get('phase_pass_status')!=stage+'_passed'
                or account.get('breached_at') or str(account.get('risk_zone') or '').lower()=='breached'):return False
        if str(account.get('purchase_id'))!=str(purchase.get('id')) or owner!=str(purchase.get('trader_id')):return False
        if (str(live.get('trader_account_id'))!=aid or str(live.get('trader_id')) not in {owner,aid}
                or str(live.get('mt5_login'))!=login or live.get('stage')!=stage):return False
        capital=num(account.get('start_balance') or account['account_size'])
        if capital<=0 or capital!=num(purchase['account_size']) or capital!=num(live['account_size']):return False
        limit=num(purchase['max_drawdown']);target=num(purchase[stage+'_target'])
        if not 0<limit<=100 or not 0<target<=100 or num(account['dd_limit_percent'])!=limit:return False
        if num(live['equity'])<capital*(1+target/100) or num(live['balance'])<=capital*(1-limit/100):return False
        if num(live['dd_used_percent'])>=100 or str(live.get('risk_zone') or '').lower()=='breached':return False
        if account.get('worst_dd_used_percent') is not None and num(account['worst_dd_used_percent'])>=100:return False
        if account.get('lowest_equity') is not None and num(account['lowest_equity'])<=capital*(1-limit/100):return False
        if abs((stamp(live['observed_at'])-stamp(account['passed_at'])).total_seconds())>300:return False
        from plan_risk_controls import assert_assignment_snapshot
        assert_assignment_snapshot(account,purchase)
        from equity_drawdown import spec,fingerprint
        rules=spec(account,purchase)
        if rules['drawdown_type']=='TRAILING' or rules.get('daily_drawdown'):
            if not journal or journal.get('rules_hash')!=fingerprint(rules) or journal.get('breached') is not False:return False
            if num(journal['worst_used_percent'])>=100:return False
        return True
    except (KeyError,ValueError,TypeError,ArithmeticError):return False
