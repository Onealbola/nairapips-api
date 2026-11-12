"""Owner-authorised replacement of a proven false first-day daily-breach event."""
import json
from uuid import UUID
from decimal import Decimal
from datetime import datetime,timezone,timedelta
PREFIX='daily_breach_correction_v1:'

def marker(purchase_id,trader_id,account_id):
    return '[NP_FALSE_DAILY_CORRECTION:'+':'.join(str(UUID(str(v))) for v in (purchase_id,trader_id,account_id))+']'

def voided(row):
    try:return row.get('account_status')=='archived' and marker(row['purchase_id'],row['trader_id'],row['id']) in str(row.get('archive_reason') or '')
    except (KeyError,ValueError,TypeError):return False

def evidence(read,account,purchase):
    """No financial writes. Require a complete first-day, reconciled trigger replay."""
    from equity_drawdown import spec,initial,evaluate,fingerprint
    from plan_risk_controls import WAT,assert_assignment_snapshot
    assert_assignment_snapshot(account,purchase)
    rules=spec(account,purchase)
    if account.get('stage')!='phase2' or rules['drawdown_type']!='STATIC' or Decimal(rules.get('daily_drawdown','0'))<=0:raise ValueError('Unsupported correction evidence')
    if str(account['purchase_id'])!=str(purchase['id']) or str(account['trader_id'])!=str(purchase['trader_id']):raise ValueError('Correction ownership mismatch')
    rows=read('package_studio_private_settings',{'key':'eq.equity_drawdown_v1:'+account['id'],'limit':'1'})
    if len(rows)!=1:raise ValueError('Exact preserved breach journal required')
    state=json.loads(rows[0]['value']);first=state.get('first_breach') or {}
    if not state.get('breached') or state.get('rules_hash')!=fingerprint(rules) or Decimal(state.get('daily_realised','NaN'))!=0:raise ValueError('Breach evidence changed or does not match this defect')
    observed=datetime.fromisoformat(first['observed_at'].replace('Z','+00:00'))
    created=datetime.fromisoformat(str(account['created_at']).replace('Z','+00:00'))
    if observed.tzinfo is None or created.tzinfo is None or observed.astimezone(WAT).date()!=created.astimezone(WAT).date():raise ValueError('Full first-day ownership evidence required')
    records=read('trader_trades',{'trader_account_id':'eq.'+account['id'],'trader_id':'eq.'+account['trader_id'],'status':'eq.closed','closed_at':'gte.'+created.isoformat(),'order':'synced_at.desc','limit':'1000'})
    if len(records)>=1000:raise ValueError('Complete trade evidence required')
    unique={}
    for trade in records:
        if (str(trade.get('trader_account_id'))!=str(account['id']) or str(trade.get('trader_id'))!=str(account['trader_id'])):raise ValueError('Trade evidence owner changed')
        if trade.get('closed_at') and created<=datetime.fromisoformat(trade['closed_at'].replace('Z','+00:00'))<=observed:
            unique.setdefault(str(trade['ticket']),trade)
    net=sum((sum((Decimal(str(t.get(k) or 0)) for k in ('profit','swap','commission')),Decimal(0)) for t in unique.values()),Decimal(0))
    balance=Decimal(first['balance']);equity=Decimal(first['equity']);capital=Decimal(rules['capital'])
    if net<=0 or abs(capital+net-balance)>Decimal('.01'):raise ValueError('Closing profit does not reconcile to the recorded trigger balance')
    wrong=evaluate(rules,initial(rules),equity,first['observed_at'],balance=balance,daily_realised=0)
    correct=evaluate(rules,initial(rules),equity,first['observed_at'],balance=balance,daily_realised=net)
    if not wrong['breached'] or correct['breached'] or Decimal(wrong['drawdown_floor'])!=Decimal(first['floor']):raise ValueError('The event is not proven false under the purchased rules')
    return {'observed_at':first['observed_at'],'closed_profit':str(net),'closing_tickets':sorted(unique),'correct_daily_loss_percent':correct['daily_loss_percent'],'correct_daily_floor':correct['daily_floor'],'original_journal':state}

def retry_pending(read,save,pick,assign):
    summary=dict(checked=0,assigned=0,waiting_inventory=0,already_fulfilled=0,errors=0)
    for stored in read('package_studio_private_settings',{'key':'like.'+PREFIX+'*','limit':'25'}):
        try:
            case=json.loads(stored['value'])
            if case.get('state') not in {'waiting_inventory','pending'}:continue
            summary['checked']+=1
            aid=str(UUID(case['source_account_id']));pid=str(UUID(case['purchase_id']));tid=str(UUID(case['trader_id']))
            if (stored['key']!=PREFIX+aid or case.get('authorized_by')!='owner_instruction' or case.get('target_stage')!='phase2'):raise ValueError('Invalid correction authority')
            ps=read('challenge_purchases',{'id':'eq.'+pid,'trader_id':'eq.'+tid,'limit':'1'});ts=read('traders',{'id':'eq.'+tid,'limit':'1'})
            chain=read('trader_accounts',{'purchase_id':'eq.'+pid,'trader_id':'eq.'+tid,'limit':'1000'})
            if len(ps)!=1 or len(ts)!=1 or len(chain)>=1000:raise ValueError('Exact correction ownership unavailable')
            p=ps[0];t=ts[0]
            if str(p.get('id'))!=pid or str(p.get('trader_id'))!=tid or str(t.get('id'))!=tid or any(str(a.get('purchase_id'))!=pid or str(a.get('trader_id'))!=tid for a in chain):raise ValueError('Correction identity changed')
            if p.get('payment_status')!='approved' or Decimal(str(p['account_size']))!=Decimal(str(case['account_size'])):raise ValueError('Approved purchase or capital changed')
            source=next((a for a in chain if a['id']==aid),None)
            if not source or not voided(source):raise ValueError('Exact false delivery has not been withdrawn')
            evidence(read,source,p)
            if read('payouts',{'trader_account_id':'eq.'+aid,'limit':'1'}):raise ValueError('Financial history requires separate review')
            parents=[a for a in chain if a['id']==case['parent_account_id'] and a.get('stage')=='phase1' and a.get('account_status')=='archived_phase1' and a.get('passed_at') and not a.get('breached_at')]
            if len(parents)!=1:raise ValueError('Exact valid Phase 1 parent required')
            from assignment_corrections import voided as any_voided
            survivors=[a for a in chain if a.get('mt5_login') and a['id']!=parents[0]['id'] and not any_voided(a)]
            if survivors:
                if len(survivors)!=1 or survivors[0].get('stage')!='phase2' or survivors[0].get('account_status')!='assigned_active':raise ValueError('Unexpected successor requires review')
                account=survivors[0];summary['already_fulfilled']+=1
            else:
                if p.get('status')!='approved_waiting_mt5' or p.get('trader_account_id') not in (None,aid) or t.get('current_account_id') not in (None,aid):raise ValueError('Current delivery changed')
                mt5=pick(float(case['account_size']),'phase2')
                if not mt5:summary['waiting_inventory']+=1;continue
                claimed=dict(case,state='processing',claimed_at=datetime.now(timezone.utc).isoformat())
                if not save(stored,claimed):continue
                try:
                    account,_=assign(t,mt5,'phase2',p,{'name':'system','username':'system','role':'system'},'Owner-authorised false daily breach correction; fresh Phase 2; case='+aid)
                except Exception:
                    save(stored,dict(claimed,state='needs_review'));raise
                summary['assigned']+=1
            case.update(state='fulfilled',replacement_account_id=account['id'],replacement_mt5=account['mt5_login'],completed_at=datetime.now(timezone.utc).isoformat())
            save(stored,case)
        except Exception as exc:
            summary['errors']+=1;print('DAILY BREACH CORRECTION:',type(exc).__name__,str(exc)[:160],flush=True)
    return summary
