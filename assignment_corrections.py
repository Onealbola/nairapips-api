"""Exact owner-authorised pool-capital corrections; retain internal audit rows."""
import json
from uuid import UUID
from decimal import Decimal
from datetime import datetime,timezone
PREFIX='pool_capital_correction_v1:'

def marker(purchase_id,trader_id):
    return '[NP_POOL_CAPITAL_CORRECTION:'+str(UUID(purchase_id))+':'+str(UUID(trader_id))+']'

def voided(row):
    try:
        return (row.get('account_status')=='archived' and marker(str(row['purchase_id']),str(row['trader_id'])) in str(row.get('archive_reason') or '')) or __import__('daily_breach_corrections').voided(row)
    except (KeyError,TypeError,ValueError):return False

def trader_view(root):
    """Hide only identified company-error deliveries, never generic archives."""
    excluded=set();replacement_stages={}
    for key in ('accounts','all_accounts','active_accounts','trader_accounts'):
        rows=root.get(key)
        if isinstance(rows,list):
            excluded.update(str(r['id']) for r in rows if isinstance(r,dict) and voided(r))
            replacement_stages.update({str(r['id']):('phase2' if __import__('daily_breach_corrections').voided(r) else 'phase1') for r in rows if isinstance(r,dict) and voided(r)})
            root[key]=[r for r in rows if not isinstance(r,dict) or not voided(r)]
    for key in ('account','current_account'):
        if isinstance(root.get(key),dict) and voided(root[key]):
            replacement_stages[str(root[key]['id'])]='phase2' if __import__('daily_breach_corrections').voided(root[key]) else 'phase1'
            excluded.add(str(root[key]['id']));root[key]=None
    if excluded:
        root['correction_excluded_account_ids']=sorted(excluded)
        trader=root.get('trader')
        if isinstance(trader,dict) and str(trader.get('current_account_id')) in excluded:
            current=root.get('current_account') or root.get('account') or {}
            target_stage=replacement_stages.get(str(trader.get('current_account_id')),'phase1')
            trader['current_account_id']=current.get('id')
            trader['mt5_login']=current.get('mt5_login')
            if not current:
                trader.update(phase=target_stage,challenge_state=target_stage+'_waiting_mt5')
                for field in ('profit','profit_percent','current_profit','current_profit_percent'):
                    if field in trader:trader[field]=0
                for field in ('balance','equity','current_balance','current_equity'):
                    if field in trader:trader[field]=None
        for key in ('latest_trades','pending_replacements','assignment_queue','phase_assignment_queue','reset_assignment_queue','previous_reset_actions'):
            if isinstance(root.get(key),list):root[key]=[r for r in root[key] if not isinstance(r,dict) or not excluded.intersection(str(r.get(k) or '') for k in ('trader_account_id','source_account_id','reset_source_account_id','completed_account_id'))]
        if isinstance(root.get('latest_monitoring'),dict) and str(root['latest_monitoring'].get('trader_account_id')) in excluded:root['latest_monitoring']={}

    return root

def retry_pending(read,save,pick,assign):
    summary=dict(checked=0,assigned=0,waiting_inventory=0,already_fulfilled=0,errors=0)
    for stored in read('package_studio_private_settings',{'key':'like.'+PREFIX+'*','limit':'25'}):
        try:
            case=json.loads(stored['value'])
            if case.get('state') not in {'waiting_inventory','pending'}:continue
            summary['checked']+=1
            pid=str(UUID(case['purchase_id']));tid=str(UUID(case['trader_id']))
            if stored.get('key')!=PREFIX+pid or case.get('target_stage')!='phase1' or case.get('authorized_by')!='owner_instruction':raise ValueError('Invalid correction authority')
            purchases=read('challenge_purchases',{'id':'eq.'+pid,'trader_id':'eq.'+tid,'limit':'1'})
            traders=read('traders',{'id':'eq.'+tid,'limit':'1'})
            chain=read('trader_accounts',{'purchase_id':'eq.'+pid,'trader_id':'eq.'+tid,'limit':'1000'})
            if len(purchases)!=1 or len(traders)!=1 or len(chain)>=1000:raise ValueError('Incomplete correction ownership')
            purchase=purchases[0]
            if str(purchase.get('id'))!=pid or str(purchase.get('trader_id'))!=tid or str(traders[0].get('id'))!=tid:raise ValueError('Correction identity changed')
            if Decimal(str(purchase['account_size']))!=Decimal(str(case['account_size'])):raise ValueError('Purchased capital changed')
            originals={str(r['id']):r for r in chain if str(r.get('id')) in case['voided_account_ids']}
            if len(originals)!=len(case['voided_account_ids']) or not all(voided(r) for r in originals.values()):raise ValueError('Correction was not applied to the exact old deliveries')
            survivors=[r for r in chain if r.get('mt5_login') and not voided(r)]
            if survivors:
                if len(survivors)!=1 or survivors[0].get('stage')!='phase1':raise ValueError('Existing successor requires review; never assign twice')
                account=survivors[0];summary['already_fulfilled']+=1
            else:
                if purchase.get('payment_status')!='approved' or purchase.get('status')!='approved_waiting_mt5':raise ValueError('Correction requires the approved waiting purchase')
                pointers=(purchase.get('trader_account_id'),traders[0].get('current_account_id'))
                if any(value and str(value) not in originals for value in pointers):raise ValueError('Another current delivery requires review')
                mt5=pick(float(case['account_size']),'phase1')
                if not mt5:summary['waiting_inventory']+=1;continue
                # Cross-process claim before any allocation. A lost/uncertain
                # write stays for review rather than replaying an assignment.
                claimed=dict(case,state='processing',claimed_at=datetime.now(timezone.utc).isoformat())
                if not save(stored,claimed):continue
                try:
                    account,_=assign(traders[0],mt5,'phase1',purchase,
                        {'name':'system','username':'system','role':'system'},
                        'Owner-authorised pool capital correction; fresh first valid Phase 1; case='+pid)
                except Exception:
                    save(stored,dict(claimed,state='needs_review'))
                    raise
                summary['assigned']+=1
            case.update(state='fulfilled',replacement_account_id=account['id'],replacement_mt5=account['mt5_login'],completed_at=datetime.now(timezone.utc).isoformat())
            save(stored,case)
        except Exception as exc:
            summary['errors']+=1
            print('POOL CAPITAL CORRECTION:',type(exc).__name__,flush=True)
    return summary
