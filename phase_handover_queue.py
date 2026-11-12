"""Prioritise exact passed accounts already waiting for their next MT5.

Uses the existing progression authority; never assigns inventory directly.
"""

def retry_waiting(read, assign, is_clean_root, limit=25):
    result=dict(checked=0,eligible=0,assigned=0,already_fulfilled=0,waiting=0,errors=0)
    traders=read('traders',{'challenge_state':'in.(phase2_waiting_mt5,funded_waiting_mt5)',
        'select':'*','limit':str(limit),'order':'updated_at.desc,id.desc'})
    for trader in traders:
        result['checked']+=1
        try:
            tid=str(trader.get('id') or '')
            sid=str(trader.get('current_account_id') or '')
            target={'phase2_waiting_mt5':'phase2','funded_waiting_mt5':'funded'}.get(trader.get('challenge_state'))
            if not tid or not sid or not target:continue
            sources=read('trader_accounts',{'id':'eq.'+sid,'trader_id':'eq.'+tid,'limit':'1'})
            if len(sources)!=1:continue
            source=sources[0]
            expected={'phase2':'phase1','funded':'phase2'}[target]
            # One-phase Phase1 -> Funded routes remain with the established broad
            # scanner. This queue prioritises verified two-phase transitions.
            if (str(source.get('id'))!=sid or str(source.get('trader_id'))!=tid
                    or source.get('stage')!=expected
                    or source.get('account_status')!='archived_'+expected
                    or not source.get('passed_at') or source.get('breached_at')
                    or source.get('package_contract_id')):continue
            pid=str(source.get('purchase_id') or '')
            if not pid:continue
            purchases=read('challenge_purchases',{'id':'eq.'+pid,'trader_id':'eq.'+tid,'limit':'1'})
            if len(purchases)!=1:continue
            purchase=purchases[0]
            if (str(purchase.get('id'))!=pid or str(purchase.get('trader_id'))!=tid
                    or str(purchase.get('trader_account_id') or '')!=sid
                    or not is_clean_root(purchase)):continue
            result['eligible']+=1
            # Existing code rechecks purchased journey, holds, prior successors,
            # per-source CAS, size, pool, age, never-used history and frozen rules.
            outcome=assign(trader,target,purchase,source,'lifecycle_progression')
            if outcome:
                result['already_fulfilled' if outcome.get('already_fulfilled') else 'assigned']+=1
            else:result['waiting']+=1
        except Exception as exc:
            result['errors']+=1
            print('PHASE HANDOVER RETRY:',type(exc).__name__,flush=True)
    return result
