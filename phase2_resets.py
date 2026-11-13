"""Optional paid package resets through the existing exact-payment workflow.

Never reopens a breached account. A verified payment buys one fresh same-stage
delivery; original trading rules, pass evidence and breach journals are retained.
"""
import re
from decimal import Decimal
from progress_lifecycle import is_progress_purchase,purchase_route
from progress_plan_bridge import managed_id,plan_row,PREFIX

RELEASE='managed-paid-resets-20261113-v2'

def install(g):
    old_plan=g['_np_reset_current_plan_for']
    old_policy=g['_np_reset_policy']
    base_policy=g['_np_reset_policy_v60_core']

    def owned_purchase(account):
        p=g['_np_reset_purchase_for_account'](account) or {}
        if not p and not account.get('purchase_id'):return {}
        if (str(p.get('id'))!=str(account.get('purchase_id')) or
                str(p.get('trader_id'))!=str(account.get('trader_id'))):
            raise ValueError('Exact reset purchase ownership is required')
        return p

    def current_plan(account,purchase=None):
        p=purchase or owned_purchase(account)
        stage=account.get('stage')
        if not is_progress_purchase(p):return old_plan(account,p)
        if stage not in purchase_route(p):return None
        # Pricing follows this exact editable offer, including closed new sales;
        # never borrow another size/package or rewrite the issued risk snapshot.
        doc,_=g['_np_plan_matrix_store'].read(fresh=True)
        for plan in doc['config']['plans']:
            for package in plan['packages']:
                source=PREFIX+plan['id']+':'+package['key']
                if managed_id(source)==str(p['plan_id']):
                    row=plan_row(plan,package,False)
                    if Decimal(str(row['account_size']))!=Decimal(str(account.get('account_size') or account.get('start_balance'))):
                        raise ValueError('Reset offer capital no longer matches the account')
                    if stage not in row['challenge_journey']:return None
                    return row
        return None

    def policy(account,trader=None):
        a=account or {}
        stage=a.get('stage')
        if stage not in {'phase1','phase2','funded'}:return old_policy(account,trader)
        p=owned_purchase(a)
        if not is_progress_purchase(p):return old_policy(account,trader)
        if stage not in purchase_route(p):return dict(eligible=False,kind='none',reason='stage_not_in_purchased_route',stage=stage)
        if trader and str(trader.get('id'))!=str(a.get('trader_id')):raise ValueError('Reset trader mismatch')
        if p.get('payment_status')!='approved' or p.get('status') in {'rejected','cancelled','canceled'}:
            return dict(eligible=False,kind='none',reason='parent_purchase_not_approved',stage=stage,source_account_id=a.get('id'))
        # Use the established paid challenge policy, including existing proof,
        # one-challenge-reset limit, zero-price disablement and pending states.
        result=base_policy(a,trader)
        return dict(result,authority=RELEASE)

    def paid_assignment(purchase,chain,stage,staff=None,note=''):
        if not is_progress_purchase(purchase) or stage not in purchase_route(purchase):return False
        from assignment_corrections import voided
        real=[a for a in chain if a.get('mt5_login') and not voided(a)]
        later=purchase_route(purchase)[purchase_route(purchase).index(stage)+1:]
        if any(a.get('stage') in later for a in real):return False
        kind='funded_reset_paid' if stage=='funded' else 'challenge_reset_paid'
        verified=[]
        for source in real:
            if source.get('stage')!=stage or not g['_np_reset_account_is_breached'](source):continue
            if str(source.get('purchase_id'))!=str(purchase['id']) or str(source.get('trader_id'))!=str(purchase['trader_id']):return False
            markers=re.findall(r'\[NP_ENTITLEMENT:'+kind+r':([^\]]+)\]',str(source.get('archive_reason') or ''),re.I)
            for oid in set(markers):
                rows=g['_np_v214_rows']('challenge_purchases',{'id':'eq.'+oid,'trader_id':'eq.'+str(purchase['trader_id']),'limit':'1'})
                if len(rows)!=1:continue
                order=rows[0]
                if str(order.get('id'))!=oid or str(order.get('trader_id'))!=str(purchase['trader_id']):continue
                ok,_,identity=g['_np_v71_paid_reset_order_firewall'](order)
                if not ok:continue
                if (identity.get('source_account_id')!=source['id'] or identity.get('parent_purchase_id')!=purchase['id'] or identity.get('stage')!=stage):continue
                if g['_np_reset_existing_child_v43'](source,order):return False
                verified.append((source['id'],oid))
        return len(set(verified))==1

    g['_np_reset_current_plan_for']=current_plan
    g['_np_reset_policy']=policy
    g['_np_phase2_paid_reset_authority']=paid_assignment
    g['_np_paid_reset_assignment_authority']=paid_assignment
