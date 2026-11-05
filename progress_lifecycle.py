"""Exact Progress purchase routing; legacy one-phase journeys stay untouched."""
from progress_plan_bridge import is_managed, PREFIX, managed_id

ROUTE=('phase1','phase2','funded')


def is_progress_purchase(p):
    source=str((p or {}).get('journey_source') or '')
    return source.startswith(PREFIX) and str((p or {}).get('plan_id'))==managed_id(source)


def install(g):
    old_journey=g['_journey_for_lifecycle']
    old_next=g['_next_stage_for_lifecycle']
    old_source=g['_journey_source_value']
    old_assign=g['_assign_mt5_to_trader']

    def exact_purchase(account, purchase):
        if purchase:return purchase
        a=account or {}
        if not a.get('purchase_id') or not a.get('trader_id') or 'phase2' not in str(a.get('challenge_journey') or a.get('journey_stages') or ''):return {}
        rows=g['_np_v214_rows']('challenge_purchases',{'id':'eq.'+str(a['purchase_id']),
            'trader_id':'eq.'+str(a['trader_id']),'limit':'1'})
        return rows[0] if rows else {}

    def journey(account=None,purchase=None,plan=None,trader=None):
        p=exact_purchase(account,purchase)
        if is_progress_purchase(p) or (not p and is_managed(plan or {})):
            return ROUTE
        return old_journey(account,purchase,plan,trader)

    def next_stage(stage,account=None,purchase=None,plan=None,trader=None):
        p=exact_purchase(account,purchase)
        if is_progress_purchase(p) or (not p and is_managed(plan or {})):
            stage=g['_normalize_lifecycle_stage'](stage)
            return {'phase1':'phase2','phase2':'funded'}.get(stage)
        return old_next(stage,account,purchase,plan,trader)

    def source_value(*values):
        if any(isinstance(v,(list,tuple)) and tuple(v)==ROUTE for v in values):return list(ROUTE)
        return old_source(*values)

    def assign(trader,mt5,stage,purchase=None,staff=None,note='MT5 assigned'):
        if is_progress_purchase(purchase):
            tid=str((trader or {}).get('id') or '')
            if not tid or tid!=str(purchase.get('trader_id') or ''):
                raise ValueError('Package assignment must belong to the exact purchase owner')
            normalized=g['_normalize_lifecycle_stage'](stage)
            chain=g['_np_v214_rows']('trader_accounts',{'purchase_id':'eq.'+str(purchase['id']),
                'trader_id':'eq.'+tid,'order':'created_at.asc','limit':'1000'})
            if len(chain)>=1000:raise ValueError('Complete package account history is required')
            real=[a for a in chain if a.get('mt5_login') and a.get('account_status')!='assignment_sync_error']
            if normalized=='phase1' and real:
                raise ValueError('This package has already received its first account; resets are not included')
            if normalized in ('phase2','funded'):
                required='phase1' if normalized=='phase2' else 'phase2'
                passed=[a for a in real if a.get('stage')==required
                    and a.get('account_status')=='archived_'+required
                    and a.get('phase_pass_status')==required+'_passed'
                    and a.get('passed_at') and not a.get('breached_at')
                    and '[NP_LIVE_PASS_V214]' in str(a.get('archive_reason') or '')]
                if not passed:raise ValueError('The exact preceding package phase has not passed broker verification')
                if normalized=='phase2' and any(a.get('stage') in ('phase2','funded') for a in real):
                    raise ValueError('This phase-pass entitlement has already been used')
            if normalized not in ROUTE:raise ValueError('Unknown package stage')
        return old_assign(trader,mt5,stage,purchase,staff,note)

    def phase2_entitlement(source):
        return bool(source and source.get('stage')=='phase1'
            and source.get('account_status')=='archived_phase1'
            and source.get('phase_pass_status')=='phase1_passed' and source.get('passed_at')
            and not source.get('breached_at')
            and '[NP_LIVE_PASS_V214]' in str(source.get('archive_reason') or '')
            and is_progress_purchase(exact_purchase(dict(source,challenge_journey=list(ROUTE)),None)))

    g['_np_progress_phase2_entitlement']=phase2_entitlement
    g['_journey_for_lifecycle']=journey
    g['_next_stage_for_lifecycle']=next_stage
    g['_journey_source_value']=source_value
    g['_assign_mt5_to_trader']=assign

    old_model=g.get('_np_progression_plan_model')
    if callable(old_model):
        def model(purchase,rows):
            if is_progress_purchase(purchase):
                return dict(model='progress_two_phase',label='2-PHASE → FUNDED',
                    challenge_reset='NOT_INCLUDED',funded_breach_reset='NOT_INCLUDED',
                    payout_renewal='UNLIMITED_WHILE_FUNDED')
            return old_model(purchase,rows)
        g['_np_progression_plan_model']=model
    old_reset=g.get('_np_reset_policy')
    if callable(old_reset):
        def reset_policy(account,trader=None):
            if is_progress_purchase(exact_purchase(account,None)):
                return dict(eligible=False,kind='none',reason='package_resets_not_included',
                    title='Resets are not included in this package',stage=(account or {}).get('stage'),
                    source_account_id=(account or {}).get('id'))
            return old_reset(account,trader)
        g['_np_reset_policy']=reset_policy
