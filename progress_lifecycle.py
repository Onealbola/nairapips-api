"""Exact Progress purchase routing; legacy one-phase journeys stay untouched."""
from progress_plan_bridge import is_managed, PREFIX, managed_id

ROUTE=('phase1','phase2','funded')
ROUTES=(ROUTE,('phase1','funded'),('funded',))

def purchase_route(p):
    route=(p or {}).get('challenge_journey')
    if route is None:return ROUTE # Existing managed purchases predate model selection.
    if isinstance(route,(list,tuple)) and tuple(route) in ROUTES:return tuple(route)
    raise ValueError('The purchased account model is invalid; assignment stopped')

def initial_stage(p):
    return purchase_route(p)[0] if is_progress_purchase(p) else 'phase1'


def is_progress_purchase(p):
    source=str((p or {}).get('journey_source') or '')
    return source.startswith(PREFIX) and str((p or {}).get('plan_id'))==managed_id(source)


def install(g):
    old_journey=g['_journey_for_lifecycle']
    old_next=g['_next_stage_for_lifecycle']
    old_source=g['_journey_source_value']
    old_assign=g['_assign_mt5_to_trader']

    def exact_purchase(account, purchase, force=False):
        if purchase:return purchase
        a=account or {}
        if not a.get('purchase_id') or not a.get('trader_id'):return {}
        route=a.get('challenge_journey') or a.get('journey_stages') or []
        # Legacy one-phase routes already resolve correctly without another DB read.
        if not force and 'phase2' not in route and route!=['funded'] and not str(a.get('journey_source') or '').startswith(PREFIX):return {}
        rows=g['_np_v214_rows']('challenge_purchases',{'id':'eq.'+str(a['purchase_id']),
            'trader_id':'eq.'+str(a['trader_id']),'limit':'1'})
        return rows[0] if rows else {}

    def recorded_pass(account,purchase,stage):
        if (account.get('stage')!=stage or account.get('account_status')!='archived_'+stage
                or account.get('phase_pass_status')!=stage+'_passed' or not account.get('passed_at')
                or account.get('breached_at') or str(account.get('trader_id'))!=str(purchase.get('trader_id'))
                or str(account.get('purchase_id'))!=str(purchase.get('id'))):return False
        if '[NP_LIVE_PASS_V214]' in str(account.get('archive_reason') or ''):return True
        try:
            rows=g['_np_v214_rows']('np_live_account_state',{'trader_account_id':'eq.'+str(account['id']),'limit':'1'})
            if len(rows)!=1:return False
            from equity_drawdown import spec,read_saved_state
            rules=spec(account,purchase);journal=None
            if rules['drawdown_type']=='TRAILING' or rules.get('daily_drawdown'):
                journal=read_saved_state(g['requests'],g['SUPABASE_URL'],g.get('SUPABASE_SERVICE_ROLE_KEY') or g['SUPABASE_KEY'],account,purchase)
            from recorded_phase_pass import verify
            return verify(account,purchase,rows[0],journal)
        except Exception:return False

    def journey(account=None,purchase=None,plan=None,trader=None):
        p=exact_purchase(account,purchase)
        if is_progress_purchase(p) or (not p and is_managed(plan or {})):
            return purchase_route(p or plan)
        return old_journey(account,purchase,plan,trader)

    def next_stage(stage,account=None,purchase=None,plan=None,trader=None):
        p=exact_purchase(account,purchase)
        if is_progress_purchase(p) or (not p and is_managed(plan or {})):
            stage=g['_normalize_lifecycle_stage'](stage)
            route=purchase_route(p or plan)
            return route[route.index(stage)+1] if stage in route[:-1] else None
        return old_next(stage,account,purchase,plan,trader)

    def source_value(*values):
        if any(isinstance(v,(list,tuple)) and tuple(v) in ROUTES for v in values):return list(next(tuple(v) for v in values if isinstance(v,(list,tuple)) and tuple(v) in ROUTES))
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
            from assignment_corrections import voided
            real=[a for a in chain if a.get('mt5_login') and a.get('account_status')!='assignment_sync_error' and not voided(a)]
            route=purchase_route(purchase)
            if normalized not in route:raise ValueError('This stage is not part of the purchased model')
            reset_authority=g.get('_np_paid_reset_assignment_authority') or g.get('_np_phase2_paid_reset_authority')
            paid_reset=bool(real and callable(reset_authority) and reset_authority(purchase,chain,normalized,staff,note))
            if normalized==route[0] and real and normalized!='funded' and not paid_reset:
                raise ValueError('This package has already received its first account; resets are not included')
            if normalized==route[0]=='funded' and not real:
                context=g.get('_NP_APPROVAL_CONTEXT_V216')
                approved=str(purchase.get('payment_status') or '').lower()=='approved'
                if not approved and not (context and context.get()):
                    raise ValueError('Instant funding requires an approved payment')
            if route==('funded',) and real and not paid_reset:
                latest=real[-1]
                if latest.get('stage')!='funded' or latest.get('breached_at') or latest.get('account_status')!='archived_reset_funded':
                    raise ValueError('Instant funded renewal requires the previous funded payout to be completed')
                payouts=g['_np_v214_rows']('payouts',{'trader_account_id':'eq.'+str(latest.get('id') or ''),
                    'trader_id':'eq.'+tid,'status':'eq.paid','limit':'2'})
                if len(payouts)!=1 or str(payouts[0].get('trader_account_id'))!=str(latest.get('id')) or payouts[0].get('status')!='paid':
                    raise ValueError('The exact paid payout is required for instant funded renewal')
            if normalized in route[1:]:
                required=route[route.index(normalized)-1]
                passed=[a for a in real if recorded_pass(a,purchase,required)]
                if not passed:raise ValueError('The exact preceding package phase has not passed broker verification')
                if normalized=='phase2' and any(a.get('stage') in ('phase2','funded') for a in real) and not paid_reset:
                    raise ValueError('This phase-pass entitlement has already been used')
            if normalized not in ROUTE:raise ValueError('Unknown package stage')
        return old_assign(trader,mt5,stage,purchase,staff,note)

    def phase2_entitlement(source):
        p=exact_purchase(source,None,force=True)
        return bool(source and is_progress_purchase(p) and purchase_route(p)==ROUTE
            and recorded_pass(source,p,'phase1'))

    g['_np_initial_purchase_stage']=initial_stage
    g['_np_progress_phase2_entitlement']=phase2_entitlement
    g['_journey_for_lifecycle']=journey
    g['_next_stage_for_lifecycle']=next_stage
    g['_journey_source_value']=source_value
    g['_assign_mt5_to_trader']=assign

    old_model=g.get('_np_progression_plan_model')
    if callable(old_model):
        def model(purchase,rows):
            if is_progress_purchase(purchase):
                route=purchase_route(purchase)
                return dict(model={3:'progress_two_phase',2:'progress_one_phase',1:'progress_instant'}[len(route)],label=' → '.join(s.upper() for s in route),
                    challenge_reset='PAID_IF_CONFIGURED',funded_breach_reset='PAID_IF_CONFIGURED',
                    payout_renewal='UNLIMITED_WHILE_FUNDED')
            return old_model(purchase,rows)
        g['_np_progression_plan_model']=model
    old_reset=g.get('_np_reset_policy')
    if callable(old_reset):
        def reset_policy(account,trader=None):
            if is_progress_purchase(exact_purchase(account,None,force=True)):
                return dict(eligible=False,kind='none',reason='package_resets_not_included',
                    title='Resets are not included in this package',stage=(account or {}).get('stage'),
                    source_account_id=(account or {}).get('id'))
            return old_reset(account,trader)
        g['_np_reset_policy']=reset_policy
