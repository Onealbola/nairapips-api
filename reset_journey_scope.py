"""Scope lifecycle presentation to the canonical account's purchased journey."""
def install(g):
    original=g['_np_bootstrap_lifecycle_authority']
    def scoped(trader,accounts,purchases,plans):
        trader=trader or {};accounts=list(accounts or []);purchases=list(purchases or [])
        aid=str(trader.get('current_account_id') or '')
        anchor=next((a for a in accounts if str(a.get('id'))==aid and str(a.get('trader_id'))==str(trader.get('id'))),None)
        if anchor:
            pid=str(anchor.get('purchase_id') or anchor.get('challenge_purchase_id') or '')
            if pid:
                result=original(trader,[a for a in accounts if str(a.get('purchase_id') or a.get('challenge_purchase_id') or '')==pid],[p for p in purchases if str(p.get('id'))==pid],plans)
                return dict(result,focus_account_id=aid,focus_purchase_id=pid)
        return original(trader,accounts,purchases,plans)
    g['_np_bootstrap_lifecycle_authority']=scoped

def reset_actions(g,trader,accounts,purchases,existing):
    """Bootstrap must include configured challenge resets, not just old Funded rows."""
    from progress_lifecycle import is_progress_purchase
    roots={str(p.get('id')):p for p in purchases}
    result={str(a.get('account_id') or a.get('source_account_id')):a for a in existing}
    for account in accounts:
        pid=str(account.get('purchase_id') or account.get('challenge_purchase_id') or '')
        if not is_progress_purchase(roots.get(pid) or {}):continue
        if str(account.get('trader_id'))!=str(trader.get('id')):continue
        if not g['_np_reset_account_is_breached'](account):continue
        aid=str(account.get('id') or '')
        # Replace stale bootstrap inference with the final payment/consumption policy.
        result.pop(aid,None)
        action=g['_np_v69_previous_reset_action'](account,trader)
        if action:result[aid]=action
    return list(result.values())
