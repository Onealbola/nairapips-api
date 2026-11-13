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
