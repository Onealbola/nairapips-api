"""Exact package delivery retry and included reset requests.

Published switches gate new sales, not obligations already accepted. All
allowances, stage transitions and stock claims remain database transactions.
"""
import time
from flask import jsonify, request
from package_contracts import identity
from package_checkout import PackageOrderRejected, PackageOrderUnconfirmed, PackageStorageError


class PackageFulfillmentStore:
    def __init__(self, approval):
        self.approval = approval
        self.orders = approval.orders

    def pending(self, page=1):
        return self.orders.read('np_package_entitlements', {
            'state':'eq.reserved', 'select':'id,contract_id,source_account_id,kind,target_stage,state,created_at',
            'order':'created_at.asc,id.asc', 'limit':'21', 'offset':str((page-1)*20)})

    def deliver(self, entitlement_id):
        eid = identity(entitlement_id)
        result = self.approval.rpc('np_package_assign_entitlement', {'p_entitlement_id':eid})
        if result.get('entitlement_id') != eid or (
                result.get('waiting') is not True and not result.get('account_id')):
            raise PackageOrderUnconfirmed('Package delivery was not confirmed. Refresh before retrying the same entitlement.')
        return result

    def included_reset(self, trader_id, account_id):
        aid, tid = identity(account_id), identity(trader_id)
        rows = self.orders.read('trader_accounts', {
            'id':'eq.'+aid, 'trader_id':'eq.'+tid,
            'select':'id,trader_id,package_contract_id,stage', 'limit':'1'})
        if not rows or not rows[0].get('package_contract_id'):
            raise PackageOrderRejected('This package account does not belong to the signed-in trader')
        account = rows[0]
        if account.get('stage') not in ('phase1','phase2','funded'):
            raise PackageOrderRejected('This account has no eligible package reset stage')
        action = 'funded_reset' if account['stage']=='funded' else 'phase_reset'
        reservation = self.approval.rpc('np_package_reserve_action', {
            'p_contract_id':identity(account['package_contract_id']),
            'p_source_account_id':aid, 'p_kind':action})
        # Counter reservation is durable even if delivery is temporarily waiting.
        # Repeating this exact account action resumes its original entitlement.
        try:
            entitlement = identity(reservation.get('id'))
        except ValueError as exc:
            raise PackageOrderUnconfirmed('Reset entitlement reservation was not confirmed. Refresh before retrying.') from exc
        return self.deliver(entitlement)

    def cycle(self, limit=3, budget_seconds=5):
        started=time.monotonic()
        results=[]
        # Same RPC is used by direct admin retry. Database locks and the unique
        # entitlement prevent two request/workers from issuing a second account.
        for row in self.pending()[:limit]:
            if time.monotonic()-started >= budget_seconds:
                break
            try:
                result=self.deliver(row['id'])
                results.append({'entitlement_id':row['id'],'waiting':result.get('waiting') is True})
            except PackageOrderRejected:
                results.append({'entitlement_id':row['id'],'state':'requires_review'})
            except PackageStorageError:
                # Never repeat an unconfirmed mutation in this invocation.
                results.append({'entitlement_id':row['id'],'state':'unconfirmed'})
                break
        return results


def register_package_fulfillment(app, store, require_admin, authenticated_trader):
    def failure(exc):
        if isinstance(exc,PackageOrderRejected):
            return jsonify(success=False,error=str(exc)),409
        if isinstance(exc,PackageStorageError):
            return jsonify(success=False,error=str(exc),delivery_status='unconfirmed'),503
        return jsonify(success=False,error='Invalid package action identity or page'),400

    @app.get('/admin/package_fulfillment')
    def pending():
        _,error=require_admin()
        if error is not None:return error
        try:
            page=int(request.args.get('page','1'))
            if not 1<=page<=10000:raise ValueError()
            rows=store.pending(page)
            return jsonify(success=True,entitlements=rows[:20],page=page,has_more=len(rows)>20)
        except (ValueError,PackageStorageError) as exc:return failure(exc)

    @app.post('/admin/package_fulfillment/retry')
    def retry():
        _,error=require_admin()
        if error is not None:return error
        try:
            body=request.get_json(silent=True)
            if not isinstance(body,dict) or set(body)!={'entitlement_id'}:raise ValueError()
            result=store.deliver(body['entitlement_id'])
            return jsonify(success=True,data=result)
        except (ValueError,PackageStorageError) as exc:return failure(exc)

    @app.post('/trader/package_included_reset')
    def reset():
        tid,error=authenticated_trader(None)
        if error:return jsonify(success=False,error=error),401
        try:
            body=request.get_json(silent=True)
            if not isinstance(body,dict) or set(body)!={'account_id'}:raise ValueError()
            result=store.included_reset(tid,body['account_id'])
            return jsonify(success=True,data=result)
        except (ValueError,PackageStorageError) as exc:return failure(exc)
