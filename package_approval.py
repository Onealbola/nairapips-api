"""Package approvals use frozen contracts and atomic entitlements, not old plans.

No import-time workers or external requests. Unknown mutation results are never
automatically repeated; an explicit retry resumes the same purchase entitlement.
"""
from flask import jsonify, request
from package_contracts import identity
from package_checkout import PackageOrderUnconfirmed, PackageOrderRejected, PackageStorageError


class PackageApprovalStore:
    def __init__(self, orders):
        self.orders = orders

    def purchase(self, purchase_id):
        rows = self.orders.read('challenge_purchases', {
            'id': 'eq.' + identity(purchase_id),
            'select': 'id,package_contract_id,trader_id', 'limit': '1'})
        return rows[0] if rows else None

    def rpc(self, name, payload):
        try:
            response = self.orders.transport.post(self.orders.base + 'rpc/' + name,
                headers=self.orders.headers, json=payload, timeout=(2, 7))
        except Exception as exc:
            raise PackageOrderUnconfirmed('Package approval or delivery was not confirmed. Refresh the purchase before retrying; the same entitlement will be resumed.') from exc
        if not response.ok:
            try:
                code = response.json().get('code')
            except Exception:
                code = None
            if code in ('P0001', '23505', '23503', '23514', 'P0002'):
                raise PackageOrderRejected('Package approval or delivery was rejected. Check payment evidence and the exact purchase record.')
            raise PackageOrderUnconfirmed('Package operation was not confirmed. Refresh the purchase before retrying.')
        try:
            result = response.json()
            if not isinstance(result, dict):
                raise ValueError('Invalid package receipt')
            return result
        except Exception as exc:
            raise PackageOrderUnconfirmed('The package operation receipt could not be confirmed. Refresh before retrying.') from exc

    def approve(self, purchase_id, actor):
        result = self.rpc('np_package_approve_purchase', {
            'p_purchase_id': identity(purchase_id), 'p_actor': str(actor)})
        if not result.get('approved') or result.get('purchase_id') != purchase_id:
            raise PackageOrderUnconfirmed('Package payment approval receipt was not confirmed')
        if result.get('account_id'):
            return {**result, 'waiting': False}
        try:
            entitlement = identity(result.get('entitlement_id'))
        except ValueError as exc:
            raise PackageOrderUnconfirmed('Package payment approval was returned without a valid delivery entitlement. Refresh before retrying.') from exc
        # This is a separate idempotent transaction. Approval remains recorded
        # if no matching verified stock is ready or the delivery response is lost.
        delivery = self.rpc('np_package_assign_entitlement', {'p_entitlement_id': entitlement})
        if delivery.get('entitlement_id') != entitlement or (
                delivery.get('waiting') is not True and not delivery.get('account_id')):
            raise PackageOrderUnconfirmed('Package approval is recorded; account delivery receipt was not confirmed')
        return {**result, **delivery}


def register_package_approval(app, store, require_admin):
    legacy = app.view_functions.get('approve_purchase')
    if not callable(legacy):
        raise RuntimeError('Existing purchase approval route must be registered first')

    def approve():
        if request.method == 'OPTIONS':
            return legacy()
        admin, error = require_admin()
        if error is not None:
            return error
        data = request.get_json(silent=True)
        if not isinstance(data, dict) or not data.get('id'):
            return jsonify(success=False, error='Purchase id is required'), 400
        try:
            purchase_id = identity(data['id'])
            purchase = store.purchase(purchase_id)
            if not purchase or not purchase.get('package_contract_id'):
                return legacy()
            if data.get('mt5_id') or data.get('mt5_pool_id') or data.get('assigned_mt5_id'):
                return jsonify(success=False, error='Package accounts are selected from verified stock using the purchased rules. Leave manual MT5 selection empty.'), 409
            actor = admin.get('id') or admin.get('username')
            if not actor:
                return jsonify(success=False, error='Authenticated approving admin is required'), 403
            result = store.approve(purchase_id, actor)
            message = ('Payment approved. Waiting for a fresh verified MT5 matching this package.'
                       if result.get('waiting') else 'Package approval and account delivery are recorded.')
            return jsonify(success=True, message=message, data=result)
        except PackageOrderRejected as exc:
            return jsonify(success=False, error=str(exc)), 409
        except ValueError:
            return jsonify(success=False, error='Invalid purchase or entitlement identity'), 400
        except (PackageStorageError, PackageOrderUnconfirmed) as exc:
            return jsonify(success=False, error=str(exc), purchase_status='unconfirmed'), 503

    app.view_functions['approve_purchase'] = approve
