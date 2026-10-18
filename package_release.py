"""Registration for the production order flow; no startup I/O or release bypass."""
from flask import jsonify
from package_contracts import RULES_VERSION
from package_checkout import PackageOrderStore,register_package_checkout
from package_approval import PackageApprovalStore,register_package_approval
from package_fulfillment import PackageFulfillmentStore,register_package_fulfillment

def register_package_production(app,transport,url,key,config,controls,require_admin,authenticated_trader,runtime):
    orders=PackageOrderStore(transport,url,key)
    def readiness():
        consumers={
            'Account assignment and renewal':runtime.get('_assign_mt5_to_trader'),
            'Payout verification':runtime.get('_np_plan_payout_split_for_account'),
            'Reset allowances':runtime.get('_np_v55_decision'),
        }
        blockers=[name for name,fn in consumers.items() if getattr(fn,'np_package_rules_version',None)!=RULES_VERSION]
        if not orders.schema_available():blockers.append('Production package database migration')
        probe=runtime.get('_np_verified_package_dd_runtime_v222')
        try:verified=callable(probe) and probe() is True
        except Exception:verified=False
        if not verified:blockers.append('Verified running monitoring and DD fleet')
        return {'ready':not blockers,'blockers':blockers}
    config.readiness=readiness
    register_package_checkout(app,orders,config,controls,authenticated_trader,readiness)
    approval=PackageApprovalStore(orders)
    register_package_approval(app,approval,require_admin)
    register_package_fulfillment(app,PackageFulfillmentStore(approval),require_admin,authenticated_trader)
    @app.get('/admin/package_readiness')
    def package_readiness():
        admin,error=require_admin()
        if error is not None:return error
        if str((admin or {}).get('role') or '').lower() not in ('owner','super_admin','superadmin'):
            return jsonify(success=False,error='Owner access required'),403
        return jsonify(success=True,**readiness())
    return orders
