"""Server-owned standard-plan cycle surcharges and reset prices."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from flask import jsonify, request
from production_controls import ControlsStore, ControlsUnavailable, Conflict
from plan_cycles import cycle_limit

KEY = 'standard_plan_cycle_prices_v1'
PAID = ('3', '4', '5', '0')
RELEASE = 'plan-reset-cycle-prices-2026-10-22'


def money(value):
    if isinstance(value, bool):
        raise ValueError('Enter a valid price')
    try:
        result = Decimal(str(value))
        if not result.is_finite() or result < 0 or result > 10000000 or result.as_tuple().exponent < -2:
            raise ValueError('Prices must be between 0 and 10,000,000 with at most two decimals')
        return float(result)
    except (InvalidOperation, TypeError):
        raise ValueError('Enter a valid price')


def defaults():
    return dict(revision=0, prices={k: None for k in PAID}, updated_at=None, updated_by=None)


def validate(doc):
    if not isinstance(doc, dict) or set(doc) != set(defaults()) or type(doc['revision']) is not int or doc['revision'] < 0:
        raise ValueError('Invalid cycle pricing settings')
    if not isinstance(doc['prices'], dict) or set(doc['prices']) != set(PAID):
        raise ValueError('Set separate prices for 3, 4, 5 and unlimited cycles')
    prices = {k: None if v is None else money(v) for k, v in doc['prices'].items()}
    if any(v is not None and v <= 0 for v in prices.values()):
        raise ValueError('Extra cycles require a positive price; leave blank to make that upgrade unavailable')
    return {**doc, 'prices': prices}


class CyclePriceStore(ControlsStore):
    def __init__(self, transport, url, key):
        def checked(doc):
            try:
                return validate(doc)
            except ValueError as exc:
                raise ControlsUnavailable('Saved cycle prices are invalid') from exc
        super().__init__(transport, url, key, KEY, defaults, checked)

    def save(self, body, actor):
        if not isinstance(body, dict) or set(body) != {'revision', 'prices'}:
            raise ValueError('Submit the loaded revision and cycle prices')
        old, raw = self.read(fresh=True)
        if type(body['revision']) is not int or body['revision'] != old['revision']:
            raise Conflict('Cycle prices changed in another session. Reload before saving.')
        doc = validate({**body, 'updated_at': None, 'updated_by': None})
        doc.update(revision=old['revision'] + 1, updated_at=datetime.now(timezone.utc).isoformat(), updated_by=str(actor))
        return self.persist(doc, raw, actor)


def priced_plan(plan, prices):
    row = dict(plan)
    if str(row.get('product_category') or 'standard').lower() != 'standard':
        return row
    cycles = cycle_limit(row.get('payout_cycles') if row.get('payout_cycles') is not None else 2)
    extra = 0 if cycles in (1, 2) else prices[str(cycles)]
    if extra is None:
        raise ValueError('Set the extra cost for this payout cycle option in Business Settings before selling this plan')
    base = money(row.get('base_fee', row.get('fee', 0)))
    row.update(base_fee=base, payout_cycle_extra=extra, fee=round(base + extra, 2))
    return row


def reset_price(plan, stage):
    if not plan:
        return 0.0
    if stage != 'funded':
        return money(plan.get('reset_fee') or 0)
    value = plan.get('funded_reset_fee')
    # NULL is the legacy automatic rule; explicit zero means Not available.
    return money(value) if value is not None else round(money(plan.get('base_fee', plan.get('fee') or plan.get('price') or 0)) * 2, 2)


def register_cycle_prices(app, store, require_admin, invalidate, audit):
    @app.get('/plan_commercial_version')
    def plan_commercial_version():
        return jsonify(success=True, release=RELEASE, default_cycles=2, free_cycle_options=[1, 2], paid_cycle_options=[3, 4, 5, 0])

    @app.route('/admin/standard_cycle_prices', methods=['GET', 'POST'])
    def standard_cycle_prices():
        admin, error = require_admin()
        if error is not None:
            return error
        if str(admin.get('role') or '').lower() not in ('owner', 'super_admin', 'superadmin'):
            return jsonify(success=False, error='Owner or super admin access required'), 403
        try:
            if request.method == 'GET':
                return jsonify(success=True, data=store.read(fresh=True)[0])
            doc = store.save(request.get_json(silent=True), admin.get('id'))
            invalidate('public_plans_snapshot', 'plans_list', 'admin_bootstrap')
            audit('business_settings', 'standard_cycle_prices_update', str(doc), admin, KEY)
            return jsonify(success=True, data=doc)
        except Conflict as exc:
            return jsonify(success=False, error=str(exc)), 409
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503
