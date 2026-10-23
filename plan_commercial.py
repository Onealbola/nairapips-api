"""Server-owned Standard/Premium cycle surcharges and reset prices."""
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from flask import jsonify, request
from production_controls import ControlsStore, ControlsUnavailable, Conflict
from plan_cycles import cycle_limit

KEY = 'standard_plan_cycle_prices_v1'
# Keep the stored key format compatible; 3/4 prices are retained but no longer charged.
PRICE_KEYS = ('3', '4', '5', '0')
DEFAULT_CYCLES = 4
FREE_CYCLES = (1, 2, 3, 4)
RELEASE = 'four-included-cycles-2026-10-23'


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
    return dict(revision=0, prices={k: None for k in PRICE_KEYS}, updated_at=None, updated_by=None)


def validate(doc):
    if not isinstance(doc, dict) or set(doc) != set(defaults()) or type(doc['revision']) is not int or doc['revision'] < 0:
        raise ValueError('Invalid cycle pricing settings')
    if not isinstance(doc['prices'], dict) or set(doc['prices']) != set(PRICE_KEYS):
        raise ValueError('Set prices for 5 and unlimited cycles')
    prices = {k: None if v is None else money(v) for k, v in doc['prices'].items()}
    if any(v is not None and v <= 0 for v in prices.values()):
        raise ValueError('Extra cycles require a positive price; leave blank to make that upgrade unavailable')
    return {**doc, 'prices': prices}


class CyclePriceStore(ControlsStore):
    def __init__(self, transport, url, key, category='standard'):
        if category not in {'standard', 'premium'}:
            raise ValueError('Choose Standard or Premium')
        self.category = category
        def checked(doc):
            try:
                return validate(doc)
            except ValueError as exc:
                raise ControlsUnavailable('Saved cycle prices are invalid') from exc
        super().__init__(transport, url, key, category + '_plan_cycle_prices_v1', defaults, checked)

    def save(self, body, actor):
        if not isinstance(body, dict) or set(body) != {'revision', 'prices'}:
            raise ValueError('Submit the loaded revision and cycle prices')
        old, raw = self.read(fresh=True)
        if type(body['revision']) is not int or body['revision'] != old['revision']:
            raise Conflict('Cycle prices changed in another session. Reload before saving.')
        doc = validate({**body, 'updated_at': None, 'updated_by': None})
        doc.update(revision=old['revision'] + 1, updated_at=datetime.now(timezone.utc).isoformat(), updated_by=str(actor))
        return self.persist(doc, raw, actor)


def priced_plan(plan, prices, category='standard'):
    row = dict(plan)
    if str(row.get('product_category') or 'standard').lower() != category:
        return row
    cycles = cycle_limit(row.get('payout_cycles') if row.get('payout_cycles') is not None else DEFAULT_CYCLES)
    extra = 0 if cycles in FREE_CYCLES else prices[str(cycles)]
    if extra is None:
        raise ValueError('Set the extra cost for this payout cycle option in Business Settings before selling this plan')
    base = money(row.get('base_fee', row.get('fee', 0)))
    row.update(base_fee=base, payout_cycle_extra=extra, fee=round(base + extra, 2))
    return row


def purchase_plan(plan, prices, cycles=DEFAULT_CYCLES):
    """Price a buyer's selection without changing the saved product or past sales."""
    cycles = cycle_limit(DEFAULT_CYCLES if cycles is None else cycles)
    category = str(plan.get('product_category') or 'standard').lower()
    if category not in {'standard', 'premium'}:
        raise ValueError('Unknown plan market')
    return priced_plan({**plan, 'payout_cycles': cycles}, prices, category=category)


def purchase_catalog(plan, prices):
    row = purchase_plan(plan, prices, DEFAULT_CYCLES)
    options = []
    for cycles in (1, 2, 3, 4, 5, 0):
        try:
            selected = purchase_plan(plan, prices, cycles)
            options.append(dict(cycles=cycles, available=True, extra=selected['payout_cycle_extra'], fee=selected['fee']))
        except ValueError:
            options.append(dict(cycles=cycles, available=False, extra=None, fee=None))
    return {**row, 'cycle_options': options, 'default_cycles': DEFAULT_CYCLES}


def resolve_purchase_plan(stores, plan_id, cycles=DEFAULT_CYCLES):
    if not plan_id or len(str(plan_id)) > 100:
        raise ValueError('Choose a plan')
    store = stores['standard']
    response = store.transport.get(store.url.rsplit('/', 1)[0] + '/challenge_plans',
        headers=store.headers, params={'id': 'eq.' + str(plan_id), 'select': '*', 'limit': '1'}, timeout=(2, 4))
    response.raise_for_status()
    rows = response.json()
    if not rows or str(rows[0].get('status') or '').lower() != 'active':
        raise ValueError('This product is not open for new purchases')
    category = str(rows[0].get('product_category') or 'standard').lower()
    if category not in stores:
        raise ValueError('Unknown plan market')
    prices = stores[category].read(fresh=True)[0]['prices']
    return purchase_plan(rows[0], prices, cycles), purchase_catalog(rows[0], prices)


def reset_price(plan, stage):
    if not plan:
        return 0.0
    if stage != 'funded':
        return money(plan.get('reset_fee') or 0)
    value = plan.get('funded_reset_fee')
    # NULL is the legacy automatic rule; explicit zero means Not available.
    return money(value) if value is not None else round(money(plan.get('base_fee', plan.get('fee') or plan.get('price') or 0)) * 2, 2)


def register_cycle_prices(app, store, require_admin, invalidate, audit, premium_store=None):
    stores = {'standard': store}
    if premium_store is not None:
        stores['premium'] = premium_store
    @app.get('/plan_commercial_version')
    def plan_commercial_version():
        return jsonify(success=True, release=RELEASE, buyer_selectable=True, markets=sorted(stores), default_cycles=DEFAULT_CYCLES, free_cycle_options=list(FREE_CYCLES), paid_cycle_options=[5, 0])

    @app.get('/plan_purchase_options')
    def plan_purchase_options():
        try:
            selected, catalog = resolve_purchase_plan(stores, request.args.get('plan_id'), request.args.get('payout_cycles', DEFAULT_CYCLES))
            response = jsonify(success=True, data={**catalog, **selected})
            response.headers['Cache-Control'] = 'no-store'
            return response
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except Exception:
            return jsonify(success=False, error='Current purchase prices could not be verified. Please retry.'), 503

    @app.route('/admin/<category>_cycle_prices', methods=['GET', 'POST'])
    def standard_cycle_prices(category):
        if category not in stores:
            return jsonify(success=False, error='Unknown plan market'), 404
        selected = stores[category]
        admin, error = require_admin()
        if error is not None:
            return error
        if str(admin.get('role') or '').lower() not in ('owner', 'super_admin', 'superadmin'):
            return jsonify(success=False, error='Owner or super admin access required'), 403
        try:
            if request.method == 'GET':
                return jsonify(success=True, data=selected.read(fresh=True)[0], category=category)
            doc = selected.save(request.get_json(silent=True), admin.get('id'))
            invalidate('public_plans_snapshot', 'plans_list', 'admin_bootstrap')
            audit('business_settings', category + '_cycle_prices_update', str(doc), admin, selected.setting_key)
            return jsonify(success=True, data=doc, category=category)
        except Conflict as exc:
            return jsonify(success=False, error=str(exc)), 409
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503
