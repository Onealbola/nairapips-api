"""Real, versioned settings and server pricing for the future package product.

Commercial configuration is independent of legacy plans. Saving a tariff never
activates checkout or alters an issued account. Production release requires the
contract/entitlement database and every runtime consumer, not a cosmetic switch.
"""
import copy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from flask import jsonify, request
from production_controls import ControlsStore, ControlsUnavailable, Conflict

KEY = 'future_package_configuration_v1'
CHOICES = {
    'capital': ['100000', '250000', '500000', '750000', '1000000', '2000000'],
    'phases': ['one', 'two'], 'drawdown': ['5', '10', '15', '20'],
    'drawdown_type': ['static', 'trailing'], 'phase1_target': ['5', '8', '10', '15', '20'],
    'phase2_target': ['5', '8', '10', '15', '20'], 'profit_share': ['60', '70', '80'],
    'profit_cap': ['20', '30', '50', 'none'], 'payout_cycles': ['1', '2', '3', '4', '5', 'unlimited'],
    'phase_resets': ['0', '1', '2'], 'funded_resets': ['0', '1', '2'],
}
LABELS = dict(capital='Account size', phases='Evaluation phases', drawdown='Maximum drawdown %',
              drawdown_type='Drawdown type', phase1_target='Phase 1 target %', phase2_target='Phase 2 target %',
              profit_share='Trader profit share %', profit_cap='Gross profit cap per funded cycle %',
              payout_cycles='Paid funded cycles', phase_resets='Included phase resets', funded_resets='Included funded resets')


def defaults():
    return dict(revision=0, enabled=False, minimum_price=None, updated_at=None, updated_by=None,
                options={key: [{'value': value, 'enabled': False, 'price': None} for value in values]
                         for key, values in CHOICES.items()})


def decimal_value(value, maximum):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('Prices must be numbers')
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0 or amount > maximum or amount.as_tuple().exponent < -2:
            raise ValueError('Enter a number from 0 to ' + str(maximum) + ', with no more than two decimal places')
        return float(amount)
    except InvalidOperation:
        raise ValueError('Enter a valid number')


def option_value(key, value):
    if not isinstance(value, str):
        raise ValueError('Option values must be text')
    if key in ('phases', 'drawdown_type'):
        if value not in CHOICES[key]:
            raise ValueError('Unsupported ' + key)
        return value
    if (key == 'profit_cap' and value == 'none') or (key == 'payout_cycles' and value == 'unlimited'):
        return value
    limits = {'capital': (1000, 1000000000), 'drawdown': (.01, 99.99),
              'phase1_target': (.01, 100), 'phase2_target': (.01, 100),
              'profit_share': (.01, 100), 'profit_cap': (.01, 1000),
              'payout_cycles': (1, 1000), 'phase_resets': (0, 100), 'funded_resets': (0, 100)}
    lo, hi = limits[key]
    parsed = decimal_value(value, hi)
    if parsed is None or parsed < lo:
        raise ValueError('Invalid ' + key + ' option')
    if key in ('capital', 'payout_cycles', 'phase_resets', 'funded_resets') and parsed != int(parsed):
        raise ValueError(key + ' must be a whole number')
    return format(Decimal(str(parsed)).normalize(), 'f')


def validate(doc):
    if not isinstance(doc, dict) or set(doc) != set(defaults()):
        raise ValueError('Invalid package settings fields')
    if type(doc['revision']) is not int or doc['revision'] < 0 or type(doc['enabled']) is not bool:
        raise ValueError('Invalid version or release checkbox')
    doc = copy.deepcopy(doc)
    try:
        doc['minimum_price'] = decimal_value(doc['minimum_price'], 10000000)
    except ValueError as exc:
        raise ValueError('Minimum purchase price: ' + str(exc)) from exc
    options = doc['options']
    if not isinstance(options, dict) or set(options) != set(CHOICES):
        raise ValueError('All package facilities must be supplied')
    for key, expected in CHOICES.items():
        rows = options[key]
        if not isinstance(rows, list) or not 1 <= len(rows) <= 30:
            raise ValueError('Invalid option list for ' + key)
        seen = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) not in ({'value', 'enabled', 'price'}, {'value', 'enabled', 'price', 'price_type'}):
                raise ValueError('Invalid choice for ' + key)
            try:
                row['value'] = option_value(key, row['value'])
            except ValueError as exc:
                raise ValueError(LABELS[key] + ' — benefit value ' + str(row['value']) + ': ' + str(exc)) from exc
            if row['value'] in seen or type(row['enabled']) is not bool:
                raise ValueError('Duplicate or invalid option for ' + key)
            seen.add(row['value'])
            mode = row.get('price_type', 'fixed' if key == 'capital' else 'percent')
            if mode not in ('fixed', 'percent') or (key == 'capital' and mode != 'fixed'):
                raise ValueError('Choose fixed or percent pricing; capital uses a fixed base price')
            if 'price_type' in row:
                row['price_type'] = mode
            try:
                row['price'] = decimal_value(row['price'], 10000000 if mode == 'fixed' else 1000)
            except ValueError as exc:
                unit = '₦ amount' if mode == 'fixed' else '% of base price'
                raise ValueError(LABELS[key] + ' — choice ' + row['value'] + ', charge ' + str(row['price']) + ' (' + unit + '): ' + str(exc)) from exc
            if row['enabled'] and row['price'] is None:
                raise ValueError(LABELS[key] + ' — choice ' + row['value'] + ': enter a charge (0 means included free), or untick Active')
            if key == 'capital' and row['enabled'] and row['price'] <= 0:
                raise ValueError('Enabled capital needs a positive base price')
    return doc


def quote(config, choices):
    config = validate(config)
    if not isinstance(choices, dict) or set(choices) - set(CHOICES):
        raise ValueError('Unknown package selectors')
    if config['minimum_price'] is None or config['minimum_price'] <= 0:
        raise ValueError('Set the minimum purchase price first')
    selected, additions, base = {}, [], None
    for key in CHOICES:
        if key == 'phase2_target' and choices.get('phases') == 'one':
            if choices.get(key) not in (None, ''):
                raise ValueError('One-phase packages cannot contain a Phase 2 target')
            selected[key] = None
            continue
        value = choices.get(key)
        row = next((r for r in config['options'][key] if r['value'] == value and r['enabled']), None)
        if row is None:
            raise ValueError('This choice is not enabled: ' + key)
        selected[key] = value
        if key == 'capital':
            base = Decimal(str(row['price']))
        else:
            additions.append(dict(facility=key, value=value, percent=row['price'] if row.get('price_type', 'percent') == 'percent' else 0, fixed=row['price'] if row.get('price_type') == 'fixed' else 0, price_type=row.get('price_type', 'percent')))
    rate = sum((Decimal(str(a['percent'])) for a in additions), Decimal(0))
    fixed = sum((Decimal(str(a['fixed'])) for a in additions), Decimal(0))
    # Match the documented private builder: aggregate percentage of capital base,
    # rounded UP to the next NGN100; the minimum is an independently saved floor.
    total = max(Decimal(str(config['minimum_price'])),
                ((base * (1 + rate / 100) + fixed) / 100).to_integral_value(rounding=ROUND_CEILING) * 100)
    return dict(selection=selected, base=float(base), additions=additions, total=float(total),
                currency='NGN', catalog_revision=config['revision'], checkout_available=False)


class PackageStore(ControlsStore):
    def __init__(self, transport, url, key, readiness=None):
        self.readiness = readiness
        def checked(doc):
            try:
                return validate(doc)
            except ValueError as exc:
                raise ControlsUnavailable('Stored package settings are invalid') from exc
        super().__init__(transport, url, key, KEY, defaults, checked)

    def save(self, body, actor):
        if not isinstance(body, dict) or set(body) != {'revision', 'enabled', 'minimum_price', 'options'}:
            raise ValueError('Submit the loaded revision, release checkbox, minimum price and all options')
        old, raw = self.read(fresh=True)
        if body['revision'] != old['revision']:
            raise Conflict('Package settings changed in another session. Reload before saving.')
        doc = validate({**body, 'updated_at': None, 'updated_by': None})
        for row in doc['options']['payout_cycles']:
            if row['enabled'] and (row['value'] == 'unlimited' or int(row['value']) > 2) and not row['price']:
                raise ValueError('Extra funded cycles (' + row['value'] + ') need a positive upfront charge')
        if doc['enabled']:
            if self.readiness is None or not self.readiness()['ready']:
                raise Conflict('Package checkout remains locked until contracts, cycle/reset ledgers and running monitoring/DD consumers are verified.')
            if doc['minimum_price'] is None or doc['minimum_price'] <= 0:
                raise ValueError('Set a minimum package price before release')
            for facility, choices in doc['options'].items():
                if facility == 'phase2_target' and not any(r['enabled'] and r['value']=='two' for r in doc['options']['phases']):
                    continue
                if not any(r['enabled'] for r in choices):
                    raise ValueError('Enable at least one choice for ' + facility)
        doc.update(revision=old['revision'] + 1, updated_at=datetime.now(timezone.utc).isoformat(), updated_by=str(actor))
        return self.persist(doc, raw, actor)


def register_package_configuration(app, store, require_admin, audit):
    def owner():
        admin, error = require_admin()
        if error is not None:
            return None, error
        if str(admin.get('role') or '').lower() not in {'owner', 'super_admin', 'superadmin'}:
            return None, (jsonify(success=False, error='Owner or super admin access required'), 403)
        return admin, None

    @app.route('/admin/package_configuration', methods=['GET', 'POST'])
    def package_configuration():
        admin, error = owner()
        if error is not None:
            return error
        try:
            if request.method == 'POST':
                doc = store.save(request.get_json(silent=True), admin.get('id') or 'owner')
                audit('package_configuration', 'future_product_configuration_saved',
                      str({'revision': doc['revision']}), admin, KEY)
            else:
                doc, _ = store.read(fresh=True)
            readiness = store.readiness() if store.readiness is not None else dict(ready=False, blockers=['Immutable contract checkout', 'Paid-cycle and reset entitlement ledgers', 'Verified monitoring and DD police consumers'])
            return jsonify(success=True, data=doc, labels=LABELS,
                           price_rule='Capital base + percentage additions of that base + fixed NGN additions; round up to NGN100, then apply the minimum price.',
                           release_ready=readiness['ready'],release_blockers=readiness['blockers'])
        except Conflict as exc:
            return jsonify(success=False, error=str(exc)), 409
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503

    @app.post('/admin/package_configuration/quote')
    def package_quote_preview():
        _, error = owner()
        if error is not None:
            return error
        try:
            config, _ = store.read(fresh=True)
            return jsonify(success=True, data=quote(config, request.get_json(silent=True)))
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503
