"""Validated owner plan editing against real production plan rows.

A database trigger freezes missing old purchase snapshots atomically on edit.
Owners can edit current plans; existing customers retain their purchased terms.
"""
from decimal import Decimal, InvalidOperation
from flask import jsonify, request

FIELDS = {
    'name', 'description', 'status', 'fee', 'account_size', 'phase1_target',
    'phase2_target', 'max_drawdown', 'payout_split', 'reset_fee', 'funded_reset_fee',
    'challenge_journey', 'drawdown_type', 'payout_cycles', 'product_category',
}
RULE_FIELDS = {'fee', 'reset_fee', 'phase1_target', 'phase2_target', 'max_drawdown', 'payout_split',
               'account_size', 'challenge_journey', 'drawdown_type', 'payout_cycles'}


def number(value, name, minimum, maximum):
    if isinstance(value, bool):
        raise ValueError(name + ' must be a number')
    try:
        x = Decimal(str(value))
        if not x.is_finite() or not minimum <= x <= maximum:
            raise ValueError(name + ' is outside the supported range')
        if x.as_tuple().exponent < -2:
            raise ValueError(name + ' supports at most two decimal places')
        return float(x)
    except (InvalidOperation, TypeError):
        raise ValueError(name + ' must be a number')


def validated_plan(current, changes):
    if not isinstance(changes, dict) or not changes or set(changes) - FIELDS:
        raise ValueError('Unknown or empty plan fields')
    row = {**current, **changes}
    if changes.get('status') == 'active' and current.get('status') != 'active':
        # Releasing an inactive draft validates its actual complete rules, not
        # just the status checkbox. An old unsupported draft cannot bypass this.
        release_fields = {k: row.get(k) for k in RULE_FIELDS if k not in ('challenge_journey', 'payout_cycles')}
        release_fields['reset_fee'] = row.get('reset_fee') or 0
        raw_route = row.get('challenge_journey')
        release_fields['challenge_journey'] = 'two_phase' if 'phase2' in str(raw_route).lower() or 'two' in str(raw_route).lower() or (not raw_route and float(row.get('phase2_target') or 0) > 0) else 'one_phase'
        if release_fields['challenge_journey'] == 'one_phase' and release_fields['phase2_target'] is None:
            release_fields['phase2_target'] = 0
        validated_plan({}, release_fields)
    if 'product_category' in changes and changes['product_category'] not in {'standard', 'premium'}:
        raise ValueError('Choose Standard or Premium')
    result = dict(changes)
    if 'funded_reset_fee' in changes:
        from plan_commercial import money
        result['funded_reset_fee'] = None if changes['funded_reset_fee'] is None else money(changes['funded_reset_fee'])
    if "payout_cycles" in changes:
        from plan_cycles import cycle_limit
        result["payout_cycles"] = cycle_limit(changes["payout_cycles"])
    for name, lo, hi in [('fee', .01, 10000000), ('account_size', 1, 1000000000),
                         ('phase1_target', .01, 100), ('phase2_target', 0, 100),
                         ('max_drawdown', .01, 99.99), ('reset_fee', 0, 10000000)]:
        if name in changes:
            result[name] = number(changes[name], name, Decimal(str(lo)), Decimal(str(hi)))
    if 'payout_split' in changes:
        result['payout_split'] = number(changes['payout_split'], 'payout_split', Decimal('0.01'), 100)
    if 'drawdown_type' in changes and str(changes['drawdown_type']).upper() != 'STATIC':
        raise ValueError('Only static drawdown is production verified; trailing cannot be released yet')
    if 'drawdown_type' in result:
        result['drawdown_type'] = 'STATIC'
    if 'status' in changes and changes['status'] not in ('active', 'inactive'):
        raise ValueError('Plan status must be active or inactive')
    if 'name' in changes and (not isinstance(changes['name'], str) or not 1 <= len(changes['name'].strip()) <= 100):
        raise ValueError('Plan name must contain 1–100 characters')
    if 'description' in changes and (not isinstance(changes['description'], str) or len(changes['description']) > 4000):
        raise ValueError('Description must contain at most 4000 characters')
    raw_journey = row.get('challenge_journey')
    journey = raw_journey if raw_journey in ('one_phase', 'two_phase') else ('two_phase' if 'phase2' in str(raw_journey).lower() or 'two' in str(raw_journey).lower() or (not raw_journey and float(row.get('phase2_target') or 0) > 0) else 'one_phase')
    if 'challenge_journey' in changes and raw_journey not in ('one_phase', 'two_phase'):
        raise ValueError('Choose one_phase or two_phase')
    if journey == 'one_phase' and float(result.get('phase2_target', row.get('phase2_target') or 0)) != 0:
        raise ValueError('One-phase plans must have a zero Phase 2 target')
    if journey == 'two_phase' and float(result.get('phase2_target', row.get('phase2_target') or 0)) <= 0:
        raise ValueError('Two-phase plans need a positive Phase 2 target')
    return result


def same_value(key, left, right):
    if key == 'challenge_journey':
        def route(value):
            text = str(value or '').lower()
            return 'two_phase' if 'phase2' in text or 'two' in text else 'one_phase'
        return route(left) == route(right)
    if key in {'fee', 'reset_fee', 'account_size', 'phase1_target', 'phase2_target', 'max_drawdown', 'payout_split'}:
        try:
            return Decimal(str(left)) == Decimal(str(right))
        except InvalidOperation:
            return False
    return left == right


def register_catalog_controls(app, transport, url, key, require_admin, invalidate, audit):
    base = url.rstrip('/') + '/rest/v1/'
    headers = {'apikey': key, 'Authorization': 'Bearer ' + key}

    def auth():
        admin, error = require_admin()
        if error is not None:
            return None, error
        if str(admin.get('role') or '').lower() not in ('owner', 'super_admin', 'superadmin'):
            return None, (jsonify(success=False, error='Owner or super admin access required'), 403)
        return admin, None

    def read(table, params):
        r = transport.get(base + table, headers=headers, params=params, timeout=(2, 5))
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list):
            raise RuntimeError('Invalid catalog response')
        return rows

    @app.get('/admin/production_catalog')
    def production_catalog_get():
        _, error = auth()
        if error is not None:
            return error
        try:
            plans = read('challenge_plans', {'select': 'id,updated_at,' + ','.join(sorted(FIELDS)),
                                            'order': 'account_size.asc,id.asc', 'limit': '1001'})
            if len(plans) > 1000:
                raise RuntimeError('Catalog too large for a complete edit view')
            return jsonify(success=True, plans=plans)
        except Exception:
            return jsonify(success=False, error='The complete catalog could not be loaded'), 503

    @app.post('/admin/production_catalog')
    def production_catalog_save():
        admin, error = auth()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or set(body) != {'id', 'updated_at', 'changes'}:
            return jsonify(success=False, error='Submit the loaded plan identity, timestamp and changed fields'), 400
        try:
            rows = read('challenge_plans', {'select': '*', 'id': 'eq.' + str(body['id']), 'limit': '1'})
            if not rows:
                return jsonify(success=False, error='Plan not found'), 404
            current = rows[0]
            if current.get('updated_at') != body['updated_at']:
                return jsonify(success=False, error='Plan changed in another session. Reload before editing.'), 409
            changes = validated_plan(current, body['changes'])
            # np_preserve_sold_plan_terms freezes historical snapshots atomically.
            from datetime import datetime, timezone
            changes['updated_at'] = datetime.now(timezone.utc).isoformat()
            compare = 'is.null' if body['updated_at'] is None else 'eq.' + str(body['updated_at'])
            r = transport.patch(base + 'challenge_plans',
                headers=dict(headers, Prefer='return=representation'),
                params={'id': 'eq.' + str(body['id']), 'updated_at': compare}, json=changes, timeout=(2, 5))
            r.raise_for_status()
            saved = r.json()
            if not saved:
                return jsonify(success=False, error='Plan changed while saving. Reload first.'), 409
            invalidate('public_plans_snapshot', 'plans_list', 'admin_bootstrap')
            audit('production_catalog', 'plan_configuration_update',
                  str({'plan_id': body['id'], 'changes': changes}), admin, str(body['id']))
            return jsonify(success=True, plan={k: v for k, v in saved[0].items() if k in FIELDS | {'id', 'updated_at'}})
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except Exception:
            return jsonify(success=False, error='Save could not be confirmed. Reload before retrying.'), 503

    @app.post('/admin/production_catalog/clone')
    def production_catalog_clone():
        admin, error = auth()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or set(body) != {'id', 'name'}:
            return jsonify(success=False, error='Select a source plan and the new plan name'), 400
        try:
            rows = read('challenge_plans', {'select': '*', 'id': 'eq.' + str(body['id']), 'limit': '1'})
            if not rows:
                return jsonify(success=False, error='Source plan not found'), 404
            changes = validated_plan(rows[0], {'name': body['name'], 'status': 'inactive'})
            from datetime import datetime, timezone
            from uuid import uuid4
            # Stable UUID lets the operator check an uncertain response without retrying creation.
            new_id = str(uuid4())
            clone = {k: v for k, v in rows[0].items() if k not in {'id', 'created_at', 'updated_at'}}
            clone.update(changes)
            clone.update(second_life_enabled=False, lives_total=1)
            clone.update(id=new_id, created_at=datetime.now(timezone.utc).isoformat(),
                         updated_at=datetime.now(timezone.utc).isoformat())
            r = transport.post(base + 'challenge_plans', headers=dict(headers, Prefer='return=representation'),
                               json=clone, timeout=(2, 5))
            r.raise_for_status()
            if not r.json():
                raise RuntimeError('Plan creation was not confirmed')
            invalidate('public_plans_snapshot', 'plans_list', 'admin_bootstrap')
            audit('production_catalog', 'inactive_plan_created',
                  str({'source_plan': body['id'], 'new_plan': new_id}), admin, new_id)
            return jsonify(success=True, id=new_id, message='New plan created inactive. Configure it before release.')
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except Exception:
            return jsonify(success=False, error='Creation could not be confirmed. Reload the catalog before trying again.'), 503


    @app.before_request
    def protect_legacy_catalog_writes():
        if request.method != 'POST' or request.path not in {'/create_challenge_plan', '/update_challenge_plan', '/delete_challenge_plan'}:
            return None
        _, error = auth()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return jsonify(success=False, error='Invalid plan data'), 400
        try:
            current = {}
            if request.path != '/create_challenge_plan':
                if not body.get('id'):
                    return jsonify(success=False, error='Plan identity is required'), 400
                rows = read('challenge_plans', {'select': '*', 'id': 'eq.' + str(body['id']), 'limit': '1'})
                if not rows:
                    return jsonify(success=False, error='Plan not found'), 404
                current = rows[0]
            if request.path != '/delete_challenge_plan':
                changes = {k: v for k, v in body.items() if k in FIELDS}
                if changes:
                    validated_plan(current, changes)
            else:
                changes = {'delete': True}
            # Editing is supported for sold products; deleting their history is not.
            if current and request.path == '/delete_challenge_plan':
                purchases = read('challenge_purchases', {'select': 'id', 'plan_id': 'eq.' + str(body['id']), 'limit': '1'})
                if purchases:
                    return jsonify(success=False, error='This product has purchase history and cannot be deleted. Pause new purchases instead.'), 409
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except Exception:
            return jsonify(success=False, error='Plan safety checks could not be completed. Please retry.'), 503

    @app.after_request
    def refresh_saved_plan_catalog(response):
        if request.method == 'POST' and request.path in {'/create_challenge_plan', '/update_challenge_plan'} and 200 <= response.status_code < 300:
            data = response.get_json(silent=True)
            if isinstance(data, dict) and data.get('success') is True:
                invalidate('public_plans_snapshot', 'plans_list', 'admin_bootstrap')
        return response
