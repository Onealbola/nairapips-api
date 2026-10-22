"""Owner-operated release controls. No account, risk or settlement rewrites.

The single versioned business_settings row is authoritative across workers.
Only new customer actions are gated; history, telemetry, risk enforcement and
already accepted obligations deliberately remain available.
"""
import copy
import json
import threading
import time
from datetime import datetime, timezone

from flask import jsonify, request

SETTING_KEY = 'production_controls_v1'
FEATURES = {
    'registrations': ('New trader registrations', True, ['/register_trader']),
    'challenge_sales': ('New challenge purchases', True, ['/create_challenge_purchase']),
    'reset_sales': ('New paid reset purchases — phase and funded', True, ['/create_reset_purchase']),
    'payout_requests': ('New payout requests', True, ['/create_payout']),
    'referral_withdrawals': ('New referral withdrawal requests', True, ['/request_affiliate_payout', '/np2_referral/payout_request']),
    'private_offers': ('Claiming private offers', True, ['/claim_private_offer']),
    'affiliate_signup': ('New referral partner enrolment', True, ['/affiliate_signup']),
    'lead_capture': ('New lead submissions', True, ['/register_lead']),
    'fxblue_registration': ('New FXBlue connections', True, ['/register_fxblue']),
    'announcements': ('Customer announcements', True, ['/trader_announcements', '/trader_announcements_v159']),
    'support_tickets': ('New support tickets', True, ['/create_support_ticket']),
}
# These are not releasable by a browser flag: they require contract, ledger and
# deployed consumer support. Advertising an available dropdown is insufficient.
UNRELEASED = {
    'package_builder': 'Package checkout requires verified account lifecycle, payout and running DD integrations before release.',
    'equity_trailing': 'The running DD fleet has not been verified for equity trailing.',
    'included_reset_budgets': 'The reset allowance ledger is installed; account runtime integration is not yet verified.',
    'limited_payout_cycles': 'The paid-cycle ledger is installed; payout and renewal runtime integration is not yet verified.',
}
PROTECTED = ['Live MT5 collection', 'DD police on issued accounts',
             'Recorded purchase rules', 'Existing payout review and settlement',
             'Approved assignment and renewal obligations', 'Account and payment history']


class ControlsUnavailable(RuntimeError):
    pass


class Conflict(ValueError):
    pass


def defaults():
    return {'revision': 0, 'features': {k: v[1] for k, v in FEATURES.items()},
            'updated_at': None, 'updated_by': None}


def validate_document(value):
    if not isinstance(value, dict) or set(value) != set(defaults()):
        raise ControlsUnavailable('Invalid production settings document')
    if type(value['revision']) is not int or value['revision'] < 0:
        raise ControlsUnavailable('Invalid production settings revision')
    flags = value['features']
    if not isinstance(flags, dict) or set(flags) - set(FEATURES) or any(type(v) is not bool for v in flags.values()):
        raise ControlsUnavailable('Invalid production feature values')
    value = copy.deepcopy(value)
    value['features'] = {**defaults()['features'], **flags}
    return value


class ControlsStore:
    def __init__(self, transport, url, key, setting_key=SETTING_KEY, default_factory=defaults, validator=validate_document):
        self.setting_key, self.default_factory, self.validator = setting_key, default_factory, validator
        self.transport, self.url = transport, url.rstrip('/') + '/rest/v1/business_settings'
        self.headers = {'apikey': key, 'Authorization': 'Bearer ' + key}
        self.lock = threading.Lock()
        self.cached = None
        self.cached_at = 0

    def read(self, fresh=False):
        if not fresh and self.cached and time.monotonic() - self.cached_at < 3:
            return copy.deepcopy(self.cached)
        if not self.lock.acquire(timeout=0.2):
            raise ControlsUnavailable('Settings are being refreshed; retry shortly')
        try:
            r = self.transport.get(self.url, headers=self.headers,
                params={'select': 'value,updated_by', 'key': 'eq.' + self.setting_key, 'limit': '1'}, timeout=(2, 4))
            r.raise_for_status()
            rows = r.json()
            if not isinstance(rows, list):
                raise ControlsUnavailable('Invalid settings response')
            raw = rows[0]['value'] if rows else None
            doc = self.validator(json.loads(raw)) if raw is not None else self.default_factory()
            token = {'value': raw, 'updated_by': rows[0].get('updated_by')} if rows else None
            result = (doc, token)
            self.cached, self.cached_at = copy.deepcopy(result), time.monotonic()
            return result
        except ControlsUnavailable:
            raise
        except Exception as exc:
            raise ControlsUnavailable('Production settings could not be read') from exc
        finally:
            self.lock.release()

    def save(self, flags, revision, actor):
        if type(revision) is not int or not isinstance(flags, dict) or set(flags) != set(FEATURES):
            raise ValueError('Supply the loaded revision and all feature checkboxes')
        if any(type(v) is not bool for v in flags.values()):
            raise ValueError('Checkbox values must be true or false')
        before, raw = self.read(fresh=True)
        if revision != before['revision']:
            raise Conflict('Settings changed in another session. Reload before saving.')
        now = datetime.now(timezone.utc).isoformat()
        doc = dict(revision=revision + 1, features=flags, updated_at=now, updated_by=str(actor))
        return self.persist(doc, raw, actor)

    def persist(self, doc, raw, actor):
        now = doc['updated_at']
        value = json.dumps(doc, sort_keys=True, separators=(',', ':'))
        headers = dict(self.headers, Prefer='return=representation')
        from uuid import uuid4
        row = dict(key=self.setting_key, value=value, updated_at=now, updated_by='np-config-write:' + str(uuid4()))
        try:
            if raw is None:
                r = self.transport.post(self.url, headers=headers, json=row, timeout=(3, 12))
                if r.status_code == 409:
                    raise Conflict('Settings were created in another session. Reload before saving.')
            else:
                compare = {'key': 'eq.' + self.setting_key}
                if str(raw.get('updated_by') or '').startswith('np-config-write:'):
                    # A compact unique token avoids putting a large tariff JSON
                    # document in the URL while preserving atomic compare/swap.
                    compare['updated_by'] = 'eq.' + raw['updated_by']
                else:
                    compare['value'] = 'eq.' + raw['value']
                r = self.transport.patch(self.url, headers=headers,
                    params=compare, json=row, timeout=(3, 12))
            r.raise_for_status()
            rows = r.json()
            if not rows:
                raise Conflict('Settings changed in another session. Reload before saving.')
        except (Conflict, ValueError):
            raise
        except Exception as exc:
            # Read back an ambiguous write once; never repeat the mutation.
            self.cached = None
            try:
                confirmed, _ = self.read(fresh=True)
                if confirmed == doc:
                    return confirmed
            except ControlsUnavailable:
                pass
            response = getattr(exc, 'response', None)
            status = getattr(response, 'status_code', None)
            if status is not None:
                # Never include database response text, URLs, or credentials.
                reason = {401: 'database authentication failed',
                          403: 'database write access was denied',
                          400: 'the database rejected the settings'}.get(status, 'database request failed')
                raise ControlsUnavailable('Settings were not confirmed: ' + reason +
                                          ' (HTTP ' + str(status) + '). Keep this page open; your entries have not been cleared.') from exc
            raise ControlsUnavailable('The database save response timed out or was interrupted, and the changes could not be verified. Keep this page open and retry Save; do not reload.') from exc
        self.cached = None
        confirmed, _ = self.read(fresh=True)
        if confirmed != doc:
            raise Conflict('The saved settings changed again. Reload to see the current state.')
        return confirmed


def register_controls(app, store, require_admin, invalidate, audit):
    def owner():
        admin, error = require_admin()
        if error is not None:
            return None, error
        if str(admin.get('role') or '').lower() not in {'super_admin', 'superadmin', 'owner'}:
            return None, (jsonify(success=False, error='Only the owner or super admin may change production settings'), 403)
        return admin, None

    def payload(doc):
        return dict(success=True, data=doc,
                    facilities=[{'key': k, 'label': v[0]} for k, v in FEATURES.items()],
                    unreleased=UNRELEASED, protected=PROTECTED, propagation_seconds=3)

    @app.get('/admin/production_controls')
    def production_controls_get():
        _, error = owner()
        if error is not None:
            return error
        try:
            return jsonify(payload(store.read(fresh=True)[0]))
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503

    @app.post('/admin/production_controls')
    def production_controls_save():
        admin, error = owner()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict) or set(body) != {'revision', 'features'}:
            return jsonify(success=False, error='Only revision and feature checkboxes may be submitted'), 400
        try:
            doc = store.save(body['features'], body['revision'], admin.get('id') or 'owner')
            audit('production_controls', 'release_controls_update',
                  json.dumps(doc, sort_keys=True), admin, SETTING_KEY)
            return jsonify(payload(doc))
        except Conflict as exc:
            return jsonify(success=False, error=str(exc)), 409
        except ValueError as exc:
            return jsonify(success=False, error=str(exc)), 400
        except ControlsUnavailable as exc:
            return jsonify(success=False, error=str(exc)), 503

    @app.get('/production_capabilities')
    def production_capabilities():
        try:
            doc, _ = store.read()
            return jsonify(success=True, revision=doc['revision'], features=doc['features'],
                           unreleased=list(UNRELEASED), propagation_seconds=3)
        except ControlsUnavailable:
            return jsonify(success=False, error='Feature availability could not be checked'), 503

    routes = {path: key for key, (_, _, paths) in FEATURES.items() for path in paths}

    @app.before_request
    def enforce_production_controls():
        if request.method == 'OPTIONS':
            return None
        key = routes.get(request.path.rstrip('/')) if request.method == 'POST' or (request.method == 'GET' and routes.get(request.path.rstrip('/')) == 'announcements') else None
        if not key:
            return None
        try:
            if key == 'challenge_sales':
                body = request.get_json(silent=True) or {}
                plan_id = body.get('plan_id') if isinstance(body, dict) else None
                if plan_id:
                    response = store.transport.get(store.url.rsplit('/', 1)[0] + '/challenge_plans',
                        headers=store.headers, params={'id': 'eq.' + str(plan_id), 'select': '*', 'limit': '1'},
                        timeout=(2, 4))
                    response.raise_for_status()
                    plans = response.json()
                    if not plans or str(plans[0].get('status') or '').lower() != 'active':
                        return jsonify(success=False, code='PRODUCT_NOT_RELEASED',
                                       error='This product is not open for new purchases.', operation_started=False), 403
                    buyer_pricer = app.config.get('NP_PURCHASE_PLAN_PRICER')
                    pricer = app.config.get('NP_STANDARD_PLAN_PRICER')
                    if buyer_pricer or pricer:
                        try:
                            plans[0] = buyer_pricer(plans[0], body.get('payout_cycles', 2)) if buyer_pricer else pricer(plans[0])
                        except ValueError as exc:
                            return jsonify(success=False, error=str(exc), operation_started=False), 409
                    from decimal import Decimal, InvalidOperation
                    try:
                        if Decimal(str(body.get('fee'))) != Decimal(str(plans[0].get('fee'))):
                            return jsonify(success=False, code='PRODUCT_PRICE_CHANGED',
                                           error='The product price changed. Refresh and confirm the current price before submitting payment proof.', operation_started=False), 409
                    except InvalidOperation:
                        return jsonify(success=False, error='A valid purchase price is required', operation_started=False), 400
                    request.environ['np_verified_checkout_plan_v221'] = plans[0]
            doc, _ = store.read()
            if not doc['features'][key]:
                return jsonify(success=False, code='FEATURE_NOT_RELEASED', feature=key,
                               error='This service is currently paused for new requests.',
                               operation_started=False), 403
        except Exception:
            return jsonify(success=False, code='FEATURE_AVAILABILITY_UNAVAILABLE',
                           error='Availability could not be verified. Please retry shortly.',
                           operation_started=False), 503
