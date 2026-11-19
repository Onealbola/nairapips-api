"""Exact purchase snapshots across lifecycle changes; no network or financial writes."""
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from decimal import Decimal, InvalidOperation

from equity_drawdown import account_mode, mode
from plan_risk_controls import inherit, frozen_terms
from withdrawal_controls import read as withdrawal_terms

_PAID_RENEWAL = ContextVar('np_paid_renewal_rules', default=None)


def amount(value):
    try:
        if isinstance(value, bool):
            raise ValueError()
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError()
        return result
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise ValueError('Verified purchase rule values are required') from exc


def validate_purchase(purchase, trader_id, capital):
    if not purchase or not purchase.get('id'):
        raise ValueError('Exact purchased rules are unavailable; renewal requires review')
    if str(purchase.get('trader_id') or '') != str(trader_id or ''):
        raise ValueError('Purchase rules belong to another trader')
    if amount(purchase.get('account_size')) != amount(capital):
        raise ValueError('Purchase rules belong to another account size')
    if purchase.get('package_contract_id'):
        raise ValueError('Package rules require the immutable package entitlement path')
    limit = amount(purchase.get('max_drawdown'))
    journey = purchase.get('challenge_journey')
    if not 0 < limit < 100 or not isinstance(journey, list) or not journey:
        raise ValueError('The immutable purchase snapshot is incomplete')
    if any(stage not in ('phase1', 'phase2', 'funded') for stage in journey):
        raise ValueError('The immutable purchase journey is invalid')
    mode(purchase.get('drawdown_type'))
    frozen_terms(purchase)
    withdrawal_terms(purchase)


@contextmanager
def paid_renewal_rules(payout, source, purchase):
    """The caller still owns paid-cycle, stock, replay and source eligibility checks."""
    validate_purchase(purchase, source.get('trader_id'), source.get('start_balance') or source.get('account_size'))
    if (str(payout.get('status') or '').lower() != 'paid'
            or not payout.get('id') or not source.get('id')
            or str(payout.get('trader_id') or '') != str(source.get('trader_id') or '')
            or str(payout.get('trader_account_id') or '') != str(source['id'])
            or str(source.get('purchase_id') or source.get('challenge_purchase_id') or '') != str(purchase['id'])
            or str(source.get('stage') or '').lower() != 'funded'):
        raise ValueError('Paid renewal rules require the exact paid account and purchase')
    token = _PAID_RENEWAL.set(deepcopy({'purchase': purchase, 'source': source}))
    try:
        yield
    finally:
        _PAID_RENEWAL.reset(token)


def assignment_purchase(trader, mt5, stage, purchase):
    """Carry rules into the original primitive without weakening duplicate guards."""
    context = _PAID_RENEWAL.get()
    if context is None:
        return purchase, None
    saved, source = context['purchase'], context['source']
    validate_purchase(saved, trader.get('id'), mt5.get('account_size'))
    if stage != 'funded' or (purchase and str(purchase.get('id')) != str(saved['id'])):
        raise ValueError('Paid renewal rule context does not match this assignment')
    return deepcopy(saved), str(source['id'])


def account_fields(account, purchase, *, recovered=False):
    """Return only purchased metadata. Never alter balances, status or breach history."""
    validate_purchase(purchase, account.get('trader_id'), account.get('start_balance') or account.get('account_size'))
    if account.get('purchase_id') and str(account['purchase_id']) != str(purchase['id']):
        raise ValueError('Account is bound to another purchase')
    stage = str(account.get('stage') or '').lower()
    if stage not in purchase['challenge_journey']:
        raise ValueError('Account stage is not included in this purchase')
    # Mark an existing account needing broker-history verification explicitly.
    prefix = 'purchase_rule_continuity_recovered' if recovered else 'purchase_rule_continuity_v1'
    if mode(purchase.get('drawdown_type')) == 'TRAILING':
        prefix = 'equity_trailing_v1:' + prefix
    fields = dict(purchase_id=purchase['id'], dd_limit_percent=float(amount(purchase['max_drawdown'])),
                  challenge_journey=deepcopy(purchase['challenge_journey']), journey_source=inherit(prefix, purchase))
    target = {'phase1': 'phase1_target', 'phase2': 'phase2_target'}.get(stage)
    if target:
        value = amount(purchase.get(target))
        if not 0 < value < 100:
            raise ValueError('Purchased phase target is unavailable')
        fields['target_percent'] = float(value)
    return fields


def assert_bound(account, purchase):
    expected = account_fields(account, purchase)
    if (str(account.get('purchase_id')) != str(expected['purchase_id'])
            or amount(account.get('dd_limit_percent')) != amount(expected['dd_limit_percent'])
            or account.get('challenge_journey') != expected['challenge_journey']
            or frozen_terms(account) != frozen_terms(purchase)
            or withdrawal_terms(account) != withdrawal_terms(purchase)
            or account_mode(account) != mode(purchase.get('drawdown_type'))):
        raise ValueError('Account rules differ from the immutable purchase snapshot')
    if 'target_percent' in expected and amount(account.get('target_percent')) != amount(expected['target_percent']):
        raise ValueError('Phase target differs from the immutable purchase snapshot')

