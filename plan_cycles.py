"""Finite legacy-plan payout cycles, frozen per purchase; old purchases stay unchanged."""


def cycle_limit(value):
    if value is None:
        return None
    if isinstance(value, bool) or str(value) not in {'0', '1', '2', '3', '4', '5'}:
        raise ValueError('Choose 1–5 funded payout cycles, or unlimited')
    return int(value)


def paid_cycle_decision(transport, url, key, payout_id, complete=True):
    response = transport.post(url.rstrip('/') + '/rest/v1/rpc/np_plan_paid_cycle_decision',
        headers={'apikey': key, 'Authorization': 'Bearer ' + key},
        json={'p_payout_id': payout_id, 'p_complete': complete}, timeout=(2, 8))
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict) or result.get('state') not in {'legacy', 'renew', 'completed'}:
        raise RuntimeError('Funded cycle entitlement could not be verified')
    return result
