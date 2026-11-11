"""Read-only account reporting. Lifecycle evidence outranks telemetry risk labels."""
from math import isfinite

LIFECYCLE_FIELDS = ('id','trader_id','purchase_id','mt5_login','account_status','stage',
    'breached_at','archive_reason','archived_at','passed_at','updated_at',
    'start_balance','account_size','dd_limit_percent')

def recorded_breach(row):
    status = str(row.get('account_status') or row.get('status') or '').lower()
    return bool(row.get('breached_at') or 'breach' in status)

def metrics(row):
    """Separate current loss (% capital), allowance usage, and a permanent breach."""
    out = dict(row)
    def number(value):
        try:
            v = float(value)
            return v if isfinite(v) else None
        except (ValueError, TypeError):
            return None
    capital = number(row.get('start_balance') or row.get('account_size'))
    values = [number(row.get(k)) for k in ('current_balance','current_equity')]
    values = [v for v in values if v is not None]
    if capital and capital > 0 and values:
        loss = max(0, (capital - min(values)) / capital * 100)
        out['capital_drawdown_percent'] = loss
        # Preserve worker drawdown for trailing rules; do not relabel it as capital loss.
        if out.get('drawdown_percent') is None:
            out['drawdown_percent'] = loss
        out['absolute_drawdown_percent'] = loss
        limit = number(row.get('dd_limit_percent'))
        if limit and limit > 0:
            usage = number(row.get('dd_used_percent'))
            out['loss_allowance_used_percent'] = usage if usage is not None else loss / limit * 100
    out['recorded_breach'] = recorded_breach(row)
    if out['recorded_breach']:
        out['risk_zone'] = 'breached'
        out['reporting_status'] = 'BREACHED'
    elif row.get('live_data_status') != 'live':
        out['reporting_status'] = 'AWAITING_UPDATE' if row.get('live_data_status') in ('missing','unavailable') else 'STALE'
    else:
        out['reporting_status'] = str(row.get('account_status') or 'unknown').upper()
    return out


def refresh_lifecycle(accounts, transport, url, key):
    """Batch exact identities; never accept a different owner, purchase or MT5 login."""
    result = {}; failed = False
    ids = sorted(accounts)
    try:
        for offset in range(0,len(ids),100):
            batch=ids[offset:offset+100]
            response=transport.get(url.rstrip('/')+'/rest/v1/trader_accounts',
                headers={'apikey':key,'Authorization':'Bearer '+key},
                params={'select':','.join(LIFECYCLE_FIELDS),'id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
            response.raise_for_status()
            rows=response.json()
            if not isinstance(rows,list):raise ValueError('Invalid lifecycle feed')
            for row in rows:
                aid=str(row.get('id') or '')
                original=accounts.get(aid)
                if not original:continue
                if any(str(row.get(k) or '')!=str(original.get(k) or '') for k in ('trader_id','mt5_login')):continue
                if original.get('purchase_id') and str(row.get('purchase_id') or '')!=str(original['purchase_id']):continue
                result[aid]={k:v for k,v in row.items() if k in LIFECYCLE_FIELDS}
    except Exception:
        failed=True
    return result, failed
