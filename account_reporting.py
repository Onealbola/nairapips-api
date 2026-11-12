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
            if out.get('dd_used_percent') is None:
                out['dd_used_percent'] = out['loss_allowance_used_percent']
    out['recorded_breach'] = recorded_breach(row)
    if out['recorded_breach']:
        out['risk_zone'] = 'breached'
        out['reporting_status'] = 'BREACHED'
    elif __import__('assignment_corrections').voided(row):
        out['risk_zone'] = 'corrected'
        out['reporting_status'] = 'ASSIGNMENT_CORRECTED'
    elif row.get('trading_rule_snapshot_status') == 'repair_required':
        out['risk_zone'] = 'rule_review_required'
        out['reporting_status'] = 'RULE_REVIEW_REQUIRED'
    elif row.get('live_data_status') != 'live':
        out['reporting_status'] = 'AWAITING_UPDATE' if row.get('live_data_status') in ('missing','unavailable') else 'STALE'
    else:
        out['reporting_status'] = str(row.get('account_status') or 'unknown').upper()
    return out


def refresh_lifecycle(accounts, transport, url, key, *, fresh_rows=None):
    """Batch exact identities; never accept a different owner, purchase or MT5 login."""
    result = {}; failed = False
    ids = sorted(accounts)
    try:
        batches = [None] if fresh_rows is not None else range(0,len(ids),100)
        for offset in batches:
            batch=ids if offset is None else ids[offset:offset+100]
            if fresh_rows is not None:
                rows=fresh_rows
            else:
                response=transport.get(url.rstrip('/')+'/rest/v1/trader_accounts',
                    headers={'apikey':key,'Authorization':'Bearer '+key},
                    params={'select':','.join(LIFECYCLE_FIELDS)+',journey_source','id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
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
                result[aid].update(rule_snapshot_status='not_applicable',trading_rule_snapshot_status='not_applicable')
                source=str(row.get('journey_source') or '')
                if source=='post9_purchase_root_immutable' or '|np_risk_controls_v1:' in source or '|np_withdrawal_v1:' in source or source.startswith('equity_trailing_v1:'):
                    result[aid]['_snapshot_source']=row.get('journey_source')
    except Exception:
        failed=True
    # Only the known destructive ownership label needs this compatibility audit.
    # Match the exact immutable purchase; never use mutable current plan settings.
    candidates={aid:r for aid,r in result.items() if '_snapshot_source' in r and r.get('purchase_id')}
    for row in candidates.values():row.update(rule_snapshot_status='unverified',trading_rule_snapshot_status='unverified')
    pids=sorted({str(r['purchase_id']) for r in candidates.values()})
    try:
        from plan_risk_controls import frozen_terms
        from withdrawal_controls import read as withdrawal_terms
        for offset in range(0,len(pids),100):
            batch=pids[offset:offset+100]
            response=transport.get(url.rstrip('/')+'/rest/v1/challenge_purchases',
                    headers={'apikey':key,'Authorization':'Bearer '+key},
                    params={'select':'id,trader_id,journey_source,drawdown_type','id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
            response.raise_for_status();purchases=response.json()
            if not isinstance(purchases,list):raise ValueError('Invalid purchase metadata feed')
            for purchase in purchases:
                for row in candidates.values():
                    if str(purchase.get('id'))!=str(row['purchase_id']) or str(purchase.get('trader_id'))!=str(row['trader_id']):continue
                    source_row=dict(row,journey_source=row['_snapshot_source'])
                    purchased=frozen_terms(purchase);assigned=frozen_terms(source_row)
                    from equity_drawdown import account_mode, mode
                    from decimal import Decimal
                    daily_missing=Decimal((purchased or {}).get('daily_drawdown',0))!=Decimal((assigned or {}).get('daily_drawdown',0))
                    trading_missing=daily_missing or account_mode(source_row)!=mode(purchase.get('drawdown_type'))
                    controls_missing=purchased!=assigned or withdrawal_terms(purchase)!=withdrawal_terms(source_row)
                    row['rule_snapshot_status']='repair_required' if controls_missing or trading_missing else 'verified'
                    row['trading_rule_snapshot_status']='repair_required' if trading_missing else 'verified'
                    row['snapshot_repair_scope']='trading_rules' if trading_missing else 'payout_controls' if controls_missing else None
    except Exception:
        pass  # Unavailable evidence stays unverified, never labelled repaired.
    for row in result.values():row.pop('_snapshot_source',None)
    return result, failed
