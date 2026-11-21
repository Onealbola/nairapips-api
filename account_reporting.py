"""Read-only account reporting. Lifecycle evidence outranks telemetry risk labels."""
from math import isfinite

LIFECYCLE_FIELDS = ('id','trader_id','purchase_id','mt5_login','account_status','stage',
    'breached_at','archive_reason','archived_at','passed_at','updated_at',
    'start_balance','account_size','dd_limit_percent','challenge_journey','target_percent',
    'package_contract_id','package_rules_hash')

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
                if (str(row.get('account_status') or '').lower() in ('assigned_active','profit_protected')
                        and not row.get('purchase_id')
                        and not source.removeprefix('equity_trailing_v1:').startswith('manual_plan_snapshot')):
                    result[aid].update(rule_snapshot_status='repair_required',trading_rule_snapshot_status='repair_required',
                                       snapshot_repair_scope='purchase_binding_unavailable')
                if (source=='post9_purchase_root_immutable' or '|np_risk_controls_v1:' in source
                        or '|np_withdrawal_v1:' in source or source.startswith('equity_trailing_v1:')
                        or str(row.get('account_status') or '').lower() in ('assigned_active','profit_protected')):
                    result[aid]['_snapshot_source']=row.get('journey_source')
    except Exception:
        failed=True
    # Audit active purchase-bound accounts even when the broken creation path
    # dropped every marker. Match the purchase, never today's mutable plan.
    candidates={aid:r for aid,r in result.items() if '_snapshot_source' in r and r.get('purchase_id')}
    for row in candidates.values():row.update(rule_snapshot_status='unverified',trading_rule_snapshot_status='unverified')
    pids=sorted({str(r['purchase_id']) for r in candidates.values()})
    contracts={}
    try:
        from plan_risk_controls import frozen_terms
        from withdrawal_controls import read as withdrawal_terms
        for offset in range(0,len(pids),100):
            batch=pids[offset:offset+100]
            response=transport.get(url.rstrip('/')+'/rest/v1/challenge_purchases',
                    headers={'apikey':key,'Authorization':'Bearer '+key},
                    params={'select':'id,trader_id,journey_source,drawdown_type,max_drawdown,challenge_journey,phase1_target,phase2_target,package_contract_id','id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
            response.raise_for_status();purchases=response.json()
            if not isinstance(purchases,list):raise ValueError('Invalid purchase metadata feed')
            for purchase in purchases:
                for row in candidates.values():
                    if str(purchase.get('id'))!=str(row['purchase_id']) or str(purchase.get('trader_id'))!=str(row['trader_id']):continue
                    source_row=dict(row,journey_source=row['_snapshot_source'])
                    if purchase.get('package_contract_id') or source_row.get('package_contract_id'):
                        try:
                            from package_account_rules import bind_package_account
                            cid=str(purchase.get('package_contract_id') or '')
                            if not cid or cid!=str(source_row.get('package_contract_id') or ''):
                                raise ValueError('Package purchase binding differs')
                            if cid not in contracts:
                                reply=transport.get(url.rstrip('/')+'/rest/v1/np_package_contracts',
                                    headers={'apikey':key,'Authorization':'Bearer '+key},
                                    params={'select':'id,trader_id,purchase_id,rules_hash,rules_version,document','id':'eq.'+cid,'limit':'1'},timeout=(2,4))
                                reply.raise_for_status();records=reply.json()
                                if not isinstance(records,list) or len(records)!=1:raise ValueError('Package contract unavailable')
                                contracts[cid]=records[0]
                            rules=bind_package_account(source_row,contracts[cid])
                            if float(source_row.get('dd_limit_percent') or 0)!=rules['dd_limit_percent'] or float(source_row.get('target_percent') or 0)!=rules['target_percent']:
                                raise ValueError('Account stage rules differ from immutable contract')
                            row.update(rule_snapshot_status='verified',trading_rule_snapshot_status='verified',
                                       snapshot_repair_scope=None,rule_authority='immutable_package_contract')
                        except Exception:
                            row.update(rule_snapshot_status='repair_required',trading_rule_snapshot_status='repair_required',
                                       snapshot_repair_scope='package_contract_verification')
                        continue
                    purchased=frozen_terms(purchase);assigned=frozen_terms(source_row)
                    from equity_drawdown import account_mode, mode
                    from decimal import Decimal
                    daily_missing=Decimal((purchased or {}).get('daily_drawdown',0))!=Decimal((assigned or {}).get('daily_drawdown',0))
                    trading_missing=daily_missing or account_mode(source_row)!=mode(purchase.get('drawdown_type')) or (purchased or {}).get('trailing_formula')!=(assigned or {}).get('trailing_formula')
                    if ('max_drawdown' in purchase and purchase.get('max_drawdown') is None
                            and str(row.get('account_status') or '').lower() in ('assigned_active','profit_protected')):
                        trading_missing=True
                    if purchase.get('max_drawdown') is not None:
                        trading_missing |= Decimal(str(row.get('dd_limit_percent') or 0))!=Decimal(str(purchase['max_drawdown']))
                    if purchase.get('challenge_journey') is not None:
                        trading_missing |= row.get('challenge_journey')!=purchase['challenge_journey']
                    target={'phase1':'phase1_target','phase2':'phase2_target'}.get(str(row.get('stage') or '').lower())
                    if target and purchase.get(target) is not None:
                        trading_missing |= Decimal(str(row.get('target_percent') or 0))!=Decimal(str(purchase[target]))
                    controls_missing=purchased!=assigned or withdrawal_terms(purchase)!=withdrawal_terms(source_row)
                    row['rule_snapshot_status']='repair_required' if controls_missing or trading_missing else 'verified'
                    row['trading_rule_snapshot_status']='repair_required' if trading_missing else 'verified'
                    row['snapshot_repair_scope']='trading_rules' if trading_missing else 'payout_controls' if controls_missing else None
                    # Restoring metadata is not the same as recovering missing
                    # historical broker evidence for an already traded account.
                    if ('purchase_rule_continuity_' in str(row.get('_snapshot_source') or '')
                            and str(row.get('account_status') or '').lower() in ('assigned_active','profit_protected')
                            and (Decimal((purchased or {}).get('daily_drawdown',0))>0
                                 or mode(purchase.get('drawdown_type'))=='TRAILING')
                            and not trading_missing):
                        row['rule_enforcement_status']='broker_history_pending'
    except Exception:
        pass  # Unavailable evidence stays unverified, never labelled repaired.
    pending={aid:row for aid,row in result.items() if row.get('rule_enforcement_status')=='broker_history_pending'}
    if pending:
        try:
            import json
            from datetime import datetime,timezone
            from equity_drawdown import spec,fingerprint,timestamp
            for offset in range(0,len(pending),100):
                batch=list(pending)[offset:offset+100]
                keys=['equity_drawdown_v1:'+aid for aid in batch]
                response=transport.get(url.rstrip('/')+'/rest/v1/package_studio_private_settings',
                    headers={'apikey':key,'Authorization':'Bearer '+key},
                    params={'select':'key,value','key':'in.('+','.join(keys)+')','limit':str(len(keys))},timeout=(2,4))
                response.raise_for_status()
                journals={r['key']:json.loads(r['value']) for r in response.json()}
                for aid in batch:
                    row=pending[aid];state=journals.get('equity_drawdown_v1:'+aid)
                    original=dict(row,journey_source=row['_snapshot_source'])
                    if not state or state.get('rules_hash')!=fingerprint(spec(original)):
                        continue
                    age=(datetime.now(timezone.utc)-timestamp(state['last_observed_at'])).total_seconds()
                    if not -5<=age<=180 or type(state.get('breached')) is not bool:
                        continue
                    row['rule_enforcement_status']='breach_observed' if state['breached'] else 'verified'
        except Exception:
            pass
        for row in pending.values():
            if row.get('rule_enforcement_status')!='verified':
                row.update(rule_snapshot_status='repair_required',trading_rule_snapshot_status='repair_required',
                           snapshot_repair_scope='broker_history_verification')
    for row in candidates.values():
        if (row.get('trading_rule_snapshot_status')=='unverified'
                and str(row.get('account_status') or '').lower() in ('assigned_active','profit_protected')):
            row.update(rule_snapshot_status='repair_required',trading_rule_snapshot_status='repair_required',
                       snapshot_repair_scope='purchase_evidence_unavailable')
    for row in result.values():row.pop('_snapshot_source',None)
    return result, failed
