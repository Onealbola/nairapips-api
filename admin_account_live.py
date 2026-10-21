"""Read-only, exact-account telemetry decoration for Admin account lists."""
from datetime import datetime, timezone
from math import isfinite
from uuid import UUID


def overlay_account_lists(payload, transport, url, key, is_live):
    collections=[]
    if isinstance(payload.get('data'),list):collections.append(('data',payload['data']))
    for name in ('accounts','trader_accounts','active_accounts','all_accounts'):
        if isinstance(payload.get(name),list):collections.append((name,payload[name]))
    accounts={}
    for _,rows in collections:
        for row in rows:
            if not isinstance(row,dict) or not is_live(row):continue
            aid=str(row.get('id') or '')
            try:UUID(aid)
            except (ValueError,TypeError):continue
            accounts[aid]=row
    snapshots={};failed=False
    ids=sorted(accounts)
    try:
        for offset in range(0,len(ids),100):
            batch=ids[offset:offset+100]
            response=transport.get(url.rstrip('/')+'/rest/v1/np_live_account_state',
                headers={'apikey':key,'Authorization':'Bearer '+key},
                params={'select':'*','trader_account_id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
            response.raise_for_status();rows=response.json()
            if not isinstance(rows,list):raise ValueError('Invalid live telemetry response')
            for snap in rows:
                aid=str(snap.get('trader_account_id') or '')
                if aid not in batch:continue
                account=accounts[aid];owner=str(account.get('trader_id') or '')
                if not owner or str(snap.get('trader_id') or '') not in {owner,aid}:continue
                if str(snap.get('mt5_login') or '') != str(account.get('mt5_login') or ''):continue
                snapshots[aid]=snap
    except Exception:
        # A partially read fleet must not appear fully live.
        failed=True
    def overlay(row):
        if not isinstance(row,dict) or not is_live(row):return row
        out=dict(row);snap=snapshots.get(str(row.get('id') or ''))
        out['live_data_status']='unavailable' if failed else 'missing'
        if not snap:return out
        try:
            balance=float(snap['balance']);equity=float(snap['equity'])
            observed=datetime.fromisoformat(str(snap['observed_at']).replace('Z','+00:00'))
            if observed.tzinfo is None:raise ValueError('Missing observation timezone')
            age=(datetime.now(timezone.utc)-observed).total_seconds()
            if not all(isfinite(v) and v>=0 for v in (balance,equity)) or age < -60:raise ValueError('Invalid observation')
        except (ValueError,TypeError,KeyError):return out
        out.update(current_balance=balance,current_equity=equity,balance=balance,equity=equity,
            latest_monitoring_snapshot=snap,last_sync_at=snap['observed_at'],
            live_monitor_update_at=snap['observed_at'],live_metrics_authority='EXACT_ADMIN_LIVE_STATE',
            live_data_status='live' if age<=180 else 'stale')
        for field in ('dd_used_percent','drawdown_percent','highest_equity','lowest_equity','risk_zone'):
            if snap.get(field) is not None:out[field]=snap[field]
        start=float(row.get('start_balance') or row.get('account_size') or 0)
        out['profit']=equity-start
        out['profit_percent']=(equity-start)/start*100 if start>0 else None
        return out
    for name,rows in collections:payload[name]=[overlay(row) for row in rows]
    payload['live_data_read_failed']=failed
    return payload


def register_admin_account_state(app, transport, url, key, require_admin, is_live):
    """Complete read-only fleet feed; independent of the 600-row overview preview."""
    from flask import jsonify
    from time import monotonic
    safe_fields={'id','trader_id','purchase_id','stage','account_status','mt5_login','mt5_server',
        'account_size','start_balance','current_balance','current_equity','dd_limit_percent',
        'dd_used_percent','absolute_drawdown_percent','drawdown_percent','highest_equity',
        'lowest_equity','risk_zone','created_at','updated_at','started_at','assigned_at',
        'archived_at','archive_reason','breached_at','passed_at','monitoring_enabled'}

    @app.get('/admin/account_state')
    def admin_account_state():
        _,error=require_admin()
        if error:return error
        deadline=monotonic()+14
        def read(table,select='*',filters=None,order='id.asc'):
            result=[]
            for offset in range(0,20000,1000):
                if monotonic()>deadline:raise TimeoutError('Fleet read timed out')
                response=transport.get(url.rstrip('/')+'/rest/v1/'+table,
                    headers={'apikey':key,'Authorization':'Bearer '+key},
                    params={'select':select,'order':order,'limit':'1000','offset':str(offset),**(filters or {})},timeout=(2,4))
                response.raise_for_status();rows=response.json()
                if not isinstance(rows,list):raise ValueError('Invalid fleet response')
                result.extend(rows)
                if len(rows)<1000:return result
            raise ValueError('Fleet exceeds complete report limit')
        try:
            accounts=read('trader_accounts')
            traders={str(t['id']):t for t in read('traders','id,name,email')}
            registry=read('monitoring_registry','trader_account_id,active',{'active':'eq.true'},order='trader_account_id.asc')
            registered={str(r.get('trader_account_id')) for r in registry}
            rows=[]
            for account in accounts:
                out={k:v for k,v in account.items() if k in safe_fields}
                trader=traders.get(str(account.get('trader_id')),{})
                out['trader_name']=trader.get('name') or trader.get('email') or ''
                out['live_monitoring_registered']=str(account.get('id')) in registered
                rows.append(out)
            payload=overlay_account_lists({'success':True,'accounts':rows,'complete':True},transport,url,key,is_live)
            response=jsonify(payload);response.headers['Cache-Control']='no-store'
            return response
        except Exception:
            return jsonify(success=False,error='The complete account fleet could not be loaded. Retry refresh; partial totals are not shown.'),503
