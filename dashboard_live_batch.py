"""Bounded, exact-account live reads for dashboard presentation only."""
from uuid import UUID


def load_observations(transport, url, key, trader_id, rows):
    accounts={}
    for row in rows:
        if not isinstance(row,dict) or str(row.get('trader_id') or '') != str(trader_id):
            continue
        aid=str(row.get('id') or '')
        try:UUID(aid)
        except (ValueError,TypeError):continue
        accounts[aid]=str(row.get('mt5_login') or '')
    result={};ids=sorted(accounts)
    for offset in range(0,len(ids),100):
        batch=ids[offset:offset+100]
        response=transport.get(url.rstrip('/')+'/rest/v1/np_live_account_state',
            headers={'apikey':key,'Authorization':'Bearer '+key},
            params={'select':'*','trader_account_id':'in.('+','.join(batch)+')','limit':str(len(batch))},timeout=(2,4))
        response.raise_for_status()
        observations=response.json()
        if not isinstance(observations,list):raise ValueError('Invalid dashboard live response')
        for snap in observations:
            aid=str(snap.get('trader_account_id') or '')
            if (aid not in batch or str(snap.get('mt5_login') or '')!=accounts[aid]
                    or str(snap.get('trader_id') or '') not in {str(trader_id),aid}):
                continue
            result[aid]=snap
    return result
