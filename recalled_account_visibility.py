"""Trader presentation only: keep recalled deliveries in internal audit storage."""
import re

ACCOUNT_LISTS=('accounts','all_accounts','active_accounts','trader_accounts','previous_accounts','archived_accounts','account_history','mt5_accounts','vault_accounts')
SINGLES=('account','current_account')
REFS=('trader_account_id','account_id','source_account_id','completed_account_id')

def recalled(row):
    if not isinstance(row,dict):return False
    status=str(row.get('account_status') or row.get('status') or '').lower().strip()
    if status in ('recalled','wrong_assignment_recalled','recalled_wrong_assignment','admin_recalled'):return True
    if status!='archived':return False
    if row.get('recalled_delivery') is True:return True
    source=' '.join(str(row.get(k) or '') for k in ('archive_reason','admin_note'))
    held=re.findall(r'\[NP_MH_ACCOUNT:([^\]]+)\]',source,re.I)
    if '[np_management_hold:active]' in source.lower() and str(row.get('id') or '') in held:return True
    original=re.findall(r'\[NP_RECALL_ORIGINAL_ACCOUNT:([^\]]+)\]',source,re.I)
    if '[np_universal_recall:' in source.lower():
        return str(row.get('id') or '') in original
    return any(mark in source.lower() for mark in ('wrong_assignment_recalled','recalled_wrong_assignment','admin_recall_wrong_assignment'))

def trader_view(root):
    if not isinstance(root,dict):return root
    excluded=set();logins=set()
    containers=[root]
    if isinstance(root.get('data'),dict):containers.append(root['data'])
    for obj in containers:
        for key in ACCOUNT_LISTS+SINGLES+('data',):
            value=obj.get(key);rows=value if isinstance(value,list) else [value] if isinstance(value,dict) and key in SINGLES else []
            for row in rows:
                if recalled(row) and row.get('id'):
                    excluded.add(str(row['id']));logins.add(str(row.get('mt5_login') or ''))
    if not excluded:return root
    def keep(row):
        return not isinstance(row,dict) or (str(row.get('id') or '') not in excluded and not any(str(row.get(k) or '') in excluded for k in REFS))
    for obj in containers:
        for key in ACCOUNT_LISTS+('data','latest_trades','trades','snapshots','previous_reset_actions','timeline','monitoring_events'):
            if isinstance(obj.get(key),list):obj[key]=[r for r in obj[key] if keep(r)]
        for key in SINGLES:
            if isinstance(obj.get(key),dict) and not keep(obj[key]):obj[key]=None
        if isinstance(obj.get('latest_monitoring'),dict) and not keep(obj['latest_monitoring']):obj['latest_monitoring']={}
        for key in ('purchases','challenge_purchases'):
            if not isinstance(obj.get(key),list):continue
            cleaned=[]
            for purchase in obj[key]:
                if isinstance(purchase,dict) and (str(purchase.get('trader_account_id') or '') in excluded or (not purchase.get('trader_account_id') and str(purchase.get('mt5_login') or '') in logins)):
                    purchase=dict(purchase)
                    for field in ('trader_account_id','mt5_login','current_mt5_login','mt5_server','current_mt5_server','mt5_master_password','mt5_password','master_password','mt5_investor_password','investor_password'):
                        if field in purchase:purchase[field]=None
                cleaned.append(purchase)
            obj[key]=cleaned
        obj['recalled_excluded_account_ids']=sorted(set(obj.get('recalled_excluded_account_ids') or [])|excluded)
        trader=obj.get('trader')
        if isinstance(trader,dict):
            for key in SINGLES:
                if isinstance(trader.get(key),dict) and not keep(trader[key]):trader[key]=None
            current=obj.get('current_account') or obj.get('account') or {}
            stale=str(trader.get('current_account_id') or '') in excluded
            stale|=not trader.get('current_account_id') and str(trader.get('mt5_login') or '') in logins
            if stale:
                trader['current_account_id']=current.get('id')
                for field in ('mt5_login','mt5_server','mt5_master_password','mt5_password','master_password','mt5_investor_password','investor_password'):
                    if field in trader:trader[field]=current.get(field)
                if not current:
                    for field in ('balance','equity','current_balance','current_equity'):
                        if field in trader:trader[field]=None
        if isinstance(obj.get('count'),int) and isinstance(obj.get('accounts'),list):obj['count']=len(obj['accounts'])
    return root
