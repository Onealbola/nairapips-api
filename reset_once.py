"""A consumed reset source stays terminal even on schemas without mirror columns."""
import re

def consumed(account):
    a=account or {}
    if a.get('reset_consumed_at') or a.get('reset_replacement_account_id'):return True
    text=' '.join(str(a.get(k) or '') for k in ('archive_reason','admin_note','notes')).lower()
    return bool(re.search(r'reset_consumed\s+replacement_account_id=|np_consumed:paid_reset:|\[np_reset_consumed:|\[np_reset_automation_v43_consumed:',text))

def install(g):
    previous=g['_np_reset_policy']
    def policy(account,trader=None):
        if consumed(account):
            return dict(eligible=False,kind='terminal',reason='reset_already_consumed',title='This Reset Has Already Been Used',source_account_id=(account or {}).get('id'))
        return previous(account,trader)
    g['_np_reset_policy']=policy
