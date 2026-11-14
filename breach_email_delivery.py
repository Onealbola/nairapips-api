"""Read-only recipient delivery checks for breach emails; never resends mail."""
import json
from datetime import datetime,timezone
from business_email_delivery import outcome

def check(service,stored):
    case=json.loads(stored['value'])
    if case.get('state')!='accepted' or not case.get('provider_id'):return False
    prior=case.get('delivery_checked_at')
    if prior and (datetime.now(timezone.utc)-datetime.fromisoformat(prior)).total_seconds()<300:return False
    recipients=list(dict.fromkeys(case.get('recipients') or [case.get('recipient')]))
    recipients+= [e for e in case.get('admin_copies',[]) if e not in recipients]
    delivery=[]
    for email in recipients:
        if not email:continue
        response=service.http.get('https://api.brevo.com/v3/smtp/statistics/events',headers={'api-key':service.g['BREVO_API_KEY'],'Accept':'application/json'},params={'email':email,'messageId':case['provider_id'],'limit':'100','sort':'desc'},timeout=(2,8))
        response.raise_for_status()
        result=outcome(response.json().get('events',[]),email,case['provider_id'])
        delivery.append(dict(email=email,audience='admin_copy' if case.get('audience')=='admin_copy' or email in case.get('admin_copies',[]) else 'trader',**result))
    return service.save(stored,dict(case,delivery=delivery,delivery_checked_at=datetime.now(timezone.utc).isoformat()))

def cycle(service):
    from breach_analysis_email import PREFIX,ADMIN_PREFIX
    rows=[]
    for prefix in (PREFIX,ADMIN_PREFIX):
        rows+=service.read('package_studio_private_settings',{'key':'like.'+prefix+'*','value':'like.*"state": "accepted"*','order':'updated_at.asc','limit':'10'})
    checked=0
    for row in rows:
        try:checked+=bool(check(service,row))
        except Exception:continue
        if checked>=2:break
    return checked
