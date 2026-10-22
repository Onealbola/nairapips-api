"""Purchase-wide payout controls for finite-cycle sales only; legacy rows are untouched."""
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from uuid import uuid4
from flask import jsonify

RELEASE = 'finite-cycle-payment-guard-2026-10-22'
OPEN = {'pending','approved','processing','payment_processing','requested','submitted','under_review','pending_review','awaiting_review','request_pending'}
MARKER = '[NP_LIMITED_PAYOUT_ACTION:'

class CycleBlocked(ValueError):
    pass


def amount(value):
    try:
        if isinstance(value, bool): raise ValueError()
        n = Decimal(str(value))
        if not n.is_finite() or n < 0: raise ValueError()
        return n
    except (InvalidOperation, ValueError, TypeError):
        raise CycleBlocked('A valid payout amount is required.')


class LimitedCycleSafety:
    def __init__(self, transport, url, key):
        self.http, self.url = transport, url.rstrip('/') + '/rest/v1/'
        self.headers = {'apikey':key,'Authorization':'Bearer '+key}

    def rows(self, table, params):
        out=[]
        for offset in range(0, 20000, 500):
            r=self.http.get(self.url+table,headers=self.headers,params={**params,'order':'id.asc','limit':'500','offset':str(offset)},timeout=(2,5))
            r.raise_for_status(); page=r.json()
            if not isinstance(page,list): raise RuntimeError('Invalid cycle ledger response')
            out.extend(page)
            if len(page)<500:return out
        raise RuntimeError('Complete cycle ledger could not be verified')

    def one(self, table, identity):
        rows=self.rows(table,{'id':'eq.'+str(identity),'select':'*'})
        if len(rows)!=1:raise CycleBlocked('The exact purchase could not be verified. No payout was authorized.')
        return rows[0]

    def cas(self, purchase, current, replacement):
        params={'id':'eq.'+str(purchase['id']),'trader_id':'eq.'+str(purchase['trader_id']),
                'archive_reason':'is.null' if current is None else 'eq.'+str(current)}
        r=self.http.patch(self.url+'challenge_purchases',headers={**self.headers,'Prefer':'return=representation'},params=params,json={'archive_reason':replacement},timeout=(2,5))
        r.raise_for_status();return r.json()

    def release(self, claim):
        if not claim:return
        for _ in range(3):
            current=self.one('challenge_purchases',claim['purchase']['id']).get('archive_reason')
            if claim['token'] not in str(current or ''):return
            restored=claim['original'] if current==claim['claimed'] else str(current).replace(claim['token'],'').strip()
            if self.cas(claim['purchase'],current,restored):return
        raise RuntimeError('Purchase payout lock requires review; it was not forcibly cleared')

    def inspect(self, purchase, account, payout_id=''):
        cap=purchase.get('payout_cycles')
        if type(cap) is not int or not 1<=cap<=5:raise CycleBlocked('Invalid purchased cycle limit. Review required.')
        owner=str(purchase.get('trader_id') or '')
        if not owner or owner!=str(account.get('trader_id') or '') or str(account.get('stage') or '').lower()!='funded':
            raise CycleBlocked('Funded purchase ownership could not be verified.')
        accounts=self.rows('trader_accounts',{'purchase_id':'eq.'+str(purchase['id']),'select':'id,trader_id,stage,mt5_login'})
        scope={str(a['id']):a for a in accounts}
        if str(account.get('id')) not in scope:raise CycleBlocked('Exact funded account is missing from its purchase.')
        ledger=[]
        ids=list(scope)
        for offset in range(0,len(ids),100):
            ledger.extend(self.rows('payouts',{'trader_account_id':'in.('+','.join(ids[offset:offset+100])+')','select':'id,trader_id,trader_account_id,mt5_login,status,paid_at'}))
        paid=set();reserved=set();seen=set()
        for p in ledger:
            if str(p['id']) in seen:continue
            seen.add(str(p['id']))
            a=scope[str(p['trader_account_id'])]
            status=str(p.get('status') or '').lower()
            is_paid=status=='paid' or bool(p.get('paid_at'))
            is_open=status in OPEN and str(p['id'])!=str(payout_id)
            if not (is_paid or is_open):continue
            if str(a.get('trader_id') or '')!=owner or str(p.get('trader_id') or '')!=owner or str(a.get('stage') or '').lower()!='funded' or not a.get('mt5_login') or str(a['mt5_login'])!=str(p.get('mt5_login')):
                raise CycleBlocked('Purchase payout ledger has an identity mismatch. Review required.')
            aid=str(a['id'])
            if is_paid:
                if aid in paid:raise CycleBlocked('Duplicate paid payout evidence requires review.')
                paid.add(aid)
            elif is_open:
                if aid in reserved:raise CycleBlocked('Duplicate open payout requests require review.')
                reserved.add(aid)
        if paid & reserved:raise CycleBlocked('A paid account has another open payout. Review required.')
        return {'limit':cap,'paid':len(paid),'reserved':len(reserved),'remaining':max(0,cap-len(paid))}

    def acquire(self, account, payout_id='', action='create'):
        if action not in {'create','approve','paid'} or account.get('package_contract_id') or not account.get('purchase_id'):return None
        p=self.one('challenge_purchases',account['purchase_id'])
        if p.get('payout_cycles') is None or p.get('payout_cycles')==0:return None
        if str(p.get('trader_id') or '')!=str(account.get('trader_id') or ''):raise CycleBlocked('Purchase ownership mismatch.')
        original=p.get('archive_reason')
        if MARKER in str(original or ''):raise CycleBlocked('Another payout action is processing for this purchase. Refresh before retrying.')
        token=MARKER+str(uuid4())+']';claimed=(str(original or '')+' '+token).strip()
        claim={'purchase':p,'token':token,'original':original,'claimed':claimed}
        # Uncertain writes fail closed; never repeat the mutation blindly.
        if not self.cas(p,original,claimed):raise CycleBlocked('Purchase changed during payout verification. Refresh before retrying.')
        try:
            state=self.inspect(p,account,payout_id)
            if p.get('lifecycle_state')=='funded_cycles_completed' or state['remaining']==0:
                raise CycleBlocked('PURCHASE COMPLETED: all purchased funded payout cycles have been paid. No further payout is authorized.')
            if state['paid']+state['reserved']>=state['limit']:
                raise CycleBlocked('The remaining purchased cycles already have payout requests. Review those requests first.')
            claim['cycles']=state
            return claim
        except Exception:
            self.release(claim)
            raise

    def check_amount(self, claim, value, quote=None, gross_cap_percent=50):
        if not claim:return
        p=claim['purchase'];n=amount(value)
        if n<=0 or n!=n.quantize(Decimal('.01')):raise CycleBlocked('Payout amount must be positive with at most two decimals.')
        capital=amount(p.get('account_size'));split=amount(p.get('payout_split'))
        if capital<=0 or not 0<split<=100:raise CycleBlocked('Purchased capital or profit split could not be verified.')
        maximum=(capital*amount(gross_cap_percent)/100*split/100).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
        if quote is not None:
            if not quote.get('verified') or amount(quote.get('start_balance'))!=capital or amount(quote.get('payout_split'))!=split:
                raise CycleBlocked('Fresh account balance and purchased profit split must be verified before approval or payment.')
            maximum=min(maximum,amount(quote.get('available_payout')))
        if n>maximum:raise CycleBlocked('Payout exceeds the verified permitted amount ('+format(maximum,'.2f')+'). No payout was authorized.')


def register_cycle_safety(app):
    @app.get('/funded_cycle_safety_version')
    def cycle_safety_version():
        return jsonify(success=True,release=RELEASE,scope='finite_purchase_limits_only',purchase_wide_serialization=True,checkpoints=['request','approval','mark_paid'],legacy_records_changed=False)
