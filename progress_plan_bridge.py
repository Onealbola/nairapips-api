"""Progress offers on the existing immutable-purchase/two-phase account runtime.

Only managed UUIDs may be written. Publication remains a separate, fresh gate;
changing a future offer never edits purchases or issued trading accounts.
"""
import hashlib
import json
from uuid import UUID, uuid5
from decimal import Decimal, InvalidOperation
from production_controls import ControlsUnavailable

PREFIX = 'progress_matrix_v1:'
NAMESPACE = UUID('79ee22a0-4b9e-4a33-a187-c9958aec91d2')


def identity(plan, package):
    return 'progress:' + plan['id'] + ':' + package['key']


def managed_id(source):
    return str(uuid5(NAMESPACE, source))


def is_managed(plan):
    source = str(plan.get('journey_source') or '')
    return source.startswith(PREFIX) and str(plan.get('id')) == managed_id(source)


def catalog_identity(plan):
    if not is_managed(plan):
        return None
    return 'progress:' + plan['journey_source'][len(PREFIX):]


def readiness(plan, package):
    # The existing deployed risk collector and settlement policy enforce these
    # terms. Other selectable draft terms must not be sold without enforcement.
    if package['daily_drawdown'] != 0:
        return False, 'Set the daily rule to None; daily drawdown enforcement is not connected yet'
    if package['profit_cap'] != 15:
        return False, 'Set the profit cap to 15%; other caps are not connected to settlement yet'
    if package['drawdown'] > 20:
        return False, 'This drawdown requires daily risk enforcement before it can be sold'
    return True, ''


def plan_row(plan, package, opened=False):
    source = PREFIX + plan['id'] + ':' + package['key']
    return dict(id=managed_id(source), journey_source=source,
        name=(plan['name'] + ' · ' + package['name'])[:100],
        description='Two-phase challenge. Continuing eligible funded payouts. No included resets or second life.',
        account_size=plan['capital'], fee=package['price'],
        phase1_target=package['phase1_target'], phase2_target=package['phase2_target'],
        max_drawdown=package['drawdown'], daily_drawdown=package.get('daily_drawdown',0), drawdown_type='STATIC',
        payout_split=package['profit_share'], payout_cycles=0, payout_frequency='Daily',
        challenge_journey=['phase1','phase2','funded'], product_category='standard',
        reset_fee=0, funded_reset_fee=0, second_life_enabled=False, lives_total=1,
        status='active' if opened else 'inactive')


NUMERIC_FIELDS={'fee','account_size','phase1_target','phase2_target','max_drawdown',
    'daily_drawdown','payout_split','payout_cycles','reset_fee','funded_reset_fee','lives_total'}

def terms_match(current,wanted):
    for key,value in wanted.items():
        stored=current.get(key)
        if key in NUMERIC_FIELDS:
            if isinstance(stored,bool) or stored is None:return False
            try:
                if Decimal(str(stored))!=Decimal(str(value)):return False
            except InvalidOperation:return False
        elif stored!=value:return False
    return True


def offer_version(plan):
    keys=('id','journey_source','name','account_size','fee','phase1_target','phase2_target',
        'max_drawdown','daily_drawdown','drawdown_type','payout_split','payout_cycles',
        'payout_frequency','challenge_journey','product_category','reset_fee',
        'funded_reset_fee','second_life_enabled','lives_total')
    frozen={k:str(Decimal(str(plan[k])).normalize()) if k in NUMERIC_FIELDS else plan[k] for k in keys}
    return hashlib.sha256(json.dumps(frozen,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def commercial_plan(plan):
    fee=float(plan['fee'])
    return dict(plan, managed_progress=True, offer_version=offer_version(plan), base_fee=fee, fee=fee,
        payout_cycles=0, payout_cycle_extra=0,
        cycle_options=[dict(cycles=0,available=True,extra=0,fee=fee)],default_cycles=0)


class ProgressPlanBridge:
    def __init__(self, http, url, key, matrix, publication, invalidate=lambda:None):
        self.http=http; self.url=url.rstrip('/')+'/rest/v1/challenge_plans'
        self.headers={'apikey':key,'Authorization':'Bearer '+key,'Prefer':'return=representation'}
        self.matrix=matrix; self.publication=publication; self.invalidate=invalidate

    def find(self, plan_id):
        try:
            r=self.http.get(self.url,headers=self.headers,
                params={'id':'eq.'+str(plan_id),'select':'*','limit':'1'},timeout=(2,5))
            r.raise_for_status(); rows=r.json()
            if not isinstance(rows,list) or len(rows)>1:raise ValueError('Invalid plan response')
            return rows[0] if rows else None
        except Exception as exc:raise ControlsUnavailable('Current package plan could not be verified') from exc

    def sync(self, products, switches, materialize_closed=False):
        """Stage/verify physical rows before opening the private publication gate."""
        try:
            for product in products.values():
                if product['family']!='progress':continue
                opened=bool(switches.get(product['id'],{}).get('purchases_open',False)
                    and product['visible'] and product['checkout_supported'])
                wanted=plan_row(product['_plan'],product['_package'],opened)
                current=self.find(wanted['id'])
                if current and not is_managed(current):raise ControlsUnavailable('Managed plan ID collision; no plan was replaced')
                # Closed products need no physical row until the owner first opens them.
                if not current and not opened and not materialize_closed:continue
                if current and terms_match(current,wanted):continue
                if current:
                    r=self.http.patch(self.url,headers=self.headers,
                        params={'id':'eq.'+wanted['id'],'journey_source':'eq.'+wanted['journey_source']},json=wanted,timeout=(2,5))
                else:r=self.http.post(self.url,headers=self.headers,json=wanted,timeout=(2,5))
                r.raise_for_status()
                confirmed=self.find(wanted['id'])
                if not confirmed or not terms_match(confirmed,wanted):
                    raise ControlsUnavailable('Package terms were not confirmed; purchases remain closed')
            self.invalidate()
        except ControlsUnavailable:raise
        except Exception as exc:raise ControlsUnavailable('Package connection could not be saved and verified. Retry saving the switches.') from exc

    def verified_identity(self, row):
        selected=catalog_identity(row)
        if not selected:return None
        doc,_=self.matrix.read(fresh=True)
        for p in doc['config']['plans']:
            for x in p['packages']:
                if identity(p,x)!=selected:continue
                from package_plan_matrix import availability
                ok,_=readiness(p,x)
                wanted=plan_row(p,x,True)
                if not ok or not availability(p,x)['available'] or not terms_match(row,wanted):
                    raise ControlsUnavailable('Package terms changed. Refresh the catalogue before purchasing.')
                return selected
        raise ControlsUnavailable('This package is no longer available')
