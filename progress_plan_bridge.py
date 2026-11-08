"""Progress offers on the existing immutable-purchase/two-phase account runtime.

Only managed UUIDs may be written. Publication remains a separate, fresh gate;
changing a future offer never edits purchases or issued trading accounts.
"""
import hashlib
import json
from uuid import UUID, uuid5
from decimal import Decimal, InvalidOperation
from production_controls import ControlsUnavailable
from package_plan_matrix import model_of, route_for

PREFIX = 'progress_matrix_v1:'
NAMESPACE = UUID('79ee22a0-4b9e-4a33-a187-c9958aec91d2')


def identity(plan, package):
    return 'progress:' + plan['id'] + ':' + package['key']


def managed_id(source):
    source = __import__('withdrawal_controls').strip(source).split('|np_risk_controls_v1:',1)[0]
    return str(uuid5(NAMESPACE, source))


def is_managed(plan):
    source = str(plan.get('journey_source') or '')
    return source.startswith(PREFIX) and str(plan.get('id')) == managed_id(__import__('withdrawal_controls').strip(source).split('|np_risk_controls_v1:',1)[0])


def catalog_identity(plan):
    if not is_managed(plan):
        return None
    return 'progress:' + __import__('withdrawal_controls').strip(plan['journey_source']).split('|np_risk_controls_v1:',1)[0][len(PREFIX):]


def readiness(plan, package):
    # The existing deployed risk collector and settlement policy enforce these
    # terms. Other selectable draft terms must not be sold without enforcement.
    from equity_drawdown import runtime_ready
    if package.get('drawdown_type','STATIC') == 'TRAILING' and not runtime_ready():
        return False, 'Equity Trailing requires the updated broker-side DD worker before purchases can open'
    from plan_risk_controls import selected_terms, daily_runtime_ready
    if float(selected_terms(package)['daily_drawdown']) > 0 and not daily_runtime_ready():
        return False, 'Daily drawdown requires all four updated DD shards to be reporting'
    from withdrawal_controls import needs_broker_update,ready
    if needs_broker_update(plan_row(plan,package)) and not ready():
        return False, 'Custom or disabled profit caps require the updated Operations engine verification'
    if package['drawdown'] > 20 and not (float(selected_terms(package)['daily_drawdown']) > 0 and daily_runtime_ready()):
        return False, 'This drawdown requires verified daily risk enforcement before it can be sold'
    return True, ''


def plan_row(plan, package, opened=False):
    source = PREFIX + plan['id'] + ':' + package['key']
    from plan_risk_controls import selected_terms, freeze
    terms = selected_terms(package)
    frozen_source = freeze(source, terms) if float(terms['daily_drawdown']) > 0 or package.get('payout_schedule_enabled',False) or terms.get('resets') else source
    frozen_source = __import__('withdrawal_controls').freeze(frozen_source,__import__('withdrawal_controls').selected(dict(package,account_size=plan['capital'],payout_split=package['profit_share'])))
    label={'two_phase':'Two-phase challenge','one_phase':'One-phase challenge','instant':'Instant funded account'}[model_of(plan)]
    return dict(id=managed_id(source), journey_source=frozen_source,
        name=(plan['name'] + ' · ' + package['name'])[:100],
        description=label+'. Continuing eligible funded payouts. No included resets or second life.',
        account_size=plan['capital'], fee=package['price'],
        phase1_target=package['phase1_target'], phase2_target=package['phase2_target'],
        max_drawdown=package['drawdown'], daily_drawdown=float(terms['daily_drawdown']), drawdown_type=package.get('drawdown_type','STATIC'),
        payout_split=package['profit_share'], payout_cycles=0, payout_frequency=terms['payout_frequency'],
        challenge_journey=list(route_for(plan)), product_category='standard',
        reset_fee=float(terms.get('resets',{}).get('phase1',0)), funded_reset_fee=float(terms.get('resets',{}).get('funded',0)), second_life_enabled=False, lives_total=1,
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
    from plan_risk_controls import frozen_terms
    frozen = frozen_terms(plan) or {}
    return dict(plan, withdrawal_rules=__import__('withdrawal_controls').read(plan), reset_stage_prices=frozen.get('resets',{}), managed_progress=True, offer_version=offer_version(plan), base_fee=fee, fee=fee,
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
                        params={'id':'eq.'+wanted['id'],'journey_source':'eq.'+current['journey_source']},json=wanted,timeout=(2,5))
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
