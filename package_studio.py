"""Owner-only saved offer design. This namespace has no checkout consumer.

Never translate this draft into legacy tariffs, contracts or entitlements.
The browser engine rehearses rules; it is not a production enforcement engine.
"""
import copy
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from flask import jsonify, request, make_response
from production_controls import ControlsStore, ControlsUnavailable, Conflict

ROOT = Path(__file__).with_name('package_studio_assets')
KEY = 'package_studio_owner_draft_v1'
RELEASE = 'owner-package-studio-draft-2026-11-02'
TEMPLATE = json.loads((ROOT / 'shape.json').read_text())
ASSET_KEY = 'package_studio_owner_assets_v1'
SELECTION = dict(capital=[250000,500000,1000000,2000000,5000000],
                 share=[50,60,70], cycles=[3,4,5], drawdown=[5,10,15,20,25,30],
                 daily=[0,10], reset=[0,1], profitCap=[15,20,30], phase1Target=[8,10])


def number(value, low, high):
    if type(value) not in (int,float) or not math.isfinite(value) or not low <= value <= high:
        raise ValueError('Invalid numeric draft setting')


def shape(value, template):
    if isinstance(template, dict):
        if not isinstance(value, dict) or set(value) != set(template):
            raise ValueError('Missing or unsupported draft fields')
        for key, child in template.items():
            shape(value[key], child)
    elif type(template) is bool:
        if type(value) is not bool:
            raise ValueError('Availability switches must be true or false')
    elif type(template) in (int,float):
        number(value, 0, 1e9)
    elif isinstance(template, list):
        if not isinstance(value, list):
            raise ValueError('Invalid combination list')


def validate_config(value):
    shape(value, TEMPLATE)
    if type(value['revision']) is not int or not 1 <= value['revision'] <= 1e9:
        raise ValueError('Invalid draft revision')
    if (value['phase1'], value['phase2']) not in ((8,5),(10,8)):
        raise ValueError('Choose either the 8% / 5% or 10% / 8% target pair')
    if value['grossCap'] != 15 or value['rounding'] != 50:
        raise ValueError('Unsupported base cap or rounding')
    for group in ('base','share','cycles','drawdown','reset','profitCap'):
        previous = -1
        for key in sorted(value[group], key=int):
            amount = value[group][key]
            number(amount, 50 if group == 'base' else 0, 1e8 if group == 'base' else 1000)
            if group == 'base' and amount % 50:
                raise ValueError('Base fees must be multiples of NGN50')
            if amount < previous:
                raise ValueError('Prices cannot decrease for a larger benefit')
            previous = amount
    for group, key in (('share','50'),('cycles','3'),('drawdown','5'),('reset','0'),('profitCap','15')):
        if value[group][key] != 0:
            raise ValueError('Included benefits must have no add-on charge')
    number(value['combination'],0,1000)
    choices = dict(maxShare=[50,60,70], maxCycles=[3,4,5], maxCap=[15,20,30],
                   maxDD=[5,10,15,20,25,30], maxResets=[0,1], minTarget=[8,10],
                   dailyAboveDD=[0,5,10,15,20,25,30])
    for policy in value['riskPolicy'].values():
        for key, allowed in choices.items():
            if policy[key] not in allowed:
                raise ValueError('Unsupported large-account policy')
        number(policy['maxPayout'],1,1e8)
        number(policy['maxPayoutToFee'],1,100)
    if len(value['blockedCombinations']) > 200:
        raise ValueError('At most 200 exact combinations can be disabled')
    for selection in value['blockedCombinations']:
        if not isinstance(selection,dict) or set(selection) != set(SELECTION):
            raise ValueError('Invalid disabled combination')
        for key, allowed in SELECTION.items():
            if type(selection[key]) not in (int,float) or selection[key] not in allowed:
                raise ValueError('Unsupported disabled-combination choice')
    return copy.deepcopy(value)


def defaults(config=None):
    return dict(revision=0, mode='draft', checkout_enabled=False,
                config=copy.deepcopy(config), updated_at=None, updated_by=None)


def validate_document(doc):
    if not isinstance(doc,dict) or set(doc) != set(defaults()):
        raise ValueError('Invalid saved draft')
    if type(doc['revision']) is not int or doc['revision'] < 0:
        raise ValueError('Invalid saved revision')
    if doc['mode'] != 'draft' or doc['checkout_enabled'] is not False:
        raise ValueError('Package Studio permits draft mode only')
    validate_config(doc['config'])
    for key in ('updated_at','updated_by'):
        if doc[key] is not None and (not isinstance(doc[key],str) or len(doc[key]) > 200):
            raise ValueError('Invalid draft metadata')
    return copy.deepcopy(doc)


class StudioStore(ControlsStore):
    def __init__(self, transport, url, key):
        def checked(doc):
            try:
                return validate_document(doc)
            except ValueError as exc:
                raise ControlsUnavailable('Saved Studio draft is invalid') from exc
        def checked_assets(doc):
            if not isinstance(doc,dict) or set(doc) != {'revision','defaults','html','updated_at'}:
                raise ControlsUnavailable('Owner Studio assets are not configured')
            validate_config(doc['defaults'])
            if not isinstance(doc['html'],str) or not 1000 < len(doc['html']) < 500000:
                raise ControlsUnavailable('Owner Studio assets are invalid')
            return copy.deepcopy(doc)
        def missing_assets():
            raise ControlsUnavailable('Owner Studio setup is required. Run the private-storage migration and install the owner Studio assets.')
        self.assets=ControlsStore(transport,url,key,ASSET_KEY,missing_assets,checked_assets)
        self.assets.url=url.rstrip('/')+'/rest/v1/package_studio_private_settings'
        def proposed_defaults():
            assets,_=self.assets.read(fresh=True)
            return defaults(assets['defaults'])
        super().__init__(transport,url,key,KEY,proposed_defaults,checked)
        self.url=url.rstrip('/')+'/rest/v1/package_studio_private_settings'

    def save_draft(self, body, actor):
        if not isinstance(body,dict) or set(body) != {'revision','config'}:
            raise ValueError('Supply the loaded server revision and applied draft settings')
        if type(body['revision']) is not int:
            raise ValueError('Invalid server revision')
        config = validate_config(body['config'])
        old, raw = self.read(fresh=True)
        if body['revision'] != old['revision']:
            raise Conflict('Another admin saved a newer draft. Export your edits, then reload the server draft.')
        doc = defaults(config)
        doc.update(revision=old['revision']+1, config=config,
                   updated_at=datetime.now(timezone.utc).isoformat(), updated_by=str(actor))
        return self.persist(doc,raw,actor)


def register_package_studio(app, store, require_admin, audit):
    def owner():
        admin, error = require_admin()
        if error is not None:
            return None,error
        if str((admin or {}).get('role') or '').lower() not in ('owner','super_admin','superadmin'):
            return None,(jsonify(success=False,error='Owner or super admin access required'),403)
        return admin,None

    def secured(response):
        response.headers.update({'Cache-Control':'no-store', 'X-Content-Type-Options':'nosniff',
                                 'Referrer-Policy':'no-referrer', 'X-Frame-Options':'DENY',
                                 'Content-Security-Policy':"default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"})
        return response

    @app.get('/package-studio')
    def package_studio_page():
        return secured(make_response((ROOT / 'login.html').read_text()))

    @app.get('/admin/package_studio/view')
    def package_studio_view():
        _,error = owner()
        if error is not None:
            return error
        try:
            assets,_=store.assets.read(fresh=True)
            return secured(make_response(assets['html']))
        except ControlsUnavailable:
            return secured(jsonify(success=False,error='Private Studio storage is not ready. Run the database setup and install the owner draft assets.')),503

    @app.route('/admin/package_studio/draft', methods=['GET','POST'])
    def package_studio_draft():
        admin,error = owner()
        if error is not None:
            return error
        try:
            if request.method == 'POST':
                if request.content_length and request.content_length > 100000:
                    raise ValueError('Draft settings are too large')
                doc = store.save_draft(request.get_json(silent=True),admin.get('id') or 'owner')
                audit('package_studio','owner_draft_saved',str({'revision':doc['revision']}),admin,KEY)
            else:
                doc,_ = store.read(fresh=True)
            return secured(jsonify(success=True,release=RELEASE,data=doc,
                                   production_effect='none',checkout_enabled=False))
        except Conflict as exc:
            return secured(jsonify(success=False,error=str(exc))),409
        except ValueError as exc:
            return secured(jsonify(success=False,error=str(exc))),400
        except ControlsUnavailable as exc:
            return secured(jsonify(success=False,error=str(exc))),503
