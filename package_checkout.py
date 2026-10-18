"""Authenticated new-product quotes/checkout over transactional package storage.

Legacy purchase endpoints and records are independent. No import-time database
work, lifecycle workers, notifications or broker calls are started here.
"""
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit
from uuid import uuid4
from flask import jsonify, request
from package_configuration import quote, CHOICES
from package_contracts import make_contract,validate_contract,identity
from production_controls import ControlsUnavailable


class PackageStorageError(RuntimeError):pass
class PackageOrderRejected(ValueError):pass
class PackageOrderUnconfirmed(PackageStorageError):pass


class PackageOrderStore:
    def __init__(self,transport,url,key):
        self.transport=transport;self.base=url.rstrip('/')+'/rest/v1/'
        self.headers={'apikey':key,'Authorization':'Bearer '+key}
    def read(self,table,params):
        try:
            r=self.transport.get(self.base+table,headers=self.headers,params=params,timeout=(2,5))
            r.raise_for_status();rows=r.json()
            if not isinstance(rows,list):raise ValueError('Invalid package storage response')
            return rows
        except Exception as exc:raise PackageStorageError('Package records are temporarily unavailable') from exc
    def schema_available(self):
        try:
            r=self.transport.get(self.base+'rpc/np_package_schema_version',headers=self.headers,timeout=(2,4))
            result=r.json()
            return r.ok and isinstance(result,dict) and result.get('schema_version')=='NP_PACKAGE_SCHEMA_1' and result.get('ready') is True
        except Exception:return False
    def create_quote(self,trader_id,config,selection):
        priced=quote(config,selection);contract=make_contract(priced,trader_id)
        now=datetime.now(timezone.utc)
        row=dict(id=str(uuid4()),trader_id=trader_id,catalog_revision=config['revision'],
                 document=contract['document'],rules_hash=contract['rules_hash'],price=priced['total'],
                 created_at=now.isoformat(),expires_at=(now+timedelta(minutes=10)).isoformat())
        try:
            r=self.transport.post(self.base+'np_package_quotes',headers=dict(self.headers,Prefer='return=representation'),json=row,timeout=(2,5))
            r.raise_for_status()
            saved=r.json()
            if not saved or saved[0].get('id')!=row['id']:raise ValueError('Quote insert was not confirmed')
        except Exception as exc:raise PackageStorageError('The quote could not be saved. No purchase has been created.') from exc
        return {**row,'price_breakdown':priced}
    def checkout(self,trader_id,body,allow_new=None):
        if not isinstance(body,dict) or set(body)!={'quote_id','checkout_key','payment_proof_url','accepted_rules_hash'}:
            raise ValueError('Quote identity, checkout key, accepted rules hash and payment proof are required')
        qid,key=identity(body['quote_id']),identity(body['checkout_key'])
        proof=body['payment_proof_url']
        if not isinstance(proof,str) or len(proof)>4096:raise ValueError('A valid HTTPS payment proof URL is required')
        parsed=urlsplit(proof)
        if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError('A valid HTTPS payment proof URL is required')
        rows=self.read('np_package_quotes',{'id':'eq.'+qid,'trader_id':'eq.'+trader_id,'select':'*','limit':'1'})
        if not rows:raise PackageOrderRejected('This quote does not belong to the signed-in trader')
        saved=rows[0]
        validate_contract(saved['document'],saved['rules_hash'])
        if saved['document']['trader_id']!=trader_id or saved['rules_hash']!=body['accepted_rules_hash']:
            raise PackageOrderRejected('The accepted contract does not match this quote')
        if saved.get('consumed_purchase_id') is None and allow_new is not None:
            allow_new()
        payload=dict(p_trader_id=trader_id,p_quote_id=qid,p_checkout_key=key,p_payment_proof=proof)
        try:
            r=self.transport.post(self.base+'rpc/np_package_checkout',headers=self.headers,json=payload,timeout=(2,7))
        except Exception as exc:
            raise PackageOrderUnconfirmed('Submission was not confirmed. Check purchase history or retry with the same checkout key.') from exc
        if not r.ok:
            try:code=r.json().get('code')
            except Exception:code=None
            if code in ('P0001','23505','23503','23514'):
                raise PackageOrderRejected('Purchase was not accepted. Refresh the quote and check your existing purchase history before submitting again.')
            raise PackageOrderUnconfirmed('Submission was not confirmed. Check purchase history or retry with the same checkout key.')
        try:
            result=r.json()
            if not isinstance(result,dict) or not result.get('purchase_id') or not result.get('contract_id'):
                raise ValueError('Invalid checkout receipt')
            return result
        except Exception as exc:
            raise PackageOrderUnconfirmed('Submission was not confirmed. Check purchase history or retry with the same checkout key.') from exc
    def contracts(self,trader_id,page=1):
        return self.read('np_package_contracts',{'trader_id':'eq.'+trader_id,
          'select':'id,purchase_id,catalog_revision,rules_version,rules_hash,document,created_at',
          'order':'created_at.desc,id.desc','limit':'21','offset':str((page-1)*20)})


def register_package_checkout(app,store,config_store,controls_store,authenticated_trader,release_readiness):
    def auth():
        tid,error=authenticated_trader(None)
        if error:return None,(jsonify(success=False,error=error),401)
        try:return identity(tid),None
        except ValueError:return None,(jsonify(success=False,error='Trader authentication is required'),401)
    def open_catalog():
        config,_=config_store.read(fresh=True)
        flags,_=controls_store.read()
        if not config['enabled'] or not flags['features']['challenge_sales']:
            raise PackageOrderRejected('The new package product is not open for purchases')
        readiness=release_readiness()
        if not readiness['ready']:
            raise PackageOrderRejected('The new package product is not open for purchases')
        return config
    def preview_catalog():
        config,_=config_store.read(fresh=True)
        # Before the owner first saves a tariff, traders can plan with the
        # known product choices. No example prices are exposed or substituted.
        initial=config['revision']==0 and all(r.get('price') is None for rows in config['options'].values() for r in rows)
        options={k:[r for r in rows if initial or r['enabled']] for k,rows in config['options'].items()}
        return config,options
    @app.get('/trader/package_catalog')
    def trader_package_catalog():
        _,error=auth()
        if error is not None:return error
        try:
            config,options=preview_catalog()
            flags,_=controls_store.read()
            checkout_open=bool(config['enabled'] and flags['features']['challenge_sales'] and release_readiness()['ready'])
            return jsonify(success=True,available=True,checkout_open=checkout_open,
              catalog_revision=config['revision'],minimum_price=config['minimum_price'],options=options)
        except (ControlsUnavailable,PackageStorageError):return jsonify(success=False,error='Package availability could not be verified'),503
    @app.post('/trader/package_price')
    def trader_package_price():
        _,error=auth()
        if error is not None:return error
        try:
            config,options=preview_catalog();body=request.get_json(silent=True)
            if not isinstance(body,dict) or set(body)!={'choices'}:raise ValueError('Submit package choices only')
            choices=body['choices']
            if not isinstance(choices,dict) or set(choices)-set(CHOICES):raise ValueError('Unknown package choices')
            selected=[]
            for key,rows in options.items():
                if key=='phase2_target' and choices.get('phases')=='one':
                    if choices.get(key) not in (None,''):raise ValueError('One phase cannot include a Phase 2 target')
                    continue
                row=next((r for r in rows if r['value']==choices.get(key)),None)
                if row is None:
                    if not rows:return jsonify(success=True,price=None,price_pending=True)
                    raise ValueError('This choice is not available: '+key)
                selected.append(row)
            if not config.get('minimum_price') or any(r.get('price') is None for r in selected):
                return jsonify(success=True,price=None,price_pending=True)
            # Pricing before launch is read-only. Order and stored quote routes
            # independently retain the publication and runtime gates below.
            priced=quote(config,choices)
            return jsonify(success=True,price=priced,price_pending=False)
        except ValueError as exc:return jsonify(success=False,error=str(exc)),400
        except (ControlsUnavailable,PackageStorageError) as exc:return jsonify(success=False,error=str(exc)),503
    @app.post('/trader/package_quote')
    def trader_package_quote():
        tid,error=auth()
        if error is not None:return error
        try:
            config=open_catalog();body=request.get_json(silent=True)
            if not isinstance(body,dict) or set(body)!={'choices'}:raise ValueError('Submit package choices only')
            saved=store.create_quote(tid,config,body['choices'])
            return jsonify(success=True,quote=saved)
        except PackageOrderRejected as exc:return jsonify(success=False,error=str(exc)),409
        except ValueError as exc:return jsonify(success=False,error=str(exc)),400
        except (ControlsUnavailable,PackageStorageError) as exc:return jsonify(success=False,error=str(exc)),503
    @app.post('/trader/package_checkout')
    def trader_package_checkout():
        tid,error=auth()
        if error is not None:return error
        try:
            # Replayed accepted orders remain recoverable even when sales pause;
            # the transactional RPC checks publication only for a new purchase.
            receipt=store.checkout(tid,request.get_json(silent=True),allow_new=open_catalog)
            return jsonify(success=True,**receipt)
        except PackageOrderRejected as exc:return jsonify(success=False,error=str(exc),purchase_status='rejected'),409
        except ValueError as exc:return jsonify(success=False,error=str(exc)),400
        except PackageOrderUnconfirmed as exc:return jsonify(success=False,error=str(exc),purchase_status='unconfirmed'),503
        except (PackageStorageError,ControlsUnavailable) as exc:return jsonify(success=False,error=str(exc)),503
    @app.get('/trader/package_contracts')
    def trader_package_contracts():
        tid,error=auth()
        if error is not None:return error
        try:
            page=int(request.args.get('page','1'))
            if not 1<=page<=10000:raise ValueError('Invalid contract page')
            rows=store.contracts(tid,page)
            for row in rows:validate_contract(row['document'],row['rules_hash'])
            return jsonify(success=True,contracts=rows[:20],page=page,has_more=len(rows)>20)
        except ValueError:return jsonify(success=False,error='Invalid contract page or stored contract'),400
        except PackageStorageError as exc:return jsonify(success=False,error=str(exc)),503
