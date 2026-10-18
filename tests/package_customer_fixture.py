"""Loopback-only customer fixture. Production app and databases are never loaded."""
import sys,copy
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from flask import Flask,request,jsonify
from werkzeug.serving import make_server
from types import SimpleNamespace
from uuid import uuid4
from test_package_checkout import Transport
from package_configuration import defaults
from package_checkout import PackageOrderStore,register_package_checkout
from package_customer import register_package_customer
app=Flask(__name__);transport=Transport();tid=str(uuid4());cfg=defaults();cfg.update(enabled=True,minimum_price=1000,revision=5)
for key,rows in cfg['options'].items():
    for row in rows:row.update(enabled=True,price=5000 if key=='capital' else 0)
next(r for r in cfg['options']['capital'] if r['value']=='250000')['price']=10000
next(r for r in cfg['options']['profit_share'] if r['value']=='70').update(price=2000,price_type='fixed')
next(r for r in cfg['options']['profit_share'] if r['value']=='80').update(price=50,price_type='percent')
ready=True
register_package_checkout(app,PackageOrderStore(transport,'https://fixture.invalid','fixture'),
 SimpleNamespace(read=lambda **kw:(copy.deepcopy(cfg),None)),SimpleNamespace(read=lambda **kw:({'features':{'challenge_sales':True}},None)),
 lambda _:(tid,None) if request.headers.get('Authorization')=='Bearer fixture-trader' else ('','Authentication required'),lambda:{'ready':ready})
register_package_customer(app)
@app.post('/login_trader')
def login():return jsonify(success=True,data={'id':tid,'auth_token':'fixture-trader'})
@app.get('/payment_accounts')
def banks():return jsonify([{'bank_name':'Fixture bank','account_name':'Fixture account','account_number':'fixture-only','status':'active'}])
@app.post('/upload_payment_proof')
def upload():return jsonify(success=True,url='https://proof.invalid/fixture.png')
@app.get('/fixture-counts')
def counts():return jsonify(quotes=len(transport.quotes),submissions=sum(1 for call in transport.calls if call[1].endswith('/rpc/np_package_checkout')))
server=make_server('127.0.0.1',0,app);print('READY:'+str(server.server_port),flush=True);server.serve_forever()
