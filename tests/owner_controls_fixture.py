"""Loopback-only fixture. No production imports or connections."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from flask import Flask,request,jsonify
from werkzeug.serving import make_server
from test_production_controls import Database
from production_controls import ControlsStore,register_controls
from catalog_controls import register_catalog_controls
from owner_controls import register_owner_controls
app=Flask(__name__);db=Database()
def auth():
    if request.headers.get('Authorization')!='Bearer owner-fixture':return None,(jsonify(success=False,error='Sign in required'),401)
    return {'id':'owner','role':'owner'},None
register_controls(app,ControlsStore(db,'https://fixture.invalid','fixture'),auth,lambda *a:None,lambda *a:None)
register_catalog_controls(app,db,'https://fixture.invalid','fixture',auth,lambda *a:None,lambda *a:None)
register_owner_controls(app)
@app.post('/staff_login')
def login():return jsonify(success=True,token='owner-fixture')
flags={'LEAGUE_ENABLED':False,'LEAGUE_PUBLIC_ENABLED':False}
@app.route('/admin/league/flags',methods=['GET','POST'])
def league():
    _,error=auth()
    if error:return error
    if request.method=='POST':
        data=request.get_json();flags[data['flag']]=data['enabled']
    return jsonify(success=True,data={'flags':[{'flag':k,'enabled':v} for k,v in flags.items()]})
server=make_server('127.0.0.1',0,app);print(server.server_port,flush=True);server.serve_forever()
