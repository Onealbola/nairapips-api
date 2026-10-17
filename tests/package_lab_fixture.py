"""Local browser fixture only. Never imports app.py or contacts production."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from flask import Flask,request,jsonify
from werkzeug.serving import make_server
import copy
from types import SimpleNamespace
class Database:
    def __init__(self):self.rows={"business_settings":[]};self.writes=[]
    def response(self,rows,status=200):return SimpleNamespace(json=lambda:copy.deepcopy(rows),status_code=status,raise_for_status=lambda:None)
    def matches(self,row,params):
        return all(key in ("select","limit","order","offset") or (row.get(key) is None if value=="is.null" else str(row.get(key))==value[3:]) for key,value in (params or {}).items())
    def get(self,url,headers=None,params=None,timeout=None):return self.response([r for r in self.rows["business_settings"] if self.matches(r,params)])
    def post(self,url,headers=None,json=None,timeout=None):
        self.rows["business_settings"].append(copy.deepcopy(json));self.writes.append(("business_settings",copy.deepcopy(json)));return self.response([json],201)
    def patch(self,url,headers=None,params=None,json=None,timeout=None):
        result=[]
        for row in self.rows["business_settings"]:
            if self.matches(row,params):row.update(copy.deepcopy(json));result.append(row)
        self.writes.append(("business_settings",copy.deepcopy(json)));return self.response(result)
from package_configuration import PackageStore,register_package_configuration
from package_lab import register_package_lab

app=Flask(__name__)
db=Database()
store=PackageStore(db,'https://fixture.invalid','fixture-only')
def auth():
    token=request.headers.get('Authorization')
    if token!='Bearer fixture-owner':return None,(jsonify(success=False,error='Owner login required'),401)
    return {'id':'fixture-owner','role':'super_admin'},None
register_package_configuration(app,store,auth,lambda *args:None)
register_package_lab(app,store,auth)
@app.post('/staff_login')
def login():
    body=request.get_json()
    if body.get('username')=='fixture-owner' and body.get('password')=='fixture-password':
        return jsonify(success=True,token='fixture-owner')
    return jsonify(success=False,error='Invalid fixture login'),401
@app.get('/fixture-writes')
def writes():
    return jsonify(tables=[name for name,_ in db.writes],config_rows=db.rows['business_settings'])

if __name__=='__main__':
    server=make_server('127.0.0.1',0,app,threaded=True)
    print('READY:'+str(server.server_port),flush=True)
    server.serve_forever()
