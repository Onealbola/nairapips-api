"""Real Gunicorn regression: a slow handler must not monopolize admin login."""
import contextlib,os,socket,subprocess,sys,tempfile,textwrap,threading,time,unittest
from pathlib import Path
import requests
ROOT=Path(__file__).resolve().parents[1]
FIXTURE=r'''
import ast,base64,copy,hashlib,hmac,time,threading
from pathlib import Path
from types import SimpleNamespace
from flask import Flask,request,jsonify
from request_serialization import SerialMutationMiddleware
app=Flask(__name__)
source=Path(SOURCE_PATH).read_text()
functions={n.name:n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef)}
for name in ('staff_login','_make_admin_auth_token'):
 node=copy.deepcopy(functions[name]);node.decorator_list=[]
 exec(compile(ast.Module(body=[node],type_ignores=[]),'auth-fixture','exec'),globals())
os=SimpleNamespace(getenv=lambda key,default=None:{
 'NAIRAPIPS_BOOTSTRAP_ADMIN_USERNAME':'private-test-admin',
 'NAIRAPIPS_BOOTSTRAP_ADMIN_PASSWORD':'private-test-password'}.get(key,default))
_trader_auth_secret=lambda:b'private-fixture-signing-key'
_np_staff_login_rows_v207=lambda username:[]
threading_for_stats=threading
threading=SimpleNamespace(Thread=lambda **kw:SimpleNamespace(start=lambda:None))
app.add_url_rule('/staff_login','staff_login',staff_login,methods=['POST'])
stats={'active':0,'maximum':0};stats_lock=threading_for_stats.Lock()
@app.get('/health')
def health():return jsonify(ok=True)
@app.route('/slow',methods=['GET','POST'])
def slow():
 with stats_lock:
  stats['active']+=1;stats['maximum']=max(stats['maximum'],stats['active'])
 Path(MARKER_DIR,request.args.get('marker','started')).touch()
 try:time.sleep(1.2);return jsonify(ok=True)
 finally:
  with stats_lock:stats['active']-=1
@app.get('/stats')
def read_stats():return jsonify(stats)
app.wsgi_app=SerialMutationMiddleware(app.wsgi_app)
'''

class HTTPWorkerTests(unittest.TestCase):
    def setUp(self):
        self.http=requests.Session();self.http.trust_env=False
    def tearDown(self):self.http.close()
    @contextlib.contextmanager
    def server(self,configured):
        with tempfile.TemporaryDirectory(prefix='np-http-worker-') as tmp:
            path=Path(tmp)
            code=FIXTURE.replace('SOURCE_PATH',repr(str(ROOT/'app.py'))).replace('MARKER_DIR',repr(tmp))
            (path/'http_fixture.py').write_text(code)
            with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            env=dict(os.environ,PYTHONPATH=str(ROOT)+os.pathsep+tmp,PYTHONDONTWRITEBYTECODE='1')
            cmd=[sys.executable,'-m','gunicorn','--bind',f'127.0.0.1:{port}','--workers','1','http_fixture:app']
            # Configured case tests automatic discovery from Render's repository
            # cwd, using no --config argument. Baseline has no config file.
            cwd=ROOT if configured else path
            with tempfile.TemporaryFile() as logs:
                proc=subprocess.Popen(cmd,cwd=cwd,env=env,stdout=logs,stderr=logs)
                url=f'http://127.0.0.1:{port}'
                try:
                    deadline=time.monotonic()+10
                    while time.monotonic()<deadline:
                        if proc.poll() is not None:break
                        try:
                            if self.http.get(url+'/health',timeout=.2).status_code==200:break
                        except requests.RequestException:pass
                        time.sleep(.05)
                    else:self.fail('Gunicorn fixture failed readiness deadline')
                    if proc.poll() is not None:
                        logs.seek(0);self.fail(logs.read().decode()[-2000:])
                    yield url,path
                finally:
                    proc.terminate()
                    try:proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=2)
    def login(self,url,password='private-test-password'):
        return self.http.post(url+'/staff_login',json={'username':'private-test-admin','password':password},timeout=4)
    def slow(self,url,marker,method='GET'):
        outcomes=[]
        def run():
            with requests.Session() as session:
                session.trust_env=False
                outcomes.append(session.request(method,url+'/slow',params={'marker':marker},timeout=6).status_code)
        thread=threading.Thread(target=run);thread.start();return thread,outcomes
    def wait_started(self,path,marker):
        deadline=time.monotonic()+3
        while not (path/marker).exists() and time.monotonic()<deadline:time.sleep(.01)
        self.assertTrue((path/marker).exists())
    def test_default_sync_worker_reproduces_login_wait(self):
        with self.server(False) as (url,path):
            thread,outcomes=self.slow(url,'baseline')
            self.wait_started(path,'baseline');started=time.monotonic()
            result=self.login(url);elapsed=time.monotonic()-started
            thread.join(3)
            self.assertEqual(result.status_code,200);self.assertEqual(outcomes,[200])
            self.assertGreater(elapsed,.85)
    def test_configured_worker_serves_secure_login_during_slow_requests(self):
        with self.server(True) as (url,path):
            thread,outcomes=self.slow(url,'read')
            self.wait_started(path,'read');started=time.monotonic()
            result=self.login(url);elapsed=time.monotonic()-started
            self.assertEqual(result.status_code,200);self.assertLess(elapsed,.7)
            self.assertTrue(result.json().get('token'))
            self.assertNotIn('password',result.json()['staff'])
            self.assertEqual(self.login(url,'wrong').status_code,401)
            thread.join(3);self.assertEqual(outcomes,[200])
    def test_mutations_remain_serial_but_do_not_block_admin_authentication(self):
        with self.server(True) as (url,path):
            first,one=self.slow(url,'one','POST');self.wait_started(path,'one')
            second,two=self.slow(url,'two','POST');time.sleep(.1)
            started=time.monotonic();result=self.login(url)
            self.assertEqual(result.status_code,200);self.assertLess(time.monotonic()-started,.7)
            first.join(4);second.join(4)
            self.assertEqual(one,[200]);self.assertEqual(two,[200])
            self.assertEqual(self.http.get(url+'/stats',timeout=2).json()['maximum'],1)

class MiddlewareTests(unittest.TestCase):
    def test_failed_response_releases_mutation_lock(self):
        from request_serialization import SerialMutationMiddleware
        def broken(env,start):
            if env.get('break'):raise RuntimeError('fixture error')
            return [b'ok']
        app=SerialMutationMiddleware(broken)
        with self.assertRaises(RuntimeError):list(app({'REQUEST_METHOD':'POST','break':True},lambda *a:None))
        result=[]
        thread=threading.Thread(target=lambda:result.extend(app({'REQUEST_METHOD':'POST'},lambda *a:None)))
        thread.start();thread.join(1)
        self.assertFalse(thread.is_alive());self.assertEqual(result,[b'ok'])

if __name__=='__main__':unittest.main()
