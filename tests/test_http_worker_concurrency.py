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
from request_admission import RequestAdmissionMiddleware
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
app.wsgi_app=SerialMutationMiddleware(app.wsgi_app, serial_paths={'/slow'})
if ENABLE_ADMISSION:
 app.wsgi_app=RequestAdmissionMiddleware(app.wsgi_app, reserved_paths=('/admin_bootstrap','/admin_operations_center','/approve_challenge_purchase') if ENABLE_RESERVED else ())
app.add_url_rule('/admin_operations_center','slow_ops',slow,methods=['GET'])
app.add_url_rule('/approve_challenge_purchase','private_approval_fixture',slow,methods=['POST'])
@app.get('/admin_bootstrap')
def bootstrap():return jsonify(ok=True)
@app.get('/healthz/http')
def priority_health():return jsonify(ok=True)
'''

class HTTPWorkerTests(unittest.TestCase):
    def setUp(self):
        self.http=requests.Session();self.http.trust_env=False
    def tearDown(self):self.http.close()
    @contextlib.contextmanager
    def server(self,configured,admission=False,reserved=False):
        with tempfile.TemporaryDirectory(prefix='np-http-worker-') as tmp:
            path=Path(tmp)
            code=FIXTURE.replace('ENABLE_RESERVED',repr(reserved)).replace('ENABLE_ADMISSION',repr(admission)).replace('SOURCE_PATH',repr(str(ROOT/'app.py'))).replace('MARKER_DIR',repr(tmp))
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
            self.assertEqual(one,[200]);self.assertEqual(two,[503])
            self.assertEqual(self.http.get(url+'/stats',timeout=2).json()['maximum'],1)
    def test_full_mutation_wave_cannot_exhaust_authentication_threads(self):
        with self.server(True) as (url,path):
            first,one=self.slow(url,'wave-first','POST');self.wait_started(path,'wave-first')
            wave=[self.slow(url,'wave-'+str(i),'POST') for i in range(12)]
            started=time.monotonic();result=self.login(url)
            self.assertEqual(result.status_code,200);self.assertLess(time.monotonic()-started,.7)
            for thread,outcomes in wave:
                thread.join(3);self.assertEqual(outcomes,[503])
            first.join(3);self.assertEqual(one,[200])
            self.assertEqual(self.http.get(url+'/stats',timeout=2).json()['maximum'],1)

    def test_slow_read_wave_leaves_login_and_health_capacity(self):
        with self.server(True,admission=True) as (url,path):
            active=[self.slow(url,'read-'+str(i)) for i in range(8)]
            for i in range(8):self.wait_started(path,'read-'+str(i))
            wave=[self.slow(url,'overflow-'+str(i)) for i in range(12)]
            started=time.monotonic();result=self.login(url)
            self.assertEqual(result.status_code,200)
            self.assertLess(time.monotonic()-started,.7)
            self.assertEqual(self.login(url,'wrong').status_code,401)
            self.assertEqual(self.http.get(url+'/healthz/http',timeout=.7).status_code,200)
            for thread,outcomes in wave:
                thread.join(3);self.assertEqual(outcomes,[503])
            for thread,outcomes in active:
                thread.join(3);self.assertEqual(outcomes,[200])
            self.assertEqual(self.http.get(url+'/stats',timeout=2).json()['maximum'],8)

    def test_admin_bootstrap_and_login_survive_busy_general_and_ops_requests(self):
        with self.server(True,admission=True,reserved=True) as (url,path):
            active=[self.slow(url,'general-'+str(i)) for i in range(5)]
            for i in range(5):self.wait_started(path,'general-'+str(i))
            outcomes=[]
            def run_ops():
                with requests.Session() as session:
                    session.trust_env=False
                    outcomes.append(session.get(url+'/admin_operations_center?marker=ops',timeout=4).status_code)
            ops=threading.Thread(target=run_ops);ops.start();self.wait_started(path,'ops')
            started=time.monotonic()
            self.assertEqual(self.http.get(url+'/admin_bootstrap',timeout=.7).status_code,200)
            self.assertEqual(self.login(url).status_code,200)
            self.assertLess(time.monotonic()-started,.7)
            self.assertEqual(self.http.get(url+'/slow',timeout=.7).status_code,503)
            approval=self.http.post(url+'/approve_challenge_purchase?marker=approval',json={'id':'private-test'},timeout=3)
            self.assertEqual(approval.status_code,200)
            self.assertTrue((path/'approval').exists())
            for thread,result in active:thread.join(3);self.assertEqual(result,[200])
            ops.join(3);self.assertEqual(outcomes,[200])

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
    def test_busy_response_does_not_run_mutation_and_preserves_cors(self):
        import json
        from request_serialization import SerialMutationMiddleware
        calls=[];entered=threading.Event();release=threading.Event()
        def work(env,start):
            calls.append(env['PATH_INFO']);entered.set();release.wait(2)
            return [b'ok']
        app=SerialMutationMiddleware(work,serial_paths={'/approve'})
        first=threading.Thread(target=lambda:list(app({'REQUEST_METHOD':'POST','PATH_INFO':'/approve'},lambda *a:None)))
        first.start();self.assertTrue(entered.wait(1));response=[]
        try:
            body=b''.join(app({'REQUEST_METHOD':'POST','PATH_INFO':'/approve','HTTP_ORIGIN':'https://nairapips.com'},lambda status,headers:response.append((status,dict(headers)))))
            self.assertTrue(response[0][0].startswith('503'))
            self.assertFalse(json.loads(body)['operation_started'])
            self.assertEqual(response[0][1]['Access-Control-Allow-Origin'],'https://nairapips.com')
            self.assertEqual(calls,['/approve'])
        finally:release.set();first.join(3)
    def test_account_ingestion_is_not_parked_behind_admin_action(self):
        from request_serialization import SerialMutationMiddleware
        entered=threading.Event();release=threading.Event()
        def work(env,start):
            if env['PATH_INFO']=='/approve':entered.set();release.wait(2)
            return [b'ok']
        app=SerialMutationMiddleware(work,serial_paths={'/approve'})
        first=threading.Thread(target=lambda:list(app({'REQUEST_METHOD':'POST','PATH_INFO':'/approve'},lambda *a:None)))
        first.start();self.assertTrue(entered.wait(1))
        try:
            started=time.monotonic()
            self.assertEqual(list(app({'REQUEST_METHOD':'POST','PATH_INFO':'/monitoring_snapshot'},lambda *a:None)),[b'ok'])
            self.assertLess(time.monotonic()-started,.2)
        finally:release.set();first.join(3)

if __name__=='__main__':unittest.main()
