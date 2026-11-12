import json, threading, unittest
from request_admission import RequestAdmissionMiddleware, configure_worker_capacity

class AdmissionTests(unittest.TestCase):
    def test_priority_response_closes_files_on_completion_and_client_disconnect(self):
        closed=[]
        class Response:
            def __iter__(self):return iter([b'first',b'second'])
            def close(self):closed.append(True)
        middleware=RequestAdmissionMiddleware(lambda *args:Response(),1)
        env={'PATH_INFO':'/downloads/dd-login-retry.ps1','REQUEST_METHOD':'GET'}
        self.assertEqual(list(middleware(env,lambda *args:None)),[b'first',b'second'])
        iterator=middleware(env,lambda *args:None);self.assertEqual(next(iterator),b'first');iterator.close()
        self.assertEqual(closed,[True,True])
    def test_recovery_installer_remains_readable_when_business_slots_are_full(self):
        from flask import Flask
        from panel_delivery import register_panel_delivery
        from pathlib import Path
        app=Flask(__name__);register_panel_delivery(app)
        middleware=RequestAdmissionMiddleware(app.wsgi_app,1);app.wsgi_app=middleware
        self.assertTrue(middleware.slots.acquire(blocking=False))
        try:
            client=app.test_client()
            expected=(Path(__file__).resolve().parents[1]/'website-downloads/nairapips-dd-login-retry.ps1').read_bytes()
            response=client.get('/downloads/dd-login-retry.ps1')
            self.assertEqual(response.status_code,200);self.assertEqual(response.data,expected)
            self.assertEqual(client.head('/downloads/dd-login-retry.ps1').status_code,200)
            for method,path in [('POST','/downloads/dd-login-retry.ps1'),('GET','/downloads/other.ps1'),('POST','/approve_challenge_purchase')]:
                result=client.open(path,method=method);self.assertEqual(result.status_code,503);self.assertFalse(result.json['operation_started'])
        finally:middleware.slots.release()
    def test_busy_does_not_start_any_business_write_and_priority_bypasses(self):
        started=threading.Event(); release=threading.Event();calls=[]
        def handler(env,start):
            calls.append(env['PATH_INFO'])
            if env['PATH_INFO']=='/slow':started.set();release.wait(2)
            return [b'ok']
        middleware=RequestAdmissionMiddleware(handler,1)
        first=threading.Thread(target=lambda:list(middleware({'PATH_INFO':'/slow','REQUEST_METHOD':'GET'},lambda *a:None)))
        first.start();self.assertTrue(started.wait(1))
        try:
            headers=[]
            response=b''.join(middleware({'PATH_INFO':'/approve_challenge_purchase','REQUEST_METHOD':'POST','HTTP_ORIGIN':'https://nairapips.com'},lambda status,h:headers.append((status,dict(h)))))
            self.assertFalse(json.loads(response)['operation_started'])
            self.assertEqual(headers[0][1]['Access-Control-Allow-Origin'],'https://nairapips.com')
            for path in ('/staff_login','/healthz/http'):
                self.assertEqual(list(middleware({'PATH_INFO':path,'REQUEST_METHOD':'POST'},lambda *a:None)),[b'ok'])
            self.assertNotIn('/approve_challenge_purchase',calls)
        finally:release.set();first.join(3)
    def test_shared_reads_use_six_slots_while_approval_bootstrap_and_login_remain_available(self):
        release=threading.Event();started=[threading.Event() for _ in range(6)]
        def handler(env,start):
            if env.get('index') is not None:
                started[env['index']].set();release.wait(3)
            return [b'ok']
        import ast
        from pathlib import Path
        tree=ast.parse((Path(__file__).resolve().parents[1]/'app.py').read_bytes())
        production=next(n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_NPAdmissionV220')
        reserved=ast.literal_eval(next(k.value for k in production.keywords if k.arg=='reserved_paths'))
        app=RequestAdmissionMiddleware(handler,8,reserved_paths=reserved)
        threads=[threading.Thread(target=lambda i=i:list(app({'PATH_INFO':'/trader_current_account/example','index':i},lambda *a:None))) for i in range(6)]
        try:
            for thread,event in zip(threads,started):
                thread.start();self.assertTrue(event.wait(1))
            self.assertEqual(app.capacity_snapshot()['ordinary_slots'],6)
            blocked=b''.join(app({'PATH_INFO':'/admin/finance_source'},lambda *a:None))
            self.assertFalse(json.loads(blocked)['operation_started'])
            for path in ('/admin_bootstrap','/approve_challenge_purchase','/staff_login','/healthz/http'):
                self.assertEqual(list(app({'PATH_INFO':path},lambda *a:None)),[b'ok'])
        finally:
            release.set()
            for thread in threads:thread.join(3)

    def test_exception_and_stream_failure_release_slot(self):
        def handler(env,start):
            if env.get('broken'):raise RuntimeError('test')
            if env.get('stream'):
                def stream():
                    yield b'part'
                    raise RuntimeError('test')
                return stream()
            return [b'ok']
        middleware=RequestAdmissionMiddleware(handler,1)
        for flag in ('broken','stream'):
            with self.assertRaises(RuntimeError):list(middleware({'PATH_INFO':'/test',flag:True},lambda *a:None))
            self.assertEqual(list(middleware({'PATH_INFO':'/test'},lambda *a:None)),[b'ok'])
    def test_runtime_worker_config(self):
        app=RequestAdmissionMiddleware(lambda *a:[],6)
        self.assertEqual(configure_worker_capacity(4),2)
        self.assertEqual(app.capacity,2)
        self.assertEqual(configure_worker_capacity(8),6)

    def test_snapshot_identifies_busy_lane_without_request_details(self):
        started=threading.Event();release=threading.Event()
        def handler(env,start):
            started.set();release.wait(2)
            return [b'ok']
        app=RequestAdmissionMiddleware(handler,4,reserved_paths=('/approve_challenge_purchase',))
        env={'PATH_INFO':'/approve_challenge_purchase','QUERY_STRING':'secret=private',
             'HTTP_AUTHORIZATION':'private-token','REQUEST_METHOD':'POST'}
        thread=threading.Thread(target=lambda:list(app(env,lambda *a:None)))
        thread.start();self.assertTrue(started.wait(1))
        try:
            snapshot=app.capacity_snapshot()
            self.assertEqual(snapshot['ordinary_slots'],3)
            self.assertEqual(snapshot['active_lanes']['/approve_challenge_purchase']['active'],1)
            self.assertNotIn('private',json.dumps(snapshot))
        finally:release.set();thread.join(3)
        self.assertEqual(app.capacity_snapshot()['active_lanes'],{})

    def test_long_request_diagnostics_exclude_ids_tokens_and_local_values(self):
        started=threading.Event();release=threading.Event()
        def handler(env,start):
            private_local='sensitive-business-value'
            started.set();release.wait(2)
            return [b'ok']
        app=RequestAdmissionMiddleware(handler,1)
        env={'PATH_INFO':'/trader_current_account/private-account-id',
             'HTTP_AUTHORIZATION':'private-token','QUERY_STRING':'private-query'}
        thread=threading.Thread(target=lambda:list(app(env,lambda *a:None)))
        thread.start();self.assertTrue(started.wait(1))
        try:
            with app._state_lock:
                for key,(route,when,ident) in list(app._executions.items()):
                    app._executions[key]=(route,when-31,ident)
            snapshot=app.capacity_snapshot()
            self.assertEqual(snapshot['long_requests'][0]['route'],'/trader_current_account')
            self.assertIn('handler',snapshot['long_requests'][0]['waiting_stack'])
            for secret in ('private-account-id','private-token','private-query','sensitive-business-value'):
                self.assertNotIn(secret,json.dumps(snapshot))
        finally:release.set();thread.join(3)
        self.assertEqual(app.capacity_snapshot()['long_requests'],[])

class ReservedAdminReadTests(unittest.TestCase):
    def test_busy_general_and_operations_cannot_consume_bootstrap_lane(self):
        general=threading.Event();ops=threading.Event();release=threading.Event()
        def handler(env,start):
            path=env['PATH_INFO']
            if path=='/ordinary':general.set();release.wait(2)
            if path=='/admin_operations_center':ops.set();release.wait(2)
            return [b'ok']
        app=RequestAdmissionMiddleware(handler,3,reserved_paths=('/admin_bootstrap','/admin_operations_center'))
        one=threading.Thread(target=lambda:list(app({'PATH_INFO':'/ordinary'},lambda *a:None)))
        two=threading.Thread(target=lambda:list(app({'PATH_INFO':'/admin_operations_center'},lambda *a:None)))
        one.start();two.start();self.assertTrue(general.wait(1));self.assertTrue(ops.wait(1))
        try:
            self.assertEqual(list(app({'PATH_INFO':'/admin_bootstrap'},lambda *a:None)),[b'ok'])
            self.assertEqual(list(app({'PATH_INFO':'/staff_login'},lambda *a:None)),[b'ok'])
            headers=[]
            list(app({'PATH_INFO':'/ordinary'},lambda status,h:headers.append(status)))
            self.assertTrue(headers[0].startswith('503'))
        finally:release.set();one.join(3);two.join(3)

class ResponseWriteTests(unittest.TestCase):
    def test_large_response_is_chunked_without_changing_any_wire_bytes(self):
        from types import SimpleNamespace
        payload=bytes(range(256))*1000;timeouts=[]
        app=RequestAdmissionMiddleware(lambda *a:[payload],1)
        parts=list(app({'PATH_INFO':'/download','gunicorn.socket':SimpleNamespace(settimeout=timeouts.append)},lambda *a:None))
        self.assertEqual(b''.join(parts),payload)
        self.assertTrue(all(len(part)<=65536 for part in parts))
        self.assertEqual(timeouts,[15]);self.assertEqual(app.capacity_snapshot()['active_lanes'],{})
