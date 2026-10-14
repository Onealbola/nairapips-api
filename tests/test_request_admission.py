import json, threading, unittest
from request_admission import RequestAdmissionMiddleware, configure_worker_capacity

class AdmissionTests(unittest.TestCase):
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
