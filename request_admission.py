"""Keep bounded request capacity available for authentication and readiness."""
import json
import threading
import weakref

_instances = weakref.WeakSet()


def configure_worker_capacity(threads):
    # Called before Gunicorn admits requests; never swap a semaphore in use.
    capacity = max(1, int(threads) - 2)
    for instance in list(_instances):
        instance.slots = threading.BoundedSemaphore(capacity)
        instance.capacity = capacity
    return capacity


class RequestAdmissionMiddleware:
    PRIORITY_PATHS = frozenset({'/staff_login', '/healthz/http'})
    ALLOWED_ORIGINS = frozenset({'https://nairapips.com', 'https://www.nairapips.com'})

    def __init__(self, application, capacity=6):
        self.application = application
        self.capacity = max(1, int(capacity))
        self.slots = threading.BoundedSemaphore(self.capacity)
        _instances.add(self)

    def __call__(self, environ, start_response):
        path = str(environ.get('PATH_INFO') or '')
        if path in self.PRIORITY_PATHS or environ.get('REQUEST_METHOD') == 'OPTIONS':
            yield from self.application(environ, start_response)
            return
        slots = self.slots
        if not slots.acquire(blocking=False):
            body = json.dumps({'success': False, 'code': 'NP_REQUEST_CAPACITY_BUSY',
                'operation_started': False,
                'error': 'The server is handling other requests. This request has not started. Please try again shortly.'}).encode()
            headers = [('Content-Type', 'application/json; charset=utf-8'),
                ('Content-Length', str(len(body))), ('Cache-Control', 'no-store'),
                ('Retry-After', '2'), ('Vary', 'Origin')]
            origin = str(environ.get('HTTP_ORIGIN') or '')
            if origin in self.ALLOWED_ORIGINS:
                headers.append(('Access-Control-Allow-Origin', origin))
            start_response('503 Service Unavailable', headers)
            yield body
            return
        try:
            response = self.application(environ, start_response)
            try:
                yield from response
            finally:
                close = getattr(response, 'close', None)
                if close:
                    close()
        finally:
            slots.release()
