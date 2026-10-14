"""Keep bounded request capacity available for authentication and readiness."""
import json
import threading
import weakref
import time
import re
import sys

_instances = weakref.WeakSet()


def configure_worker_capacity(threads):
    # Called before Gunicorn admits requests; never swap a semaphore in use.
    capacity = max(1, int(threads) - 2)
    for instance in list(_instances):
        instance.configure_capacity(capacity)
    return capacity


class RequestAdmissionMiddleware:
    PRIORITY_PATHS = frozenset({'/staff_login', '/healthz/http'})
    ALLOWED_ORIGINS = frozenset({'https://nairapips.com', 'https://www.nairapips.com'})

    def __init__(self, application, capacity=6, reserved_paths=()):
        self.application = application
        self._state_lock = threading.Lock()
        self._active = {}
        self._executions = {}
        self.reserved_paths = tuple(dict.fromkeys(reserved_paths))
        self.configure_capacity(capacity)
        _instances.add(self)

    def configure_capacity(self, capacity):
        self.capacity = max(1, int(capacity))
        # One separate lane for each essential read. Operations traffic cannot
        # take the bootstrap lane. No new auth authority is introduced.
        reserved = self.reserved_paths[:max(0, self.capacity - 1)]
        self.reserved_slots = {path: threading.BoundedSemaphore(1) for path in reserved}
        self.slots = threading.BoundedSemaphore(self.capacity - len(reserved))

    def capacity_snapshot(self):
        """Operational counts only; exclude URLs, identities and request bodies."""
        now = time.monotonic()
        with self._state_lock:
            lanes = {name: {'active': len(started),
                'oldest_seconds': round(max((now - t for t in started.values()), default=0), 1)}
                for name, started in self._active.items() if started}
            executions = list(self._executions.values())
        frames = sys._current_frames()
        stuck = []
        for route, started, thread in executions:
            if now - started < 30:
                continue
            frame = frames.get(thread)
            stack = []
            while frame is not None and len(stack) < 12:
                stack.append(frame.f_code.co_name)
                frame = frame.f_back
            stuck.append({'route': route, 'seconds': round(now-started, 1), 'waiting_stack': stack})
        return {'business_slots': self.capacity,
            'ordinary_slots': self.capacity - len(self.reserved_slots),
            'reserved_paths': list(self.reserved_slots), 'active_lanes': lanes,
            'long_requests': stuck}

    def __call__(self, environ, start_response):
        path = str(environ.get('PATH_INFO') or '')
        if path in self.PRIORITY_PATHS or environ.get('REQUEST_METHOD') == 'OPTIONS':
            yield from self.application(environ, start_response)
            return
        slots = self.reserved_slots.get(path, self.slots)
        lane = path if path in self.reserved_slots else 'ordinary'
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
        token = object()
        # Static first route segment only; never include account IDs or queries.
        segment = path.strip('/').split('/')[0]
        route = '/'+segment if re.fullmatch(r'[A-Za-z][A-Za-z0-9_]{0,79}', segment) else '/dynamic'
        with self._state_lock:
            self._active.setdefault(lane, {})[token] = time.monotonic()
            self._executions[token] = (route, time.monotonic(), threading.get_ident())
        try:
            response = self.application(environ, start_response)
            try:
                yield from response
            finally:
                close = getattr(response, 'close', None)
                if close:
                    close()
        finally:
            with self._state_lock:
                self._active[lane].pop(token, None)
                self._executions.pop(token, None)
            slots.release()
