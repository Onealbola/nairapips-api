"""Serialize admin writes without parking Gunicorn's request threads."""
import json
import threading


class SerialMutationMiddleware:
    MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
    AUTH_PATHS = frozenset({"/staff_login", "/admin_view_trader_token", "/admin_view_trader_session"})
    ALLOWED_ORIGINS = frozenset({"https://nairapips.com", "https://www.nairapips.com"})

    def __init__(self, application, serial_paths=None, admin_prefixes=(), bypass_paths=()):
        self.application = application
        self.lock = threading.RLock()
        self.serial_paths = None if serial_paths is None else frozenset(serial_paths)
        self.admin_prefixes = tuple(admin_prefixes)
        self.bypass_paths = self.AUTH_PATHS | frozenset(bypass_paths)

    def __call__(self, environ, start_response):
        method = str(environ.get("REQUEST_METHOD") or "").upper()
        path = str(environ.get("PATH_INFO") or "")
        protected = self.serial_paths is None or path in self.serial_paths or path.startswith(self.admin_prefixes)
        if method not in self.MUTATING_METHODS or path in self.bypass_paths or not protected:
            return self.application(environ, start_response)
        return self._serialized(environ, start_response)

    def _serialized(self, environ, start_response):
        # Blocking here parked every HTTP thread behind a single slow action.
        # A busy response is sent BEFORE calling Flask: no operation has begun,
        # no account has been issued, and a retry can never repeat this request.
        if not self.lock.acquire(blocking=False):
            body = json.dumps({"success": False, "code": "NP_ADMIN_ACTION_BUSY",
                "operation_started": False,
                "error": "Another admin action is still finishing. This request has not started. Please try again shortly."}).encode("utf-8")
            headers = [("Content-Type", "application/json; charset=utf-8"),
                ("Content-Length", str(len(body))), ("Retry-After", "2"),
                ("Cache-Control", "no-store"), ("Vary", "Origin")]
            origin = str(environ.get("HTTP_ORIGIN") or "")
            if origin in self.ALLOWED_ORIGINS:
                headers.append(("Access-Control-Allow-Origin", origin))
            start_response("503 Service Unavailable", headers)
            yield body
            return
        try:
            response = self.application(environ, start_response)
            try:
                yield from response
            finally:
                close = getattr(response, "close", None)
                if close is not None:
                    close()
        finally:
            self.lock.release()
