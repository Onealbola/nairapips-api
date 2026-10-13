"""Preserve synchronous-worker mutation concurrency when enabling HTTP threads."""
import threading


class SerialMutationMiddleware:
    """Each worker executes at most one business mutation at a time.

    Authentication still runs through the original Flask route and security
    hooks. Only admin credential verification is exempt from this lock; it does
    not issue accounts, approve purchases or alter payouts. Cross-process money
    and assignment guards remain authoritative in the database.
    """
    MUTATING_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

    def __init__(self, application):
        self.application = application
        self.lock = threading.RLock()

    def __call__(self, environ, start_response):
        method = str(environ.get("REQUEST_METHOD") or "").upper()
        path = str(environ.get("PATH_INFO") or "")
        if method not in self.MUTATING_METHODS or path == "/staff_login":
            return self.application(environ, start_response)
        return self._serialized(environ, start_response)

    def _serialized(self, environ, start_response):
        with self.lock:
            response = self.application(environ, start_response)
            try:
                yield from response
            finally:
                close = getattr(response, "close", None)
                if close is not None:
                    close()
