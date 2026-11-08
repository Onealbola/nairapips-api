"""Bound signup-email work so an unavailable provider cannot block the shop."""
import queue
import threading
from functools import wraps
from flask import copy_current_request_context, jsonify


def bounded(view, timeout=12):
    gate = threading.BoundedSemaphore(1)

    @wraps(view)
    def wrapper(*args, **kwargs):
        if not gate.acquire(blocking=False):
            response = jsonify(success=False, error='Verification service is busy. Please wait before trying again.')
            response.headers['Retry-After'] = '15'
            return response, 503
        result = queue.Queue(maxsize=1)

        @copy_current_request_context
        def work():
            try:
                result.put((True, view(*args, **kwargs)))
            except Exception:
                result.put((False, None))
            finally:
                gate.release()

        try:
            threading.Thread(target=work, name='np-signup-email', daemon=True).start()
        except Exception:
            gate.release()
            raise
        try:
            success, response = result.get(timeout=timeout)
        except queue.Empty:
            response = jsonify(success=False, error='Verification is taking longer than expected. Please wait before requesting another code.')
            response.headers['Retry-After'] = '15'
            return response, 503
        if not success:
            return jsonify(success=False, error='Verification service unavailable. Please retry shortly.'), 503
        return response

    return wrapper


def install(app):
    if not getattr(app, '_np_bounded_signup_email', False):
        app.view_functions['send_email_otp'] = bounded(app.view_functions['send_email_otp'])
        app._np_bounded_signup_email = True
