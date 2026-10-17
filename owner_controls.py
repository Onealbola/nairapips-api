"""Owner control page; runtime switches use the registered authoritative stores."""
from pathlib import Path
from flask import make_response

def register_owner_controls(app):
    @app.get('/business-controls')
    def business_controls_page():
        response=make_response(Path(__file__).with_name('owner_controls.html').read_text())
        response.headers.update({'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store',
            'X-Content-Type-Options':'nosniff','Referrer-Policy':'no-referrer',
            'Content-Security-Policy':"default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"})
        return response
