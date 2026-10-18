"""Same-origin customer package builder. No example or owner pricing in this page."""
from pathlib import Path
from flask import make_response

def register_package_customer(app):
    def serve(name,kind):
        response=make_response(Path(__file__).with_name(name).read_text())
        response.headers.update({'Content-Type':kind,'Cache-Control':'no-store','Referrer-Policy':'no-referrer','X-Content-Type-Options':'nosniff',
         'Content-Security-Policy':"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"})
        return response
    @app.get('/build-package')
    def package_customer_page():return serve('package_customer.html','text/html; charset=utf-8')
    @app.get('/package-trader.js')
    def package_customer_script():return serve('package_trader.js','text/javascript; charset=utf-8')
