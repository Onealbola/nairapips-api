"""Deliver the existing public login UIs when cPanel assets are unavailable.

Financial APIs retain their authentication and authorization; no session is embedded.
"""
from pathlib import Path
from flask import send_file

RELEASE='hosted-admin-mobile-navigation-2026-10-22'

def register_panel_delivery(app, root=None):
    assets=Path(root) if root else Path(__file__).resolve().parent/'website-downloads'
    def deliver(name):
        response=send_file(assets/name,mimetype='text/html',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['X-NairaPips-Panel-Release']=RELEASE
        return response
    @app.get('/control-panel')
    @app.get('/admin/admin_clean.html')
    def hosted_admin_login():
        return deliver('admin_clean_cpanel.html')
    @app.get('/dashboard/')
    @app.get('/dashboard/trader_clean.html')
    def hosted_trader_login():
        return deliver('trader_clean.html')

    @app.get('/dashboard/referral.html')
    def hosted_referral_portal():
        return deliver('referral.html')
