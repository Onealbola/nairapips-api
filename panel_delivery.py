"""Deliver the existing public login UIs when cPanel assets are unavailable.

Financial APIs retain their authentication and authorization; no session is embedded.
"""
from pathlib import Path
from flask import send_file, make_response

RELEASE='hosted-admin-mobile-navigation-2026-10-22'

def register_panel_delivery(app, root=None):
    assets=Path(root) if root else Path(__file__).resolve().parent/'website-downloads'
    @app.get('/public/trader-catalogue.js')
    def trader_catalogue_script():
        r=make_response((assets/'account-plan-selector.js').read_text()+'\n'+(assets/'trader-catalog-sync.js').read_text());r.mimetype='application/javascript'
        r.headers['Cache-Control']='no-store, max-age=0'
        r.headers['X-Content-Type-Options']='nosniff'
        return r

    @app.get('/downloads/trader-catalogue-page.html')
    def trader_catalogue_download():
        return send_file(assets/'trader_clean.html',mimetype='text/html',as_attachment=True,download_name='trader_clean.html',conditional=False,etag=False,max_age=0)

    @app.get('/public/landing-catalogue.js')
    def landing_catalogue_script():
        r=make_response((assets/'account-plan-selector.js').read_text()+'\n'+(assets/'landing-catalogue-sync.js').read_text());r.mimetype='application/javascript'
        r.headers['Cache-Control']='no-store, max-age=0'
        r.headers['X-Content-Type-Options']='nosniff'
        return r

    @app.get('/downloads/repair-plan-editing.sql')
    def plan_editing_database_repair():
        return send_file(Path(__file__).resolve().parent/'migrations'/'20261105_fix_text_plan_share.sql',mimetype='text/plain',as_attachment=True,download_name='NairaPips-Repair-Plan-Editing.sql',conditional=False,etag=False,max_age=0)

    @app.get('/downloads/complete-catalogue-update.zip')
    def complete_catalogue_update():
        return send_file(assets/'nairapips-complete-catalogue-update.zip',mimetype='application/zip',as_attachment=True,download_name='NairaPips-Complete-Catalogue-Update.zip',conditional=False,etag=False,max_age=0)

    @app.get('/downloads/catalogue-sync-update.zip')
    def catalogue_sync_update_archive():
        response=send_file(assets/'nairapips-catalogue-sync.zip',mimetype='application/zip',
            as_attachment=True,download_name='NairaPips-Trader-Catalogue-Update.zip',
            conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.get('/downloads/sales-outcomes-update.zip')
    def sales_outcomes_update_archive():
        response=send_file(assets/'nairapips-admin-small.zip',mimetype='application/zip',
            as_attachment=True,download_name='NairaPips-Sales-Outcomes-Update.zip',
            conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

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
    @app.get('/dashboard/index.html')
    def hosted_trader_entry():
        return deliver('trader_entry_redirect.html')
    @app.get('/dashboard/register.html')
    def hosted_trader_registration():
        return deliver('trader_login.html')
    @app.get('/dashboard/trader_clean.html')
    def hosted_trader_login():
        return deliver('trader_clean.html')

    @app.get('/dashboard/referral.html')
    def hosted_referral_portal():
        return deliver('referral.html')
