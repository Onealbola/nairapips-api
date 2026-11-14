"""Deliver the existing public login UIs when cPanel assets are unavailable.

Financial APIs retain their authentication and authorization; no session is embedded.
"""
from pathlib import Path
from flask import send_file, make_response

RELEASE='hosted-admin-mobile-navigation-2026-10-22'

def register_panel_delivery(app, root=None):
    assets=Path(root) if root else Path(__file__).resolve().parent/'website-downloads'
    @app.get('/public/nairapips-gold-architecture.png')
    def luxury_architecture_asset():
        return send_file(assets/'nairapips-gold-architecture.png',mimetype='image/png',max_age=86400)

    @app.get('/public/nairapips-luxury-concept.png')
    def luxury_concept_image():
        response=send_file(assets/'nairapips-luxury-concept.png',mimetype='image/png',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.get('/design-preview/luxury')
    def luxury_concept_view():
        response=make_response('<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow"><title>NairaPips — Luxury concept</title><style>body{margin:0;background:#080806;color:#f6e9bc;font:16px/1.5 system-ui}header{display:flex;gap:20px;justify-content:space-between;align-items:center;padding:16px 24px;border-bottom:1px solid #705929}a{color:#ffe29a}main{max-width:1536px;margin:auto}img{display:block;width:100%;height:auto}p{margin:16px 24px;color:#c9c1ad}@media(max-width:600px){header{align-items:start;flex-direction:column;gap:6px}}</style></head><body><header><strong>NAIRAPIPS · Luxury concept</strong><a href="/public/nairapips-luxury-concept.png" target="_blank" rel="noopener">Open full-size image ↗</a></header><main><img src="/public/nairapips-luxury-concept.png" alt="NairaPips luxury design concept: polished gold architecture, black panels and a premium account selector"><p>Visual concept only. Your live website and business settings have not changed.</p></main></body></html>')
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Robots-Tag']='noindex, nofollow'
        return response

    @app.get('/public/trader-catalogue.js')
    def trader_catalogue_script():
        r=make_response((assets/'account-plan-selector.js').read_text()+'\n'+(assets/'trader-catalog-sync.js').read_text()+'\n'+(assets/'account-reporting.js').read_text()+'\n'+(assets/'private-offer-scope.js').read_text()+'\n'+(assets/'reset-journey-scope.js').read_text()+'\n'+(assets/'payout-profit-target.js').read_text());r.mimetype='application/javascript'
        r.headers['Cache-Control']='no-store, max-age=0'
        r.headers['X-Content-Type-Options']='nosniff'
        return r

    @app.get('/downloads/trader-catalogue-page.html')
    def trader_catalogue_download():
        return send_file(assets/'trader_clean.html',mimetype='text/html',as_attachment=True,download_name='trader_clean.html',conditional=False,etag=False,max_age=0)

    @app.get('/public/landing-catalogue.js')
    def landing_catalogue_script():
        from landing_bundle import build_landing_bundle
        r=make_response(build_landing_bundle(assets));r.mimetype='application/javascript'
        r.headers['Cache-Control']='no-store, max-age=0'
        r.headers['X-Content-Type-Options']='nosniff'
        return r

    @app.get('/downloads/repair-plan-editing.sql')
    def plan_editing_database_repair():
        return send_file(Path(__file__).resolve().parent/'migrations'/'20261105_fix_text_plan_share.sql',mimetype='text/plain',as_attachment=True,download_name='NairaPips-Repair-Plan-Editing.sql',conditional=False,etag=False,max_age=0)

    @app.get('/downloads/rule-history-check.zip')
    def rule_history_check_download():
        response=send_file(assets/'nairapips-rule-history-check.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Rule-History-Check.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        response.headers['X-Content-Type-Options']='nosniff'
        return response

    @app.get('/downloads/operations-withdrawal-update.zip')
    def operations_withdrawal_download():
        response=send_file(assets/'nairapips-operations-withdrawal-update.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Operations-Withdrawal-Update.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/luxury-landing-update.zip')
    def luxury_landing_update_download():
        response=send_file(assets/'nairapips-luxury-landing-update.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Luxury-Landing-Update.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/landing-recovery.zip')
    def landing_recovery_download():
        response=send_file(assets/'nairapips-landing-recovery.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Landing-Recovery.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

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

    @app.get('/downloads/operations-recovery.py')
    def operations_recovery_download():
        response=send_file(assets/'nairapips-operations-recovery.py',mimetype='text/plain',as_attachment=True,
            download_name='nairapips-operations-recovery.py',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/dd-supervisor-repair.zip')
    def dd_supervisor_repair_download():
        response=send_file(assets/'nairapips-dd-supervisor-repair.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-DD-Supervisor-Repair.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/dd-history-session.ps1')
    def dd_history_session_download():
        response=send_file(assets/'nairapips-dd-history-session.ps1',mimetype='text/plain',as_attachment=True,download_name='NairaPips-DD-History-Session.ps1',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/dd-daily-audit.ps1')
    def dd_daily_audit_download():
        response=send_file(assets/'nairapips-dd-daily-audit.ps1',mimetype='text/plain',as_attachment=True,download_name='NairaPips-DD-Daily-Audit.ps1',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/dd-daily-history.ps1')
    def dd_daily_history_download():
        response=send_file(assets/'nairapips-dd-daily-history.ps1',mimetype='text/plain',as_attachment=True,download_name='NairaPips-DD-Daily-History.ps1',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/dd-login-retry.ps1')
    def dd_login_retry_download():
        response=send_file(assets/'nairapips-dd-login-retry.ps1',mimetype='text/plain',as_attachment=True,download_name='NairaPips-DD-Login-Retry.ps1',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/static-trailing-dd-update.zip')
    def static_trailing_dd_update():
        response=send_file(assets/'nairapips-static-trailing-dd-update.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Static-Trailing-DD-Update.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/business-email-update.zip')
    def business_email_update_archive():
        response=send_file(assets/'nairapips-business-email-update.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Business-Email-Update.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
        return response

    @app.get('/downloads/private-offers-update.zip')
    def private_offers_update_archive():
        response=send_file(assets/'nairapips-admin-small.zip',mimetype='application/zip',as_attachment=True,
            download_name='NairaPips-Private-Offers-Update.zip',conditional=False,etag=False,max_age=0)
        response.headers['Cache-Control']='no-store, max-age=0'
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
