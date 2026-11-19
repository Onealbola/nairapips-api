# Mobile panel update — 17 November 2026

The trader and admin panels retain the black-and-gold design with layouts for phones, tablets and desktop. Traders have four primary bottom shortcuts and a searchable full menu, including profile, support and sign out. Admin retains its existing permission-filtered menu and quick actions. Cards, forms, reports and dialogs use consistent mobile spacing and touch targets. Wide data tables scroll inside an accessible region without widening the page. Both panels include safe-area padding for phone navigation bars.

## cPanel installation

Back up the existing panel files before replacing them. Extract `website-downloads/nairapips-mobile-update.zip` into the website's document root, preserving its folders:

- `admin/admin_clean.html` and `admin/admin-assets/…`: compact admin panel and matching scripts. Upload the complete assets folder with the HTML.
- `dashboard/trader_clean.html`: self-contained trader panel.

Keep the website's existing entry pages, domain and registration page. If your panel filenames differ, replace the contents of the existing panel files and put `admin-assets` beside the admin HTML. Do not upload this archive into the API application folder. Refresh the browser after upload; invalidate any cPanel/CDN HTML cache if enabled.

Standalone full admin HTML is also available in `website-downloads/admin_clean_cpanel.html` if inline scripts are preferred. `admin-restore.zip` contains that version at `admin/admin_clean.html`.

The backend-hosted aliases use the same updated source files after the API application is deployed. This source update and archive do not by themselves install files on cPanel or prove a production deployment.

## Rebuilding and verification

Run `python tools/package_mobile_panels.py` from the repository. Shared CSS and JavaScript live in `tools/mobile-panels.css` and `tools/mobile-panels.js`; the builder embeds them in both full admin variants and the trader HTML, then regenerates the compact admin and delivery archives. Repeated builds produce identical content and do not duplicate the mobile assets.

Run `node tests/test_mobile_panels_browser.js` with Playwright and Chromium installed. It uses intercepted offline requests and synthetic sessions, exercises actual panel rendering from 320px to 1440px, and checks trader navigation, menu filtering/focus, table overflow and dialogs. The existing admin mobile-navigation, Sales Outcomes, access-review and panel-delivery tests also apply. Browser checks do not authenticate to production or submit real financial requests.

## Touch-scroll correction — 19 November 2026

The legacy `overflow-x:hidden` on the body created an intermediate scrolling
container. Combined with mobile `overscroll-behavior-y:none`, Android finger
gestures could not reach the document scroller. Mobile overflow now uses clipping
without creating that intermediate container and allows vertical scroll chaining.
The menu retains its temporary body lock and accessible modal behavior.

`node tests/test_mobile_scroll_browser.js` uses real touch-start/move/end events
in mobile Chromium and selects modules through the actual drawer. The previous
release reproduced zero vertical movement; the corrected release scrolls.

## Funded card fitting — 19 November 2026

A previous-account reset notice could set the implicit dashboard grid's minimum
width through its long authority label. The funded view alone did not reproduce
this. Mobile dashboard tracks now use zero-minimum sizing, card children can
shrink, and long reset/funded labels wrap. The funded regression test includes
the actual reset-action panel and long release label at 320–768px, and verifies
the entire passport card fits in the viewport. Scroll behavior is rechecked.
