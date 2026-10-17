# Private owner package test

`/package-lab` serves an owner sign-in page from the existing API. It uses the
existing `/staff_login` endpoint; the bearer token is held in page memory only.
Every configuration and rehearsal request checks owner/super-admin authority.
HTML has no embedded credentials, no-store caching, no-referrer, and framing
blocked by CSP. The page needs no cPanel upload and no VPS process changes.

The rehearsal endpoint uses the same server-side `quote`, immutable contract,
DD/pass evaluator, and payout-share/cap calculation as the staged package code.
It models phase progression, separate fresh accounts, included reset counters
and paid-cycle limits entirely in request-local memory. It creates no purchase,
MT5 account, entitlement, payout, broker observation or email. Browser actions
are not evidence of real broker activity, passing or payment.

Example tariff edits are temporary and cannot be saved. Loading saved settings
permits actual future tariff edits via `/admin/package_configuration`; saving
requires an owner token and keeps an unreleased product disabled. The test page
refuses to edit an already released product. Saving is an explicit separate
action; rehearsing a tariff does not persist it.

This is a rule/pricing acceptance screen, NOT a production checkout test or a
claim that the Windows fleet enforces custom rules. Customer checkout stays
unreleased until real runtime integration and delivery/settlement checks pass.

## Verification

- `python -m unittest discover -s tests -p 'test_package_lab.py'`
- `node tests/test_package_lab_browser.js` with the installed Playwright runtime.
- The browser fixture binds a temporary loopback port; it imports no production
  app and uses an in-memory settings transport. It exercises actual Flask
  pricing/rehearsal endpoints and configuration persistence without live data.
- Browser checks include payout renewals, reset exhaustion, no financial writes,
  explicit inactive-tariff saves, mobile layout, password clearing and logout.

Do not start or import production `app.py` locally with production bindings:
that starts existing financial/lifecycle background workers. Test its sources
through the existing isolated AST and module tests instead.
