# Production control centre — staged implementation

This change prepares owner controls for the existing products and future package product. It does **not** release the private package simulator to customers or claim that custom cycle/reset rules are enforced by the running VPS fleet.

## Existing products stay available to the business

An owner can switch each product between active and inactive using `/admin/production_catalog`. Inactive means closed for new purchases; it does not delete the plan, purchases, account history, broker monitoring or payment history. The same product can be reopened later.

Sold products cannot have their capital, targets, DD, share or commercial prices silently changed. The new-version action copies the product to a new UUID, initially inactive. Configure and release that version for new purchases. Previous customers retain their original plan identity and snapshots. Older plan-edit/delete routes also enforce this boundary. Presentation edits and opening/closing sales remain possible.

Checkout verifies the actual current plan status and price directly, and uses that verified plan for the purchase snapshot rather than a stale cache or browser-supplied terms. Current funded-reset pricing is still explicitly twice the challenge fee. Equity trailing and unsupported payout shares cannot be sneaked into the older plan editor.

## Saved service switches

`/admin/production_controls` requires a signed owner/super-admin token. It controls accepting new registrations, challenge purchases, paid reset purchases, payout requests, referral withdrawals, private offer claims and support tickets. History and already accepted processing remain accessible. The switches do not stop monitoring, breach enforcement, settlement or accepted assignment/renewal work.

Existing services default to their current enabled behavior. A settings outage blocks only a gated new operation, with `operation_started: false`; it cannot disable the DD engine or health endpoints. Separate workers refresh the shared database document within three seconds. Writes compare the previous unique write token and revision, read back the saved state, and audit the actor; stale sessions receive 409 rather than overwriting another owner's edits.

## Real settings for the future package product

`/admin/package_configuration` persists the future product separately from old products. Each option has an editable value, a checkbox and an editable base price or upgrade percentage; owners can add numeric choices within validated ranges: capital, one/two phases, stage targets, DD allowance/type, profit share, gross profit cap, paid cycles and separate phase/funded reset budgets. No test tariff is silently approved: prices begin empty and options begin off.

`/admin/package_configuration/quote` computes from the saved version on the server, rejects disabled choices and browser price fields, clears Phase 2 for a one-phase selection, and calculates an exact Decimal total. It is an authenticated owner price check, not a customer order or a broker simulation. Minimum price and all upgrade percentages are editable and durable.

Storage uses two distinct versioned JSON documents in the existing `business_settings` key/value table. No new database schema is necessary for these configuration controls. No real account, purchase or payout rows are written by configuration saves. The live database transport was checked read-only; an isolated non-configuration permission probe was cleaned up. Anonymous insert was denied and anonymous update did not modify the probe row.

## Release boundary — remaining work

The master package checkout switch is locked server-side. The server rejects attempts to enable it until a subsequent implementation supplies:

1. Immutable server-owned quote/order/contract storage and atomic checkout.
2. Purchase-scoped cycle/reset entitlement and settlement ledgers, including replay-safe assignment and renewal.
3. Contract bindings and rule readers in assignment, payout/reset processing, monitoring, live state and the running DD/VPS collectors.
4. Trader package checkout and purchased-contract display with production end-to-end verification.

The deployment task must connect and verify these consumers before unlocking checkout. Merely copying the simulator, changing a label, or saving a price is not a production launch. This staged change does not modify the landing page, monitoring engine or VPS binaries, and does not migrate legacy accounts.

## Review and delivery

Backend changes: `production_controls.py`, `catalog_controls.py`, `package_configuration.py`, and the small registration/verified-checkout additions in `app.py`.

Dashboard delivery files live on the dashboard branch: `website-downloads/admin_clean.html`, `website-downloads/trader_clean.html`, `website-downloads/production-settings.js`. The admin file contains the new script inline, so only the two HTML files need publishing. Trader plan/purchase cards hide inactive products; history is retained. The old blanket “all modules operational” claim has been replaced with the actual settings and release state.

Keep backend and dashboard changes staged together for review. A backend Git branch push does not publish the cPanel HTML files. No customer package checkout should be advertised yet.

Validation uses the backend unittest suite and the dashboard's `tests/test_production_settings_browser.js`. Tests cover authorization, actual changed-field payloads, persistence/restart, revision conflicts, direct inactive-product purchases, stale browser prices, sold-rule protection, new inactive versions, outage behavior, unsupported rule rejection, server prices, failure recovery, HTML escaping and mobile layout. Browser API fixtures do not constitute production end-to-end verification.
