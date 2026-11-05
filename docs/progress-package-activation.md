# Progress package activation — 5 November 2026

The four-package offers use managed challenge_plans rows and the established payment-proof, approval, fresh-MT5, monitoring and payout runtime. All twelve initial offers are prepared INACTIVE. The release does not enable any new sales or modify customer purchases/accounts/payments.

Owner flow: open `/package-studio?catalog=1`, tick or untick **Open for new purchases**, then **Save purchase switches**. Complete prices and benefits are edited at `/package-studio`. Standard/Premium controls retain their previous behavior. Global challenge-sales pause overrides every product switch.

A Progress activation stages and reads back the exact production plan before saving its private publication switch. Direct plan-ID submissions are resolved back to that private switch. Current matrix terms must match the physical plan. Failures and conflicting edits close checkout rather than using stale terms. The settings document uses revision CAS. Offer-version checks also reject checkout after same-price benefit changes.

Each new purchase snapshots its exact terms and server-owned Progress marker. Only that marker and deterministic plan UUID enable Phase 1 → Phase 2 → Funded routing. Existing one-phase purchases retain their route. The existing exact broker pass writer, claim and replay guards serve both phases. A final assignment guard rejects skipped/repeated evaluation phases, cross-owner assignments and unverified passes. Payout share and drawdown come from frozen purchase/account snapshots. Continuing payouts use the already-supported explicit `payout_cycles=0`, with no additional cycle surcharge. New Progress products include no resets or second lives.

Currently supported activation terms: static drawdown within configured limits, no separate daily rule, 15% gross funded profit cap, either 10%/8% or 8%/5% targets, configurable complete fee and trader share. Future daily rules and different profit caps remain editable drafts with specific activation warnings until their enforcement is implemented. The separate old custom-contract builder retains its own release checks.

The hosted `/control-panel` and `/dashboard/trader_clean.html` update on deploy. The independently hosted cPanel site needs the updated `/downloads/catalogue-sync-update.zip` once: extract into `public_html`, preserving its `admin/` and `dashboard/` directories. Later price and purchase-switch changes require no new upload.

Validation includes owner on/off behavior, direct-ID bypass attempts, stale fees/benefits, numerical database representations, failed writes and identity collisions, immutable existing purchases, two-phase broker proof/routing, skipped-phase/breach guards, unchanged old one-phase routes, payout caps, and browser tests of owner controls and the real trader proof-purchase modal. Test fixtures never submit real customer payments or broker assignments.
