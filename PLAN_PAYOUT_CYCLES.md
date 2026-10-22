# Funded payout cycles

New plan purchases include two paid funded payout cycles by default. The owner
can offer 1, 2, 3, 4, 5 or unlimited cycles on the plan creation/edit card.
Unlimited is stored explicitly as `0`; SQL NULL on a historical purchase means
its existing entitlement is preserved. No historical plan or purchase is backfilled.

A cycle is consumed when an exact funded account's payout is marked PAID with a
paid timestamp. Pending/approved requests consume nothing. Multiple paid payout
rows for the same funded account consume one cycle. A paid cycle below the
purchased limit follows the existing fresh funded renewal process. At the limit,
the final source is archived, monitoring is retired and no new account is issued.
Another independently purchased account remains untouched.

## Prices and publication

- For a standard challenge plan, Fee is the base plan price. Business Settings
  stores separate fixed naira extra costs for 3, 4, 5 and unlimited cycles. The
  saved charge is added automatically to the base fee at purchase and checked
  server-side. 2 cycles is the default; 1 cycle remains available without an
  extra charge. Blank paid options cannot be sold until priced. Premium fixed
  plans retain their existing fee behavior. There is no later extension charge.
- For Build Your Package, set the charge for each cycle choice in Business
  settings. Two cycles can be included at zero additional charge. Active choices
  above two or unlimited must have a positive upfront charge. Existing configured
  prices are retained; new unpriced choices remain inactive.
- The purchased cycle count is snapshotted from the server plan, never the
  browser, and protected against later edits by a database trigger.
- Owners can edit current standard and premium plans. The plan-edit trigger freezes
  missing historical snapshots before applying new terms; existing purchases keep
  their original cycle limits. Copying a plan is optional.
- Package checkout retains its existing launch lock. This feature does not
  certify or enable the unfinished package engine integration.

## Installation

Apply `migrations/20261020_plan_payout_cycles.sql` before deploying this backend.
It adds nullable cycle columns, private paid-cycle RPC and assignment guards.
No real purchase, payout or MT5 assignment is created by installation.

Replace the existing cPanel files with `website-downloads/admin_clean.html` and
`website-downloads/trader_clean.html` to display the fields and customer card terms.
No standalone page or ZIP installation is needed.

## Verification

`python tools/test_plan_cycles_database.py` starts a disposable PostgreSQL 16
container with no network and tmpfs storage, tests the real migration/RPC and
removes the container. It uses no production credentials. It covers limits 1–5,
unlimited and legacy terms, duplicates/replays, read-only previews, final-source
retirement, purchase isolation, immutable snapshots and exhausted-budget assignment
rejection. The unit and browser tests verify actual checkout snapshots, renewal
hooks, upfront upgrade validation and the plan creation card.

## Reset choices and cycle price update (22 October 2026)

Standard plan creation/editing and the Business Settings product editor expose
Phase 1 Reset: Not available / Custom price, and Funded Reset: Not available /
Automatic 2× base plan fee / Custom price. Zero is Not available, not a free
reset; NULL funded_reset_fee selects automatic pricing. Custom prices are not
replaced when the base fee changes. The shared Phase reset price continues to
cover challenge stages on two-phase products. Already approved reset orders and
historical included Second Life entitlements keep their existing fulfillment.

Cycle prices use the existing versioned business_settings key/value storage,
under standard_plan_cycle_prices_v1. No guessed prices are installed. Configure
3, 4, 5 and unlimited separately; blank means that upgrade is unavailable.
These are additional upfront charges, not a charge each time a payout is made.
They apply to the cycle count selected on the plan card. Premium fixed-plan and
Build Your Package pricing remain separate. Recorded purchase fees and cycle
snapshots are not rewritten. Existing promo/affiliate rules apply to the full
quoted purchase price, including the cycle charge.

Deploy the backend changes together with plan_commercial.py before replacing
website-downloads/admin_clean.html and website-downloads/trader_clean.html in
cPanel. production-settings.js is synchronized for standalone consumers.
This change reuses existing reset price and business_settings columns. Existing
cycle/snapshot migrations listed above must already be installed.
