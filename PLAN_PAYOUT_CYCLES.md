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

- For a fixed challenge plan, its Fee is the full upfront purchase price for the
  selected cycle entitlement. Set that fee to include the upgrade when offering
  3–5 cycles or unlimited. There is no automatic later extension charge.
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
