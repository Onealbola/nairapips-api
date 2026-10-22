# Production business overview

The existing admin Overview now uses the authenticated read-only
`GET /admin/business_overview?period=today|7d|30d|all`. The full and compact
admin panels embed the same `business-overview.js` module. The backend is
`business_overview.py`; it never imports or executes the production app in tests.
No schema installation, assignment, payment decision or record rewrite is needed.

## Reporting rules

- Current work has separate queues for submitted plan proofs, submitted reset
  proofs, approved first purchases without assignment evidence, payout requests,
  approved payouts without a paid timestamp and unanswered open support tickets.
  A legacy assignment reference is a link to reconcile, not a new account owed.
- Customer profiles, customers with an approved payment, current broker accounts
  and historical rows use different units. Counts are not added together.
- A current account must have an open status, a broker login and server, an owner,
  and no recorded archive, pass or breach. Repeated open rows for one owner and
  purchase are counted once. Conflicting open ownership is held for record review.
  Protected/awaiting-payout accounts are separated from trading accounts.
- Fresh monitoring requires an exact account/customer/MT5 observation within
  180 seconds. Missing observations never produce invented live balances.
- Money uses the saved final purchase fee, or fee/amount due only when the
  preceding field is absent. Explicit zero is retained. Payouts use payout amount,
  not gross broker profit. Paid timestamps and rejection/cancellation evidence
  take precedence over stale status labels. Contradictory financial evidence is
  held for reconciliation. Invalid amounts are excluded and disclosed.
- Recorded approval totals use the approval timestamp, or submission timestamp
  when no approval timestamp exists. Recorded payout totals require paid_at.
  Today/7-day/30-day windows follow the existing Africa/Lagos business dates.
  Work queues always include all dates. These are recorded transactions, not
  bank-reconciled cash, bank balance, net profit or forecasts.
- Test transactions do not become production work or financial totals. Revenue
  exclusions affect the financial report; they do not cancel a real outstanding
  assignment or payment obligation. Responded/closed support rows are not counted
  as awaiting an answer even when another status field is stale.
- Record checks identify distinct rows to investigate and may overlap a work
  queue. They are not automatically critical incidents, debts or new assignments.

## Freshness, access and completeness

The adapter uses narrow field selections, complete keyset pagination and a bounded
four-thread read pool across the existing tables. Broker observations are matched
by exact identities. Passwords, bank account details and proof URLs are not returned
by the overview. Each HTTP response contains counted-record membership and safe
supporting rows so every displayed figure can open its exact snapshot records.

These are complete reads across a short read window, not a single database
transaction. The UI displays the start/end of that window; activity arriving
during it may appear on the next refresh. There is a five-second per-worker source
cache, bypassed by explicit Refresh. The visible Overview polls every 30 seconds
and refreshes when the browser tab becomes visible. Failed or incomplete reads
return 503, never partial totals. The client retains the last good data with a
stale warning and clears it when the authenticated session changes. Responses
are no-store and gzip-compressed when requested. Read-only release identification
is available at `/business_overview_version`; business records remain admin-only.

## Validation and deployment

Unit fixtures cover identity conflicts, repeated account history, terminal state
precedence, exact telemetry, missing proof/amounts, exclusions, legacy assignment,
period boundaries, complete paging and authorization failures. Browser fixtures
cover both the standalone module and compact admin panel, exact record drilldowns,
escaping, reporting periods, stale refreshes and session isolation. Full-panel
funded account behavior and the existing plan controls are also checked.

Deploy `app.py` with `business_overview.py`, then replace cPanel's
`admin_clean.html` with `website-downloads/admin_clean_cpanel.html`.
The trader panel is unchanged. The source admin HTML remains readable; the compact
upload preserves public JS names without changing program logic. No private
production audit files or real customer data belong in this repository.
