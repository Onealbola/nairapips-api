# Global exact-account payout financial authority

The dashboard read np_live_account_state, while create_payout calculated its
amount limit from older trader_accounts values. This could show customers current
profit while rejecting their requests against assignment-time or stale equity.

Both paths now use _np_verified_payout_quote. It checks exact account, owner and
MT5 identity, requires broker observed_at within 180 seconds (maximum future skew
60 seconds), and applies the existing 50% gross-profit ceiling and exact plan
share. Missing/unverified updates return temporary unavailability, rather than
being classified as verified zero profit. Browser financial values are ignored.
Existing eligibility, one-open-payout checks, rejected-cycle rules, pre-lock and
post-paid renewal logic remain in place. Existing management-authorised exact
account recovery is retained. No payouts are created, approved or paid by these
changes or their verification.

The preceding local dashboard corrections are included: exact account reads
use the base live-state table, do not borrow history by reused login, preserve
historical rows and reuse observations within a request.

Validation: 16 local backend tests cover dashboard identity protection, live
profit, stale/missing/future evidence, split and ceiling, invalid monetary values,
request validation and existing lock entry. Imports are isolated from production
background workers. Read-only production live-state queries confirmed expected
columns and fresh observations. Updated trader HTML passed JavaScript syntax and
functional quote/unavailable/stale-state checks.

Website file: /workspace/payout-dashboard-fix/trader_clean.html, based on the
owner's current uploaded trader HTML. It uses exact backend quotes, maintains
them on refresh, distinguishes unavailable data, and removes the fixed 60% copy.
This file requires deployment to the website host separately from Render.
The uploaded admin HTML does not perform this customer payout calculation; its
payout records already use backend verified_profit and available_payout fields.
