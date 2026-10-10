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

## V202: receive requests during update delays

A missing or stale financial update no longer rejects an otherwise eligible
funded trader's submission. The existing duplicate/rejected-cycle checks,
payment details and pre-submission trading lock remain. The request is stored
as pending, with [NP_PAYOUT_VERIFY_PENDING] in its existing admin_note field.
Unverified requests have zero verified financial amounts; the requested amount
is a request for review, not a confirmed withdrawable entitlement.

The marker survives the existing legacy-schema insert fallback. Approval of a
marked request requires a fresh exact-account quote and sufficient verified
profit. The financial evidence and marker removal are written in the same
conditional pending-to-approved update. The effective V122 mark-paid handler
blocks requests still carrying the verification marker. Existing cancellation
releases the payout lock; no new payout status or schema migration is required.
There is no automatic approval or payment. Admin's approval action verifies the
existing request after updates resume; the trader need not submit again.

Dashboard quotes can retain a stale last-verified amount for presentation only.
They remain verified=false, with an observation timestamp. Submission does not
trust the browser quote. The trader page keeps requests open during delays,
labels the last known amount and its date, and displays a received/verifying
label in history. The admin page identifies requests awaiting verification and
labels the approval button Verify & Approve. These website changes are in both
/workspace/payout-dashboard-fix/trader_clean.html and admin_clean.html and still
require publication to the website host separately from Render.

Validation: 24 backend tests, including delayed acceptance, duplicate prevention,
legacy fallback, blocked premature approval/payment, resumed verification,
insufficient funds remaining pending, and cancellation. JavaScript syntax
checks pass for both pages. tests/test_payout_dashboard.js verifies delayed form
submission, stale amount display, fresh limits, destination validation and
pending labels. Read-only production schema inspection confirmed all payout
columns needed by the verification update. No production payout was created or
changed during testing. Full infrastructure outages can still prevent storage;
the UI must never report receipt if no request was saved.

## V203: collector ownership metadata compatibility — 10 October 2026

A read-only audit found all 66 persisted live-state rows had trader_id equal to
trader_account_id, rather than the canonical trader_accounts.trader_id. The
legacy deployed V3.20 collector caused this. V201's new strict owner filter
therefore rejected otherwise exact-account broker observations and the page
rendered unknown amounts/share as zero. This was a regression in the new payout
verification path, not evidence that traders lost profit.

Quotes now select only by immutable account UUID and still verify canonical
account ownership against the authenticated trader. An observation's owner may
match either that canonical trader UUID or the exact same account UUID (the
known legacy metadata error). Observation account UUID and MT5 login must also
match. Another trader UUID, another account UUID or a reused login alone cannot
authorize a payout. No balance or account record is rewritten by this fix.
Missing quote amounts, profit and share are null, not zero. The revised trader
HTML displays Verifying instead of a fabricated zero when evidence is absent.

Read-only verification of MT5 477509676 returned fresh equity/balance 570981.44,
starting capital 500000, profit 70981.44, share 60%, available payout 42588.86.
27 backend tests passed, including legacy owner aliases with exact ownership,
rejection of other accounts/logins and unknown financial values. The live-state
service is separately corrected to resolve the canonical owner before its RPC,
with a bounded 30-second lookup cache to avoid a database read per observation.
