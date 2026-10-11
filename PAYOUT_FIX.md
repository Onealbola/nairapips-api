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

Dashboard refresh consistency and read performance (10 October)
- Fast current-account refresh and account-list responses now overlay the same
  exact live-account observation as bootstrap, so polling cannot replace fresh
  balances with stored assignment values. Archived accounts are preserved.
- Balance and payout reads use the existing REST-first database helper, avoiding
  the shared Supabase connection already documented as unreliable in production.
- Live account enrichment skips serial old snapshot/event history scans when
  live-state evidence exists. No lifecycle or database records are changed.
- Read-only live verification again returned balance/equity 570981.44 and
  available share 42588.86. Backend checks cover refresh consistency, duplicate
  read suppression, history preservation, payout verification and queue safety.
- Public Render HTTP checks are blocked by this environment's network proxy;
  deployment completion and end-to-end browser speed remain unconfirmed.

Global Account Truth refresh consistency
The authenticated Account Truth route now combines the exact account's newest
live observation with its existing historic evidence. Its delayed history response
therefore cannot replace a newly observed high with an old assignment balance.
Owner/account/login validation also accepts only the known exact-account legacy
owner alias. Existing low-water marks, limit crossings and their evidence remain
intact. Tests cover the refreshed high and timestamp, preserved past breaches,
and rejection of other accounts/owners/logins. All 32 backend tests passed.

Global duplicate payout protection
- Every create/approve/mark-paid/cancel/reject/reopen-review route now requires
  authenticated trader ownership or a signed administrator token.
- Financial actions acquire a compare-and-set guard in the database, shared by
  all API processes and duplicate records for the same MT5 login/broker server.
  The oldest account row is the common serialization anchor. Guard failure or
  unverifiable history fails closed; it never authorizes another payout.
- All sibling IDs for that broker account are checked. Paid status OR paid_at
  evidence makes further financial actions terminal, even when recovery overrides
  or duplicated internal accounts would otherwise call the account eligible.
- Existing open requests on any sibling block new requests, approval and payment.
- Mark Paid changes only approved, unpaid rows atomically and emits completion
  side effects only after a successful transition.
- Rich/fallback insert attempts share one UUID. A lost response after commit
  recovers the existing request and preserves its lock rather than duplicating it.
- Same-account paid cycle reactivation remains disabled. Fresh renewal must use
  a different broker account; a fresh UUID alone cannot bypass paid history.
- Crash-stuck action guards require explicit investigation, not automatic expiry.
  Guard cleanup uses compare-and-set and preserves appended renewal/audit notes.

Read-only audit of 345 payout records found 170 marked paid. No duplicate paid
immutable IDs existed, but grouping by broker server + MT5 login revealed three
broker logins with multiple paid records: 477287946, 477214688, 477213799. Five
paid broker accounts also had open requests. The private reports remain outside
Git. Record statuses establish risk, not proof of actual bank transfers/fraud.
For 477287946, 46 internal records share Exness-MT5Trial9 and that broker login.
Two payments are marked paid: 90400 on September 28 and 89900 on October 8;
two pending requests remain 89999 and 89900. Production data was not altered.

42 backend tests passed, including concurrent claims/submission, repeated payment,
clone-ID bypass prevention, broker separation, ownership/authentication, preservation
of payment evidence, and recovery after lost insertion responses. Frontend payout
validation checks passed. Render completion cannot be checked through this proxy.

Staff sign-in availability (11 October)
The staff login lookup now uses a direct database request with a two-second
connection timeout and four-second read timeout, rather than the shared staff
client. Submitted passwords are compared on the server and are not placed in
lookup URLs. Existing local bootstrap authentication, active-account checks,
signed tokens and deferred audit writes remain intact. Dependency failures return
a generic 503 without database details. No credentials or permissions changed.
Read-only production staff lookup returned HTTP 200 in 1.07 seconds. Six targeted
login checks plus the existing payout/dashboard checks passed: 48 total tests.
This addresses the identified lookup delay; public Render access remains blocked
by the environment proxy, so server queueing and end-to-end login remain unverified.

Admin Action Centre refresh (v208): replace shared-client bulk reads with four
independent bounded REST reads, paginate complete history, and reuse account
rows across all management-recall reconciliation passes within each request.
Failures raise rather than report empty successful totals. No financial writes
or payout approval rules changed. Live read-only validation: 487 traders, 1,903
accounts, 467 purchases and 348 payouts in 3.5 seconds. 52 tests passed.

Admin View session (v209): exact trader identity resolution uses bounded REST
instead of the shared SDK. Missing exact traders return 404; failed or mismatched
identity reads return 503 without minting a token or entering legacy lookup.
Admin authorization remains required. Read-only live lookup verified in 0.69s.
57 backend tests passed; browser-script tests cover immediate and delayed
same-origin handoff rendering, acknowledgement and handoff removal.

Owner-reconciled paid replacement (v210): breached-source renewal remains blocked
unless both the exact PAID payout and funded source carry a privileged exact
payout exception marker. Require paid_at, matching trader/account/login and
funded stage. Reset, recall and management-hold checks remain enforced, as do
existing-delivery checks and database source claims. Startup repair also queues
these explicitly owed replacements even when the trader mirror points elsewhere.
62 tests passed. Production reconciliation is private and outside the repository;
no money was transferred. Original breach facts are retained.
