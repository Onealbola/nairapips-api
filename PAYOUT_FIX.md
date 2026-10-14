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

Payout-renewal transport (v211): renewal worker uses bounded independent REST for
its existing query chains, assignment writes and dashboard pointer persistence.
ContextVar limits this transport change to that worker; other routes retain their
existing database path. CAS filters remain intact. Inserts generate one UUID and
recover only that exact row after a lost response, without repeating the POST.
Preserve required purchase_id when older schemas lack optional predecessor fields.
66 tests passed, including lost-response and cross-thread isolation checks.

V212: Startup outstanding-paid discovery now enters the same isolated REST
context as execution. Exact trader and management-hold reads honor that context;
ordinary requests retain their existing transport. Recovery selection and
management-hold rules remain unchanged. 67 tests passed, including startup
context restoration.

V213 partial-delivery recovery: startup discovery queues an existing exact child
without a durable completion receipt, rather than silently skipping it. Finalize
must successfully register monitoring and persist the current dashboard pointer
before closing the source cycle. Existing-child reuse prevents a second MT5.
69 tests passed, including blocked finalization on monitoring failure and startup
partial-vs-completed delivery behavior. Etim's previously inserted replacement
was completed directly with exact pool/payout identity, source CAS, active registry
verification and persisted trader pointer; private before/after audit outside Git.

V214 global live-pass reconciliation: the existing lifecycle worker now reconciles
exact current evaluation telemetry before running progression. The dashboard's
np_live_account_state feed previously had no durable evaluation-pass consumer;
retry scanned only already-archived passes, so active rows with target-reaching
balances never entered the assignment queue. Both Phase 1 and Phase 2 are handled.

Proof requires exact account/login/owner/stage/capital, immutable purchase target,
finite metrics, recent observation after assignment, and absence of breach/reset
or management hold. CAS includes original status/reason/update timestamp and null
breach time. Historical equity highs and pass metrics are persisted; the source
monitoring registry is retired. Existing exact progression then assigns from the
correct pool under the current lifecycle lock using isolated bounded transport.

Progression uses a database source claim and checks successors against source
creation time, preventing late-recorded passes from issuing another funded MT5
when a prior successor later breached. Active partial successors are completed;
breached successors are never reactivated. Pass recovery runs before older
purchase/reset scans and diagnostic counts are exposed in existing admin status.

Read-only production dry-run: 36 evaluation snapshots, 3 verified unrecorded
passes including Sanni's 477297550. Full 92-test regression suite passed, followed
by updated 24 pass-specific checks (including one added capital-mismatch case).
Existing dashboard handoff and payout-display JavaScript checks also passed.

## V215 — bound recurring Admin startup and refresh waits

The login UI waits before receiving `/staff_login`; dashboard loading starts only
following successful authentication. The overview still contained eight serial
REST reads with retry/backoff and an SDK fallback, plus an unbounded SDK count.
A read-only production check found monitoring-events history timing out while
core tables completed in approximately 0.7–1.6 seconds.

Overview reads now use one fixed four-thread pool and a ten-second response
deadline. Core read failures use recent (at most five minutes) saved data marked
degraded, or return 503 without false empty business data. Optional announcement
and monitoring-event failures are marked separately and do not discard successful
core reads. Failed bootstrap requests no longer repeat a complete refresh or fall
back to the shared SDK connection pool. Exact customer counts use bounded REST
with `Prefer: count=exact`; missing count metadata fails rather than guessing.
The full Traders feed also uses bounded, stable pagination instead of SDK waits.

No financial mutations, account lifecycle rules, authentication checks, pricing,
or worker concurrency settings change. Local tests cover authentication failure,
real zero count, partial read errors, optional history timeout, recent cache age,
concurrent reads, response deadline cancellation and full trader pagination.
Read-only live overview validation returned the exact customer count (495) and
core data in 5.61 seconds with the monitoring-history timeout clearly isolated.
Render service connectivity is blocked by the execution environment's proxy, so
this check does not establish deployed login latency or deployment completion.
The bounded full Traders feed also read all 495 customers and 1,921 account records
in 5.07 seconds; pagination preserves records beyond PostgREST's default page cap.
Validation: 103 Python tests passed, plus the existing Admin View handoff and
trader payout UI checks. No full production app import or financial write was used.

## V216 — make purchase approval use isolated assignment transport

The Purchase Approve + Auto Assign request repeated server-side history scans,
used shared SDK calls for purchase/trader/pool/affiliate reads and writes, and
queried/possibly repaired journeys again after the protected assignment had
already completed. Credential sends remain synchronous; their recipient lookup
and email logging now honor the isolated transport, avoiding another shared SDK
wait after provider acceptance. No email is sent by tests or read-only probes.

Ordinary approval now enters the existing bounded REST transport used by renewal
and phase-pass recovery. The reset-payment bridge retains its prior transport.
Server eligibility may issue a request-local picker proof; the final gate still
re-reads the ownership ledger and selected pool immediately before insertion.
Interactive safety reads use one bounded attempt and cannot fall back to a stuck
SDK client or interpret non-list responses as an unused account. A failed exact
purchase-history read stops assignment rather than continuing without the guard.

A conditional purchase-note claim prevents simultaneous approvals of the same
purchase. Assigned/rejected purchases and outstanding claims are blocked. A
precommit failure releases only its own claim; partial/uncertain assignments keep
it for reconciliation rather than creating another account on retry. The core
still freezes rules, binds the exact journey, persists the current account,
activates/verifies monitoring and sends credentials. The redundant post-success
journey sweep was removed; it never owned the actual assignment validation.

A read-only never-used inventory verification completed in 2.62 seconds. This is
not an end-to-end approval benchmark: no production purchase was approved or
account/email issued for testing. Tests exercise concurrent requests, interrupted
writes, authentication/rejection guards, context restoration, reset routing,
expired proof, newly consumed inventory and invalid safety responses.
Out-of-stock approval keeps its exact waiting entitlement and removes its
in-flight claim. The REST adapter forwards the existing rejection `neq` filters.
Production read-only schema checks confirmed UUID identifiers for email,
lifecycle and affiliate ledgers, but integer identity IDs for admin_audit_logs;
audit inserts preserve that integer identity and do not repeat lost-response
inserts. Final validation: 124 Python tests passed, plus the existing Admin View
handoff and payout dashboard JavaScript checks. Render deployment and live
end-to-end approval latency remain unconfirmed from this environment.

## V217 — serve admin authentication while another HTTP request is busy

The continued screenshot is an authentication-response timeout, preceding all
Admin dashboard loading. Gunicorn's default synchronous worker serves only one
request at a time. A real isolated Gunicorn test reproduced login waiting behind
an intentionally slow handler, even though credential verification itself is
local and fast. The actual Render start command/runtime configuration has not
been read; this reproduction establishes the failure mode, not a confirmed
inspection of Render's running workers.

The automatically discovered gunicorn.conf.py selects gthread with eight request
threads, preserves Gunicorn/command-line worker count, and does not preload this
app (its import starts background workers). Startup logs identify the effective
worker class/thread count; access logs include duration and omit query strings.
SerialMutationMiddleware preserves one business-mutating HTTP handler at a time
inside each worker. Admin credential verification remains on the original signed
Flask authentication route and bypasses only that scheduling lock. Existing DB
claims/CAS guards remain authoritative across workers. It does not bypass auth,
change passwords, resurrect historical accounts or change financial eligibility.

Login background last-login/audit writes also enter bounded REST transport so a
successful sign-in does not accumulate SDK-blocked background threads. The
transport context is always restored, including failure cases.

Real Gunicorn tests verify automatic config loading, successful signed login
while a slow read runs, rejection of incorrect passwords, concurrent business
mutations staying serial, login responding while mutations are queued, and lock
release on handler failure. These tests use only private fixture credentials and
an AST-extracted auth function; they do not import the production app, contact
customers, send email or issue real accounts.

Live verification remains blocked by this cloud environment's restricted egress.
A configuration draft preserves *.supabase.co and adds only the API hostname,
nairapips.com and www.nairapips.com. The tool confirmed saving and requires user
review/save/publish before those runtime network changes take effect. This draft
is unrelated to the Render deployment triggered by the application Git push.
Final V217 validation: 130 Python tests passed, including real Gunicorn HTTP
concurrency tests and bounded staff-audit failure/context-restoration cases.

## V218 — do not park HTTP threads behind an admin-write lock

A live read for Odinaka Emmanuel (the exact 1ceoxauusd@gmail.com trader) found
purchase f1093f83-966f-458e-a1ae-2d07e7d0482d still pending_review, no assigned
purchase mirrors and no trader_account for that purchase. Candidate 477500066
remains available with no ownership/archive history and one exact pool row.
No approval, account issue, historical account revival or email was performed
by these diagnostic queries. Public API probes then repeatedly timed out even
on the root health response, despite previously responding within a second.

V217's blocking mutation-lock acquisition could park all Gunicorn threads behind
one slow write. The gate now applies to administrative writes, not customer
payout submissions or ordinary account-data intake, and known engine heartbeat
routes explicitly bypass it. Lock acquisition is nonblocking. A competing admin
write gets JSON 503 with NP_ADMIN_ACTION_BUSY, operation_started=false, Retry-After
and correct browser CORS before Flask runs any handler. No write is silently
queued and no operation is reported as successful without executing it. Database
claims/financial CAS remain authoritative across workers and for customer requests.

Both remaining Supabase SDK clients used the SDK's 120-second default database
timeout; this is now eight seconds for connection, pool and read waits using the
SDK's public synchronous ClientOptions export. Storage settings are unchanged.
The real SDK clients were instantiated with private fixture keys (no network
calls), validating the effective timeout and privileged-client absence behavior.

Added /healthz/http returns only a scheduling release tag and optional public
RENDER_GIT_COMMIT, with no-store caching, to confirm actual live code independently
of Git push. A real Gunicorn test covers one active write plus twelve competing
writes: login remains responsive, only one write runs and others explicitly
return not-started busy responses. Additional tests cover data-ingestion progress
while an admin action runs, busy-response CORS and release after exceptions.
Validation: 135 Python tests passed, including real Gunicorn saturation, SDK
client-construction timeout checks, and the existing financial/lifecycle tests.
Existing Admin View handoff and trader payout JavaScript checks also passed.

### V219 — read-only sales cohort outcomes

Adds authenticated GET /admin/sales_outcomes?from=YYYY-MM-DD&to=YYYY-MM-DD.
Inclusive Africa/Lagos dates use the original purchase creation/transaction date,
not approval date. Approved original purchases are counted once by purchase ID;
reset purchases and rejected/cancelled/refunded purchases are excluded. Exact
purchase IDs, account mirrors with matching owners, and exact same-owner source
links connect account history. No name/email/login matching merges purchases.
Ever-breached, passed-at-least-one-phase, and reached-funded milestones overlap;
percentages use sold purchases as denominator. Replacement history cannot create
another sale. A funded placeholder without a broker login is not a funded issue.

Independent bounded REST pagination reads complete history, up to 50,000 rows per
table; deadline/cap/schema errors return 503 instead of partial/zero totals. No
business writes, notifications, monitoring rules, or assignment changes.

Updated website file: /workspace/payout-dashboard-fix/admin_sales_outcomes.html,
based on the current publicly served admin page downloaded 14 October 2026.
Adds Sales Outcomes navigation, dates, four KPI cards, plan breakdown and purchase
history. HTML values are escaped; stale report responses cannot replace new ones.
Website upload remains separate from backend auto-deployment.

Validation: 141 Python regression tests passed before adding two endpoint
validation/auth tests; all eight focused report tests then passed. All inline
admin scripts parse and mocked UI success/error/escaping checks pass. Read-only
live validation loaded 481 purchases and 1,927 accounts without mutations.

### V220 — reserve HTTP capacity for admin login

14 October: live website responded HTTP 200 in 1.48 seconds, but separate
healthz/http and empty-body staff_login probes repeatedly timed out at 6–12
seconds. These checks do not use production credentials or perform business
writes. This establishes API unresponsiveness, not its precise Render runtime
cause; restart/memory/worker configuration requires Render logs to distinguish.

The V218 write lock does not prevent concurrent slow GETs from occupying every
Gunicorn thread. Add outer read/write admission middleware: six ordinary requests
per eight-thread worker; staff_login, healthz/http and preflight bypass admission.
Gunicorn post_worker_init derives capacity from its actual configured thread
count, reserving two where available. Overflow returns 503 with no-store, CORS,
Retry-After and operation_started:false BEFORE invoking any business handler.
No automatic write retry, lifecycle rule change, auth bypass or new account
assignment. Held slots release on handler/stream errors and response close.
The existing serial admin mutation middleware remains inside this boundary.

Real Gunicorn test: six overlapping slow reads + twelve overflow reads; overflow
503 without execution, authenticated test login <0.7 seconds, wrong password401,
readiness200; existing mutation serialization tests pass. Additional middleware
checks cover rejected financial writes, CORS, stream failures and actual worker
capacity. Tests use private fixture credentials and AST-extracted auth, never
import production app.py or send notifications.

### V221 — distinguish paid payouts and funded breaches in sales outcomes

Read-only report now reads payouts alongside purchases/accounts, with the same
bounded complete pagination. Adds collected_payout purchases, collected rate,
paid transaction count, amount paid and funded_breached purchases/rate. A paid
payout must have status paid, an exact account ID and matching account owner.
Pending/approved/cancelled/rejected requests never count even with a stray old
paid_at timestamp. Distinct payment IDs sum once each using Decimal; multiple
payments still count the purchase once. Payments remain visible if that funded
account later breaches. Evaluation-only breaches are not funded breaches.
Invalid amounts fail the report instead of silently understating money; unlinked
paid rows are explicitly reported rather than assigned by name or broker login.
Dates select the sales cohort; paid amounts cover that cohort's history to now.

UI based on current live admin HTML, adds six KPI cards, funded-breach/collected/
amount columns by plan and by purchase. No account or financial actions changed.
Validation: 13 report/auth/date/payment tests, whole inline-script syntax and UI
render/escaping/error checks passed. Read-only live aggregation verified complete
history and exact paid associations; no database mutations or notifications.

### V222 — reconcile sales outcomes at a glance

Replace overlapping headline milestones in the website with exclusive historical
purchase outcomes. Retain old backend milestone fields for compatibility. Adds
outcomes and reconciliation_total by cohort and plan, with one outcome_label per
purchase. Sold reconciles exactly to breached-before-funded + not-funded-yet +
reached-funded + records-needing-review. Not-funded-yet splits into awaiting first
account, no pass/breach recorded, and passed a phase but not funded. A recorded
breach from an older challenge does not double-count a recovered funded purchase.
Funded splits into paid/no funded breach, paid/also funded breach, no payout/funded
breach, and no payout/no funded breach. A paid account with no funded proof is
explicitly classified as needs_review, not silently credited as a funded success.
No unbreached account is presumed passed or active; missing outcomes are labelled.

UI removes “ever breached” and “passed a phase” as headline competing totals;
shows reconciled arithmetic, funded breakdown, separately labelled payout result,
plan reconciliation and one labelled outcome per original purchase. Paid is a
recorded status, not independent bank-transfer verification. No business writes.

Validation: 15 Python report/auth/payment/reconciliation tests; inline script
parsing and UI rendering/error/escaping checks pass. Read-only full-history sample:
336 sold = 121 breached before funded + 52 not yet funded + 163 funded + 0 review.
Funded: 25 no payout/no funded breach + 50 no payout/funded breach + 17 paid/no
funded breach + 71 paid/funded breach = 163. 88 recorded payout recipients.

### V226 — reserved essential admin reads and failed-load honesty

User's overview displayed registered count but zero business data after request
admission reported capacity busy. Two independent problems: generic slow traffic
could take all six non-auth HTTP slots; website fallback converted core HTTP
failures to empty arrays then displayed them as zero totals.

Reserve one request lane each for /admin_bootstrap and /admin_operations_center,
separate from each other and ordinary requests, within the same existing bounded
business capacity. With eight Gunicorn threads: four ordinary requests, one
bootstrap, one operations; sign-in/readiness keep their remaining capacity. Auth
checks still run normally and database/financial CAS guards are unchanged. No
queued mutation or automatic mutation replay is introduced.

Website coalesces overlapping core refreshes, caps fallback feed concurrency at
two, requires core feeds to succeed, preserves previous data on failure and
propagates failure instead of announcing successful refresh. Missing initial data
renders unavailable rather than zero cards. Operations refreshes likewise share
one in-flight request and preserve previously loaded data.

Validation: all 156 Python tests passed, including real Gunicorn test with four
slow ordinary requests plus slow operations: bootstrap and authenticated fixture
login responsive under 0.7 seconds; overflow rejected before execution. JS checks
cover cached state preservation, first-load unavailable rendering, core refresh
coalescing, two-read fallback concurrency; date/report checks still pass. No
production imports, notifications or financial writes during verification.

### V227 — simplified paid-account journey

User-requested first row: total bought, total breached, total reached funded,
total got payout, figures first with percentages of selected-cohort purchases.
Next card contains only Got paid and Got paid → breached → closed. The closed
figure is a subset of Got paid and requires a paid_at strictly before breached_at
within the SAME exact purchase journey and an explicit terminal breached/closed/archived
state. Missing dates, reversed/equal order, active state and another purchase's
breach do not establish this sequence. A later closed Funded replacement in the
exact original purchase journey counts after its parent account's paid payout. Original paid amounts remain preserved;
no account closure or lifecycle/payment changes occur.

Adds paid_then_breached_closed count/rate to backend report and per-plan totals.
Website keeps strict DD/MM/YYYY handling, date-scoped recalculation, Total row,
and admin failed-load safeguards. 16 report/auth/date/payment tests passed plus
JS date scope, input refresh, failed-load and operations preservation checks.

### V228 — confirmed schema mismatch blocking ordinary purchase assignment

Read-only inspection of Odinaka's exact new purchase f1093f83-966f-458e-a1ae-
2d07e7d0482d confirmed pending payment/status, no assigned broker/account mirror,
no account linked to the purchase and no retained approval claim. A direct live
query using approve_purchase's history projection returned HTTP400 / PostgreSQL
42703: trader_accounts.status does not exist. Real field is account_status.
V216's required fail-closed ownership check therefore stopped approval before
trader resolution or assignment; it did not indicate a lack of inventory.

Remove nonexistent fields from literal trader_accounts read projections, using
live field-name metadata only: approval history, assignment-status polling,
rejection history, assignment queue, clean pass retry, reset entitlement lookup
and summary reads. These read-only compatibility changes preserve all rejection,
duplicate, exact-owner, account-size, fresh-inventory and final ownership guards.
No errors are swallowed, no historical accounts reactivated, and no customer
payment approvals/MT5 allocations are performed during diagnosis.

The final approval-body regression test uses the actual AST-extracted function
with a strict field schema: valid pending purchase reaches a stub assigner; an
existing assignment/rejected purchase/failed ownership read still stops before
assignment. Assignment-status polling now uses supported fields (including
removing nonexistent assigned_at from trader_accounts). All eight corrected
projections returned HTTP200 against live PostgREST with read-only limit1 queries.
A fixture contains column names only, no account values or credentials.
