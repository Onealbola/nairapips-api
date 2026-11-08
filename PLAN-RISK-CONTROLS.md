# Owner-configured plan tools

Every package can independently select Static or equity Trailing drawdown, an
optional daily loss percentage, an optional payout schedule, and paid resets for
Phase 1, Phase 2 and/or Funded stages. New package controls default off; deploying
this code does not edit saved prices, activate products or rewrite issued terms.

Package Studio contains every package's controls. Business controls also exposes
the same switches for ordinary plans. Owner authentication and optimistic save
checks remain required. The server constructs and validates rule metadata;
callers cannot submit arbitrary frozen account terms.

Daily loss is negative trading net since Nigeria midnight plus current floating
loss, measured against initial capital. Deal commissions, swaps and fees count;
cash deposits and withdrawals do not. Broker account identity is checked before
and after history reads. The existing overall DD guard remains authoritative if
daily history is unavailable and an overall breach is provable. A daily breach
is permanent for that account, even after midnight.

Payout schedules are Daily, Weekly, Biweekly (14 days), calendar Monthly or On
Demand. Scheduled eligibility starts at the funded account assignment, then the
latest paid payout for that exact account. Other existing eligibility rules still
apply. Issued rules are frozen in purchase/account journey metadata without a
schema migration. Old unmarked accounts retain their prior policy.

Paid resets are independent per-stage offers at owner-entered positive prices.
Disabled stages quote unavailable. Current plan settings govern new reset quotes;
a submitted payment keeps its already quoted amount. A newly configurable offer
cannot fall back to another product's reset terms. Existing lifecycle and paid
reset approval checks remain in place. This does not grant free lives or renew
consumed payout entitlements.

The Windows ZIP includes a checked rolling update: one exact worker at a time
under the existing four-shard supervisor, followed by its exact breach relay.
It preserves journals, latches, terminals, outbox and Operations/Target engine.
All four authenticated daily-history capability signals are required before new
daily-rule purchases can open. The capability record is technical installation
state, not a commercial activation. Existing accounts are not used for deliberate
breach testing. Broker installation remains pending until performed on the VPS.

## Optional withdrawal thresholds and funded profit cap

All packages and ordinary plans expose independently enabled minimum payout in
naira, minimum gross profit as a percentage of initial capital, and a gross profit
cap per funded cycle. Minimum amount is measured after the trader's profit split.
Minimum profit is measured before that split. Impossible target/cap or minimum
amount/split combinations are rejected. Disabled minimums impose no threshold.
Existing saved cap behaviour is retained until the owner changes it.

Withdrawal metadata uses a separate marker before the existing risk marker. This
keeps old daily/trailing metadata readable and preserves stable managed plan IDs.
Purchase snapshots, stage assignment and resets inherit the purchased policy;
editing a plan never rewrites issued account policy.

Exact-account verified payout quotes apply the purchased cap; disabled cap means
no cap reduction. Existing exposure, payout availability, duplicate, identity,
trading-lock and paid-account guards remain. Minimums are checked before request,
approval and marking paid. Approval/payment require a fresh verified quote for
accounts with these rules. A stale receivable request cannot authorize payment.

The supplied Operations update reads the canonical purchased cap and uses it for
broker cap detection and scan priority. Legacy accounts keep their former 15%
cap. Evaluation targets and DD ownership are unchanged. CHECK_ONLY and backup-only
installation are provided; no existing VPS process is stopped by the installer.
Custom or disabled caps stay closed to new purchases/assignment until the owner
verifies the updated Operations engine and sets technical environment flag
NP_PLAN_WITHDRAWAL_VERIFIED=NP_WITHDRAWAL_RULES_V1_20261108 on the main API.
This is installation readiness, not a product setting. Minimum-only rules with
the existing 15% cap require no Operations code change to enforce withdrawals.


Verified Operations installations can also be recorded in the existing private
settings table under withdrawal_controls_runtime_v1. The readiness check is
read-only: it requires enabled=true, the expected release and exact Operations
engine SHA256, plus a valid verification timestamp. This records installation
readiness after the owner has installed the checksum-checked update and shown a
fresh successful broker scan. It never enables a product or changes a price.
Revoking the record blocks new custom-cap sales; issued protection continues.
The explicit Render verification flag remains supported for compatibility.
