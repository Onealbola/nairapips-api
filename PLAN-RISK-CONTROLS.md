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
