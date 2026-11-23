# Consistent future checkout versions

Managed TRAILING plans stored before the closed-balance rollout can omit its
formula marker. `commercial_plan` adds that prospective marker to the advertised
checkout plan. Comparing that advertised version against the raw stored row
incorrectly rejected unchanged offers as `PACKAGE_OFFER_CHANGED`.

`offer_version` now projects the same prospective terms before hashing. It is
idempotent for already projected plans and leaves STATIC versions unchanged.
Actual fee, drawdown, payout-share, frequency or other term changes still require
fresh acceptance. Stored plan rows and existing purchases/accounts are never
rewritten by this calculation.

`checkout-terms.js` ships through the existing hosted trader catalogue bundle.
It renders the current configured profit cap and optional paid-reset availability
rather than the old hard-coded 15%/no-reset text. Failed quote reads block
submission; late responses cannot overwrite a different checkout. No purchase
POST is retried automatically and no new payment is requested.

Validation: a previously failing regression covering advertised trailing offers,
strict stale-offer rejection and no physical-row mutation; a read-only snapshot
of the reported NGN2.5m More Share offer confirms its public version now matches
the stored-row check; isolated Chromium checkout/term tests. No production
purchase, payment, account, plan or trader records were changed during testing.
