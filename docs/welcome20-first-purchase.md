# WELCOME20 — first account purchase only

20% off the current server-priced initial account purchase. Enter WELCOME20 in
the existing promo field. No minimum spend, campaign expiry or registration-age
cutoff was requested; zero purchase/account history defines eligibility.

Requires a signed-in registered trader, exact current plan, and no earlier or
pending challenge purchase or existing MT5 account. Matching registered email
(case/whitespace normalized) and phone (including Nigerian 0/+234 variants) are
checked across profiles and purchase history. Browser-supplied contacts are not
eligibility evidence. Pending/rejected/cancelled orders count as history under
the owner's strict "no history of purchase" instruction; users should resume
an existing order instead of creating a second discounted order.

Not valid for resets, funded resets, renewals, payouts, upgrades, second life,
or later account purchases. No reset/renewal pricing functions were modified.
It cannot stack with referral/private-offer discounts. The promo itself has
zero affiliate commission; separate registration-based referral accounting
retains the existing policy. Normal referral and private-offer pricing is unchanged.

A before-insert database trigger rechecks eligibility and exact discount values.
All purchase inserts share a short transaction lock to prevent parallel first
orders. A private RLS-protected hashed identity ledger retains promo-redemption
evidence even if the purchase is later deleted. Ledger keys
contain hashes rather than raw email or phone. RPC and ledger privileges are
server-only; no public/browser role is granted eligibility-history access.
Existing purchases and financial records are not rewritten.

Protection uses recorded account/contact identity; it does not prove that a
person using wholly different registration details is the same individual.

Tests: tests/test_first_purchase_promo.py and temporary-only database fixtures
in tests/first_purchase_promo_checks.sql. The latter run atomically with the
migration and do not create real customers, orders, payments or emails.
