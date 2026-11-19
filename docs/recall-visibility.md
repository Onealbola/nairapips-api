# Recalled deliveries in trader views

Management Recall/Hold and Universal Recall archive an exact delivery for audit.
An archive alone is not a recall: passed, breached, reset and ordinary historical
accounts remain visible according to their existing rules.

`recalled_account_visibility.py` recognizes an archived source using its exact
account-bound Management Hold or Universal Recall marker, plus supported explicit
legacy recalled statuses. A live replacement referencing a recalled source is
not hidden. The safe trader row carries a boolean visibility flag so omission
of internal archive notes never restores a recalled credential card.

Trader bootstrap, current-account, dashboard and authenticated trader account-list
responses omit recalled deliveries and stale credential mirrors. The admin-wide
account feed retains the records for audit. This layer makes no database,
financial, entitlement, broker or lifecycle changes.

The hosted `account-reporting.js` updates already-installed trader panels through
the existing catalogue script. It purges recalled rows from local lists,
selections, credential views and delayed responses. In-page exclusions are sticky
so an old request completing after recall cannot resurrect a card. Reloading
starts from the current server authority, including explicit admin returns.

Validation covers both recall types, exact-source markers, replacement identity,
normal historical accounts, safe-row filtering, trader/admin response scope,
actual Vault/Accounts/Dashboard rendering, and delayed browser responses.
