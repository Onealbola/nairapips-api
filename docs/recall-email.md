# Recall notification email

Future Management Recall/Hold and Universal Recall actions notify the exact
registered trader. The branded message identifies the withdrawn MT5 login,
asks the trader to stop using that account and disregard its old credentials,
and asks them to wait for admin instructions. Any replacement is communicated
separately. Passwords, internal recall markers and account balances are excluded.

Recall hooks enqueue identities only after the original operation succeeds;
provider/network work stays outside the recall request thread. A durable scan
of new archival events recovers crashes before enqueue. A private activation
cursor prevents deployment from sending historical bulk notices.

Each exact account delivery has one durable receipt and provider idempotency key.
A compare-and-set claim prevents competing workers sending the same notice.
The account, owner and recipient are checked again before provider submission.
An account no longer recalled cancels delivery. Definitively rejected provider
rate/configuration errors wait before retry; ambiguous provider failures become
unconfirmed and are not automatically resent. A receipt-write failure retains
sending status instead of replaying an uncertain email. Accepted means provider
acceptance, not proof that the recipient opened or received the message.

The existing service-only private settings table stores the activation cursor
and receipts under `recall_email_activation_v1` and `recall_email_v1:<account UUID>`.
Normal email logs use `account_recall`. No schema, website upload, financial,
entitlement, broker or original recall-lifecycle changes are required.
