# Daily enforcement timing investigation

Threshold calculation, sampling frequency, broker closure, and backend reporting
are separate events. The current worker uses the purchased daily allowance in
its risk calculation and priority score. Equality with the loss floor triggers a
permanent latch; closure/zero-exposure verification precedes relay paperwork.
A faster dashboard refresh cannot enforce a broker limit between observations.
Market gaps and execution slippage also prevent a promise of an exact loss fill.
Do not reduce a customer's purchased allowance or move the floor to disguise an
execution overshoot.

The reported account's saved journal shows daily allowance250000 and recorded
loss255166.28 at the first breach observation. That overshoot was already present
in the sampled observation. It does not by itself identify scan delay, a closing
trade, or broker slippage. The synced dashboard trade ledger is incomplete and
must not substitute for the broker's full history.

`/downloads/dd-timing-audit.ps1` is a read-only Windows diagnostic, not an engine
installer. It reads whitelisted shard-health fields, reviewed code timing
constants, bounded recent log tails and whitelisted exact-login outbox evidence.
It captures position count at detection and the machine time zone for comparison.
It never exports the roster, passwords, full process command lines or raw latch
files. It does not connect/login to MT5, issue orders, restart processes, or
change rules/journals/statuses. It writes a report to the user's Desktop and
opens Notepad. Log tails may omit old evidence and are not a complete history.
The Windows script has static safety/delivery checks; actual VPS execution and
execution timing remain pending the user's report.

Admin breach rows now use the recorded daily/overall cause instead of a generic
100%-used label and label the cumulative loss column Overall Loss %. Frozen
account rules take precedence over a current mutable plan. The hosted admin and
rebuilt cPanel packages include the labels; existing cPanel installations need
the updated admin package to change their embedded UI.
