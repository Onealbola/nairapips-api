# Static and equity-trailing rules for future offers

The owner chooses `STATIC` or `TRAILING` separately for each package, in every
1-phase, 2-phase and instant-funded account size. New sizes default to Static.
The legacy saved matrix remains readable without the optional `drawdown_type`.
Saving a draft does not open sales. Browser checkout cannot choose risk rules.

Trailing is uncapped highest observed equity minus the starting-capital naira
allowance. Floating profit raises the watermark. Losses and cash withdrawals
never lower it. Equality with the floor breaches. Each separately issued stage
or reset account has its own watermark. An earlier low is never compared with a
later high to manufacture a historical breach.

Future purchase snapshots include the mode; issued trailing accounts carry an
`equity_trailing_v1:` source marker. Subsequent offer edits do not change these
accounts. Equivalent numeric database/API forms have identical fingerprints.

The DD worker commits watermark state atomically before acting on a risk result,
using an exact-account file and process lock. A corrupt journal, wrong identity,
changed frozen terms, stale observation or missing old watermark cannot become a
new initial balance. Broker closing and existing first-breach latch/relay stay in
the DD worker. Static accounts keep their existing calculation branch.

The API/live-state services maintain a second cross-worker CAS journal in the
existing service-only `package_studio_private_settings` table, under a separate
`equity_drawdown_v1:<account UUID>` key. This requires no schema migration and
does not write commercial settings, payments, purchases or emails. Authenticated
collector reports carry the worker's observed peak so latest-only delivery does
not lose an intervening equity high. The server compares observation order and
rejects unauthenticated trailing updates. The current Static hot path does not
add a private-state read or an extra purchase lookup.

Pass validation and payout quotes verify the durable floor. A stale/missing
trailing state cannot authorize a pass or payout. Payout capacity reserves the
floor plus one kobo; a withdrawal never lowers a purchased trailing floor.
Dashboard responses show the saved mode and floor for a future trailing account.

## Installation status and release control

The cloud API updates are independent of the Windows DD installation. The
Windows worker is delivered in `NairaPips-Static-Trailing-DD-Update.zip` with a
check-only-first PowerShell installer, backups and SHA256 checksums. It has not
been run on the VPS from this workspace. No broker session was started here.

The collector authentication secret must match the main API, monitoring API and
live-state service. Existing `NP_LIVE_STATE_INGEST_KEY` may be reused; the dedicated
override is `NP_EQUITY_TRAILING_INGEST_KEY`. Keep its value out of chat and files.

After every DD shard and all three server consumers are verified, the operator
can set `NP_EQUITY_TRAILING_VERIFIED=NP_EQUITY_TRAILING_V1_20261107` on the main API.
Before that, drafts can be saved but trailing purchases and assignments stay
blocked. This guard is adjustable; it is not a permanent product restriction.
The owner still opens or pauses each product using its normal purchase switch.
Removing the runtime verification pauses new trailing sales/assignments; it does
not rewrite or disable risk monitoring on already-issued accounts.

No new product, feature flag, purchase switch or account has been activated by
this development. The existing custom-contract package builder retains its other
release blockers; this update does not silently release it.
