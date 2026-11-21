# Prospective closed-balance percentage trailing — 21 November 2026

Requested by the owner: future purchases of TRAILING plans use
`highest verified realised balance × (1 − purchased DD percentage / 100)`.
No cap at initial balance. The reference never falls. Floating profits never
raise it; live equity touching or crossing its floor still breaches. Fees are
included in realised trading growth. Cash deposits are excluded from growth;
withdrawals do not lower the reference. Daily limits remain independent.

## Frozen contract and compatibility

New normal/managed purchases freeze `trailing_formula=highest_closed_balance_percent`
inside the existing purchase risk marker. Each assignment, phase, reset and
funded/payout renewal inherits that exact purchase snapshot. Custom-package
quotes freeze the same formula in each stage's immutable hashed contract.
Pre-release unconsumed legacy trailing custom quotes must be refreshed.
Already accepted orders remain recoverable.

Existing purchases, accounts, journal fingerprints, terminal breach latches,
static rules and prior equity/fixed-naira trailing calculations are unchanged.
No historical financial rows or customer records are migrated by this release.
The existing database purchase-continuity guard copies the entire risk marker.

## Activation and Windows installation

Cloud support alone does not activate prospective sales. All four Windows DD
shards must publish authenticated `dd_police_balance_pct_v1_shard_N_of_4` health
signals after successful exact-login/server, reconciled broker-ledger probes.
The private technical capability receipt is written only after that proof.
Fresh older-worker signals block sales again, including during a mixed rollout.
A proven installation remains recorded when no accounts are active.

Download `/downloads/static-trailing-dd-update.zip` from the main Render API.
Extract on the existing **Windows DD VPS**, never cPanel. Run `CHECK_ONLY.cmd`,
then `ROLL_UPDATE.cmd`. The installer validates the existing path/supervisor,
backs up code and rolls the existing four workers one at a time. It requires
both daily and closed-balance probe readiness for each replacement. Preserve
all journals, latches, relay state, terminals and the existing supervisor.
Do not start a second supervisor. A paused rollout needs inspection, not state
removal. This workspace has no direct Windows remote-execution connection.

New worker release: `NAIRAPIPS_DD_POLICE_BALANCE_PERCENT_V1_20261121`.
Cloud formula support: `NP_BALANCE_PERCENT_TRAILING_V1_20261121`.
The main `/drawdown_version` reports `future_trailing_formula` and
`balance_trailing_sales_ready`; only the latter confirms activation.
The older `release`/`trailing_basis` fields remain for legacy integrations.
