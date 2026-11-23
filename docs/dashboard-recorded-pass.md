# Recorded phase pass visibility

A passed phase is archived to stop trading. The legacy trader helper also
classified every row with `archived_at` as reset history. As a result, a genuine
`archived_phase1`/`archived_phase2` row could disappear from Overview while an
unrelated count still called old funded profit-cap records active.

The hosted `dashboard-account-state.js` patch treats an archival timestamp alone
as insufficient reset evidence for an authoritative recorded phase-pass status.
All other reset/replacement evidence remains checked by the original helper.
Breaches and explicit `phase_pass_recorded:false` do not gain pass status.
Progression consumption follows the row's saved journey instead of defaulting a
Phase 1 source straight to Funded; exact successor/purchase checks remain intact.
The active-account count excludes archival and payout-locked records and counts
unique broker-server/login pairs. Payout records remain visible for financial
review. No database row, trade permission, pass, reset, payout or MT5 assignment
is changed by these display helpers.

Read-only reproduction used the reported trader's 48 safe account rows: an exact
recorded Phase1 pass with an archival timestamp, 31 older funded-cap records,
and other histories. Before the patch, it reproduced SAFE, no MT5, zero balance,
and 31 active. Afterwards it selected the latest recorded pass, showed its
actual balance/profit and saved Phase2 waiting stage, with zero tradable active
accounts. Regression fixtures use anonymous synthetic identities, preserving
real reset/breach handling, exact successor consumption and independent journeys.

The patch ships in the existing hosted `/public/trader-catalogue.js` bundle, so
installed trader pages receive it after refreshing; no cPanel upload is required.
