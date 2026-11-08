NAIRAPIPS STATIC / TRAILING / OPTIONAL DAILY LIMITS — 8 NOVEMBER 2026

THIS IS A WINDOWS VPS UPDATE, NOT A CPANEL UPDATE.
No plan, sales checkbox, account terms or reset price is activated by installation.

Static: starting capital minus the fixed naira loss allowance.
Trailing: highest observed equity minus that same fixed naira allowance.
Floating profit counts. The trailing floor never falls after a withdrawal/loss.
Optional daily limits use today's broker trading net (fees included) plus current
floating P/L, against starting capital. Deposits/withdrawals are excluded.
The trading day changes at Nigeria midnight (UTC+1). A breach remains latched.
Existing issued accounts retain their purchased rules; missing old history is
never silently reconstructed or cleared.

1. Extract this ZIP into a staging folder on the DD Windows VPS.
2. Double-click CHECK_ONLY.cmd. Review the detected exact worker path.
3. Double-click ROLL_UPDATE.cmd. It backs up code, installs the reviewed files,
   then rolls one of the existing four workers at a time under its supervisor.
   Each new shard must verify broker daily-history access before the next rolls.
   The existing relay restarts last. Operations/Target and MT5 terminals stay up.
   Keep the existing supervisor and Operations windows open/minimized.
4. If the rollout pauses, keep its output and inspect the reported shard. Do
   not start another supervisor or delete latch, journal, relay or state files.
   The backup directory is printed. No automatic rollback erases stored state.
5. Verify all four fresh health files report:
   NAIRAPIPS_DD_POLICE_STATIC_TRAILING_DAILY_V1_20261108
   and daily_history_probe_ready=true.
   The APIs require all four authenticated shard capability signals before new
   daily-limit purchases are allowed. Successful installation is recorded as a
   technical capability; per-plan owner checkboxes still decide what sells.

The existing matching ingest keys remain required on the VPS and APIs. Do not
paste secrets in chat. Broker verification cannot be replaced by Linux tests.
Journal directory: equity_dd_state beside the worker or the existing override.
Preserve it through every restart and upgrade.
