NAIRAPIPS STATIC / EQUITY TRAILING — 7 NOVEMBER 2026

Static: starting capital minus the fixed naira loss allowance.
Trailing: highest observed equity minus the SAME fixed naira allowance.
Example: N1m, 10% DD, peak equity N1.2m -> breach at N1.1m.
Includes open-trade profit. There is no breakeven cap; the floor never falls.
Withdrawals do not lower the floor. Payout checks reserve the floor plus one kobo.
New stage/reset accounts start separate journals. Existing accounts keep frozen terms.

THIS IS A WINDOWS VPS UPDATE, NOT A CPANEL UPDATE.
The API/editor can display/save Trailing before broker installation, but purchases
and assignment stay blocked until the runtime verification setting is enabled.
No existing account has been changed or activated by this release.

1. Extract this ZIP to a staging folder on the DD Windows VPS.
2. Preserve all existing DD latch, relay and journal folders. Do not delete them.
3. Double-click CHECK_ONLY.cmd on the VPS. It detects the exact DD worker from
   running Python processes and verifies the release/checksums. It changes nothing.
   Earlier setup folder: C:\Users\Administrator\Documents\nairapips_mt5_rotation_engine (1)
   Fleet state/logs are separate: C:\NairaPips_DD_Fleet
4. Review the detected worker path. Double-click INSTALL_UPDATE.cmd to back up
   and install under the filename your existing supervisor already uses.
   Multiple different running DD worker paths stop the installer without changes.
   If the worker is not running, only the check can use the old setup folder;
   installation then requires an explicitly confirmed -WorkerPath in PowerShell.
5. Restart the existing DD supervisor only. Do not start a second supervisor or
   change Operations/Target Engine, MT5 passwords, shard counts or assignments.
6. Verify every shard's fresh dd_health_shard_XX.json reports:
   NAIRAPIPS_DD_POLICE_STATIC_EQUITY_TRAILING_V1_20261107
   Verify all existing Static accounts remain covered and normal close/relay
   operation remains healthy. Never test by intentionally breaching a customer.
7. The collector uses NP_EQUITY_TRAILING_INGEST_KEY when set; otherwise it uses
   the existing NP_LIVE_STATE_INGEST_KEY. Configure the matching secret on the
   main API and monitoring API as NP_EQUITY_TRAILING_INGEST_KEY. The live-state
   service can keep its existing NP_LIVE_STATE_INGEST_KEY when reusing that key.
   If you choose a separate trailing key, set NP_EQUITY_TRAILING_INGEST_KEY
   consistently on the VPS and all three API services. Never paste keys in chat.
8. Only after ALL shards and the matching server deployments are verified, set
   NP_EQUITY_TRAILING_VERIFIED=NP_EQUITY_TRAILING_V1_20261107 on the main API.
   The normal per-package purchase tick/untick controls still decide what sells.
   Removing this verification setting closes NEW trailing purchases, while
   monitoring on already-issued accounts continues.

Journal storage: equity_dd_state beside the installed worker, or the existing
NP_EQUITY_DD_JOURNAL_DIR override. Keep this directory through restarts/upgrades.
A missing journal can initialize only within five minutes of assignment. An old
account with missing/corrupt history requires verified recovery, never a reset.

The installer does not stop processes, launch workers, touch terminals, change
customer records, or open purchases. Windows broker operation must be verified
on the VPS; Linux offline/browser checks are not a substitute for that check.
